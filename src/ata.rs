use std::{sync::Arc, time::Duration};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status_client_types::TransactionConfirmationStatus;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use std::str::FromStr;
use tokio::sync::broadcast;

use crate::{
    measure::{measure_pool_cu, MeasureOutcome},
    mongo::Repo,
    pool::PoolDoc,
    ws::ServerMsg,
    zeroslot,
};

const CU_PRICE: u64 = 100_000;
const CU_LIMIT: u32 = 50_000;
const BACKOFF: [u64; 3] = [1, 3, 9];

pub async fn create(
    pool: String,
    base_mint: Pubkey,
    token_program: Pubkey,
    wallet_kp: Arc<Keypair>,
    rpc_url: String,
    repo: Arc<Repo>,
    broadcast: broadcast::Sender<ServerMsg>,
    doc: PoolDoc,
) {
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let wallet_pk = wallet_kp.pubkey();
    let http = match zeroslot::build_http_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ata] {pool} zeroslot client build failed: {e:#}");
            return;
        }
    };
    let zs_tip_pk = match Pubkey::from_str(zeroslot::ZEROSLOT_TIP_ACCOUNT) {
        Ok(pk) => pk,
        Err(e) => {
            eprintln!("[ata] {pool} bad ZEROSLOT_TIP_ACCOUNT: {e}");
            return;
        }
    };

    let price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);
    let limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT);
    let ata_ix =
        create_associated_token_account_idempotent(&wallet_pk, &wallet_pk, &base_mint, &token_program);
    // Zeroslot priority lane requires an inline tip transfer to its tip
    // account. 100k lamports (~$0.01) matches `zeroslot::TIP_LAMPORTS`.
    let tip_ix = system_instruction::transfer(&wallet_pk, &zs_tip_pk, zeroslot::TIP_LAMPORTS);

    for attempt in 1usize..=3 {
        if let Err(e) = repo.bump_ata_attempts(&pool).await {
            eprintln!("[ata] {pool} bump_ata_attempts failed: {e:#}");
        }

        let blockhash = match rpc.get_latest_blockhash().await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[ata] {pool} attempt {attempt} get_latest_blockhash: {e}");
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(BACKOFF[attempt - 1])).await;
                }
                continue;
            }
        };

        let tx = Transaction::new_signed_with_payer(
            &[price_ix.clone(), limit_ix.clone(), ata_ix.clone(), tip_ix.clone()],
            Some(&wallet_pk),
            &[&*wallet_kp],
            blockhash,
        );

        // Send via Zeroslot (replaces the prior public-RPC
        // send_and_confirm). Confirm via the regular RPC because
        // Zeroslot doesn't expose getSignatureStatuses.
        let sig = match zeroslot::send_transaction(&http, &tx).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[ata] {pool} attempt {attempt} zeroslot send failed: {e}");
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(BACKOFF[attempt - 1])).await;
                }
                continue;
            }
        };
        // Poll for confirmation up to 60s. Identical pattern to
        // `measure_inner` so failure semantics are uniform.
        let confirm_start = std::time::Instant::now();
        let confirm_timeout = Duration::from_secs(60);
        let mut confirmed = false;
        let mut on_chain_err: Option<String> = None;
        while confirm_start.elapsed() < confirm_timeout {
            match rpc.get_signature_statuses(&[sig]).await {
                Ok(resp) => {
                    if let Some(Some(status)) = resp.value.into_iter().next() {
                        if let Some(conf) = &status.confirmation_status {
                            if matches!(
                                conf,
                                TransactionConfirmationStatus::Confirmed
                                    | TransactionConfirmationStatus::Finalized,
                            ) {
                                if let Some(err) = &status.err {
                                    on_chain_err = Some(format!("{err:?}"));
                                } else {
                                    confirmed = true;
                                }
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ata] {pool} attempt {attempt} get_signature_statuses: {e}"
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if confirmed {
            if let Err(e) = repo.mark_ata_confirmed(&pool).await {
                eprintln!("[ata] {pool} mark_ata_confirmed failed: {e:#}");
            }
            // Optionally fire a 0.001 SOL probe buy to record the pool's
            // CU consumption. The underlying `measure_pool_cu` now signs
            // with `get_latest_blockhash()` (see measure.rs module doc),
            // so there's no longer a BUY_NONCE collision with the bot
            // and the prior `ENABLE_CU_PROBES` gate has been removed.
            // The probe still costs ~226k lamports of real on-chain
            // budget per pool (tip + base fee + WSOL swap-in) — fire
            // unconditionally on every fresh pool so the first WS
            // `new_pool` broadcast carries a measured CU value.
            let measured_cu = measure_one(&rpc, &wallet_kp, &doc, &repo, &pool).await;
            let _ = broadcast.send(ServerMsg::NewPool {
                pool: doc.pool.clone(),
                accounts: doc.accounts.clone(),
                pair_created_at_ms: doc.pair_created_at_ms,
                compute_unit_limit: measured_cu,
            });
            return;
        }
        if let Some(err) = on_chain_err {
            eprintln!("[ata] {pool} attempt {attempt} on-chain err: {err}");
        } else {
            eprintln!(
                "[ata] {pool} attempt {attempt} confirm timeout sig={sig}"
            );
        }
        if attempt < 3 {
            tokio::time::sleep(Duration::from_secs(BACKOFF[attempt - 1])).await;
        }
    }
    eprintln!("[ata] {pool} exhausted all 3 attempts — row stays pending");
}

/// Run the CU probe for a freshly-ATA'd pool. Retries up to 3 times on
/// transient errors (confirm timeout, get_transaction returning null,
/// etc.) before giving up and broadcasting without cu — that way bots
/// see a measured value for the first trade rather than the static
/// `CU_LIMIT_PUMP_FUN` fallback. `SkipAtaMissing` is never retried (it
/// indicates a chain-state race, not a probe failure).
async fn measure_one(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    doc: &PoolDoc,
    repo: &Repo,
    pool: &str,
) -> Option<i32> {
    const PROBE_BACKOFF: [u64; 3] = [2, 5, 15];
    for attempt in 1usize..=3 {
        match measure_pool_cu(rpc, wallet_kp, doc).await {
            Ok(MeasureOutcome::Ok(cu)) => {
                let cu_i32 = cu as i32;
                if let Err(e) = repo.update_cu_limit(pool, cu_i32).await {
                    eprintln!("[ata] {pool} update_cu_limit failed: {e:#}");
                    return None;
                }
                println!("[ata] {pool} measured cu={cu} (attempt {attempt}/3)");
                return Some(cu_i32);
            }
            Ok(MeasureOutcome::SkipAtaMissing) => {
                // ATA confirmed seconds ago — this is a chain-state race,
                // not something a retry will fix. Bail without retrying.
                eprintln!(
                    "[ata] {pool} cu probe reported ATA missing after successful ATA-create — \
                     broadcasting without cu (no retry)"
                );
                return None;
            }
            Ok(MeasureOutcome::SkipNonWsolQuote) => {
                // Pool is quoted in something other than WSOL (e.g. USDC).
                // Our buy ix is WSOL-only — the bot wouldn't trade it
                // anyway. Broadcast without a CU value; no retry.
                eprintln!(
                    "[ata] {pool} cu probe skipped (non-WSOL quote mint) — broadcasting without cu"
                );
                return None;
            }
            Err(e) => {
                eprintln!(
                    "[ata] {pool} cu probe attempt {attempt}/3 failed: {e:#}"
                );
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(PROBE_BACKOFF[attempt - 1])).await;
                }
            }
        }
    }
    eprintln!(
        "[ata] {pool} cu probe exhausted all 3 attempts — broadcasting without cu \
         (bot will use static CU_LIMIT_PUMP_FUN until next measure_cu pass)"
    );
    None
}
