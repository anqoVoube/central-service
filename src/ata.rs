use std::{sync::Arc, time::Duration};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use tokio::sync::broadcast;

use crate::{
    measure::{measure_pool_cu, MeasureOutcome},
    mongo::Repo,
    pool::PoolDoc,
    ws::ServerMsg,
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

    let price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);
    let limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT);
    let ata_ix =
        create_associated_token_account_idempotent(&wallet_pk, &wallet_pk, &base_mint, &token_program);

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
            &[price_ix.clone(), limit_ix.clone(), ata_ix.clone()],
            Some(&wallet_pk),
            &[&*wallet_kp],
            blockhash,
        );

        match rpc.send_and_confirm_transaction_with_spinner(&tx).await {
            Ok(_sig) => {
                if let Err(e) = repo.mark_ata_confirmed(&pool).await {
                    eprintln!("[ata] {pool} mark_ata_confirmed failed: {e:#}");
                }
                // Now that the ATA exists, fire a 0.001 SOL probe buy to
                // record the pool's CU consumption. Result is persisted +
                // included in the NewPool broadcast so bots see the value
                // at first sight, not after a separate measurement pass.
                let measured_cu = measure_one(&rpc, &wallet_kp, &doc, &repo, &pool).await;
                let _ = broadcast.send(ServerMsg::NewPool {
                    pool: doc.pool.clone(),
                    accounts: doc.accounts.clone(),
                    pair_created_at_ms: doc.pair_created_at_ms,
                    compute_unit_limit: measured_cu,
                });
                return;
            }
            Err(e) => {
                eprintln!("[ata] {pool} attempt {attempt} failed: {e}");
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(BACKOFF[attempt - 1])).await;
                }
            }
        }
    }
    eprintln!("[ata] {pool} exhausted all 3 attempts — row stays pending");
}

/// Run the CU probe for a freshly-ATA'd pool. Logs but never propagates
/// errors — the broadcast happens regardless, just with `compute_unit_limit:
/// None` on failure (bot falls back to its static `CU_LIMIT_PUMP_FUN`).
async fn measure_one(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    doc: &PoolDoc,
    repo: &Repo,
    pool: &str,
) -> Option<i32> {
    match measure_pool_cu(rpc, wallet_kp, doc).await {
        Ok(MeasureOutcome::Ok(cu)) => {
            let cu_i32 = cu as i32;
            if let Err(e) = repo.update_cu_limit(pool, cu_i32).await {
                eprintln!("[ata] {pool} update_cu_limit failed: {e:#}");
                return None;
            }
            println!("[ata] {pool} measured cu={cu}");
            Some(cu_i32)
        }
        Ok(MeasureOutcome::SkipAtaMissing) => {
            // Shouldn't happen — we just confirmed the ATA above. Log and
            // broadcast without cu so bot uses its static fallback.
            eprintln!(
                "[ata] {pool} cu probe reported ATA missing after a successful ATA-create — \
                 chain state may be racy. Broadcasting without cu."
            );
            None
        }
        Err(e) => {
            eprintln!("[ata] {pool} cu probe failed: {e:#} — broadcasting without cu");
            None
        }
    }
}
