//! close_all_atas — close EVERY empty token ATA the wallet owns, reclaim rent.
//!
//! Aggressive companion to `close_unused_atas`. NO Mongo, NO keep-set:
//! enumerates every SPL token account (Token + Token-2022) owned by the wallet
//! and closes ALL of them that are empty (`amount == 0`) — no matter which
//! mint. Rent is swept back to the wallet. Batched `--per-tx` (default 5) per
//! tx, fire-and-forget, with a batched status check at the end.
//!
//! ⚠️ WARNING — this does NOT protect the WSOL ATA or any live-pool ATA. It
//!    closes anything empty. Held positions (`amount > 0`) are still skipped
//!    (CloseAccount requires a zero balance), and WSOL usually holds wrapped
//!    SOL so it won't be empty — but if it IS empty it WILL be closed, which
//!    breaks the bot's WSOL path until recreated. Prefer running during
//!    downtime, or use `close_unused_atas` (orphans-only) while bots run.
//!    A loud line prints if the WSOL ATA is among the candidates.
//!
//! Requires only WALLET_KEYPAIR + RPC_URL (both mandatory — no Mongo).
//!
//! Flags: --apply (execute; dry-run otherwise), --with-burn (also burn+close
//!        non-empty accounts — IRREVERSIBLE; WSOL unwrap-closed, never burned),
//!        --tx-number N (cap # of txs, test with 1), --per-tx N (accounts/tx,
//!        default 5, max 24; capped to 10 with --with-burn), --pace-ms N (50).
//!
//! Run:
//!   cargo run --release --bin close_all_atas                            # dry-run (empty only)
//!   cargo run --release --bin close_all_atas -- --with-burn             # dry-run (incl. burns)
//!   cargo run --release --bin close_all_atas -- --apply --tx-number 1   # one test tx
//!   cargo run --release --bin close_all_atas -- --apply                 # close all empty
//!   cargo run --release --bin close_all_atas -- --with-burn --apply     # burn dust + close everything

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Context;
use solana_account_decoder_client_types::UiAccountData;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig,
    rpc_request::TokenAccountsFilter,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};

use central_service::swap_pump_fun::{token_program_pk, wsol_pk};

/// SPL Token-2022 program.
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const BATCH: usize = 5; // default CloseAccount ixs per tx (override: --per-tx)
const CU_PRICE: u64 = 100_000; // priority-fee µlamports/CU
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

struct TokenAcct {
    pubkey: Pubkey,
    mint: Pubkey,
    amount: u64,
    lamports: u64,
    program: Pubkey,
}

/// SPL Token `CloseAccount` (tag 9). Sweeps `account`'s lamports to
/// `destination`; `owner` signs. `token_prog` is Token or Token-2022.
fn ix_close(account: &Pubkey, destination: &Pubkey, owner: &Pubkey, token_prog: &Pubkey) -> Instruction {
    Instruction {
        program_id: *token_prog,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*owner, true),
        ],
        data: vec![9],
    }
}

/// SPL Token `Burn` (tag 8). IRREVERSIBLY destroys `amount` tokens from
/// `account` (decrementing the mint's supply); `owner` signs. Used only under
/// `--with-burn` on non-empty, non-WSOL accounts so the ATA can then be closed.
fn ix_burn(account: &Pubkey, mint: &Pubkey, owner: &Pubkey, amount: u64, token_prog: &Pubkey) -> Instruction {
    let mut data = Vec::with_capacity(9);
    data.push(8u8);
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: *token_prog,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new(*mint, false),
            AccountMeta::new_readonly(*owner, true),
        ],
        data,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let apply = args.iter().any(|a| a == "--apply");
    let arg_num = |name: &str| -> Option<usize> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse::<usize>().ok())
    };
    let tx_limit: Option<usize> = arg_num("--tx-number");
    // `--with-burn`: also process NON-empty accounts — burn the token balance
    // (SPL Burn) then close. WSOL is never burned (it's wrapped SOL); it's
    // unwrap-closed instead. Without the flag, only empty accounts are closed.
    let with_burn = args.iter().any(|a| a == "--with-burn");
    let mut per_tx: usize = arg_num("--per-tx").unwrap_or(BATCH).clamp(1, 24);
    // Burn adds the mint as an extra account key per entry, so a burn batch is
    // ~2× the keys — cap lower to stay under the 1232-byte packet limit.
    if with_burn {
        per_tx = per_tx.min(10);
    }
    let pace_ms: u64 = arg_num("--pace-ms").unwrap_or(50) as u64;

    // ONLY these two env vars — no Mongo.
    let wallet_keypair_b58 = std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let rpc_url = std::env::var("RPC_URL").context("RPC_URL not set")?;

    let wallet_kp = Keypair::from_base58_string(&wallet_keypair_b58);
    let wallet_pk = wallet_kp.pubkey();
    tracing::info!(wallet = %wallet_pk, apply, with_burn, ?tx_limit, per_tx, pace_ms, "close_all_atas starting");

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let wsol = wsol_pk();

    // ---- Enumerate wallet token accounts (Token + Token-2022) ----
    let token_prog = token_program_pk();
    let token2022 = Pubkey::from_str(TOKEN_2022_PROGRAM).unwrap();
    let mut accts: Vec<TokenAcct> = Vec::new();
    for prog in [token_prog, token2022] {
        let list = rpc
            .get_token_accounts_by_owner(&wallet_pk, TokenAccountsFilter::ProgramId(prog))
            .await
            .with_context(|| format!("get_token_accounts_by_owner prog={prog}"))?;
        for keyed in list {
            let pubkey = match Pubkey::from_str(&keyed.pubkey) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let lamports = keyed.account.lamports;
            let (mint, amount) = match &keyed.account.data {
                UiAccountData::Json(pa) => {
                    let info = &pa.parsed["info"];
                    let mint = info["mint"].as_str().and_then(|s| Pubkey::from_str(s).ok());
                    let amount = info["tokenAmount"]["amount"]
                        .as_str()
                        .and_then(|s| s.parse::<u64>().ok());
                    match (mint, amount) {
                        (Some(m), Some(a)) => (m, a),
                        _ => continue,
                    }
                }
                _ => continue,
            };
            accts.push(TokenAcct { pubkey, mint, amount, lamports, program: prog });
        }
    }
    tracing::info!(token_accounts = accts.len(), "enumerated wallet token accounts");

    // ---- Candidates ----
    // Default: empty accounts only. With --with-burn: EVERY account — non-empty
    // non-WSOL balances get burned first, then closed; WSOL is unwrap-closed.
    let mut candidates: Vec<&TokenAcct> = accts
        .iter()
        .filter(|a| with_burn || a.amount == 0)
        .collect();
    candidates.sort_by_key(|a| a.pubkey.to_bytes()); // stable batching/output

    let will_burn = |a: &TokenAcct| with_burn && a.amount > 0 && a.mint != wsol;
    let burn_n = candidates.iter().filter(|a| will_burn(a)).count();
    let held_kept = if with_burn { 0 } else { accts.iter().filter(|a| a.amount > 0).count() };
    let reclaimable: u64 = candidates.iter().map(|a| a.lamports).sum();
    let wsol_in_set = candidates.iter().any(|a| a.mint == wsol);

    tracing::info!(
        candidates = candidates.len(),
        burn = burn_n,
        held_skipped = held_kept,
        reclaimable_sol = reclaimable as f64 / LAMPORTS_PER_SOL,
        "scan complete"
    );
    for a in &candidates {
        let prog = if a.program == token2022 { "T22" } else { "TOK" };
        let action = if will_burn(a) {
            "BURN+CLOSE"
        } else if a.mint == wsol && a.amount > 0 {
            "UNWRAP+CLOSE"
        } else {
            "close     "
        };
        println!(
            "  {action} ata={} mint={} bal={} rent={:.6} SOL prog={prog}",
            a.pubkey,
            a.mint,
            a.amount,
            a.lamports as f64 / LAMPORTS_PER_SOL
        );
    }
    if burn_n > 0 {
        println!("\n⚠️  --with-burn: {burn_n} account(s) will have their token balance BURNED (irreversible) before close.");
    }
    if wsol_in_set {
        println!("⚠️  WSOL ATA is in the set and WILL be closed (unwrapped if it holds SOL) — breaks the bot's WSOL path until recreated.");
    }

    if candidates.is_empty() {
        tracing::info!("nothing to close");
        return Ok(());
    }
    if !apply {
        println!(
            "\nDRY RUN — {} ATA(s) ({} to burn), {:.6} SOL reclaimable. Re-run with --apply \
             (add `--tx-number 1` to send just one test tx).",
            candidates.len(),
            burn_n,
            reclaimable as f64 / LAMPORTS_PER_SOL
        );
        return Ok(());
    }

    // ---- Apply: fire-and-forget, `per_tx` closes per tx ----
    let full_batches = candidates.len().div_ceil(per_tx);
    let total_batches = tx_limit.map(|l| l.min(full_batches)).unwrap_or(full_batches);
    tracing::info!(
        per_tx,
        pace_ms,
        "--apply: sending {total_batches} of {full_batches} tx(s) (fire-and-forget)"
    );
    let send_cfg = RpcSendTransactionConfig { skip_preflight: true, ..Default::default() };
    let mut bh = rpc.get_latest_blockhash().await?;
    let mut bh_at = Instant::now();
    let mut sent: Vec<(Signature, usize, u64)> = Vec::new(); // (sig, n_accts, lamports)
    let mut send_err = 0usize;
    for (i, chunk) in candidates.chunks(per_tx).enumerate() {
        if i >= total_batches {
            break; // --tx-number cap
        }
        if bh_at.elapsed() > Duration::from_secs(30) {
            if let Ok(new) = rpc.get_latest_blockhash().await {
                bh = new;
                bh_at = Instant::now();
            }
        }
        // burn+close ≈ 12k CU/acct, close-only ≈ 6k.
        let per_acct_cu: u32 = if with_burn { 12_000 } else { 6_000 };
        let cu_limit = (chunk.len() as u32).saturating_mul(per_acct_cu).saturating_add(5_000);
        let mut ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE),
        ];
        for a in chunk {
            if will_burn(a) {
                ixs.push(ix_burn(&a.pubkey, &a.mint, &wallet_pk, a.amount, &a.program));
            }
            ixs.push(ix_close(&a.pubkey, &wallet_pk, &wallet_pk, &a.program));
        }
        let tx = Transaction::new_signed_with_payer(&ixs, Some(&wallet_pk), &[&wallet_kp], bh);
        let chunk_lamports: u64 = chunk.iter().map(|a| a.lamports).sum();
        match rpc.send_transaction_with_config(&tx, send_cfg).await {
            Ok(sig) => {
                sent.push((sig, chunk.len(), chunk_lamports));
                if (i + 1) % 25 == 0 {
                    tracing::info!("dispatched {}/{total_batches} tx", i + 1);
                }
            }
            Err(e) => {
                send_err += 1;
                tracing::warn!("[tx {}/{total_batches}] send failed: {e:#}", i + 1);
            }
        }
        tokio::time::sleep(Duration::from_millis(pace_ms)).await;
    }
    tracing::info!(sent = sent.len(), send_err, "all sends dispatched; verifying landings");

    // ---- Final: batched status check (getSignatureStatuses, 256/call) ----
    tokio::time::sleep(Duration::from_secs(5)).await;
    let sigs: Vec<Signature> = sent.iter().map(|(s, _, _)| *s).collect();
    let mut landed = 0usize;
    let mut reclaimed: u64 = 0;
    for (grp_i, group) in sigs.chunks(256).enumerate() {
        match rpc.get_signature_statuses(group).await {
            Ok(resp) => {
                for (j, st) in resp.value.iter().enumerate() {
                    let ok = st.as_ref().map(|s| s.err.is_none()).unwrap_or(false);
                    if ok {
                        landed += 1;
                        reclaimed += sent[grp_i * 256 + j].2;
                    }
                }
            }
            Err(e) => tracing::warn!("status check failed: {e:#}"),
        }
    }
    tracing::info!(
        tx_dispatched = sent.len(),
        tx_landed = landed,
        tx_send_err = send_err,
        reclaimed_sol = reclaimed as f64 / LAMPORTS_PER_SOL,
        "close_all_atas done — re-run to retry any that didn't land"
    );
    Ok(())
}
