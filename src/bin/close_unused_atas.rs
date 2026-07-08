//! close_unused_atas — reclaim SOL rent from ORPHAN token ATAs.
//!
//! Enumerates every SPL token account (Token + Token-2022) owned by the
//! wallet and closes the ones that are BOTH:
//!   (a) empty (`amount == 0`), and
//!   (b) whose mint appears in NO Mongo pool doc — i.e. true orphans that
//!       no bot will ever trade.
//! Rent is swept back to the wallet. Closes are batched 5 per tx.
//!
//! SAFETY — why orphans only:
//!   The bot's buy tx does NOT create the wallet token ATA (layout is
//!   nonce→cu_limit→cu_price→loaded_data_size→swap; ATAs are assumed to
//!   pre-exist), and central only creates ATAs on discovery / for pending
//!   pools — it does NOT recreate them for already-`confirmed` pools. So
//!   closing an ATA whose mint is still referenced by a pool doc would
//!   permanently break that pool's buys. We therefore KEEP every mint that
//!   appears in ANY pool doc (via `Repo::load_all`), plus WSOL, and only
//!   close mints that are in no pool doc at all. Held positions (amount>0)
//!   are skipped automatically (CloseAccount requires a zero balance).
//!
//! DRY RUN by default — lists candidates + total reclaimable SOL and closes
//! nothing. Pass `--apply` to actually close.
//!
//! Env: WALLET_KEYPAIR, MONGO_URI, MONGO_DB, RPC_URL (falls back to Helius).
//!
//! Run:
//!   cargo run --release --bin close_unused_atas                       # dry-run
//!   cargo run --release --bin close_unused_atas -- --apply            # close all
//!   cargo run --release --bin close_unused_atas -- --apply --tx-number 1  # one test tx (≤5 accts)

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use solana_account_decoder_client_types::UiAccountData;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_request::TokenAccountsFilter};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};

use central_service::{
    mongo::Repo,
    pool::PoolAccounts,
    swap_pump_fun::{token_program_pk, wsol_pk},
};

/// SPL Token-2022 program. Not defined in `swap_pump_fun` (the bot's
/// `owner_program` field carries it per-pool), so declared here.
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=75715a51-2511-436d-ad3a-1d8c76208072";
const BATCH: usize = 5; // CloseAccount ixs per tx (per request)
const CU_LIMIT: u32 = 60_000; // 5 closes ≈ ~15k CU; ample headroom
const CU_PRICE: u64 = 100_000;
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

struct TokenAcct {
    pubkey: Pubkey,
    mint: Pubkey,
    amount: u64,
    lamports: u64,
    program: Pubkey,
}

/// All mint pubkeys (both sides) referenced by a pool doc. We keep every
/// one — the wallet's tradeable ATA is one of them, and over-keeping the
/// WSOL side is harmless.
fn mints_of(accounts: &PoolAccounts) -> Vec<&str> {
    match accounts {
        PoolAccounts::PumpFun(p) => vec![p.base_mint.as_str(), p.quote_mint.as_str()],
        PoolAccounts::RaydiumAmm(a) => {
            vec![a.coin_vault_mint.as_str(), a.pc_vault_mint.as_str()]
        }
        PoolAccounts::RaydiumCpmm(c) => {
            vec![c.token_0_mint.as_str(), c.token_1_mint.as_str()]
        }
    }
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
    // `--tx-number N` caps how many transactions the --apply pass sends
    // (each tx still closes up to 5 accounts). Handy for a single-tx test
    // run (`--apply --tx-number 1`). Absent = no cap (send all batches).
    let tx_limit: Option<usize> = args
        .iter()
        .position(|a| a == "--tx-number")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<usize>().ok());

    let wallet_keypair_b58 = std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| HELIUS_RPC.to_string());

    let wallet_kp = Keypair::from_base58_string(&wallet_keypair_b58);
    let wallet_pk = wallet_kp.pubkey();
    tracing::info!(wallet = %wallet_pk, apply, ?tx_limit, "close_unused_atas starting");

    let repo = Repo::connect(&mongo_uri, &mongo_db).await?;
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // ---- Keep set: WSOL + every mint in any pool doc ----
    let mut keep: HashSet<Pubkey> = HashSet::new();
    keep.insert(wsol_pk());
    let docs = repo.load_all().await?;
    let mut bad_mint = 0usize;
    for d in &docs {
        for m in mints_of(&d.accounts) {
            match Pubkey::from_str(m) {
                Ok(pk) => {
                    keep.insert(pk);
                }
                Err(_) => bad_mint += 1,
            }
        }
    }
    tracing::info!(
        pool_docs = docs.len(),
        keep_mints = keep.len(),
        bad_mint,
        "keep-set built"
    );

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

    // ---- Candidates: empty AND orphan (mint in no pool doc) ----
    let mut candidates: Vec<&TokenAcct> = accts
        .iter()
        .filter(|a| a.amount == 0 && !keep.contains(&a.mint))
        .collect();
    candidates.sort_by_key(|a| a.pubkey.to_bytes()); // stable batching/output

    let held = accts.iter().filter(|a| a.amount > 0).count();
    let kept_empty = accts
        .iter()
        .filter(|a| a.amount == 0 && keep.contains(&a.mint))
        .count();
    let reclaimable: u64 = candidates.iter().map(|a| a.lamports).sum();

    tracing::info!(
        candidates = candidates.len(),
        held_skipped = held,
        empty_but_in_pool_docs = kept_empty,
        reclaimable_sol = reclaimable as f64 / LAMPORTS_PER_SOL,
        "scan complete"
    );
    for a in &candidates {
        let prog = if a.program == token2022 { "T22" } else { "TOK" };
        println!(
            "  close ata={} mint={} rent={:.6} SOL prog={prog}",
            a.pubkey,
            a.mint,
            a.lamports as f64 / LAMPORTS_PER_SOL
        );
    }

    if candidates.is_empty() {
        tracing::info!("no orphan ATAs to close");
        return Ok(());
    }
    if !apply {
        println!(
            "\nDRY RUN — {} orphan ATA(s), {:.6} SOL reclaimable. Re-run with --apply to close \
             (add `--tx-number 1` to send just one test tx).",
            candidates.len(),
            reclaimable as f64 / LAMPORTS_PER_SOL
        );
        return Ok(());
    }

    // ---- Apply: BATCH closes per tx, capped by --tx-number ----
    let full_batches = candidates.len().div_ceil(BATCH);
    let total_batches = tx_limit.map(|l| l.min(full_batches)).unwrap_or(full_batches);
    if let Some(l) = tx_limit {
        tracing::info!(
            "--tx-number {l}: sending {total_batches} of {full_batches} batch(es) this run"
        );
    }
    let mut closed = 0usize;
    let mut reclaimed: u64 = 0;
    let mut failed = 0usize;
    for (i, chunk) in candidates.chunks(BATCH).enumerate() {
        if i >= total_batches {
            break; // --tx-number cap reached
        }
        let mut ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT),
            ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE),
        ];
        for a in chunk {
            ixs.push(ix_close(&a.pubkey, &wallet_pk, &wallet_pk, &a.program));
        }
        let bh = match rpc.get_latest_blockhash().await {
            Ok(bh) => bh,
            Err(e) => {
                tracing::error!("[batch {}/{total_batches}] blockhash failed: {e:#}", i + 1);
                failed += chunk.len();
                continue;
            }
        };
        let tx = Transaction::new_signed_with_payer(&ixs, Some(&wallet_pk), &[&wallet_kp], bh);
        let chunk_lamports: u64 = chunk.iter().map(|a| a.lamports).sum();
        match rpc.send_and_confirm_transaction(&tx).await {
            Ok(sig) => {
                closed += chunk.len();
                reclaimed += chunk_lamports;
                tracing::info!(
                    "[batch {}/{total_batches}] closed {} ata(s) +{:.6} SOL sig={sig}",
                    i + 1,
                    chunk.len(),
                    chunk_lamports as f64 / LAMPORTS_PER_SOL
                );
            }
            Err(e) => {
                failed += chunk.len();
                tracing::warn!("[batch {}/{total_batches}] close failed: {e:#}", i + 1);
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    tracing::info!(
        closed,
        failed,
        reclaimed_sol = reclaimed as f64 / LAMPORTS_PER_SOL,
        "close_unused_atas done"
    );
    Ok(())
}
