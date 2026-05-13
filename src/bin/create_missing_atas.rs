//! Batch ATA-creation companion to `measure_cu`.
//!
//! Walks the same Mongo filter (`pools_for_cu_measurement`), pre-checks
//! each pool's wallet base-mint ATA on-chain, and fires
//! `create_idempotent_associated_token_account` for the missing ones.
//! After this binary runs, re-running `measure_cu` should pick up every
//! previously-skipped pool.
//!
//! Tx layout per pool:
//!   ix[0] set_compute_unit_limit(50_000)
//!   ix[1] set_compute_unit_price(100_000)
//!   ix[2] create_idempotent_associated_token_account
//!
//! No nonce — uses a regular blockhash. ATA-create is a one-off
//! housekeeping op; doesn't need to share the bot's nonce lane.
//!
//! Run:
//!   `cd ~/Work/central-service && cargo run --release --bin create_missing_atas`

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_transaction_status_client_types::TransactionConfirmationStatus;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use central_service::{
    config::Config,
    mongo::Repo,
    pool::PoolAccounts,
    swap_pump_fun::{find_ata, token_program_pk},
};

const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=e57668cb-43f4-4d35-9d83-fbb9c1d71ad2";
const CU_LIMIT: u32 = 50_000;
const CU_PRICE: u64 = 100_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env()?;
    let wallet_kp = Keypair::from_base58_string(&cfg.wallet_keypair_b58);
    let wallet_pk = wallet_kp.pubkey();
    tracing::info!(wallet = %wallet_pk, "starting create_missing_atas");

    let repo = Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?;
    let rpc =
        RpcClient::new_with_commitment(HELIUS_RPC.to_string(), CommitmentConfig::confirmed());

    let pools = repo.pools_for_cu_measurement().await?;
    tracing::info!(count = pools.len(), "candidate pools (same filter as measure_cu)");

    let mut already_exists = 0usize;
    let mut created = 0usize;
    let mut failed = 0usize;
    let _ = token_program_pk(); // pre-load lazy if needed elsewhere

    for (idx, pool_doc) in pools.iter().enumerate() {
        let pump = match &pool_doc.accounts {
            PoolAccounts::PumpFun(p) => p,
            _ => continue,
        };
        let base_mint = match Pubkey::from_str(&pump.base_mint) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!(pool = %pool_doc.pool, err = %e, "skip: bad base_mint");
                failed += 1;
                continue;
            }
        };
        let owner_program = match Pubkey::from_str(&pump.owner_program) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!(pool = %pool_doc.pool, err = %e, "skip: bad owner_program");
                failed += 1;
                continue;
            }
        };

        let ata = find_ata(&wallet_pk, &base_mint, &owner_program);
        let prefix = format!("[{}/{}] pool={}", idx + 1, pools.len(), pool_doc.pool);

        // Pre-check: ATA already exists on chain? Skip without firing.
        if rpc.get_account(&ata).await.is_ok() {
            tracing::debug!("{prefix} ata={ata} already exists, skipping");
            already_exists += 1;
            continue;
        }

        match create_ata(&rpc, &wallet_kp, wallet_pk, &base_mint, &owner_program).await {
            Ok(sig) => {
                tracing::info!("{prefix} created ata={ata} sig={sig}");
                created += 1;
            }
            Err(e) => {
                tracing::warn!("{prefix} ata={ata} failed: {e:#}");
                failed += 1;
            }
        }

        // Light pacing — sequential, don't hammer Helius.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tracing::info!(
        total = pools.len(),
        already_exists,
        created,
        failed,
        "create_missing_atas done"
    );
    Ok(())
}

async fn create_ata(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    wallet_pk: Pubkey,
    base_mint: &Pubkey,
    token_program: &Pubkey,
) -> anyhow::Result<solana_sdk::signature::Signature> {
    let create_ix = create_associated_token_account_idempotent(
        &wallet_pk,
        &wallet_pk,
        base_mint,
        token_program,
    );
    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT);
    let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);

    let blockhash = rpc.get_latest_blockhash().await.context("get_latest_blockhash")?;
    let mut tx = Transaction::new_with_payer(
        &[cu_limit_ix, cu_price_ix, create_ix],
        Some(&wallet_pk),
    );
    tx.sign(&[wallet_kp], blockhash);

    let sig = rpc.send_transaction(&tx).await.context("send_transaction")?;

    // Poll briefly. ATA create is fast — usually 1-2 slots.
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    loop {
        let statuses = rpc.get_signature_statuses(&[sig]).await?;
        if let Some(Some(status)) = statuses.value.into_iter().next() {
            if let Some(conf) = &status.confirmation_status {
                if matches!(
                    conf,
                    TransactionConfirmationStatus::Confirmed
                        | TransactionConfirmationStatus::Finalized,
                ) {
                    if let Some(err) = status.err {
                        return Err(anyhow!("tx failed on-chain: {err:?}"));
                    }
                    return Ok(sig);
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("confirm timeout for sig={sig}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
