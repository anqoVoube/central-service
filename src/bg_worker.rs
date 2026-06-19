//! Periodic background worker that:
//!   1. Retries ATA creation for pools still stuck in `ata_status=pending`
//!      (subject to `ATA_RETRY_CAP` total attempts per pool — once
//!      `ata_attempts >= cap`, the pool is permanently skipped to avoid
//!      hammering the chain on a structurally-broken row).
//!   2. Measures CU on any pool with `ata_status=confirmed` but missing
//!      `compute_unit_limit`. Sequential pacing matches
//!      `bin/measure_cu.rs` (200ms between pools).
//!
//! Runs every `interval` (default 10 min from `main.rs`). Errors at any
//! level are logged and the loop continues — never bails. Implementation
//! mirrors `poll::run`'s shape exactly so failure modes are uniform.

use std::{str::FromStr, sync::Arc, time::Duration};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair};
use tokio::sync::broadcast;

use crate::{
    ata, measure,
    mongo::Repo,
    pool::PoolAccounts,
    ws::ServerMsg,
};

/// Hard ceiling on per-pool ATA attempts before this worker stops
/// retrying. `ata::create` already runs 3 attempts per call, so cap=12
/// gives ~4 worker passes before a pool is permanently skipped.
/// Increase if persistent RPC outages are eating the budget for
/// otherwise-fine pools.
const ATA_RETRY_CAP: i32 = 12;

/// Sequential pacing between CU probe txs in a single pass. Matches
/// `bin/measure_cu.rs:286` so we don't hammer Helius any harder than
/// the manual tool already does.
const CU_PROBE_PACING_MS: u64 = 200;

pub async fn run(
    repo: Arc<Repo>,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
    broadcast_tx: broadcast::Sender<ServerMsg>,
    interval: Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    // Skip the immediate first tick — main.rs already does a one-shot
    // replay on boot. Letting the first worker tick wait `interval`
    // avoids double-firing on every restart.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = retry_pending_atas(
            Arc::clone(&repo),
            rpc_url.clone(),
            Arc::clone(&wallet_kp),
            broadcast_tx.clone(),
        )
        .await
        {
            tracing::error!("[bg-worker] ata retry pass failed: {e:#}");
        }
        if let Err(e) = measure_missing_cu(
            Arc::clone(&repo),
            rpc_url.clone(),
            Arc::clone(&wallet_kp),
        )
        .await
        {
            tracing::error!("[bg-worker] cu measure pass failed: {e:#}");
        }
    }
}

/// One full pass over all rows with `ata_status=pending` and
/// `ata_attempts < ATA_RETRY_CAP`. Spawns one `ata::create` task per
/// pool — `ata::create` itself owns the 3-attempt retry loop, so we
/// don't add our own. Per-pool failures are logged inside `ata::create`.
async fn retry_pending_atas(
    repo: Arc<Repo>,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
    broadcast_tx: broadcast::Sender<ServerMsg>,
) -> anyhow::Result<()> {
    let pending = repo.pools_pending_for_retry(ATA_RETRY_CAP).await?;
    if pending.is_empty() {
        tracing::debug!("[bg-worker] ata: no pending pools to retry");
        return Ok(());
    }
    tracing::info!("[bg-worker] ata: retrying {} pending pool(s)", pending.len());
    for doc in pending {
        // Reconstruct the args ata::create needs from the PoolDoc.
        // Only PumpFun pools carry a meaningful `base_mint` +
        // `owner_program` for the ATA we want — Raydium ATAs are
        // pre-derived at discovery and don't need retry.
        let (base_mint, owner_program) = match &doc.accounts {
            PoolAccounts::PumpFun(p) => {
                let bm = match Pubkey::from_str(&p.base_mint) {
                    Ok(pk) => pk,
                    Err(e) => {
                        tracing::warn!(
                            "[bg-worker] ata: pool={} bad base_mint={}: {e}",
                            doc.pool, p.base_mint
                        );
                        continue;
                    }
                };
                let op = match Pubkey::from_str(&p.owner_program) {
                    Ok(pk) => pk,
                    Err(e) => {
                        tracing::warn!(
                            "[bg-worker] ata: pool={} bad owner_program={}: {e}",
                            doc.pool, p.owner_program
                        );
                        continue;
                    }
                };
                (bm, op)
            }
            _ => {
                // Non-PumpFun rows shouldn't be pending; skip silently.
                continue;
            }
        };
        // ata::create is fire-and-forget; it logs internally and
        // mutates Mongo on success. Spawn so a single hung RPC doesn't
        // serialize the whole pass.
        let pool_str = doc.pool.clone();
        let pool_for_log = pool_str.clone();
        let rpc_url = rpc_url.clone();
        let wallet_kp = Arc::clone(&wallet_kp);
        let repo = Arc::clone(&repo);
        let broadcast = broadcast_tx.clone();
        let doc_clone = doc.clone();
        tokio::spawn(async move {
            tracing::info!("[bg-worker] ata: retrying pool={pool_for_log}");
            ata::create(
                pool_str,
                base_mint,
                owner_program,
                wallet_kp,
                rpc_url,
                repo,
                broadcast,
                doc_clone,
            )
            .await;
        });
    }
    Ok(())
}

/// One full pass over pools that have `ata_status=confirmed` but no
/// persisted `compute_unit_limit`. Sequential — `measure::measure_pool_cu`
/// signs with `BUY_NONCE` so concurrent measurements would conflict.
/// 200ms pacing between probes.
async fn measure_missing_cu(
    repo: Arc<Repo>,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
) -> anyhow::Result<()> {
    let to_measure = repo.pools_for_cu_measurement().await?;
    if to_measure.is_empty() {
        tracing::debug!("[bg-worker] cu: no pools missing CU");
        return Ok(());
    }
    tracing::info!(
        "[bg-worker] cu: measuring {} pool(s)",
        to_measure.len()
    );
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let mut measured = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for (idx, doc) in to_measure.iter().enumerate() {
        let pool_for_log = doc.pool.clone();
        match measure::measure_pool_cu(&rpc, &wallet_kp, doc).await {
            Ok(measure::MeasureOutcome::Ok(cu_raw)) => {
                let cu_signed = cu_raw.min(i32::MAX as u32) as i32;
                if let Err(e) = repo.update_cu_limit(&pool_for_log, cu_signed).await {
                    tracing::warn!(
                        "[bg-worker] cu: pool={pool_for_log} update_cu_limit failed: {e:#}"
                    );
                    failed += 1;
                } else {
                    tracing::info!(
                        "[bg-worker] cu: [{}/{}] pool={pool_for_log} cu_raw={cu_raw}",
                        idx + 1,
                        to_measure.len()
                    );
                    measured += 1;
                }
            }
            Ok(measure::MeasureOutcome::SkipAtaMissing) => {
                tracing::debug!(
                    "[bg-worker] cu: pool={pool_for_log} skip (wallet base-mint ATA not initialised)"
                );
                skipped += 1;
            }
            Ok(measure::MeasureOutcome::SkipNonWsolQuote) => {
                tracing::debug!(
                    "[bg-worker] cu: pool={pool_for_log} skip (non-WSOL quote mint)"
                );
                skipped += 1;
            }
            Err(e) => {
                tracing::warn!("[bg-worker] cu: pool={pool_for_log} failed: {e:#}");
                failed += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(CU_PROBE_PACING_MS)).await;
    }
    tracing::info!(
        "[bg-worker] cu pass done: measured={} skipped_ata_missing={} failed={}",
        measured, skipped, failed
    );
    Ok(())
}
