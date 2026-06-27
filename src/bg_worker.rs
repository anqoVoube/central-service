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

use anyhow::{anyhow, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status_client_types::TransactionConfirmationStatus;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use tokio::sync::{broadcast, Semaphore};

use crate::{
    ata, measure,
    mongo::Repo,
    pool::PoolAccounts,
    swap_pump_fun::find_ata,
    ws::ServerMsg,
    zeroslot,
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

/// Cap on concurrent ATA-create txs spawned by the divergence-recovery
/// pass. Without this, a mass-divergence event (wallet rotation, mongo
/// restore, RPC blip mis-classified as missing) would fan out N parallel
/// zeroslot sends + signature polls. The cap also pressures zeroslot's
/// per-IP rate limit gently. 4 keeps fleet-wide spend bounded.
const RECOVER_MAX_INFLIGHT: usize = 4;

/// CU + tip params for the recovery-side ATA-create tx. Mirrors
/// `bin/create_missing_atas.rs`. The tip is required for zeroslot's
/// priority lane.
const RECOVER_CU_LIMIT: u32 = 50_000;
const RECOVER_CU_PRICE: u64 = 100_000;

/// Per-pool ATA-create attempts cap inside the recovery path. Once a
/// row has been touched this many times by recovery, stop retrying
/// even if `get_account` still says missing — protects against an
/// edge case where the same row keeps "going divergent" tick after
/// tick (some unidentified upstream bug). 6 = 1 RPC + ~5 ticks of
/// recovery effort before we give up and require operator intervention.
const RECOVER_ATTEMPTS_CAP: i32 = 6;

/// True iff the error from `RpcClient::get_account` actually means the
/// account is missing on chain (vs. a transient RPC failure — timeout,
/// rate-limit, network blip). solana-rpc-client v2 returns a `ForUser`
/// error whose message starts with `"AccountNotFound: pubkey="` for
/// the missing case. Every other variant is treated as transient and
/// skipped — recovery is REAL MONEY (zeroslot tip + ATA rent) so we
/// must not amplify RPC degradation into a fleet-wide fire.
fn is_account_not_found(err: &solana_client::client_error::ClientError) -> bool {
    // The string form is the documented contract for v2.x. The kind
    // is `RpcError::ForUser("AccountNotFound: pubkey=...")`. Match on
    // the substring so future client-version reformats still work.
    err.to_string().contains("AccountNotFound")
}

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
    // Both passes run unconditionally now. The CU-probe pass used to
    // be opt-in (ENABLE_CU_PROBES env var) because `measure_pool_cu`
    // signed with the bot's `BUY_NONCE` and advancing it tripped the
    // bot's geyser nonce-sub → fleet-wide `rebuild_all_prebuilds` →
    // dumps during the rebuild window returned `Stale`. Since
    // `measure_pool_cu` was rewritten to sign with
    // `get_latest_blockhash()` instead (see measure.rs module doc),
    // there's no bot-side collision — the gate is gone.
    loop {
        ticker.tick().await;
        // Pass 1: ata_status=pending rows. These are rows that
        // discovery flagged as needing an ATA but the create hasn't
        // landed yet (or exhausted its 3-attempt budget). Filter
        // includes `ata_attempts < ATA_RETRY_CAP` so permanently-
        // broken rows don't burn budget every tick.
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
        // Pass 2: ata_status=confirmed rows where the on-chain ATA
        // is actually MISSING (Mongo↔chain divergence). The standalone
        // `bin/create_missing_atas` walks the same filter. Origins of
        // the divergence: wallet rotated, ATA closed on chain, or an
        // earlier discovery path optimistically marked confirmed
        // without an on-chain check. Without this pass, those rows
        // get filtered out of pass 1 (they're confirmed, not pending)
        // AND the CU measurement skips them as `SkipAtaMissing`. They
        // sit indefinitely until the operator runs the standalone bin.
        if let Err(e) = recover_divergent_atas(
            Arc::clone(&repo),
            rpc_url.clone(),
            Arc::clone(&wallet_kp),
            broadcast_tx.clone(),
        )
        .await
        {
            tracing::error!("[bg-worker] ata divergence pass failed: {e:#}");
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

/// One full pass over `ata_status=confirmed` PumpFun WSOL pools to catch
/// Mongo↔chain divergence: rows that Mongo says are confirmed but
/// whose ATA doesn't actually exist on chain. Mirrors the standalone
/// `bin/create_missing_atas` flow with stricter safety because the
/// background path runs unattended.
///
/// SAFETY MODEL (adversarial review 2026-06-27):
///   1. LIVENESS PROBE before iterating. If the RPC's read of the
///      wallet's own account fails, the whole pass aborts. Prevents a
///      degraded Helius from cascading thousands of `Err(timeout)` →
///      false "missing" classifications → real-money ATA-create fan-out.
///   2. PRECISE ERROR CLASSIFICATION. Only `RpcError::ForUser` with
///      `"AccountNotFound"` payload counts as missing. Every other
///      `get_account` error (timeout, 429, TLS, decode) → log and skip.
///   3. CREATE-ONLY HELPER. We do NOT call `ata::create` here. That
///      function bumps `ata_attempts` per attempt (mutates a confirmed
///      row's bookkeeping), runs `measure_one` which fires a real BUY
///      with `SWAP_IN_LAMPORTS=121_335` plus a 100k-lamport zeroslot
///      tip, clobbers `compute_unit_limit`, and re-broadcasts NewPool
///      to every bot. Recovery only needs ONE outcome: the ATA exists
///      on chain. `recreate_ata_only` performs that single 4-ix tx.
///   4. CONCURRENCY CAP. A `Semaphore::new(RECOVER_MAX_INFLIGHT)` caps
///      parallel ATA creates regardless of how many candidates the
///      query returns. Acquire-before-spawn keeps spend bounded under
///      a mass-divergence event (wallet rotation, mongo restore).
///   5. PER-POOL ATTEMPTS CAP via `ata_attempts < RECOVER_ATTEMPTS_CAP`.
///      The 200ms pacing between probes matches `bin/create_missing_atas`.
async fn recover_divergent_atas(
    repo: Arc<Repo>,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
    _broadcast_tx: broadcast::Sender<ServerMsg>,
) -> anyhow::Result<()> {
    let candidates = repo.pools_for_remeasurement().await?;
    if candidates.is_empty() {
        tracing::debug!("[bg-worker] ata-recover: no confirmed pools to check");
        return Ok(());
    }
    let rpc = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let wallet_pk = wallet_kp.pubkey();
    // (1) LIVENESS PROBE. If the RPC can't even fetch the wallet's
    // own account, abort the whole pass. Prevents a degraded Helius
    // from being mis-read as "every ATA is missing" → fleet-wide
    // ATA-create storm.
    match rpc.get_account(&wallet_pk).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(
                "[bg-worker] ata-recover: RPC liveness probe failed ({e}) — skipping pass; will retry next tick"
            );
            return Ok(());
        }
    }
    let semaphore = Arc::new(Semaphore::new(RECOVER_MAX_INFLIGHT));
    let mut checked = 0usize;
    let mut already_exists = 0usize;
    let mut missing_recovered = 0usize;
    let mut transient_skipped = 0usize;
    let mut attempts_exhausted = 0usize;
    let mut errors = 0usize;
    tracing::info!(
        "[bg-worker] ata-recover: checking {} confirmed pool(s) for ATA divergence",
        candidates.len()
    );
    for doc in candidates {
        let pump = match &doc.accounts {
            PoolAccounts::PumpFun(p) => p,
            _ => continue,
        };
        let base_mint = match Pubkey::from_str(&pump.base_mint) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!(
                    "[bg-worker] ata-recover: pool={} bad base_mint={}: {e}",
                    doc.pool, pump.base_mint
                );
                errors += 1;
                continue;
            }
        };
        let owner_program = match Pubkey::from_str(&pump.owner_program) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!(
                    "[bg-worker] ata-recover: pool={} bad owner_program={}: {e}",
                    doc.pool, pump.owner_program
                );
                errors += 1;
                continue;
            }
        };
        let ata = find_ata(&wallet_pk, &base_mint, &owner_program);
        checked += 1;
        match rpc.get_account(&ata).await {
            // (2) ATA exists → no divergence.
            Ok(_) => {
                already_exists += 1;
            }
            // ATA truly absent on chain.
            Err(e) if is_account_not_found(&e) => {
                // (5) Per-pool attempts cap. `doc.ata_attempts` was
                // bumped by earlier ata::create / recovery passes. If
                // we've already tried this many times, hands off until
                // the operator investigates.
                if doc.ata_attempts >= RECOVER_ATTEMPTS_CAP {
                    tracing::warn!(
                        "[bg-worker] ata-recover: pool={} ATA={} missing AND attempts={} >= cap={} — refusing further retries (operator: investigate)",
                        doc.pool, ata, doc.ata_attempts, RECOVER_ATTEMPTS_CAP
                    );
                    attempts_exhausted += 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                tracing::warn!(
                    "[bg-worker] ata-recover: pool={} ATA={} marked confirmed but missing on chain — recreating (attempt {}/{})",
                    doc.pool, ata, doc.ata_attempts + 1, RECOVER_ATTEMPTS_CAP
                );
                missing_recovered += 1;
                // (4) Acquire a permit BEFORE spawn. If all
                // RECOVER_MAX_INFLIGHT permits are held, this awaits
                // — natural backpressure. The spawned task releases
                // the permit on completion (via _permit drop).
                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(p) => p,
                    Err(_closed) => {
                        // Semaphore closed — runtime shutting down.
                        tracing::warn!(
                            "[bg-worker] ata-recover: semaphore closed mid-pass; aborting"
                        );
                        break;
                    }
                };
                let pool_str = doc.pool.clone();
                let rpc_url_c = rpc_url.clone();
                let wallet_kp_c = Arc::clone(&wallet_kp);
                let repo_c = Arc::clone(&repo);
                tokio::spawn(async move {
                    let _permit = permit; // released on drop
                    if let Err(e) = recreate_ata_only(
                        &pool_str,
                        base_mint,
                        owner_program,
                        wallet_kp_c,
                        rpc_url_c,
                        Arc::clone(&repo_c),
                    )
                    .await
                    {
                        tracing::warn!(
                            "[bg-worker] ata-recover: pool={} recreate failed: {e:#}",
                            pool_str
                        );
                    } else {
                        tracing::info!(
                            "[bg-worker] ata-recover: pool={} recreate succeeded",
                            pool_str
                        );
                    }
                });
            }
            // (2) Transient RPC error — DO NOT classify as missing.
            Err(e) => {
                tracing::warn!(
                    "[bg-worker] ata-recover: pool={} ATA={} transient get_account err ({e}) — skipping (no recreate)",
                    doc.pool, ata
                );
                transient_skipped += 1;
            }
        }
        // 200ms pacing matches the standalone bin so we don't hammer
        // Helius's free tier when a large divergence backlog appears.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tracing::info!(
        "[bg-worker] ata-recover pass done: checked={} already_exists={} missing_recovered={} transient_skipped={} attempts_exhausted={} errors={}",
        checked, already_exists, missing_recovered, transient_skipped, attempts_exhausted, errors
    );
    Ok(())
}

/// Minimal ATA-create flow for the divergence-recovery path. Does ONLY
/// the on-chain `create_associated_token_account_idempotent` tx + confirm
/// poll. NO measure_pool_cu probe, NO NewPool broadcast, NO
/// mark_ata_confirmed (the row already is confirmed), NO compute_unit_limit
/// overwrite. The single Mongo side-effect is one `bump_ata_attempts`
/// per call (regardless of success) so the per-pool cap in
/// `recover_divergent_atas` can stop a runaway recovery loop.
///
/// Tx layout mirrors `bin/create_missing_atas::create_ata`:
///   ix[0] set_compute_unit_limit
///   ix[1] set_compute_unit_price
///   ix[2] create_associated_token_account_idempotent
///   ix[3] system::transfer (zeroslot tip)
///
/// One attempt. The caller's cap (`RECOVER_ATTEMPTS_CAP`) provides
/// retry semantics across ticks. We deliberately don't loop here so
/// each tick contributes at most one tx of cost per divergent pool.
async fn recreate_ata_only(
    pool: &str,
    base_mint: Pubkey,
    token_program: Pubkey,
    wallet_kp: Arc<Keypair>,
    rpc_url: String,
    repo: Arc<Repo>,
) -> anyhow::Result<Signature> {
    // Bump attempts FIRST (mirrors ata::create's accounting): even if
    // the send fails downstream, the attempt is counted toward the
    // cap. Errors here don't abort — we still try the send.
    if let Err(e) = repo.bump_ata_attempts(pool).await {
        tracing::warn!(
            "[bg-worker] ata-recover: pool={pool} bump_ata_attempts failed: {e:#}"
        );
    }
    let rpc = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let wallet_pk = wallet_kp.pubkey();

    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(RECOVER_CU_LIMIT);
    let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(RECOVER_CU_PRICE);
    let create_ix = create_associated_token_account_idempotent(
        &wallet_pk,
        &wallet_pk,
        &base_mint,
        &token_program,
    );
    let zs_tip_pk: Pubkey = zeroslot::ZEROSLOT_TIP_ACCOUNT
        .parse()
        .context("parse zeroslot tip account")?;
    let tip_ix = system_instruction::transfer(&wallet_pk, &zs_tip_pk, zeroslot::TIP_LAMPORTS);

    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;
    let mut tx = Transaction::new_with_payer(
        &[cu_limit_ix, cu_price_ix, create_ix, tip_ix],
        Some(&wallet_pk),
    );
    tx.sign(&[wallet_kp.as_ref()], blockhash);

    let http = zeroslot::build_http_client().context("build zeroslot http client")?;
    let sig = zeroslot::send_transaction(&http, &tx)
        .await
        .context("zeroslot send_transaction")?;

    // Poll confirm. ATA creates are fast (1-2 slots); 30s timeout
    // matches the standalone bin.
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
                        return Err(anyhow!("ata-recover tx failed on-chain: {err:?}"));
                    }
                    return Ok(sig);
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("ata-recover confirm timeout for sig={sig}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// One full pass over pools that have `ata_status=confirmed` but no
/// persisted `compute_unit_limit`. Sequential by design — 200ms pacing
/// between probes mirrors `bin/measure_cu.rs` so we don't hammer the
/// RPC any harder than the manual tool. `measure::measure_pool_cu`
/// signs with `get_latest_blockhash()` so no nonce collision; the
/// sequential walk is a politeness/cost-control decision, not a
/// correctness one.
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
