//! Single-pool CU measurement helper. Shared by:
//!   - `bin/measure_cu.rs` (batch sweep over all unmeasured pools)
//!   - `ata::create` (auto-measure each new pool right after its ATA lands,
//!     before `NewPool` is broadcast to bots)
//!   - `bg_worker::measure_missing_cu` (periodic 10-min sweep)
//!
//! Tx layout (5 ixs):
//!   ix[0] system::transfer(TIP_LAMPORTS → Zeroslot tip vault)
//!   ix[1] set_compute_unit_limit(CU_LIMIT_CEILING)
//!   ix[2] set_compute_unit_price(CU_PRICE)
//!   ix[3] set_loaded_accounts_data_size_limit(LOADED_DATA_SIZE_LIMIT)
//!   ix[4] pump_fun_buy_exact_in(sol_in = SWAP_IN_LAMPORTS)
//!
//! Signed with `get_latest_blockhash()` — NOT the bot's `BUY_NONCE`. The
//! prior nonce-based signing advanced BUY_NONCE on every probe, which
//! tripped the bot's geyser nonce-sub and triggered a fleet-wide
//! `rebuild_all_prebuilds`, dropping any incoming dump during the
//! rebuild window as `Stale`. The blockhash variant has zero
//! interaction with the bot, so bg_worker and ata::create can both
//! invoke it freely (no ENABLE_CU_PROBES gate needed).
//!
//! CU skew vs. bot's production buy: production buys carry a
//! 6th ix (`advance_nonce_account`) costing ~3,000-5,000 CU. The
//! measured value is therefore ~3-5k LOW relative to what the bot
//! actually consumes. The bot's `BUY_CU_MARGIN_PCT` (≥5%) absorbs
//! this delta; if you observe `ExceededMaxCUs` aborts on pools whose
//! CU was bg-worker-measured, bump the bot-side margin.
//!
//! Tip-at-slot-1 (vs the previous tip-at-end) drops measured CU by a
//! few thousand units. Order must match the bot's production layout in
//! `services::build_tx`, otherwise stored values don't reflect what
//! the bot actually consumes.
//!
//! Returns the raw `compute_units_consumed` from `meta`. The bot applies
//! its `BUY_CU_MARGIN_PCT` at fire time — single source of truth in the bot.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status_client_types::{
    TransactionConfirmationStatus, UiTransactionEncoding,
};

use crate::pool::{PoolAccounts, PoolDoc, PumpFunAccounts};
use crate::swap_pump_fun::{
    build_pump_fun_buy_ix, find_ata, wsol_pk, PumpStaticPdas,
    token_program_pk,
};

/// **Deprecated**: kept as a const so other crates / bins importing
/// `measure::BUY_NONCE` don't break. `measure_pool_cu` no longer uses
/// it — see the module doc-comment. Will be removed once no consumers
/// reference it.
#[deprecated(note = "measure_pool_cu now signs with get_latest_blockhash, this is no longer used")]
pub const BUY_NONCE: &str = "RaL8vMu4CCapTZSsNkB4w5AqVi8xErYfMmakQXGDtJ4";
/// Tip recipient — Zeroslot's tip account. Replaces the prior SUPRA
/// vault address (which was paired with the old public-RPC send).
/// Mirrors `crate::zeroslot::ZEROSLOT_TIP_ACCOUNT`.
pub const TIP_RECIPIENT: &str = "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3";
/// 100_000 lamports (~$0.01) — Zeroslot's priority-lane tip. Mirrors
/// `crate::zeroslot::TIP_LAMPORTS`.
pub const TIP_LAMPORTS: u64 = 100_000;
/// 0.0001213357 SOL — 10× smaller than `TIP_LAMPORTS`. Both
/// `base_amount_out` and `max_quote_amount_in` derived inside the buy ix
/// scale linearly with this for the small-amount-vs-reserves regime, so
/// the on-chain CU charge stays representative while the per-pool SOL
/// cost of a full sweep drops 10×.
pub const SWAP_IN_LAMPORTS: u64 = 121_335;
pub const CU_LIMIT_CEILING: u32 = 400_000;
pub const CU_PRICE: u64 = 1_000_000;
pub const LOADED_DATA_SIZE_LIMIT: u32 = 13_500_000;
pub const SLIPPAGE_BPS: u32 = 5_000;  // 50% — we just want it to land

/// Outcome of a single measurement attempt.
pub enum MeasureOutcome {
    /// Measurement landed; payload is the raw `compute_units_consumed`.
    Ok(u32),
    /// Wallet doesn't have the base-mint ATA on chain — buy ix would
    /// AccountNotInitialized. Caller (ata::create) should ensure ATA is
    /// created before calling. `bin/measure_cu` uses this to skip.
    SkipAtaMissing,
    /// Pool's quote mint isn't WSOL (e.g. USDC-quoted PumpFun pool).
    /// Our buy ix is hardcoded to spend WSOL, so the tx would revert
    /// with `InvalidQuoteMint`. Skip silently — these pools aren't in
    /// the bot's trading scope.
    SkipNonWsolQuote,
}

/// Measure CU for a single PumpFun pool. Returns the raw on-chain
/// `compute_units_consumed` value (no margin applied).
///
/// Caller responsibilities:
/// - Pool's PoolDoc must be a `PoolAccounts::PumpFun(_)` variant; the function
///   errors otherwise.
/// - Wallet must have enough WSOL in its WSOL ATA to cover `SWAP_IN_LAMPORTS`
///   plus base fee. Caller doesn't pre-check; tx will fail if balance is low.
///
/// No nonce collision: signs with `get_latest_blockhash()`. Safe to run
/// concurrently with the bot. See the module doc-comment for the ~3-5k
/// CU underestimate this implies vs the bot's production buy.
pub async fn measure_pool_cu(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    pool_doc: &PoolDoc,
) -> anyhow::Result<MeasureOutcome> {
    let pool_pk = Pubkey::from_str(&pool_doc.pool).context("pool pubkey")?;
    let pump = match &pool_doc.accounts {
        PoolAccounts::PumpFun(p) => p,
        _ => anyhow::bail!("measure_pool_cu: pool is not PumpFun"),
    };
    // Skip non-WSOL-quoted PumpFun pools (e.g. USDC-quoted). Our buy ix
    // hardcodes WSOL on the quote side, so any other quote mint trips
    // `InvalidQuoteMint` (error 0x1779) and burns a real on-chain fee.
    // Belt-and-suspenders: callers now also filter at the Mongo layer
    // (mongo.rs::pools_*_for_*), so reaching this branch in production
    // implies a stale row or a manual call from `bin/measure_cu`.
    if pump.quote_mint != wsol_pk().to_string() {
        return Ok(MeasureOutcome::SkipNonWsolQuote);
    }
    let wallet_pk = wallet_kp.pubkey();
    // Resolve the LIVE protocol fee recipient from PumpFun's
    // GlobalConfig — the hardcoded `PUMP_PROTOCOL_FEE_RECIPIENT` stales
    // whenever PumpFun rotates the recipient slot, tripping error 6013
    // (InvalidProtocolFeeRecipient). One RPC call per probe is cheap
    // given the 10-min bg-worker cadence.
    let active_recipient =
        crate::swap_pump_fun::fetch_active_protocol_fee_recipient(rpc).await?;
    let pdas = PumpStaticPdas::derive_with_recipient(&wallet_pk, active_recipient);
    let wallet_wsol_ata = find_ata(&wallet_pk, &wsol_pk(), &token_program_pk());
    let tip_to = Pubkey::from_str(TIP_RECIPIENT)?;

    measure_inner(
        rpc,
        wallet_kp,
        wallet_pk,
        wallet_wsol_ata,
        &pdas,
        tip_to,
        pool_pk,
        pump,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn measure_inner(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    wallet_pk: Pubkey,
    wallet_wsol_ata: Pubkey,
    pdas: &PumpStaticPdas,
    tip_to: Pubkey,
    pool_pk: Pubkey,
    pump: &PumpFunAccounts,
) -> anyhow::Result<MeasureOutcome> {
    let base_mint = Pubkey::from_str(&pump.base_mint)?;
    let pool_base_vault = Pubkey::from_str(&pump.pool_base_token_account)?;
    let pool_quote_vault = Pubkey::from_str(&pump.pool_quote_token_account)?;
    let coin_creator = Pubkey::from_str(&pump.coin_creator)?;
    let owner_program = Pubkey::from_str(&pump.owner_program)?;

    // 1. Pre-check the wallet's base-mint ATA exists.
    let wallet_token_ata = find_ata(&wallet_pk, &base_mint, &owner_program);
    if rpc.get_account(&wallet_token_ata).await.is_err() {
        return Ok(MeasureOutcome::SkipAtaMissing);
    }

    // 2. Live reserves for the slippage-floor computation.
    let base_vault_acct = rpc.get_account(&pool_base_vault).await?;
    let quote_vault_acct = rpc.get_account(&pool_quote_vault).await?;
    if base_vault_acct.data.len() < 72 || quote_vault_acct.data.len() < 72 {
        anyhow::bail!("vault account data too short");
    }
    let base_reserves = u64::from_le_bytes(base_vault_acct.data[64..72].try_into().unwrap());
    let quote_reserves =
        u64::from_le_bytes(quote_vault_acct.data[64..72].try_into().unwrap());

    let swap_ix = build_pump_fun_buy_ix(
        &pool_pk,
        &base_mint,
        &pool_base_vault,
        &pool_quote_vault,
        &coin_creator,
        &owner_program,
        pump.is_cashback,
        base_reserves,
        quote_reserves,
        &wallet_pk,
        &wallet_wsol_ata,
        &wallet_token_ata,
        pdas,
        SWAP_IN_LAMPORTS,
        SLIPPAGE_BPS,
    );

    // 3. Fresh blockhash (no nonce — see module doc).
    let latest_blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;

    // 4. Compose the tx — same layout as the bot's production buy MINUS
    // the leading advance_nonce_ix. See module doc for the CU-skew
    // caveat (advance_nonce ix costs ~3-5k CU on chain; production buys
    // include it, so measured value is ~3-5k LOW).
    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_CEILING);
    let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);
    let data_size_ix =
        ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(LOADED_DATA_SIZE_LIMIT);
    let tip_ix = system_instruction::transfer(&wallet_pk, &tip_to, TIP_LAMPORTS);

    let message = Message::new_with_blockhash(
        &[
            tip_ix,            // slot 0 — empirically cheaper than slot N
            cu_limit_ix,
            cu_price_ix,
            data_size_ix,
            swap_ix,
        ],
        Some(&wallet_pk),
        &latest_blockhash,
    );
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[wallet_kp], latest_blockhash);

    // 5. Send via Zeroslot (replaces the prior public-RPC
    // send_transaction call so central's outbound CU probes go through
    // the operator's paid lane). Confirm via the regular RPC since
    // Zeroslot doesn't surface getSignatureStatuses.
    let http = crate::zeroslot::build_http_client()
        .context("build zeroslot http client")?;
    let sig: Signature = crate::zeroslot::send_transaction(&http, &tx)
        .await
        .context("zeroslot send_transaction")?;

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
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
                    break;
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("confirm timeout for sig={sig}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 6. Read meta.compute_units_consumed.
    let tx_info = rpc
        .get_transaction_with_config(
            &sig,
            solana_client::rpc_config::RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        )
        .await
        .context("get_transaction")?;
    let meta = tx_info
        .transaction
        .meta
        .ok_or_else(|| anyhow!("tx meta missing"))?;
    let cu_opt: Option<u64> = meta.compute_units_consumed.into();
    let cu = cu_opt.ok_or_else(|| anyhow!("compute_units_consumed missing"))?;
    Ok(MeasureOutcome::Ok(cu.min(u32::MAX as u64) as u32))
}
