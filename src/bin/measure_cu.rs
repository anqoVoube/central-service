//! One-shot CU measurement script.
//!
//! Walks Mongo pools (PumpFun, ata_status=confirmed, < 14 days old, not
//! measured in last 14 days), fires a real 0.0001 SOL buy per pool via
//! Helius, reads `meta.compute_units_consumed` from the landed tx, and
//! persists the RAW value into `compute_unit_limit`. The bot applies the
//! per-tx safety margin (+1% or whatever it's set to) when sizing the
//! ComputeBudget ix at fire time. Storing raw keeps the source of truth
//! single — the margin policy can be tuned in the bot without re-running
//! this script.
//!
//! Tx layout matches the bot's production shape so the recorded CU is
//! what the bot would actually consume:
//!   ix[0] advance_nonce_account
//!   ix[1] set_compute_unit_limit(400_000)               ← high ceiling
//!   ix[2] set_compute_unit_price(1_000_000)             ← ~$0.04 priority
//!   ix[3] set_loaded_accounts_data_size_limit(13_500_000)
//!   ix[4] swap_buy_ix                                   ← 0.0001 SOL in
//!   ix[5] system::transfer(1 lamport → SUPRA tip)
//!
//! Sequential, 1 pool at a time. Idempotent: pools with recent
//! `cu_measured_at` are skipped on re-run. Pools missing the wallet's ATA
//! are skipped (they'll be retried after the ATA is created).
//!
//! Run:
//!   `cd ~/Work/central-service && cargo run --release --bin measure_cu`
//! Requires `.env` (or environment) with `WALLET_KEYPAIR`, `MONGO_URI`,
//! `MONGO_DB`.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status_client_types::{
    TransactionConfirmationStatus, UiTransactionEncoding,
};

use central_service::{
    config::Config,
    mongo::Repo,
    pool::{PoolAccounts, PoolDoc},
    swap_pump_fun::{
        build_pump_fun_buy_ix, system_program_pk, wsol_pk, PumpStaticPdas, find_ata,
        token_program_pk,
    },
};

const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=e57668cb-43f4-4d35-9d83-fbb9c1d71ad2";
const BUY_NONCE: &str = "RaL8vMu4CCapTZSsNkB4w5AqVi8xErYfMmakQXGDtJ4";
const TIP_RECIPIENT: &str = "SUPRAJhgwn1K3xMj9gwNAaDTrkfhZzeBgygtRG4jBHV";
const TIP_LAMPORTS: u64 = 1_213_357;               // 0.001213357 SOL — matches send / measure.rs
const SWAP_IN_LAMPORTS: u64 = 121_335;             // 0.0001213357 SOL — 10× smaller per ask
const CU_LIMIT_CEILING: u32 = 400_000;             // high enough to never bite
const CU_PRICE: u64 = 1_000_000;                   // microlamports/CU → ~$0.04 priority
const LOADED_DATA_SIZE_LIMIT: u32 = 13_500_000;    // matches bot's prod constant
const SLIPPAGE_BPS: u32 = 5_000;                   // 50% — generous, we just want it to land
const SVRECENT_BLOCKHASHES: &str = "SysvarRecentB1ockHashes11111111111111111111";


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
    tracing::info!(wallet = %wallet_pk, "starting measure_cu");

    let repo = Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?;
    let rpc =
        RpcClient::new_with_commitment(HELIUS_RPC.to_string(), CommitmentConfig::confirmed());

    // Fetch nonce blockhash up-front. Sequential script, so we read it once
    // and re-fetch on each iter (the advance_nonce ix advances it on the
    // chain too).
    let nonce_pk = Pubkey::from_str(BUY_NONCE)?;
    let tip_to = Pubkey::from_str(TIP_RECIPIENT)?;
    let sysvar_recent_blockhashes = Pubkey::from_str(SVRECENT_BLOCKHASHES)?;

    let wallet_wsol_ata = find_ata(&wallet_pk, &wsol_pk(), &token_program_pk());
    {
        // Sanity check: WSOL balance enough to cover the planned swaps.
        let info = rpc.get_account(&wallet_wsol_ata).await
            .context("wallet WSOL ATA not initialised — wrap some SOL first")?;
        if info.data.len() < 72 {
            anyhow::bail!("wallet WSOL ATA data too short");
        }
        let amount = u64::from_le_bytes(info.data[64..72].try_into().unwrap());
        tracing::info!(amount_lamports = amount, "wallet WSOL ATA balance");
        if amount < SWAP_IN_LAMPORTS * 10 {
            tracing::warn!(
                "WSOL balance ({amount}) might be too low — script will fail mid-run \
                 if pool count × {SWAP_IN_LAMPORTS} exceeds it"
            );
        }
    }

    // CLI: pass `--force` (or any first arg) to ignore the cu_measured_at
    // recency filter and re-measure every pump-fun pool. Useful after a
    // tx-layout change that invalidates prior CU values.
    let force = std::env::args().any(|a| a == "--force" || a == "--all");
    let pools = if force {
        repo.pools_for_remeasurement().await?
    } else {
        repo.pools_for_cu_measurement().await?
    };
    tracing::info!(
        count = pools.len(),
        force,
        "pools queued for measurement"
    );

    let mut measured = 0usize;
    let mut skipped_ata = 0usize;
    let mut failed = 0usize;
    let pdas = PumpStaticPdas::derive(&wallet_pk);

    for (idx, pool_doc) in pools.iter().enumerate() {
        let pool_pk = match Pubkey::from_str(&pool_doc.pool) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!(pool = %pool_doc.pool, err = %e, "skip: bad pubkey");
                failed += 1;
                continue;
            }
        };
        let pump = match &pool_doc.accounts {
            PoolAccounts::PumpFun(p) => p,
            _ => {
                // Filter is pump_fun-only but defensive — skip if Mongo
                // surfaces a non-pump_fun row.
                continue;
            }
        };

        let prefix = format!("[{}/{}] pool={}", idx + 1, pools.len(), pool_doc.pool);
        match measure_one(
            &rpc,
            &repo,
            &wallet_kp,
            wallet_pk,
            wallet_wsol_ata,
            &pdas,
            nonce_pk,
            sysvar_recent_blockhashes,
            tip_to,
            pool_pk,
            pump,
        )
        .await
        {
            Ok(MeasureOutcome::Ok { cu_consumed }) => {
                tracing::info!("{prefix} measured cu={cu_consumed} (stored raw)");
                measured += 1;
            }
            Ok(MeasureOutcome::SkipAtaMissing) => {
                tracing::info!("{prefix} skip: wallet base-mint ATA not initialised");
                skipped_ata += 1;
            }
            Err(e) => {
                tracing::warn!("{prefix} failed: {e:#}");
                failed += 1;
            }
        }

        // Light pacing between pools — sequential, but don't hammer Helius.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tracing::info!(
        total = pools.len(),
        measured,
        skipped_ata_missing = skipped_ata,
        failed,
        "measure_cu done"
    );
    Ok(())
}

enum MeasureOutcome {
    Ok { cu_consumed: u64 },
    SkipAtaMissing,
}

#[allow(clippy::too_many_arguments)]
async fn measure_one(
    rpc: &RpcClient,
    repo: &Repo,
    wallet_kp: &Keypair,
    wallet_pk: Pubkey,
    wallet_wsol_ata: Pubkey,
    pdas: &PumpStaticPdas,
    nonce_pk: Pubkey,
    sysvar_recent_blockhashes: Pubkey,
    tip_to: Pubkey,
    pool_pk: Pubkey,
    pump: &central_service::pool::PumpFunAccounts,
) -> anyhow::Result<MeasureOutcome> {
    let base_mint = Pubkey::from_str(&pump.base_mint)?;
    let pool_base_vault = Pubkey::from_str(&pump.pool_base_token_account)?;
    let pool_quote_vault = Pubkey::from_str(&pump.pool_quote_token_account)?;
    let coin_creator = Pubkey::from_str(&pump.coin_creator)?;
    let owner_program = Pubkey::from_str(&pump.owner_program)?;

    // 1. Pre-check the wallet's base-mint ATA exists. If not, skip — the
    //    swap ix would fail with AccountNotInitialized; sending it anyway
    //    burns priority fee for no measurement.
    let wallet_token_ata = find_ata(&wallet_pk, &base_mint, &owner_program);
    if rpc.get_account(&wallet_token_ata).await.is_err() {
        return Ok(MeasureOutcome::SkipAtaMissing);
    }

    // 2. Fetch live pool reserves so the slippage gate can be computed.
    let base_vault_acct = rpc.get_account(&pool_base_vault).await?;
    let quote_vault_acct = rpc.get_account(&pool_quote_vault).await?;
    if base_vault_acct.data.len() < 72 || quote_vault_acct.data.len() < 72 {
        anyhow::bail!("vault account data too short");
    }
    let base_reserves = u64::from_le_bytes(base_vault_acct.data[64..72].try_into().unwrap());
    let quote_reserves =
        u64::from_le_bytes(quote_vault_acct.data[64..72].try_into().unwrap());

    // 3. Build the swap ix matching the bot's prod layout.
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

    // 4. Read the nonce's stored blockhash. Used as the tx's
    //    recent_blockhash; the advance_nonce ix rotates it on land so each
    //    iteration sees a fresh hash.
    let nonce_acct = rpc.get_account(&nonce_pk).await.context("get nonce account")?;
    if nonce_acct.data.len() < 72 {
        anyhow::bail!("nonce account data too short");
    }
    let nonce_hash_bytes: [u8; 32] = nonce_acct.data[40..72]
        .try_into()
        .context("slice nonce blockhash")?;
    let nonce_blockhash = Hash::new_from_array(nonce_hash_bytes);

    // 5. Compose the tx. advance_nonce + compute-budget + swap + tip.
    let advance_nonce_ix = Instruction {
        program_id: system_program_pk(),
        accounts: vec![
            AccountMeta::new(nonce_pk, false),
            AccountMeta::new_readonly(sysvar_recent_blockhashes, false),
            AccountMeta::new_readonly(wallet_pk, true),
        ],
        data: vec![4, 0, 0, 0],  // SystemInstruction::AdvanceNonceAccount = 4
    };
    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_CEILING);
    let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);
    let data_size_ix =
        ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(LOADED_DATA_SIZE_LIMIT);
    let tip_ix = system_instruction::transfer(&wallet_pk, &tip_to, TIP_LAMPORTS);

    let message = Message::new_with_blockhash(
        &[
            advance_nonce_ix,
            tip_ix,            // slot 1 — matches bot's production layout
            cu_limit_ix,
            cu_price_ix,
            data_size_ix,
            swap_ix,
        ],
        Some(&wallet_pk),
        &nonce_blockhash,
    );
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[wallet_kp], nonce_blockhash);

    // 6. Send + poll.
    let sig: Signature = rpc.send_transaction(&tx).await.context("send_transaction")?;
    tracing::debug!(sig = %sig, "sent");

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

    // 7. Fetch the landed tx + read meta.compute_units_consumed.
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
    let cu_consumed_opt: Option<u64> = meta.compute_units_consumed.into();
    let cu_consumed =
        cu_consumed_opt.ok_or_else(|| anyhow!("compute_units_consumed missing"))?;
    let cu_raw = cu_consumed.min(i32::MAX as u64) as i32;

    // 8. Persist raw — bot applies its safety margin at fire time.
    repo.update_cu_limit(&pool_pk.to_string(), cu_raw).await?;

    Ok(MeasureOutcome::Ok { cu_consumed })
}
