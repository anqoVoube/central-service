//! One-shot send of a real on-chain PumpFun pAMM buy. Hard-coded swap
//! amount and tip amount per the current ask: 0.001213357 SOL each.
//!
//! Tx layout matches the bot's production buy:
//!   ix[0] advance_nonce_account
//!   ix[1] system::transfer(TIP_LAMPORTS → SUPRA tip vault)
//!   ix[2] set_compute_unit_limit(CU_LIMIT_CEILING)
//!   ix[3] set_compute_unit_price(CU_PRICE)
//!   ix[4] set_loaded_accounts_data_size_limit(LOADED_DATA_SIZE_LIMIT)
//!   ix[5] pump_fun_buy_exact_in(sol_in = SWAP_IN_LAMPORTS)
//!
//! Run (either form works):
//!   `cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/send <pool_pubkey>`
//!   `cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/send --pool <pool_pubkey>`
//!
//! Other `--*` flags are accepted but ignored — kept for compatibility with
//! the old cu-sim-test send binary so muscle memory still works.
//!
//! Requires `.env` with `WALLET_KEYPAIR`, `MONGO_URI`, `MONGO_DB`.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use mongodb::bson::doc;
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
    pool::{PoolAccounts, PoolDoc},
    swap_pump_fun::{
        build_pump_fun_buy_ix, find_ata, system_program_pk, token_program_pk, wsol_pk,
        PumpStaticPdas,
    },
};

const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=75715a51-2511-436d-ad3a-1d8c76208072";
const BUY_NONCE: &str = "RaL8vMu4CCapTZSsNkB4w5AqVi8xErYfMmakQXGDtJ4";
const TIP_RECIPIENT: &str = "SUPRAJhgwn1K3xMj9gwNAaDTrkfhZzeBgygtRG4jBHV";
const SYSVAR_RECENT_BLOCKHASHES: &str = "SysvarRecentB1ockHashes11111111111111111111";

/// 0.001213357 SOL.
const SWAP_IN_LAMPORTS: u64 = 1_213_357;
/// 0.001213357 SOL — same as the swap, per request.
const TIP_LAMPORTS: u64 = 1_213_357;
const CU_LIMIT_CEILING: u32 = 400_000;
const CU_PRICE: u64 = 1_000_000;
const LOADED_DATA_SIZE_LIMIT: u32 = 13_500_000;
const SLIPPAGE_BPS: u32 = 5_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let pool_str = parse_pool_arg().context(
        "usage: send <pool_pubkey>  |  send --pool <pool_pubkey>",
    )?;
    let pool_pk = Pubkey::from_str(&pool_str).context("invalid pool pubkey")?;

    let wallet_keypair_b58 =
        std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
    let wallet_kp = Keypair::from_base58_string(&wallet_keypair_b58);

    let client = mongodb::Client::with_uri_str(&mongo_uri)
        .await
        .context("mongo connect")?;
    let pool_doc = client
        .database(&mongo_db)
        .collection::<PoolDoc>("pools")
        .find_one(doc! { "pool": &pool_str })
        .await?
        .ok_or_else(|| anyhow!("pool {pool_str} not found in Mongo"))?;
    let pump = match &pool_doc.accounts {
        PoolAccounts::PumpFun(p) => p.clone(),
        _ => anyhow::bail!("pool is not pump_fun"),
    };

    let rpc =
        RpcClient::new_with_commitment(HELIUS_RPC.to_string(), CommitmentConfig::confirmed());

    tracing::info!(
        pool = %pool_pk,
        swap_lamports = SWAP_IN_LAMPORTS,
        tip_lamports = TIP_LAMPORTS,
        "send starting"
    );

    let wallet_pk = wallet_kp.pubkey();
    let pdas = PumpStaticPdas::derive(&wallet_pk);
    let wallet_wsol_ata = find_ata(&wallet_pk, &wsol_pk(), &token_program_pk());

    let base_mint = Pubkey::from_str(&pump.base_mint)?;
    let pool_base_vault = Pubkey::from_str(&pump.pool_base_token_account)?;
    let pool_quote_vault = Pubkey::from_str(&pump.pool_quote_token_account)?;
    let coin_creator = Pubkey::from_str(&pump.coin_creator)?;
    let owner_program = Pubkey::from_str(&pump.owner_program)?;
    let wallet_token_ata = find_ata(&wallet_pk, &base_mint, &owner_program);

    if rpc.get_account(&wallet_token_ata).await.is_err() {
        anyhow::bail!("wallet base-mint ATA missing — run create_missing_atas first");
    }

    let base_vault_acct = rpc.get_account(&pool_base_vault).await?;
    let quote_vault_acct = rpc.get_account(&pool_quote_vault).await?;
    if base_vault_acct.data.len() < 72 || quote_vault_acct.data.len() < 72 {
        anyhow::bail!("vault data too short");
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
        &pdas,
        SWAP_IN_LAMPORTS,
        SLIPPAGE_BPS,
    );

    let nonce_pk = Pubkey::from_str(BUY_NONCE)?;
    let tip_to = Pubkey::from_str(TIP_RECIPIENT)?;
    let sysvar_recent_blockhashes = Pubkey::from_str(SYSVAR_RECENT_BLOCKHASHES)?;
    let nonce_acct = rpc.get_account(&nonce_pk).await.context("get nonce account")?;
    if nonce_acct.data.len() < 72 {
        anyhow::bail!("nonce account too short");
    }
    let nonce_hash_bytes: [u8; 32] = nonce_acct.data[40..72].try_into()?;
    let nonce_blockhash = Hash::new_from_array(nonce_hash_bytes);

    let advance_nonce_ix = Instruction {
        program_id: system_program_pk(),
        accounts: vec![
            AccountMeta::new(nonce_pk, false),
            AccountMeta::new_readonly(sysvar_recent_blockhashes, false),
            AccountMeta::new_readonly(wallet_pk, true),
        ],
        data: vec![4, 0, 0, 0],
    };
    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_CEILING);
    let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(CU_PRICE);
    let data_size_ix =
        ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(LOADED_DATA_SIZE_LIMIT);
    let tip_ix = system_instruction::transfer(&wallet_pk, &tip_to, TIP_LAMPORTS);

    let message = Message::new_with_blockhash(
        &[
            advance_nonce_ix,
            tip_ix,
            cu_limit_ix,
            cu_price_ix,
            data_size_ix,
            swap_ix,
        ],
        Some(&wallet_pk),
        &nonce_blockhash,
    );
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[&wallet_kp], nonce_blockhash);

    let sig: Signature = rpc.send_transaction(&tx).await.context("send_transaction")?;
    tracing::info!("sent sig={sig}");
    println!("https://solscan.io/tx/{sig}");

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
                        anyhow::bail!("tx failed on-chain: {err:?}");
                    }
                    break;
                }
            }
        }
        if start.elapsed() > timeout {
            anyhow::bail!("confirm timeout sig={sig}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

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
    let cu = cu_opt.unwrap_or(0);

    println!();
    println!("=== landed ===");
    println!("pool:         {pool_pk}");
    println!("sig:          {sig}");
    println!("swap_in:      {SWAP_IN_LAMPORTS} lamports (0.001213357 SOL)");
    println!("tip:          {TIP_LAMPORTS} lamports (0.001213357 SOL)");
    println!("cu_consumed:  {cu}");
    println!("base_fee:     {} lamports", meta.fee);
    Ok(())
}

/// Pool pubkey can be passed positionally (`send <pubkey>`) or with the
/// `--pool <pubkey>` flag. Other `--*` flags are consumed (key + value) so
/// they don't get treated as the positional arg.
fn parse_pool_arg() -> Option<String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    let mut positional: Option<String> = None;
    while i < args.len() {
        let a = &args[i];
        if a == "--pool" {
            return args.get(i + 1).cloned();
        } else if let Some(v) = a.strip_prefix("--pool=") {
            return Some(v.to_string());
        } else if a.starts_with("--") {
            // Skip flag + its value (if any). This is a forgiving parser —
            // an unknown bool-only flag would also skip the next positional,
            // but for this binary the only positional we care about is the
            // pool pubkey and the user can always pass it after the flags.
            i += 2;
            continue;
        } else if positional.is_none() {
            positional = Some(a.clone());
        }
        i += 1;
    }
    positional
}
