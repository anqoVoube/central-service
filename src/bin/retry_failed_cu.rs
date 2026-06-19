//! Targeted CU measurement for a list of pool pubkeys. Paste in the pools
//! you want measured (e.g. transient failures from a `measure_cu --force`
//! run) as positional CLI args; each is hit once. Skips Mongo's recency
//! filter — operates only on what you pass.
//!
//! Already-measured pools you don't pass are left alone, so this is safe
//! to run alongside / after a successful bulk pass.
//!
//! Usage:
//!   cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/retry_failed_cu \
//!     <pool_pubkey> <pool_pubkey> ...
//!
//! Requires `.env` (or env) with WALLET_KEYPAIR, MONGO_URI, MONGO_DB.

use std::time::Duration;

use anyhow::Context;
use mongodb::bson::{doc, DateTime as BsonDateTime};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

use central_service::{
    measure::{measure_pool_cu, MeasureOutcome},
    pool::PoolDoc,
};

const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=75715a51-2511-436d-ad3a-1d8c76208072";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let pool_pks: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .collect();
    if pool_pks.is_empty() {
        anyhow::bail!(
            "usage: retry_failed_cu <pool_pubkey> [<pool_pubkey> ...] \
             — paste the pubkeys logged as failed by measure_cu"
        );
    }

    let wallet_keypair_b58 =
        std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
    let wallet_kp = Keypair::from_base58_string(&wallet_keypair_b58);

    let client = mongodb::Client::with_uri_str(&mongo_uri)
        .await
        .context("mongo connect")?;
    let pools_coll = client.database(&mongo_db).collection::<PoolDoc>("pools");

    let rpc =
        RpcClient::new_with_commitment(HELIUS_RPC.to_string(), CommitmentConfig::confirmed());

    tracing::info!(count = pool_pks.len(), "retry_failed_cu starting");

    let mut measured = 0usize;
    let mut skipped_ata = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for (idx, pool_str) in pool_pks.iter().enumerate() {
        let prefix = format!("[{}/{}] pool={}", idx + 1, pool_pks.len(), pool_str);

        let pool_doc = match pools_coll.find_one(doc! { "pool": pool_str }).await {
            Ok(Some(d)) => d,
            Ok(None) => {
                tracing::warn!("{prefix} not found in Mongo — skipped");
                failed.push(pool_str.clone());
                continue;
            }
            Err(e) => {
                tracing::warn!("{prefix} mongo lookup failed: {e:#}");
                failed.push(pool_str.clone());
                continue;
            }
        };

        match measure_pool_cu(&rpc, &wallet_kp, &pool_doc).await {
            Ok(MeasureOutcome::Ok(cu)) => {
                let cu_i32 = cu.min(i32::MAX as u32) as i32;
                match pools_coll
                    .update_one(
                        doc! { "pool": pool_str },
                        doc! { "$set": {
                            "compute_unit_limit": cu_i32,
                            "cu_measured_at": BsonDateTime::now(),
                            "updated_at": BsonDateTime::now(),
                        } },
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!("{prefix} measured cu={cu} (stored raw)");
                        measured += 1;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "{prefix} measured cu={cu} but Mongo update failed: {e:#}"
                        );
                        failed.push(pool_str.clone());
                    }
                }
            }
            Ok(MeasureOutcome::SkipAtaMissing) => {
                tracing::warn!("{prefix} wallet base-mint ATA missing — skipped");
                skipped_ata += 1;
            }
            Ok(MeasureOutcome::SkipNonWsolQuote) => {
                tracing::info!("{prefix} non-WSOL quote mint — skipped");
                skipped_ata += 1;
            }
            Err(e) => {
                tracing::warn!("{prefix} failed: {e:#}");
                failed.push(pool_str.clone());
            }
        }

        // Pace Helius — same 200ms spacing as the batch binary.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    if !failed.is_empty() {
        tracing::warn!(count = failed.len(), "still-failing pools:");
        for p in &failed {
            tracing::warn!("  {p}");
        }
    }

    tracing::info!(
        total = pool_pks.len(),
        measured,
        skipped_ata_missing = skipped_ata,
        failed = failed.len(),
        "retry_failed_cu done"
    );
    Ok(())
}
