use std::{sync::Arc, time::Duration};

use solana_sdk::signature::{Keypair, Signer};
use tokio::sync::broadcast;

mod ata;
mod config;
mod discover;
mod mongo;
mod poll;
mod pool;
mod ws;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = config::Config::from_env()?;
    let wallet_kp = Arc::new(Keypair::from_base58_string(&cfg.wallet_keypair_b58));
    tracing::info!(
        "config loaded: bind={} poll_interval_secs={} whitelist={} wallet={}",
        cfg.ws_bind,
        cfg.poll_interval_secs,
        cfg.whitelist_ips.len(),
        wallet_kp.pubkey(),
    );

    let repo = Arc::new(mongo::Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?);
    repo.ensure_indexes().await?;

    let (broadcast_tx, _rx) = broadcast::channel::<ws::ServerMsg>(1024);
    let (discover_tx, discover_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    {
        let repo = Arc::clone(&repo);
        let tx = broadcast_tx.clone();
        let rpc_url = cfg.rpc_url.clone();
        let interval = Duration::from_secs(cfg.poll_interval_secs);
        tokio::spawn(async move {
            if let Err(e) = poll::run(repo, rpc_url, interval, tx).await {
                tracing::error!("poll task exited: {e:#}");
            }
        });
    }

    {
        let repo = Arc::clone(&repo);
        let tx = broadcast_tx.clone();
        let rpc_url = cfg.rpc_url.clone();
        let kp = Arc::clone(&wallet_kp);
        tokio::spawn(discover::run(discover_rx, rpc_url, kp, repo, tx));
    }

    ws::serve(cfg.ws_bind, cfg.whitelist_ips, repo, broadcast_tx, discover_tx).await
}
