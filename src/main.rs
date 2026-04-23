use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;

mod config;
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
    tracing::info!(
        "config loaded: bind={} poll_interval_secs={} whitelist={}",
        cfg.ws_bind,
        cfg.poll_interval_secs,
        cfg.whitelist_ips.len()
    );

    let repo = Arc::new(mongo::Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?);
    repo.ensure_indexes().await?;

    let (tx, _rx) = broadcast::channel::<ws::ServerMsg>(1024);

    {
        let repo = Arc::clone(&repo);
        let tx = tx.clone();
        let rpc_url = cfg.rpc_url.clone();
        let interval = Duration::from_secs(cfg.poll_interval_secs);
        tokio::spawn(async move {
            if let Err(e) = poll::run(repo, rpc_url, interval, tx).await {
                tracing::error!("poll task exited: {e:#}");
            }
        });
    }

    ws::serve(cfg.ws_bind, cfg.whitelist_ips, repo, tx).await
}
