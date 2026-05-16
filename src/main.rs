use std::{sync::Arc, time::Duration};

use solana_sdk::signature::{Keypair, Signer};
use tokio::sync::broadcast;

use central_service::{
    alts, ata, backfill, bans, block_detail, config, discover, lanes, leaders, mongo, poll, pool,
    positions, validators, ws,
};

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
    {
        // Echo the parsed whitelist on startup so it's clear which IPs were
        // actually loaded from `WHITELIST_IPS` — a refused connection then
        // means the source IP isn't in this list (NAT / wrong NIC / typo).
        let mut ips: Vec<String> = cfg.whitelist_ips.iter().map(|ip| ip.to_string()).collect();
        ips.sort();
        println!("[whitelist] {} ip(s) loaded: {}", ips.len(), ips.join(", "));
    }
    println!("For rebuild!");

    let repo = Arc::new(mongo::Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?);
    repo.ensure_indexes().await?;

    // Migrate any pool docs that pre-date `pair_created_at_ms`. Blocks
    // startup so the WS init filter (< 7 days) sees a fully-populated set
    // on first connect. After the first run this is a no-op since all
    // pools have the field set.
    backfill::run(&repo).await;

    let positions = positions::Positions::load_and_spawn(cfg.positions_log.clone()).await?;

    let (broadcast_tx, _rx) = broadcast::channel::<ws::ServerMsg>(1024);
    let (discover_tx, discover_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let alts = alts::AltStore::open(
        &cfg.alts_db_path,
        cfg.rpc_url.clone(),
        broadcast_tx.clone(),
    )?;
    tracing::info!("alts db opened at {}", cfg.alts_db_path.display());

    let bans = bans::BansStore::open(
        &cfg.bans_db_path,
        cfg.rpc_url.clone(),
        broadcast_tx.clone(),
    )?;
    tracing::info!("bans db opened at {}", cfg.bans_db_path.display());

    let lanes = lanes::LaneStore::open(&cfg.lanes_db_path, cfg.rpc_url.clone())?;
    tracing::info!("lanes db opened at {}", cfg.lanes_db_path.display());

    let validators = Arc::new(validators::ValidatorMap::fetch(&cfg.validators_csv_url).await);
    let leaders = leaders::LeaderStore::open(
        &cfg.leaders_db_path,
        cfg.rpc_url.clone(),
        Arc::clone(&validators),
    )?;
    tracing::info!("leaders db opened at {}", cfg.leaders_db_path.display());

    let block_details =
        block_detail::BlockDetailStore::open(&cfg.block_details_db_path, cfg.rpc_url.clone())?;
    tracing::info!(
        "block_details db opened at {}",
        cfg.block_details_db_path.display()
    );

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

    // Replay pending pools through the discovery pipeline so the ATA
    // creator gets another shot at each. New rows reset their attempts
    // counter via the seed binary's `--retry-pending` flag before central
    // is restarted; the actual retry happens here. Rows whose
    // `ata_attempts` still equals 3 will fail again immediately and stay
    // pending — which is the same end state as not running this at all.
    match repo.load_pending_pubkeys().await {
        Ok(pending) if !pending.is_empty() => {
            tracing::info!(
                "discover: replaying {} pending pool(s) into the ATA creator",
                pending.len()
            );
            for pool in pending {
                let _ = discover_tx.send(pool);
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("load_pending_pubkeys failed: {e:#}"),
    }

    ws::serve(
        cfg.ws_bind,
        cfg.whitelist_ips,
        repo,
        positions,
        cfg.positions_log,
        broadcast_tx,
        discover_tx,
        alts,
        bans,
        lanes,
        leaders,
        block_details,
    )
    .await
}
