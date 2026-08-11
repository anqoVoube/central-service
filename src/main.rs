use std::{sync::Arc, time::Duration};

use solana_sdk::signature::{Keypair, Signer};
use tokio::sync::broadcast;

use central_service::{
    alts, auto_unwrap, backfill, bans, block_detail, config, discover, fee_config, lanes,
    leaders, mongo, poll, positions, validators, ws,
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
    println!("For rebuild.");

    let repo = Arc::new(mongo::Repo::connect(&cfg.mongo_uri, &cfg.mongo_db).await?);
    repo.ensure_indexes().await?;

    // Migrate any pool docs that pre-date `pair_created_at_ms`. Blocks
    // startup so the WS init filter (< 30 days) sees a fully-populated set
    // on first connect. After the first run this is a no-op since all
    // pools have the field set.
    backfill::run(&repo).await;

    // Migrate any pump_fun pool docs that pre-date the `is_mayhem_mode`
    // field. Reads pool account byte 243 via RPC and writes the flag to
    // Mongo. MUST run before the WS server accepts client connections —
    // bots consult this flag to route the pAMM protocol_fee_recipient,
    // and stale docs (deserialize `is_mayhem_mode = false` via
    // `#[serde(default)]`) will fail every buy on mayhem pools with
    // Anchor 6013 InvalidProtocolFeeRecipient.
    backfill::run_mayhem(&repo, &cfg.rpc_url).await;

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
        block_detail::BlockDetailStore::open(
            &cfg.block_details_db_path,
            // Dedicated endpoint: `getBlock(full)` is the heaviest call we
            // make and would otherwise contend with orderflow/bans/leaders.
            cfg.block_detail_rpc_url.clone(),
        )?;
    {
        // Log host only — the URL carries an api key.
        let host = cfg
            .block_detail_rpc_url
            .split("://")
            .nth(1)
            .and_then(|r| r.split('/').next())
            .unwrap_or("?");
        tracing::info!(
            "block_details db opened at {} (rpc={host}{})",
            cfg.block_details_db_path.display(),
            if cfg.block_detail_rpc_url == cfg.rpc_url { ", SHARED with RPC_URL" } else { ", dedicated" },
        );
    }

    // Orderflow detections that reached the chain (landed/failed). Persisted
    // so the dashboard survives restarts; never-landed txs are dropped.
    let orderflow = central_service::orderflow::OrderflowStore::open(
        &cfg.orderflow_db_path,
        cfg.rpc_url.clone(),
    )?;

    // Secondary copy-trading bot's trade log. Pure storage: the front-run
    // verdict is computed BY THE BOT from its geyser stream (which carries
    // slot and intra-block index on every transaction) and pushed here, so
    // central makes no RPC calls for it at all.
    let copy_trades =
        central_service::copytrades::CopyTradeStore::open(&cfg.copy_trades_db_path)?;

    // Exits, one row each. A mirror leaves a position in pieces, and the trade
    // row can only ever hold the last of them.
    let copy_sells =
        central_service::copysells::CopySellStore::open(&cfg.copy_sells_db_path)?;

    // Rows written before the block-order backfill existed carry no ordering
    // for their reverted buys. New verdicts fill themselves in; these cannot,
    // so sweep the recent ones once at startup. Bounded and paced on purpose —
    // each row is a getBlock, and the history page must not be competing with
    // it for the RPC.
    {
        let trades = copy_trades.clone();
        let details = block_details.clone();
        tokio::spawn(async move {
            central_service::ws::backfill_block_order(trades, details, 60).await;
        });
    }

    let tip_priority =
        central_service::tip_priority::TipPriorityStore::open(
            &cfg.tip_priority_db_path,
            broadcast_tx.clone(),
        )?;
    tracing::info!(
        "tip_priority db opened at {}",
        cfg.tip_priority_db_path.display()
    );

    let guaranteed =
        central_service::guaranteed::GuaranteedStore::open(
            &cfg.guaranteed_db_path,
            broadcast_tx.clone(),
        )?;
    tracing::info!(
        "guaranteed db opened at {}",
        cfg.guaranteed_db_path.display()
    );

    let fee_config_file = fee_config::FeeConfigFile::open(&cfg.fee_config_path)?;

    // Auto-unwrap poller: 30 s tick, keeps native SOL ≥ low_threshold by
    // unwrapping WSOL up to high_threshold. Config lives in a JSON file
    // (`auto_unwrap_config.json`); the operator toggles via the dashboard
    // which proxies to `POST /auto-unwrap/config`. Handle held here so
    // the HTTP layer + poller both see the same ArcSwap.
    let auto_unwrap_config = Arc::new(arc_swap::ArcSwap::from(Arc::new(auto_unwrap::load_config())));
    auto_unwrap::spawn(
        cfg.rpc_url.clone(),
        Arc::clone(&wallet_kp),
        Arc::clone(&auto_unwrap_config),
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
        tokio::spawn(discover::run(discover_rx, rpc_url, repo, tx));
    }

    // No periodic background worker any more. It used to create and repair
    // ATAs (that moved into the bot's own buy) and to measure each pool's CU
    // by firing a real 121_335-lamport buy with a 100_000-lamport tip every
    // ten minutes. Nothing reads the measurement: the copy strategy pins
    // `COPY_CU_LIMIT`, and the legacy path falls back to a static constant.
    // Measure by hand with `bin/measure_cu` if it is ever needed again.

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
        orderflow,
        copy_trades,
        copy_sells,
        tip_priority,
        guaranteed,
        fee_config_file,
        auto_unwrap_config,
    )
    .await
}
