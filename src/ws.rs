use std::{collections::HashSet, net::{IpAddr, SocketAddr}, path::PathBuf, str::FromStr, sync::Arc};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, Path, Query, State,
    },
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, mpsc::UnboundedSender};
use tokio_util::io::ReaderStream;

use crate::{
    alts::AltStore,
    bans::BansStore,
    block_detail::BlockDetailStore,
    lanes::{decode_lane, LaneResolution, LaneStore},
    leaders::{LeaderResolution, LeaderStore},
    mongo::Repo,
    pool::{PoolAccounts, PoolDoc},
    positions::{ClosedReport, FailedReport, OpenedReport, Positions},
    validators::LeaderInfo,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    NewPool {
        pool: String,
        #[serde(flatten)]
        accounts: PoolAccounts,
        /// Pool creation time (unix ms) from Dexscreener's `pairCreatedAt`,
        /// resolved at discovery. `None` if Dexscreener hadn't indexed the
        /// pool yet.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pair_created_at_ms: Option<i64>,
        /// Raw `compute_units_consumed` from the 0.001 SOL probe tx that
        /// the discover pipeline runs once the wallet's ATA is created.
        /// `None` when the measurement failed (e.g. pool drained mid-flow);
        /// bot falls back to its static `CU_LIMIT_PUMP_FUN` constant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compute_unit_limit: Option<i32>,
    },
    CreatorChange {
        pool: String,
        old_creator: String,
        new_creator: String,
    },
    /// Forwarded to all locations after the lander reports it. Each location
    /// uses this as the authoritative source for setting `Holding::Held`.
    PositionOpened {
        pool: String,
        token_amount: u64,
        buy_price_sol: f64,
        landed_location_idx: u8,
        sig: String,
        ts_ms: u64,
        landed_path: u8,
        token_name: Option<String>,
        token_symbol: Option<String>,
        dump_pct: f64,
        opportunity_sig: String,
        process_us: u32,
        /// Resolved by central from `opportunity_sig` → slot → leader.
        /// `None` when unresolved or the validator isn't in the CSV.
        #[serde(default)]
        leader: Option<LeaderInfo>,
        /// Authoritative on-chain cost in lamports from the bot's `tx.meta`
        /// delta. 0 for legacy bots — receivers fall back to local estimate.
        #[serde(default)]
        cost_lamports: u64,
        /// Pure on-chain WSOL spent on the swap (no fee/tip/rent). Drives
        /// partial-sell tier classification at bot-side restart-seed. 0
        /// for legacy bots; receivers fall back to `cost_lamports`.
        #[serde(default)]
        buy_size_lamports: u64,
        /// Pool USD liquidity at fire time, snapshotted bot-side. 0 for
        /// legacy bots.
        #[serde(default)]
        liquidity_usd: f64,
        /// Slot the bot read at fire time (see `OpenedReport::observed_slot`).
        /// Surfaces on the dashboard's history/live page.
        #[serde(default)]
        observed_slot: u64,
    },
    /// Forwarded to all locations after the lander reports it. Each location
    /// uses this to clear `Holding::Empty`. `sell_price_sol` / `tokens_sold`
    /// default to 0.0 / 0 from older bots.
    PositionClosed {
        pool: String,
        sig: String,
        ts_ms: u64,
        sell_price_sol: f64,
        tokens_sold: u64,
        /// Authoritative SOL received in lamports (wallet WSOL ATA delta).
        /// 0 for legacy reports.
        sol_received_lamports: u64,
        landed_location_idx: u8,
        landed_path: u8,
    },
    /// Broadcast after central resolves an ALT (either newly-seen, or refetched
    /// because a bot reported it stale). Each location replaces the cache
    /// entry — addresses are the full current on-chain list.
    AltResolved {
        table: String,
        addresses: Vec<String>,
    },
    /// Broadcast after a fake-dumper wallet is identified (failed opp tx with
    /// insufficient ATA balance). Locations cache `wallet → banned_until_ms`
    /// and skip shred-path processing for any tx whose first signer is banned.
    WalletBanned {
        wallet: String,
        banned_until_ms: u64,
        reason: String,
    },
    /// Broadcast after an operator flips a validator's tip-priority status
    /// via the dashboard CHANGE button. Locations update their in-memory
    /// `Arc<ArcSwap<HashSet<Pubkey>>>` and rebuild the leader-schedule
    /// bitmap so subsequent fires use the new routing.
    TipPriorityChanged {
        pubkey: String,
        is_priority: bool,
    },
    /// Broadcast after an operator flips a validator's [G] guaranteed
    /// marker via the dashboard G button. Pure UX flag — dashboards
    /// re-render the [G] pill; bots ignore this message. Serializes as
    /// `{"type":"guaranteed_changed", "pubkey":"...", "is_guaranteed":true|false}`.
    GuaranteedChanged {
        pubkey: String,
        is_guaranteed: bool,
    },
    /// Broadcast after operator POSTs new fee config to
    /// `/fee-config.json`. Each connected bot logs and calls
    /// `std::process::exit(0)` so systemd auto-restarts it with the
    /// fresh config. No payload — the file is the source of truth and
    /// the bot re-fetches it on its next startup. Serializes as
    /// `{"type":"fee_config_reload"}`.
    FeeConfigReload,
    /// Late-arriving leader info for a `position_opened` whose leader RPC
    /// took longer than the bot's open→close cycle (or any time after the
    /// open). Bots merge this into the matching `central_positions` entry
    /// keyed by `opportunity_sig`; if no entry exists (already closed),
    /// the message is a no-op. Never re-creates a row.
    LeaderResolved {
        opportunity_sig: String,
        leader: LeaderInfo,
    },
    /// Broadcast after a buy attempt landed on chain but reverted. Pure
    /// dashboard feed — locations don't act on this; their own balance-delta
    /// detection already keeps `Holding` in sync.
    PositionFailed {
        pool: String,
        sig: String,
        ts_ms: u64,
        landed_location_idx: u8,
        landed_path: u8,
        dump_pct: f64,
        opportunity_sig: String,
        token_name: Option<String>,
        token_symbol: Option<String>,
        buy_size_lamports: u64,
        /// Bot's pre-fire budget estimate (fee + tip). Overstates by the
        /// tip amount for landed-and-reverted txs since the tip ix reverts
        /// with the tx. Kept for legacy compatibility; prefer
        /// `actual_fee_lamports` when present.
        expected_cost_lamports: u64,
        /// Authoritative `meta.fee` from `getTransaction` — sig fee +
        /// priority fee, no tip. 0 when central couldn't resolve the tx.
        #[serde(default)]
        actual_fee_lamports: u64,
        #[serde(default)]
        leader: Option<LeaderInfo>,
        /// Pool USD liquidity at fire time, snapshotted bot-side. 0 for
        /// legacy bots.
        #[serde(default)]
        liquidity_usd: f64,
        /// Bot's local dispatch latency at fire time (µs). 0 for legacy bots.
        #[serde(default)]
        process_us: u32,
        /// Slot the bot read at fire time (see `FailedReport::observed_slot`).
        #[serde(default)]
        observed_slot: u64,
    },
    /// Broadcast after the dashboard bans a pool (`POST /ban`). Every
    /// location drops the pool from its in-memory `Pools` + prebuild
    /// store, so it stops trading immediately. Mongo's `disabled` flag
    /// keeps it out of future inits. Serializes as
    /// `{"type":"pool_disabled","pool":"<pubkey>"}`.
    PoolDisabled {
        pool: String,
    },
    /// Broadcast after the dashboard `/ttp` page toggles a pool's
    /// token-tip-priority flag (`POST /ttp`). Every bot updates its
    /// in-memory `PoolState.is_ttp` immediately so the next shred-path
    /// buy on that pool respects the new flag without restart. Mongo's
    /// `is_ttp` flag is the source of truth for the next init.
    /// Serializes as `{"type":"pool_ttp_changed","pool":"<pubkey>","is_ttp":<bool>}`.
    PoolTtpChanged {
        pool: String,
        is_ttp: bool,
    },
    /// Rebroadcast of a bot's fire sig prefix. Only FR2 subscribes to
    /// the wallet-scoped `transactions_status` geyser filter, so every
    /// location's failures stream through FR2 — but FR2's
    /// `DISPATCHED_SIG_PREFIXES` only contains FR2's own fires unless
    /// siblings actively broadcast theirs. Every bot's
    /// `handle_runtime_msg` calls `intern_dispatched_sig(prefix)` on
    /// this message so FR2 recognizes sibling fires when their failure
    /// arrives via geyser. Serializes as
    /// `{"type":"sig_dispatched","prefix":"<8 base58>"}`.
    SigDispatched {
        prefix: String,
    },
}

/// Inbound from a location. `discovered_pool` is Frankfurt-only;
/// `position_opened` / `position_closed` come from the lander.
/// `alts_unknown` comes from any location's shred path on a v0 tx whose
/// referenced ALT is missing or stale — central re-fetches and broadcasts
/// `alt_resolved` to all clients.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    DiscoveredPool { pool: String },
    PositionOpened(OpenedReport),
    PositionClosed(ClosedReport),
    PositionFailed(FailedReport),
    AltsUnknown { tables: Vec<String> },
    /// After the bot dispatches a shred-path buy on an opportunity tx, it
    /// reports the dumper's tx info. Central waits ~5s, queries
    /// `getSignatureStatuses(sig)`, and if the tx failed, pulls the dumper's
    /// ATA balance via `getAccountInfo(dumper_ata)` and bans for 24h when
    /// `balance < amount_in`.
    OppCheck {
        sig: String,
        dumper_pk: String,
        dumper_ata: String,
        amount_in: u64,
    },
    /// A bot just fired a tx — its 8-char sig prefix is echoed to
    /// central for fan-out to every WS client. Closes the FR2-only
    /// `transactions_status` cross-location gap (see
    /// `ServerMsg::SigDispatched` for the design rationale).
    SigDispatched {
        prefix: String,
    },
}

#[derive(Clone)]
struct AppState {
    repo: Arc<Repo>,
    positions: Positions,
    whitelist: Arc<HashSet<IpAddr>>,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
    positions_log: Arc<PathBuf>,
    alts: AltStore,
    bans: BansStore,
    lanes: LaneStore,
    leaders: LeaderStore,
    block_details: BlockDetailStore,
    tip_priority: crate::tip_priority::TipPriorityStore,
    guaranteed: crate::guaranteed::GuaranteedStore,
    fee_config_file: crate::fee_config::FeeConfigFile,
    /// Live-mutable auto-unwrap config. Read by `GET /auto-unwrap/config`,
    /// swapped by `POST /auto-unwrap/config`. The poller task in
    /// `auto_unwrap::spawn` shares the same ArcSwap so operator changes
    /// take effect on the next 30 s tick without restart.
    auto_unwrap_config: Arc<arc_swap::ArcSwap<crate::auto_unwrap::AutoUnwrapConfig>>,
}

pub async fn serve(
    bind: SocketAddr,
    whitelist_ips: HashSet<IpAddr>,
    repo: Arc<Repo>,
    positions: Positions,
    positions_log: PathBuf,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
    alts: AltStore,
    bans: BansStore,
    lanes: LaneStore,
    leaders: LeaderStore,
    block_details: BlockDetailStore,
    tip_priority: crate::tip_priority::TipPriorityStore,
    guaranteed: crate::guaranteed::GuaranteedStore,
    fee_config_file: crate::fee_config::FeeConfigFile,
    auto_unwrap_config: Arc<arc_swap::ArcSwap<crate::auto_unwrap::AutoUnwrapConfig>>,
) -> anyhow::Result<()> {
    let state = AppState {
        repo,
        positions,
        whitelist: Arc::new(whitelist_ips),
        tx,
        discover_tx,
        positions_log: Arc::new(positions_log),
        alts,
        bans,
        lanes,
        leaders,
        block_details,
        tip_priority,
        guaranteed,
        fee_config_file,
        auto_unwrap_config,
    };
    // TTP sweeper: every 5s, drop expired temporary tip-priority entries.
    // `prune_expired` broadcasts `tip_priority_changed{is_priority:false}`
    // per removed validator, so bots revert it to default within ~5s of
    // expiry. Permanent entries are never touched.
    {
        let tp = state.tip_priority.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                if let Err(e) = tp.prune_expired() {
                    tracing::error!("ttp prune_expired failed: {e:#}");
                }
            }
        });
    }
    let app = Router::new()
        .route("/", get(serve_sig_ui))
        .route("/ws", get(upgrade))
        .route("/positions.jsonl", get(serve_positions_log))
        .route("/alts.bin", get(serve_alts_snapshot))
        .route("/bans.bin", get(serve_bans_snapshot))
        .route("/bans/:wallet", get(serve_ban_lookup))
        .route("/sig/:sig", get(serve_sig_search))
        .route("/time/:time", get(serve_time_search))
        .route("/block-detail/:opp_sig", get(serve_block_detail))
        .route("/ban", post(serve_ban_pool))
        .route("/banned", get(serve_banned_pools))
        .route("/ttp", post(serve_set_pool_ttp))
        .route("/ttp/list", get(serve_pool_ttp_view))
        .route("/tip-priority", post(serve_tip_priority_set))
        .route("/tip-priority.bin", get(serve_tip_priority_snapshot))
        .route("/tip-priority-status", get(serve_tip_priority_status))
        .route("/guaranteed", post(serve_guaranteed_set))
        .route("/guaranteed.bin", get(serve_guaranteed_snapshot))
        .route(
            "/fee-config.json",
            get(serve_fee_config_json).post(serve_fee_config_set),
        )
        .route(
            "/auto-unwrap/config",
            get(serve_auto_unwrap_get).post(serve_auto_unwrap_post),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("ws server listening on {bind}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Wrap [`LeaderStore::resolve`] so an empty / missing opp sig short-circuits
/// to `None` without hitting RPC. Sells (`position_closed`) have no
/// opp_sig — only buys feed this path.
async fn resolve_leader(store: &LeaderStore, opp_sig: &str) -> Option<LeaderInfo> {
    if opp_sig.is_empty() {
        return None;
    }
    match store.resolve(opp_sig).await {
        LeaderResolution::Resolved(info) => Some(info),
        LeaderResolution::Unknown => None,
    }
}

/// IP-whitelisted file download. Streams `positions.jsonl` from disk so
/// dashboard processes on each location box can read it without needing
/// shared filesystem access. Same whitelist as the WS upgrade.
async fn serve_positions_log(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting positions.jsonl from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /positions.jsonl from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let path = state.positions_log.as_path();
    let file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // No events yet — return an empty body so the dashboard can show
            // "no trades" instead of erroring.
            return Response::builder()
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .body(Body::empty())
                .expect("build empty body");
        }
        Err(e) => {
            tracing::error!("positions.jsonl open failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "open failed").into_response();
        }
    };
    let stream = ReaderStream::new(file);
    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .expect("build streamed body")
}

/// Bot-init endpoint. Bots fetch this once at startup before opening the WS,
/// then receive incremental `alt_resolved` broadcasts. IP-whitelisted; same
/// list as the WS upgrade and `/positions.jsonl`.
async fn serve_alts_snapshot(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting alts.bin from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /alts.bin from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let bytes = match state.alts.snapshot_bincode() {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("alts snapshot build failed: {e:#}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "snapshot failed").into_response();
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(bytes))
        .expect("build alts body")
}

#[derive(Deserialize)]
struct BanPoolReq {
    pool: String,
}

/// Permanently ban a pool. Triggered by the dashboard BAN button (relayed
/// to this endpoint). Sets Mongo `disabled: true` then broadcasts
/// `pool_disabled` so every connected bot drops it from its in-memory
/// `Pools` immediately. IP-whitelisted — same list as the WS upgrade.
async fn serve_ban_pool(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<BanPoolReq>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /ban from non-whitelisted ip {ip}");
        println!("[whitelist] reject POST /ban from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let pool = req.pool.trim().to_owned();
    if pool.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing pool").into_response();
    }
    if let Err(e) = state.repo.set_pool_disabled(&pool).await {
        tracing::error!("set_pool_disabled({pool}) failed: {e:#}");
        return (StatusCode::INTERNAL_SERVER_ERROR, "mongo update failed").into_response();
    }
    // Broadcast to every connected bot. `send` errs only when there are
    // no live receivers — harmless (the Mongo flag still keeps it out of
    // future inits).
    let _ = state.tx.send(ServerMsg::PoolDisabled { pool: pool.clone() });
    println!("[ban] pool={pool} disabled + broadcast");
    (StatusCode::OK, "banned").into_response()
}

#[derive(Deserialize)]
struct SetPoolTtpReq {
    pool: String,
    is_ttp: bool,
}

/// Toggle a pool's `is_ttp` flag (token-tip-priority). Dashboard's
/// `/ttp` page CHANGE button relays here. Persists to Mongo +
/// broadcasts `PoolTtpChanged` so every bot updates its in-memory
/// `PoolState.is_ttp` for the next shred-path fire. IP-whitelisted.
async fn serve_set_pool_ttp(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<SetPoolTtpReq>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /ttp from non-whitelisted ip {ip}");
        println!("[whitelist] reject POST /ttp from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let pool = req.pool.trim().to_owned();
    if pool.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing pool").into_response();
    }
    if let Err(e) = state.repo.set_pool_ttp(&pool, req.is_ttp).await {
        tracing::error!("set_pool_ttp({pool}, {}) failed: {e:#}", req.is_ttp);
        return (StatusCode::INTERNAL_SERVER_ERROR, "mongo update failed").into_response();
    }
    let _ = state.tx.send(ServerMsg::PoolTtpChanged {
        pool: pool.clone(),
        is_ttp: req.is_ttp,
    });
    println!("[ttp] pool={pool} is_ttp={} broadcast", req.is_ttp);
    (StatusCode::OK, "ok").into_response()
}

/// Dashboard `/ttp` page list endpoint. Returns all confirmed PumpFun
/// WSOL pools with their `is_ttp` flag + token meta. IP-whitelisted.
async fn serve_pool_ttp_view(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting GET /ttp/list from non-whitelisted ip {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let pools = match state.repo.pools_for_ttp_view().await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("pools_for_ttp_view failed: {e:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "mongo query failed",
            )
                .into_response();
        }
    };
    let rows: Vec<serde_json::Value> = pools
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "pool": p.pool,
                "token_name": p.token_name,
                "token_symbol": p.token_symbol,
                "pair_created_at_ms": p.pair_created_at_ms,
                "is_ttp": p.is_ttp,
                "is_unique": p.is_unique,
            })
        })
        .collect();
    axum::Json(rows).into_response()
}

#[derive(Deserialize)]
struct TipPrioritySetReq {
    pubkey: String,
    /// "tip_priority" or "default"
    status: String,
}

/// Flip a validator's tip-priority status. Dashboard's CHANGE button
/// relays here. Persists to sled + broadcasts `TipPriorityChanged` so
/// every connected bot updates immediately. IP-whitelisted.
async fn serve_tip_priority_set(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<TipPrioritySetReq>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /tip-priority from non-whitelisted ip {ip}");
        println!("[whitelist] reject POST /tip-priority from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let pk = match solana_sdk::pubkey::Pubkey::from_str(req.pubkey.trim()) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid pubkey: {e}"))
                .into_response();
        }
    };
    // "ttp" = temporary tip-priority: tip-priority now, auto-reverts to
    // default after the operator-configured `ttp_ttl_secs` (from fee config).
    let result = match req.status.trim() {
        "tip_priority" => state.tip_priority.set(pk, true),
        "default" => state.tip_priority.set(pk, false),
        "ttp" => {
            let ttl = ttp_ttl_secs(&state);
            let expires_at = now_unix().saturating_add(ttl);
            state.tip_priority.set_temporary(pk, expires_at)
        }
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "status must be \"tip_priority\", \"ttp\", or \"default\", got {other:?}"
                ),
            )
                .into_response();
        }
    };
    if let Err(e) = result {
        tracing::error!("tip_priority set({pk}, {:?}) failed: {e:#}", req.status);
        return (StatusCode::INTERNAL_SERVER_ERROR, "sled write failed")
            .into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

/// Current unix time in whole seconds.
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read the operator-configured TTP lifetime (seconds) from the live fee
/// config, falling back to the built-in default if it can't be read/parsed.
fn ttp_ttl_secs(state: &AppState) -> u64 {
    state
        .fee_config_file
        .read_bytes()
        .ok()
        .and_then(|b| serde_json::from_slice::<crate::fee_config::FeeConfig>(&b).ok())
        .map(|c| c.ttp_ttl_secs)
        .unwrap_or_else(crate::fee_config::default_ttp_ttl_secs)
}

/// JSON map `{ "<pubkey>": remaining_secs, ... }` of active temporary
/// (TTP) entries. Dashboard polls this to render the [TTP] pill + countdown.
async fn serve_tip_priority_status(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    match state.tip_priority.temporary_status() {
        Ok(rows) => {
            let map: std::collections::HashMap<String, u64> = rows.into_iter().collect();
            Json(map).into_response()
        }
        Err(e) => {
            tracing::error!("tip_priority temporary_status failed: {e:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "status failed").into_response()
        }
    }
}

/// Bincode `Vec<Pubkey>` snapshot of the current tip-priority set.
/// Bots fetch at startup + on WS reconnect to refresh in-memory state.
async fn serve_tip_priority_snapshot(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting tip-priority.bin from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /tip-priority.bin from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let bytes = match state.tip_priority.snapshot_bincode() {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("tip-priority snapshot build failed: {e:#}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "snapshot failed")
                .into_response();
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(bytes))
        .expect("build tip-priority body")
}

#[derive(Deserialize)]
struct GuaranteedSetReq {
    pubkey: String,
    /// `true` adds the [G] marker, `false` removes it.
    is_guaranteed: bool,
}

/// Flip a validator's [G] guaranteed marker. Dashboard's G button
/// relays here. Persists to sled + broadcasts `GuaranteedChanged`.
/// IP-whitelisted. Pure UX flag — no bot consumption.
async fn serve_guaranteed_set(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<GuaranteedSetReq>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /guaranteed from non-whitelisted ip {ip}");
        println!("[whitelist] reject POST /guaranteed from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let pk = match solana_sdk::pubkey::Pubkey::from_str(req.pubkey.trim()) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid pubkey: {e}"))
                .into_response();
        }
    };
    if let Err(e) = state.guaranteed.set(pk, req.is_guaranteed) {
        tracing::error!("guaranteed.set({pk}, {}) failed: {e:#}", req.is_guaranteed);
        return (StatusCode::INTERNAL_SERVER_ERROR, "sled write failed")
            .into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

/// Bincode `Vec<Pubkey>` snapshot of the current [G] set.
/// Dashboards fetch at startup + on poll to refresh in-memory state.
async fn serve_guaranteed_snapshot(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting guaranteed.bin from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /guaranteed.bin from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let bytes = match state.guaranteed.snapshot_bincode() {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("guaranteed snapshot build failed: {e:#}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "snapshot failed")
                .into_response();
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(bytes))
        .expect("build guaranteed body")
}

/// Replace the fee-config JSON file + broadcast a reload signal so
/// every connected bot restarts (via `std::process::exit(0)` →
/// systemd). Dashboard's `/api/config` POSTs here. Validates the body
/// parses as `FeeConfig` (serde catches schema bugs), then writes
/// atomically (tempfile + rename) before broadcasting. IP-whitelisted.
async fn serve_fee_config_set(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(cfg): Json<crate::fee_config::FeeConfig>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /fee-config.json from non-whitelisted ip {ip}");
        println!("[whitelist] reject POST /fee-config.json from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    if let Err(e) = state.fee_config_file.write_atomic(&cfg) {
        tracing::error!("fee_config_file.write_atomic failed: {e:#}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("write failed: {e}"),
        )
            .into_response();
    }
    // Broadcast last — if write fails, nothing has changed and we
    // don't want bots to restart with stale config.
    let subs = state.tx.send(ServerMsg::FeeConfigReload).unwrap_or(0);
    println!(
        "[fee-config] updated {} + broadcast fee_config_reload (subscribers={subs})",
        state.fee_config_file.path().display()
    );
    (StatusCode::OK, "ok").into_response()
}

/// Serve the fee-config JSON file. Bots fetch this once at startup
/// and treat the result as static — there's no broadcast / re-fetch
/// mechanism. To roll out an edit: change the file on central, then
/// `systemctl restart bot` on every location. IP-whitelisted (same
/// list as `/alts.bin`, `/positions.jsonl`, etc.).
async fn serve_fee_config_json(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting fee-config.json from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /fee-config.json from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    match state.fee_config_file.read_bytes() {
        Ok(bytes) => Response::builder()
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .body(Body::from(bytes))
            .expect("build fee-config response"),
        Err(e) => {
            tracing::error!(
                "fee-config read failed (path={:?}): {e}",
                state.fee_config_file.path()
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "fee-config read failed").into_response()
        }
    }
}

/// GET current auto-unwrap config. Returned in the same JSON shape the
/// dashboard's `/api/sol-wsol-swap/auto-config` proxy already serves, so
/// the UI code doesn't need to know whether the response came from bot
/// or central. IP-whitelisted.
async fn serve_auto_unwrap_get(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting GET /auto-unwrap/config from non-whitelisted ip {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let c = **state.auto_unwrap_config.load();
    Json(serde_json::json!({
        "ok": true,
        "data": {
            "enabled": c.enabled,
            "low_threshold_lamports": c.low_threshold_lamports,
            "high_threshold_lamports": c.high_threshold_lamports,
        }
    }))
    .into_response()
}

/// POST update auto-unwrap config. Body is the `AutoUnwrapConfig` shape;
/// validation runs before persistence. On success, ArcSwap replaces the
/// live handle and the poller reads the new value on its next 30 s tick.
/// IP-whitelisted.
async fn serve_auto_unwrap_post(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<crate::auto_unwrap::AutoUnwrapConfig>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting POST /auto-unwrap/config from non-whitelisted ip {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    if let Err(e) = req.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "ok": false, "error": e })),
        )
            .into_response();
    }
    if let Err(e) = crate::auto_unwrap::save_config(&req) {
        tracing::error!("auto-unwrap persist failed: {e:#}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": format!("persist failed: {e}") })),
        )
            .into_response();
    }
    state.auto_unwrap_config.store(Arc::new(req));
    println!(
        "[auto-unwrap] config updated: enabled={} low={:.4} SOL high={:.4} SOL",
        req.enabled,
        req.low_threshold_lamports as f64 / 1e9,
        req.high_threshold_lamports as f64 / 1e9,
    );
    Json(serde_json::json!({ "ok": true })).into_response()
}

/// JSON array of currently-banned pool pubkeys. The dashboard fetches
/// this to gray-out banned rows in the history view. IP-whitelisted.
async fn serve_banned_pools(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    match state.repo.load_disabled_pool_keys().await {
        Ok(keys) => Json(keys).into_response(),
        Err(e) => {
            tracing::error!("load_disabled_pool_keys failed: {e:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "mongo query failed").into_response()
        }
    }
}

/// Wholesale ban-list snapshot for bot startup. Bots fetch once, then receive
/// incremental `wallet_banned` broadcasts. Same IP whitelist as `/alts.bin`.
async fn serve_bans_snapshot(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting bans.bin from non-whitelisted ip {ip}");
        println!("[whitelist] reject GET /bans.bin from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    let bytes = match state.bans.snapshot_bincode() {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("bans snapshot build failed: {e:#}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "snapshot failed").into_response();
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(bytes))
        .expect("build bans body")
}

/// Single-wallet ban lookup. Returns 200 + JSON if banned, 404 if not.
/// **Open to any IP** by design — wallet addresses and ban reasons are
/// public information (the reason cites an on-chain sig), and the endpoint
/// is intended as a debug helper reachable from anywhere. The other ban
/// endpoints (`/bans.bin` snapshot, `/ws` upgrade) remain whitelisted
/// because they're bot-init paths, not public diagnostics.
///   GET /bans/<wallet_pubkey>
///   200 → {"wallet":..., "banned_until_ms":..., "remaining_secs":..., "reason":...}
///   404 → "not banned"
async fn serve_ban_lookup(
    State(state): State<AppState>,
    Path(wallet): Path<String>,
) -> Response {
    let wallet_pk: solana_sdk::pubkey::Pubkey = match wallet.parse() {
        Ok(pk) => pk,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid pubkey: {e}"),
            )
                .into_response()
        }
    };
    let entry = match state.bans.lookup(&wallet_pk) {
        Ok(opt) => opt,
        Err(e) => {
            tracing::error!("ban lookup failed for {wallet_pk}: {e:#}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };
    match entry {
        Some(e) => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let remaining_secs = e.banned_until_ms.saturating_sub(now_ms) / 1000;
            Json(json!({
                "wallet": wallet,
                "banned_until_ms": e.banned_until_ms,
                "remaining_secs": remaining_secs,
                "reason": e.reason,
            }))
            .into_response()
        }
        None => (StatusCode::NOT_FOUND, "not banned").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct BlockDetailQuery {
    /// Pool pubkey we tried to buy on. Required — the resolver filters the
    /// block down to PumpFun BUYs targeting this pool.
    pool: String,
}

/// `GET /block-detail/<opp_sig>?pool=<pubkey>` — dashboard history-row
/// dropdown. Cache hit → 200 JSON `BlockDetail`. Cache miss → 202 with an
/// empty body so the dashboard knows to poll. Resolution is auto-kicked
/// when the bot reports `position_opened` / `position_failed`, so a typical
/// click ~5-10s after the row lands hits the cache directly.
///
/// IP whitelisted — same list as `/positions.jsonl`.
async fn serve_block_detail(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Path(opp_sig): Path<String>,
    Query(q): Query<BlockDetailQuery>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting block-detail from non-whitelisted ip {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    if opp_sig.is_empty() || q.pool.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing opp_sig or pool").into_response();
    }
    if let Some(d) = state.block_details.get_cached(&opp_sig, &q.pool) {
        return Json(d).into_response();
    }
    // Not in cache yet — kick off resolution in the background so a later
    // poll lands on a warm entry. Bot-driven resolution already runs on
    // every Opened/Failed; this branch covers click-through on legacy rows
    // whose Opened predated this feature.
    let store = state.block_details.clone();
    let opp = opp_sig.clone();
    let pool = q.pool.clone();
    tokio::spawn(async move {
        let _ = store.resolve(&opp, &pool).await;
    });
    (StatusCode::ACCEPTED, Json(serde_json::json!({"status": "pending"}))).into_response()
}

/// `GET /` — serve the embedded HTML UI for sig-trace search. Open to any IP
/// (matches the search endpoint). Single-page app: search bar + filters,
/// fetches `/sig/:sig` over fetch() to render results client-side.
async fn serve_sig_ui() -> Response {
    const SIG_UI_HTML: &str = include_str!("sig_ui.html");
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(SIG_UI_HTML))
        .expect("build sig_ui body")
}

/// `GET /sig/<sig>` — grep the FR bot's sig-trace files for any line that
/// contains `<sig>` as a substring, return matches as `text/plain` (one line
/// per match, oldest file first, then within-file order).
///
/// Open to any IP (matches the `/bans/<wallet>` debug-endpoint pattern).
/// The trace file is local to this central host — `SIG_TRACE_DIR` env
/// (default `./sig_trace`) must point at the bot's writer directory.
///
/// Search order: rolled files newest→oldest (`sig_trace.1.jsonl` is the
/// most recent rolled, `sig_trace.{MAX_FILES-1}.jsonl` the oldest), then the
/// active `sig_trace.jsonl`. We return events oldest-first by reading rolled
/// files in DESCENDING numeric order, then the active file last.
async fn serve_sig_search(Path(sig): Path<String>) -> Response {
    if sig.len() < 8 {
        return (StatusCode::BAD_REQUEST, "sig must be at least 8 chars").into_response();
    }
    let dir = std::env::var("SIG_TRACE_DIR").unwrap_or_else(|_| "/home/ubuntu/sig_trace".to_owned());
    let dir = std::path::PathBuf::from(dir);
    if !dir.exists() {
        return (StatusCode::NOT_FOUND, "sig_trace dir not found").into_response();
    }

    // Collect files in oldest-first order: .9, .8, ..., .1, then active.
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    for i in (1..10).rev() {
        let p = dir.join(format!("sig_trace.{}.jsonl", i));
        if p.exists() {
            files.push(p);
        }
    }
    let active = dir.join("sig_trace.jsonl");
    if active.exists() {
        files.push(active);
    }
    if files.is_empty() {
        return (StatusCode::NOT_FOUND, "no trace files yet").into_response();
    }

    // Stream matches into a string. For multi-GB files this could be slow;
    // we cap output at 10 MB to keep the response sane.
    const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
    let needle = sig.clone();
    let body_result = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        use std::io::{BufRead, BufReader};
        let mut out = String::new();
        for path in files {
            let f = std::fs::File::open(&path)?;
            let reader = BufReader::new(f);
            for line in reader.lines().map_while(Result::ok) {
                if line.contains(&needle) {
                    out.push_str(&line);
                    out.push('\n');
                    if out.len() >= MAX_BODY_BYTES {
                        out.push_str("...[truncated at 10 MB]\n");
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    })
    .await;

    match body_result {
        Ok(Ok(body)) => {
            if body.is_empty() {
                (StatusCode::NOT_FOUND, format!("no matches for sig={sig}")).into_response()
            } else {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Body::from(body))
                    .expect("build sig response")
            }
        }
        Ok(Err(e)) => {
            tracing::error!("sig search io error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("io: {e}")).into_response()
        }
        Err(e) => {
            tracing::error!("sig search join error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "join failed").into_response()
        }
    }
}

#[derive(Debug, serde::Deserialize, Default)]
struct TimeQuery {
    /// Half-window in seconds. Total searched range is `[t - window, t + window]`.
    /// Default 6 (so a 12 s search) — covers the bot's worst-case batch
    /// drift (5 s) with a 1 s margin on each side. Capped at 300 (5 min).
    window: Option<u64>,
    /// Optional output filter:
    ///   - `triggers` (default) → only lines containing `trigger` (covers
    ///     `[buy-trigger]`, `[shred-buy-trigger]`, `[sell-trigger]`,
    ///     `[time-sell-trigger]`).
    ///   - `all`              → no filter (raw firehose for the window).
    /// Most ad-hoc queries want triggers; the firehose is huge.
    filter: Option<String>,
}

/// `GET /time/<HH:MM:SS>?window=<seconds>` — return all trace lines whose
/// `ts_ms=` prefix falls within `[t - window, t + window]` (UTC, today).
/// Default window = 5 s. Open to any IP.
///
/// Useful for finding the opportunity tx that *triggered* a buy when the
/// buy itself failed and didn't make it into journalctl. Drop in the
/// approximate UTC time of the failure and grep the file for any
/// `[buy-trigger]` / `[shred-buy-trigger]` events nearby.
async fn serve_time_search(
    Path(time_str): Path<String>,
    Query(q): Query<TimeQuery>,
) -> Response {
    // Parse `HH:MM:SS` (UTC, today). Allow `HH:MM` too — assume 00 seconds.
    let parts: Vec<&str> = time_str.split(':').collect();
    let (h, m, s) = match parts.as_slice() {
        [h, m] => match (h.parse::<u32>(), m.parse::<u32>()) {
            (Ok(h), Ok(m)) => (h, m, 0u32),
            _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
        },
        [h, m, s] => match (h.parse::<u32>(), m.parse::<u32>(), s.parse::<u32>()) {
            (Ok(h), Ok(m), Ok(s)) => (h, m, s),
            _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
        },
        _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
    };
    if h >= 24 || m >= 60 || s >= 60 {
        return (StatusCode::BAD_REQUEST, "out of range").into_response();
    }

    // Today's UTC midnight + (h, m, s) → epoch ms.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let day_ms = 86_400_000u64;
    let today_midnight_ms = (now_ms / day_ms) * day_ms;
    let target_ms = today_midnight_ms
        + (h as u64 * 3_600_000)
        + (m as u64 * 60_000)
        + (s as u64 * 1_000);

    let window_secs = q.window.unwrap_or(6).min(300);
    let lo = target_ms.saturating_sub(window_secs * 1000);
    let hi = target_ms.saturating_add(window_secs * 1000);

    // Default filter: triggers only. `?filter=all` to disable.
    let triggers_only = match q.filter.as_deref() {
        Some("all") => false,
        _ => true, // default + "triggers" + anything else → triggers
    };

    let dir = std::env::var("SIG_TRACE_DIR").unwrap_or_else(|_| "/home/ubuntu/sig_trace".to_owned());
    let dir = std::path::PathBuf::from(dir);
    if !dir.exists() {
        return (StatusCode::NOT_FOUND, "sig_trace dir not found").into_response();
    }

    let mut files: Vec<std::path::PathBuf> = Vec::new();
    for i in (1..10).rev() {
        let p = dir.join(format!("sig_trace.{}.jsonl", i));
        if p.exists() {
            files.push(p);
        }
    }
    let active = dir.join("sig_trace.jsonl");
    if active.exists() {
        files.push(active);
    }
    if files.is_empty() {
        return (StatusCode::NOT_FOUND, "no trace files yet").into_response();
    }

    const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
    let body_result = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        use std::io::{BufRead, BufReader};
        let mut out = String::new();
        for path in files {
            let f = std::fs::File::open(&path)?;
            let reader = BufReader::new(f);
            for line in reader.lines().map_while(Result::ok) {
                // Lines start with `ts_ms=<digits> ` (writer-prepended).
                // Strip `ts_ms=`, take digits up to next space, parse u64.
                let Some(rest) = line.strip_prefix("ts_ms=") else { continue };
                let Some(sp) = rest.find(' ') else { continue };
                let Ok(ts) = rest[..sp].parse::<u64>() else { continue };
                if ts >= lo && ts <= hi {
                    if triggers_only && !line.contains("trigger") {
                        continue;
                    }
                    out.push_str(&line);
                    out.push('\n');
                    if out.len() >= MAX_BODY_BYTES {
                        out.push_str("...[truncated at 10 MB]\n");
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    })
    .await;

    match body_result {
        Ok(Ok(body)) => {
            if body.is_empty() {
                (
                    StatusCode::NOT_FOUND,
                    format!(
                        "no lines in [{} \u{00b1} {}s] (target_ms={target_ms})",
                        time_str, window_secs
                    ),
                )
                    .into_response()
            } else {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Body::from(body))
                    .expect("build time response")
            }
        }
        Ok(Err(e)) => {
            tracing::error!("time search io error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("io: {e}")).into_response()
        }
        Err(e) => {
            tracing::error!("time search join error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "join failed").into_response()
        }
    }
}

async fn upgrade(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if !state.whitelist.contains(&addr.ip()) {
        let ip = addr.ip();
        tracing::warn!("rejecting ws upgrade from non-whitelisted ip {ip}");
        println!("[whitelist] reject WS upgrade from {ip}");
        return (StatusCode::FORBIDDEN, "not whitelisted").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

async fn handle_socket(socket: WebSocket, addr: SocketAddr, state: AppState) {
    tracing::info!("ws client connected: {addr}");
    let (mut sender, mut receiver) = socket.split();

    let mut rx = state.tx.subscribe();

    let pools: Vec<PoolDoc> = match state.repo.load_all_confirmed().await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("failed to load pools for init: {e:#}");
            return;
        }
    };
    // Filter init to "fresh" pools only — younger than INIT_POOL_MAX_AGE_MS.
    // Older pools stay in Mongo (creator-drift poll, etc.) but are not
    // shipped to bots, so they won't be traded. Pools with unknown age
    // (`pair_created_at_ms == None` after the startup backfill) are also
    // excluded — we can't verify they're fresh, so conservatively drop.
    //
    // `is_unique == true` bypasses the age check entirely — used for
    // long-lived high-value pools we want to keep shipping past the
    // freshness window.
    use crate::config::POOL_MAX_AGE_MS as INIT_POOL_MAX_AGE_MS;
    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let total = pools.len();
    let pools: Vec<PoolDoc> = pools
        .into_iter()
        .filter(|p| {
            // Banned pools are never shipped (defensive — load_all_confirmed
            // already excludes them, but guard here too).
            if p.disabled {
                return false;
            }
            if p.is_unique {
                return true;
            }
            p.pair_created_at_ms
                .map(|c| now_ms.saturating_sub(c) < INIT_POOL_MAX_AGE_MS)
                .unwrap_or(false)
        })
        .collect();
    println!(
        "[ws] init for {addr}: {} of {total} pools admitted (7-day window OR is_unique)",
        pools.len()
    );
    let positions_payload = state.positions.current_open();
    let init_payload = json!({
        "type": "init",
        "pools": pools,
        "positions": positions_payload,
    });
    match serde_json::to_string(&init_payload) {
        Ok(payload) => {
            if sender.send(Message::Text(payload)).await.is_err() {
                return;
            }
        }
        Err(e) => {
            tracing::error!("failed to serialize init: {e:#}");
            return;
        }
    }

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(m) => match serde_json::to_string(&m) {
                    Ok(payload) => {
                        if sender.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => tracing::error!("failed to serialize broadcast msg: {e:#}"),
                },
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("ws client {addr} lagged, dropped {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            inbound = receiver.next() => match inbound {
                Some(Ok(Message::Text(t))) => match serde_json::from_str::<ClientMsg>(&t) {
                    Ok(ClientMsg::DiscoveredPool { pool }) => {
                        let _ = state.discover_tx.send(pool);
                    }
                    Ok(ClientMsg::PositionOpened(r)) => {
                        tracing::info!(
                            "[position_opened] pool={} amt={} buy_price={} loc={} sig={} opp={}",
                            r.pool, r.token_amount, r.buy_price_sol, r.landed_location_idx, r.sig, r.opportunity_sig
                        );
                        // Persist and broadcast IMMEDIATELY (leader=None).
                        // Both the history JSONL and the live dashboard
                        // must see `opened` before any following `closed`
                        // for the same pool — a fast TP that completes
                        // inside the ~5s leader RPC window would otherwise
                        // strand the row as an orphan close (in the file)
                        // or worse, re-create a stale `central_positions`
                        // entry on the bot (live page would show a held
                        // row that's already sold).
                        let opp_sig = r.opportunity_sig.clone();
                        state.positions.record_open(r.clone(), None);
                        let _ = state.tx.send(ServerMsg::PositionOpened {
                            pool: r.pool.clone(),
                            token_amount: r.token_amount,
                            buy_price_sol: r.buy_price_sol,
                            landed_location_idx: r.landed_location_idx,
                            sig: r.sig.clone(),
                            ts_ms: r.ts_ms,
                            landed_path: r.landed_path,
                            token_name: r.token_name.clone(),
                            token_symbol: r.token_symbol.clone(),
                            dump_pct: r.dump_pct,
                            opportunity_sig: opp_sig.clone(),
                            process_us: r.process_us,
                            leader: None,
                            cost_lamports: r.cost_lamports,
                            buy_size_lamports: r.buy_size_lamports,
                            liquidity_usd: r.liquidity_usd,
                            observed_slot: r.observed_slot,
                        });
                        // Resolve leader async; emit a follow-up
                        // `leader_resolved` JSONL line + WS broadcast that
                        // patches the matching `central_positions` row on
                        // the bot. The bot never re-inserts: if the row
                        // was already removed by `position_closed`, the
                        // `leader_resolved` message is a no-op.
                        let leaders = state.leaders.clone();
                        let positions = state.positions.clone();
                        let bcast = state.tx.clone();
                        let opp_for_leader = opp_sig.clone();
                        tokio::spawn(async move {
                            let Some(leader) = resolve_leader(&leaders, &opp_for_leader).await
                            else {
                                return;
                            };
                            positions
                                .record_leader_resolved(opp_for_leader.clone(), leader.clone());
                            let _ = bcast.send(ServerMsg::LeaderResolved {
                                opportunity_sig: opp_for_leader,
                                leader,
                            });
                        });
                        // Resolve block-detail (competitor buys in same
                        // block) async — warms the sled cache so the
                        // dashboard's row-dropdown click hits a ready
                        // entry. Failure is silent; the dashboard
                        // re-kicks on its own GET.
                        if !opp_sig.is_empty() {
                            let store = state.block_details.clone();
                            let pool = r.pool.clone();
                            tokio::spawn(async move {
                                let _ = store.resolve(&opp_sig, &pool).await;
                            });
                        }
                    }
                    Ok(ClientMsg::AltsUnknown { tables }) => {
                        let mut parsed: Vec<solana_sdk::pubkey::Pubkey> = Vec::with_capacity(tables.len());
                        for s in &tables {
                            match s.parse() {
                                Ok(pk) => parsed.push(pk),
                                Err(e) => tracing::warn!("alts_unknown bad pubkey {s}: {e}"),
                            }
                        }
                        if !parsed.is_empty() {
                            state.alts.handle_unknown(parsed);
                        }
                    }
                    Ok(ClientMsg::OppCheck { sig, dumper_pk, dumper_ata, amount_in }) => {
                        match (dumper_pk.parse::<solana_sdk::pubkey::Pubkey>(),
                               dumper_ata.parse::<solana_sdk::pubkey::Pubkey>()) {
                            (Ok(dpk), Ok(dat)) => {
                                state.bans.handle_opp_check(sig, dpk, dat, amount_in);
                            }
                            (Err(e), _) => tracing::warn!("opp_check bad dumper_pk {dumper_pk}: {e}"),
                            (_, Err(e)) => tracing::warn!("opp_check bad dumper_ata {dumper_ata}: {e}"),
                        }
                    }
                    Ok(ClientMsg::SigDispatched { prefix }) => {
                        // Sanity-bound to the 8-char base58 prefix the bot
                        // emits. Anything else is malformed — drop silently
                        // to avoid log spam from a bad actor or stale code.
                        if prefix.len() != 8
                            || !prefix.chars().all(|c| c.is_ascii_alphanumeric())
                        {
                            tracing::warn!("sig_dispatched malformed prefix: {prefix:?}");
                            continue;
                        }
                        // Fan out to every connected bot. Each bot's
                        // `handle_runtime_msg` calls `intern_dispatched_sig`
                        // (non-broadcasting) so the inbound message doesn't
                        // recursively echo. Includes the originating bot;
                        // its local `record_dispatched_sig` already
                        // inserted the prefix, so the re-insert is a
                        // harmless no-op (HashMap idempotent on same value).
                        let _ = state.tx.send(ServerMsg::SigDispatched { prefix });
                    }
                    Ok(ClientMsg::PositionClosed(r)) => {
                        tracing::info!(
                            "[position_closed] pool={} sig={} sell_price={} tokens_sold={} sol_received_lamports={}",
                            r.pool, r.sig, r.sell_price_sol, r.tokens_sold, r.sol_received_lamports
                        );
                        let broadcast = ServerMsg::PositionClosed {
                            pool: r.pool.clone(),
                            sig: r.sig.clone(),
                            ts_ms: r.ts_ms,
                            sell_price_sol: r.sell_price_sol,
                            tokens_sold: r.tokens_sold,
                            sol_received_lamports: r.sol_received_lamports,
                            landed_location_idx: r.landed_location_idx,
                            landed_path: r.landed_path,
                        };
                        state.positions.record_close(r);
                        let _ = state.tx.send(broadcast);
                    }
                    Ok(ClientMsg::PositionFailed(mut r)) => {
                        tracing::info!(
                            "[position_failed] pool={} sig={} loc_reported={} dump_pct={:.3} opp={}",
                            r.pool, r.sig, r.landed_location_idx, r.dump_pct, r.opportunity_sig
                        );
                        // Two parallel RPC-driven resolves before persist:
                        //   * lane (from our failed tx's CU price) —
                        //     overwrites the bot's FR-biased report.
                        //   * leader (from the opp tx's slot) — joins
                        //     against the validators CSV.
                        // RPC failures → u8::MAX / leader=None so the
                        // dashboard renders `—` instead of misattributing.
                        let lanes = state.lanes.clone();
                        let leaders = state.leaders.clone();
                        let positions = state.positions.clone();
                        let bcast = state.tx.clone();
                        // Same shape as the opened-path: warm the block-detail
                        // cache so the dashboard dropdown loads instantly when
                        // the user clicks a failed-buy row a few seconds later.
                        if !r.opportunity_sig.is_empty() {
                            let store = state.block_details.clone();
                            let opp = r.opportunity_sig.clone();
                            let pool = r.pool.clone();
                            tokio::spawn(async move {
                                let _ = store.resolve(&opp, &pool).await;
                            });
                        }
                        tokio::spawn(async move {
                            let lane_fut = lanes.resolve(&r.sig);
                            let leader_fut = resolve_leader(&leaders, &r.opportunity_sig);
                            let (lane_res, leader) = tokio::join!(lane_fut, leader_fut);
                            let mut actual_fee_lamports: u64 = 0;
                            match lane_res {
                                LaneResolution::Resolved { lane, actual_fee_lamports: fee } => {
                                    let (path, loc) = decode_lane(lane);
                                    r.landed_location_idx = loc;
                                    r.landed_path = path;
                                    actual_fee_lamports = fee;
                                    tracing::info!(
                                        "[position_failed] resolved sig={} lane={} loc={} path={} fee_lamports={}",
                                        r.sig, lane, loc, path, fee
                                    );
                                }
                                LaneResolution::Unknown => {
                                    r.landed_location_idx = u8::MAX;
                                    r.landed_path = u8::MAX;
                                }
                            }
                            r.actual_fee_lamports = actual_fee_lamports;
                            let broadcast = ServerMsg::PositionFailed {
                                pool: r.pool.clone(),
                                sig: r.sig.clone(),
                                ts_ms: r.ts_ms,
                                landed_location_idx: r.landed_location_idx,
                                landed_path: r.landed_path,
                                dump_pct: r.dump_pct,
                                opportunity_sig: r.opportunity_sig.clone(),
                                token_name: r.token_name.clone(),
                                token_symbol: r.token_symbol.clone(),
                                buy_size_lamports: r.buy_size_lamports,
                                expected_cost_lamports: r.expected_cost_lamports,
                                actual_fee_lamports,
                                leader: leader.clone(),
                                liquidity_usd: r.liquidity_usd,
                                process_us: r.process_us,
                                observed_slot: r.observed_slot,
                            };
                            positions.record_failed(r, leader);
                            let _ = bcast.send(broadcast);
                        });
                    }
                    Err(e) => {
                        tracing::warn!("ws inbound parse error from {addr}: {e}; payload={t}");
                    }
                },
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => {
                    tracing::warn!("ws recv error from {addr}: {e}");
                    break;
                }
                _ => {}
            }
        }
    }
    tracing::info!("ws client disconnected: {addr}");
}
