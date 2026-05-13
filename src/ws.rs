use std::{collections::HashSet, net::{IpAddr, SocketAddr}, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, Path, Query, State,
    },
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
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
    };
    let app = Router::new()
        .route("/", get(serve_sig_ui))
        .route("/ws", get(upgrade))
        .route("/positions.jsonl", get(serve_positions_log))
        .route("/alts.bin", get(serve_alts_snapshot))
        .route("/bans.bin", get(serve_bans_snapshot))
        .route("/bans/:wallet", get(serve_ban_lookup))
        .route("/sig/:sig", get(serve_sig_search))
        .route("/time/:time", get(serve_time_search))
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
    // `is_unique == Some(true)` bypasses the age check entirely — used for
    // long-lived high-value pools we want to keep shipping past the
    // freshness window.
    const INIT_POOL_MAX_AGE_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let total = pools.len();
    let pools: Vec<PoolDoc> = pools
        .into_iter()
        .filter(|p| {
            if p.is_unique == Some(true) {
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
                        tokio::spawn(async move {
                            let Some(leader) = resolve_leader(&leaders, &opp_sig).await else {
                                return;
                            };
                            positions.record_leader_resolved(opp_sig.clone(), leader.clone());
                            let _ = bcast.send(ServerMsg::LeaderResolved {
                                opportunity_sig: opp_sig,
                                leader,
                            });
                        });
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
