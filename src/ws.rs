use std::{collections::HashSet, net::{IpAddr, SocketAddr}, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, mpsc::UnboundedSender};
use tokio_util::io::ReaderStream;

use crate::{
    alts::AltStore,
    bans::BansStore,
    mongo::Repo,
    pool::{PoolAccounts, PoolDoc},
    positions::{ClosedReport, OpenedReport, Positions},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    NewPool {
        pool: String,
        #[serde(flatten)]
        accounts: PoolAccounts,
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
    };
    let app = Router::new()
        .route("/ws", get(upgrade))
        .route("/positions.jsonl", get(serve_positions_log))
        .route("/alts.bin", get(serve_alts_snapshot))
        .route("/bans.bin", get(serve_bans_snapshot))
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

/// IP-whitelisted file download. Streams `positions.jsonl` from disk so
/// dashboard processes on each location box can read it without needing
/// shared filesystem access. Same whitelist as the WS upgrade.
async fn serve_positions_log(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    if !state.whitelist.contains(&addr.ip()) {
        tracing::warn!("rejecting positions.jsonl from non-whitelisted ip {}", addr.ip());
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
        tracing::warn!("rejecting alts.bin from non-whitelisted ip {}", addr.ip());
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
        tracing::warn!("rejecting bans.bin from non-whitelisted ip {}", addr.ip());
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

async fn upgrade(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if !state.whitelist.contains(&addr.ip()) {
        tracing::warn!("rejecting ws upgrade from non-whitelisted ip {}", addr.ip());
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
                            "[position_opened] pool={} amt={} buy_price={} loc={} sig={}",
                            r.pool, r.token_amount, r.buy_price_sol, r.landed_location_idx, r.sig
                        );
                        let broadcast = ServerMsg::PositionOpened {
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
                            opportunity_sig: r.opportunity_sig.clone(),
                            process_us: r.process_us,
                        };
                        state.positions.record_open(r);
                        let _ = state.tx.send(broadcast);
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
