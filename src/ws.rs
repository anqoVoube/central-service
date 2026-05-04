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
    },
    /// Forwarded to all locations after the lander reports it. Each location
    /// uses this to clear `Holding::Empty`.
    PositionClosed {
        pool: String,
        sig: String,
        ts_ms: u64,
    },
}

/// Inbound from a location. `discovered_pool` is Frankfurt-only;
/// `position_opened` / `position_closed` come from the lander.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    DiscoveredPool { pool: String },
    PositionOpened(OpenedReport),
    PositionClosed(ClosedReport),
}

#[derive(Clone)]
struct AppState {
    repo: Arc<Repo>,
    positions: Positions,
    whitelist: Arc<HashSet<IpAddr>>,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
    positions_log: Arc<PathBuf>,
}

pub async fn serve(
    bind: SocketAddr,
    whitelist_ips: HashSet<IpAddr>,
    repo: Arc<Repo>,
    positions: Positions,
    positions_log: PathBuf,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
) -> anyhow::Result<()> {
    let state = AppState {
        repo,
        positions,
        whitelist: Arc::new(whitelist_ips),
        tx,
        discover_tx,
        positions_log: Arc::new(positions_log),
    };
    let app = Router::new()
        .route("/ws", get(upgrade))
        .route("/positions.jsonl", get(serve_positions_log))
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
                        };
                        state.positions.record_open(r);
                        let _ = state.tx.send(broadcast);
                    }
                    Ok(ClientMsg::PositionClosed(r)) => {
                        tracing::info!(
                            "[position_closed] pool={} sig={}",
                            r.pool, r.sig
                        );
                        let broadcast = ServerMsg::PositionClosed {
                            pool: r.pool.clone(),
                            sig: r.sig.clone(),
                            ts_ms: r.ts_ms,
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
