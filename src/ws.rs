use std::{collections::HashSet, net::{IpAddr, SocketAddr}, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc::UnboundedSender};

use crate::{mongo::Repo, pool::{PoolAccounts, PoolDoc}};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Init {
        pools: Vec<PoolDoc>,
    },
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
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    DiscoveredPool { pool: String },
}

#[derive(Clone)]
struct AppState {
    repo: Arc<Repo>,
    whitelist: Arc<HashSet<IpAddr>>,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
}

pub async fn serve(
    bind: SocketAddr,
    whitelist_ips: HashSet<IpAddr>,
    repo: Arc<Repo>,
    tx: broadcast::Sender<ServerMsg>,
    discover_tx: UnboundedSender<String>,
) -> anyhow::Result<()> {
    let state = AppState {
        repo,
        whitelist: Arc::new(whitelist_ips),
        tx,
        discover_tx,
    };
    let app = Router::new()
        .route("/ws", get(upgrade))
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

    let init = match state.repo.load_all_confirmed().await {
        Ok(pools) => ServerMsg::Init { pools },
        Err(e) => {
            tracing::error!("failed to load pools for init: {e:#}");
            return;
        }
    };
    match serde_json::to_string(&init) {
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
                Some(Ok(Message::Text(t))) => {
                    if let Ok(ClientMsg::DiscoveredPool { pool }) = serde_json::from_str(&t) {
                        let _ = state.discover_tx.send(pool);
                    }
                }
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
