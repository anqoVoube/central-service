//! Central position registry. Append-only JSONL on disk; in-memory map of
//! open positions sent in the WS `init` payload to every location.
//!
//! Lander reports `position_opened` / `position_closed` over WS; we record
//! both events and broadcast nothing back at runtime — locations sync state
//! via on-chain observation. The disk file feeds dashboard / future analytics.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use std::sync::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub pool: String,
    pub token_amount: u64,
    pub buy_price_sol: f64,
    pub landed_location_idx: u8,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenedReport {
    pub pool: String,
    pub token_amount: u64,
    pub buy_price_sol: f64,
    pub landed_location_idx: u8,
    pub sig: String,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClosedReport {
    pub pool: String,
    pub sig: String,
    pub ts_ms: u64,
}

/// On-disk event shape. `kind` discriminator keeps the file forward-compatible.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LogEvent {
    Opened {
        ts_ms: u64,
        pool: String,
        token_amount: u64,
        buy_price_sol: f64,
        landed_location_idx: u8,
        sig: String,
    },
    Closed {
        ts_ms: u64,
        pool: String,
        sig: String,
    },
}

/// Owns the in-memory map of open positions and the writer task channel.
/// Cheap to clone (just an `Arc`).
#[derive(Clone)]
pub struct Positions {
    inner: Arc<Inner>,
}

struct Inner {
    open: Mutex<HashMap<String, OpenPosition>>,
    writer: mpsc::UnboundedSender<LogEvent>,
}

impl Positions {
    /// Replay the log to reconstruct the open-position map, then spawn the
    /// background writer task that owns the file handle.
    pub async fn load_and_spawn(path: PathBuf) -> anyhow::Result<Self> {
        let open = replay(&path);
        tracing::info!(
            "positions: loaded {} open positions from {}",
            open.len(),
            path.display()
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let path_clone = path.clone();
        tokio::spawn(async move {
            writer_loop(path_clone, rx).await;
        });
        Ok(Self {
            inner: Arc::new(Inner {
                open: Mutex::new(open),
                writer: tx,
            }),
        })
    }

    pub fn current_open(&self) -> Vec<OpenPosition> {
        self.inner.open.lock().expect("positions mutex poisoned")
            .values().cloned().collect()
    }

    pub fn record_open(&self, r: OpenedReport) {
        let pos = OpenPosition {
            pool: r.pool.clone(),
            token_amount: r.token_amount,
            buy_price_sol: r.buy_price_sol,
            landed_location_idx: r.landed_location_idx,
            ts_ms: r.ts_ms,
        };
        self.inner.open.lock().expect("positions mutex poisoned")
            .insert(r.pool.clone(), pos);
        let _ = self.inner.writer.send(LogEvent::Opened {
            ts_ms: r.ts_ms,
            pool: r.pool,
            token_amount: r.token_amount,
            buy_price_sol: r.buy_price_sol,
            landed_location_idx: r.landed_location_idx,
            sig: r.sig,
        });
    }

    pub fn record_close(&self, r: ClosedReport) {
        self.inner.open.lock().expect("positions mutex poisoned")
            .remove(&r.pool);
        let _ = self.inner.writer.send(LogEvent::Closed {
            ts_ms: r.ts_ms,
            pool: r.pool,
            sig: r.sig,
        });
    }
}

fn replay(path: &Path) -> HashMap<String, OpenPosition> {
    let mut out = HashMap::new();
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return out,
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ev: LogEvent = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("positions replay: skipping malformed line: {e}");
                continue;
            }
        };
        match ev {
            LogEvent::Opened {
                ts_ms,
                pool,
                token_amount,
                buy_price_sol,
                landed_location_idx,
                ..
            } => {
                out.insert(
                    pool.clone(),
                    OpenPosition {
                        pool,
                        token_amount,
                        buy_price_sol,
                        landed_location_idx,
                        ts_ms,
                    },
                );
            }
            LogEvent::Closed { pool, .. } => {
                out.remove(&pool);
            }
        }
    }
    out
}

async fn writer_loop(path: PathBuf, mut rx: mpsc::UnboundedReceiver<LogEvent>) {
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("positions writer: failed to open {}: {e}", path.display());
            return;
        }
    };
    while let Some(ev) = rx.recv().await {
        let line = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("positions writer: serialize failed: {e}");
                continue;
            }
        };
        if let Err(e) = writeln!(file, "{line}") {
            tracing::error!("positions writer: write failed: {e}");
            continue;
        }
        if let Err(e) = file.sync_all() {
            tracing::error!("positions writer: fsync failed: {e}");
        }
    }
}

#[allow(dead_code)]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
