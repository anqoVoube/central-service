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

use crate::validators::LeaderInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub pool: String,
    pub token_amount: u64,
    pub buy_price_sol: f64,
    pub landed_location_idx: u8,
    pub ts_ms: u64,
    /// Buy-side tx signature. Empty string for legacy lines (pre-this-change)
    /// where the field wasn't recorded.
    #[serde(default)]
    pub sig: String,
    /// 0=SHREDS, 1=GEYSER, 2=DASHBOARD. Tens digit of the on-chain CU
    /// price lane (`globals::decode_lane`). Legacy lines default to SHREDS.
    #[serde(default)]
    pub landed_path: u8,
    /// Display name from Dexscreener. Empty for legacy / no-data pools.
    #[serde(default)]
    pub token_name: Option<String>,
    /// Display symbol from Dexscreener.
    #[serde(default)]
    pub token_symbol: Option<String>,
    /// Price drop % at trigger time (negative number, e.g. -3.52). 0.0 if
    /// not captured (legacy lines or manual buys, of which we have none today).
    #[serde(default)]
    pub dump_pct: f64,
    /// Signature of the external tx that triggered our buy (the dumper).
    /// Empty for legacy / manual.
    #[serde(default)]
    pub opportunity_sig: String,
    /// Microseconds from message arrival to `Dispatcher::fire` entry on the
    /// buy. 0 for legacy lines or unmeasured fires.
    #[serde(default)]
    pub process_us: u32,
    /// Resolved leader of the opportunity tx's slot (the dumper's
    /// signature). `None` when the RPC resolve failed, the opp sig never
    /// confirmed, or the leader pubkey isn't in the validators CSV.
    #[serde(default)]
    pub leader: Option<LeaderInfo>,
    /// Authoritative on-chain cost of the buy in lamports: native SOL paid
    /// (fee + priority + tip + any system rent) + WSOL spent on the swap.
    /// Computed by the bot from `tx.meta` balance deltas at landing time
    /// (mirrors P6's `sol_received_lamports` recv-truth pattern). 0 for
    /// legacy lines; dashboard then falls back to its estimate.
    #[serde(default)]
    pub cost_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenedReport {
    pub pool: String,
    pub token_amount: u64,
    pub buy_price_sol: f64,
    pub landed_location_idx: u8,
    pub sig: String,
    pub ts_ms: u64,
    #[serde(default)]
    pub landed_path: u8,
    #[serde(default)]
    pub token_name: Option<String>,
    #[serde(default)]
    pub token_symbol: Option<String>,
    #[serde(default)]
    pub dump_pct: f64,
    #[serde(default)]
    pub opportunity_sig: String,
    #[serde(default)]
    pub process_us: u32,
    /// See `OpenPosition::cost_lamports`. 0 for legacy bots.
    #[serde(default)]
    pub cost_lamports: u64,
}

/// A buy attempt that landed on chain but reverted — the wallet paid the
/// priority fee + tip but no position was opened. Reported by the location
/// that owned the matching `Holding::BuyPending` at the moment of failure.
/// Informational only: dashboard renders these in history alongside
/// succeeded trades for visibility, not for trading-state decisions.
#[derive(Debug, Clone, Deserialize)]
pub struct FailedReport {
    pub pool: String,
    pub sig: String,
    pub ts_ms: u64,
    #[serde(default = "unknown_location")]
    pub landed_location_idx: u8,
    #[serde(default)]
    pub landed_path: u8,
    #[serde(default)]
    pub dump_pct: f64,
    #[serde(default)]
    pub opportunity_sig: String,
    #[serde(default)]
    pub token_name: Option<String>,
    #[serde(default)]
    pub token_symbol: Option<String>,
    /// WSOL lamports the buy ix would have spent if it had succeeded. 0
    /// for legacy / pre-snapshot reports.
    #[serde(default)]
    pub buy_size_lamports: u64,
    /// Approximate fee+tip lamports paid for the landed-and-reverted tx
    /// (whichever of the 3 fan-out variants won the nonce race). Computed
    /// bot-side from `buy_size_lamports` via the bucket-rate table —
    /// **overstates** the real cost on failed buys (the tip ix reverts with
    /// the tx). Kept as a fallback; prefer `actual_fee_lamports`.
    #[serde(default)]
    pub expected_cost_lamports: u64,
    /// On-chain `meta.fee` from `getTransaction` (sig + priority fee only,
    /// no tip). Set by central during the post-fire RPC resolve. 0 when
    /// resolve failed.
    #[serde(default)]
    pub actual_fee_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClosedReport {
    pub pool: String,
    pub sig: String,
    pub ts_ms: u64,
    /// Lamports per raw-token at the moment the sell landed. 0.0 if missing
    /// (older bot that doesn't carry it).
    #[serde(default)]
    pub sell_price_sol: f64,
    /// Raw-unit count of tokens that left the wallet ATA on the sell. 0 if
    /// missing.
    #[serde(default)]
    pub tokens_sold: u64,
    /// Authoritative SOL we actually received (lamports), read from the
    /// wallet's WSOL ATA pre→post delta on the sell tx. `tokens_sold ×
    /// sell_price_sol` overstates this by AMM fees + CP curvature; this
    /// field is the truth. 0 if older bot didn't report it (dashboard then
    /// falls back to the spot-based estimate).
    #[serde(default)]
    pub sol_received_lamports: u64,
    /// Location whose tx landed the sell. `u8::MAX` if missing.
    #[serde(default = "unknown_location")]
    pub landed_location_idx: u8,
    /// 0=SHREDS, 1=GEYSER, 2=DASHBOARD. Tens digit of the on-chain CU
    /// price lane (`globals::decode_lane`). Legacy lines default to SHREDS.
    #[serde(default)]
    pub landed_path: u8,
}

fn unknown_location() -> u8 { u8::MAX }

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
        #[serde(default)]
        landed_path: u8,
        #[serde(default)]
        token_name: Option<String>,
        #[serde(default)]
        token_symbol: Option<String>,
        #[serde(default)]
        dump_pct: f64,
        #[serde(default)]
        opportunity_sig: String,
        #[serde(default)]
        process_us: u32,
        /// Leader of the opportunity tx's slot, joined against the
        /// validators CSV by central. `None` when unresolved or the
        /// validator isn't in the CSV.
        #[serde(default)]
        leader: Option<LeaderInfo>,
        /// See `OpenPosition::cost_lamports`. 0 for legacy lines.
        #[serde(default)]
        cost_lamports: u64,
    },
    Closed {
        ts_ms: u64,
        pool: String,
        sig: String,
        #[serde(default)]
        sell_price_sol: f64,
        #[serde(default)]
        tokens_sold: u64,
        /// Wallet WSOL ATA delta in lamports — authoritative SOL received.
        /// 0 for legacy lines that didn't carry it.
        #[serde(default)]
        sol_received_lamports: u64,
        #[serde(default = "unknown_location")]
        landed_location_idx: u8,
        #[serde(default)]
        landed_path: u8,
    },
    /// A buy that landed on chain but reverted (paid fee+tip, no tokens).
    /// Replay treats this as a no-op for the open-position map; rendered
    /// in dashboard history alongside succeeded trades.
    Failed {
        ts_ms: u64,
        pool: String,
        sig: String,
        #[serde(default = "unknown_location")]
        landed_location_idx: u8,
        #[serde(default)]
        landed_path: u8,
        #[serde(default)]
        dump_pct: f64,
        #[serde(default)]
        opportunity_sig: String,
        #[serde(default)]
        token_name: Option<String>,
        #[serde(default)]
        token_symbol: Option<String>,
        #[serde(default)]
        buy_size_lamports: u64,
        #[serde(default)]
        expected_cost_lamports: u64,
        /// `meta.fee` from `getTransaction` — sig + priority fee, no tip.
        /// 0 for legacy lines or unresolved sigs.
        #[serde(default)]
        actual_fee_lamports: u64,
        #[serde(default)]
        leader: Option<LeaderInfo>,
    },
    /// Late-arriving leader info, emitted after central's async resolve
    /// completes. Keyed by `opportunity_sig`. The dashboard merges this
    /// into the matching `Opened` row at parse time. Decoupled from
    /// `Opened` writes so a fast TP that closes before the leader RPC
    /// returns doesn't strand the open as an orphan in the file.
    LeaderResolved {
        ts_ms: u64,
        opportunity_sig: String,
        leader: LeaderInfo,
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

    pub fn record_open(&self, r: OpenedReport, leader: Option<LeaderInfo>) {
        let pos = OpenPosition {
            pool: r.pool.clone(),
            token_amount: r.token_amount,
            buy_price_sol: r.buy_price_sol,
            landed_location_idx: r.landed_location_idx,
            ts_ms: r.ts_ms,
            sig: r.sig.clone(),
            landed_path: r.landed_path,
            token_name: r.token_name.clone(),
            token_symbol: r.token_symbol.clone(),
            dump_pct: r.dump_pct,
            opportunity_sig: r.opportunity_sig.clone(),
            process_us: r.process_us,
            leader: leader.clone(),
            cost_lamports: r.cost_lamports,
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
            landed_path: r.landed_path,
            token_name: r.token_name,
            token_symbol: r.token_symbol,
            dump_pct: r.dump_pct,
            opportunity_sig: r.opportunity_sig,
            process_us: r.process_us,
            leader,
            cost_lamports: r.cost_lamports,
        });
    }

    pub fn record_close(&self, r: ClosedReport) {
        self.inner.open.lock().expect("positions mutex poisoned")
            .remove(&r.pool);
        let _ = self.inner.writer.send(LogEvent::Closed {
            ts_ms: r.ts_ms,
            pool: r.pool,
            sig: r.sig,
            sell_price_sol: r.sell_price_sol,
            tokens_sold: r.tokens_sold,
            sol_received_lamports: r.sol_received_lamports,
            landed_location_idx: r.landed_location_idx,
            landed_path: r.landed_path,
        });
    }

    /// Append a `leader_resolved` event. Decoupled from `record_open` so
    /// the open row hits the jsonl before any close that may follow.
    pub fn record_leader_resolved(&self, opportunity_sig: String, leader: LeaderInfo) {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let _ = self.inner.writer.send(LogEvent::LeaderResolved {
            ts_ms,
            opportunity_sig,
            leader,
        });
    }

    /// Append a failed-buy event. Doesn't touch the open-position map —
    /// failed buys never opened a position, so there's nothing to track.
    pub fn record_failed(&self, r: FailedReport, leader: Option<LeaderInfo>) {
        let _ = self.inner.writer.send(LogEvent::Failed {
            ts_ms: r.ts_ms,
            pool: r.pool,
            sig: r.sig,
            landed_location_idx: r.landed_location_idx,
            landed_path: r.landed_path,
            dump_pct: r.dump_pct,
            opportunity_sig: r.opportunity_sig,
            token_name: r.token_name,
            token_symbol: r.token_symbol,
            buy_size_lamports: r.buy_size_lamports,
            expected_cost_lamports: r.expected_cost_lamports,
            actual_fee_lamports: r.actual_fee_lamports,
            leader,
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
                sig,
                landed_path,
                token_name,
                token_symbol,
                dump_pct,
                opportunity_sig,
                process_us,
                leader,
                cost_lamports,
            } => {
                out.insert(
                    pool.clone(),
                    OpenPosition {
                        pool,
                        token_amount,
                        buy_price_sol,
                        landed_location_idx,
                        ts_ms,
                        sig,
                        landed_path,
                        token_name,
                        token_symbol,
                        dump_pct,
                        opportunity_sig,
                        process_us,
                        leader,
                        cost_lamports,
                    },
                );
            }
            LogEvent::Closed { pool, .. } => {
                out.remove(&pool);
            }
            LogEvent::Failed { .. } => {
                // Failed buys don't open positions — nothing to replay
                // into the in-memory open-position map.
            }
            LogEvent::LeaderResolved { .. } => {
                // Pure dashboard hint; not relevant to the in-memory open
                // map. Dashboard re-parses the file and merges separately.
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
