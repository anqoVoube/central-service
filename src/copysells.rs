//! Every EXIT the copy bot made, one row per sell.
//!
//! Separate from `copytrades.rs` on purpose. A trade row holds ONE sell,
//! which was fine while every exit was a full dump — but a mirror scales out,
//! so a position leaves in three or five pieces and only the last of them
//! could ever be recorded. The history page read "open" while the holding fell
//! by a third, and nothing showed what the partial exits had cost or where they
//! had landed.
//!
//! The row carries the same facts the buy side carries: what we bid for
//! position, where we landed in the block, and where the wallet we mirror
//! landed. `buy_sig` ties them back to the trade they belong to.
//!
//! Keyed `ts_ms:sell_sig` so a reverse range scan is newest-first, matching
//! `copytrades.rs` and `orderflow.rs`.

use std::{
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// Cap on stored sells. A position can exit in many pieces, so this is
/// deliberately looser than the trade cap.
const MAX_ROWS: usize = 400_000;

/// One exit.
///
/// NOTE: stored with bincode, which is positional and NOT self-describing —
/// adding a field invalidates every existing row unless a fallback decoder is
/// added alongside, exactly as `copytrades.rs` does.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CopySell {
    pub ts_ms: u64,
    /// The trade this exit belongs to. Several rows can share one.
    pub buy_sig: String,
    pub sell_sig: String,
    pub pool: String,
    pub mint: String,
    /// `mirror` | `timer` | `dump_now` | `manual`.
    pub reason: String,
    pub tokens_sold: u64,
    /// What remains after this exit. Zero means the position closed here.
    pub tokens_left: u64,
    /// Percent of the holding this exit took, at the moment it was taken.
    pub fraction_pct: f64,
    pub closed: bool,

    // ---- the exit race ----
    pub our_slot: u64,
    pub our_index: u64,
    /// Where the mirrored wallet's own sell landed, when there is one.
    pub trader_slot: Option<u64>,
    pub trader_index: Option<u64>,
    pub trader_sig: Option<String>,
    pub trader_wallet: Option<String>,
    /// `before` | `after`, or `None` when there is nothing to be ordered
    /// against — a timer exit has no counterparty.
    pub verdict: Option<String>,

    // ---- what we paid for position ----
    /// Our bid, not the winning rung: the fill arrives as a token delta and
    /// carries no memory of which transaction of the wave won.
    pub tip_lamports: u64,
    pub priority_fee_lamports: u64,
    pub loc: u8,
}

#[derive(Clone)]
pub struct CopySellStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Db,
    /// Cached because a polled endpoint must never scan the tree — `count()`
    /// as an iteration materialises every value and stalls the runtime.
    count: AtomicUsize,
}

impl CopySellStore {
    pub fn open(db_path: &Path) -> anyhow::Result<Self> {
        let db = sled::open(db_path)
            .with_context(|| format!("open copysells db at {}", db_path.display()))?;
        let n = db.len();
        tracing::info!("[copysells] opened at {} ({n} sells)", db_path.display());
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                count: AtomicUsize::new(n),
            }),
        })
    }

    pub fn record(&self, sell: CopySell) {
        let key = format!("{:013}:{}", sell.ts_ms, sell.sell_sig);
        let Ok(bytes) = bincode::serialize(&sell) else {
            tracing::warn!("[copysells] serialize failed for {}", sell.sell_sig);
            return;
        };
        match self.inner.db.insert(key.as_bytes(), bytes) {
            Ok(prev) => {
                if prev.is_none() {
                    let n = self.inner.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if n > MAX_ROWS {
                        self.trim();
                    }
                }
            }
            Err(e) => tracing::warn!("[copysells] store failed: {e:#}"),
        }
    }

    /// Drop the oldest rows back to the cap. Bounded work per call: the range
    /// is walked from the front and stops as soon as the surplus is gone.
    fn trim(&self) {
        let n = self.inner.count.load(std::sync::atomic::Ordering::Relaxed);
        let surplus = n.saturating_sub(MAX_ROWS);
        if surplus == 0 {
            return;
        }
        let keys: Vec<sled::IVec> = self
            .inner
            .db
            .iter()
            .keys()
            .take(surplus)
            .filter_map(Result::ok)
            .collect();
        for k in keys {
            if self.inner.db.remove(&k).is_ok() {
                self.inner
                    .count
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub fn count(&self) -> usize {
        self.inner.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Newest first. Values are only deserialised for the page asked for.
    pub fn page(&self, offset: usize, limit: usize) -> Vec<CopySell> {
        self.inner
            .db
            .iter()
            .rev()
            .filter_map(Result::ok)
            .skip(offset)
            .take(limit)
            .filter_map(|(_, v)| bincode::deserialize::<CopySell>(&v).ok())
            .collect()
    }

    /// Headline numbers for the page. Walks the newest window only — a full
    /// scan on a polled endpoint is what froze the dashboard once already.
    pub fn summary(&self, window: usize) -> serde_json::Value {
        let rows = self.page(0, window);
        let mirror = rows.iter().filter(|r| r.reason == "mirror").count();
        let closed = rows.iter().filter(|r| r.closed).count();
        let before = rows
            .iter()
            .filter(|r| r.verdict.as_deref() == Some("before"))
            .count();
        let after = rows
            .iter()
            .filter(|r| r.verdict.as_deref() == Some("after"))
            .count();
        let bid: u64 = rows
            .iter()
            .map(|r| r.tip_lamports + r.priority_fee_lamports)
            .sum();
        serde_json::json!({
            "total": self.count(),
            "window": rows.len(),
            "mirror": mirror,
            "closed": closed,
            "partial": rows.len().saturating_sub(closed),
            "before": before,
            "after": after,
            "bid_sol": bid as f64 / 1e9,
        })
    }
}
