//! Persistent registry of ORDERFLOW-detected dumps + their on-chain outcome.
//!
//! Bots detect dumps on the Nozomi orderflow stream PRE-BLOCK and report each
//! one over WS (`orderflow_detected`). Central waits for the tx to settle,
//! asks the cluster what happened, and persists the result to sled so the
//! dashboard survives restarts and shows detections from EVERY location — not
//! just the dashboard host (the previous in-memory `state.json` ring was both
//! volatile and single-box).
//!
//! Retention policy (operator decision): keep only txs that actually made it
//! on chain.
//!   • `landed`  — confirmed, no error   → stored
//!   • `failed`  — confirmed, reverted   → stored
//!   • not found — never landed          → DROPPED, not stored, never shown
//! Pre-block detection means most opportunities never land, so storing them
//! would swamp the DB and the UI with noise.
//!
//! Keyed by `ts_ms:sig` so a sled range scan returns newest-last in time
//! order and the reader can just take the tail.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

/// How long to wait before asking the cluster about a reported sig. Orderflow
/// hands us the tx before it's broadcast to a leader, so this must cover
/// propagation + confirmation, not just confirmation.
const STATUS_CHECK_DELAY: Duration = Duration::from_secs(12);

/// Cap on stored rows. Oldest are pruned past this so the DB stays bounded.
const MAX_ROWS: usize = 20_000;

/// Prune only occasionally rather than on every insert.
const PRUNE_EVERY: u64 = 256;

/// One detected dump that reached the chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderflowRow {
    pub ts_ms: u64,
    /// Venue label from the bot's parser: `direct_pump` / `direct_cpmm` /
    /// `jupiter` / `okx` / `dflow` / `axiom`.
    pub venue: String,
    pub sig: String,
    pub pool: String,
    pub dumper: String,
    pub amount_in: u64,
    /// `landed` | `failed` — never "pending"/"not-landed"; those aren't stored.
    pub status: String,
    /// Which bot location reported it (`LOCATION_INDEX`), for attribution.
    pub loc: u8,
}

#[derive(Clone)]
pub struct OrderflowStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    /// Dedup by sig — every location that sees the same dump reports it, and
    /// we only want one RPC + one row.
    in_flight: Mutex<HashSet<String>>,
    rpc_url: String,
    inserts: Mutex<u64>,
}

impl OrderflowStore {
    pub fn open(db_path: &Path, rpc_url: String) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled orderflow db")?;
        let tree = db.open_tree("orderflow").context("open orderflow tree")?;
        let n = tree.iter().count();
        println!("[orderflow-db] opened at {} ({n} rows)", db_path.display());
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                rpc_url,
                inserts: Mutex::new(0),
            }),
        })
    }

    /// Bot reported a detection. Dedups by sig, then resolves + stores in the
    /// background so the WS read loop never blocks.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_detected(
        &self,
        sig: String,
        venue: String,
        pool: String,
        dumper: String,
        amount_in: u64,
        loc: u8,
        ts_ms: u64,
    ) {
        // Already stored (e.g. central restarted mid-flight, or a duplicate
        // report arrived late) — nothing to do.
        if self.contains_sig(&sig) {
            return;
        }
        {
            let mut g = self.inner.in_flight.lock().unwrap();
            if !g.insert(sig.clone()) {
                return; // another location already reported this sig
            }
        }
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let row = OrderflowRow {
                ts_ms,
                venue,
                sig: sig.clone(),
                pool,
                dumper,
                amount_in,
                status: String::new(),
                loc,
            };
            if let Err(e) = resolve_and_store(&inner, row).await {
                tracing::debug!("[orderflow-db] resolve failed sig={sig}: {e:#}");
            }
            inner.in_flight.lock().unwrap().remove(&sig);
        });
    }

    /// Newest-first rows, capped at `limit`.
    pub fn recent(&self, limit: usize) -> Vec<OrderflowRow> {
        let mut out: Vec<OrderflowRow> = Vec::with_capacity(limit.min(1024));
        for kv in self.inner.db.iter().rev() {
            let Ok((_, v)) = kv else { continue };
            match bincode::deserialize::<OrderflowRow>(&v) {
                Ok(r) => out.push(r),
                Err(_) => continue,
            }
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    pub fn count(&self) -> usize {
        self.inner.db.iter().count()
    }

    fn contains_sig(&self, sig: &str) -> bool {
        // Keys are `ts_ms:sig`; a suffix scan is fine at this size and avoids
        // a second index.
        self.inner.db.iter().keys().any(|k| {
            k.map(|k| {
                std::str::from_utf8(&k)
                    .map(|s| s.ends_with(sig))
                    .unwrap_or(false)
            })
            .unwrap_or(false)
        })
    }
}

async fn resolve_and_store(inner: &Inner, mut row: OrderflowRow) -> anyhow::Result<()> {
    tokio::time::sleep(STATUS_CHECK_DELAY).await;
    let rpc = RpcClient::new_with_commitment(inner.rpc_url.clone(), CommitmentConfig::confirmed());
    let parsed: solana_sdk::signature::Signature = row.sig.parse().context("invalid signature")?;
    // `searchTransactionHistory` is off: we only care about txs recent enough
    // to be in the status cache. Anything older effectively never landed.
    let statuses = rpc
        .get_signature_statuses(&[parsed])
        .await
        .context("getSignatureStatuses")?;

    let Some(Some(status)) = statuses.value.into_iter().next() else {
        // Never landed — the common case for pre-block detection. DROP it:
        // not stored, not counted, never shown in the dashboard.
        return Ok(());
    };
    row.status = if status.err.is_none() { "landed" } else { "failed" }.to_owned();

    let key = format!("{:013}:{}", row.ts_ms, row.sig);
    let bytes = bincode::serialize(&row).context("encode orderflow row")?;
    inner
        .db
        .insert(key.as_bytes(), bytes)
        .context("sled insert orderflow")?;
    inner.db.flush().context("sled flush orderflow")?;

    let should_prune = {
        let mut n = inner.inserts.lock().unwrap();
        *n += 1;
        *n % PRUNE_EVERY == 0
    };
    if should_prune {
        prune(inner);
    }
    Ok(())
}

/// Drop the oldest rows past `MAX_ROWS`. Keys are time-ordered, so the
/// oldest are simply the front of the tree.
fn prune(inner: &Inner) {
    let total = inner.db.iter().count();
    if total <= MAX_ROWS {
        return;
    }
    let excess = total - MAX_ROWS;
    let old: Vec<sled::IVec> = inner
        .db
        .iter()
        .keys()
        .take(excess)
        .filter_map(|k| k.ok())
        .collect();
    for k in &old {
        let _ = inner.db.remove(k);
    }
    let _ = inner.db.flush();
    println!("[orderflow-db] pruned {} old row(s), {} remain", old.len(), total - old.len());
}
