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

/// Cap on stored rows. Effectively "keep everything" for months while staying
/// bounded so sled and the dashboard remain predictable (operator decision:
/// keep all landed+failed, don't delete).
const MAX_ROWS: usize = 1_000_000;

/// Prune only occasionally rather than on every insert.
const PRUNE_EVERY: u64 = 4_096;

/// Dumper wallets whose NOT-LANDED detections we keep. Everything that reaches
/// the chain (landed/failed) is stored for every dumper; never-landed ones are
/// dropped as noise EXCEPT for these, which the operator tracks specifically.
const KEEP_NOT_LANDED_DUMPERS: &[&str] = &[
    "hnu5iBK8UoHb51UFsH1RYTUAYdrhjHvV5YMTf9T1CYN",
    "FYX5JQ2kP7TD8gWb9WP1tjmwWWUAzi8edEZTr5Z8F1ck",
];

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
    /// Dumper's `min_amount_out` in WSOL lamports, from the swap ix. `0` when
    /// the venue exposes none (aggregator routers) — rendered as "—".
    #[serde(default)]
    pub min_amount_out: u64,
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
    /// `sig -> ()` index. The main tree is keyed `ts_ms:sig` for time order,
    /// so "have I already stored this sig?" used to be a FULL key scan — O(n)
    /// per detection, fine at 20k rows and hopeless at 1M. This makes it a
    /// point lookup.
    by_sig: sled::Tree,
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
        let by_sig = db
            .open_tree("orderflow_by_sig")
            .context("open orderflow_by_sig tree")?;
        let n = tree.iter().count();
        println!("[orderflow-db] opened at {} ({n} rows)", db_path.display());
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                by_sig,
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
        min_amount_out: u64,
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
                min_amount_out,
                status: String::new(),
                loc,
            };
            if let Err(e) = resolve_and_store(&inner, row).await {
                tracing::debug!("[orderflow-db] resolve failed sig={sig}: {e:#}");
            }
            inner.in_flight.lock().unwrap().remove(&sig);
        });
    }

    /// One newest-first page. Keys are time-ordered, so a reverse iterator
    /// walked `offset` forward is the page start — no full materialisation.
    pub fn page(&self, offset: usize, limit: usize) -> Vec<OrderflowRow> {
        let mut out: Vec<OrderflowRow> = Vec::with_capacity(limit.min(1024));
        for kv in self.inner.db.iter().rev().skip(offset) {
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
        self.inner
            .by_sig
            .contains_key(sig.as_bytes())
            .unwrap_or(false)
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

    match statuses.value.into_iter().next() {
        // On chain: err distinguishes a clean land from a revert. Always kept.
        Some(Some(status)) => {
            row.status = if status.err.is_none() { "landed" } else { "failed" }.to_owned();
        }
        // Never landed. Normally dropped — with pre-block detection these are
        // the majority and pure noise — EXCEPT for the operator-tracked
        // dumpers, whose misses are themselves the signal.
        _ => {
            if !KEEP_NOT_LANDED_DUMPERS.contains(&row.dumper.as_str()) {
                return Ok(());
            }
            row.status = "not-landed".to_owned();
        }
    }

    let key = format!("{:013}:{}", row.ts_ms, row.sig);
    let bytes = bincode::serialize(&row).context("encode orderflow row")?;
    inner
        .db
        .insert(key.as_bytes(), bytes)
        .context("sled insert orderflow")?;
    // Index the sig so the next duplicate report is a point lookup, and
    // remember its main-tree key so pruning can evict both together.
    inner
        .by_sig
        .insert(row.sig.as_bytes(), key.as_bytes())
        .context("sled insert orderflow_by_sig")?;
    inner.db.flush().context("sled flush orderflow")?;
    inner.by_sig.flush().context("sled flush orderflow_by_sig")?;

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
        // Key is `ts_ms:sig` — recover the sig to drop its index entry too,
        // otherwise the index grows forever and would resurrect dedup hits
        // for rows that no longer exist.
        if let Ok(ks) = std::str::from_utf8(k) {
            if let Some((_, sig)) = ks.split_once(':') {
                let _ = inner.by_sig.remove(sig.as_bytes());
            }
        }
        let _ = inner.db.remove(k);
    }
    let _ = inner.db.flush();
    let _ = inner.by_sig.flush();
    println!("[orderflow-db] pruned {} old row(s), {} remain", old.len(), total - old.len());
}
