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

/// The operator-tracked dumper wallets. Two roles:
///  1. INGEST — their never-landed detections are kept (everyone else's are
///     dropped as noise; landed/failed are stored for every dumper).
///  2. SERVE — `page_filtered` returns ONLY these wallets, so the /orderflow
///     page shows just the tracked competitors and nothing else.
/// Keep this in sync with `copy_trading_v2.competitors` in the fee config.
const KEEP_NOT_LANDED_DUMPERS: &[&str] = &[
    "hnu5iBK8UoHb51UFsH1RYTUAYdrhjHvV5YMTf9T1CYN",
    "FYX5JQ2kP7TD8gWb9WP1tjmwWWUAzi8edEZTr5Z8F1ck",
    "popo3Rj6arKNttyUFpWfbkv2gG8uS13TGtmH6JPMuHz",
];

/// One detected dump that reached the chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderflowRow {
    pub ts_ms: u64,
    /// Venue label from the bot's parser: `direct_pump` / `direct_cpmm` /
    /// `jupiter` / `okx` / `dflow` / `axiom`.
    pub venue: String,
    /// `sell` = a dump (the strategy signal). `buy` = a watched wallet's
    /// entry, surfaced for observation only. Defaults to `sell` so rows
    /// written before this field existed still deserialize.
    #[serde(default = "default_side")]
    pub side: String,
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

    // ---- what the transaction bid, straight off its instructions ----
    //
    // Populated for the watched trader's BUYS only. On dumps these stay 0 and
    // render as "—": deriving the tip means walking every System transfer,
    // which is fine for their handful of buys but not for the dump path, which
    // runs on every orderflow transaction and is latency-critical.
    /// Lamports transferred to a known tip account.
    #[serde(default)]
    pub tip_lamports: u64,
    /// `SetComputeUnitPrice`, microlamports per compute unit.
    #[serde(default)]
    pub cu_price: u64,
    /// `SetComputeUnitLimit`, compute units.
    #[serde(default)]
    pub cu_limit: u32,
    /// `cu_limit x cu_price / 1e6`. Computed bot-side so the dashboard and the
    /// copytrading page cannot disagree about the formula.
    #[serde(default)]
    pub priority_fee_lamports: u64,
}

fn default_side() -> String {
    "sell".to_owned()
}


/// The row shape as it was BEFORE `side` was added.
///
/// bincode is positional and non-self-describing, so `#[serde(default)]` does
/// nothing for it: a row written without `side` cannot be read by a
/// deserializer that expects `side`, and every byte after that point is
/// misread. Without this fallback, adding the field would have silently
/// orphaned the entire stored history — the rows stay in sled but decode to
/// nothing, which shows up as a row count that disagrees with the table.
#[derive(Deserialize)]
struct LegacyOrderflowRow {
    ts_ms: u64,
    venue: String,
    sig: String,
    pool: String,
    dumper: String,
    amount_in: u64,
    min_amount_out: u64,
    status: String,
    loc: u8,
}

/// The layout before the per-transaction bid fields.
#[derive(Deserialize)]
struct OrderflowRowV2 {
    ts_ms: u64,
    venue: String,
    side: String,
    sig: String,
    pool: String,
    dumper: String,
    amount_in: u64,
    min_amount_out: u64,
    status: String,
    loc: u8,
}

/// Decode a stored row, falling back through each earlier layout.
fn decode_row(v: &[u8]) -> Option<OrderflowRow> {
    if let Ok(r) = bincode::deserialize::<OrderflowRow>(v) {
        return Some(r);
    }
    if let Ok(l) = bincode::deserialize::<OrderflowRowV2>(v) {
        return Some(OrderflowRow {
            ts_ms: l.ts_ms, venue: l.venue, side: l.side, sig: l.sig, pool: l.pool,
            dumper: l.dumper, amount_in: l.amount_in, min_amount_out: l.min_amount_out,
            status: l.status, loc: l.loc,
            tip_lamports: 0, cu_price: 0, cu_limit: 0, priority_fee_lamports: 0,
        });
    }
    bincode::deserialize::<LegacyOrderflowRow>(v)
        .ok()
        .map(|l| OrderflowRow {
            ts_ms: l.ts_ms,
            venue: l.venue,
            // Everything recorded before the split was a dump.
            side: default_side(),
            sig: l.sig,
            pool: l.pool,
            dumper: l.dumper,
            amount_in: l.amount_in,
            min_amount_out: l.min_amount_out,
            status: l.status,
            loc: l.loc,
            tip_lamports: 0,
            cu_price: 0,
            cu_limit: 0,
            priority_fee_lamports: 0,
        })
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
    /// ONE client, shared.
    ///
    /// This was constructed per detection, and every `RpcClient` builds its
    /// own reqwest client and connection pool. The trader alone fans out ~4
    /// transactions per opportunity and every dump is a detection too, so at
    /// live rates that is thousands of fresh TCP/TLS setups a minute, each
    /// held open across a 12s sleep. Sharing one reuses the pool.
    rpc: RpcClient,
    inserts: Mutex<u64>,
    /// Live row count.
    ///
    /// `count()` used to be `db.iter().count()`, which walks the whole tree
    /// and materialises every VALUE from sled. The dashboard polls the
    /// unfiltered endpoint every 3 seconds, and that scan runs inside an async
    /// handler with no `spawn_blocking` — so at 64k rows it stalls the entire
    /// central runtime on a timer, taking WS ingest and orderflow storage down
    /// with it. Symptom: the page freezes AND new detections stop arriving.
    /// Counted incrementally instead; the only full scan is once at open.
    total: std::sync::atomic::AtomicUsize,
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
                rpc: RpcClient::new_with_commitment(
                    rpc_url.clone(),
                    CommitmentConfig::confirmed(),
                ),
                rpc_url,
                inserts: Mutex::new(0),
                total: std::sync::atomic::AtomicUsize::new(n),
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
        side: String,
        pool: String,
        dumper: String,
        amount_in: u64,
        min_amount_out: u64,
        loc: u8,
        ts_ms: u64,
        // What the transaction bid. Zeroes on the dump path, where reading
        // them would cost hot-path work on every orderflow transaction.
        tip_lamports: u64,
        cu_price: u64,
        cu_limit: u32,
        priority_fee_lamports: u64,
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
                side,
                sig: sig.clone(),
                pool,
                dumper,
                amount_in,
                min_amount_out,
                status: String::new(),
                loc,
                tip_lamports,
                cu_price,
                cu_limit,
                priority_fee_lamports,
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
        self.page_filtered(offset, limit, None, None).0
    }

    /// One newest-first page of rows matching an optional status and/or side.
    ///
    /// Filtering happens HERE, not in the browser. Filtering a
    /// already-paginated response would return "the landed rows among the
    /// most recent 50", which is a handful — not the 50 landed rows the
    /// operator asked for.
    ///
    /// Returns `(rows, has_more)`. `has_more` comes from peeking one row past
    /// the page rather than counting every match: a full count would mean
    /// deserializing the entire tree on every request, and this endpoint is
    /// polled every 3 seconds.
    pub fn page_filtered(
        &self,
        offset: usize,
        limit: usize,
        status: Option<&str>,
        side: Option<&str>,
    ) -> (Vec<OrderflowRow>, bool) {
        let mut out: Vec<OrderflowRow> = Vec::with_capacity(limit.min(1024));
        let mut skipped = 0usize;
        let mut has_more = false;
        for kv in self.inner.db.iter().rev() {
            let Ok((_, v)) = kv else { continue };
            let Some(r) = decode_row(&v) else {
                continue;
            };
            // Only the operator-tracked dumpers are ever shown on /orderflow.
            if !KEEP_NOT_LANDED_DUMPERS.contains(&r.dumper.as_str()) {
                continue;
            }
            if let Some(want) = status {
                if r.status != want {
                    continue;
                }
            }
            if let Some(want) = side {
                if r.side != want {
                    continue;
                }
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if out.len() == limit {
                // One match beyond the page — enough to enable "next".
                has_more = true;
                break;
            }
            out.push(r);
        }
        (out, has_more)
    }

    /// Count of rows matching a filter. Only computed when a filter is
    /// active and the table is small enough that a scan is cheap; callers
    /// pass `None` to skip it entirely.
    pub fn count_filtered(&self, status: Option<&str>, side: Option<&str>) -> usize {
        self.inner
            .db
            .iter()
            .filter_map(|kv| kv.ok())
            .filter_map(|(_, v)| decode_row(&v))
            .filter(|r| status.is_none_or(|w| r.status == w))
            .filter(|r| side.is_none_or(|w| r.side == w))
            .count()
    }

    /// O(1). See `Inner::total` for why this must never scan.
    pub fn count(&self) -> usize {
        self.inner.total.load(std::sync::atomic::Ordering::Relaxed)
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
    let parsed: solana_sdk::signature::Signature = row.sig.parse().context("invalid signature")?;
    // `searchTransactionHistory` is off: we only care about txs recent enough
    // to be in the status cache. Anything older effectively never landed.
    let statuses = inner
        .rpc
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
    inner
        .total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
    let total = inner.total.load(std::sync::atomic::Ordering::Relaxed);
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
    inner
        .total
        .fetch_sub(old.len(), std::sync::atomic::Ordering::Relaxed);
    println!("[orderflow-db] pruned {} old row(s), {} remain", old.len(), total - old.len());
}
