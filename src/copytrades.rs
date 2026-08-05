//! Persistent registry of the SECONDARY copy-trading bot's trades, with a
//! front-run verdict per trade.
//!
//! The bot mirrors one trader's buys from `SECOND_LOGIC_WALLET`, then exits on
//! its own TP / SL / max-hold. Detection is PRE-BLOCK (orderflow), which means
//! a copy buy can land *ahead of* the very trader being copied. That is a
//! front-run, and finding it is the entire reason this bot runs unreported
//! while under test.
//!
//! So the headline field here is not PnL, it's `verdict`:
//!   * `after`        — the trader landed first; we are behind them.
//!   * `before`       — WE landed first. We front-ran them.
//!   * `trader_failed` — we landed, their transaction REVERTED on chain.
//!                       Invisible to the observation ring (geyser filters
//!                       failed txs), so it is resolved by a status lookup
//!                       over their fan-out.
//!   * `no_trader_tx` — we landed, they never landed at all.
//!   * `unresolved`   — we could not even locate our OWN buy. Measurement
//!                      failure, worth distinguishing from the above.
//!   * `pending`      — resolution has not run yet.
//!
//! Same-slot is NOT a verdict: the block scan yields intra-block order for
//! both transactions, so it always resolves to before or after.
//!
//! The verdict is computed BY THE BOT from its geyser stream, which carries
//! slot and intra-block index on every transaction, and pushed here as
//! `copy_trade_verdict`. Central used to fetch three full blocks per trade to
//! derive the same ordering; it now stores what it is told.
//!
//! Keyed `ts_ms:buy_sig` so a reverse range scan is newest-first, matching
//! `orderflow.rs`.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// Cap on stored trades. Generous — this is a low-rate bot and the history is
/// the point of it.
const MAX_ROWS: usize = 200_000;

/// One copy trade, from entry through exit.
///
/// NOTE: stored with bincode, which is positional and NOT self-describing.
/// `skip_serializing_if` must never appear on these fields — omitting one on
/// write shifts every byte after it, and the row becomes undecodable. The
/// symptom is silent: `count()` still sees the key while `page()` and
/// `summary()` drop the row, so the dashboard reports N trades above an empty
/// table. For the same reason, ADDING a field invalidates every existing row
/// (`#[serde(default)]` cannot help — bincode has no way to know a field is
/// absent), so any new field needs a legacy fallback like `orderflow.rs` has.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CopyTrade {
    pub ts_ms: u64,
    /// Pool we traded.
    pub pool: String,
    /// Token mint bought.
    pub mint: String,
    /// The trader's transaction we reacted to (the first of their fan-out we
    /// happened to see — usually NOT the one that landed).
    pub trader_sig: String,
    /// The trader's wallet.
    #[serde(default)]
    pub trader_wallet: String,
    /// `amount_in` from their PumpFun buy instruction. This, with the pool, is
    /// what identifies their LANDED buy: they fan one buy across several
    /// senders and the instruction data is byte-identical across all of them,
    /// so the amount matches whichever won while the signature does not.
    #[serde(default)]
    pub trader_amount_in: u64,
    /// Our copy BUY signature.
    pub buy_sig: String,
    /// Lamports of WSOL committed on the buy.
    pub buy_size_lamports: u64,
    /// Entry price (SOL per token) as observed when we fired.
    #[serde(default)]
    pub buy_price_sol: f64,
    /// Which bot location fired it.
    #[serde(default)]
    pub loc: u8,

    // ---- exit, filled in when the position closes ----
    #[serde(default)]
    pub sell_sig: Option<String>,
    #[serde(default)]
    pub sell_price_sol: Option<f64>,
    /// `tp` | `sl` | `max_hold` | `manual`.
    #[serde(default)]
    pub exit_reason: Option<String>,
    /// Realised SOL delta on the round trip, when known.
    #[serde(default)]
    pub pnl_sol: Option<f64>,
    #[serde(default)]
    pub closed_ts_ms: Option<u64>,

    // ---- front-run analysis ----
    /// `after` | `same_slot` | `before` | `unresolved` | `pending`.
    #[serde(default = "verdict_pending")]
    pub verdict: String,
    #[serde(default)]
    pub trader_slot: Option<u64>,
    #[serde(default)]
    pub our_slot: Option<u64>,
    /// Position within the block, when we had to look. Lower = earlier.
    #[serde(default)]
    pub trader_block_index: Option<u32>,
    #[serde(default)]
    pub our_block_index: Option<u32>,

    // ---- the trader's side, for display ----
    /// The trader signature that ACTUALLY landed, resolved by the verdict
    /// matcher. `trader_sig` above is merely the first of their fan-out we
    /// saw, which usually never landed — a link built from it is dead.
    #[serde(default)]
    pub trader_landed_sig: Option<String>,
    /// Lamports the trader tipped on this buy.
    #[serde(default)]
    pub trader_tip_lamports: u64,
    /// The trader's own implied slippage tolerance in bps, derived from their
    /// instruction against the reserves at detection. Recorded for comparison
    /// against ours — theirs is priced before their buy moves the pool, so it
    /// is a floor on what we need, not a value to copy.
    #[serde(default)]
    pub trader_slippage_bps: u32,

    // ---- exit race ----
    /// Did OUR mirror sell land ahead of theirs? `before` | `after` |
    /// `unresolved` | `n/a`.
    ///
    /// Only a mirror exit is a race. A solo exit fires on a timer with the
    /// trader still holding, so there is no sell of theirs to order against
    /// and the field stays `n/a`.
    #[serde(default = "sell_verdict_na")]
    pub sell_verdict: String,
    #[serde(default)]
    pub sell_trader_sig: Option<String>,
    #[serde(default)]
    pub sell_our_slot: Option<u64>,
    #[serde(default)]
    pub sell_our_index: Option<u64>,
    #[serde(default)]
    pub sell_trader_slot: Option<u64>,
    #[serde(default)]
    pub sell_trader_index: Option<u64>,

    /// Microseconds from the bot decoding the trader's transaction to ours
    /// going on the wire. Our reaction time — the only part of the race we
    /// control, as distinct from network and leader scheduling.
    #[serde(default)]
    pub build_us: u64,

    /// Every transaction the trader submitted for this opportunity.
    ///
    /// They fan one buy across several senders with escalating tip and CU
    /// price, so a single "their tip" figure hides the ladder — which is the
    /// part that says how badly they wanted the fill.
    #[serde(default)]
    pub trader_attempts: Vec<TraderAttempt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraderAttempt {
    pub sig: String,
    pub tip_lamports: u64,
    pub cu_price: u64,
    pub cu_limit: u32,
    /// `cu_limit x cu_price / 1e6`, computed by the bot so the dashboard does
    /// not have to know the formula.
    pub priority_fee_lamports: u64,
}

fn verdict_pending() -> String {
    "pending".to_owned()
}

fn sell_verdict_na() -> String {
    "n/a".to_owned()
}

/// The layout before the exit-race fields. bincode is positional, so without
/// this every trade recorded up to now would decode to nothing — present as a
/// row count that disagrees with an empty table.
#[derive(Deserialize)]
struct LegacyCopyTrade {
    ts_ms: u64,
    pool: String,
    mint: String,
    trader_sig: String,
    trader_wallet: String,
    trader_amount_in: u64,
    buy_sig: String,
    buy_size_lamports: u64,
    buy_price_sol: f64,
    loc: u8,
    sell_sig: Option<String>,
    sell_price_sol: Option<f64>,
    exit_reason: Option<String>,
    pnl_sol: Option<f64>,
    closed_ts_ms: Option<u64>,
    verdict: String,
    trader_slot: Option<u64>,
    our_slot: Option<u64>,
    trader_block_index: Option<u32>,
    our_block_index: Option<u32>,
    trader_landed_sig: Option<String>,
    trader_tip_lamports: u64,
    trader_slippage_bps: u32,
}

/// The layout before `build_us`. bincode is positional, so each added field
/// needs its own predecessor here or the rows written before it stop decoding.
#[derive(Deserialize)]
struct CopyTradeV2 {
    ts_ms: u64,
    pool: String,
    mint: String,
    trader_sig: String,
    trader_wallet: String,
    trader_amount_in: u64,
    buy_sig: String,
    buy_size_lamports: u64,
    buy_price_sol: f64,
    loc: u8,
    sell_sig: Option<String>,
    sell_price_sol: Option<f64>,
    exit_reason: Option<String>,
    pnl_sol: Option<f64>,
    closed_ts_ms: Option<u64>,
    verdict: String,
    trader_slot: Option<u64>,
    our_slot: Option<u64>,
    trader_block_index: Option<u32>,
    our_block_index: Option<u32>,
    trader_landed_sig: Option<String>,
    trader_tip_lamports: u64,
    trader_slippage_bps: u32,
    sell_verdict: String,
    sell_trader_sig: Option<String>,
    sell_our_slot: Option<u64>,
    sell_our_index: Option<u64>,
    sell_trader_slot: Option<u64>,
    sell_trader_index: Option<u64>,
}

/// The layout before `trader_attempts`.
#[derive(Deserialize)]
struct CopyTradeV3 {
    ts_ms: u64, pool: String, mint: String, trader_sig: String,
    trader_wallet: String, trader_amount_in: u64, buy_sig: String,
    buy_size_lamports: u64, buy_price_sol: f64, loc: u8,
    sell_sig: Option<String>, sell_price_sol: Option<f64>,
    exit_reason: Option<String>, pnl_sol: Option<f64>, closed_ts_ms: Option<u64>,
    verdict: String, trader_slot: Option<u64>, our_slot: Option<u64>,
    trader_block_index: Option<u32>, our_block_index: Option<u32>,
    trader_landed_sig: Option<String>, trader_tip_lamports: u64,
    trader_slippage_bps: u32, sell_verdict: String,
    sell_trader_sig: Option<String>, sell_our_slot: Option<u64>,
    sell_our_index: Option<u64>, sell_trader_slot: Option<u64>,
    sell_trader_index: Option<u64>, build_us: u64,
}

/// Decode a stored row, falling back through each earlier layout.
fn decode_trade(v: &[u8]) -> Option<CopyTrade> {
    if let Ok(t) = bincode::deserialize::<CopyTrade>(v) {
        return Some(t);
    }
    if let Ok(l) = bincode::deserialize::<CopyTradeV3>(v) {
        return Some(CopyTrade {
            ts_ms: l.ts_ms, pool: l.pool, mint: l.mint, trader_sig: l.trader_sig,
            trader_wallet: l.trader_wallet, trader_amount_in: l.trader_amount_in,
            buy_sig: l.buy_sig, buy_size_lamports: l.buy_size_lamports,
            buy_price_sol: l.buy_price_sol, loc: l.loc, sell_sig: l.sell_sig,
            sell_price_sol: l.sell_price_sol, exit_reason: l.exit_reason,
            pnl_sol: l.pnl_sol, closed_ts_ms: l.closed_ts_ms, verdict: l.verdict,
            trader_slot: l.trader_slot, our_slot: l.our_slot,
            trader_block_index: l.trader_block_index, our_block_index: l.our_block_index,
            trader_landed_sig: l.trader_landed_sig,
            trader_tip_lamports: l.trader_tip_lamports,
            trader_slippage_bps: l.trader_slippage_bps,
            sell_verdict: l.sell_verdict, sell_trader_sig: l.sell_trader_sig,
            sell_our_slot: l.sell_our_slot, sell_our_index: l.sell_our_index,
            sell_trader_slot: l.sell_trader_slot, sell_trader_index: l.sell_trader_index,
            build_us: l.build_us,
            // Not recorded before this existed.
            trader_attempts: Vec::new(),
        });
    }
    if let Ok(l) = bincode::deserialize::<CopyTradeV2>(v) {
        return Some(CopyTrade {
            ts_ms: l.ts_ms, pool: l.pool, mint: l.mint, trader_sig: l.trader_sig,
            trader_wallet: l.trader_wallet, trader_amount_in: l.trader_amount_in,
            buy_sig: l.buy_sig, buy_size_lamports: l.buy_size_lamports,
            buy_price_sol: l.buy_price_sol, loc: l.loc, sell_sig: l.sell_sig,
            sell_price_sol: l.sell_price_sol, exit_reason: l.exit_reason,
            pnl_sol: l.pnl_sol, closed_ts_ms: l.closed_ts_ms, verdict: l.verdict,
            trader_slot: l.trader_slot, our_slot: l.our_slot,
            trader_block_index: l.trader_block_index, our_block_index: l.our_block_index,
            trader_landed_sig: l.trader_landed_sig,
            trader_tip_lamports: l.trader_tip_lamports,
            trader_slippage_bps: l.trader_slippage_bps,
            sell_verdict: l.sell_verdict, sell_trader_sig: l.sell_trader_sig,
            sell_our_slot: l.sell_our_slot, sell_our_index: l.sell_our_index,
            sell_trader_slot: l.sell_trader_slot, sell_trader_index: l.sell_trader_index,
            // Not measured before this existed.
            build_us: 0,
            trader_attempts: Vec::new(),
        });
    }
    bincode::deserialize::<LegacyCopyTrade>(v).ok().map(|l| CopyTrade {
        ts_ms: l.ts_ms,
        pool: l.pool,
        mint: l.mint,
        trader_sig: l.trader_sig,
        trader_wallet: l.trader_wallet,
        trader_amount_in: l.trader_amount_in,
        buy_sig: l.buy_sig,
        buy_size_lamports: l.buy_size_lamports,
        buy_price_sol: l.buy_price_sol,
        loc: l.loc,
        sell_sig: l.sell_sig,
        sell_price_sol: l.sell_price_sol,
        exit_reason: l.exit_reason,
        pnl_sol: l.pnl_sol,
        closed_ts_ms: l.closed_ts_ms,
        verdict: l.verdict,
        trader_slot: l.trader_slot,
        our_slot: l.our_slot,
        trader_block_index: l.trader_block_index,
        our_block_index: l.our_block_index,
        trader_landed_sig: l.trader_landed_sig,
        trader_tip_lamports: l.trader_tip_lamports,
        trader_slippage_bps: l.trader_slippage_bps,
        // Nothing recorded before this existed was measured.
        sell_verdict: sell_verdict_na(),
        sell_trader_sig: None,
        sell_our_slot: None,
        sell_our_index: None,
        sell_trader_slot: None,
        sell_trader_index: None,
        build_us: 0,
        trader_attempts: Vec::new(),
    })
}

#[derive(Clone)]
pub struct CopyTradeStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    /// `buy_sig -> key`, so updating a trade on exit is a point lookup rather
    /// than a scan of the whole tree.
    by_buy_sig: sled::Tree,
    in_flight: Mutex<HashSet<String>>,
}

impl CopyTradeStore {
    pub fn open(db_path: &Path) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled copytrades db")?;
        let tree = db.open_tree("copytrades").context("open copytrades tree")?;
        let by_buy_sig = db
            .open_tree("copytrades_by_sig")
            .context("open copytrades_by_sig tree")?;
        let n = tree.iter().count();
        println!("[copytrades] opened at {} ({n} trades)", db_path.display());
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                by_buy_sig,
                in_flight: Mutex::new(HashSet::new()),
            }),
        })
    }

    /// A copy buy was dispatched and landed. Stores it immediately (so the
    /// trade is visible right away) and resolves the front-run verdict in the
    /// background.
    #[allow(clippy::too_many_arguments)]
    pub fn record_buy(&self, trade: CopyTrade) {
        let key = format!("{:013}:{}", trade.ts_ms, trade.buy_sig);
        if let Err(e) = self.put(&key, &trade) {
            tracing::warn!("[copytrades] store buy failed: {e:#}");
            return;
        }
        let _ = self
            .inner
            .by_buy_sig
            .insert(trade.buy_sig.as_bytes(), key.as_bytes());
        let _ = self.inner.by_buy_sig.flush();

    }


    /// Store a verdict computed by the bot from its geyser stream.
    ///
    /// Central no longer resolves this itself. The ordering that decides it —
    /// (slot, intra-block index) — arrives free on every geyser transaction
    /// the bot already receives, so fetching three full blocks per trade was
    /// buying data we were being handed.
    #[allow(clippy::too_many_arguments)]
    /// The exit race resolved: did our mirror sell land ahead of theirs?
    pub fn apply_sell_verdict(
        &self,
        buy_sig: &str,
        verdict: String,
        trader_sig: Option<String>,
        our_slot: Option<u64>,
        our_index: Option<u64>,
        trader_slot: Option<u64>,
        trader_index: Option<u64>,
    ) {
        let Some(key) = self.key_for(buy_sig) else {
            tracing::debug!("[copytrades] sell verdict for unknown buy_sig={buy_sig}");
            return;
        };
        let Some(mut t) = self.get(&key) else { return };
        if verdict == "before" {
            println!(
                "[copytrades] FRONT-RAN THEIR SELL buy={buy_sig} pool={} \
                 ours=({our_slot:?},{our_index:?}) theirs=({trader_slot:?},{trader_index:?})",
                t.pool
            );
        }
        t.sell_verdict = verdict;
        if trader_sig.is_some() {
            t.sell_trader_sig = trader_sig;
        }
        t.sell_our_slot = our_slot;
        t.sell_our_index = our_index;
        t.sell_trader_slot = trader_slot;
        t.sell_trader_index = trader_index;
        if let Err(e) = self.put(&key, &t) {
            tracing::warn!("[copytrades] store sell verdict failed: {e:#}");
        }
    }

    pub fn apply_verdict(
        &self,
        buy_sig: &str,
        verdict: String,
        trader_landed_sig: Option<String>,
        our_slot: Option<u64>,
        our_index: Option<u64>,
        trader_slot: Option<u64>,
        trader_index: Option<u64>,
    ) {
        let Some(key) = self.key_for(buy_sig) else {
            tracing::debug!("[copytrades] verdict for unknown buy_sig={buy_sig}");
            return;
        };
        let Some(mut t) = self.get(&key) else { return };
        if verdict == "before" {
            println!(
                "[copytrades] FRONT-RUN buy={buy_sig} pool={} ours=({our_slot:?},{our_index:?}) \
                 theirs=({trader_slot:?},{trader_index:?})",
                t.pool
            );
        }
        t.verdict = verdict;
        if trader_landed_sig.is_some() {
            t.trader_landed_sig = trader_landed_sig;
        }
        t.our_slot = our_slot;
        t.trader_slot = trader_slot;
        t.our_block_index = our_index.map(|v| v as u32);
        t.trader_block_index = trader_index.map(|v| v as u32);
        if let Err(e) = self.put(&key, &t) {
            tracing::warn!("[copytrades] store verdict failed: {e:#}");
        }
    }

    /// Position closed — patch the existing row rather than adding a new one.
    pub fn record_sell(
        &self,
        buy_sig: &str,
        sell_sig: String,
        sell_price_sol: f64,
        exit_reason: String,
        pnl_sol: Option<f64>,
        closed_ts_ms: u64,
    ) {
        let Some(key) = self.key_for(buy_sig) else {
            tracing::debug!("[copytrades] sell for unknown buy_sig={buy_sig}");
            return;
        };
        let Some(mut t) = self.get(&key) else { return };
        t.sell_sig = Some(sell_sig);
        t.sell_price_sol = Some(sell_price_sol);
        t.exit_reason = Some(exit_reason);
        t.pnl_sol = pnl_sol;
        t.closed_ts_ms = Some(closed_ts_ms);
        if let Err(e) = self.put(&key, &t) {
            tracing::warn!("[copytrades] store sell failed: {e:#}");
        }
    }

    /// One newest-first page.
    pub fn page(&self, offset: usize, limit: usize) -> Vec<CopyTrade> {
        let mut out = Vec::with_capacity(limit.min(1024));
        for kv in self.inner.db.iter().rev().skip(offset) {
            let Ok((_, v)) = kv else { continue };
            if let Some(t) = decode_trade(&v) {
                out.push(t);
            }
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Rows that actually DECODE — not raw key count.
    ///
    /// Counting keys is what let the summary claim "2 trades" over an empty
    /// table. The store is small, so the scan is cheap and the two numbers
    /// can never disagree again.
    pub fn count(&self) -> usize {
        self.inner
            .db
            .iter()
            .filter_map(|kv| kv.ok())
            .filter(|(_, v)| decode_trade(v).is_some())
            .count()
    }

    /// Aggregate stats over the whole DB — the numbers that decide whether
    /// this bot is safe to report to Temporal.
    pub fn summary(&self) -> serde_json::Value {
        let (mut before, mut after, mut unresolved) = (0u64, 0u64, 0u64);
        // Their buy never landed at all — a real outcome, not a measurement
        // failure, so it is counted separately from `unresolved`.
        let mut no_trader = 0u64;
        // WE landed, THEY reverted. Distinct from "never landed": once an
        // anti-front-run contract is live, a revert of theirs may be the guard
        // firing rather than an ordinary failure.
        let mut trader_failed = 0u64;
        let (mut closed, mut wins, mut pnl) = (0u64, 0u64, 0f64);
        // Exit race, counted only over mirror exits (solo exits are `n/a`).
        let (mut sell_before, mut sell_after) = (0u64, 0u64);
        for kv in self.inner.db.iter() {
            let Ok((_, v)) = kv else { continue };
            let Some(t) = decode_trade(&v) else { continue };
            match t.verdict.as_str() {
                "before" => before += 1,
                "after" => after += 1,
                "no_trader_tx" => no_trader += 1,
                "trader_failed" => trader_failed += 1,
                _ => unresolved += 1,
            }
            match t.sell_verdict.as_str() {
                "before" => sell_before += 1,
                "after" => sell_after += 1,
                _ => {}
            }
            if let Some(p) = t.pnl_sol {
                closed += 1;
                pnl += p;
                if p > 0.0 {
                    wins += 1;
                }
            }
        }
        // A same-slot landing is no longer its own verdict: intra-block order
        // comes straight from the block scan, so it always resolves to
        // before or after.
        let judged = before + after;
        serde_json::json!({
            "total": self.count(),
            "front_run": before,
            "backrun": after,
            "unresolved": unresolved,
            "no_trader_tx": no_trader,
            "trader_failed": trader_failed,
            // The number that gates reporting this bot to Temporal.
            "front_run_pct": if judged > 0 { before as f64 * 100.0 / judged as f64 } else { 0.0 },
            "closed": closed,
            "wins": wins,
            "win_pct": if closed > 0 { wins as f64 * 100.0 / closed as f64 } else { 0.0 },
            "pnl_sol": pnl,
            // Exit race. `sell_judged` is the denominator that matters —
            // solo exits are excluded entirely, so a low count here just
            // means few positions were ever mirrored.
            "sell_front_run": sell_before,
            "sell_backrun": sell_after,
            "sell_front_run_pct": if sell_before + sell_after > 0 {
                sell_before as f64 * 100.0 / (sell_before + sell_after) as f64
            } else { 0.0 },
        })
    }

    // ---- internals ----

    fn key_for(&self, buy_sig: &str) -> Option<String> {
        self.inner
            .by_buy_sig
            .get(buy_sig.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
    }

    fn get(&self, key: &str) -> Option<CopyTrade> {
        self.inner
            .db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| decode_trade(&v))
    }

    fn put(&self, key: &str, t: &CopyTrade) -> anyhow::Result<()> {
        let bytes = bincode::serialize(t).context("encode copy trade")?;
        self.inner
            .db
            .insert(key.as_bytes(), bytes)
            .context("sled insert copytrade")?;
        self.inner.db.flush().context("sled flush copytrades")?;
        self.prune();
        Ok(())
    }

    fn prune(&self) {
        let total = self.inner.db.iter().count();
        if total <= MAX_ROWS {
            return;
        }
        for k in self
            .inner
            .db
            .iter()
            .keys()
            .take(total - MAX_ROWS)
            .filter_map(|k| k.ok())
        {
            if let Ok(ks) = std::str::from_utf8(&k) {
                if let Some((_, sig)) = ks.split_once(':') {
                    let _ = self.inner.by_buy_sig.remove(sig.as_bytes());
                }
            }
            let _ = self.inner.db.remove(&k);
        }
    }
}
