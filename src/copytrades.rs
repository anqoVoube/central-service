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
//!   * `after`      — the trader landed first; we backran them. Correct.
//!   * `same_slot`  — same block; decided by intra-block order.
//!   * `before`     — WE landed first. We front-ran them. This is the bug.
//!   * `unresolved` — one of the two txs never landed, or RPC failed.
//!
//! Resolution is lazy and cheap in the common case: `getTransaction` on both
//! signatures gives two slots, which usually settles it. Only when the slots
//! are EQUAL do we pay for a `getBlock` to compare positions within the block
//! — and that call goes to the dedicated block-detail RPC, since it is by far
//! the heaviest thing we do.
//!
//! Keyed `ts_ms:buy_sig` so a reverse range scan is newest-first, matching
//! `orderflow.rs`.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcTransactionConfig},
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status_client_types::{TransactionDetails, UiTransactionEncoding};

/// Wait before resolving. Both txs must have had time to confirm; the copy
/// buy is dispatched pre-block so it settles at roughly the same time as the
/// trader's.
const RESOLVE_DELAY: Duration = Duration::from_secs(15);

/// Cap on stored trades. Generous — this is a low-rate bot and the history is
/// the point of it.
const MAX_ROWS: usize = 200_000;

/// One copy trade, from entry through exit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CopyTrade {
    pub ts_ms: u64,
    /// Pool we traded.
    pub pool: String,
    /// Token mint bought.
    pub mint: String,
    /// The trader's transaction we reacted to.
    pub trader_sig: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sell_sig: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sell_price_sol: Option<f64>,
    /// `tp` | `sl` | `max_hold` | `manual`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// Realised SOL delta on the round trip, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pnl_sol: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_ts_ms: Option<u64>,

    // ---- front-run analysis ----
    /// `after` | `same_slot` | `before` | `unresolved` | `pending`.
    #[serde(default = "verdict_pending")]
    pub verdict: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trader_slot: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub our_slot: Option<u64>,
    /// Position within the block, when we had to look. Lower = earlier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trader_block_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub our_block_index: Option<u32>,
}

fn verdict_pending() -> String {
    "pending".to_owned()
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
    /// Light RPC (slots). Same endpoint as the rest of central.
    rpc_url: String,
    /// Heavy RPC (`getBlock`) — only used for same-slot tie-breaks.
    block_rpc_url: String,
}

impl CopyTradeStore {
    pub fn open(db_path: &Path, rpc_url: String, block_rpc_url: String) -> anyhow::Result<Self> {
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
                rpc_url,
                block_rpc_url,
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

        // Resolve the verdict once, in the background.
        {
            let mut g = self.inner.in_flight.lock().unwrap();
            if !g.insert(trade.buy_sig.clone()) {
                return;
            }
        }
        let inner = Arc::clone(&self.inner);
        let store = self.clone();
        tokio::spawn(async move {
            let sig = trade.buy_sig.clone();
            if let Err(e) = store.resolve_verdict(&key).await {
                tracing::debug!("[copytrades] verdict resolve failed sig={sig}: {e:#}");
            }
            inner.in_flight.lock().unwrap().remove(&sig);
        });
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
            if let Ok(t) = bincode::deserialize::<CopyTrade>(&v) {
                out.push(t);
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

    /// Aggregate stats over the whole DB — the numbers that decide whether
    /// this bot is safe to report to Temporal.
    pub fn summary(&self) -> serde_json::Value {
        let (mut before, mut same, mut after, mut unresolved) = (0u64, 0u64, 0u64, 0u64);
        let (mut closed, mut wins, mut pnl) = (0u64, 0u64, 0f64);
        for kv in self.inner.db.iter() {
            let Ok((_, v)) = kv else { continue };
            let Ok(t) = bincode::deserialize::<CopyTrade>(&v) else { continue };
            match t.verdict.as_str() {
                "before" => before += 1,
                "same_slot" => same += 1,
                "after" => after += 1,
                _ => unresolved += 1,
            }
            if let Some(p) = t.pnl_sol {
                closed += 1;
                pnl += p;
                if p > 0.0 {
                    wins += 1;
                }
            }
        }
        let judged = before + same + after;
        serde_json::json!({
            "total": self.count(),
            "front_run": before,
            "same_slot": same,
            "backrun": after,
            "unresolved": unresolved,
            // The number that gates reporting this bot to Temporal.
            "front_run_pct": if judged > 0 { before as f64 * 100.0 / judged as f64 } else { 0.0 },
            "closed": closed,
            "wins": wins,
            "win_pct": if closed > 0 { wins as f64 * 100.0 / closed as f64 } else { 0.0 },
            "pnl_sol": pnl,
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
            .and_then(|v| bincode::deserialize(&v).ok())
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

    /// Decide whether we backran or front-ran the trader.
    async fn resolve_verdict(&self, key: &str) -> anyhow::Result<()> {
        tokio::time::sleep(RESOLVE_DELAY).await;
        let Some(mut t) = self.get(key) else { return Ok(()) };

        let rpc =
            RpcClient::new_with_commitment(self.inner.rpc_url.clone(), CommitmentConfig::confirmed());
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let slot_of = |sig: &str| {
            let sig = sig.to_owned();
            let rpc = &rpc;
            async move {
                let parsed: solana_sdk::signature::Signature = sig.parse().ok()?;
                rpc.get_transaction_with_config(&parsed, cfg).await.ok().map(|r| r.slot)
            }
        };

        t.trader_slot = slot_of(&t.trader_sig).await;
        t.our_slot = slot_of(&t.buy_sig).await;

        t.verdict = match (t.trader_slot, t.our_slot) {
            // One of them never landed — nothing to compare.
            (None, _) | (_, None) => "unresolved".to_owned(),
            (Some(their), Some(ours)) if ours > their => "after".to_owned(),
            (Some(their), Some(ours)) if ours < their => "before".to_owned(),
            // Same slot: order inside the block decides it. Only here do we
            // pay for a getBlock, and it goes to the heavy-RPC endpoint.
            (Some(slot), Some(_)) => {
                match self.block_order(slot, &t.trader_sig, &t.buy_sig).await {
                    Some((their_idx, our_idx)) => {
                        t.trader_block_index = Some(their_idx);
                        t.our_block_index = Some(our_idx);
                        if our_idx > their_idx { "after".to_owned() } else { "before".to_owned() }
                    }
                    None => "same_slot".to_owned(),
                }
            }
        };
        self.put(key, &t)?;
        if t.verdict == "before" {
            // Loud: this is the failure mode the whole test phase exists for.
            println!(
                "[copytrades] FRONT-RUN buy={} trader={} our_slot={:?} trader_slot={:?}",
                t.buy_sig, t.trader_sig, t.our_slot, t.trader_slot
            );
        }
        Ok(())
    }

    /// Positions of two signatures within one block, if both are present.
    async fn block_order(&self, slot: u64, a: &str, b: &str) -> Option<(u32, u32)> {
        let rpc = RpcClient::new_with_commitment(
            self.inner.block_rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );
        let cfg = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(false),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let block = rpc.get_block_with_config(slot, cfg).await.ok()?;
        let sigs = block.signatures?;
        let ia = sigs.iter().position(|s| s == a)? as u32;
        let ib = sigs.iter().position(|s| s == b)? as u32;
        Some((ia, ib))
    }
}
