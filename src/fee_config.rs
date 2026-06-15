//! Runtime-editable buy-side fee table.
//!
//! Replaces the bot's hardcoded fee tier ladder in
//! `services/prebuild.rs::build_one_rung` with a sled-backed config owned
//! by central-service. Operator edits via the dashboard `/config` page →
//! dashboard POSTs `/fee-config` → central persists to sled + broadcasts
//! `FeeConfigChanged` via WS → every connected bot updates its in-memory
//! `Arc<ArcSwap<FeeConfig>>` and signals a full prebuild rebuild so the
//! next dump dispatch consumes signed rungs built against the new table.
//!
//! Seed semantics: if the sled tree is empty on startup (first boot, or
//! operator wiped `fee_config.db`), it's populated from `default_fee_config()`.
//! Once non-empty, the seed is ignored — sled is the source of truth.
//!
//! Scope: buys only. The fee table is read once per rung at prebuild
//! time, not on the hot path; sells continue to use the static
//! `TIP_LAMPORTS_SELL` + `cu_price_for_sell` constants.

use std::{path::Path, sync::Arc};

use anyhow::Context;
use std::sync::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::ws::ServerMsg;

const FEE_CONFIG_TREE: &str = "fee_config";
const FEE_CONFIG_KEY: &[u8] = b"current";

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeBucket {
    /// Upper bound (EXCLUSIVE) of this bucket in lamports. `fee_bps`
    /// applies when `quote_amount_in < max_sol_lamports`. Exclusive
    /// preserves the original hardcoded ladder's semantics so exact
    /// boundary values (e.g. 5 SOL exactly) keep the same fee bucket
    /// after the dynamic table replaces the static ladder.
    pub max_sol_lamports: u64,
    /// Fee budget in basis points of `quote_amount_in`. 100 = 1.0%.
    pub fee_bps: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FeeConfig {
    /// Sorted ascending by `max_sol_lamports`. Lookup is linear (fixed
    /// at ~5 entries per operator spec).
    pub fee_table: Vec<FeeBucket>,
    /// Minimum liquidity-dump fraction we act on (0..1). Mirrors the
    /// bot's `MIN_LIQ_DUMP` const that was hardcoded at client.rs.
    /// Default seeds at 0.05 (5%). Operator edits via the dashboard's
    /// /config page; bot reads via `FEE_CONFIG_HANDLE.load()`.
    pub min_liq_dump_pct: f64,
    /// Dynamic per-sender tunings for Jito + Harmonic. Each carries a
    /// budget multiplier and up to 2 slot definitions; each slot may
    /// fire on default leaders, TP leaders, both, or neither (via
    /// `Option<FeeTipSplit>`). Other senders (helius_rpc / jet /
    /// paid / band-1) stay compile-time; their TP routing flows through
    /// the static `TIP_PRIORITY_VARIANTS`.
    pub jito: SenderTuning,
    pub harmonic: SenderTuning,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SenderTuning {
    /// Budget multiplier × 10. `30` = ×3.0. Replaces the prior
    /// hard-coded `budget_tenths` constant for Jito + Harmonic (was
    /// `20` = ×2.0).
    pub multiplier_x10: u32,
    /// Up to 2 slot definitions. Each slot = up to 2 pre-baked txs (one
    /// for default leaders, one for TP). Missing entries (None) skip
    /// that slot on that leader class. Worker pool spawns a fixed 2
    /// tokio tasks per fire for these senders; tasks for None slots
    /// no-op.
    pub slots: Vec<SlotTuning>,
    /// Per-tx ceiling on `fee + tip` in lamports. `None` → fall back to
    /// the bot's compile-time `HARMONIC_JITO_TOTAL_LAMPORTS_PER_TX`
    /// const (~$33 at SOL=$66). `Some(n)` → use `n` directly. Acts as
    /// the sum cap inside `dynamic_split_for_slot`; raising it lets a
    /// 30/200 split scale up before the proportional clamp kicks in.
    #[serde(default)]
    pub per_tx_cap_lamports: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotTuning {
    pub default: Option<FeeTipSplit>,
    pub tp_leader: Option<FeeTipSplit>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeTipSplit {
    /// % of budget allocated to priority fee. `100` = 100%.
    pub fee_pct: u32,
    /// % of budget allocated to tip. `200` = 200%.
    pub tip_pct: u32,
}

/// Defaults seeded into sled on first boot. Mirrors what the bot's
/// `statics::default_fee_config()` returns — keep these in sync. Sled is
/// authoritative once populated; this is only the bootstrap.
pub fn default_fee_config() -> FeeConfig {
    let jito_harmonic = SenderTuning {
        multiplier_x10: 30,
        slots: vec![
            // Slot 0 — default: FeeOnly (100% fee, 0% tip). TP: off.
            SlotTuning {
                default: Some(FeeTipSplit { fee_pct: 100, tip_pct: 0 }),
                tp_leader: None,
            },
            // Slot 1 — default: Fee100Tip200; TP: Fee30Tip200.
            SlotTuning {
                default: Some(FeeTipSplit { fee_pct: 100, tip_pct: 200 }),
                tp_leader: Some(FeeTipSplit { fee_pct: 30, tip_pct: 200 }),
            },
        ],
        // None → bot falls back to HARMONIC_JITO_TOTAL_LAMPORTS_PER_TX
        // (~$33). Operator can override per-sender via the dashboard.
        per_tx_cap_lamports: None,
    };
    FeeConfig {
        fee_table: vec![
            FeeBucket { max_sol_lamports: 3_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 5_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 7_000_000_000, fee_bps: 150 },
            FeeBucket { max_sol_lamports: 10_000_000_000, fee_bps: 200 },
            FeeBucket { max_sol_lamports: 20_000_000_000, fee_bps: 300 },
        ],
        min_liq_dump_pct: 0.05,
        jito: jito_harmonic.clone(),
        harmonic: jito_harmonic,
    }
}

fn validate(cfg: &FeeConfig) -> anyhow::Result<()> {
    if cfg.fee_table.is_empty() {
        anyhow::bail!("fee_table must not be empty");
    }
    let mut prev: u64 = 0;
    for (i, b) in cfg.fee_table.iter().enumerate() {
        if b.max_sol_lamports == 0 {
            anyhow::bail!("fee_table[{i}].max_sol_lamports must be > 0");
        }
        if b.max_sol_lamports <= prev {
            anyhow::bail!(
                "fee_table[{i}].max_sol_lamports = {} must be strictly greater than previous {prev}",
                b.max_sol_lamports
            );
        }
        if b.fee_bps == 0 {
            anyhow::bail!("fee_table[{i}].fee_bps must be > 0");
        }
        if b.fee_bps > 10_000 {
            anyhow::bail!(
                "fee_table[{i}].fee_bps = {} exceeds 100% (10000 bps)",
                b.fee_bps
            );
        }
        prev = b.max_sol_lamports;
    }
    if !cfg.min_liq_dump_pct.is_finite() {
        anyhow::bail!("min_liq_dump_pct must be finite");
    }
    if cfg.min_liq_dump_pct <= 0.0 || cfg.min_liq_dump_pct >= 0.5 {
        anyhow::bail!(
            "min_liq_dump_pct = {} must be in (0, 0.5) — 0 disables, 0.5+ would clamp every gate",
            cfg.min_liq_dump_pct
        );
    }
    validate_sender_tuning("jito", &cfg.jito)?;
    validate_sender_tuning("harmonic", &cfg.harmonic)?;
    Ok(())
}

/// Per-sender tuning sanity. Multiplier in [×0.1, ×10.0]; up to 2 slots;
/// each split's fee_pct ≤ 500 (5×), tip_pct ≤ 500. Total per-slot
/// (fee+tip) up to 1000 — the sum cap downstream still clamps the
/// landed cost in lamports.
fn validate_sender_tuning(label: &str, t: &SenderTuning) -> anyhow::Result<()> {
    if t.multiplier_x10 == 0 || t.multiplier_x10 > 100 {
        anyhow::bail!(
            "{label}.multiplier_x10 = {} must be in (0, 100]",
            t.multiplier_x10
        );
    }
    if t.slots.is_empty() || t.slots.len() > 2 {
        anyhow::bail!("{label}.slots must have 1 or 2 entries (got {})", t.slots.len());
    }
    for (i, slot) in t.slots.iter().enumerate() {
        for (which, split) in [("default", slot.default), ("tp_leader", slot.tp_leader)] {
            if let Some(s) = split {
                if s.fee_pct > 500 || s.tip_pct > 500 {
                    anyhow::bail!(
                        "{label}.slots[{i}].{which}: fee_pct/tip_pct ≤ 500 (got fee={} tip={})",
                        s.fee_pct,
                        s.tip_pct
                    );
                }
                if s.fee_pct == 0 && s.tip_pct == 0 {
                    anyhow::bail!(
                        "{label}.slots[{i}].{which}: fee_pct and tip_pct both 0 — use None to skip"
                    );
                }
            }
        }
        if slot.default.is_none() && slot.tp_leader.is_none() {
            anyhow::bail!(
                "{label}.slots[{i}]: both default and tp_leader are None — remove the slot"
            );
        }
    }
    // Per-tx cap bound: 0 disables (we'd never fire); >5_000_000_000
    // (5 SOL ≈ $330 at SOL=$66) is almost certainly a typo.
    if let Some(cap) = t.per_tx_cap_lamports {
        if cap == 0 {
            anyhow::bail!("{label}.per_tx_cap_lamports = 0 — would gate every fire");
        }
        if cap > 5_000_000_000 {
            anyhow::bail!(
                "{label}.per_tx_cap_lamports = {cap} lamports (>5 SOL) looks like a typo"
            );
        }
    }
    Ok(())
}

/// Persistent + in-memory fee config. Bots fetch a wholesale snapshot
/// from `GET /fee-config.bin` at startup, then receive incremental
/// `fee_config_changed` broadcasts.
#[derive(Clone)]
pub struct FeeConfigStore {
    inner: Arc<Inner>,
}

struct Inner {
    tree: sled::Tree,
    bcast: broadcast::Sender<ServerMsg>,
    current: Mutex<FeeConfig>,
}

impl FeeConfigStore {
    pub fn open(
        db_path: &Path,
        bcast: broadcast::Sender<ServerMsg>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled fee_config db")?;
        let tree = db
            .open_tree(FEE_CONFIG_TREE)
            .context("open sled fee_config tree")?;
        let current = match tree.get(FEE_CONFIG_KEY).context("sled get fee_config")? {
            Some(bytes) => match bincode::deserialize::<FeeConfig>(&bytes) {
                Ok(cfg) => {
                    println!(
                        "[fee-config] loaded {} buckets from sled",
                        cfg.fee_table.len()
                    );
                    cfg
                }
                Err(e) => {
                    eprintln!(
                        "[fee-config] sled value decode failed ({e}); reseeding from defaults"
                    );
                    let cfg = default_fee_config();
                    let buf = bincode::serialize(&cfg).context("encode default fee_config")?;
                    tree.insert(FEE_CONFIG_KEY, buf).context("seed fee_config")?;
                    tree.flush().context("flush fee_config seed")?;
                    cfg
                }
            },
            None => {
                let cfg = default_fee_config();
                let buf = bincode::serialize(&cfg).context("encode default fee_config")?;
                tree.insert(FEE_CONFIG_KEY, buf).context("seed fee_config")?;
                tree.flush().context("flush fee_config seed")?;
                println!(
                    "[fee-config] seeded sled with {} default buckets",
                    cfg.fee_table.len()
                );
                cfg
            }
        };
        Ok(Self {
            inner: Arc::new(Inner {
                tree,
                bcast,
                current: Mutex::new(current),
            }),
        })
    }

    /// Replace the persisted + broadcast config. Validates first.
    pub fn set(&self, new: FeeConfig) -> anyhow::Result<()> {
        validate(&new)?;
        let buf = bincode::serialize(&new).context("encode fee_config")?;
        self.inner
            .tree
            .insert(FEE_CONFIG_KEY, buf)
            .context("sled insert fee_config")?;
        self.inner.tree.flush().context("sled flush")?;
        *self.inner.current.lock().expect("fee_config mutex") = new.clone();
        let _ = self
            .inner
            .bcast
            .send(ServerMsg::FeeConfigChanged { config: new.clone() });
        println!(
            "[fee-config] set {} buckets — broadcast sent",
            new.fee_table.len()
        );
        Ok(())
    }

    /// Returns `bincode::serialize(&FeeConfig)` for `GET /fee-config.bin`.
    /// Bots decode this at startup + on WS reconnect to refresh in-memory state.
    pub fn snapshot_bincode(&self) -> anyhow::Result<Vec<u8>> {
        let cfg = self.inner.current.lock().expect("fee_config mutex").clone();
        bincode::serialize(&cfg).context("encode fee_config snapshot")
    }

    /// Current cached config, for diagnostics. Hot-path callers should
    /// read the bot-side `FEE_CONFIG_HANDLE` ArcSwap, not this.
    #[allow(dead_code)]
    pub fn get(&self) -> FeeConfig {
        self.inner.current.lock().expect("fee_config mutex").clone()
    }
}
