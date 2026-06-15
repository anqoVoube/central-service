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
    /// Dynamic fire profiles for Jito + Harmonic. Both senders share
    /// the same profiles — when a dump's slot leader is TP, both fire
    /// every variation in `tp`; otherwise both fire every variation
    /// in `def`. Other senders (helius_rpc / jet / paid / band-1) stay
    /// compile-time.
    pub fire_profiles: FireProfiles,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FireProfiles {
    /// Budget multiplier × 10. `30` = ×3.0. Replaces the prior
    /// hard-coded `budget_tenths = 20` for Jito + Harmonic. Applies
    /// to BOTH `tp` and `def` profiles.
    pub multiplier_x10: u32,
    /// Per-tx ceiling on `fee + tip` in lamports. `None` → bot falls
    /// back to its compile-time `HARMONIC_JITO_TOTAL_LAMPORTS_PER_TX`
    /// const (~$33 at SOL=$66). Acts as the sum cap inside
    /// `dynamic_split_for_slot`; raising it lets a 30/200 split scale
    /// up before the proportional clamp kicks in.
    #[serde(default)]
    pub per_tx_cap_lamports: Option<u64>,
    /// TP-leader profile — fires when the dump's leader is in the
    /// tip-priority validator set.
    pub tp: ProfileVariations,
    /// Default profile — fires on all other leaders.
    pub def: ProfileVariations,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileVariations {
    /// Operator-defined (fee%, tip%) variations. Each entry = one
    /// pre-baked tx fired per sender per dump. Dynamic count, capped at
    /// `MAX_DYNAMIC_SLOTS - 1` (one slot reserved for the FeeOnly
    /// variation when `fee_only = true`).
    pub splits: Vec<FeeTipSplit>,
    /// When true, an additional FeeOnly (100% fee, 0% tip) variation
    /// is appended to `splits` at prebuild time.
    pub fee_only: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeTipSplit {
    /// % of budget allocated to priority fee. `100` = 100%.
    pub fee_pct: u32,
    /// % of budget allocated to tip. `200` = 200%.
    pub tip_pct: u32,
}

/// Maximum number of dynamic slots per sender. Used both as the
/// validator's upper bound and as the bot's `VARIANTS_DYNAMIC` slice
/// size. `splits.len() + (fee_only ? 1 : 0) ≤ MAX_DYNAMIC_SLOTS`.
pub const MAX_DYNAMIC_SLOTS: usize = 8;

/// Defaults seeded into sled on first boot. Mirrors what the bot's
/// `statics::default_fee_config()` returns — keep these in sync. Sled is
/// authoritative once populated; this is only the bootstrap.
pub fn default_fee_config() -> FeeConfig {
    FeeConfig {
        fee_table: vec![
            FeeBucket { max_sol_lamports: 3_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 5_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 7_000_000_000, fee_bps: 150 },
            FeeBucket { max_sol_lamports: 10_000_000_000, fee_bps: 200 },
            FeeBucket { max_sol_lamports: 20_000_000_000, fee_bps: 300 },
        ],
        min_liq_dump_pct: 0.05,
        fire_profiles: FireProfiles {
            multiplier_x10: 30,
            per_tx_cap_lamports: None,
            tp: ProfileVariations {
                // Per operator default: TP profile = [80/20, 0/100],
                // fee_only disabled.
                splits: vec![
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                    FeeTipSplit { fee_pct: 0, tip_pct: 100 },
                ],
                fee_only: false,
            },
            def: ProfileVariations {
                // Per operator default: DEF profile = [20/80, 80/20],
                // fee_only enabled.
                splits: vec![
                    FeeTipSplit { fee_pct: 20, tip_pct: 80 },
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                ],
                fee_only: true,
            },
        },
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
    validate_fire_profiles(&cfg.fire_profiles)?;
    Ok(())
}

fn validate_fire_profiles(fp: &FireProfiles) -> anyhow::Result<()> {
    if fp.multiplier_x10 == 0 || fp.multiplier_x10 > 100 {
        anyhow::bail!(
            "fire_profiles.multiplier_x10 = {} must be in (0, 100]",
            fp.multiplier_x10
        );
    }
    if let Some(cap) = fp.per_tx_cap_lamports {
        if cap == 0 {
            anyhow::bail!("fire_profiles.per_tx_cap_lamports = 0 — would gate every fire");
        }
        if cap > 5_000_000_000 {
            anyhow::bail!(
                "fire_profiles.per_tx_cap_lamports = {cap} lamports (>5 SOL) looks like a typo"
            );
        }
    }
    validate_variations("fire_profiles.tp", &fp.tp)?;
    validate_variations("fire_profiles.def", &fp.def)?;
    // At least ONE leader class must have at least one variation that
    // actually fires (splits or fee_only). Otherwise Jito + Harmonic
    // become silent.
    let tp_count = fp.tp.splits.len() + (fp.tp.fee_only as usize);
    let def_count = fp.def.splits.len() + (fp.def.fee_only as usize);
    if tp_count == 0 && def_count == 0 {
        anyhow::bail!(
            "fire_profiles: both tp and def have zero variations — Jito + Harmonic would be silent"
        );
    }
    Ok(())
}

fn validate_variations(label: &str, p: &ProfileVariations) -> anyhow::Result<()> {
    // Reserve one slot for fee_only when enabled.
    let max_splits = MAX_DYNAMIC_SLOTS - (p.fee_only as usize);
    if p.splits.len() > max_splits {
        anyhow::bail!(
            "{label}.splits has {} entries — max {} when fee_only={}",
            p.splits.len(),
            max_splits,
            p.fee_only
        );
    }
    for (i, s) in p.splits.iter().enumerate() {
        if s.fee_pct > 500 || s.tip_pct > 500 {
            anyhow::bail!(
                "{label}.splits[{i}]: fee_pct/tip_pct ≤ 500 (got fee={} tip={})",
                s.fee_pct,
                s.tip_pct
            );
        }
        if s.fee_pct == 0 && s.tip_pct == 0 {
            anyhow::bail!(
                "{label}.splits[{i}]: fee_pct and tip_pct both 0 — remove the entry instead"
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
