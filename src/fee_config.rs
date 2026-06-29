//! Server-side fee config: a JSON file on disk that central
//! auto-creates with defaults on first start, then serves to bots via
//! `GET /fee-config.json`. Bots fetch once at startup and treat the
//! result as static (no WS broadcast, no live reload).
//!
//! To change settings: edit the file on central, then restart bots.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeBucket {
    /// Upper bound (EXCLUSIVE) of this bucket in lamports.
    pub max_sol_lamports: u64,
    /// Fee budget in basis points of `quote_amount_in`.
    pub fee_bps: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FeeConfig {
    pub fee_table: Vec<FeeBucket>,
    pub fire_profiles: FireProfiles,
    /// Prebuilt buy slippage as a fraction of the observed dump, in
    /// basis points. Mirrors `statics::FeeConfig::buy_slip_pct_of_dump_bps`.
    #[serde(default = "default_buy_slip_pct_of_dump_bps")]
    pub buy_slip_pct_of_dump_bps: u32,
    /// Buy-size tier table keyed on the observed liquidity-dump
    /// fraction. Sorted ascending by `min_liq_dump_pct`. The lowest
    /// tier's threshold IS the bot-wide min-liq-dump floor (replaces
    /// the prior `min_liq_dump_pct` field). Mirrors
    /// `statics::FeeConfig::buy_size_tiers`.
    #[serde(default = "default_buy_size_tiers")]
    pub buy_size_tiers: Vec<BuySizeTier>,
    /// Per-rung minimum buy size (lamports). Dashboard surfaces this
    /// as a "min sol" input on fee_table tier 1. Default 225M (0.225 SOL ~ $20).
    #[serde(default = "default_rung_min_sol_lamports")]
    pub rung_min_sol_lamports: u64,
    /// Temporary-tip-priority (TTP) lifetime in seconds. When an operator
    /// marks a validator "ttp" on the dashboard, central treats it as
    /// tip-priority for this many seconds, then auto-reverts it to default.
    /// Dashboard-configurable; default 1800 (30 min).
    #[serde(default = "default_ttp_ttl_secs")]
    pub ttp_ttl_secs: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct BuySizeTier {
    pub min_liq_dump_pct: f64,
    pub buy_size_bps: u32,
}

pub fn default_buy_slip_pct_of_dump_bps() -> u32 {
    1667
}

/// Three-tier default: 3%→15%, 6%→30%, 25%→50%. Below 3% no fire.
pub fn default_buy_size_tiers() -> Vec<BuySizeTier> {
    vec![
        BuySizeTier { min_liq_dump_pct: 0.03, buy_size_bps: 1500 },
        BuySizeTier { min_liq_dump_pct: 0.06, buy_size_bps: 3000 },
        BuySizeTier { min_liq_dump_pct: 0.25, buy_size_bps: 5000 },
    ]
}

/// Default rung floor — 0.225 SOL (~$20 at $89/SOL).
pub fn default_rung_min_sol_lamports() -> u64 {
    225_000_000
}

/// Default TTP (temporary tip-priority) lifetime — 30 minutes.
pub fn default_ttp_ttl_secs() -> u64 {
    1800
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FireProfiles {
    pub multiplier_x10: u32,
    /// Per-tx ceiling for DEFAULT senders (non-Jito, non-Harmonic).
    /// `None` → bot falls back to `MAX_TOTAL_LAMPORTS_PER_TX` ($22).
    /// Surfaced in dashboard as "max default sender fee ($)".
    #[serde(default, alias = "per_tx_cap_lamports")]
    pub per_tx_cap_lamports_default: Option<u64>,
    /// Per-tx ceiling for Jito + Harmonic. `None` → bot falls back to
    /// `HARMONIC_JITO_TOTAL_LAMPORTS_PER_TX` ($33). Surfaced in dashboard
    /// as "max jito+harmonic fee ($)".
    #[serde(default)]
    pub per_tx_cap_lamports_jito_harmonic: Option<u64>,
    /// Per-COMPONENT priority-fee ceiling for any single variant tx.
    /// `None` → bot falls back to hardcoded `MAX_PRIORITY_FEE_LAMPORTS`
    /// (0.333 SOL). Dashboard label: "max priority fee per tx ($)".
    /// Jito + Harmonic bypass this (sum cap only for them).
    #[serde(default)]
    pub max_priority_fee_lamports: Option<u64>,
    /// Per-COMPONENT tip ceiling for any single variant tx. `None` →
    /// bot falls back to hardcoded `MAX_TIP_LAMPORTS` (0.333 SOL).
    /// Dashboard label: "max tip per tx ($)". Jito + Harmonic bypass.
    #[serde(default)]
    pub max_tip_lamports: Option<u64>,
    pub tp: ProfileVariations,
    pub def: ProfileVariations,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileVariations {
    pub splits: Vec<FeeTipSplit>,
    pub fee_only: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeTipSplit {
    pub fee_pct: u32,
    pub tip_pct: u32,
}

/// Mirrors `statics::MAX_DYNAMIC_SLOTS` in the bot. Each profile may
/// configure up to 8 explicit splits; the bot's prebuild allocates
/// exactly 8 worker slots per dynamic sender, so anything past this is
/// silently dropped at fire time. Validate at central so operators get
/// a 400 instead of mystery missing slots.
pub const MAX_DYNAMIC_SLOTS: usize = 8;

/// Reject configs that would silently misbehave at the bot. Called from
/// `FeeConfigFile::open` (catches hand-edited files at central startup)
/// AND from the dashboard POST path (rejects bad dashboard input with
/// 400 before persisting).
pub fn validate(cfg: &FeeConfig) -> anyhow::Result<()> {
    if cfg.fire_profiles.multiplier_x10 == 0 {
        anyhow::bail!("multiplier_x10 must be > 0 (0 zeroes every dynamic budget)");
    }
    if cfg.buy_slip_pct_of_dump_bps == 0 {
        anyhow::bail!(
            "buy_slip_pct_of_dump_bps must be > 0 (0 = zero slippage, every buy reverts)"
        );
    }
    if cfg.buy_slip_pct_of_dump_bps > 10_000 {
        anyhow::bail!(
            "buy_slip_pct_of_dump_bps={} exceeds 10000 (100% of dump); refusing",
            cfg.buy_slip_pct_of_dump_bps
        );
    }
    if cfg.fire_profiles.multiplier_x10 > 1000 {
        anyhow::bail!(
            "multiplier_x10={} is absurdly high (cap: 1000 = 100×); refusing",
            cfg.fire_profiles.multiplier_x10
        );
    }
    // Per-tx cap sanity guards. None = "use compiled default" (bot side).
    // Some(0) would silently zero every fire's budget — reject.
    if let Some(0) = cfg.fire_profiles.per_tx_cap_lamports_default {
        anyhow::bail!(
            "per_tx_cap_lamports_default=0 disables every default-sender fire; refusing"
        );
    }
    if let Some(0) = cfg.fire_profiles.per_tx_cap_lamports_jito_harmonic {
        anyhow::bail!(
            "per_tx_cap_lamports_jito_harmonic=0 disables every Jito/Harmonic fire; refusing"
        );
    }
    if cfg.fee_table.is_empty() {
        anyhow::bail!("fee_table must have at least one bucket");
    }
    if cfg.rung_min_sol_lamports == 0 {
        anyhow::bail!("rung_min_sol_lamports must be > 0");
    }
    if cfg.ttp_ttl_secs < 60 || cfg.ttp_ttl_secs > 86_400 {
        anyhow::bail!(
            "ttp_ttl_secs={} must be in [60, 86400] (1 min .. 24 h)",
            cfg.ttp_ttl_secs
        );
    }
    let last_max = cfg
        .fee_table
        .last()
        .map(|b| b.max_sol_lamports)
        .unwrap_or(0);
    if cfg.rung_min_sol_lamports >= last_max {
        anyhow::bail!(
            "rung_min_sol_lamports ({}) must be strictly less than fee_table's last bucket's max_sol_lamports ({}); otherwise every buy clamps to a single value",
            cfg.rung_min_sol_lamports,
            last_max
        );
    }
    let mut prev_max: u64 = 0;
    for (i, b) in cfg.fee_table.iter().enumerate() {
        if b.max_sol_lamports <= prev_max {
            anyhow::bail!(
                "fee_table[{i}].max_sol_lamports={} is not strictly increasing (prev={})",
                b.max_sol_lamports,
                prev_max
            );
        }
        if b.fee_bps > 10_000 {
            anyhow::bail!(
                "fee_table[{i}].fee_bps={} exceeds 10000 (100%)",
                b.fee_bps
            );
        }
        prev_max = b.max_sol_lamports;
    }
    for (name, p) in [("tp", &cfg.fire_profiles.tp), ("def", &cfg.fire_profiles.def)]
    {
        let effective = p.splits.len() + (p.fee_only as usize);
        if effective == 0 {
            anyhow::bail!(
                "fire_profiles.{name}: profile is empty (no splits and fee_only=false) — sender would never fire"
            );
        }
        if effective > MAX_DYNAMIC_SLOTS {
            anyhow::bail!(
                "fire_profiles.{name}: effective slots ({}) exceeds MAX_DYNAMIC_SLOTS ({MAX_DYNAMIC_SLOTS}). \
                 splits.len()={} + fee_only={}",
                effective,
                p.splits.len(),
                p.fee_only as u8,
            );
        }
        for (i, s) in p.splits.iter().enumerate() {
            // Per-slot percentages multiply the budget — a 300 here means
            // "3× total_budget for this component before per_tx_cap clamps it".
            // Cap at 1000% (10×) per component as an absurd-high guard;
            // operator values ≤ 500 are routine.
            if s.fee_pct > 1000 || s.tip_pct > 1000 {
                anyhow::bail!(
                    "fire_profiles.{name}.splits[{i}]: fee_pct={} tip_pct={} — each must be in [0, 1000]",
                    s.fee_pct,
                    s.tip_pct
                );
            }
            if s.fee_pct == 0 && s.tip_pct == 0 {
                anyhow::bail!(
                    "fire_profiles.{name}.splits[{i}]: both fee_pct and tip_pct are 0 — slot is a no-op"
                );
            }
        }
    }
    if cfg.buy_size_tiers.is_empty() {
        anyhow::bail!(
            "buy_size_tiers is empty — the lowest tier is the bot-wide min-liq-dump floor; with no tiers no fire ever happens"
        );
    }
    let mut prev_threshold: f64 = -1.0;
    for (i, t) in cfg.buy_size_tiers.iter().enumerate() {
        if !t.min_liq_dump_pct.is_finite()
            || t.min_liq_dump_pct <= 0.0
            || t.min_liq_dump_pct > 0.5
        {
            anyhow::bail!(
                "buy_size_tiers[{i}].min_liq_dump_pct={} must be in (0.0, 0.5]",
                t.min_liq_dump_pct
            );
        }
        if t.min_liq_dump_pct <= prev_threshold {
            anyhow::bail!(
                "buy_size_tiers[{i}].min_liq_dump_pct={} is not strictly increasing (prev={})",
                t.min_liq_dump_pct,
                prev_threshold
            );
        }
        if t.buy_size_bps == 0 {
            anyhow::bail!(
                "buy_size_tiers[{i}].buy_size_bps=0 — tier would never buy anything"
            );
        }
        if t.buy_size_bps > 10_000 {
            anyhow::bail!(
                "buy_size_tiers[{i}].buy_size_bps={} exceeds 10000 (100%); refusing",
                t.buy_size_bps
            );
        }
        prev_threshold = t.min_liq_dump_pct;
    }
    Ok(())
}

pub fn default_fee_config() -> FeeConfig {
    FeeConfig {
        fee_table: vec![
            FeeBucket { max_sol_lamports: 3_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 5_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 7_000_000_000, fee_bps: 150 },
            FeeBucket { max_sol_lamports: 10_000_000_000, fee_bps: 200 },
            FeeBucket { max_sol_lamports: 20_000_000_000, fee_bps: 300 },
        ],
        fire_profiles: FireProfiles {
            multiplier_x10: 30,
            per_tx_cap_lamports_default: None,
            per_tx_cap_lamports_jito_harmonic: None,
            max_priority_fee_lamports: None,
            max_tip_lamports: None,
            tp: ProfileVariations {
                splits: vec![
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                    FeeTipSplit { fee_pct: 0, tip_pct: 100 },
                ],
                fee_only: false,
            },
            def: ProfileVariations {
                splits: vec![
                    FeeTipSplit { fee_pct: 20, tip_pct: 80 },
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                ],
                fee_only: true,
            },
        },
        buy_slip_pct_of_dump_bps: default_buy_slip_pct_of_dump_bps(),
        buy_size_tiers: default_buy_size_tiers(),
        rung_min_sol_lamports: default_rung_min_sol_lamports(),
        ttp_ttl_secs: default_ttp_ttl_secs(),
    }
}

/// File-on-disk loader. Auto-seeds the file with `default_fee_config()`
/// if it doesn't exist. Always validates parse. Returns the raw file
/// bytes (re-read each request) so manual edits land without restart.
#[derive(Clone)]
pub struct FeeConfigFile {
    path: PathBuf,
}

impl FeeConfigFile {
    /// Ensures the file at `path` exists with valid JSON. If missing,
    /// writes `default_fee_config()` formatted with 2-space indent.
    /// If present, parses to verify it's valid JSON+schema (logs +
    /// returns the path; corrupted files do NOT get auto-overwritten —
    /// surface the error to the operator).
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            let cfg = default_fee_config();
            // Sanity-check the built-in defaults — guards against a
            // future edit to `default_fee_config` slipping in a config
            // that the bot would silently reject.
            validate(&cfg).context("default fee_config failed validation")?;
            let json = serde_json::to_string_pretty(&cfg)
                .context("serialize default fee_config")?;
            fs::write(&path, &json)
                .with_context(|| format!("write default fee_config to {path:?}"))?;
            println!(
                "[fee-config] created {path:?} with defaults ({} buckets, mult_x10={}, tp.splits={}, def.splits={})",
                cfg.fee_table.len(),
                cfg.fire_profiles.multiplier_x10,
                cfg.fire_profiles.tp.splits.len(),
                cfg.fire_profiles.def.splits.len(),
            );
        } else {
            // Validate the existing file parses cleanly so operators
            // catch JSON errors at central startup instead of seeing
            // every bot crash on first restart.
            let bytes = fs::read(&path)
                .with_context(|| format!("read existing fee_config at {path:?}"))?;
            let cfg: FeeConfig = serde_json::from_slice(&bytes).with_context(|| {
                format!("existing fee_config at {path:?} is not valid JSON")
            })?;
            validate(&cfg).with_context(|| {
                format!("existing fee_config at {path:?} failed semantic validation")
            })?;
            println!(
                "[fee-config] using existing {path:?} ({} buckets, mult_x10={}, tp.splits={}, def.splits={})",
                cfg.fee_table.len(),
                cfg.fire_profiles.multiplier_x10,
                cfg.fire_profiles.tp.splits.len(),
                cfg.fire_profiles.def.splits.len(),
            );
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the file from disk each call so operator edits land
    /// without restarting central. Bots fetch at their own startup, so
    /// staleness during a save is bounded to the next bot restart.
    pub fn read_bytes(&self) -> std::io::Result<Vec<u8>> {
        fs::read(&self.path)
    }

    /// Atomically replace the config file. Writes to `<path>.tmp` first,
    /// fsyncs, then renames over the original. A central kill mid-write
    /// leaves the original file intact (the rename is atomic on POSIX
    /// filesystems). Used by `POST /fee-config.json`.
    pub fn write_atomic(&self, cfg: &FeeConfig) -> anyhow::Result<()> {
        // Reject silently-broken configs at the operator boundary (POST)
        // rather than letting them land on disk + propagate to bots.
        validate(cfg).context("fee_config failed validation; refusing to persist")?;
        let json = serde_json::to_string_pretty(cfg)
            .context("serialize fee_config")?;
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .with_context(|| format!("create tmp {tmp:?}"))?;
            f.write_all(json.as_bytes())
                .with_context(|| format!("write tmp {tmp:?}"))?;
            // sync_all on the file forces the data to disk before we
            // rename — without it, a kernel crash could leave the new
            // file empty while the rename succeeded.
            f.sync_all().with_context(|| format!("sync tmp {tmp:?}"))?;
        }
        fs::rename(&tmp, &self.path)
            .with_context(|| format!("rename {tmp:?} -> {:?}", self.path))?;
        // Best-effort sync the parent dir so the rename itself is
        // durable across power loss. Ignore errors — not all FS
        // (notably tmpfs) support fsync on a directory.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}
