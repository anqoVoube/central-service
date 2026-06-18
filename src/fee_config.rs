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
    pub min_liq_dump_pct: f64,
    pub fire_profiles: FireProfiles,
    /// Prebuilt buy slippage as a fraction of the observed dump, in
    /// basis points. Mirrors `statics::FeeConfig::buy_slip_pct_of_dump_bps`.
    /// Default 1667 = 16.67% of dump (matches the prior fixed divisor=6).
    /// Hot path: `slip_bps = drop_bps × this / 10_000`, clamped at 2000.
    #[serde(default = "default_buy_slip_pct_of_dump_bps")]
    pub buy_slip_pct_of_dump_bps: u32,
}

pub fn default_buy_slip_pct_of_dump_bps() -> u32 {
    1667
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FireProfiles {
    pub multiplier_x10: u32,
    #[serde(default)]
    pub per_tx_cap_lamports: Option<u64>,
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
    if cfg.fee_table.is_empty() {
        anyhow::bail!("fee_table must have at least one bucket");
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
            if s.fee_pct > 200 || s.tip_pct > 200 {
                anyhow::bail!(
                    "fire_profiles.{name}.splits[{i}]: fee_pct={} tip_pct={} — each must be in [0, 200]",
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
        min_liq_dump_pct: 0.05,
        fire_profiles: FireProfiles {
            multiplier_x10: 30,
            per_tx_cap_lamports: None,
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
