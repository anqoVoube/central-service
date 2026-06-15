//! Server-side fee config: a JSON file on disk that central
//! auto-creates with defaults on first start, then serves to bots via
//! `GET /fee-config.json`. Bots fetch once at startup and treat the
//! result as static (no WS broadcast, no live reload).
//!
//! To change settings: edit the file on central, then restart bots.

use std::{
    fs,
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
}
