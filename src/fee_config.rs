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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeConfig {
    /// Sorted ascending by `max_sol_lamports`. Lookup is linear (fixed
    /// at ~5 entries per operator spec).
    pub fee_table: Vec<FeeBucket>,
}

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
