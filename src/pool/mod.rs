use serde::{Deserialize, Serialize};

pub mod pump_fun;
pub mod raydium_amm;
pub mod raydium_cpmm;

pub use pump_fun::PumpFunAccounts;
pub use raydium_amm::RaydiumAmmAccounts;
pub use raydium_cpmm::RaydiumCpmmAccounts;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "pool_type", content = "accounts", rename_all = "snake_case")]
pub enum PoolAccounts {
    RaydiumAmm(RaydiumAmmAccounts),
    RaydiumCpmm(RaydiumCpmmAccounts),
    PumpFun(PumpFunAccounts),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtaStatus {
    Pending,
    Confirmed,
}

impl Default for AtaStatus {
    fn default() -> Self {
        // Default to Confirmed so any pool doc lacking an explicit
        // `ata_status` field reaches bots via `init.pools`. The on-chain
        // ATA creation tx is still spawned by `discover::run` (via
        // `ata::create`) on best-effort — visibility-to-bots no longer
        // waits for it to succeed.
        Self::Confirmed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDoc {
    pub pool: String,
    #[serde(flatten)]
    pub accounts: PoolAccounts,
    #[serde(default)]
    pub ata_status: AtaStatus,
    #[serde(default)]
    pub ata_attempts: i32,
    /// Token display name (Dexscreener `name`). `None` if lookup failed
    /// or pool was inserted before the field existed.
    #[serde(default)]
    pub token_name: Option<String>,
    /// Token display symbol (Dexscreener `symbol`). Same fallback rules.
    #[serde(default)]
    pub token_symbol: Option<String>,
    /// Pool creation time in unix-ms, read from Dexscreener's `pairCreatedAt`
    /// for the pair whose `pairAddress` matches this pool. `None` when the
    /// pair isn't indexed yet (very fresh pools) or the Dexscreener fetch
    /// failed. Stored once at discovery; never refreshed.
    #[serde(default)]
    pub pair_created_at_ms: Option<i64>,
    /// Pool-specific CU ceiling derived from a one-shot 0.0001 SOL buy
    /// measurement: `ceil(meta.compute_units_consumed × 1.01)`. The bot
    /// reads this and uses it in place of the static `CU_LIMIT_*` constants
    /// when present (per-pool optimisation). `None` for pools that haven't
    /// been measured yet — bot falls back to its static constant.
    /// Re-measured on schedule (see `cu_measured_at`).
    #[serde(default)]
    pub compute_unit_limit: Option<i32>,
    /// Timestamp of the last successful CU measurement. Measurement script
    /// skips pools whose value is recent (< 30 days). `None` for unmeasured.
    #[serde(default)]
    pub cu_measured_at: Option<mongodb::bson::DateTime>,
    /// Override the 7-day age filter on the WS `init.pools` payload. When
    /// `true`, the pool is shipped to bots regardless of
    /// `pair_created_at_ms`. Used for high-value long-lived pools we want
    /// to keep trading past the freshness window. `false` (the
    /// `#[serde(default)]` for missing fields) → age filter applies.
    #[serde(default)]
    pub is_unique: bool,
}
