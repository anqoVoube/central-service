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
        Self::Pending
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
}
