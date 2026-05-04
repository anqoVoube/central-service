use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmAccounts {
    pub coin_vault_mint: String,
    pub coin_vault: String,
    pub pc_vault_mint: String,
    pub pc_vault: String,
    /// Decimals of the non-WSOL (token) side. Defaults to 6 if absent in old docs.
    #[serde(default = "default_decimals")]
    pub token_decimals: u8,
}

fn default_decimals() -> u8 {
    6
}
