use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumCpmmAccounts {
    pub token_0_mint: String,
    pub token_1_mint: String,
    pub token_0_vault: String,
    pub token_1_vault: String,
    pub observation_key: String,
    pub amm_config: String,
    /// Decimals of the non-WSOL (token) side. Defaults to 6 if absent in old docs.
    #[serde(default = "default_decimals")]
    pub token_decimals: u8,
}

fn default_decimals() -> u8 {
    6
}
