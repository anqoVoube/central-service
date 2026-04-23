use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmAccounts {
    pub coin_vault_mint: String,
    pub coin_vault: String,
    pub pc_vault_mint: String,
    pub pc_vault: String,
}
