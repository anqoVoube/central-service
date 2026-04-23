use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

pub const COIN_CREATOR_OFFSET: usize = 211;
pub const COIN_CREATOR_END: usize = 243;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpFunAccounts {
    pub base_mint: String,
    pub quote_mint: String,
    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
    pub coin_creator: String,
    pub owner_program: String,
}

pub fn parse_coin_creator(data: &[u8]) -> Option<Pubkey> {
    if data.len() < COIN_CREATOR_END {
        return None;
    }
    Pubkey::try_from(&data[COIN_CREATOR_OFFSET..COIN_CREATOR_END]).ok()
}
