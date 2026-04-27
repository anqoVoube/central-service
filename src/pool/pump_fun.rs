use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

const POOL_DISC: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];

const BASE_MINT_OFF: usize = 43;
const QUOTE_MINT_OFF: usize = 75;
const POOL_BASE_VAULT_OFF: usize = 139;
const POOL_QUOTE_VAULT_OFF: usize = 171;
const COIN_CREATOR_OFF: usize = 211;
pub const POOL_DATA_MIN: usize = 243;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpFunAccounts {
    pub base_mint: String,
    pub quote_mint: String,
    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
    pub coin_creator: String,
    pub owner_program: String,
    #[serde(default)]
    pub is_cashback: bool,
}

pub struct ParsedPool {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub coin_creator: Pubkey,
}

fn pubkey_at(data: &[u8], off: usize) -> anyhow::Result<Pubkey> {
    Pubkey::try_from(&data[off..off + 32]).context("slice → pubkey")
}

pub fn parse_pool(data: &[u8]) -> anyhow::Result<ParsedPool> {
    if data.len() < POOL_DATA_MIN {
        anyhow::bail!("pool data too short: {} bytes (need >= {POOL_DATA_MIN})", data.len());
    }
    if data[0..8] != POOL_DISC {
        anyhow::bail!("pool discriminator mismatch — not a PumpFun pAMM pool");
    }
    Ok(ParsedPool {
        base_mint: pubkey_at(data, BASE_MINT_OFF)?,
        quote_mint: pubkey_at(data, QUOTE_MINT_OFF)?,
        pool_base_token_account: pubkey_at(data, POOL_BASE_VAULT_OFF)?,
        pool_quote_token_account: pubkey_at(data, POOL_QUOTE_VAULT_OFF)?,
        coin_creator: pubkey_at(data, COIN_CREATOR_OFF)?,
    })
}

pub fn parse_coin_creator(data: &[u8]) -> Option<Pubkey> {
    if data.len() < POOL_DATA_MIN {
        return None;
    }
    pubkey_at(data, COIN_CREATOR_OFF).ok()
}
