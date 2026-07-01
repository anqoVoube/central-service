use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

const POOL_DISC: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];

const BASE_MINT_OFF: usize = 43;
const QUOTE_MINT_OFF: usize = 75;
const POOL_BASE_VAULT_OFF: usize = 139;
const POOL_QUOTE_VAULT_OFF: usize = 171;
const COIN_CREATOR_OFF: usize = 211;
// Minimum bytes needed for pool-data validity. Must be > IS_CASHBACK_COIN_OFF
// so `data.get(244)` returns Some(_) rather than None (silent false on
// mayhem/cashback detection).
pub const POOL_DATA_MIN: usize = 245;
// Byte 243 = is_mayhem_mode. Determines which set of fee recipients the
// pAMM program accepts on this pool:
//   false → GlobalConfig.protocol_fee_recipients[8]  (the "normal" set)
//   true  → GlobalConfig.reserved_fee_recipient + reserved_fee_recipients[7]
// Picking from the wrong set → Anchor 6013 InvalidProtocolFeeRecipient at
// pump-amm/src/instructions/swap/mod.rs:160. See CLAUDE.md "Aggregator
// decoders" section notes.
pub const IS_MAYHEM_MODE_OFF: usize = 243;
// Offset of the is_cashback_coin bool field in the pool account.
pub const IS_CASHBACK_COIN_OFF: usize = 244;

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
    /// See `IS_MAYHEM_MODE_OFF`. Immutable per pool (set at creation) so
    /// stored once at discovery/backfill; drives protocol_fee_recipient
    /// selection in the swap ix builder. Default `false` — pre-Phase-8
    /// pool docs lack the field but are also non-mayhem by construction
    /// (mayhem-mode pools began appearing after the April-2025 upgrade).
    #[serde(default)]
    pub is_mayhem_mode: bool,
    /// Decimals of the base (token) mint. Defaults to 6 if absent in old docs.
    #[serde(default = "default_decimals")]
    pub token_decimals: u8,
}

fn default_decimals() -> u8 {
    6
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

pub fn parse_is_cashback_coin(data: &[u8]) -> bool {
    data.get(IS_CASHBACK_COIN_OFF).copied().unwrap_or(0) != 0
}

pub fn parse_is_mayhem_mode(data: &[u8]) -> bool {
    data.get(IS_MAYHEM_MODE_OFF).copied().unwrap_or(0) != 0
}
