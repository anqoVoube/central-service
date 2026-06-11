//! PumpFun pAMM buy-ix builder, ported from
//! `~/Work/supra-stop-loss/examples/rust/src/bin/swap/pump_fun.rs`.
//! Adapted to solana-sdk 2.x's single meta-crate import shape.
//!
//! Used by the `measure_cu` binary to build the same swap_ix the bot would
//! fire for a given pool, so the recorded `compute_units_consumed` reflects
//! the production-shape tx.

use std::str::FromStr;

use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

// =============================================================================
// Static program / config pubkeys (mirror of statics/mod.rs in the bot)
// =============================================================================

pub const PUMP_FUN: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const PUMP_FEE_PROGRAM: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
pub const PUMP_GLOBAL_CONFIG: &str = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";
pub const PUMP_PROTOCOL_FEE_RECIPIENT: &str = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
pub const PUMP_AMM_FEE_RECIPIENT: &str = "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD";

pub const WSOL: &str = "So11111111111111111111111111111111111111112";
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// `BuyExactQuoteIn` discriminator. Identical to the bot's constant.
pub const PUMP_FUN_PREFIX_BUY_EXACT_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];

/// `Buy` (exact-output) discriminator — ordinary buy. Empirically ~3k CU
/// cheaper than `BuyExactQuoteIn`. Identical accounts, swapped u64 args
/// (`base_amount_out`, `max_quote_amount_in`). Mirrors `PUMP_FUN_PREFIX_BUY`
/// in the bot's `statics/mod.rs`.
pub const PUMP_FUN_PREFIX_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];

pub fn pump_fun_pk() -> Pubkey { Pubkey::from_str(PUMP_FUN).unwrap() }
pub fn pump_fee_program_pk() -> Pubkey { Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap() }
pub fn pump_global_config_pk() -> Pubkey { Pubkey::from_str(PUMP_GLOBAL_CONFIG).unwrap() }
pub fn pump_protocol_fee_recipient_pk() -> Pubkey { Pubkey::from_str(PUMP_PROTOCOL_FEE_RECIPIENT).unwrap() }
pub fn pump_amm_fee_recipient_pk() -> Pubkey { Pubkey::from_str(PUMP_AMM_FEE_RECIPIENT).unwrap() }
pub fn wsol_pk() -> Pubkey { Pubkey::from_str(WSOL).unwrap() }
pub fn token_program_pk() -> Pubkey { Pubkey::from_str(TOKEN_PROGRAM).unwrap() }
pub fn ata_program_pk() -> Pubkey { Pubkey::from_str(ATA_PROGRAM).unwrap() }
pub fn system_program_pk() -> Pubkey { Pubkey::from_str(SYSTEM_PROGRAM).unwrap() }

// =============================================================================
// Helpers
// =============================================================================

pub fn find_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &ata_program_pk(),
    )
    .0
}

pub fn constant_product_out(delta_in: u64, reserve_in: u64, reserve_out: u64) -> u64 {
    let num = (delta_in as u128).saturating_mul(reserve_out as u128);
    let denom = (reserve_in as u128).saturating_add(delta_in as u128);
    if denom == 0 {
        return 0;
    }
    (num / denom) as u64
}

pub fn apply_slippage_floor(amount: u64, bps: u32) -> u64 {
    let bps = bps.min(10_000) as u128;
    ((amount as u128 * (10_000u128 - bps)) / 10_000u128) as u64
}

#[derive(Debug, Clone)]
pub struct PumpStaticPdas {
    pub event_authority: Pubkey,
    pub global_volume_accumulator: Pubkey,
    pub user_volume_accumulator: Pubkey,
    pub user_volume_accumulator_wsol_ata: Pubkey,
    pub fee_config: Pubkey,
    pub protocol_fee_recipient_ata: Pubkey,
    pub amm_fee_recipient: Pubkey,
    pub amm_fee_recipient_wsol_ata: Pubkey,
}

impl PumpStaticPdas {
    pub fn derive(wallet_pk: &Pubkey) -> Self {
        let pump = pump_fun_pk();
        let fee_program = pump_fee_program_pk();
        let token_prog = token_program_pk();
        let wsol = wsol_pk();

        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &pump);
        let (global_volume_accumulator, _) =
            Pubkey::find_program_address(&[b"global_volume_accumulator"], &pump);
        let (user_volume_accumulator, _) = Pubkey::find_program_address(
            &[b"user_volume_accumulator", wallet_pk.as_ref()],
            &pump,
        );
        let user_volume_accumulator_wsol_ata =
            find_ata(&user_volume_accumulator, &wsol, &token_prog);
        let (fee_config, _) =
            Pubkey::find_program_address(&[b"fee_config", pump.as_ref()], &fee_program);
        let protocol_fee_recipient_ata =
            find_ata(&pump_protocol_fee_recipient_pk(), &wsol, &token_prog);
        let amm_fee_recipient = pump_amm_fee_recipient_pk();
        let amm_fee_recipient_wsol_ata = find_ata(&amm_fee_recipient, &wsol, &token_prog);

        Self {
            event_authority,
            global_volume_accumulator,
            user_volume_accumulator,
            user_volume_accumulator_wsol_ata,
            fee_config,
            protocol_fee_recipient_ata,
            amm_fee_recipient,
            amm_fee_recipient_wsol_ata,
        }
    }
}

pub fn creator_vault(coin_creator: &Pubkey) -> (Pubkey, Pubkey) {
    let pump = pump_fun_pk();
    let (authority, _) =
        Pubkey::find_program_address(&[b"creator_vault", coin_creator.as_ref()], &pump);
    let ata = find_ata(&authority, &wsol_pk(), &token_program_pk());
    (authority, ata)
}

pub fn pool_v2_pda(base_mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool-v2", base_mint.as_ref()], &pump_fun_pk()).0
}

// =============================================================================
// Buy (exact-output) ix builder
// =============================================================================

/// Build a PumpFun pAMM ordinary `Buy` ix matching the bot's production
/// layout. Account ordering + data shape are byte-identical with the bot;
/// CU consumption when this lands is what `measure_cu` records.
///
/// Interface stays SOL-denominated (`sol_in`, `slippage_bps`) so callers
/// don't have to know about the exact-out semantics. Internally:
///   * `base_amount_out` = constant-product output for `sol_in` against the
///     given reserves (which on the shred path are the simulated post-dump
///     reserves — caller mutates the snapshot before calling, same as
///     before).
///   * `max_quote_amount_in` = `sol_in × (1 + slippage_bps/10_000)` — caps
///     how much SOL the program is allowed to debit before reverting.
///
/// On a fake dump the actual SOL needed at pre-dump reserves exceeds
/// `max_quote_amount_in` → `SlippageToleranceExceeded` (mirror of the
/// previous `min_base_amount_out` floor on `buy_exact_in`).
#[allow(clippy::too_many_arguments)]
pub fn build_pump_fun_buy_ix(
    pool_pk: &Pubkey,
    base_mint: &Pubkey,
    pool_base_token_account: &Pubkey,
    pool_quote_token_account: &Pubkey,
    coin_creator: &Pubkey,
    owner_program: &Pubkey,    // base mint's token program (Token vs Token-2022)
    is_cashback: bool,
    base_reserves: u64,
    quote_reserves: u64,
    wallet_pk: &Pubkey,
    wallet_wsol_ata: &Pubkey,
    wallet_token_ata: &Pubkey,
    static_pdas: &PumpStaticPdas,
    sol_in: u64,
    slippage_bps: u32,
) -> Instruction {
    let base_amount_out = constant_product_out(sol_in, quote_reserves, base_reserves);
    let max_quote_amount_in =
        ((sol_in as u128 * (10_000u128 + slippage_bps as u128)) / 10_000u128) as u64;

    let (coin_creator_vault_authority, coin_creator_vault_ata) = creator_vault(coin_creator);
    let pool_v2 = pool_v2_pda(base_mint);

    let pump = pump_fun_pk();
    let mut accounts = vec![
        AccountMeta::new(*pool_pk, false),                                // 0
        AccountMeta::new(*wallet_pk, true),                               // 1 (signer)
        AccountMeta::new_readonly(pump_global_config_pk(), false),        // 2
        AccountMeta::new_readonly(*base_mint, false),                     // 3
        AccountMeta::new_readonly(wsol_pk(), false),                      // 4 quote_mint
        AccountMeta::new(*wallet_token_ata, false),                       // 5
        AccountMeta::new(*wallet_wsol_ata, false),                        // 6
        AccountMeta::new(*pool_base_token_account, false),                // 7
        AccountMeta::new(*pool_quote_token_account, false),               // 8
        AccountMeta::new_readonly(pump_protocol_fee_recipient_pk(), false), // 9
        AccountMeta::new(static_pdas.protocol_fee_recipient_ata, false),  // 10
        AccountMeta::new_readonly(*owner_program, false),                 // 11
        AccountMeta::new_readonly(token_program_pk(), false),             // 12
        AccountMeta::new_readonly(system_program_pk(), false),            // 13
        AccountMeta::new_readonly(ata_program_pk(), false),               // 14
        AccountMeta::new_readonly(static_pdas.event_authority, false),    // 15
        AccountMeta::new_readonly(pump, false),                           // 16 (program)
        AccountMeta::new(coin_creator_vault_ata, false),                  // 17
        AccountMeta::new_readonly(coin_creator_vault_authority, false),   // 18
        AccountMeta::new(static_pdas.global_volume_accumulator, false),   // 19
        AccountMeta::new(static_pdas.user_volume_accumulator, false),     // 20
        AccountMeta::new_readonly(static_pdas.fee_config, false),         // 21
        AccountMeta::new_readonly(pump_fee_program_pk(), false),          // 22
    ];
    if is_cashback {
        accounts.push(AccountMeta::new(
            static_pdas.user_volume_accumulator_wsol_ata,
            false,
        ));
    }
    accounts.push(AccountMeta::new_readonly(pool_v2, false));
    accounts.push(AccountMeta::new_readonly(static_pdas.amm_fee_recipient, false));
    accounts.push(AccountMeta::new(static_pdas.amm_fee_recipient_wsol_ata, false));

    // data = [disc(8), base_amount_out(8), max_quote_amount_in(8), track_volume=Some(true)]
    let mut data = Vec::with_capacity(26);
    data.extend_from_slice(&PUMP_FUN_PREFIX_BUY);
    data.extend_from_slice(&base_amount_out.to_le_bytes());
    data.extend_from_slice(&max_quote_amount_in.to_le_bytes());
    data.push(1); // OptionBool::Some
    data.push(1); // true
    Instruction {
        program_id: pump,
        accounts,
        data,
    }
}

// =============================================================================
// BuyExactIn (exact-input) ix builder
// =============================================================================

/// Build a PumpFun pAMM `BuyExactQuoteIn` ix. Mirrors `build_pump_fun_buy_ix`'s
/// signature for drop-in callsites; differs only in the ix `data` layout:
///   * disc = `PUMP_FUN_PREFIX_BUY_EXACT_IN`
///   * `quote_amount_in` = exact SOL the program is allowed to debit
///     (no slippage cap on the input side)
///   * `min_base_amount_out` = constant-product output × (1 - slippage_bps)
///     — slippage floor on tokens received
///
/// CU consumption is ~3k higher than the ordinary `Buy` op (per the
/// upstream-code note at `PUMP_FUN_PREFIX_BUY`'s doc-comment). Use this
/// builder when the caller wants exact-SOL-in semantics (e.g. CU
/// measurement against a fixed input size); use `build_pump_fun_buy_ix`
/// when the dump-trigger path wants `max_quote_amount_in` to act as the
/// fake-dump revert gate.
#[allow(clippy::too_many_arguments)]
pub fn build_pump_fun_buy_exact_in_ix(
    pool_pk: &Pubkey,
    base_mint: &Pubkey,
    pool_base_token_account: &Pubkey,
    pool_quote_token_account: &Pubkey,
    coin_creator: &Pubkey,
    owner_program: &Pubkey,    // base mint's token program (Token vs Token-2022)
    is_cashback: bool,
    base_reserves: u64,
    quote_reserves: u64,
    wallet_pk: &Pubkey,
    wallet_wsol_ata: &Pubkey,
    wallet_token_ata: &Pubkey,
    static_pdas: &PumpStaticPdas,
    sol_in: u64,
    slippage_bps: u32,
) -> Instruction {
    let base_amount_out = constant_product_out(sol_in, quote_reserves, base_reserves);
    let min_base_amount_out = apply_slippage_floor(base_amount_out, slippage_bps);

    let (coin_creator_vault_authority, coin_creator_vault_ata) = creator_vault(coin_creator);
    let pool_v2 = pool_v2_pda(base_mint);

    let pump = pump_fun_pk();
    let mut accounts = vec![
        AccountMeta::new(*pool_pk, false),
        AccountMeta::new(*wallet_pk, true),
        AccountMeta::new_readonly(pump_global_config_pk(), false),
        AccountMeta::new_readonly(*base_mint, false),
        AccountMeta::new_readonly(wsol_pk(), false),
        AccountMeta::new(*wallet_token_ata, false),
        AccountMeta::new(*wallet_wsol_ata, false),
        AccountMeta::new(*pool_base_token_account, false),
        AccountMeta::new(*pool_quote_token_account, false),
        AccountMeta::new_readonly(pump_protocol_fee_recipient_pk(), false),
        AccountMeta::new(static_pdas.protocol_fee_recipient_ata, false),
        AccountMeta::new_readonly(*owner_program, false),
        AccountMeta::new_readonly(token_program_pk(), false),
        AccountMeta::new_readonly(system_program_pk(), false),
        AccountMeta::new_readonly(ata_program_pk(), false),
        AccountMeta::new_readonly(static_pdas.event_authority, false),
        AccountMeta::new_readonly(pump, false),
        AccountMeta::new(coin_creator_vault_ata, false),
        AccountMeta::new_readonly(coin_creator_vault_authority, false),
        AccountMeta::new(static_pdas.global_volume_accumulator, false),
        AccountMeta::new(static_pdas.user_volume_accumulator, false),
        AccountMeta::new_readonly(static_pdas.fee_config, false),
        AccountMeta::new_readonly(pump_fee_program_pk(), false),
    ];
    if is_cashback {
        accounts.push(AccountMeta::new(
            static_pdas.user_volume_accumulator_wsol_ata,
            false,
        ));
    }
    accounts.push(AccountMeta::new_readonly(pool_v2, false));
    accounts.push(AccountMeta::new_readonly(static_pdas.amm_fee_recipient, false));
    accounts.push(AccountMeta::new(static_pdas.amm_fee_recipient_wsol_ata, false));

    // data = [disc(8), quote_amount_in(8), min_base_amount_out(8), track_volume=Some(true)]
    let mut data = Vec::with_capacity(26);
    data.extend_from_slice(&PUMP_FUN_PREFIX_BUY_EXACT_IN);
    data.extend_from_slice(&sol_in.to_le_bytes());
    data.extend_from_slice(&min_base_amount_out.to_le_bytes());
    data.push(1); // OptionBool::Some
    data.push(1); // true (track_volume)
    Instruction {
        program_id: pump,
        accounts,
        data,
    }
}
