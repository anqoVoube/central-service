//! Server-side fee config: a JSON file on disk that central
//! auto-creates with defaults on first start, then serves to bots via
//! `GET /fee-config.json`. Bots fetch once at startup and treat the
//! result as static (no WS broadcast, no live reload).
//!
//! To change settings: edit the file on central, then restart bots.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeBucket {
    /// Upper bound (EXCLUSIVE) of this bucket in lamports.
    pub max_sol_lamports: u64,
    /// Fee budget in basis points of `quote_amount_in`.
    pub fee_bps: u32,
}

/// Config for the SECONDARY copy-trading bot.
///
/// There is no on/off field here. The switch is the `COPY_TRADING_HOST` env
/// var on the one box that runs it — a shared-config toggle defaulted to
/// `false` on every config file written before this feature existed, so a
/// correctly-provisioned host would sit idle until someone saved the page.
/// These fields are parameters only.
///
/// Separate from everything above: that config drives the dump-reversion bot,
/// this drives a wallet (`SECOND_LOGIC_WALLET`) that mirrors one trader's buys
/// and then exits on ITS OWN thresholds — the trader's sells are deliberately
/// ignored, so the entry signal can be evaluated on its own merits.
///
/// Dispatches from this bot are NOT reported to Temporal while it is under
/// test: detection is pre-block, so a copy tx can land ahead of the trader we
/// are copying, and that front-run needs finding and fixing before any of it
/// is filed as evidence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CopyTrading {
    /// Wallet whose buys we mirror.
    #[serde(default = "default_copy_trader")]
    pub trader_wallet: String,
    /// Fixed SOL per copy buy. Static while testing.
    #[serde(default = "default_copy_buy_size_sol")]
    pub buy_size_sol: f64,
    /// Take profit, percent (2.0 = +2%).
    #[serde(default = "default_copy_tp_pct")]
    pub tp_pct: f64,
    /// Stop loss, percent (20.0 = -20%).
    #[serde(default = "default_copy_sl_pct")]
    pub sl_pct: f64,
    /// Force-sell after this long if neither TP nor SL is hit, so a token that
    /// goes quiet can't tie up capital indefinitely.
    #[serde(default = "default_copy_max_hold_secs")]
    pub max_hold_secs: u64,
    /// Hard cap on simultaneously open copy positions. Bounds exposure to
    /// `max_open_positions × buy_size_sol` no matter how active the trader is.
    #[serde(default = "default_copy_max_open")]
    pub max_open_positions: u32,
    /// Buy slippage, basis points. Entry lands behind the trader into a pool
    /// they just moved, so it must cover their impact as well as ours.
    #[serde(default = "default_copy_buy_slippage_bps")]
    pub buy_slippage_bps: u32,
    /// Sell slippage, basis points. Exits favour getting out.
    #[serde(default = "default_copy_sell_slippage_bps")]
    pub sell_slippage_bps: u32,
    /// Buy tip used ONLY when the trader's tx carries no readable tip.
    /// Normally our tip is theirs plus a random 10-15%.
    #[serde(default = "default_copy_fallback_tip_sol")]
    pub fallback_tip_sol: f64,
    /// Hard ceiling on the buy tip, in SOL. Our tip tracks the trader's, and
    /// theirs is whatever they chose, so without a cap one aggressive entry
    /// of theirs could spend an unbounded amount on a single trade.
    #[serde(default = "default_copy_max_tip_sol")]
    pub max_tip_sol: f64,
    /// Ceiling on the priority fee, microlamports per compute unit.
    ///
    /// Our CU price is the trader's DOUBLED, and theirs is whatever they
    /// chose, so this is the bound. At 150k CU the default works out to
    /// ~0.0075 SOL of priority fee per transaction (~$0.55 at $73/SOL).
    #[serde(default = "default_copy_max_cu_price")]
    pub max_cu_price_micro_lamports: u64,

    // ---- how our bid is derived from theirs ----
    /// `true` = bid ABOVE their tip, `false` = below.
    #[serde(default = "default_copy_tip_more")]
    pub tip_more: bool,
    #[serde(default = "default_copy_pct_min")]
    pub tip_min_pct: f64,
    #[serde(default = "default_copy_pct_max")]
    pub tip_max_pct: f64,
    /// `true` = bid ABOVE their CU price, `false` = below.
    #[serde(default = "default_copy_fee_more")]
    pub fee_more: bool,
    #[serde(default = "default_copy_pct_min")]
    pub fee_min_pct: f64,
    #[serde(default = "default_copy_pct_max")]
    pub fee_max_pct: f64,
    /// Ignore the trader's buys smaller than this, in SOL. `0` disables.
    #[serde(default = "default_copy_min_trader_sol_in")]
    pub min_trader_sol_in: f64,
    /// Ignore the trader's buys larger than this, in SOL. `0` disables.
    #[serde(default = "default_copy_max_trader_sol_in")]
    pub max_trader_sol_in: f64,
    /// Our size as a percentage OF THEIRS — a slice, so no direction.
    #[serde(default = "default_copy_pct_min")]
    pub size_min_pct: f64,
    #[serde(default = "default_copy_pct_max")]
    pub size_max_pct: f64,
}

pub fn default_copy_max_cu_price() -> u64 { 50_000_000 }
pub fn default_copy_tip_more() -> bool { true }
pub fn default_copy_fee_more() -> bool { true }
pub fn default_copy_pct_min() -> f64 { 10.0 }
pub fn default_copy_min_trader_sol_in() -> f64 { 0.0 }
pub fn default_copy_max_trader_sol_in() -> f64 { 0.0 }
pub fn default_copy_pct_max() -> f64 { 15.0 }

pub fn default_copy_fallback_tip_sol() -> f64 { 0.001 }
/// $1 at $73/SOL.
pub fn default_copy_max_tip_sol() -> f64 { 0.02 }

pub fn default_copy_buy_slippage_bps() -> u32 { 1_500 }
pub fn default_copy_sell_slippage_bps() -> u32 { 5_000 }

pub fn default_copy_trader() -> String {
    "hnu5iBK8UoHb51UFsH1RYTUAYdrhjHvV5YMTf9T1CYN".to_owned()
}
/// $20 at $73/SOL. CEILING on a copy buy, not the size.
pub fn default_copy_buy_size_sol() -> f64 { 0.274 }
pub fn default_copy_tp_pct() -> f64 { 2.0 }
pub fn default_copy_sl_pct() -> f64 { 20.0 }
/// SOLO exit timer — only when the trader's buy did not land.
pub fn default_copy_max_hold_secs() -> u64 { 5 }
pub fn default_copy_max_open() -> u32 { 10 }

impl Default for CopyTrading {
    fn default() -> Self {
        Self {
            trader_wallet: default_copy_trader(),
            buy_size_sol: default_copy_buy_size_sol(),
            tp_pct: default_copy_tp_pct(),
            sl_pct: default_copy_sl_pct(),
            max_hold_secs: default_copy_max_hold_secs(),
            max_open_positions: default_copy_max_open(),
            buy_slippage_bps: default_copy_buy_slippage_bps(),
            sell_slippage_bps: default_copy_sell_slippage_bps(),
            fallback_tip_sol: default_copy_fallback_tip_sol(),
            max_tip_sol: default_copy_max_tip_sol(),
            max_cu_price_micro_lamports: default_copy_max_cu_price(),
            tip_more: default_copy_tip_more(),
            tip_min_pct: default_copy_pct_min(),
            tip_max_pct: default_copy_pct_max(),
            fee_more: default_copy_fee_more(),
            fee_min_pct: default_copy_pct_min(),
            fee_max_pct: default_copy_pct_max(),
            min_trader_sol_in: default_copy_min_trader_sol_in(),
            max_trader_sol_in: default_copy_max_trader_sol_in(),
            size_min_pct: default_copy_pct_min(),
            size_max_pct: default_copy_pct_max(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FeeConfig {
    pub fee_table: Vec<FeeBucket>,
    pub fire_profiles: FireProfiles,
    /// Prebuilt buy slippage as a fraction of the observed dump, in
    /// basis points. Mirrors `statics::FeeConfig::buy_slip_pct_of_dump_bps`.
    #[serde(default = "default_buy_slip_pct_of_dump_bps")]
    pub buy_slip_pct_of_dump_bps: u32,
    /// Buy-size tier table keyed on the observed liquidity-dump
    /// fraction. Sorted ascending by `min_liq_dump_pct`. The lowest
    /// tier's threshold IS the bot-wide min-liq-dump floor (replaces
    /// the prior `min_liq_dump_pct` field). Mirrors
    /// `statics::FeeConfig::buy_size_tiers`.
    #[serde(default = "default_buy_size_tiers")]
    pub buy_size_tiers: Vec<BuySizeTier>,
    /// Per-rung minimum buy size (lamports). Dashboard surfaces this
    /// as a "min sol" input on fee_table tier 1. Default 225M (0.225 SOL ~ $20).
    #[serde(default = "default_rung_min_sol_lamports")]
    pub rung_min_sol_lamports: u64,
    /// Temporary-tip-priority (TTP) lifetime in seconds. When an operator
    /// marks a validator "ttp" on the dashboard, central treats it as
    /// tip-priority for this many seconds, then auto-reverts it to default.
    /// Dashboard-configurable; default 1800 (30 min).
    #[serde(default = "default_ttp_ttl_secs")]
    pub ttp_ttl_secs: u64,
    /// Secondary copy-trading bot. `#[serde(default)]` so pre-existing config
    /// files load unchanged, with the bot disabled.
    #[serde(default)]
    pub copy_trading: CopyTrading,
    /// Copy-trading v2 — the whole strategy on the `copy-trading` branch.
    /// Kept alongside the legacy single-wallet block so a config written by
    /// either branch still loads.
    #[serde(default)]
    pub copy_trading_v2: CopyTradingV2,
    /// The second, simple copy bot. See `CopySimple`.
    #[serde(default)]
    pub copy_simple: CopySimple,
    /// Partial-sell tiers (Usual / Fast schedules per buy-size bucket).
    /// Mirrors `statics::FeeConfig::partial_sell_tiers`. Tiers must be
    /// sorted ascending by `max_sol_lamports`; last tier uses
    /// `u64::MAX` as catch-all. See `PartialSellTier` for full semantics.
    #[serde(default = "default_partial_sell_tiers")]
    pub partial_sell_tiers: Vec<PartialSellTier>,
}

/// One Fast/Usual schedule pair for a buy-size tier.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartialSellTier {
    /// Inclusive upper bound for this tier (lamports). Last tier
    /// uses `u64::MAX` as catch-all.
    pub max_sol_lamports: u64,
    /// Fast-case trigger threshold: minimum % of buy size that the
    /// pool's WSOL reserve must grow by during the next slot after
    /// our buy lands. 0-100. Default 50.
    pub fast_threshold_pct: u8,
    pub usual: PartialSellSchedule,
    pub fast: PartialSellSchedule,
}

/// A single partial-sell schedule. See bot-side `statics::PartialSellSchedule`
/// for full semantics. Invariants validated in `validate()`:
///   * `delays_ms.len() == portions_pct.len()`
///   * `1 ≤ parts ≤ 8` (MAX_PARTIAL_PARTS on bot side)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartialSellSchedule {
    pub delays_ms: Vec<u32>,
    pub portions_pct: Vec<u8>,
}

pub fn default_partial_sell_tiers() -> Vec<PartialSellTier> {
    let tier_1part = PartialSellSchedule {
        delays_ms: vec![500],
        portions_pct: vec![100],
    };
    let tier_2part = PartialSellSchedule {
        delays_ms: vec![500, 500],
        portions_pct: vec![50, 50],
    };
    let tier_3part = PartialSellSchedule {
        delays_ms: vec![500, 1_000, 2_500],
        portions_pct: vec![33, 33, 34],
    };
    let tier_5part = PartialSellSchedule {
        delays_ms: vec![1_000, 1_000, 1_000, 1_000, 1_000],
        portions_pct: vec![20, 20, 20, 20, 20],
    };
    vec![
        PartialSellTier {
            max_sol_lamports: 3_000_000_000,
            fast_threshold_pct: 50,
            usual: tier_1part.clone(),
            fast: tier_1part,
        },
        PartialSellTier {
            max_sol_lamports: 5_000_000_000,
            fast_threshold_pct: 50,
            usual: tier_2part.clone(),
            fast: tier_2part,
        },
        PartialSellTier {
            max_sol_lamports: 10_000_000_000,
            fast_threshold_pct: 50,
            usual: tier_3part.clone(),
            fast: tier_3part,
        },
        PartialSellTier {
            // Catch-all sentinel = 2^53 - 1 (JS Number.MAX_SAFE_INTEGER).
            // See bot-side `default_partial_sell_tiers` for the rationale
            // — the dashboard JS round-trips this via JS double precision
            // and `u64::MAX` would lose precision.
            max_sol_lamports: 9_007_199_254_740_991,
            fast_threshold_pct: 50,
            usual: tier_5part.clone(),
            fast: tier_5part,
        },
    ]
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct BuySizeTier {
    pub min_liq_dump_pct: f64,
    pub buy_size_bps: u32,
}

pub fn default_buy_slip_pct_of_dump_bps() -> u32 {
    1667
}

/// Three-tier default: 3%→15%, 6%→30%, 25%→50%. Below 3% no fire.
pub fn default_buy_size_tiers() -> Vec<BuySizeTier> {
    vec![
        BuySizeTier { min_liq_dump_pct: 0.03, buy_size_bps: 1500 },
        BuySizeTier { min_liq_dump_pct: 0.06, buy_size_bps: 3000 },
        BuySizeTier { min_liq_dump_pct: 0.25, buy_size_bps: 5000 },
    ]
}

/// Default rung floor — 0.225 SOL (~$20 at $89/SOL).
pub fn default_rung_min_sol_lamports() -> u64 {
    225_000_000
}

/// Default TTP (temporary tip-priority) lifetime — 30 minutes.
pub fn default_ttp_ttl_secs() -> u64 {
    1800
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FireProfiles {
    pub multiplier_x10: u32,
    /// Per-tx ceiling for DEFAULT senders (non-Jito, non-Harmonic).
    /// `None` → bot falls back to `MAX_TOTAL_LAMPORTS_PER_TX` ($22).
    /// Surfaced in dashboard as "max default sender fee ($)".
    #[serde(default, alias = "per_tx_cap_lamports")]
    pub per_tx_cap_lamports_default: Option<u64>,
    /// Per-tx ceiling for Jito + Harmonic. `None` → bot falls back to
    /// `HARMONIC_JITO_TOTAL_LAMPORTS_PER_TX` ($33). Surfaced in dashboard
    /// as "max jito+harmonic fee ($)".
    #[serde(default)]
    pub per_tx_cap_lamports_jito_harmonic: Option<u64>,
    /// Per-COMPONENT priority-fee ceiling for any single variant tx.
    /// `None` → bot falls back to hardcoded `MAX_PRIORITY_FEE_LAMPORTS`
    /// (0.333 SOL). Dashboard label: "max priority fee per tx ($)".
    /// Jito + Harmonic bypass this (sum cap only for them).
    #[serde(default)]
    pub max_priority_fee_lamports: Option<u64>,
    /// Per-COMPONENT tip ceiling for any single variant tx. `None` →
    /// bot falls back to hardcoded `MAX_TIP_LAMPORTS` (0.333 SOL).
    /// Dashboard label: "max tip per tx ($)". Jito + Harmonic bypass.
    #[serde(default)]
    pub max_tip_lamports: Option<u64>,
    pub tp: ProfileVariations,
    pub def: ProfileVariations,
    /// Fee-Priority profile — mirrors the bot. Fires only no-tip senders
    /// (helius_rpc / corvus / atlas / jet / harmonic) at 100% fee; every
    /// tip service is skipped. Default = pure fee-only. `#[serde(default)]`
    /// so pre-FP configs still load.
    #[serde(default = "default_fp_profile")]
    pub fp: ProfileVariations,
}

/// Default Fee-Priority profile: pure fee-only (no tip splits).
pub fn default_fp_profile() -> ProfileVariations {
    ProfileVariations { splits: Vec::new(), fee_only: true }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileVariations {
    pub splits: Vec<FeeTipSplit>,
    pub fee_only: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeTipSplit {
    pub fee_pct: u32,
    pub tip_pct: u32,
}

/// copy-simple — the second, deliberately simple copy bot.
///
/// Separate from `CopyTradingV2` rather than a mode of it, because the two
/// invert each other: this one sends through jito/nozomi/harmonic (which v2
/// excludes), reports every dispatch to Nozomi (which v2 must never do), and
/// backruns instead of racing for block position. Sharing a config block would
/// make every field ambiguous about which bot it governs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CopySimple {
    /// Wallets to follow. The MAIN bot reads this too, to know which events are
    /// worth forwarding over the local feed.
    #[serde(default)]
    pub competitors: Vec<String>,

    /// Our buy size as a percentage of THEIR `amount_in`.
    #[serde(default = "cs_buy_pct")]
    pub buy_pct_of_theirs: f64,
    /// Our tip as a percentage of their HIGHEST fan-out tip.
    ///
    /// Under 100 on purpose: we want to land behind them, not ahead. This bot
    /// backruns, so out-tipping the wallet it follows is a cost with no
    /// benefit.
    #[serde(default = "cs_tip_pct")]
    pub tip_pct_of_theirs: f64,
    /// Hard ceiling on the tip, in SOL, after the percentage is applied.
    #[serde(default = "cs_max_tip")]
    pub max_tip_sol: f64,

    /// Ignore their buys below this many SOL. `0` = off.
    #[serde(default)]
    pub min_competitor_sol_in: f64,
    /// Ignore their buys above this many SOL. `0` = no cap.
    #[serde(default)]
    pub max_competitor_sol_in: f64,

    #[serde(default = "cs_max_open")]
    pub max_open_positions: u32,
    #[serde(default = "cs_buy_slip")]
    pub buy_slippage_bps: u32,
    #[serde(default = "cs_sell_slip")]
    pub sell_slippage_bps: u32,
}

fn cs_buy_pct() -> f64 { 1.0 }
fn cs_tip_pct() -> f64 { 50.0 }
fn cs_max_tip() -> f64 { 0.05 }
fn cs_max_open() -> u32 { 10 }
fn cs_buy_slip() -> u32 { 1_500 }
fn cs_sell_slip() -> u32 { 3_000 }

impl Default for CopySimple {
    fn default() -> Self {
        Self {
            competitors: Vec::new(),
            buy_pct_of_theirs: cs_buy_pct(),
            tip_pct_of_theirs: cs_tip_pct(),
            max_tip_sol: cs_max_tip(),
            min_competitor_sol_in: 0.0,
            max_competitor_sol_in: 0.0,
            max_open_positions: cs_max_open(),
            buy_slippage_bps: cs_buy_slip(),
            sell_slippage_bps: cs_sell_slip(),
        }
    }
}

impl CopySimple {
    pub fn validate(&self) -> Result<(), String> {
        if !(self.buy_pct_of_theirs > 0.0 && self.buy_pct_of_theirs <= 100.0) {
            return Err("copy-simple buy % must be in (0, 100]".into());
        }
        if !(self.tip_pct_of_theirs >= 0.0 && self.tip_pct_of_theirs <= 1_000.0) {
            return Err("copy-simple tip % must be in [0, 1000]".into());
        }
        if self.max_tip_sol < 0.0 {
            return Err("copy-simple max tip must be >= 0".into());
        }
        if self.max_competitor_sol_in > 0.0
            && self.min_competitor_sol_in > self.max_competitor_sol_in
        {
            return Err("copy-simple min size must be <= max (0 = no cap)".into());
        }
        if self.max_open_positions == 0 {
            return Err("copy-simple max open positions must be >= 1".into());
        }
        for w in &self.competitors {
            if w.parse::<solana_sdk::pubkey::Pubkey>().is_err() {
                return Err(format!("copy-simple: not a valid pubkey: {w}"));
            }
        }
        Ok(())
    }
}

/// Copy-trading v2 — the whole strategy of the `copy-trading` branch.
///
/// Replaces the single-wallet `CopyTrading` block. Everything here is editable
/// on the dashboard and pushed to every location, so adding a competitor or
/// retuning a filter is not a deploy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CopyTradingV2 {
    /// Competitor wallets we copy. ONE list — the A/B split in the sell logic
    /// is derived at runtime from who bought before or after us, not from
    /// separate lists here.
    #[serde(default)]
    pub competitors: Vec<String>,

    // ---- entry filter ----
    /// Only copy buys whose `amount_in` is at least this, in SOL.
    #[serde(default = "d_min_in")]
    pub min_competitor_sol_in: f64,
    /// ...and at most this. `0` = no upper bound.
    #[serde(default = "d_max_in")]
    pub max_competitor_sol_in: f64,

    // ---- sizing ----
    /// Our `amount_in` as a percentage of THEIRS.
    #[serde(default = "d_size_pct")]
    pub size_pct_of_their_amount_in: f64,

    // ---- fee model ----
    /// How the competitor's own (tip + priority fee) budget is split across
    /// the transactions we send. One entry per variant; every variant goes to
    /// every sender except nozomi / harmonic / jito.
    #[serde(default = "d_variants")]
    pub variants: Vec<FeeTipSplit>,
    /// Per-transaction ceiling on the tip, after the split.
    #[serde(default = "d_max_tip")]
    pub max_tip_sol: f64,
    /// Per-transaction ceiling on the priority fee, after the split.
    #[serde(default = "d_max_fee")]
    pub max_priority_fee_sol: f64,

    // ---- exits ----
    /// How long after our buy we resolve mirror-vs-timer, in ms. Case A (a
    /// tracked wallet buying in after us) fires immediately and is not gated
    /// on this.
    #[serde(default = "d_case_a_ms")]
    pub case_a_window_ms: u64,
    /// Case C: nobody tracked is in the pool, so exit on a timer.
    #[serde(default = "d_timer")]
    pub timer_secs: u64,
    #[serde(default = "d_sell_tip")]
    pub sell_tip_sol: f64,
    #[serde(default = "d_sell_fee")]
    pub sell_priority_fee_sol: f64,
    /// Normal sell slippage. Retries escalate to 100%.
    #[serde(default = "d_sell_slip")]
    pub sell_slippage_bps: u32,

    // ---- exposure ----
    #[serde(default = "d_max_open")]
    pub max_open_positions: u32,

    /// Buy price ceiling as a percent ABOVE our own intended spend:
    /// `our_sol_in * (1 + buy_ceiling_pct%)`. Regulated by OUR volume, not theirs.
    #[serde(default = "d_buy_ceiling")]
    pub buy_ceiling_pct: f64,

    // ---- per-lane bid over-percents (110 = +10%) ----
    /// TIP as a percent of the competitor's LARGEST fan-out combo, on the
    /// tip-bearing HTTP senders.
    #[serde(default = "d_over_pct")]
    pub tip_over_pct: u64,
    /// JITO bundle tip as a percent of that same largest combo.
    #[serde(default = "d_over_pct")]
    pub jito_over_pct: u64,
    /// HARMONIC bundle priority fee as a percent of the competitor's LARGEST combo
    /// (same base as tip & jito).
    #[serde(default = "d_over_pct")]
    pub harmonic_over_pct: u64,

    // ---- which services the BUY fan-out goes to ----
    /// FREE group — tip-bearing services + jito. Carry the FREE (tip-heavy,
    /// `tip >= fee`) copied bids. The exit always uses the tip-bearing services.
    #[serde(default = "d_true")]
    pub use_free: bool,
    /// PAID group — fee-only RPCs (`helius_rpc`, `corvus_rpc`, jet). Carry the
    /// PAID (fee-heavy) copied bids (whole budget as priority fee, no tip). PAID
    /// only reaches marked (whitelisted) leaders — see the fire gate.
    #[serde(default = "d_true")]
    pub use_paid: bool,
    /// Reserve-free fallback: when central has not onboarded a pool
    /// (`pool_not_ready`), the bot builds the copy buy off the COMPETITOR's own
    /// instruction accounts instead of skipping. PumpFun only. Off by default.
    #[serde(default = "d_false")]
    pub use_ix_fallback: bool,
    /// Validator identities the copy strategy is allowed to fire on. When the
    /// upcoming slot's leader is not one of these, the fire is skipped. EMPTY =
    /// gate off (fire on every leader). Served to the bot, which seeds its
    /// `COPY_FIRE_LEADER_HANDLE` at boot; a change takes effect on bot restart.
    #[serde(default)]
    pub fire_leader_whitelist: Vec<String>,
}

fn d_true() -> bool { true }
fn d_false() -> bool { false }
fn d_min_in() -> f64 { 7.0 }
fn d_max_in() -> f64 { 0.0 }
fn d_size_pct() -> f64 { 10.0 }
fn d_max_tip() -> f64 { 0.05 }
fn d_max_fee() -> f64 { 0.02 }
fn d_case_a_ms() -> u64 { 1_500 }
fn d_timer() -> u64 { 5 }
fn d_sell_tip() -> f64 { 0.001 }
fn d_sell_fee() -> f64 { 0.0001 }
fn d_sell_slip() -> u32 { 3_000 }
fn d_max_open() -> u32 { 10 }
fn d_over_pct() -> u64 { 110 }
fn d_buy_ceiling() -> f64 { 3.0 }
fn d_variants() -> Vec<FeeTipSplit> {
    vec![
        FeeTipSplit { fee_pct: 80, tip_pct: 20 },
        FeeTipSplit { fee_pct: 20, tip_pct: 80 },
    ]
}

impl Default for CopyTradingV2 {
    fn default() -> Self {
        Self {
            competitors: Vec::new(),
            min_competitor_sol_in: d_min_in(),
            max_competitor_sol_in: d_max_in(),
            size_pct_of_their_amount_in: d_size_pct(),
            variants: d_variants(),
            max_tip_sol: d_max_tip(),
            max_priority_fee_sol: d_max_fee(),
            case_a_window_ms: d_case_a_ms(),
            timer_secs: d_timer(),
            sell_tip_sol: d_sell_tip(),
            sell_priority_fee_sol: d_sell_fee(),
            sell_slippage_bps: d_sell_slip(),
            max_open_positions: d_max_open(),
            buy_ceiling_pct: d_buy_ceiling(),
            tip_over_pct: d_over_pct(),
            jito_over_pct: d_over_pct(),
            harmonic_over_pct: d_over_pct(),
            use_free: d_true(),
            use_paid: d_true(),
            use_ix_fallback: d_false(),
            fire_leader_whitelist: Vec::new(),
        }
    }
}

impl CopyTradingV2 {
    /// Reject a config that would trade wrongly rather than let it through.
    pub fn validate(&self) -> Result<(), String> {
        if self.variants.is_empty() {
            return Err("at least one fee/tip variant is required".into());
        }
        if self.variants.len() > MAX_DYNAMIC_SLOTS {
            return Err(format!("at most {MAX_DYNAMIC_SLOTS} variants"));
        }
        for (i, v) in self.variants.iter().enumerate() {
            if v.fee_pct + v.tip_pct != 100 {
                return Err(format!(
                    "variant {} splits {}/{} — fee_pct + tip_pct must be 100",
                    i + 1,
                    v.fee_pct,
                    v.tip_pct
                ));
            }
        }
        if self.size_pct_of_their_amount_in <= 0.0 || self.size_pct_of_their_amount_in > 100.0 {
            return Err("size % must be in (0, 100]".into());
        }
        if self.max_competitor_sol_in > 0.0
            && self.min_competitor_sol_in > self.max_competitor_sol_in
        {
            return Err("min competitor size must be <= max (0 = no cap)".into());
        }
        if self.max_open_positions == 0 {
            return Err("max open positions must be >= 1".into());
        }
        if !(0.0..=100.0).contains(&self.buy_ceiling_pct) {
            return Err("buy ceiling % must be in [0, 100]".into());
        }
        for (label, pct) in [
            ("tip", self.tip_over_pct),
            ("jito", self.jito_over_pct),
            ("harmonic", self.harmonic_over_pct),
        ] {
            if !(1..=100_000).contains(&pct) {
                return Err(format!("{label} over-percent must be in [1, 100000] (got {pct})"));
            }
        }
        // Refused here rather than at fire time. With neither group the buy has
        // nowhere to go, and the bot would discover that one opportunity at a
        // time, in a log line, having already decided to trade.
        if !self.use_free && !self.use_paid {
            return Err("at least one group (Free or Paid) must be enabled".into());
        }
        for w in &self.competitors {
            if w.parse::<solana_sdk::pubkey::Pubkey>().is_err() {
                return Err(format!("not a valid pubkey: {w}"));
            }
        }
        Ok(())
    }
}

/// Mirrors `statics::MAX_DYNAMIC_SLOTS` in the bot. Each profile may
/// configure up to 8 explicit splits; the bot's prebuild allocates
/// exactly 8 worker slots per dynamic sender, so anything past this is
/// silently dropped at fire time. Validate at central so operators get
/// a 400 instead of mystery missing slots.
pub const MAX_DYNAMIC_SLOTS: usize = 8;

/// Reject configs that would silently misbehave at the bot. Called from
/// `FeeConfigFile::open` (catches hand-edited files at central startup)
/// AND from the dashboard POST path (rejects bad dashboard input with
/// 400 before persisting).
pub fn validate(cfg: &FeeConfig) -> anyhow::Result<()> {
    if cfg.fire_profiles.multiplier_x10 == 0 {
        anyhow::bail!("multiplier_x10 must be > 0 (0 zeroes every dynamic budget)");
    }
    if cfg.buy_slip_pct_of_dump_bps == 0 {
        anyhow::bail!(
            "buy_slip_pct_of_dump_bps must be > 0 (0 = zero slippage, every buy reverts)"
        );
    }
    if cfg.buy_slip_pct_of_dump_bps > 10_000 {
        anyhow::bail!(
            "buy_slip_pct_of_dump_bps={} exceeds 10000 (100% of dump); refusing",
            cfg.buy_slip_pct_of_dump_bps
        );
    }
    if cfg.fire_profiles.multiplier_x10 > 1000 {
        anyhow::bail!(
            "multiplier_x10={} is absurdly high (cap: 1000 = 100×); refusing",
            cfg.fire_profiles.multiplier_x10
        );
    }
    // Per-tx cap sanity guards. None = "use compiled default" (bot side).
    // Some(0) would silently zero every fire's budget — reject.
    if let Some(0) = cfg.fire_profiles.per_tx_cap_lamports_default {
        anyhow::bail!(
            "per_tx_cap_lamports_default=0 disables every default-sender fire; refusing"
        );
    }
    if let Some(0) = cfg.fire_profiles.per_tx_cap_lamports_jito_harmonic {
        anyhow::bail!(
            "per_tx_cap_lamports_jito_harmonic=0 disables every Jito/Harmonic fire; refusing"
        );
    }
    if cfg.fee_table.is_empty() {
        anyhow::bail!("fee_table must have at least one bucket");
    }
    if cfg.rung_min_sol_lamports == 0 {
        anyhow::bail!("rung_min_sol_lamports must be > 0");
    }
    if cfg.ttp_ttl_secs < 60 || cfg.ttp_ttl_secs > 86_400 {
        anyhow::bail!(
            "ttp_ttl_secs={} must be in [60, 86400] (1 min .. 24 h)",
            cfg.ttp_ttl_secs
        );
    }
    let last_max = cfg
        .fee_table
        .last()
        .map(|b| b.max_sol_lamports)
        .unwrap_or(0);
    if cfg.rung_min_sol_lamports >= last_max {
        anyhow::bail!(
            "rung_min_sol_lamports ({}) must be strictly less than fee_table's last bucket's max_sol_lamports ({}); otherwise every buy clamps to a single value",
            cfg.rung_min_sol_lamports,
            last_max
        );
    }
    let mut prev_max: u64 = 0;
    for (i, b) in cfg.fee_table.iter().enumerate() {
        if b.max_sol_lamports <= prev_max {
            anyhow::bail!(
                "fee_table[{i}].max_sol_lamports={} is not strictly increasing (prev={})",
                b.max_sol_lamports,
                prev_max
            );
        }
        if b.fee_bps > 10_000 {
            anyhow::bail!(
                "fee_table[{i}].fee_bps={} exceeds 10000 (100%)",
                b.fee_bps
            );
        }
        prev_max = b.max_sol_lamports;
    }
    for (name, p) in [
        ("tp", &cfg.fire_profiles.tp),
        ("def", &cfg.fire_profiles.def),
        ("fp", &cfg.fire_profiles.fp),
    ] {
        let effective = p.splits.len() + (p.fee_only as usize);
        if effective == 0 {
            anyhow::bail!(
                "fire_profiles.{name}: profile is empty (no splits and fee_only=false) — sender would never fire"
            );
        }
        if effective > MAX_DYNAMIC_SLOTS {
            anyhow::bail!(
                "fire_profiles.{name}: effective slots ({}) exceeds MAX_DYNAMIC_SLOTS ({MAX_DYNAMIC_SLOTS}). \
                 splits.len()={} + fee_only={}",
                effective,
                p.splits.len(),
                p.fee_only as u8,
            );
        }
        for (i, s) in p.splits.iter().enumerate() {
            // Per-slot percentages multiply the budget — a 300 here means
            // "3× total_budget for this component before per_tx_cap clamps it".
            // Cap at 1000% (10×) per component as an absurd-high guard;
            // operator values ≤ 500 are routine.
            if s.fee_pct > 1000 || s.tip_pct > 1000 {
                anyhow::bail!(
                    "fire_profiles.{name}.splits[{i}]: fee_pct={} tip_pct={} — each must be in [0, 1000]",
                    s.fee_pct,
                    s.tip_pct
                );
            }
            if s.fee_pct == 0 && s.tip_pct == 0 {
                anyhow::bail!(
                    "fire_profiles.{name}.splits[{i}]: both fee_pct and tip_pct are 0 — slot is a no-op"
                );
            }
        }
    }
    if cfg.buy_size_tiers.is_empty() {
        anyhow::bail!(
            "buy_size_tiers is empty — the lowest tier is the bot-wide min-liq-dump floor; with no tiers no fire ever happens"
        );
    }
    let mut prev_threshold: f64 = -1.0;
    for (i, t) in cfg.buy_size_tiers.iter().enumerate() {
        if !t.min_liq_dump_pct.is_finite()
            || t.min_liq_dump_pct <= 0.0
            || t.min_liq_dump_pct > 0.5
        {
            anyhow::bail!(
                "buy_size_tiers[{i}].min_liq_dump_pct={} must be in (0.0, 0.5]",
                t.min_liq_dump_pct
            );
        }
        if t.min_liq_dump_pct <= prev_threshold {
            anyhow::bail!(
                "buy_size_tiers[{i}].min_liq_dump_pct={} is not strictly increasing (prev={})",
                t.min_liq_dump_pct,
                prev_threshold
            );
        }
        if t.buy_size_bps == 0 {
            anyhow::bail!(
                "buy_size_tiers[{i}].buy_size_bps=0 — tier would never buy anything"
            );
        }
        if t.buy_size_bps > 10_000 {
            anyhow::bail!(
                "buy_size_tiers[{i}].buy_size_bps={} exceeds 10000 (100%); refusing",
                t.buy_size_bps
            );
        }
        prev_threshold = t.min_liq_dump_pct;
    }
    // ── partial_sell_tiers validation ──
    if cfg.partial_sell_tiers.is_empty() {
        anyhow::bail!(
            "partial_sell_tiers is empty — bots have no schedule to fire and would never exit any position"
        );
    }
    const MAX_PARTS: usize = 8; // mirrors bot's MAX_PARTIAL_PARTS
    let mut prev_tier_max: u64 = 0;
    for (i, t) in cfg.partial_sell_tiers.iter().enumerate() {
        if t.max_sol_lamports == 0 {
            anyhow::bail!(
                "partial_sell_tiers[{i}].max_sol_lamports=0 — tier matches nothing"
            );
        }
        if t.max_sol_lamports <= prev_tier_max {
            anyhow::bail!(
                "partial_sell_tiers[{i}].max_sol_lamports={} is not strictly increasing (prev={})",
                t.max_sol_lamports,
                prev_tier_max
            );
        }
        if t.fast_threshold_pct > 100 {
            anyhow::bail!(
                "partial_sell_tiers[{i}].fast_threshold_pct={} exceeds 100",
                t.fast_threshold_pct
            );
        }
        for (case_name, sched) in [("usual", &t.usual), ("fast", &t.fast)] {
            if sched.delays_ms.len() != sched.portions_pct.len() {
                anyhow::bail!(
                    "partial_sell_tiers[{i}].{case_name}: delays_ms.len()={} != portions_pct.len()={}",
                    sched.delays_ms.len(),
                    sched.portions_pct.len()
                );
            }
            if sched.delays_ms.is_empty() {
                anyhow::bail!(
                    "partial_sell_tiers[{i}].{case_name}: schedule is empty (no parts)"
                );
            }
            if sched.delays_ms.len() > MAX_PARTS {
                anyhow::bail!(
                    "partial_sell_tiers[{i}].{case_name}: parts ({}) exceeds MAX_PARTIAL_PARTS ({MAX_PARTS})",
                    sched.delays_ms.len()
                );
            }
            // 0ms delays are allowed — Fast variation may want to fire
            // its first part the instant the case is decided. The bot
            // handles 0ms cleanly (sleep(0) is a no-op, sweeper fires
            // on the next tick).
            let _ = sched.delays_ms.iter();
            let portion_sum: u32 = sched.portions_pct.iter().map(|&p| p as u32).sum();
            // Allow a small ±5% slack since the final part is rebased to
            // pos.token_amount at fire time anyway. Reject obvious typos.
            if portion_sum < 95 || portion_sum > 105 {
                anyhow::bail!(
                    "partial_sell_tiers[{i}].{case_name}.portions_pct sums to {} — expected ~100",
                    portion_sum
                );
            }
        }
        prev_tier_max = t.max_sol_lamports;
    }
    // Last tier must be the catch-all sentinel (2^53 - 1). Anything
    // smaller leaves a "no tier matches" gap for very large buys; the
    // bot defensively falls back to `last()` in that case, but the
    // operator's intent (hard cap) would silently differ.
    const PARTIAL_SELL_CATCHALL_SENTINEL: u64 = 9_007_199_254_740_991;
    let last_max = cfg
        .partial_sell_tiers
        .last()
        .map(|t| t.max_sol_lamports)
        .unwrap_or(0);
    if last_max != PARTIAL_SELL_CATCHALL_SENTINEL {
        anyhow::bail!(
            "partial_sell_tiers: last tier's max_sol_lamports ({last_max}) must equal the catch-all sentinel {PARTIAL_SELL_CATCHALL_SENTINEL} (2^53-1). Add a final tier with max_sol_lamports = {PARTIAL_SELL_CATCHALL_SENTINEL}."
        );
    }
    Ok(())
}

pub fn default_fee_config() -> FeeConfig {
    FeeConfig {
        fee_table: vec![
            FeeBucket { max_sol_lamports: 3_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 5_000_000_000, fee_bps: 100 },
            FeeBucket { max_sol_lamports: 7_000_000_000, fee_bps: 150 },
            FeeBucket { max_sol_lamports: 10_000_000_000, fee_bps: 200 },
            FeeBucket { max_sol_lamports: 20_000_000_000, fee_bps: 300 },
        ],
        fire_profiles: FireProfiles {
            multiplier_x10: 30,
            per_tx_cap_lamports_default: None,
            per_tx_cap_lamports_jito_harmonic: None,
            max_priority_fee_lamports: None,
            max_tip_lamports: None,
            tp: ProfileVariations {
                splits: vec![
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                    FeeTipSplit { fee_pct: 0, tip_pct: 100 },
                ],
                fee_only: false,
            },
            def: ProfileVariations {
                splits: vec![
                    FeeTipSplit { fee_pct: 20, tip_pct: 80 },
                    FeeTipSplit { fee_pct: 80, tip_pct: 20 },
                ],
                fee_only: true,
            },
            fp: default_fp_profile(),
        },
        buy_slip_pct_of_dump_bps: default_buy_slip_pct_of_dump_bps(),
        buy_size_tiers: default_buy_size_tiers(),
        rung_min_sol_lamports: default_rung_min_sol_lamports(),
        ttp_ttl_secs: default_ttp_ttl_secs(),
        copy_trading: CopyTrading::default(),
            copy_trading_v2: CopyTradingV2::default(),
            copy_simple: CopySimple::default(),
        partial_sell_tiers: default_partial_sell_tiers(),
    }
}

/// File-on-disk loader. Auto-seeds the file with `default_fee_config()`
/// if it doesn't exist. Always validates parse. Returns the raw file
/// bytes (re-read each request) so manual edits land without restart.
#[derive(Clone)]
pub struct FeeConfigFile {
    path: PathBuf,
}

impl FeeConfigFile {
    /// Ensures the file at `path` exists with valid JSON. If missing,
    /// writes `default_fee_config()` formatted with 2-space indent.
    /// If present, parses to verify it's valid JSON+schema (logs +
    /// returns the path; corrupted files do NOT get auto-overwritten —
    /// surface the error to the operator).
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            let cfg = default_fee_config();
            // Sanity-check the built-in defaults — guards against a
            // future edit to `default_fee_config` slipping in a config
            // that the bot would silently reject.
            validate(&cfg).context("default fee_config failed validation")?;
            let json = serde_json::to_string_pretty(&cfg)
                .context("serialize default fee_config")?;
            fs::write(&path, &json)
                .with_context(|| format!("write default fee_config to {path:?}"))?;
            println!(
                "[fee-config] created {path:?} with defaults ({} buckets, mult_x10={}, tp.splits={}, def.splits={})",
                cfg.fee_table.len(),
                cfg.fire_profiles.multiplier_x10,
                cfg.fire_profiles.tp.splits.len(),
                cfg.fire_profiles.def.splits.len(),
            );
        } else {
            // Validate the existing file parses cleanly so operators
            // catch JSON errors at central startup instead of seeing
            // every bot crash on first restart.
            let bytes = fs::read(&path)
                .with_context(|| format!("read existing fee_config at {path:?}"))?;
            let cfg: FeeConfig = serde_json::from_slice(&bytes).with_context(|| {
                format!("existing fee_config at {path:?} is not valid JSON")
            })?;
            validate(&cfg).with_context(|| {
                format!("existing fee_config at {path:?} failed semantic validation")
            })?;
            println!(
                "[fee-config] using existing {path:?} ({} buckets, mult_x10={}, tp.splits={}, def.splits={})",
                cfg.fee_table.len(),
                cfg.fire_profiles.multiplier_x10,
                cfg.fire_profiles.tp.splits.len(),
                cfg.fire_profiles.def.splits.len(),
            );
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the file from disk each call so operator edits land
    /// without restarting central. Bots fetch at their own startup, so
    /// staleness during a save is bounded to the next bot restart.
    pub fn read_bytes(&self) -> std::io::Result<Vec<u8>> {
        fs::read(&self.path)
    }

    /// Atomically replace the config file. Writes to `<path>.tmp` first,
    /// fsyncs, then renames over the original. A central kill mid-write
    /// leaves the original file intact (the rename is atomic on POSIX
    /// filesystems). Used by `POST /fee-config.json`.
    pub fn write_atomic(&self, cfg: &FeeConfig) -> anyhow::Result<()> {
        // Reject silently-broken configs at the operator boundary (POST)
        // rather than letting them land on disk + propagate to bots.
        validate(cfg).context("fee_config failed validation; refusing to persist")?;
        let json = serde_json::to_string_pretty(cfg)
            .context("serialize fee_config")?;
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .with_context(|| format!("create tmp {tmp:?}"))?;
            f.write_all(json.as_bytes())
                .with_context(|| format!("write tmp {tmp:?}"))?;
            // sync_all on the file forces the data to disk before we
            // rename — without it, a kernel crash could leave the new
            // file empty while the rename succeeded.
            f.sync_all().with_context(|| format!("sync tmp {tmp:?}"))?;
        }
        fs::rename(&tmp, &self.path)
            .with_context(|| format!("rename {tmp:?} -> {:?}", self.path))?;
        // Best-effort sync the parent dir so the rename itself is
        // durable across power loss. Ignore errors — not all FS
        // (notably tmpfs) support fsync on a directory.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}
