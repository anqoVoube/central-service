//! Resolve the on-chain block that an `opportunity_sig` landed in and list
//! every PumpFun BUY / BUY_EXACT_IN instruction in that block targeting the
//! same pool. Used by the dashboard's history row dropdown to show
//! competitor buys that landed alongside (or beat) our own buy on the same
//! dump.
//!
//! Same shape as `leaders.rs` / `lanes.rs`: sled-persisted cache
//! (`block_details.db`, opp_sig → bincode `BlockDetail`), in-flight dedup,
//! 5s pre-RPC sleep, retry on transient RPC errors.
//!
//! The dropdown is purely observational — failure modes (RPC unreachable,
//! block dropped, opp tx never confirmed) return `Resolution::Unknown` and
//! the dashboard renders an empty list.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcTransactionConfig};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status_client_types::{
    EncodedTransactionWithStatusMeta, TransactionDetails, UiInnerInstructions, UiInstruction,
    UiTransactionEncoding,
};

/// PumpFun program id (`pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`). Hardcoded
/// to avoid pulling in the bot's `statics` module.
const PUMP_FUN: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
/// ComputeBudget program id.
const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";
/// System program id.
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// PumpFun `Buy` 8-byte anchor discriminator.
const PUMP_PREFIX_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
/// PumpFun `BuyExactIn` 8-byte anchor discriminator.
const PUMP_PREFIX_BUY_EXACT_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];

/// ComputeBudget `SetComputeUnitLimit` discriminator byte.
const SET_CU_LIMIT_DISC: u8 = 2;
/// ComputeBudget `SetComputeUnitPrice` discriminator byte.
const SET_CU_PRICE_DISC: u8 = 3;
/// System program `Transfer` discriminator (u32 LE).
const SYSTEM_TRANSFER_DISC: u32 = 2;

/// Wait this long before the first RPC. Same reasoning as `leaders.rs`:
/// the opp_sig is reported the instant the shred stream sees the dumper tx,
/// usually before that tx is `confirmed`. Block retrieval needs the tx
/// confirmed first.
const RESOLVE_INITIAL_DELAY: Duration = Duration::from_secs(5);

/// Backoff between RPC retries on transient errors.
const RESOLVE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Retries on RPC failure. `Ok(_)` (block fetched, attempts walked) is a
/// definitive answer and not retried.
const RESOLVE_MAX_RETRIES: usize = 3;

/// Polls (× 200ms each) a deduped waiter does before giving up on the
/// in-flight resolver. Covers `INITIAL_DELAY + MAX_RETRIES × RETRY_DELAY`
/// (5 + 3×2 = 11s) plus margin — block fetches are heavier than tx fetches
/// so we give it twice the room.
const RESOLVE_WAIT_POLLS: usize = 120;

/// One competitor (or own) buy attempt in the dump's block or the
/// immediately-following block (slot+1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyAttempt {
    /// Tx signature (base58).
    pub sig: String,
    /// First signer (fee payer).
    pub fee_payer: String,
    /// `true` if the tx landed cleanly, `false` if it landed-and-reverted.
    pub success: bool,
    /// Pool pubkey (always equals the queried pool — filtered before insert).
    pub pool: String,
    /// SOL lamports the buyer committed. `BuyExactIn` → `quote_amount_in`;
    /// `Buy` → `max_quote_amount_in`. Both are the upper-bound SOL spend.
    pub sol_in_lamports: u64,
    /// `true` if BuyExactIn; `false` if Buy (exact-out).
    pub is_buy_exact_in: bool,
    /// `ComputeBudget::SetComputeUnitLimit` value. 0 if no such ix.
    pub cu_limit: u32,
    /// `ComputeBudget::SetComputeUnitPrice` value (microlamports per CU).
    /// 0 if no such ix.
    pub cu_price: u64,
    /// `cu_price * cu_limit / 1_000_000` — the lamports the buyer paid as
    /// the priority fee component of the tx fee.
    pub priority_fee_lamports: u64,
    /// Lamports paid in the final `system::transfer` ix of the tx (the
    /// tip ix). 0 if none.
    pub tip_lamports: u64,
    /// Recipient pubkey of that final transfer. Empty when there's no
    /// trailing transfer (e.g. FeeOnly variants).
    pub tip_recipient: String,
    /// 0-based position in the block's transaction list — preserves
    /// validator landing order so the dashboard can sort by it.
    pub intra_block_order: u32,
    /// Slot this attempt landed in. Either `BlockDetail.slot` (the dump's
    /// block) or `BlockDetail.next_slot` (the immediately-following block).
    /// `#[serde(default)]` so older cached entries keep deserialising.
    #[serde(default)]
    pub slot: u64,
}

/// Aggregated result for one `(opp_sig, pool)` lookup. Covers two
/// consecutive blocks — the dump's block plus the next produced block —
/// so an attempt that slipped into the following slot is still visible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockDetail {
    pub opp_sig: String,
    pub pool: String,
    pub slot: u64,
    #[serde(default)]
    pub block_height: Option<u64>,
    #[serde(default)]
    pub block_time_ms: Option<i64>,
    /// Slot of the next produced block walked (typically `slot + 1`, but
    /// can be `slot + 2` if slot+1 was skipped by its leader). `None` if
    /// the follow-up fetch failed (skipped slot beyond our scan window,
    /// RPC error, etc.).
    #[serde(default)]
    pub next_slot: Option<u64>,
    #[serde(default)]
    pub next_block_height: Option<u64>,
    #[serde(default)]
    pub next_block_time_ms: Option<i64>,
    pub attempts: Vec<BuyAttempt>,
}

#[derive(Debug, Clone)]
pub enum Resolution {
    Resolved(BlockDetail),
    Unknown,
}

/// Sled-persisted `(opp_sig, pool)` → `BlockDetail` cache + RPC resolver.
#[derive(Clone)]
pub struct BlockDetailStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    in_flight: Mutex<HashSet<String>>,
    /// Keys whose opp tx is NOT on chain. Since the orderflow migration the
    /// bot detects opportunities PRE-BLOCK, so a large share of `opp_sig`s
    /// never land — `getTransaction` returns `null` forever. Without this,
    /// every dashboard poll re-ran the 5 s delay + 3 retries and logged a
    /// WARN, hammering the RPC with calls that can never succeed.
    not_landed: Mutex<HashSet<String>>,
    rpc_url: String,
}

/// Bound on `not_landed` so a long-running central can't grow it without
/// limit. On overflow we clear it wholesale — worst case is one extra
/// resolve attempt for keys that were already known-absent.
const NOT_LANDED_CAP: usize = 20_000;

/// True when the error is "the cluster has no such transaction" rather than a
/// transient RPC failure. The JSON-RPC returns `null` for an unknown
/// signature, which surfaces as a serde error against the expected struct.
/// Retrying can't help, so this is terminal.
fn is_tx_not_found(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}");
    s.contains("invalid type: null")
}

impl BlockDetailStore {
    pub fn open(db_path: &Path, rpc_url: String) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled block_details db")?;
        let tree = db
            .open_tree("block_details")
            .context("open sled block_details tree")?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                not_landed: Mutex::new(HashSet::new()),
                rpc_url,
            }),
        })
    }

    /// Cache hit fast path. Returns `None` if the entry is missing or
    /// fails to deserialize.
    pub fn get_cached(&self, opp_sig: &str, pool: &str) -> Option<BlockDetail> {
        self.cache_get(&cache_key(opp_sig, pool))
    }

    /// True once we've established this opportunity tx is NOT on chain, so
    /// there is no block to fetch and never will be.
    ///
    /// Without this the HTTP layer could only ever answer "still pending":
    /// `resolve` caches nothing for a never-landed tx, so every poll was a
    /// cache miss and the dashboard spun through all 12 attempts before
    /// giving up with a misleading timeout. Since orderflow detects
    /// pre-block, never-landed is the COMMON case, not an edge one.
    pub fn is_known_absent(&self, opp_sig: &str, pool: &str) -> bool {
        self.inner
            .not_landed
            .lock()
            .unwrap()
            .contains(&cache_key(opp_sig, pool))
    }

    /// Resolve `(opp_sig, pool)` → `Resolution`. Cache fast path on hit;
    /// 5s pre-RPC delay + retries on miss. The same dedup pattern as
    /// `lanes.rs` / `leaders.rs` so concurrent callers share one RPC.
    pub async fn resolve(&self, opp_sig: &str, pool: &str) -> Resolution {
        let key = cache_key(opp_sig, pool);
        if let Some(d) = self.cache_get(&key) {
            return Resolution::Resolved(d);
        }
        // Known-absent short-circuit: an opp tx that never landed will never
        // land, so don't pay the 5 s delay + retries again on every poll.
        if self.inner.not_landed.lock().unwrap().contains(&key) {
            return Resolution::Unknown;
        }
        let already_in_flight = {
            let mut g = self.inner.in_flight.lock().unwrap();
            !g.insert(key.clone())
        };
        if already_in_flight {
            for _ in 0..RESOLVE_WAIT_POLLS {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(d) = self.cache_get(&key) {
                    return Resolution::Resolved(d);
                }
            }
            return Resolution::Unknown;
        }

        tokio::time::sleep(RESOLVE_INITIAL_DELAY).await;
        let mut result = fetch_block_detail(&self.inner.rpc_url, opp_sig, pool).await;
        for attempt in 1..=RESOLVE_MAX_RETRIES {
            match &result {
                Ok(_) => break,
                // Terminal: the tx isn't on chain. Retrying just burns RPC.
                Err(e) if is_tx_not_found(e) => break,
                Err(_) => {}
            }
            tracing::debug!(
                "[block_detail] opp={opp_sig} pool={pool} RPC failed; retry {attempt}/{RESOLVE_MAX_RETRIES} in {:?}",
                RESOLVE_RETRY_DELAY
            );
            tokio::time::sleep(RESOLVE_RETRY_DELAY).await;
            result = fetch_block_detail(&self.inner.rpc_url, opp_sig, pool).await;
        }
        self.inner.in_flight.lock().unwrap().remove(&key);

        match result {
            Ok(Some(d)) => {
                if let Err(e) = self.cache_put(&key, &d) {
                    tracing::warn!("[block_detail] sled write failed key={key}: {e:#}");
                }
                Resolution::Resolved(d)
            }
            Ok(None) => {
                tracing::warn!(
                    "[block_detail] opp={opp_sig} pool={pool}: tx not confirmed or block missing"
                );
                Resolution::Unknown
            }
            // Expected since the orderflow migration: we detect opportunities
            // pre-block, so many never land. Record it and stop asking —
            // debug, not warn, because it isn't a fault.
            Err(e) if is_tx_not_found(&e) => {
                tracing::debug!(
                    "[block_detail] opp={opp_sig} pool={pool}: opp tx never landed (pre-block detection)"
                );
                let mut g = self.inner.not_landed.lock().unwrap();
                if g.len() >= NOT_LANDED_CAP {
                    g.clear();
                }
                g.insert(key);
                Resolution::Unknown
            }
            Err(e) => {
                tracing::warn!("[block_detail] opp={opp_sig} pool={pool} resolve failed: {e:#}");
                Resolution::Unknown
            }
        }
    }

    fn cache_get(&self, key: &str) -> Option<BlockDetail> {
        let v = self.inner.db.get(key.as_bytes()).ok()??;
        bincode::deserialize(&v).ok()
    }

    fn cache_put(&self, key: &str, d: &BlockDetail) -> anyhow::Result<()> {
        let bytes = bincode::serialize(d).context("bincode serialize block_detail")?;
        self.inner
            .db
            .insert(key.as_bytes(), bytes)
            .context("sled insert block_detail")?;
        self.inner.db.flush().context("sled flush block_details")?;
        Ok(())
    }
}

fn cache_key(opp_sig: &str, pool: &str) -> String {
    format!("{opp_sig}|{pool}")
}

/// How many additional slots past the dump's slot to scan for late-landing
/// competitor / own buys. Solana leader slot length is ~400ms; latency
/// between shred-stream observation of the dump and our buy hitting a
/// leader's QUIC is usually well under one slot, but occasionally the buy
/// lands in slot+1 (or +2 if slot+1 was skipped). Two extra blocks covers
/// the typical worst case without ballooning the per-resolve RPC bill.
const NEXT_BLOCK_SCAN_RANGE: u64 = 2;

/// `getTransaction(opp_sig)` → slot, then `getBlock(slot, full+base64)` for
/// the dump's slot plus the next produced block (best-effort), walking
/// every tx and filtering PumpFun BUY / BUY_EXACT_IN for the given pool.
/// `Ok(None)` = the tx isn't confirmed yet or the dump's block isn't
/// retrievable. Failure to fetch the follow-up block degrades gracefully
/// — primary block's attempts are still returned with `next_slot = None`.
async fn fetch_block_detail(
    rpc_url: &str,
    opp_sig: &str,
    pool: &str,
) -> anyhow::Result<Option<BlockDetail>> {
    let rpc = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());
    let parsed: Signature = opp_sig.parse().context("invalid opp signature")?;
    let pool_pk: Pubkey = pool.parse().context("invalid pool pubkey")?;
    let tx_cfg = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let resp = rpc
        .get_transaction_with_config(&parsed, tx_cfg)
        .await
        .context("getTransaction(opp_sig)")?;
    let slot = resp.slot;
    if slot == 0 {
        return Ok(None);
    }

    let block_cfg = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: Some(TransactionDetails::Full),
        rewards: Some(false),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let block = rpc
        .get_block_with_config(slot, block_cfg.clone())
        .await
        .context("getBlock")?;
    let block_height = block.block_height;
    let block_time_ms = block.block_time.map(|s| s * 1000);
    let txs = block.transactions.unwrap_or_default();

    let mut attempts: Vec<BuyAttempt> = Vec::new();
    for (idx, tx) in txs.iter().enumerate() {
        if let Some(mut att) = try_parse_pump_buy(tx, &pool_pk, idx as u32) {
            att.slot = slot;
            attempts.push(att);
        }
    }

    // Best-effort scan of the next 1-2 slots so a buy that landed in
    // slot+1 (or +2 if +1 was skipped) is still surfaced. Use the first
    // successfully-fetched block as `next_*`; any RPC error is logged and
    // ignored so the dump-block attempts still come back.
    let mut next_slot_out: Option<u64> = None;
    let mut next_block_height: Option<u64> = None;
    let mut next_block_time_ms: Option<i64> = None;
    for offset in 1..=NEXT_BLOCK_SCAN_RANGE {
        let candidate = slot + offset;
        match rpc.get_block_with_config(candidate, block_cfg.clone()).await {
            Ok(b) => {
                next_slot_out = Some(candidate);
                next_block_height = b.block_height;
                next_block_time_ms = b.block_time.map(|s| s * 1000);
                let txs2 = b.transactions.unwrap_or_default();
                for (idx, tx) in txs2.iter().enumerate() {
                    if let Some(mut att) = try_parse_pump_buy(tx, &pool_pk, idx as u32) {
                        att.slot = candidate;
                        attempts.push(att);
                    }
                }
                break;
            }
            Err(e) => {
                // `BlockNotAvailable` is the normal case for a skipped
                // slot — keep walking. Other errors (network etc.) we
                // also tolerate but log.
                tracing::debug!(
                    "[block_detail] getBlock(slot={candidate}) failed for opp={opp_sig}: {e:#}; trying next offset"
                );
                continue;
            }
        }
    }

    Ok(Some(BlockDetail {
        opp_sig: opp_sig.to_owned(),
        pool: pool.to_owned(),
        slot,
        block_height,
        block_time_ms,
        next_slot: next_slot_out,
        next_block_height,
        next_block_time_ms,
        attempts,
    }))
}

/// Parse one `EncodedTransactionWithStatusMeta`. Returns `Some` iff the tx
/// contains a PumpFun BUY or BUY_EXACT_IN ix whose first account == `pool`.
fn try_parse_pump_buy(
    tx: &EncodedTransactionWithStatusMeta,
    pool: &Pubkey,
    intra_block_order: u32,
) -> Option<BuyAttempt> {
    let vt = tx.transaction.decode()?;
    let msg = &vt.message;
    let static_keys = msg.static_account_keys();
    let (loaded_writable, loaded_readonly) = loaded_addresses(&tx.meta)?;

    // Solana runtime ordering for v0: static_keys ++ loaded.writable ++ loaded.readonly.
    let mut all_keys: Vec<Pubkey> = Vec::with_capacity(
        static_keys.len() + loaded_writable.len() + loaded_readonly.len(),
    );
    all_keys.extend_from_slice(static_keys);
    all_keys.extend(loaded_writable.iter().copied());
    all_keys.extend(loaded_readonly.iter().copied());

    let sig = vt.signatures.first()?.to_string();
    let fee_payer = static_keys.first()?.to_string();
    let success = tx
        .meta
        .as_ref()
        .map(|m| m.err.is_none())
        .unwrap_or(false);

    let pump_pk: Pubkey = PUMP_FUN.parse().ok()?;
    let cb_pk: Pubkey = COMPUTE_BUDGET.parse().ok()?;
    let sys_pk: Pubkey = SYSTEM_PROGRAM.parse().ok()?;

    let mut sol_in_lamports: u64 = 0;
    let mut is_buy_exact_in = false;
    let mut found_pump_buy = false;
    let mut cu_limit: u32 = 0;
    let mut cu_price: u64 = 0;
    let mut tip_lamports: u64 = 0;
    let mut tip_recipient = String::new();

    for ix in msg.instructions() {
        let prog_idx = ix.program_id_index as usize;
        let Some(prog) = all_keys.get(prog_idx) else {
            continue;
        };

        if *prog == pump_pk {
            if let Some((amount, exact_in)) = parse_pump_buy_data(&ix.data) {
                let pool_idx = ix.accounts.first().copied().unwrap_or(0) as usize;
                let Some(ix_pool) = all_keys.get(pool_idx) else {
                    continue;
                };
                if ix_pool == pool {
                    // For BuyExactIn the cap == exact spend (always accurate).
                    // For Buy the cap is often u64::MAX (CPI wrappers disable
                    // the slippage cap and let their own logic enforce it),
                    // so derive actual SOL spent from the pool's quote vault
                    // delta. quote_vault is at PumpFun account index 8.
                    sol_in_lamports = if exact_in {
                        amount
                    } else {
                        pool_quote_vault_delta(&tx.meta, &ix.accounts)
                            .unwrap_or(amount)
                    };
                    is_buy_exact_in = exact_in;
                    found_pump_buy = true;
                }
            }
        } else if *prog == cb_pk {
            if let Some((limit, price)) = parse_compute_budget_data(&ix.data) {
                if let Some(l) = limit {
                    cu_limit = l;
                }
                if let Some(p) = price {
                    cu_price = p;
                }
            }
        } else if *prog == sys_pk {
            if let Some(lamports) = parse_system_transfer(&ix.data) {
                if let Some(&recipient_idx) = ix.accounts.get(1) {
                    if let Some(recipient) = all_keys.get(recipient_idx as usize) {
                        // Last transfer wins — competitor tx layouts vary,
                        // but the trailing system::transfer is the tip on
                        // every layout we've observed.
                        tip_lamports = lamports;
                        tip_recipient = recipient.to_string();
                    }
                }
            }
        }
    }

    // Inner-ix walk: aggregators (Jupiter/OKX/DFlow) and bot wrappers
    // invoke PumpFun via CPI, so the BUY ix lands inside meta.inner_instructions
    // rather than the outer ix list. Same combined `all_keys` applies (CPI
    // re-uses the parent tx's address resolution). CU + tip stay outer-only
    // (those ixs are never CPI'd).
    if !found_pump_buy {
        if let Some(inners) = inner_instructions(&tx.meta) {
            'inner_search: for group in inners {
                for ix in &group.instructions {
                    let UiInstruction::Compiled(c) = ix else {
                        continue;
                    };
                    let prog_idx = c.program_id_index as usize;
                    let Some(prog) = all_keys.get(prog_idx) else {
                        continue;
                    };
                    if *prog != pump_pk {
                        continue;
                    }
                    let Ok(data) = bs58::decode(&c.data).into_vec() else {
                        continue;
                    };
                    let Some((amount, exact_in)) = parse_pump_buy_data(&data) else {
                        continue;
                    };
                    let pool_idx = c.accounts.first().copied().unwrap_or(0) as usize;
                    let Some(ix_pool) = all_keys.get(pool_idx) else {
                        continue;
                    };
                    if ix_pool == pool {
                        // Inner BUY ixs almost always have max_quote == u64::MAX;
                        // resolve to actual spend from the pool's quote vault
                        // delta (account index 8 in PumpFun's layout).
                        sol_in_lamports = if exact_in {
                            amount
                        } else {
                            pool_quote_vault_delta(&tx.meta, &c.accounts)
                                .unwrap_or(amount)
                        };
                        is_buy_exact_in = exact_in;
                        found_pump_buy = true;
                        break 'inner_search;
                    }
                }
            }
        }
    }

    if !found_pump_buy {
        return None;
    }

    let priority_fee_lamports =
        ((cu_price as u128).saturating_mul(cu_limit as u128) / 1_000_000) as u64;

    Some(BuyAttempt {
        sig,
        fee_payer,
        success,
        pool: pool.to_string(),
        sol_in_lamports,
        is_buy_exact_in,
        cu_limit,
        cu_price,
        priority_fee_lamports,
        // Filled in by the caller (fetch_block_detail) so try_parse_pump_buy
        // stays agnostic about which block it's walking.
        slot: 0,
        tip_lamports,
        tip_recipient,
        intra_block_order,
    })
}

/// PumpFun account index of the pool's quote (WSOL) vault. Same for both
/// `Buy` and `BuyExactIn` ixs — the program dispatches on the discriminator
/// but accounts are identical.
const PUMP_QUOTE_VAULT_IX_ACCOUNT_IDX: usize = 8;

/// Actual SOL deposited into the pool by this BUY ix — `post - pre` on the
/// pool's quote (WSOL) vault. Used to override `max_quote_amount_in` from
/// the ix data, which is often `u64::MAX` for CPI'd buys.
///
/// `ix_accounts` is the BUY ix's `Vec<u8>` of account indices (works for
/// both outer `CompiledInstruction` and inner `UiCompiledInstruction`).
fn pool_quote_vault_delta(
    meta: &Option<solana_transaction_status_client_types::UiTransactionStatusMeta>,
    ix_accounts: &[u8],
) -> Option<u64> {
    use solana_transaction_status_client_types::option_serializer::OptionSerializer;
    let m = meta.as_ref()?;
    let vault_account_idx = *ix_accounts.get(PUMP_QUOTE_VAULT_IX_ACCOUNT_IDX)?;

    let pre = match &m.pre_token_balances {
        OptionSerializer::Some(v) => v,
        _ => return None,
    };
    let post = match &m.post_token_balances {
        OptionSerializer::Some(v) => v,
        _ => return None,
    };
    let pre_amt = pre
        .iter()
        .find(|b| b.account_index == vault_account_idx)
        .and_then(|b| b.ui_token_amount.amount.parse::<u64>().ok())?;
    let post_amt = post
        .iter()
        .find(|b| b.account_index == vault_account_idx)
        .and_then(|b| b.ui_token_amount.amount.parse::<u64>().ok())?;
    Some(post_amt.saturating_sub(pre_amt))
}

/// Returns the inner-instruction groups for a tx, or `None` if absent.
/// Used to catch PumpFun BUY ixs invoked via CPI from an aggregator
/// (Jupiter / OKX / DFlow) or a smart-contract wrapper — outer-only
/// walks miss these and undercount competitor landings.
fn inner_instructions(
    meta: &Option<solana_transaction_status_client_types::UiTransactionStatusMeta>,
) -> Option<&Vec<UiInnerInstructions>> {
    use solana_transaction_status_client_types::option_serializer::OptionSerializer;
    let m = meta.as_ref()?;
    match &m.inner_instructions {
        OptionSerializer::Some(v) => Some(v),
        _ => None,
    }
}

/// Returns `(loaded.writable, loaded.readonly)` for v0 txs, or empty for
/// legacy. The meta-level `loaded_addresses` is server-resolved and matches
/// runtime ordering.
fn loaded_addresses(
    meta: &Option<solana_transaction_status_client_types::UiTransactionStatusMeta>,
) -> Option<(Vec<Pubkey>, Vec<Pubkey>)> {
    let m = meta.as_ref()?;
    use solana_transaction_status_client_types::option_serializer::OptionSerializer;
    match &m.loaded_addresses {
        OptionSerializer::Some(la) => {
            let w = la
                .writable
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect();
            let r = la
                .readonly
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect();
            Some((w, r))
        }
        _ => Some((Vec::new(), Vec::new())),
    }
}

/// PumpFun BUY: `[disc(8), base_amount_out(8), max_quote_amount_in(8), ...]`.
/// PumpFun BUY_EXACT_IN: `[disc(8), quote_amount_in(8), min_base_amount_out(8), ...]`.
/// Returns `(sol_in_lamports, is_buy_exact_in)`.
fn parse_pump_buy_data(data: &[u8]) -> Option<(u64, bool)> {
    if data.len() < 24 {
        return None;
    }
    let disc: [u8; 8] = data[0..8].try_into().ok()?;
    if disc == PUMP_PREFIX_BUY_EXACT_IN {
        // quote_amount_in (SOL in)
        let amt = u64::from_le_bytes(data[8..16].try_into().ok()?);
        Some((amt, true))
    } else if disc == PUMP_PREFIX_BUY {
        // max_quote_amount_in (SOL upper bound)
        let amt = u64::from_le_bytes(data[16..24].try_into().ok()?);
        Some((amt, false))
    } else {
        None
    }
}

/// Returns `(cu_limit, cu_price)` from a ComputeBudget ix. Both `None` if
/// the discriminator doesn't match either of the two ixs we care about.
fn parse_compute_budget_data(data: &[u8]) -> Option<(Option<u32>, Option<u64>)> {
    let disc = *data.first()?;
    match disc {
        SET_CU_LIMIT_DISC if data.len() >= 5 => {
            let v = u32::from_le_bytes(data[1..5].try_into().ok()?);
            Some((Some(v), None))
        }
        SET_CU_PRICE_DISC if data.len() >= 9 => {
            let v = u64::from_le_bytes(data[1..9].try_into().ok()?);
            Some((None, Some(v)))
        }
        _ => None,
    }
}

/// System program `Transfer`: `[disc_u32_le=2, lamports_u64_le]`. Returns
/// lamports for `Transfer`, `None` otherwise (covers Nonce ixs etc).
fn parse_system_transfer(data: &[u8]) -> Option<u64> {
    if data.len() < 12 {
        return None;
    }
    let disc = u32::from_le_bytes(data[0..4].try_into().ok()?);
    if disc != SYSTEM_TRANSFER_DISC {
        return None;
    }
    Some(u64::from_le_bytes(data[4..12].try_into().ok()?))
}
