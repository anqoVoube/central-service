//! Startup migrations:
//!   1. Populate `pair_created_at_ms` for any pool doc that pre-dates the
//!      field. Queries Dexscreener's pair endpoint
//!      (`/latest/dex/pairs/solana/<pool>`) which returns the pair directly
//!      by address — no mint lookup needed, works for every pool type.
//!   2. Populate `accounts.is_mayhem_mode` for pump_fun pool docs that
//!      pre-date the field. Reads the pool account from RPC and pulls
//!      byte 243 (see `pool::pump_fun::IS_MAYHEM_MODE_OFF`). Critical for
//!      routing the pAMM protocol_fee_recipient — mayhem pools require a
//!      different 8-slot set than the default. Without this backfill,
//!      pre-existing mayhem pools (like Bongo-WSOL) deserialize with
//!      `is_mayhem_mode = false` via `#[serde(default)]` and every buy on
//!      them fails 6013 InvalidProtocolFeeRecipient.
//!
//! Both best-effort. Pools we can't reach stay unchanged; the mayhem
//! backfill will retry them on next central startup.

use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{stream, StreamExt};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use crate::mongo::Repo;

/// Init filter cutoff — pools older than this aren't shipped to bots.
/// Imported from `config::POOL_MAX_AGE_MS` so the post-backfill report
/// shows the same set the WS init will later drop.
use crate::config::POOL_MAX_AGE_MS as INIT_POOL_MAX_AGE_MS;

/// How many Dexscreener requests can be in flight at once. The public API
/// is generous but no need to hammer.
const BACKFILL_CONCURRENCY: usize = 10;

/// Per-request HTTP timeout. Matches the existing discovery fetch.
const BACKFILL_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn run(repo: &Repo) {
    let pools = match repo.load_all_confirmed().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[backfill] failed to load confirmed pools: {e:#}");
            return;
        }
    };
    let missing: Vec<String> = pools
        .into_iter()
        .filter(|p| p.pair_created_at_ms.is_none())
        .map(|p| p.pool)
        .collect();
    if missing.is_empty() {
        println!("[backfill] no pools need pair_created_at_ms");
        return;
    }
    let total = missing.len();
    println!(
        "[backfill] {total} pool(s) missing pair_created_at_ms; fetching from Dexscreener (concurrency={BACKFILL_CONCURRENCY})"
    );

    let client = match reqwest::Client::builder().timeout(BACKFILL_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[backfill] reqwest client build failed: {e:#}");
            return;
        }
    };

    let results: Vec<(String, Option<i64>)> = stream::iter(missing)
        .map(|pool| {
            let client = client.clone();
            async move {
                let ms = fetch_pair_created_at(&client, &pool).await;
                (pool, ms)
            }
        })
        .buffer_unordered(BACKFILL_CONCURRENCY)
        .collect()
        .await;

    let mut filled = 0usize;
    let mut still_missing = 0usize;
    for (pool, ms) in results {
        match ms {
            Some(ms) => {
                if let Err(e) = repo.update_pair_created_at_ms(&pool, ms).await {
                    eprintln!("[backfill] mongo update {pool} failed: {e:#}");
                } else {
                    filled += 1;
                }
            }
            None => {
                still_missing += 1;
            }
        }
    }
    println!(
        "[backfill] done — {filled}/{total} filled; {still_missing} pool(s) still unindexed"
    );

    report_outdated(repo).await;
}

/// One-shot report after backfill: every confirmed pool that would be
/// filtered out by the WS init's 30-day window. Two buckets:
///   * `[outdated old]`  — has age, but older than 30d.
///   * `[outdated none]` — age missing (Dexscreener never indexed it).
/// Use this to sanity-check that what gets dropped from `init.pools` is
/// what you expected.
async fn report_outdated(repo: &Repo) {
    let pools = match repo.load_all_confirmed().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[outdated] failed to reload pools: {e:#}");
            return;
        }
    };
    let now_ms: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut old_count = 0usize;
    let mut none_count = 0usize;
    for p in &pools {
        match p.pair_created_at_ms {
            Some(c) => {
                let age_ms = now_ms.saturating_sub(c);
                if age_ms >= INIT_POOL_MAX_AGE_MS {
                    let days = age_ms as f64 / (24.0 * 60.0 * 60.0 * 1_000.0);
                    println!(
                        "[outdated old] pool={} age_days={:.1} token={}",
                        p.pool,
                        days,
                        p.token_symbol
                            .clone()
                            .or_else(|| p.token_name.clone())
                            .unwrap_or_default()
                    );
                    old_count += 1;
                }
            }
            None => {
                println!(
                    "[outdated none] pool={} token={}",
                    p.pool,
                    p.token_symbol
                        .clone()
                        .or_else(|| p.token_name.clone())
                        .unwrap_or_default()
                );
                none_count += 1;
            }
        }
    }
    let total = pools.len();
    let fresh = total - old_count - none_count;
    println!(
        "[outdated] summary: total={total} fresh<30d={fresh} old>=30d={old_count} unindexed={none_count}"
    );
}

/// Startup mayhem backfill. Reads `accounts.is_mayhem_mode` for every
/// pump_fun pool doc missing it, by fetching the pool account and pulling
/// byte 243. Idempotent — `pools_missing_is_mayhem_mode` returns only
/// docs where the field is absent, so re-runs are cheap (empty query).
///
/// Call BEFORE the WS server accepts client connections so bots don't
/// receive un-backfilled pool docs in `init.pools`. Called from
/// `main::main` right after the Dexscreener backfill.
pub async fn run_mayhem(repo: &Repo, rpc_url: &str) {
    let pools = match repo.pools_missing_is_mayhem_mode().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[backfill-mayhem] failed to load pools: {e:#}");
            return;
        }
    };
    if pools.is_empty() {
        println!("[backfill-mayhem] no pools need is_mayhem_mode");
        return;
    }
    let total = pools.len();
    println!(
        "[backfill-mayhem] {total} pump_fun pool(s) missing is_mayhem_mode; probing pool accounts (concurrency={BACKFILL_CONCURRENCY})"
    );
    let rpc = RpcClient::new(rpc_url.to_string());
    let results: Vec<(String, Option<bool>)> = stream::iter(pools)
        .map(|p| {
            let pool_str = p.pool.clone();
            let rpc = &rpc;
            async move {
                let val = fetch_is_mayhem_mode(rpc, &pool_str).await;
                (pool_str, val)
            }
        })
        .buffer_unordered(BACKFILL_CONCURRENCY)
        .collect()
        .await;

    let mut filled = 0usize;
    let mut mayhem = 0usize;
    let mut failed = 0usize;
    for (pool, val) in results {
        match val {
            Some(v) => {
                if let Err(e) = repo.update_is_mayhem_mode(&pool, v).await {
                    eprintln!("[backfill-mayhem] mongo update {pool} failed: {e:#}");
                    failed += 1;
                } else {
                    filled += 1;
                    if v {
                        mayhem += 1;
                    }
                }
            }
            None => failed += 1,
        }
    }
    println!(
        "[backfill-mayhem] done — {filled}/{total} filled ({mayhem} mayhem, {}) non-mayhem, {failed} failed)",
        filled - mayhem
    );
}

async fn fetch_is_mayhem_mode(rpc: &RpcClient, pool: &str) -> Option<bool> {
    let pk = Pubkey::from_str(pool).ok()?;
    match rpc.get_account(&pk).await {
        Ok(acc) => Some(crate::pool::pump_fun::parse_is_mayhem_mode(&acc.data)),
        Err(e) => {
            eprintln!("[backfill-mayhem] {pool} rpc get_account failed: {e}");
            None
        }
    }
}

/// Dexscreener `/latest/dex/pairs/solana/<pool>` returns a single pair (or
/// empty `pairs[]` when unindexed). Returns `pairCreatedAt` if present.
async fn fetch_pair_created_at(client: &reqwest::Client, pool: &str) -> Option<i64> {
    let url = format!("https://api.dexscreener.com/latest/dex/pairs/solana/{pool}");
    let resp = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            // 4xx is the common "not indexed" case — keep quiet.
            if r.status().as_u16() >= 500 {
                eprintln!("[backfill] {pool} → {}", r.status());
            }
            return None;
        }
        Err(e) => {
            eprintln!("[backfill] {pool} request failed: {e}");
            return None;
        }
    };
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[backfill] {pool} parse failed: {e}");
            return None;
        }
    };
    v.get("pairs")
        .and_then(|x| x.as_array())
        .and_then(|arr| arr.first())
        .and_then(|p| p.get("pairCreatedAt"))
        .and_then(|x| x.as_i64())
}
