//! Startup migration: populate `pair_created_at_ms` for any pool doc that
//! pre-dates the field. Queries Dexscreener's pair endpoint
//! (`/latest/dex/pairs/solana/<pool>`) which returns the pair directly by
//! address — no mint lookup needed, works for every pool type.
//!
//! Best-effort. Pools Dexscreener can't index (dust / dead / very fresh)
//! stay `None`; the WS init filter then drops them when computing the
//! "< 14 days" set, which is the intended behavior.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{stream, StreamExt};

use crate::mongo::Repo;

/// Init filter cutoff — pools older than this aren't shipped to bots.
/// Mirrored from `ws.rs::INIT_POOL_MAX_AGE_MS` so the post-backfill report
/// shows the same set the WS init will later drop.
const INIT_POOL_MAX_AGE_MS: i64 = 14 * 24 * 60 * 60 * 1_000;

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
/// filtered out by the WS init's 7-day window. Two buckets:
///   * `[outdated old]`  — has age, but older than 14d.
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
        "[outdated] summary: total={total} fresh<14d={fresh} old>=14d={old_count} unindexed={none_count}"
    );
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
