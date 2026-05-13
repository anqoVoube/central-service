//! One-shot migration: set `is_unique = true` on every pool doc except the
//! single excluded pubkey. After this, the WS `init.pools` filter ships
//! every other pool regardless of its `pair_created_at_ms` age — only the
//! excluded pool is still gated by the 7-day window.
//!
//! Run:
//!   `cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/set_is_unique`
//!
//! Requires `.env` (or env) with MONGO_URI, MONGO_DB.

use anyhow::Context;
use mongodb::bson::{doc, Document};

/// The single pool to leave gated by `pair_created_at_ms` — `is_unique`
/// is NOT set on this doc, so the standard 7-day filter still applies.
const EXCLUDE_POOL: &str = "GyqM8Pe9FUbkk5hCw7ffpeUt6F6WcHnc8pPdMYcnShiS";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;

    let client = mongodb::Client::with_uri_str(&mongo_uri)
        .await
        .context("mongo connect")?;
    let pools = client
        .database(&mongo_db)
        .collection::<Document>("pools");

    let total_before = pools.count_documents(doc! {}).await? as usize;
    println!("[set_is_unique] total pool docs: {total_before}");
    println!("[set_is_unique] excluding pool:  {EXCLUDE_POOL}");

    let filter = doc! { "pool": { "$ne": EXCLUDE_POOL } };
    let update = doc! { "$set": { "is_unique": true } };
    let res = pools
        .update_many(filter, update)
        .await
        .context("update_many")?;

    println!(
        "[set_is_unique] matched={} modified={} (matched - modified = docs already at is_unique=true)",
        res.matched_count, res.modified_count
    );

    // Sanity: count how many docs now have is_unique=true vs the holdout.
    let with_flag = pools
        .count_documents(doc! { "is_unique": true })
        .await? as usize;
    let excluded_has_flag = pools
        .count_documents(doc! { "pool": EXCLUDE_POOL, "is_unique": true })
        .await? as usize;
    println!(
        "[set_is_unique] post-state: {with_flag} docs have is_unique=true; excluded pool has flag = {excluded_has_flag} (expected 0)"
    );

    Ok(())
}
