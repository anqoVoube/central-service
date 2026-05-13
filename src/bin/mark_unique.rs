//! One-shot: set `is_unique = true` on a single existing pool doc. After
//! this, the WS init filter ships the pool regardless of its
//! `pair_created_at_ms` age.
//!
//! Use this for pools that already exist in Mongo but weren't flagged at
//! insert time (or were inserted via the regular discovery flow, which
//! defaults `is_unique = None`).
//!
//! For brand-new pools that aren't in Mongo yet, use `add_pool` instead —
//! it already defaults to `is_unique = true`.
//!
//! Usage:
//!   cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/mark_unique <pool_pubkey>
//!
//! Requires `.env` (or env) with MONGO_URI, MONGO_DB.

use anyhow::Context;
use mongodb::bson::{doc, Document};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let pool_str = std::env::args()
        .nth(1)
        .context("usage: mark_unique <pool_pubkey>")?;

    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;

    let client = mongodb::Client::with_uri_str(&mongo_uri)
        .await
        .context("mongo connect")?;
    let pools = client
        .database(&mongo_db)
        .collection::<Document>("pools");

    let res = pools
        .update_one(
            doc! { "pool": &pool_str },
            doc! { "$set": { "is_unique": true } },
        )
        .await
        .context("update_one")?;

    if res.matched_count == 0 {
        anyhow::bail!(
            "pool {pool_str} not found in Mongo — use `add_pool` to insert first, \
             or check the pubkey"
        );
    }

    println!(
        "[mark_unique] pool={pool_str} matched={} modified={} (modified=0 means it already had is_unique=true)",
        res.matched_count, res.modified_count
    );
    Ok(())
}
