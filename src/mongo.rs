use anyhow::Context;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, DateTime},
    error::{ErrorKind, WriteFailure},
    options::IndexOptions,
    Client, Collection, IndexModel,
};

use crate::pool::PoolDoc;

pub struct Repo {
    pools: Collection<PoolDoc>,
}

impl Repo {
    pub async fn connect(uri: &str, db: &str) -> anyhow::Result<Self> {
        let client = Client::with_uri_str(uri).await.context("mongo connect")?;
        let pools = client.database(db).collection::<PoolDoc>("pools");
        Ok(Self { pools })
    }

    pub async fn ensure_indexes(&self) -> anyhow::Result<()> {
        let unique_pool = IndexModel::builder()
            .keys(doc! { "pool": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        let by_type = IndexModel::builder()
            .keys(doc! { "pool_type": 1 })
            .build();
        self.pools
            .create_indexes(vec![unique_pool, by_type])
            .await?;
        Ok(())
    }

    pub async fn load_all_confirmed(&self) -> anyhow::Result<Vec<PoolDoc>> {
        Ok(self
            .pools
            .find(doc! { "ata_status": "confirmed" })
            .await?
            .try_collect()
            .await?)
    }

    pub async fn load_pump_fun_confirmed(&self) -> anyhow::Result<Vec<PoolDoc>> {
        Ok(self
            .pools
            .find(doc! { "pool_type": "pump_fun", "ata_status": "confirmed" })
            .await?
            .try_collect()
            .await?)
    }

    /// Pool pubkeys for every row still stuck in `ata_status: pending`.
    /// Used at central startup to re-feed these into the discovery pipeline
    /// so the ATA creator gets another shot (subject to a fresh 3-attempt
    /// budget after the seed binary resets `ata_attempts`).
    pub async fn load_pending_pubkeys(&self) -> anyhow::Result<Vec<String>> {
        let docs: Vec<PoolDoc> = self
            .pools
            .find(doc! { "ata_status": "pending" })
            .await?
            .try_collect()
            .await?;
        Ok(docs.into_iter().map(|d| d.pool).collect())
    }

    pub async fn exists(&self, pool: &str) -> anyhow::Result<bool> {
        Ok(self
            .pools
            .find_one(doc! { "pool": pool })
            .await?
            .is_some())
    }

    /// Insert a pending pool doc. Returns `true` if inserted, `false` if the
    /// unique index rejected it (another node already inserted this pool).
    pub async fn upsert_pending(&self, doc: &PoolDoc) -> anyhow::Result<bool> {
        match self.pools.insert_one(doc).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if let ErrorKind::Write(WriteFailure::WriteError(ref we)) = *e.kind {
                    if we.code == 11000 {
                        return Ok(false);
                    }
                }
                Err(e.into())
            }
        }
    }

    pub async fn mark_ata_confirmed(&self, pool: &str) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": { "ata_status": "confirmed", "updated_at": DateTime::now() } },
            )
            .await?;
        Ok(())
    }

    pub async fn bump_ata_attempts(&self, pool: &str) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$inc": { "ata_attempts": 1 }, "$set": { "updated_at": DateTime::now() } },
            )
            .await?;
        Ok(())
    }

    /// Set `pair_created_at_ms` for a pool. Called by the startup backfill
    /// once it resolves a missing age from Dexscreener — never overwrites
    /// an existing value, the caller pre-filters by `is_none()`.
    pub async fn update_pair_created_at_ms(
        &self,
        pool: &str,
        ms: i64,
    ) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": {
                    "pair_created_at_ms": ms,
                    "updated_at": DateTime::now(),
                } },
            )
            .await?;
        Ok(())
    }

    pub async fn update_creator(&self, pool: &str, new_creator: &str) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": {
                    "accounts.coin_creator": new_creator,
                    "updated_at": DateTime::now(),
                } },
            )
            .await?;
        Ok(())
    }

    /// Pools that the `measure_cu` binary should hit on this run. Filter:
    ///   - `ata_status == "confirmed"` (the wallet's ATA exists for the base
    ///     mint, otherwise the buy ix errors with AccountNotInitialized)
    ///   - `pair_created_at_ms > now - 7 days` (fresh pools only; the bot's
    ///     `init.pools` filter uses the same window)
    ///   - `cu_measured_at` is null OR older than 7 days (idempotent re-run:
    ///     pools we've recently measured are skipped)
    ///   - `pool_type == "pump_fun"` for now — Raydium AMM/CPMM measurement
    ///     can be added when the bot needs per-pool CU for those too.
    pub async fn pools_for_cu_measurement(&self) -> anyhow::Result<Vec<PoolDoc>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let seven_days_ms: i64 = 7 * 24 * 60 * 60 * 1000;
        let stale_cutoff_ms = now_ms - seven_days_ms;
        let stale_cutoff = DateTime::from_millis(stale_cutoff_ms);
        let filter = doc! {
            "ata_status": "confirmed",
            "pool_type": "pump_fun",
            "pair_created_at_ms": { "$gt": stale_cutoff_ms },
            "$or": [
                { "cu_measured_at": null },
                { "cu_measured_at": { "$lt": stale_cutoff } },
            ],
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
    }

    /// Persist a fresh CU measurement (already padded by the caller's 1%
    /// margin) and stamp the time. Idempotent — overwrites prior values.
    pub async fn update_cu_limit(&self, pool: &str, cu_with_margin: i32) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": {
                    "compute_unit_limit": cu_with_margin,
                    "cu_measured_at": DateTime::now(),
                    "updated_at": DateTime::now(),
                } },
            )
            .await?;
        Ok(())
    }
}
