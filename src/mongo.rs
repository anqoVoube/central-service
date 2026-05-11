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
}
