use anyhow::Context;
use futures::TryStreamExt;
use mongodb::{bson::{doc, DateTime}, options::IndexOptions, Client, Collection, IndexModel};

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

    pub async fn load_all(&self) -> anyhow::Result<Vec<PoolDoc>> {
        Ok(self.pools.find(doc! {}).await?.try_collect().await?)
    }

    pub async fn load_pump_fun(&self) -> anyhow::Result<Vec<PoolDoc>> {
        Ok(self
            .pools
            .find(doc! { "pool_type": "pump_fun" })
            .await?
            .try_collect()
            .await?)
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
