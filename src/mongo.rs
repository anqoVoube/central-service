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
        // `disabled: { $ne: true }` excludes banned pools — they never
        // ship to bots on init. Matches both missing field and `false`.
        Ok(self
            .pools
            .find(doc! { "ata_status": "confirmed", "disabled": { "$ne": true } })
            .await?
            .try_collect()
            .await?)
    }

    /// Pool pubkeys currently flagged `disabled: true`. Used by the
    /// dashboard's `GET /banned` proxy to gray-out banned rows in the
    /// history view. Banned pools are few, so loading full docs is fine.
    pub async fn load_disabled_pool_keys(&self) -> anyhow::Result<Vec<String>> {
        let docs: Vec<PoolDoc> = self
            .pools
            .find(doc! { "disabled": true })
            .await?
            .try_collect()
            .await?;
        Ok(docs.into_iter().map(|d| d.pool).collect())
    }

    /// Permanently ban a pool: set `disabled: true`. Bots are told to
    /// drop it via the `pool_disabled` WS broadcast; future init loads
    /// and discovery/poll queries skip it. No un-ban path.
    /// Toggle the per-pool token-tip-priority flag. Triggered by the
    /// dashboard `/ttp` page (`POST /ttp` → broadcast `pool_ttp_changed`).
    /// When set, the bot forces TP-only routing on every shred-path
    /// buy for this pool regardless of the leader's TP/loc match.
    pub async fn set_pool_ttp(&self, pool: &str, is_ttp: bool) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": { "is_ttp": is_ttp, "updated_at": DateTime::now() } },
            )
            .await
            .context("set_pool_ttp")?;
        Ok(())
    }

    /// Full pool list for the dashboard `/ttp` page. Same filter as
    /// `load_all_confirmed` (confirmed + non-disabled + pump_fun + WSOL)
    /// but returns the entire `PoolDoc` so the dashboard can render
    /// token name/symbol, current `is_ttp`, and creation age.
    pub async fn pools_for_ttp_view(&self) -> anyhow::Result<Vec<PoolDoc>> {
        let filter = doc! {
            "ata_status": "confirmed",
            "disabled": { "$ne": true },
            "pool_type": "pump_fun",
            "accounts.quote_mint": crate::swap_pump_fun::WSOL,
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
    }

    pub async fn set_pool_disabled(&self, pool: &str) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": { "disabled": true, "updated_at": DateTime::now() } },
            )
            .await
            .context("set_pool_disabled")?;
        Ok(())
    }

    pub async fn load_pump_fun_confirmed(&self) -> anyhow::Result<Vec<PoolDoc>> {
        Ok(self
            .pools
            .find(doc! { "pool_type": "pump_fun", "ata_status": "confirmed" })
            .await?
            .try_collect()
            .await?)
    }

    /// Pools eligible for the creator-drift poll: confirmed pump_fun pools
    /// that the WS init filter would actually ship to bots — i.e.
    /// `is_unique == true OR pair_created_at_ms > now - 30d`. Old non-unique
    /// pools stay in Mongo for history but skip polling, since the bots
    /// won't see them anyway.
    pub async fn load_pump_fun_for_creator_poll(&self) -> anyhow::Result<Vec<PoolDoc>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let stale_cutoff_ms = now_ms - crate::config::POOL_MAX_AGE_MS;
        let filter = doc! {
            "pool_type": "pump_fun",
            "ata_status": "confirmed",
            "disabled": { "$ne": true },
            "$or": [
                { "is_unique": true },
                { "pair_created_at_ms": { "$gt": stale_cutoff_ms } },
            ],
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
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

    /// Full `PoolDoc` rows for pools still pending an ATA and whose
    /// `ata_attempts` is below `cap`. Used by the periodic
    /// `bg_worker::run` to retry ATA creation without going through
    /// `discover::handle_one` (which short-circuits on `exists()`).
    /// Excludes `disabled: true` rows (matches `load_all_confirmed`).
    pub async fn pools_pending_for_retry(
        &self,
        cap: i32,
    ) -> anyhow::Result<Vec<PoolDoc>> {
        // Bg_worker only processes PumpFun rows + WSOL-quote pools (the
        // bot's trading scope). Filter at the DB layer so:
        //   (a) non-PumpFun pending rows don't waste a Mongo cursor +
        //       Rust-side silent skip
        //   (b) USDC- / other-quoted PumpFun pools are never retried —
        //       our buy ix is WSOL-only and would revert with
        //       `InvalidQuoteMint` anyway.
        // PoolDoc serializes as `{pool_type, accounts: {...}}` via
        // `#[serde(tag = "pool_type", content = "accounts")]`, so the
        // Mongo path for PumpFun's `quote_mint` field is
        // `accounts.quote_mint`.
        let filter = doc! {
            "ata_status": "pending",
            "ata_attempts": { "$lt": cap },
            "disabled": { "$ne": true },
            "pool_type": "pump_fun",
            "accounts.quote_mint": crate::swap_pump_fun::WSOL,
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
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

    /// Counterpart to `bump_ata_attempts`. Called by `bg_worker`'s
    /// divergence-recovery pass when it observes the on-chain ATA exists
    /// — clears any failed-attempt accounting accumulated by prior ticks
    /// so a future divergence on the same pool gets a fresh
    /// `RECOVER_ATTEMPTS_CAP` budget. Without this, a pool that hit the
    /// cap once (e.g. during a Helius outage) stays permanently locked
    /// out of bg_worker even after a successful manual recovery.
    pub async fn reset_ata_attempts(&self, pool: &str) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": { "ata_attempts": 0, "updated_at": DateTime::now() } },
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

    /// Set `accounts.is_mayhem_mode` for a pump_fun pool. Called by the
    /// startup mayhem-backfill once it reads the pool account byte 243
    /// (see `pool::pump_fun::IS_MAYHEM_MODE_OFF`). `is_mayhem_mode` is
    /// immutable per pool (set at pool creation), so backfill runs at
    /// most once per pool; subsequent restarts skip pools already
    /// carrying the field.
    pub async fn update_is_mayhem_mode(&self, pool: &str, val: bool) -> anyhow::Result<()> {
        self.pools
            .update_one(
                doc! { "pool": pool },
                doc! { "$set": {
                    "accounts.is_mayhem_mode": val,
                    "updated_at": DateTime::now(),
                } },
            )
            .await?;
        Ok(())
    }

    /// Pump-fun pools missing `accounts.is_mayhem_mode` — driven by the
    /// startup backfill. Once every existing pool has the field set,
    /// subsequent runs are no-ops (query returns empty). Filter narrower
    /// than `load_pump_fun_confirmed` — includes even non-WSOL-quote pools
    /// so we don't leak un-backfilled docs into the working set later.
    pub async fn pools_missing_is_mayhem_mode(&self) -> anyhow::Result<Vec<PoolDoc>> {
        use futures::TryStreamExt;
        let filter = doc! {
            "pool_type": "pump_fun",
            "accounts.is_mayhem_mode": { "$exists": false },
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
    }

    /// Pools that the `measure_cu` binary should hit on this run. Filter:
    ///   - `ata_status == "confirmed"` (the wallet's ATA exists for the base
    ///     mint, otherwise the buy ix errors with AccountNotInitialized)
    ///   - `pair_created_at_ms > now - 30 days` (fresh pools only; the bot's
    ///     `init.pools` filter uses the same window)
    ///   - `cu_measured_at` is null OR older than 30 days (idempotent re-run:
    ///     pools we've recently measured are skipped)
    ///   - `pool_type == "pump_fun"` for now — Raydium AMM/CPMM measurement
    ///     can be added when the bot needs per-pool CU for those too.
    /// Like `pools_for_cu_measurement` but ignores the `cu_measured_at`
    /// recency clause — used by the `--force` flag on `measure_cu` to
    /// re-measure ALL pump-fun pools (e.g. after a tx-layout change
    /// invalidates earlier CU values).
    ///
    /// Age gate: `pair_created_at_ms > now - 30d` OR `is_unique == true`.
    /// Matches the WS init filter semantics — pools the bot trades are
    /// the pools we measure.
    pub async fn pools_for_remeasurement(&self) -> anyhow::Result<Vec<PoolDoc>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let stale_cutoff_ms = now_ms - crate::config::POOL_MAX_AGE_MS;
        // WSOL-quote-only — same reasoning as pools_pending_for_retry +
        // pools_for_cu_measurement: our buy ix is WSOL-only.
        // `disabled: { $ne: true }` excludes operator-banned pools so
        // bg_worker's divergence-recovery pass can't resurrect them by
        // recreating the ATA. Matches the exclusion in load_all_confirmed
        // and pools_pending_for_retry.
        let filter = doc! {
            "ata_status": "confirmed",
            "pool_type": "pump_fun",
            "disabled": { "$ne": true },
            "accounts.quote_mint": crate::swap_pump_fun::WSOL,
            "$or": [
                { "is_unique": true },
                { "pair_created_at_ms": { "$gt": stale_cutoff_ms } },
            ],
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
    }

    /// Age gate identical to `pools_for_remeasurement`; additionally
    /// requires the pool to be unmeasured (so default re-runs are
    /// idempotent — any pool with a persisted `compute_unit_limit`
    /// is skipped, regardless of how old the measurement is).
    /// Use `--force` / `pools_for_remeasurement` to re-measure pools
    /// that already have a value (e.g. after a tx-layout change
    /// invalidates earlier CU values).
    pub async fn pools_for_cu_measurement(&self) -> anyhow::Result<Vec<PoolDoc>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let stale_cutoff_ms = now_ms - crate::config::POOL_MAX_AGE_MS;
        let filter = doc! {
            "ata_status": "confirmed",
            "pool_type": "pump_fun",
            // WSOL-quote-only — our buy ix is WSOL-only.
            "accounts.quote_mint": crate::swap_pump_fun::WSOL,
            "$and": [
                {
                    "$or": [
                        { "is_unique": true },
                        { "pair_created_at_ms": { "$gt": stale_cutoff_ms } },
                    ]
                },
                {
                    "$or": [
                        { "compute_unit_limit": null },
                        { "compute_unit_limit": { "$exists": false } },
                    ]
                },
            ],
        };
        Ok(self.pools.find(filter).await?.try_collect().await?)
    }

    /// Lookup a single PumpFun pool doc by its pubkey. Returns `None`
    /// if the pool isn't in the collection. Used by `measure_cu` when
    /// passed `--pool <pubkey>` to target one specific pool, and any
    /// other single-pool bin that needs the same shape `PoolDoc`.
    pub async fn pool_by_pubkey(&self, pool: &str) -> anyhow::Result<Option<PoolDoc>> {
        Ok(self.pools.find_one(doc! { "pool": pool }).await?)
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
