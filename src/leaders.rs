//! Resolve the slot leader for an opportunity tx (the dumper's signature).
//! For each shred-path / geyser-path buy we record the `opportunity_sig`;
//! this module fetches the on-chain slot for that sig, asks the cluster who
//! the leader was, and joins against [`crate::validators::ValidatorMap`] to
//! produce a [`crate::validators::LeaderInfo`].
//!
//! Same shape as `lanes.rs`: sled-persisted cache (`leaders.db`, sig →
//! 32-byte leader pubkey), in-flight dedup, pre-RPC sleep + retry so the
//! tx has time to confirm before the resolve fires.
//!
//! Failure modes — RPC unreachable, opp tx never confirmed, slot leader
//! missing from the CSV — all return `LeaderResolution::Unknown`. The
//! caller writes `leader: null` into the wire payload and the dashboard
//! renders `—`.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status_client_types::UiTransactionEncoding;

use crate::validators::{LeaderInfo, ValidatorMap};

/// Wait this long before the first RPC. The shred path reports `opp_sig`
/// the instant the dumper tx hits the shred stream — most of the time
/// that's well before the tx is `confirmed`, so we mirror the `lanes.rs`
/// pre-RPC sleep instead of racing.
const LEADER_RESOLVE_INITIAL_DELAY: Duration = Duration::from_secs(5);

/// Backoff between RPC retries on transient errors.
const LEADER_RESOLVE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Retries on RPC failure. `Ok(None)` (tx confirmed but missing slot, or
/// `getSlotLeaders` returned empty) is not retried — it means the cluster
/// returned a well-formed answer that just doesn't carry the leader.
const LEADER_RESOLVE_MAX_RETRIES: usize = 3;

/// Polls (× 200ms) a deduped waiter does before giving up on the in-flight
/// resolver. Sized to cover `INITIAL_DELAY + MAX_RETRIES × RETRY_DELAY`
/// (5 + 3×2 = 11s) plus margin.
const LEADER_RESOLVE_WAIT_POLLS: usize = 60;

#[derive(Debug, Clone)]
pub enum LeaderResolution {
    Resolved(LeaderInfo),
    Unknown,
}

#[derive(Clone)]
pub struct LeaderStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    in_flight: Mutex<HashSet<String>>,
    rpc_url: String,
    validators: Arc<ValidatorMap>,
}

impl LeaderStore {
    pub fn open(
        db_path: &Path,
        rpc_url: String,
        validators: Arc<ValidatorMap>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled leaders db")?;
        let tree = db.open_tree("leaders").context("open sled leaders tree")?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                rpc_url,
                validators,
            }),
        })
    }

    /// Resolve a signature → [`LeaderResolution`]. Cache fast path on hit;
    /// pre-RPC sleep + retry on miss (matches `lanes.rs`).
    pub async fn resolve(&self, sig_str: &str) -> LeaderResolution {
        if let Some(pk) = self.cache_get(sig_str) {
            return LeaderResolution::Resolved(self.inner.validators.leader_for(&pk));
        }

        let already_in_flight = {
            let mut g = self.inner.in_flight.lock().unwrap();
            !g.insert(sig_str.to_string())
        };
        if already_in_flight {
            for _ in 0..LEADER_RESOLVE_WAIT_POLLS {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(pk) = self.cache_get(sig_str) {
                    return LeaderResolution::Resolved(self.inner.validators.leader_for(&pk));
                }
            }
            return LeaderResolution::Unknown;
        }

        tokio::time::sleep(LEADER_RESOLVE_INITIAL_DELAY).await;
        let mut result = fetch_leader(&self.inner.rpc_url, sig_str).await;
        for attempt in 1..=LEADER_RESOLVE_MAX_RETRIES {
            if !matches!(result, Err(_)) {
                break;
            }
            tracing::debug!(
                "[leaders] sig={sig_str} RPC failed; retry {attempt}/{LEADER_RESOLVE_MAX_RETRIES} in {:?}",
                LEADER_RESOLVE_RETRY_DELAY
            );
            tokio::time::sleep(LEADER_RESOLVE_RETRY_DELAY).await;
            result = fetch_leader(&self.inner.rpc_url, sig_str).await;
        }
        self.inner.in_flight.lock().unwrap().remove(sig_str);

        match result {
            Ok(Some(pk)) => {
                if let Err(e) = self.cache_put(sig_str, &pk) {
                    tracing::warn!("[leaders] sled write failed sig={sig_str}: {e:#}");
                }
                LeaderResolution::Resolved(self.inner.validators.leader_for(&pk))
            }
            Ok(None) => {
                tracing::warn!(
                    "[leaders] sig={sig_str}: no slot / no leader returned — leaving leader unknown"
                );
                LeaderResolution::Unknown
            }
            Err(e) => {
                tracing::warn!("[leaders] sig={sig_str} resolve failed: {e:#}");
                LeaderResolution::Unknown
            }
        }
    }

    /// Resolve a SLOT directly to its leader identity via `getSlotLeaders`.
    /// Unlike `resolve` (sig → slot → leader), the slot is already known, so
    /// this is a single RPC and is not cached — used by the copy-trade
    /// auto-mark. `None` on any RPC error or empty result.
    pub async fn slot_leader(&self, slot: u64) -> Option<Pubkey> {
        let rpc = RpcClient::new_with_commitment(
            self.inner.rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );
        rpc.get_slot_leaders(slot, 1).await.ok()?.into_iter().next()
    }

    fn cache_get(&self, sig_str: &str) -> Option<Pubkey> {
        let parsed: Signature = sig_str.parse().ok()?;
        let v = self.inner.db.get(parsed.as_ref()).ok()??;
        let bytes: [u8; 32] = v.as_ref().try_into().ok()?;
        Some(Pubkey::new_from_array(bytes))
    }

    fn cache_put(&self, sig_str: &str, pk: &Pubkey) -> anyhow::Result<()> {
        let parsed: Signature = sig_str.parse().context("invalid signature")?;
        self.inner
            .db
            .insert(parsed.as_ref(), pk.to_bytes().to_vec())
            .context("sled insert leader")?;
        self.inner.db.flush().context("sled flush leaders")?;
        Ok(())
    }
}

/// `getTransaction(sig)` → slot, then `getSlotLeaders(slot, 1)` → pubkey.
/// `Ok(None)` is "RPC answered but no leader info"; `Err` is "RPC call
/// failed". The caller retries `Err` but not `Ok(None)`.
async fn fetch_leader(rpc_url: &str, sig_str: &str) -> anyhow::Result<Option<Pubkey>> {
    let rpc = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());
    let parsed: Signature = sig_str.parse().context("invalid signature")?;
    let cfg = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let resp = rpc
        .get_transaction_with_config(&parsed, cfg)
        .await
        .context("getTransaction")?;
    let slot = resp.slot;
    if slot == 0 {
        return Ok(None);
    }
    let leaders = rpc
        .get_slot_leaders(slot, 1)
        .await
        .context("getSlotLeaders")?;
    Ok(leaders.into_iter().next())
}
