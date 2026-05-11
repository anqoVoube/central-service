//! Resolve which location actually sent a failed buy by RPC-fetching the
//! landed-and-reverted tx, walking its instructions to find the
//! `ComputeBudget::SetComputeUnitPrice` ix, and decoding the lane from the
//! price's last decimal digit.
//!
//! Why: the bot's `handle_failed_tx` subscribes to wallet-scoped failed-tx
//! geyser updates **at FR only**, then reports `position_failed` with
//! `landed_location_idx = LOCATION_INDEX` (always 0/FR). That's wrong — the
//! buy could have been sent from any of the 4 locations; FR is just the
//! observer. The on-chain CU price preserves the lane via
//! `cu_price = base + lane_idx`, so `price % 10` reconstructs the lane,
//! and `decode_lane` maps it back to `(path, location)`.
//!
//! Caching is sled-persisted (`lanes.db`) following the same pattern as
//! `alts.db` / `bans.db`. Each entry is one byte (the lane), keyed by the
//! raw 64-byte signature. Sig → lane is immutable once a tx lands; we
//! never expire entries.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_compute_budget_interface as compute_budget;
use solana_sdk::{
    commitment_config::CommitmentConfig, message::VersionedMessage, signature::Signature,
};
use solana_transaction_status_client_types::UiTransactionEncoding;

/// Discriminator byte for `SetComputeUnitPrice` in the ComputeBudget program.
const SET_CU_PRICE_DISCRIMINATOR: u8 = 3;

/// Result of decoding a sig → lane.
///
/// `Resolved(lane)` carries the raw lane index (0..=8 in the current
/// encoding; any value ≥0 from the on-chain price modulo 10). Caller
/// converts this to `(path, location)` via [`decode_lane`].
///
/// `Unknown` is returned when the tx has no `SetComputeUnitPrice` ix, the
/// RPC call failed, or the data is malformed. Caller writes `u8::MAX`
/// into `landed_location_idx` / `landed_path` so the dashboard renders
/// the row as `—` rather than mis-attributing to FR.
#[derive(Debug, Clone, Copy)]
pub enum LaneResolution {
    Resolved(u8),
    Unknown,
}

/// Decode an on-chain lane index into `(path, location)`.
///
/// Mirrors the bot's `globals::decode_lane`:
/// - 0..=3  → path=GEYSER (0), location=lane
/// - 4..=7  → path=SHREDS (1), location=lane-4
/// - 8      → path=DASHBOARD (2), location=0 (FR — dashboard is FR-only)
/// - else   → (u8::MAX, u8::MAX)  (unknown / future encoding)
pub fn decode_lane(lane: u8) -> (u8, u8) {
    match lane {
        0..=3 => (0, lane),
        4..=7 => (1, lane - 4),
        8 => (2, 0),
        _ => (u8::MAX, u8::MAX),
    }
}

/// Sled-persisted sig → lane cache + RPC-fallback resolver.
///
/// Used to attribute the location of a failed buy tx. The bot reports
/// failures from FR only (where the wallet-scoped failed-tx subscriber
/// lives), so the bot-reported `landed_location_idx` is unreliable;
/// central re-derives it from the on-chain `SetComputeUnitPrice` ix.
#[derive(Clone)]
pub struct LaneStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    in_flight: Mutex<HashSet<String>>,
    rpc_url: String,
}

impl LaneStore {
    pub fn open(db_path: &Path, rpc_url: String) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled lanes db")?;
        let tree = db.open_tree("lanes").context("open sled lanes tree")?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                rpc_url,
            }),
        })
    }

    /// Resolve a signature → `LaneResolution`. Returns immediately on
    /// cache hit; falls back to RPC `getTransaction` on miss. RPC failures
    /// surface as `LaneResolution::Unknown` so the caller can write
    /// `u8::MAX` and never blocks on a network glitch.
    ///
    /// In-flight dedup: if the same sig is currently being resolved by
    /// another task (multi-location report collision), this call waits
    /// briefly for the in-flight task's result via cache re-read rather
    /// than firing a second RPC.
    pub async fn resolve(&self, sig_str: &str) -> LaneResolution {
        // Cache hit fast path.
        if let Some(lane) = self.cache_get(sig_str) {
            return LaneResolution::Resolved(lane);
        }

        // In-flight dedup: if another task is already resolving this sig,
        // wait briefly and re-check the cache. Don't double-RPC. The mutex
        // is std::sync, so we scope the guard tightly to release it before
        // any await (otherwise the future is not Send).
        let already_in_flight = {
            let mut g = self.inner.in_flight.lock().unwrap();
            !g.insert(sig_str.to_string())
        };
        if already_in_flight {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(lane) = self.cache_get(sig_str) {
                    return LaneResolution::Resolved(lane);
                }
            }
            return LaneResolution::Unknown;
        }

        let result = fetch_and_decode(&self.inner.rpc_url, sig_str).await;
        self.inner.in_flight.lock().unwrap().remove(sig_str);

        match result {
            Ok(Some(lane)) => {
                if let Err(e) = self.cache_put(sig_str, lane) {
                    tracing::warn!("[lanes] sled write failed sig={sig_str}: {e:#}");
                }
                LaneResolution::Resolved(lane)
            }
            Ok(None) => {
                tracing::warn!(
                    "[lanes] sig={sig_str}: tx has no SetComputeUnitPrice ix — leaving lane unknown"
                );
                LaneResolution::Unknown
            }
            Err(e) => {
                tracing::warn!("[lanes] sig={sig_str} resolve failed: {e:#}");
                LaneResolution::Unknown
            }
        }
    }

    fn cache_get(&self, sig_str: &str) -> Option<u8> {
        let parsed: Signature = sig_str.parse().ok()?;
        let v = self.inner.db.get(parsed.as_ref()).ok()??;
        v.first().copied()
    }

    fn cache_put(&self, sig_str: &str, lane: u8) -> anyhow::Result<()> {
        let parsed: Signature = sig_str.parse().context("invalid signature")?;
        self.inner
            .db
            .insert(parsed.as_ref(), &[lane])
            .context("sled insert lane")?;
        self.inner.db.flush().context("sled flush lanes")?;
        Ok(())
    }
}

async fn fetch_and_decode(rpc_url: &str, sig_str: &str) -> anyhow::Result<Option<u8>> {
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
    let vtx = resp
        .transaction
        .transaction
        .decode()
        .context("decode encoded transaction (non-binary encoding?)")?;
    Ok(decode_lane_from_message(&vtx.message))
}

/// Walk a `VersionedMessage`'s instructions, find the
/// `ComputeBudget::SetComputeUnitPrice` ix, return `price % 10`.
/// Returns `None` if there's no such ix or its data is malformed.
fn decode_lane_from_message(msg: &VersionedMessage) -> Option<u8> {
    let keys = msg.static_account_keys();
    for ix in msg.instructions() {
        let prog = keys.get(ix.program_id_index as usize)?;
        if prog != &compute_budget::ID {
            continue;
        }
        if ix.data.first().copied() != Some(SET_CU_PRICE_DISCRIMINATOR) || ix.data.len() < 9 {
            continue;
        }
        let price = u64::from_le_bytes(ix.data[1..9].try_into().ok()?);
        return Some((price % 10) as u8);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_lane_table() {
        // Spot-check every known lane and one unknown.
        assert_eq!(decode_lane(0), (0, 0));   // GEYSER × FR
        assert_eq!(decode_lane(1), (0, 1));   // GEYSER × AMS
        assert_eq!(decode_lane(2), (0, 2));   // GEYSER × NY
        assert_eq!(decode_lane(3), (0, 3));   // GEYSER × TYO
        assert_eq!(decode_lane(4), (1, 0));   // SHREDS × FR
        assert_eq!(decode_lane(5), (1, 1));   // SHREDS × AMS
        assert_eq!(decode_lane(6), (1, 2));   // SHREDS × NY
        assert_eq!(decode_lane(7), (1, 3));   // SHREDS × TYO
        assert_eq!(decode_lane(8), (2, 0));   // DASHBOARD (FR-only)
        assert_eq!(decode_lane(9), (u8::MAX, u8::MAX));
        assert_eq!(decode_lane(42), (u8::MAX, u8::MAX));
    }
}
