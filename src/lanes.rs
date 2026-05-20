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

/// Wait this long before the first RPC attempt. The bot subscribes to
/// `transactions_status` at `processed` commitment and fires `position_failed`
/// the instant it sees the failed tx, but `getTransaction` returns nothing
/// until the tx reaches `confirmed` (~400ms–2s after processed). Without this
/// delay every resolve races the confirmation and falls through to `Unknown`.
/// Mirrors the 5s delay used by `bans.rs` for the same reason.
const LANE_RESOLVE_INITIAL_DELAY: Duration = Duration::from_secs(5);

/// Backoff between RPC retries on transient errors.
const LANE_RESOLVE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Retries on RPC failure. `Ok(None)` (tx decoded but no `SetComputeUnitPrice`
/// ix found) is not retried — that's a definitive answer.
const LANE_RESOLVE_MAX_RETRIES: usize = 3;

/// Polls (× 200ms each) a deduped waiter does before giving up on the
/// in-flight resolver. Sized to cover the worst-case resolve window
/// (initial delay + max retries × retry delay = 5 + 3×2 = 11s) plus a
/// small margin.
const LANE_RESOLVE_WAIT_POLLS: usize = 60;

/// Result of decoding a sig → lane.
///
/// `Resolved` carries the raw lane index (split into path/location via
/// [`decode_lane`]) plus the on-chain `meta.fee` (sig fee + priority fee
/// in lamports). The fee is the authoritative cost of a failed buy: the
/// tip ix reverts with the rest of the tx, so anything the bot estimated
/// pre-fire (which included the tip) overstates by the tip amount.
///
/// `Unknown` is returned when the tx has no `SetComputeUnitPrice` ix, the
/// RPC call failed, or the data is malformed. Caller writes `u8::MAX`
/// into `landed_location_idx` / `landed_path` and leaves the fee at 0.
#[derive(Debug, Clone, Copy)]
pub enum LaneResolution {
    Resolved {
        lane: u8,
        actual_fee_lamports: u64,
    },
    Unknown,
}

/// Decode an on-chain lane index into `(path, location)`.
///
/// Mirrors the bot's `globals::decode_lane`. Lane is a two-digit decimal:
/// tens digit = path, ones digit = location.
///
/// - path 0 = GEYSER, 1 = SHRED_SHREDER, 2 = SHRED_RAIDEN,
///        3 = SHRED_CORVUS, 4 = SHRED_UDP (Raiden's UDP forward),
///        5 = SHRED_DOUBLEZERO (DoubleZero's UDP forward),
///        6..=8 reserved, 9 = DASHBOARD
/// - loc  0 = FR, 1 = AMS, 2 = NY, 3 = TYO, 4 = FR2, 5 = AMS2, 6 = LT,
///        7..=9 reserved
///
/// Reserved path (6..=8) → `(u8::MAX, u8::MAX)`. Reserved location slots
/// inside a valid path are returned as-is and rendered as `—` by the
/// dashboard.
pub fn decode_lane(lane: u8) -> (u8, u8) {
    let path = lane / 10;
    let loc = lane % 10;
    match path {
        0 | 1 | 2 | 3 | 4 | 5 => (path, loc),
        9 => (9, loc),
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
        if let Some((lane, fee)) = self.cache_get(sig_str) {
            return LaneResolution::Resolved {
                lane,
                actual_fee_lamports: fee,
            };
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
            for _ in 0..LANE_RESOLVE_WAIT_POLLS {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some((lane, fee)) = self.cache_get(sig_str) {
                    return LaneResolution::Resolved {
                        lane,
                        actual_fee_lamports: fee,
                    };
                }
            }
            return LaneResolution::Unknown;
        }

        // Tx is at `processed` when the bot reports; wait for `confirmed` to
        // catch up before the first RPC attempt. Retry only on `Err` —
        // `Ok(None)` is a definitive "no CU-price ix" answer.
        tokio::time::sleep(LANE_RESOLVE_INITIAL_DELAY).await;
        let mut result = fetch_and_decode(&self.inner.rpc_url, sig_str).await;
        for attempt in 1..=LANE_RESOLVE_MAX_RETRIES {
            if !matches!(result, Err(_)) {
                break;
            }
            tracing::debug!(
                "[lanes] sig={sig_str} RPC failed; retry {attempt}/{LANE_RESOLVE_MAX_RETRIES} in {:?}",
                LANE_RESOLVE_RETRY_DELAY
            );
            tokio::time::sleep(LANE_RESOLVE_RETRY_DELAY).await;
            result = fetch_and_decode(&self.inner.rpc_url, sig_str).await;
        }
        self.inner.in_flight.lock().unwrap().remove(sig_str);

        match result {
            Ok(Some((lane, fee))) => {
                if let Err(e) = self.cache_put(sig_str, lane, fee) {
                    tracing::warn!("[lanes] sled write failed sig={sig_str}: {e:#}");
                }
                LaneResolution::Resolved {
                    lane,
                    actual_fee_lamports: fee,
                }
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

    /// Cached payload layout: `[lane_u8, fee_u64_le]` = 9 bytes. Older
    /// entries written before the fee was tracked are 1-byte and decoded
    /// as `(lane, 0)`; the dashboard then falls back to the bot-reported
    /// `expected_cost_lamports`.
    fn cache_get(&self, sig_str: &str) -> Option<(u8, u64)> {
        let parsed: Signature = sig_str.parse().ok()?;
        let v = self.inner.db.get(parsed.as_ref()).ok()??;
        let lane = *v.first()?;
        let fee = if v.len() >= 9 {
            u64::from_le_bytes(v[1..9].try_into().ok()?)
        } else {
            0
        };
        Some((lane, fee))
    }

    fn cache_put(&self, sig_str: &str, lane: u8, actual_fee_lamports: u64) -> anyhow::Result<()> {
        let parsed: Signature = sig_str.parse().context("invalid signature")?;
        let mut buf = [0u8; 9];
        buf[0] = lane;
        buf[1..9].copy_from_slice(&actual_fee_lamports.to_le_bytes());
        self.inner
            .db
            .insert(parsed.as_ref(), &buf)
            .context("sled insert lane")?;
        self.inner.db.flush().context("sled flush lanes")?;
        Ok(())
    }
}

async fn fetch_and_decode(rpc_url: &str, sig_str: &str) -> anyhow::Result<Option<(u8, u64)>> {
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
    // `meta.fee` is the validator's authoritative sig+priority lamports —
    // the tip ix reverts with the rest of a failed tx, so this is what was
    // actually deducted from the wallet.
    let actual_fee_lamports = resp
        .transaction
        .meta
        .as_ref()
        .map(|m| m.fee)
        .unwrap_or(0);
    let vtx = resp
        .transaction
        .transaction
        .decode()
        .context("decode encoded transaction (non-binary encoding?)")?;
    Ok(decode_lane_from_message(&vtx.message).map(|lane| (lane, actual_fee_lamports)))
}

/// Walk a `VersionedMessage`'s instructions, find the
/// `ComputeBudget::SetComputeUnitPrice` ix, return `price % 100` (the
/// two-digit lane index — see [`decode_lane`]). Returns `None` if there's
/// no such ix or its data is malformed.
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
        return Some((price % 100) as u8);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_lane_table() {
        // Two-digit encoding: tens = path (0=GEYSER, 1=SHRED_SHREDER,
        // 2=SHRED_RAIDEN, 3=SHRED_CORVUS, 4=SHRED_UDP, 5=SHRED_DOUBLEZERO,
        // 6..=8 reserved, 9=DASHBOARD), ones = location (0=FR, 1=AMS,
        // 2=NY, 3=TYO, 4=FR2, 5=AMS2, 6=LT, 7..=9 reserved).
        assert_eq!(decode_lane(0),  (0, 0));         // GEYSER × FR
        assert_eq!(decode_lane(1),  (0, 1));         // GEYSER × AMS
        assert_eq!(decode_lane(2),  (0, 2));         // GEYSER × NY
        assert_eq!(decode_lane(3),  (0, 3));         // GEYSER × TYO
        assert_eq!(decode_lane(4),  (0, 4));         // GEYSER × FR2
        assert_eq!(decode_lane(6),  (0, 6));         // GEYSER × LT
        assert_eq!(decode_lane(10), (1, 0));         // SHRED_SHREDER × FR
        assert_eq!(decode_lane(14), (1, 4));         // SHRED_SHREDER × FR2
        assert_eq!(decode_lane(20), (2, 0));         // SHRED_RAIDEN × FR
        assert_eq!(decode_lane(24), (2, 4));         // SHRED_RAIDEN × FR2
        assert_eq!(decode_lane(36), (3, 6));         // SHRED_CORVUS × LT
        assert_eq!(decode_lane(40), (4, 0));         // SHRED_UDP × FR
        assert_eq!(decode_lane(44), (4, 4));         // SHRED_UDP × FR2
        assert_eq!(decode_lane(50), (5, 0));         // SHRED_DOUBLEZERO × FR
        assert_eq!(decode_lane(54), (5, 4));         // SHRED_DOUBLEZERO × FR2
        assert_eq!(decode_lane(90), (9, 0));         // DASHBOARD × FR
        // Reserved location slots still resolve — path is valid.
        assert_eq!(decode_lane(7),  (0, 7));         // GEYSER × reserved-7
        assert_eq!(decode_lane(19), (1, 9));         // SHRED_SHREDER × reserved-9
        // Reserved path slots (6..=8) are unknown.
        assert_eq!(decode_lane(65), (u8::MAX, u8::MAX));
        assert_eq!(decode_lane(85), (u8::MAX, u8::MAX));
        // Lane 99 = path 9 (DASHBOARD), loc 9 (reserved).
        assert_eq!(decode_lane(99), (9, 9));
    }
}
