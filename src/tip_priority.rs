//! Runtime-editable tip-priority validator set.
//!
//! Replaces the bot's hardcoded `TIP_PRIORITY_LEADERS_B58` const with a
//! sled-backed set owned by central-service. Operator flips a validator
//! via the dashboard CHANGE button → dashboard POSTs `/tip-priority` →
//! central persists to sled + broadcasts `TipPriorityChanged` via WS →
//! every connected bot updates its in-memory `ArcSwap<HashSet<Pubkey>>`
//! and rebuilds its leader bitmap.
//!
//! Seed semantics: if the sled tree is empty on startup (first boot, or
//! operator wiped `tip_priority.db`), it's populated from
//! `TIP_PRIORITY_SEED_B58`. Once non-empty, the seed is ignored — sled
//! is the source of truth.

use std::{
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::ws::ServerMsg;

/// Current unix time in whole seconds. Used for TTP (temporary
/// tip-priority) expiry stamping and comparison.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Initial seed for `tip_priority.db` on first boot — mirror of the bot's
/// `statics/mod.rs::TIP_PRIORITY_LEADERS_B58`. Keep in sync with bot edits
/// to the const; only matters when sled is empty (first boot or wipe).
pub const TIP_PRIORITY_SEED_B58: &[&str] = &[
    "DRpbCBMxVnDK7maPM5tGv6MvB3v1sRMC86PZ8okm21hy",
    "HpcB5Qg8Y9E73dUkot5e8HkgmrLNGGvD3sSCWFB1JJsT",
    "FNKgX9dYUhYQFRTM9bkeKoRpgFqgFGdpYxCRgsBNX8GS",
    "9eGrDohdNTAo61DRHyfMuqKWXqYnA3i254Wiszxe8FoY",
    "BtsmiEEvnSuUnKxqXj2PZRYpBFm7gK9LcRDfqxYmPaLU",
    "Fd7btgySsrjuo25CJCj7oE7VPMyezDhnx7pZkj2v69Nk",
    "GBQ2GvTzmjXMu97dr7WUnLKYZ3uVBjVx5sxC8s8DAAfn",
    "EkvdKhULbMFqjKBKotAzGi3kHmuk3F1qWhCRzg3Vw9hT",
    "7cVfgArCheMR6Cs4t6vz5rfnTLYpoFqMmRRSVgxJ8AzP",
    "CAo1dCGYrB6NhHh5xb1cGjUiJ6Bf2qZF8jKkbWp48Nzc",
    "5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm",
    "q9XWcZ7T1wP4bW9SB4XgNNwjnFEJ982nE8aVbbNuwot",
    "EvnRmnMrd69kFdbLMxWkTn1iCkfWmYY2bVfxRb7pXAuG",
    // FIXME (mirrors bot): decodes to 33 bytes, not 32. Skipped here too.
    "JupmVLmA8RoyTUbTMMuTtoPWvgsRpHQYBjbWWk5mkjkR",
    "JD549HsbJHeEKKUrKgg4Fj2iZsXRiYqUH5z4VqLp5x14",
    "Hz5aLvpKScNWoe9YZWxBLrQAFhEbBAtWPLcoXRoLKQqs",
    "6WgdYhhGE53WrZ7ywJA15hBVbB5F76RaxR1A8WhRpXFL",
    "forb5u56XgvzxiKfRt4FVNFQKJrd2LWAfNCsCqL6P7q",
    "ana2y2YvQ3ZPMwm6qhnN3nJoUSiT3qx5Pvetkq9xcfY",
    "EvnRmnMrd69kFdbLMxWkTn1icZ7DCceRhvmb2SJXqDo4",
    "DmTz9qp8BYMXHpSeG9dbM92u14jNTTZkmyY2rnoT1N3k",
    "8uJiHDJ1b7UDQ4KFsQGJXK9nUCkokdKRJymg1Wy9nxvM",
    "GwHH8ciFhR8vejWCqmg8FWZUCNtubPY2esALvy5tBvji",
];

/// Persistent + in-memory tip-priority validator set. Bots fetch a
/// wholesale snapshot from `GET /tip-priority.bin` at startup, then
/// receive incremental `tip_priority_changed` broadcasts.
#[derive(Clone)]
pub struct TipPriorityStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    bcast: broadcast::Sender<ServerMsg>,
}

impl TipPriorityStore {
    pub fn open(
        db_path: &Path,
        bcast: broadcast::Sender<ServerMsg>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled tip_priority db")?;
        let tree = db
            .open_tree("tip_priority")
            .context("open sled tip_priority tree")?;
        let store = Self {
            inner: Arc::new(Inner {
                db: tree,
                bcast,
            }),
        };
        if store.inner.db.iter().next().is_none() {
            store.seed_from_const()?;
        } else {
            let count = store.inner.db.iter().count();
            println!("[tip-priority] DB has {count} pubkeys (skipping seed)");
        }
        Ok(store)
    }

    /// Set or clear a validator's tip-priority status. Persists to sled,
    /// broadcasts a `TipPriorityChanged` event, returns success.
    /// Idempotent: setting the same status twice is a no-op against sled
    /// but still broadcasts (bots tolerate duplicates).
    pub fn set(&self, pk: Pubkey, is_priority: bool) -> anyhow::Result<()> {
        if is_priority {
            self.inner
                .db
                .insert(pk.as_ref(), &[])
                .context("sled insert tip_priority")?;
        } else {
            self.inner
                .db
                .remove(pk.as_ref())
                .context("sled remove tip_priority")?;
        }
        self.inner.db.flush().context("sled flush")?;
        let set_size = self.inner.db.iter().count();
        let _ = self.inner.bcast.send(ServerMsg::TipPriorityChanged {
            pubkey: pk.to_string(),
            is_priority,
        });
        println!(
            "[tip-priority] set pubkey={pk} is_priority={is_priority} set_size={set_size}"
        );
        Ok(())
    }

    /// Mark `pk` as **temporary** tip-priority (TTP): it counts as
    /// tip-priority (included in the snapshot, fires the TP variants) until
    /// `expires_at` (unix secs), after which `prune_expired` removes it and
    /// it reverts to default. Re-marking an active entry just overwrites the
    /// expiry, so it extends the timer. Value encoding: temporary entries
    /// store the 8-byte LE expiry as the sled value; permanent entries store
    /// an empty value (so old rows stay valid).
    pub fn set_temporary(&self, pk: Pubkey, expires_at: u64) -> anyhow::Result<()> {
        self.inner
            .db
            .insert(pk.as_ref(), &expires_at.to_le_bytes())
            .context("sled insert ttp")?;
        self.inner.db.flush().context("sled flush ttp")?;
        let _ = self.inner.bcast.send(ServerMsg::TipPriorityChanged {
            pubkey: pk.to_string(),
            is_priority: true,
        });
        let remaining = expires_at.saturating_sub(now_unix());
        println!("[tip-priority] set TTP pubkey={pk} expires_in={remaining}s");
        Ok(())
    }

    /// Remove temporary (TTP) entries whose expiry has passed, broadcasting
    /// `TipPriorityChanged{is_priority:false}` for each so every bot reverts
    /// it to default. Permanent entries (empty value) are never touched.
    /// Returns the count pruned. Called periodically by the sweeper task.
    pub fn prune_expired(&self) -> anyhow::Result<usize> {
        let now = now_unix();
        let mut expired: Vec<Pubkey> = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, v) = kv.context("sled iter")?;
            if k.len() != 32 || v.len() != 8 {
                continue; // not a pubkey, or a permanent entry
            }
            let expiry = u64::from_le_bytes(v.as_ref().try_into().unwrap());
            if expiry <= now {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&k);
                expired.push(Pubkey::new_from_array(arr));
            }
        }
        for pk in &expired {
            self.inner
                .db
                .remove(pk.as_ref())
                .context("sled remove expired ttp")?;
            let _ = self.inner.bcast.send(ServerMsg::TipPriorityChanged {
                pubkey: pk.to_string(),
                is_priority: false,
            });
            println!("[tip-priority] TTP expired pubkey={pk} -> default");
        }
        if !expired.is_empty() {
            self.inner.db.flush().context("sled flush prune")?;
        }
        Ok(expired.len())
    }

    /// Active temporary (TTP) entries as `(pubkey_b58, remaining_secs)`.
    /// Permanent entries are omitted. Drives the dashboard [TTP] pill.
    pub fn temporary_status(&self) -> anyhow::Result<Vec<(String, u64)>> {
        let now = now_unix();
        let mut out = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, v) = kv.context("sled iter")?;
            if k.len() != 32 || v.len() != 8 {
                continue;
            }
            let expiry = u64::from_le_bytes(v.as_ref().try_into().unwrap());
            if expiry > now {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&k);
                out.push((Pubkey::new_from_array(arr).to_string(), expiry - now));
            }
        }
        Ok(out)
    }

    /// Returns `bincode::serialize(&Vec<Pubkey>)` for `GET /tip-priority.bin`.
    /// Bots decode this at startup + on WS reconnect to refresh the full set.
    pub fn snapshot_bincode(&self) -> anyhow::Result<Vec<u8>> {
        let now = now_unix();
        let mut out: Vec<Pubkey> = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, v) = kv.context("sled iter")?;
            if k.len() != 32 {
                continue;
            }
            // Temporary (TTP) entries carry an 8-byte LE expiry — exclude
            // expired ones so the snapshot bots fetch already drops them
            // even before the sweeper prunes. Permanent entries (empty
            // value) are always included.
            if v.len() == 8 {
                let expiry = u64::from_le_bytes(v.as_ref().try_into().unwrap());
                if expiry <= now {
                    continue;
                }
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&k);
            out.push(Pubkey::new_from_array(arr));
        }
        bincode::serialize(&out).context("encode tip_priority snapshot")
    }

    /// Diagnostic / admin: current set size.
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.inner.db.iter().count()
    }

    fn seed_from_const(&self) -> anyhow::Result<()> {
        let mut inserted = 0usize;
        let mut skipped = 0usize;
        for s in TIP_PRIORITY_SEED_B58 {
            match Pubkey::from_str(s) {
                Ok(pk) => {
                    self.inner
                        .db
                        .insert(pk.as_ref(), &[])
                        .context("sled seed insert")?;
                    inserted += 1;
                }
                Err(e) => {
                    eprintln!("[tip-priority] seed pubkey {s:?} invalid: {e} — skipping");
                    skipped += 1;
                }
            }
        }
        self.inner.db.flush().context("sled flush seed")?;
        println!(
            "[tip-priority] seeded {inserted} pubkeys ({skipped} skipped) — sled was empty"
        );
        Ok(())
    }
}
