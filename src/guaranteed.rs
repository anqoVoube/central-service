//! Runtime-editable "guaranteed" validator marker.
//!
//! Pure UX flag — has NO effect on bot behavior. The dashboard's [G] pill
//! lets the operator visually tag a leader as "guaranteed" (semantics
//! up to the operator). Set lives in central-service's sled DB so it
//! survives across browsers / devices / restarts.
//!
//! Operator flips a validator via the dashboard's per-leader G button →
//! dashboard POSTs `/guaranteed` → central persists to sled + broadcasts
//! `GuaranteedChanged` via WS → every dashboard instance updates its
//! in-memory `ArcSwap<HashSet<String>>` and re-renders.
//!
//! Seed: empty by default. The operator curates the set from scratch.

use std::{path::Path, sync::Arc};

use anyhow::Context;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::ws::ServerMsg;

/// Persistent + in-memory guaranteed-leader set. Dashboards fetch a
/// wholesale snapshot from `GET /guaranteed.bin` at startup, then
/// receive incremental `guaranteed_changed` broadcasts.
#[derive(Clone)]
pub struct GuaranteedStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    bcast: broadcast::Sender<ServerMsg>,
}

impl GuaranteedStore {
    pub fn open(
        db_path: &Path,
        bcast: broadcast::Sender<ServerMsg>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled guaranteed db")?;
        let tree = db
            .open_tree("guaranteed")
            .context("open sled guaranteed tree")?;
        let count = tree.iter().count();
        println!("[guaranteed] DB has {count} pubkeys");
        Ok(Self {
            inner: Arc::new(Inner { db: tree, bcast }),
        })
    }

    /// Set or clear a validator's guaranteed status. Persists to sled,
    /// broadcasts a `GuaranteedChanged` event, returns success.
    /// Idempotent.
    pub fn set(&self, pk: Pubkey, is_guaranteed: bool) -> anyhow::Result<()> {
        if is_guaranteed {
            self.inner
                .db
                .insert(pk.as_ref(), &[])
                .context("sled insert guaranteed")?;
        } else {
            self.inner
                .db
                .remove(pk.as_ref())
                .context("sled remove guaranteed")?;
        }
        self.inner.db.flush().context("sled flush")?;
        let set_size = self.inner.db.iter().count();
        let _ = self.inner.bcast.send(ServerMsg::GuaranteedChanged {
            pubkey: pk.to_string(),
            is_guaranteed,
        });
        println!(
            "[guaranteed] set pubkey={pk} is_guaranteed={is_guaranteed} set_size={set_size}"
        );
        Ok(())
    }

    /// Returns `bincode::serialize(&Vec<Pubkey>)` for `GET /guaranteed.bin`.
    /// Dashboards decode this at startup + on poll to refresh the full set.
    pub fn snapshot_bincode(&self) -> anyhow::Result<Vec<u8>> {
        let mut out: Vec<Pubkey> = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, _) = kv.context("sled iter")?;
            if k.len() != 32 {
                continue;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&k);
            out.push(Pubkey::new_from_array(arr));
        }
        bincode::serialize(&out).context("encode guaranteed snapshot")
    }

    /// Diagnostic / admin: current set size.
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.inner.db.iter().count()
    }
}
