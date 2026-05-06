use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    address_lookup_table::state::AddressLookupTable, commitment_config::CommitmentConfig,
    pubkey::Pubkey,
};
use tokio::sync::broadcast;

use crate::ws::ServerMsg;

/// Persistent + in-memory ALT registry. Sled is the source of truth (durable
/// across restarts). Bots fetch a full snapshot once via `GET /alts.bin`, then
/// receive incremental `alt_resolved` broadcasts.
///
/// Resolution is on-demand: bots send `alts_unknown { tables: [...] }` when
/// they hit a v0 tx referencing an ALT they don't have (or one whose cached
/// length is too short for the tx's index). Per-table in-flight dedup makes a
/// burst from N locations cost a single RPC.
#[derive(Clone)]
pub struct AltStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    in_flight: Mutex<HashSet<Pubkey>>,
    rpc_url: String,
    bcast: broadcast::Sender<ServerMsg>,
}

impl AltStore {
    pub fn open(
        db_path: &Path,
        rpc_url: String,
        bcast: broadcast::Sender<ServerMsg>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled alt db")?;
        let tree = db.open_tree("alts").context("open sled alts tree")?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                rpc_url,
                bcast,
            }),
        })
    }

    /// Iterate the sled tree and return a bincode `Vec<(Pubkey, Vec<Pubkey>)>`
    /// payload. Naive in-memory build; fine while the table count stays
    /// modest. Switch to streaming if footprint becomes an issue.
    pub fn snapshot_bincode(&self) -> anyhow::Result<Vec<u8>> {
        let mut out: Vec<(Pubkey, Vec<Pubkey>)> = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, v) = kv?;
            if k.len() != 32 {
                continue;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&k);
            let table = Pubkey::new_from_array(arr);
            let addrs: Vec<Pubkey> = bincode::deserialize(&v).context("decode sled value")?;
            out.push((table, addrs));
        }
        bincode::serialize(&out).context("encode snapshot")
    }

    /// Handler for `alts_unknown { tables: [...] }` C→S messages. Spawns a
    /// resolution task per table (deduped via `in_flight`). Always re-fetches
    /// — covers both the brand-new-ALT case and the stale-extension case
    /// (bot's cached length too short for current on-chain length).
    pub fn handle_unknown(&self, tables: Vec<Pubkey>) {
        for table in tables {
            {
                let mut g = self.inner.in_flight.lock().unwrap();
                if !g.insert(table) {
                    continue;
                }
            }
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let result = resolve_one(&inner, table).await;
                if let Err(e) = result {
                    println!("[alt] fetch failed table={table}: {e:#}");
                }
                inner.in_flight.lock().unwrap().remove(&table);
            });
        }
    }
}

async fn resolve_one(inner: &Inner, table: Pubkey) -> anyhow::Result<()> {
    let rpc = RpcClient::new_with_commitment(inner.rpc_url.clone(), CommitmentConfig::confirmed());
    let account = rpc
        .get_account(&table)
        .await
        .with_context(|| format!("getAccountInfo {table}"))?;
    let alt = AddressLookupTable::deserialize(&account.data)
        .map_err(|e| anyhow::anyhow!("ALT deserialize {table}: {e:?}"))?;
    let addresses: Vec<Pubkey> = alt.addresses.to_vec();
    let bytes = bincode::serialize(&addresses).context("encode addresses")?;
    inner
        .db
        .insert(table.as_ref(), bytes)
        .context("sled insert")?;
    inner.db.flush_async().await.context("sled flush")?;
    let _ = inner.bcast.send(ServerMsg::AltResolved {
        table: table.to_string(),
        addresses: addresses.iter().map(|p| p.to_string()).collect(),
    });
    println!(
        "[alt] resolved table={table} addresses={}",
        addresses.len()
    );
    Ok(())
}
