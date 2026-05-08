use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use tokio::sync::broadcast;

use crate::ws::ServerMsg;

/// 24h ban for any dumper whose ATA balance was less than the `amount_in` they
/// declared in their failed swap ix. The fake-dumper attack pattern is to
/// submit a tx that's guaranteed to fail (insufficient balance) so shred
/// readers like us see a "dump" signal and react. Banning that wallet for 24h
/// stops them from baiting again with the same wallet.
const BAN_DURATION: Duration = Duration::from_secs(24 * 60 * 60);
/// How long we wait after receiving an `opp_check` before asking the RPC for
/// the tx status. The shred arrived pre-block, so the tx is still pending —
/// confirmed/failed status takes ~600-800ms after shred. 5s is comfortable.
const STATUS_CHECK_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanEntry {
    pub banned_until_ms: u64,
    pub reason: String,
}

/// Persistent + in-memory ban registry. Bots fetch a wholesale snapshot from
/// `GET /bans.bin` once at startup, then receive incremental `wallet_banned`
/// broadcasts. Shred path on the bot consults this; geyser is unaffected.
#[derive(Clone)]
pub struct BansStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: sled::Tree,
    in_flight: Mutex<HashSet<String>>, // dedup by opp_sig
    rpc_url: String,
    bcast: broadcast::Sender<ServerMsg>,
}

impl BansStore {
    pub fn open(
        db_path: &Path,
        rpc_url: String,
        bcast: broadcast::Sender<ServerMsg>,
    ) -> anyhow::Result<Self> {
        let db = sled::open(db_path).context("open sled bans db")?;
        let tree = db.open_tree("bans").context("open sled bans tree")?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: tree,
                in_flight: Mutex::new(HashSet::new()),
                rpc_url,
                bcast,
            }),
        })
    }

    /// Look up a single wallet. Returns `None` if not banned (no entry, or
    /// entry expired). Used by the `GET /bans/:wallet` debug endpoint —
    /// not on the hot path.
    pub fn lookup(&self, wallet: &Pubkey) -> anyhow::Result<Option<BanEntry>> {
        let now_ms = chrono_now_ms();
        let v = self.inner.db.get(wallet.as_ref()).context("sled get")?;
        let Some(bytes) = v else { return Ok(None) };
        let entry: BanEntry = bincode::deserialize(&bytes).context("decode ban entry")?;
        if entry.banned_until_ms <= now_ms {
            return Ok(None);
        }
        Ok(Some(entry))
    }

    /// Bincode `Vec<(Pubkey, BanEntry)>` for `GET /bans.bin`. Skips entries
    /// whose `banned_until_ms` has already passed (lazy eviction at snapshot
    /// time so the bot doesn't load expired bans).
    pub fn snapshot_bincode(&self) -> anyhow::Result<Vec<u8>> {
        let now_ms = chrono_now_ms();
        let mut out: Vec<(Pubkey, BanEntry)> = Vec::new();
        for kv in self.inner.db.iter() {
            let (k, v) = kv?;
            if k.len() != 32 {
                continue;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&k);
            let wallet = Pubkey::new_from_array(arr);
            let entry: BanEntry = bincode::deserialize(&v).context("decode ban entry")?;
            if entry.banned_until_ms <= now_ms {
                continue;
            }
            out.push((wallet, entry));
        }
        bincode::serialize(&out).context("encode bans snapshot")
    }

    /// Handle inbound `opp_check` from a bot. Spawns a task that waits a few
    /// seconds for the tx to commit, then queries status. If the tx failed and
    /// the dumper's ATA balance is less than `amount_in`, the wallet is banned.
    pub fn handle_opp_check(
        &self,
        sig: String,
        dumper_pk: Pubkey,
        dumper_ata: Pubkey,
        amount_in: u64,
    ) {
        {
            let mut g = self.inner.in_flight.lock().unwrap();
            if !g.insert(sig.clone()) {
                return; // another location already reported this sig
            }
        }
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let result = process_opp(&inner, &sig, dumper_pk, dumper_ata, amount_in).await;
            if let Err(e) = result {
                println!("[ban] opp_check failed sig={sig} dumper={dumper_pk}: {e:#}");
            }
            inner.in_flight.lock().unwrap().remove(&sig);
        });
    }
}

async fn process_opp(
    inner: &Inner,
    sig: &str,
    dumper_pk: Pubkey,
    dumper_ata: Pubkey,
    amount_in: u64,
) -> anyhow::Result<()> {
    tokio::time::sleep(STATUS_CHECK_DELAY).await;
    let rpc = RpcClient::new_with_commitment(inner.rpc_url.clone(), CommitmentConfig::confirmed());
    let parsed_sig: solana_sdk::signature::Signature =
        sig.parse().context("invalid signature")?;
    let statuses = rpc
        .get_signature_statuses(&[parsed_sig])
        .await
        .context("getSignatureStatuses")?;
    let Some(Some(status)) = statuses.value.into_iter().next() else {
        // Not yet processed (or expired). Drop — we don't ban without proof.
        return Ok(());
    };
    if status.err.is_none() {
        // Tx succeeded — real dump, not a fake. Drop.
        return Ok(());
    }
    // Tx failed on-chain. Check ATA balance to determine if it was an
    // intentional insufficient-funds fake. Use _with_commitment so we get
    // Response<Option<Account>> — distinguishes "not found" from RPC errors.
    let resp = rpc
        .get_account_with_commitment(&dumper_ata, CommitmentConfig::confirmed())
        .await
        .context("getAccountInfo on dumper_ata")?;
    let account = match resp.value {
        Some(a) => a,
        None => {
            // ATA doesn't exist — couldn't have held any tokens at tx time.
            // Treat as balance=0, ban applies.
            ban_wallet(inner, dumper_pk, "ata_not_found", amount_in, 0)?;
            return Ok(());
        }
    };
    let balance = read_token_account_amount(&account.data)
        .ok_or_else(|| anyhow::anyhow!("ata data too small to read amount"))?;
    if balance < amount_in {
        ban_wallet(inner, dumper_pk, "insufficient_balance", amount_in, balance)?;
    }
    Ok(())
}

fn ban_wallet(
    inner: &Inner,
    wallet: Pubkey,
    reason: &str,
    amount_in: u64,
    balance: u64,
) -> anyhow::Result<()> {
    let banned_until_ms = chrono_now_ms() + BAN_DURATION.as_millis() as u64;
    let entry = BanEntry {
        banned_until_ms,
        reason: reason.to_owned(),
    };
    let bytes = bincode::serialize(&entry).context("encode ban entry")?;
    inner
        .db
        .insert(wallet.as_ref(), bytes)
        .context("sled insert ban")?;
    inner.db.flush().context("sled flush")?;
    let _ = inner.bcast.send(ServerMsg::WalletBanned {
        wallet: wallet.to_string(),
        banned_until_ms,
        reason: reason.to_owned(),
    });
    println!(
        "[ban] wallet={wallet} reason={reason} amount_in={amount_in} balance={balance} until={banned_until_ms}"
    );
    Ok(())
}

/// Read the `amount: u64` field at offset 64 of an SPL Token / Token-2022
/// account. Layout for both: mint(32) + owner(32) + amount(8) + ...
fn read_token_account_amount(data: &[u8]) -> Option<u64> {
    if data.len() < 72 {
        return None;
    }
    let arr: [u8; 8] = data[64..72].try_into().ok()?;
    Some(u64::from_le_bytes(arr))
}

fn chrono_now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Map of currently active bans (wallet → BanEntry). Used internally for
/// inspection; not exposed to clients (they consume via /bans.bin + WS).
#[allow(dead_code)]
pub fn active_bans(store: &BansStore) -> HashMap<Pubkey, BanEntry> {
    let now_ms = chrono_now_ms();
    let mut out = HashMap::new();
    for kv in store.inner.db.iter().flatten() {
        let (k, v) = kv;
        if k.len() != 32 {
            continue;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&k);
        let wallet = Pubkey::new_from_array(arr);
        if let Ok(entry) = bincode::deserialize::<BanEntry>(&v) {
            if entry.banned_until_ms > now_ms {
                out.insert(wallet, entry);
            }
        }
    }
    out
}
