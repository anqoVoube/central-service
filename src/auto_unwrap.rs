//! Auto-unwrap poller — keeps the shared wallet's native SOL balance
//! topped up so bots (across ALL locations) can always pay fees/tips.
//! Runs on central so multi-location bots don't race to unwrap.
//!
//! Every 30 s the poller reads native SOL. When it falls below the
//! operator-configured `low_threshold`, we unwrap enough WSOL to bring
//! it back to `high_threshold`. If WSOL is empty, a Telegram alert
//! fires (deduped to at most once per 30 min) instead of a swap.
//!
//! Config is exposed to the dashboard via `GET/POST /auto-unwrap/config`
//! on central's HTTP surface — the dashboard is a thin proxy.
//!
//! Persistence: JSON file at `AUTO_UNWRAP_CONFIG_PATH` (default
//! `./auto_unwrap_config.json`). Atomic rewrite (tmp + rename) on
//! operator save; loaded once at startup. `enabled: false` is the
//! default — bots never auto-swap until the operator toggles on.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};

pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// Poller cadence. Chosen to be short enough that a fee drought is
/// caught within ~30 s, long enough that idle RPC cost is negligible.
const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Suppress duplicate WSOL-empty Telegram alerts within this window.
const ALERT_DEDUP: Duration = Duration::from_secs(1800);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AutoUnwrapConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Trigger unwrap when native SOL < this (default 1 SOL).
    #[serde(default = "default_low")]
    pub low_threshold_lamports: u64,
    /// Target native SOL after unwrap completes (default 3 SOL).
    #[serde(default = "default_high")]
    pub high_threshold_lamports: u64,
}

fn default_low() -> u64 {
    1_000_000_000
}
fn default_high() -> u64 {
    3_000_000_000
}

impl Default for AutoUnwrapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            low_threshold_lamports: default_low(),
            high_threshold_lamports: default_high(),
        }
    }
}

impl AutoUnwrapConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.low_threshold_lamports == 0 {
            return Err("low threshold must be > 0".into());
        }
        if self.high_threshold_lamports <= self.low_threshold_lamports {
            return Err("high threshold must be > low".into());
        }
        Ok(())
    }
}

pub fn config_path() -> PathBuf {
    std::env::var("AUTO_UNWRAP_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("auto_unwrap_config.json"))
}

pub fn load_config() -> AutoUnwrapConfig {
    let p = config_path();
    match std::fs::read_to_string(&p) {
        Ok(s) => match serde_json::from_str::<AutoUnwrapConfig>(&s) {
            Ok(cfg) => match cfg.validate() {
                Ok(()) => {
                    println!(
                        "[auto-unwrap] loaded: enabled={} low={:.4} SOL high={:.4} SOL",
                        cfg.enabled,
                        cfg.low_threshold_lamports as f64 / 1e9,
                        cfg.high_threshold_lamports as f64 / 1e9,
                    );
                    cfg
                }
                Err(e) => {
                    eprintln!(
                        "[auto-unwrap] config at {} invalid ({e}); using defaults",
                        p.display()
                    );
                    AutoUnwrapConfig::default()
                }
            },
            Err(e) => {
                eprintln!("[auto-unwrap] parse {} failed: {e}; using defaults", p.display());
                AutoUnwrapConfig::default()
            }
        },
        Err(_) => {
            println!(
                "[auto-unwrap] no config at {}, using defaults (disabled)",
                p.display()
            );
            AutoUnwrapConfig::default()
        }
    }
}

pub fn save_config(cfg: &AutoUnwrapConfig) -> anyhow::Result<()> {
    let p = config_path();
    let tmp = p.with_extension("json.tmp");
    let body = serde_json::to_string_pretty(cfg)?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, &p)?;
    Ok(())
}

/// ATA derivation: PDA of (owner, token_program, mint) under the ATA
/// program. Matches spl-associated-token-account's off-chain helper.
fn find_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let token_prog = Pubkey::try_from(SPL_TOKEN_PROGRAM).unwrap();
    let ata_prog = Pubkey::try_from(ATA_PROGRAM).unwrap();
    Pubkey::find_program_address(
        &[owner.as_ref(), token_prog.as_ref(), mint.as_ref()],
        &ata_prog,
    )
    .0
}

/// SPL Token `CloseAccount` (tag 9). Sweeps `account`'s SOL balance to
/// `destination`, owner signs.
fn ix_close_account(account: &Pubkey, destination: &Pubkey, owner: &Pubkey) -> Instruction {
    let token_prog = Pubkey::try_from(SPL_TOKEN_PROGRAM).unwrap();
    Instruction {
        program_id: token_prog,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*owner, true),
        ],
        data: vec![9],
    }
}

/// SPL Token `SyncNative` (tag 17). Refreshes the WSOL account's
/// balance field to the lamports actually deposited via a SOL transfer.
fn ix_sync_native(ata: &Pubkey) -> Instruction {
    let token_prog = Pubkey::try_from(SPL_TOKEN_PROGRAM).unwrap();
    Instruction {
        program_id: token_prog,
        accounts: vec![AccountMeta::new(*ata, false)],
        data: vec![17],
    }
}

/// SPL Associated Token Account `CreateIdempotent` (tag 1). Creates the
/// ATA if absent; no-op if it already exists. Payer + funding_wallet
/// both = our wallet.
fn ix_create_ata_idempotent(
    payer: &Pubkey,
    ata: &Pubkey,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Instruction {
    let ata_prog = Pubkey::try_from(ATA_PROGRAM).unwrap();
    let token_prog = Pubkey::try_from(SPL_TOKEN_PROGRAM).unwrap();
    let system_prog = Pubkey::try_from(SYSTEM_PROGRAM).unwrap();
    Instruction {
        program_id: ata_prog,
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(*ata, false),
            AccountMeta::new_readonly(*owner, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(system_prog, false),
            AccountMeta::new_readonly(token_prog, false),
        ],
        data: vec![1],
    }
}

/// Read the SPL token-account `amount` (offset 64..72). Returns 0 if
/// the account doesn't exist yet — the caller decides how to interpret.
async fn get_wsol_balance(rpc: &RpcClient, ata: &Pubkey) -> anyhow::Result<u64> {
    match rpc.get_account(ata).await {
        Ok(a) if a.data.len() >= 72 => {
            let bytes: [u8; 8] = a.data[64..72].try_into().unwrap();
            Ok(u64::from_le_bytes(bytes))
        }
        Ok(_) => Ok(0),
        Err(e) => {
            // AccountNotFound is expected when the WSOL ATA hasn't been
            // created yet — treat as balance = 0. Other RPC errors bubble.
            let s = e.to_string();
            if s.contains("AccountNotFound") || s.contains("could not find account") {
                Ok(0)
            } else {
                Err(e).context("get_account(wsol_ata)")
            }
        }
    }
}

/// Build + send the unwrap tx. Closes the WSOL account (sweeping the
/// full balance to native SOL), then re-wraps `remainder = balance -
/// amount` back into a fresh ATA so subsequent buys still find it.
/// If `remainder == 0`, the ATA stays closed and its rent (~0.00204 SOL)
/// is returned — the next bot buy that touches WSOL will re-create it
/// via `create_ata_idempotent` in the swap ix layout.
async fn do_unwrap(
    rpc: &RpcClient,
    wallet_kp: &Keypair,
    wsol_ata: Pubkey,
    amount: u64,
) -> anyhow::Result<String> {
    let owner = wallet_kp.pubkey();
    let wsol_mint = Pubkey::try_from(WSOL_MINT).unwrap();
    let wsol_balance = get_wsol_balance(rpc, &wsol_ata).await?;
    if wsol_balance == 0 {
        anyhow::bail!("wSOL account is empty or does not exist");
    }
    if amount > wsol_balance {
        anyhow::bail!("amount {amount} > wSOL balance {wsol_balance}");
    }
    let remainder = wsol_balance - amount;
    let mut ixs = vec![ix_close_account(&wsol_ata, &owner, &owner)];
    if remainder > 0 {
        ixs.push(ix_create_ata_idempotent(&owner, &wsol_ata, &owner, &wsol_mint));
        ixs.push(system_instruction::transfer(&owner, &wsol_ata, remainder));
        ixs.push(ix_sync_native(&wsol_ata));
    }
    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&owner), &[wallet_kp], blockhash);
    let sig = rpc
        .send_and_confirm_transaction(&tx)
        .await
        .context("send_and_confirm_transaction")?;
    Ok(sig.to_string())
}

/// Best-effort Telegram alert. No-op if TELEGRAM_BOT_TOKEN or
/// TELEGRAM_CHAT_ID env vars are unset.
async fn telegram_alert(msg: impl Into<String>) {
    let body = msg.into();
    eprintln!("[alert] {body}");
    let token = match std::env::var("TELEGRAM_BOT_TOKEN") {
        Ok(v) if !v.is_empty() => v,
        _ => return,
    };
    let chat = match std::env::var("TELEGRAM_CHAT_ID") {
        Ok(v) if !v.is_empty() => v,
        _ => return,
    };
    let client = reqwest::Client::new();
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    if let Err(e) = client
        .post(&url)
        .json(&serde_json::json!({ "chat_id": chat, "text": body }))
        .send()
        .await
    {
        eprintln!("[alert] telegram send failed: {e}");
    }
}

/// Spawn the 30 s poller. Reads config via ArcSwap so the operator's
/// `/auto-unwrap/config` POST takes effect on the next tick with no
/// restart.
pub fn spawn(
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
    config: Arc<ArcSwap<AutoUnwrapConfig>>,
) {
    tokio::spawn(async move {
        let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
        let owner = wallet_kp.pubkey();
        let wsol_mint = Pubkey::try_from(WSOL_MINT).unwrap();
        let wsol_ata = find_ata(&owner, &wsol_mint);
        println!(
            "[auto-unwrap] poller started owner={owner} wsol_ata={wsol_ata} tick={:?}",
            POLL_INTERVAL
        );
        let mut last_alert_at: Option<Instant> = None;
        let mut in_flight = false;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if in_flight {
                continue;
            }
            let cfg = **config.load();
            if !cfg.enabled {
                continue;
            }
            let sol = match rpc.get_balance(&owner).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[auto-unwrap] getBalance failed: {e}");
                    continue;
                }
            };
            if sol >= cfg.low_threshold_lamports {
                continue;
            }
            let need = cfg.high_threshold_lamports.saturating_sub(sol);
            if need == 0 {
                continue;
            }
            let wsol = match get_wsol_balance(&rpc, &wsol_ata).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[auto-unwrap] wsol balance failed: {e:#}");
                    continue;
                }
            };
            // Unwrap as much as we can toward the target. When WSOL is short
            // of the full gap we still unwrap what's there (partial top-up)
            // instead of refusing — a partial top-up beats leaving SOL below
            // the floor. Only alert when there's literally nothing to unwrap.
            if wsol == 0 {
                let now = Instant::now();
                let should_alert = last_alert_at
                    .map(|t| now.duration_since(t) > ALERT_DEDUP)
                    .unwrap_or(true);
                if should_alert {
                    last_alert_at = Some(now);
                    telegram_alert(format!(
                        "[auto-unwrap] WSOL empty — SOL {:.4} below low {:.4}, nothing to unwrap. Manually top up.",
                        sol as f64 / 1e9,
                        cfg.low_threshold_lamports as f64 / 1e9,
                    ))
                    .await;
                }
                continue;
            }
            let amount = need.min(wsol); // unwrap the smaller of gap / available
            in_flight = true;
            println!(
                "[auto-unwrap] SOL={:.4} < low={:.4}, unwrapping {:.4} WSOL (have {:.4}, target={:.4} SOL)",
                sol as f64 / 1e9,
                cfg.low_threshold_lamports as f64 / 1e9,
                amount as f64 / 1e9,
                wsol as f64 / 1e9,
                cfg.high_threshold_lamports as f64 / 1e9,
            );
            match do_unwrap(&rpc, &wallet_kp, wsol_ata, amount).await {
                Ok(sig) => println!("[auto-unwrap] ok sig={sig}"),
                Err(e) => {
                    eprintln!("[auto-unwrap] tx failed: {e:#}");
                    telegram_alert(format!("[auto-unwrap] tx failed: {e:#}")).await;
                }
            }
            in_flight = false;
        }
    });
}

// Silence the `Pack` import warning when only used indirectly.
#[allow(dead_code)]
fn _pack_marker<T: Pack>() {}
