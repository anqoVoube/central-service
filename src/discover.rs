use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use tokio::sync::{broadcast, mpsc::UnboundedReceiver};


use crate::{
    mongo::Repo,
    pool::{pump_fun, AtaStatus, PoolAccounts, PoolDoc, PumpFunAccounts},
    ws::ServerMsg,
};

/// Discovery no longer signs anything, so it no longer takes the wallet:
/// the only transaction it ever sent was the ATA create, and provisioning
/// now happens inside the bot's own buy.
pub async fn run(
    mut rx: UnboundedReceiver<String>,
    rpc_url: String,
    repo: Arc<Repo>,
    broadcast: broadcast::Sender<ServerMsg>,
) {
    while let Some(pool_str) = rx.recv().await {
        let rpc_url = rpc_url.clone();
        let repo = Arc::clone(&repo);
        let broadcast = broadcast.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_one(pool_str, rpc_url, repo, broadcast).await {
                eprintln!("[discover] {e:#}");
            }
        });
    }
}

async fn handle_one(
    pool_str: String,
    rpc_url: String,
    repo: Arc<Repo>,
    broadcast: broadcast::Sender<ServerMsg>,
) -> anyhow::Result<()> {
    let pool_pk = Pubkey::from_str(&pool_str).context("invalid pool pubkey")?;

    if repo.exists(&pool_str).await.context("exists check")? {
        return Ok(());
    }

    let rpc = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    let pool_acc = rpc
        .get_account(&pool_pk)
        .await
        .with_context(|| format!("getAccountInfo({pool_pk})"))?;

    let parsed = pump_fun::parse_pool(&pool_acc.data)
        .with_context(|| format!("parse_pool({pool_pk})"))?;

    let is_cashback = pump_fun::parse_is_cashback_coin(&pool_acc.data);
    let is_mayhem_mode = pump_fun::parse_is_mayhem_mode(&pool_acc.data);

    let base_mint = parsed.base_mint;
    let mint_acc = rpc
        .get_account(&base_mint)
        .await
        .with_context(|| format!("getAccountInfo(base_mint={base_mint})"))?;
    let token_program = mint_acc.owner;
    // SPL Token Mint layout: byte 44 is the decimals field (Token-2022 keeps this).
    let token_decimals = mint_acc
        .data
        .get(44)
        .copied()
        .with_context(|| format!("mint {base_mint} data too short for decimals field"))?;

    let (token_name, token_symbol, pair_created_at_ms) =
        fetch_pair_meta(&base_mint, &pool_pk).await;

    let doc = PoolDoc {
        pool: pool_str.clone(),
        accounts: PoolAccounts::PumpFun(PumpFunAccounts {
            base_mint: parsed.base_mint.to_string(),
            quote_mint: parsed.quote_mint.to_string(),
            pool_base_token_account: parsed.pool_base_token_account.to_string(),
            pool_quote_token_account: parsed.pool_quote_token_account.to_string(),
            coin_creator: parsed.coin_creator.to_string(),
            owner_program: token_program.to_string(),
            is_cashback,
            is_mayhem_mode,
            token_decimals,
        }),
        // Always Confirmed. The field is vestigial: central no longer
        // creates ATAs at all. The bot's buy transaction carries
        // `create_associated_token_account_idempotent` for the token side,
        // so provisioning happens at trade time, in the same transaction
        // that needs it. Keeping the value at Confirmed is what makes the
        // WS init filter and `pools_for_cu_measurement` still see the row.
        ata_status: AtaStatus::Confirmed,
        ata_attempts: 0,
        token_name,
        token_symbol,
        pair_created_at_ms,
        compute_unit_limit: None,
        cu_measured_at: None,
        is_unique: false,
        disabled: false,
        is_ttp: false,
    };

    let inserted = repo.upsert_pending(&doc).await.context("upsert_pending")?;
    if !inserted {
        return Ok(());
    }

    // Announce the pool. This used to be sent from inside `ata::create` on
    // ATA confirmation, which coupled a bot's visibility of a pool to an
    // on-chain transaction succeeding — and, once ATA creation went away,
    // would have meant no running bot ever heard about a new pool until it
    // reconnected. `compute_unit_limit` is None here: the CU probe pass in
    // `bg_worker` fills it in later if it ever runs. The copy strategy uses
    // a fixed `COPY_CU_LIMIT` and does not read this value.
    let _ = broadcast.send(ServerMsg::NewPool {
        pool: doc.pool.clone(),
        accounts: doc.accounts.clone(),
        pair_created_at_ms: doc.pair_created_at_ms,
        compute_unit_limit: None,
    });

    Ok(())
}

/// Best-effort Dexscreener lookup. 3s timeout. Returns
/// `(name, symbol, pair_created_at_ms)`; all three are `None` on any
/// failure (network, status, parse, or no relevant entry).
///
/// Match strategy: prefer the pair whose `pairAddress` equals our pool
/// pubkey — that's the exact Pump.fun pAMM pool we're tracking and its
/// `pairCreatedAt` is authoritative. If Dexscreener hasn't indexed the
/// pool yet (common for very fresh pools), fall back to the first pair
/// whose base/quote token matches our mint for name/symbol; pair age is
/// left `None` in that case so we don't pin it to a different pool's
/// timestamp.
async fn fetch_pair_meta(
    mint: &Pubkey,
    pool: &Pubkey,
) -> (Option<String>, Option<String>, Option<i64>) {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{mint}");
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return (None, None, None),
    };
    let resp = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            eprintln!("[dexscreener] {url} → {}", r.status());
            return (None, None, None);
        }
        Err(e) => {
            eprintln!("[dexscreener] {url} failed: {e}");
            return (None, None, None);
        }
    };
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[dexscreener] {mint} parse failed: {e}");
            return (None, None, None);
        }
    };
    let mint_s = mint.to_string();
    let pool_s = pool.to_string();
    let pairs = match v.get("pairs").and_then(|x| x.as_array()) {
        Some(p) => p,
        None => return (None, None, None),
    };

    let pick_name_symbol = |p: &serde_json::Value| -> (Option<String>, Option<String>) {
        for side in ["baseToken", "quoteToken"] {
            let Some(t) = p.get(side) else { continue };
            let addr = t.get("address").and_then(|x| x.as_str()).unwrap_or("");
            if addr == mint_s {
                let name = t.get("name").and_then(|x| x.as_str()).map(str::to_owned);
                let symbol = t.get("symbol").and_then(|x| x.as_str()).map(str::to_owned);
                return (name, symbol);
            }
        }
        (None, None)
    };

    // Pass 1: exact pool match → reliable pair age.
    for p in pairs {
        let addr = p.get("pairAddress").and_then(|x| x.as_str()).unwrap_or("");
        if addr == pool_s {
            let (name, symbol) = pick_name_symbol(p);
            let created = p.get("pairCreatedAt").and_then(|x| x.as_i64());
            return (name, symbol, created);
        }
    }
    // Pass 2: no exact pool — keep name/symbol from the first mint match,
    // leave pair age unknown to avoid pinning to the wrong pool.
    for p in pairs {
        let (name, symbol) = pick_name_symbol(p);
        if name.is_some() || symbol.is_some() {
            return (name, symbol, None);
        }
    }
    (None, None, None)
}
