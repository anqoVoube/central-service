use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair};
use tokio::sync::{broadcast, mpsc::UnboundedReceiver};


use crate::{
    ata,
    mongo::Repo,
    pool::{pump_fun, AtaStatus, PoolAccounts, PoolDoc, PumpFunAccounts},
    ws::ServerMsg,
};

pub async fn run(
    mut rx: UnboundedReceiver<String>,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
    repo: Arc<Repo>,
    broadcast: broadcast::Sender<ServerMsg>,
) {
    while let Some(pool_str) = rx.recv().await {
        let rpc_url = rpc_url.clone();
        let wallet_kp = Arc::clone(&wallet_kp);
        let repo = Arc::clone(&repo);
        let broadcast = broadcast.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_one(pool_str, rpc_url, wallet_kp, repo, broadcast).await {
                eprintln!("[discover] {e:#}");
            }
        });
    }
}

async fn handle_one(
    pool_str: String,
    rpc_url: String,
    wallet_kp: Arc<Keypair>,
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

    let (token_name, token_symbol) = fetch_token_meta(&base_mint).await;

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
            token_decimals,
        }),
        ata_status: AtaStatus::Pending,
        ata_attempts: 0,
        token_name,
        token_symbol,
    };

    let inserted = repo.upsert_pending(&doc).await.context("upsert_pending")?;
    if !inserted {
        return Ok(());
    }

    tokio::spawn(ata::create(
        pool_str,
        base_mint,
        token_program,
        wallet_kp,
        rpc_url,
        repo,
        broadcast,
        doc,
    ));

    Ok(())
}

/// Best-effort Dexscreener lookup. 3s timeout. On any failure (network,
/// status, parse, or no matching token in the response) returns `(None, None)`.
async fn fetch_token_meta(mint: &Pubkey) -> (Option<String>, Option<String>) {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{mint}");
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return (None, None),
    };
    let resp = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            eprintln!("[dexscreener] {url} → {}", r.status());
            return (None, None);
        }
        Err(e) => {
            eprintln!("[dexscreener] {url} failed: {e}");
            return (None, None);
        }
    };
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[dexscreener] {mint} parse failed: {e}");
            return (None, None);
        }
    };
    let mint_s = mint.to_string();
    let pairs = match v.get("pairs").and_then(|x| x.as_array()) {
        Some(p) => p,
        None => return (None, None),
    };
    for p in pairs {
        for side in ["baseToken", "quoteToken"] {
            let Some(t) = p.get(side) else { continue };
            let addr = t.get("address").and_then(|x| x.as_str()).unwrap_or("");
            if addr == mint_s {
                let name = t
                    .get("name")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_owned());
                let symbol = t
                    .get("symbol")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_owned());
                return (name, symbol);
            }
        }
    }
    (None, None)
}
