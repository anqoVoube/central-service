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
