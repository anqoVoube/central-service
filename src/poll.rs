use std::{str::FromStr, sync::Arc, time::Duration};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::{
    mongo::Repo,
    pool::{pump_fun, PoolAccounts},
    ws::ServerMsg,
};

pub async fn run(
    repo: Arc<Repo>,
    rpc_url: String,
    interval: Duration,
    tx: broadcast::Sender<ServerMsg>,
) -> anyhow::Result<()> {
    let rpc = RpcClient::new(rpc_url);
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if let Err(e) = scan_once(&repo, &rpc, &tx).await {
            tracing::error!("pump_fun creator poll failed: {e:#}");
        }
    }
}

async fn scan_once(
    repo: &Repo,
    rpc: &RpcClient,
    tx: &broadcast::Sender<ServerMsg>,
) -> anyhow::Result<()> {
    // Narrowed to pools the bot will actually trade — `is_unique=true OR
    // pair_created_at_ms > now - 30d`. Old non-unique pools stay in Mongo
    // but skip polling to keep RPC load proportional to active set.
    let pools = repo.load_pump_fun_for_creator_poll().await?;
    if pools.is_empty() {
        return Ok(());
    }
    tracing::info!(
        "scanning {} pump_fun pools for creator drift (is_unique=true OR age<30d)",
        pools.len()
    );

    for chunk in pools.chunks(100) {
        let keys: Vec<Pubkey> = chunk
            .iter()
            .filter_map(|p| Pubkey::from_str(&p.pool).ok())
            .collect();
        if keys.len() != chunk.len() {
            tracing::warn!(
                "{} pool addresses in this chunk were unparseable",
                chunk.len() - keys.len()
            );
        }
        let fetched = rpc.get_multiple_accounts(&keys).await?;
        for (idx, opt) in fetched.into_iter().enumerate() {
            let Some(acc) = opt else { continue };
            let pool_doc = &chunk[idx];
            let PoolAccounts::PumpFun(ref pf) = pool_doc.accounts else {
                continue;
            };
            let Some(on_chain) = pump_fun::parse_coin_creator(&acc.data) else {
                tracing::warn!("could not parse coin_creator for {}", pool_doc.pool);
                continue;
            };
            let on_chain_str = on_chain.to_string();
            if on_chain_str == pf.coin_creator {
                continue;
            }
            tracing::info!(
                "coin_creator drift for {}: {} -> {}",
                pool_doc.pool,
                pf.coin_creator,
                on_chain_str
            );
            if let Err(e) = repo.update_creator(&pool_doc.pool, &on_chain_str).await {
                tracing::error!(
                    "failed to persist creator change for {}: {e:#}",
                    pool_doc.pool
                );
                continue;
            }
            let _ = tx.send(ServerMsg::CreatorChange {
                pool: pool_doc.pool.clone(),
                old_creator: pf.coin_creator.clone(),
                new_creator: on_chain_str,
            });
        }
    }
    Ok(())
}
