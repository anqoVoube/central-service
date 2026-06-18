//! One-shot Astralane shred-pay tipper.
//!
//! Sends a fixed **0.1 SOL** transfer from `WALLET_KEYPAIR` to Astralane's
//! first tip address, submitted via their **Frankfurt** shred-pay endpoint.
//! Astralane aggregates cumulative tips per wallet over a trailing 24h
//! window: 0.2 SOL → tier-1, 0.5 SOL → tier-2 (see Astralane docs).
//!
//! Run (after `cargo build --release -p central-service`):
//!   ~/Work/central-service/target/release/astra-top-up
//!
//! Requires `.env` with `WALLET_KEYPAIR` (base58-encoded keypair).
//!
//! Tx layout (3 instructions, no nonce, no compute-budget — fee paid via
//! standard priority fee = 0 lamports/CU since tip itself is the priority):
//!   ix[0] system::transfer(0.1 SOL → ASTZHpta…)
//!
//! Recent blockhash is fetched from Helius (regular mainnet RPC), the tx is
//! signed, then submitted via JSON-RPC POST to the Astralane shred-pay
//! endpoint. Confirmation is polled via Helius (the shred-pay endpoint may
//! not expose `getSignatureStatuses`).

use std::time::Duration;

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use solana_transaction_status_client_types::TransactionConfirmationStatus;

/// Astralane Frankfurt shred-pay endpoint. The api-key in the URL is
/// operator-issued via portal.astralane.io. Hardcoded here per operator
/// request — rotate by editing this constant.
const ASTRALANE_SHRED_PAY_URL: &str =
    "http://fr.gateway.astralane.io/shred-pay?api-key=magdamoIsDGDc8KVNBjdNcLgHDyLRNNbwBUc0w2Exy9pewMO6lRz4uobPPydUvNC";

/// First of Astralane's published tip addresses (4 total). Astralane
/// aggregates tips per-sender across the whole set, so any one works;
/// we always use this one for simplicity.
const ASTRALANE_TIP_RECIPIENT: &str = "ASTZHptaMgYVMX6DAocDr1vVXLran5PpfKfQtVTSWkfE";

/// Regular RPC for fetching blockhash + confirming the tx landed.
const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=75715a51-2511-436d-ad3a-1d8c76208072";

/// 0.1 SOL = 100M lamports. Five back-to-back runs in 24h reach the
/// tier-2 threshold (0.5 SOL).
const TIP_LAMPORTS: u64 = 100_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let wallet_keypair_b58 =
        std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let wallet_kp = Keypair::from_base58_string(&wallet_keypair_b58);
    let wallet_pk = wallet_kp.pubkey();
    let tip_to: Pubkey = ASTRALANE_TIP_RECIPIENT
        .parse()
        .context("parse Astralane tip recipient pubkey")?;

    let helius = RpcClient::new_with_commitment(
        HELIUS_RPC.to_string(),
        CommitmentConfig::confirmed(),
    );
    let astralane = RpcClient::new_with_commitment(
        ASTRALANE_SHRED_PAY_URL.to_string(),
        CommitmentConfig::confirmed(),
    );

    let (recent_blockhash, _last_valid) = helius
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
        .await
        .context("get latest blockhash from Helius")?;

    let transfer_ix = system_instruction::transfer(&wallet_pk, &tip_to, TIP_LAMPORTS);
    let msg = Message::new_with_blockhash(
        &[transfer_ix],
        Some(&wallet_pk),
        &recent_blockhash,
    );
    let mut tx = Transaction::new_unsigned(msg);
    tx.sign(&[&wallet_kp], recent_blockhash);

    tracing::info!(
        wallet = %wallet_pk,
        recipient = %tip_to,
        lamports = TIP_LAMPORTS,
        "submitting Astralane tip via FR shred-pay endpoint"
    );

    let sig: Signature = astralane
        .send_transaction(&tx)
        .await
        .context("astralane send_transaction")?;
    tracing::info!("submitted sig={sig}");
    println!("https://solscan.io/tx/{sig}");

    // Confirm via Helius — the Astralane shred-pay endpoint is a
    // tip-only RPC and may not implement getSignatureStatuses.
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    loop {
        let statuses = helius.get_signature_statuses(&[sig]).await?;
        if let Some(Some(status)) = statuses.value.into_iter().next() {
            if let Some(conf) = &status.confirmation_status {
                if matches!(
                    conf,
                    TransactionConfirmationStatus::Confirmed
                        | TransactionConfirmationStatus::Finalized,
                ) {
                    if let Some(err) = status.err {
                        anyhow::bail!("Astralane tip tx failed on-chain: {err:?}");
                    }
                    tracing::info!(
                        elapsed_ms = %start.elapsed().as_millis(),
                        "tip landed — counted toward Astralane 24h tier"
                    );
                    return Ok(());
                }
            }
        }
        if start.elapsed() > timeout {
            anyhow::bail!("confirm timeout sig={sig}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
