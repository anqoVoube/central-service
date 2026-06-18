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

use anyhow::{anyhow, Context};
use base64::Engine;
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
    "http://fr.gateway.astralane.io/shred-pay?api-key=lsd19O5gQJjwDiv2EaesM7g7pcOASZyyBE810zQKK5BFoLBkTeeMPt4ys2bTk0DX";

/// First of Astralane's published tip addresses (4 total). Astralane
/// aggregates tips per-sender across the whole set, so any one works;
/// we always use this one for simplicity.
const ASTRALANE_TIP_RECIPIENT: &str = "ASTZHptaMgYVMX6DAocDr1vVXLran5PpfKfQtVTSWkfE";

/// Regular RPC for fetching blockhash + confirming the tx landed.
const HELIUS_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=75715a51-2511-436d-ad3a-1d8c76208072";

/// 0.01 SOL = 10M lamports. Conservative starter amount while we
/// confirm the wire format; 20 back-to-back runs in 24h would reach
/// tier-1 (0.2 SOL), 50 would reach tier-2 (0.5 SOL).
const TIP_LAMPORTS: u64 = 10_000_000;

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

    // Astralane's shred-pay endpoint expects standard JSON-RPC
    // `sendTransaction` with `encoding=base64`. `RpcClient::send_transaction`
    // historically sends base58 which trips a 400 on `/shred-pay`; do the
    // POST by hand to match the bot's known-working `/iris` body shape.
    let tx_bytes = bincode::serialize(&tx).context("bincode serialize tx")?;
    let tx_b64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            tx_b64,
            { "encoding": "base64", "skipPreflight": true }
        ]
    });

    let local_sig: Signature = tx.signatures[0];
    tracing::info!(
        wallet = %wallet_pk,
        recipient = %tip_to,
        lamports = TIP_LAMPORTS,
        local_sig = %local_sig,
        "POSTing Astralane tip to FR shred-pay endpoint"
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build reqwest client")?;
    let body_str = serde_json::to_string(&body).context("serialize request body")?;
    let resp = http
        .post(ASTRALANE_SHRED_PAY_URL)
        .header("Content-Type", "application/json")
        .body(body_str.clone())
        .send()
        .await
        .context("POST to Astralane shred-pay")?;
    let status = resp.status();
    let resp_version = resp.version();
    // Capture headers BEFORE consuming the body.
    let headers_dump: String = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            format!(
                "    {}: {}",
                k.as_str(),
                v.to_str().unwrap_or("<non-utf8>")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let resp_bytes = resp.bytes().await.unwrap_or_default();
    let resp_text = String::from_utf8_lossy(&resp_bytes).to_string();
    if !status.is_success() {
        // Dump everything we know so we can see exactly why the gateway
        // is rejecting us — Astralane sometimes returns empty 400 bodies.
        eprintln!("=== Astralane request failed ===");
        eprintln!("URL: {}", ASTRALANE_SHRED_PAY_URL);
        eprintln!("Request method: POST");
        eprintln!("Request Content-Type: application/json");
        eprintln!("Request body ({} bytes):", body_str.len());
        eprintln!("    {}", body_str);
        eprintln!("Response HTTP version: {:?}", resp_version);
        eprintln!("Response status: {}", status);
        eprintln!("Response headers:");
        eprintln!("{}", headers_dump);
        eprintln!(
            "Response body ({} bytes, utf8-lossy):",
            resp_bytes.len()
        );
        eprintln!("    {:?}", resp_text);
        eprintln!(
            "Response body hex ({} bytes):",
            resp_bytes.len()
        );
        eprintln!("    {}", hex_dump(&resp_bytes));
        eprintln!("================================");
        return Err(anyhow!(
            "Astralane shred-pay HTTP {status} (body={} bytes, see stderr dump above)",
            resp_bytes.len()
        ));
    }
    let resp_json: serde_json::Value = serde_json::from_str(&resp_text)
        .with_context(|| format!("parse Astralane response: {resp_text}"))?;
    if let Some(err) = resp_json.get("error") {
        return Err(anyhow!("Astralane RPC error: {err}"));
    }
    let sig_str = resp_json
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("no `result` field in Astralane response: {resp_text}"))?;
    let sig: Signature = sig_str
        .parse()
        .with_context(|| format!("parse returned signature {sig_str}"))?;
    if sig != local_sig {
        tracing::warn!(
            local = %local_sig,
            returned = %sig,
            "Astralane returned a signature different from the one we signed"
        );
    }
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

/// Hex-dump bytes as `XX XX XX …` grouped 16 per line. Helpful when the
/// response body is non-UTF8 / has invisible chars / is truly empty (the
/// hex dump makes the difference obvious).
fn hex_dump(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "<empty>".to_owned();
    }
    let mut out = String::with_capacity(bytes.len() * 3 + bytes.len() / 16);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && i % 16 == 0 {
            out.push_str("\n    ");
        } else if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", b));
    }
    out
}
