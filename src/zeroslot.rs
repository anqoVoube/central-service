//! Shared Zeroslot (0slot.trade) send-transaction helper.
//!
//! Replaces `RpcClient::send_transaction` / `send_and_confirm_transaction`
//! in `ata::create` and `measure::measure_pool_cu` so central's outbound
//! txs go through the operator's paid Zeroslot lane instead of the
//! public mainnet RPC.
//!
//! Body shape is standard JSON-RPC `sendTransaction` with `encoding=base64`
//! (same as the bot's `zeroslot_for_loc` sender at
//! `services/mod.rs:1327`). Returns the local tx signature; the caller
//! is responsible for confirming separately via the regular RPC
//! (`get_signature_statuses`).

use std::time::Duration;

use anyhow::{anyhow, Context};
use base64::Engine;
use solana_sdk::{signature::Signature, transaction::Transaction};

/// Zeroslot Germany endpoint — central runs at FR so this is closest.
/// API key matches the bot's `services/mod.rs:1329` so rate-limit /
/// quotas are shared with the production senders. Body is plain
/// JSON-RPC `sendTransaction`. Swap the `de1` → `ams1`/`ny1`/`jp1` if
/// central moves.
pub const ZEROSLOT_URL: &str =
    "http://de1.0slot.trade?api-key=bf3104bce97a421a97c6d2f4063f9cf3";

/// Zeroslot tip recipient — matches the bot's
/// `services/mod.rs:1334`. Caller must include an inline
/// `system_instruction::transfer(wallet → this, TIP_LAMPORTS)` ix
/// for Zeroslot to accept the tx into its priority lane.
pub const ZEROSLOT_TIP_ACCOUNT: &str = "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3";

/// Default tip (lamports). 100k = ~$0.01 at $97/SOL. Operator-tunable
/// at the call site if needed.
pub const TIP_LAMPORTS: u64 = 100_000;

/// Send a signed transaction via Zeroslot's HTTP `sendTransaction`
/// endpoint. Returns the local signature on a successful 2xx response.
/// Confirmation (waiting for the tx to land) is the caller's job.
pub async fn send_transaction(
    http: &reqwest::Client,
    tx: &Transaction,
) -> anyhow::Result<Signature> {
    let tx_bytes = bincode::serialize(tx).context("bincode serialize tx")?;
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
    let local_sig: Signature = tx.signatures.first().copied().ok_or_else(|| {
        anyhow!("tx has no signatures — cannot send_via_zeroslot")
    })?;
    let resp = http
        .post(ZEROSLOT_URL)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .context("zeroslot http send")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "zeroslot non-2xx: status={status} body={text}"
        ));
    }
    let parsed: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("zeroslot parse response: {text}"))?;
    if let Some(err) = parsed.get("error") {
        return Err(anyhow!("zeroslot rpc-level error: {err}"));
    }
    Ok(local_sig)
}

/// Convenience: a shared `reqwest::Client` tuned for Zeroslot sends.
pub fn build_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(8)
        .tcp_nodelay(true)
        .build()
        .context("build zeroslot http client")
}
