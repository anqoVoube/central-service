//! Shared Astralane send-transaction helper.
//!
//! Replaces `RpcClient::send_transaction` / `send_and_confirm_transaction`
//! in `ata::create` and `measure::measure_pool_cu` so central's outbound
//! txs go through the operator's paid Astralane lane instead of the
//! public mainnet RPC.
//!
//! Body shape is the bot's known-working `/iris` JSON-RPC sendTransaction
//! with `encoding=base64` (matches `services/mod.rs` astralane_http
//! sender + this repo's `astra-top-up` bin). Returns the local tx
//! signature; the caller is responsible for confirming separately via
//! the regular RPC (`get_signature_statuses`).

use std::time::Duration;

use anyhow::{anyhow, Context};
use base64::Engine;
use solana_sdk::{signature::Signature, transaction::Transaction};

/// Astralane Frankfurt regular send endpoint. Central runs on FR so the
/// closest region is the right default. Body shape is identical across
/// regions — swap the URL to `ams`/`ny` if central moves.
pub const ASTRALANE_URL: &str =
    "http://fr.gateway.astralane.io/iris?api-key=magdamoIsDGDc8KVNBjdNcLgHDyLRNNbwBUc0w2Exy9pewMO6lRz4uobPPydUvNC";

/// Send a signed transaction via Astralane's HTTP `sendTransaction`
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
        anyhow!("tx has no signatures — cannot send_via_astralane")
    })?;
    let resp = http
        .post(ASTRALANE_URL)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .context("astralane http send")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "astralane non-2xx: status={status} body={text}"
        ));
    }
    // Iris returns the standard JSON-RPC envelope — check for an
    // `error` field even on 200. We don't use the `result` (signature)
    // string; the local signature is authoritative.
    let parsed: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("astralane parse response: {text}"))?;
    if let Some(err) = parsed.get("error") {
        return Err(anyhow!("astralane rpc-level error: {err}"));
    }
    Ok(local_sig)
}

/// Convenience: a shared `reqwest::Client` tuned for Astralane sends.
/// 15s timeout matches `astra-top-up`; keep-alive pool prevents
/// per-call TCP handshakes from dominating latency.
pub fn build_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(8)
        .tcp_nodelay(true)
        .build()
        .context("build astralane http client")
}
