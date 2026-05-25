//! Harmonic bundle submission over gRPC.
//!
//! Harmonic is a Jito-compatible block engine (same searcher protos) but with
//! Searcher role = 3 and tips expressed as priority fees rather than transfers
//! to a tip account. Auth is a challenge-response signed by the searcher
//! keypair (which must be whitelisted by Harmonic).
//!
//! Each region's block engine only builds blocks for its co-located validator,
//! so a bundle can land only while that region is leading. Per Harmonic's docs
//! the bundle is therefore submitted to every region at once — whichever region
//! is leading lands it (same signature ⇒ only one can), the rest drop the
//! duplicate.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use solana_sdk::signature::{Keypair, Signer};
use tonic::transport::{Channel, ClientTlsConfig};

// Generated gRPC client code. Each proto package is a sibling module so prost's
// cross-package paths (e.g. `super::packet::Packet` from `bundle`) resolve.
pub mod proto {
    pub mod auth {
        tonic::include_proto!("auth");
    }
    pub mod searcher {
        tonic::include_proto!("searcher");
    }
    pub mod bundle {
        tonic::include_proto!("bundle");
    }
    pub mod packet {
        tonic::include_proto!("packet");
    }
    pub mod shared {
        tonic::include_proto!("shared");
    }
}

use proto::auth::{
    auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest, Role,
};
use proto::bundle::Bundle;
use proto::packet::{Meta, Packet, PacketFlags};
use proto::searcher::{searcher_service_client::SearcherServiceClient, SendBundleRequest};
use proto::shared::Header;

/// All Harmonic block-engine regions. Docs recommend submitting to every region
/// at once so the bundle reaches whichever region is currently leading.
pub const ENDPOINTS: &[&str] = &[
    "https://fra.be.harmonic.gg",
    "https://lon.be.harmonic.gg",
    "https://ams.be.harmonic.gg",
    "https://ewr.be.harmonic.gg",
    "https://tyo.be.harmonic.gg",
    "https://sgp.be.harmonic.gg",
];

/// Authenticate against one region and submit `tx_bytes` as a single-tx bundle.
/// Returns the server-assigned bundle UUID.
async fn submit_region(endpoint: &str, wallet: Arc<Keypair>, tx_bytes: Vec<u8>) -> Result<String> {
    let pubkey = wallet.pubkey();

    let channel = Channel::from_shared(endpoint.to_string())?
        .tls_config(ClientTlsConfig::new().with_webpki_roots())?
        .connect()
        .await
        .context("connect")?;

    // ---- auth: challenge -> sign "{pubkey}-{challenge}" -> tokens ----
    let mut auth = AuthServiceClient::new(channel.clone());
    let challenge_resp = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await
        .context("GenerateAuthChallenge (is your searcher pubkey whitelisted?)")?
        .into_inner();

    let challenge = format!("{}-{}", pubkey, challenge_resp.challenge);
    let signed = wallet.sign_message(challenge.as_bytes());
    let tokens = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed.as_ref().to_vec(),
        })
        .await
        .context("GenerateAuthTokens")?
        .into_inner();
    let access = tokens
        .access_token
        .context("server returned no access_token")?
        .value;

    // ---- send bundle with Bearer token ----
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let bundle = Bundle {
        header: Some(Header {
            ts: Some(prost_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
        }),
        packets: vec![Packet {
            data: tx_bytes.clone(),
            meta: Some(Meta {
                size: tx_bytes.len() as u64,
                addr: String::new(),
                port: 0,
                flags: Some(PacketFlags::default()),
                sender_stake: 0,
            }),
        }],
    };

    let mut searcher = SearcherServiceClient::new(channel);
    let mut req = tonic::Request::new(SendBundleRequest {
        bundle: Some(bundle),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {access}").parse().context("auth header")?,
    );

    let uuid = searcher
        .send_bundle(req)
        .await
        .context("SendBundle")?
        .into_inner()
        .uuid;
    Ok(uuid)
}

/// Submit the same signed transaction as a bundle to every Harmonic region
/// concurrently. Returns `Ok` if at least one region accepted it (with the
/// bundle UUID); errors only if every region failed (last error is surfaced).
pub async fn submit_bundle(wallet: Arc<Keypair>, tx_bytes: Vec<u8>) -> Result<String> {
    let mut handles = Vec::with_capacity(ENDPOINTS.len());
    for &endpoint in ENDPOINTS {
        let w = Arc::clone(&wallet);
        let txb = tx_bytes.clone();
        handles.push(tokio::spawn(async move {
            (endpoint, submit_region(endpoint, w, txb).await)
        }));
    }

    let mut accepted: Option<String> = None;
    let mut last_err: Option<anyhow::Error> = None;
    for h in handles {
        match h.await {
            Ok((endpoint, Ok(uuid))) => {
                tracing::debug!(region = endpoint, uuid = %uuid, "harmonic region accepted");
                accepted.get_or_insert(uuid);
            }
            Ok((endpoint, Err(e))) => {
                tracing::debug!(region = endpoint, err = %format!("{e:#}"), "harmonic region rejected");
                last_err = Some(e);
            }
            Err(join) => last_err = Some(anyhow::anyhow!("task join: {join}")),
        }
    }

    match accepted {
        Some(uuid) => Ok(uuid),
        None => Err(last_err
            .unwrap_or_else(|| anyhow::anyhow!("no regions attempted"))
            .context("all Harmonic regions rejected the bundle")),
    }
}
