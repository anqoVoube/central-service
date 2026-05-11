//! Validator metadata loaded from
//! `iGroza/Solana-Validator-IP-Geolocation/solana_validators.csv`.
//!
//! Pulled once at startup, keyed by `NodePubkey` (the validator's node
//! identity that shows up as the slot leader in `getSlotLeaders`). Used to
//! turn a slot-leader pubkey into a human-readable city / country / hosting
//! tuple + a Jito flag for the dashboard.
//!
//! Fetch is best-effort: if GitHub is unreachable or the CSV is malformed,
//! we log and continue with an empty map — the dashboard renders `—`
//! everywhere but the rest of the service operates normally.

use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

/// Per-validator subset of the CSV we care about. Fields are optional
/// because the upstream rows have missing values in many columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
    /// ISO 2-letter country code (e.g. `DE`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    /// Hosting provider / datacenter (CSV column `Hosting`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosting: Option<String>,
    /// AS id (CSV column `HostingCompanyId`, e.g. `AS47447`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosting_id: Option<String>,
    #[serde(default)]
    pub is_jito: bool,
}

/// Wire-shape leader payload sent on `position_opened` / `position_failed`
/// and persisted into `positions.jsonl`. Carries the raw pubkey so the
/// dashboard / future readers can re-derive metadata if the CSV
/// gets refreshed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub pubkey: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosting: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosting_id: Option<String>,
    #[serde(default)]
    pub is_jito: bool,
}

/// In-memory validator registry keyed by `NodePubkey`.
#[derive(Debug, Default, Clone)]
pub struct ValidatorMap {
    by_node: HashMap<Pubkey, ValidatorInfo>,
}

impl ValidatorMap {
    pub fn len(&self) -> usize {
        self.by_node.len()
    }

    /// Look up by leader pubkey, attach the pubkey itself to the returned
    /// `LeaderInfo` so the wire payload always carries it (the metadata
    /// fields are `None` when the validator isn't in the CSV).
    pub fn leader_for(&self, pubkey: &Pubkey) -> LeaderInfo {
        let pk_str = pubkey.to_string();
        match self.by_node.get(pubkey) {
            Some(v) => LeaderInfo {
                pubkey: pk_str,
                name: v.name.clone(),
                city: v.city.clone(),
                country: v.country.clone(),
                hosting: v.hosting.clone(),
                hosting_id: v.hosting_id.clone(),
                is_jito: v.is_jito,
            },
            None => LeaderInfo {
                pubkey: pk_str,
                name: None,
                city: None,
                country: None,
                hosting: None,
                hosting_id: None,
                is_jito: false,
            },
        }
    }

    /// Fetch + parse the CSV at startup. Returns an empty map on any
    /// network / parse failure (logged at warn level) so central can
    /// still come up.
    pub async fn fetch(url: &str) -> Self {
        match try_fetch(url).await {
            Ok(map) => {
                tracing::info!("validators: loaded {} rows from {}", map.len(), url);
                map
            }
            Err(e) => {
                tracing::warn!(
                    "validators: fetch failed ({e:#}); starting with empty registry — leader info will be unknown"
                );
                Self::default()
            }
        }
    }
}

async fn try_fetch(url: &str) -> anyhow::Result<ValidatorMap> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let body = client.get(url).send().await?.error_for_status()?.text().await?;
    parse_csv(body.as_bytes())
}

fn parse_csv(bytes: &[u8]) -> anyhow::Result<ValidatorMap> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(bytes);

    // Resolve column indices by header name — the CSV may evolve.
    let headers = rdr.headers()?.clone();
    let idx = |name: &str| {
        headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(name))
            .ok_or_else(|| anyhow::anyhow!("csv missing column: {name}"))
    };
    let i_node = idx("NodePubkey")?;
    let i_name = idx("Name")?;
    let i_city = idx("City")?;
    let i_country = idx("Country")?;
    let i_hosting = idx("Hosting")?;
    let i_hosting_id = idx("HostingCompanyId")?;
    let i_is_jito = idx("IsJito")?;

    let mut by_node: HashMap<Pubkey, ValidatorInfo> = HashMap::new();
    for rec in rdr.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("validators: skipping bad row: {e}");
                continue;
            }
        };
        let pk_str = rec.get(i_node).unwrap_or("").trim();
        if pk_str.is_empty() {
            continue;
        }
        let pubkey: Pubkey = match pk_str.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let info = ValidatorInfo {
            name: non_empty(rec.get(i_name)),
            city: non_empty(rec.get(i_city)),
            country: non_empty(rec.get(i_country)),
            hosting: non_empty(rec.get(i_hosting)),
            hosting_id: non_empty(rec.get(i_hosting_id)),
            is_jito: rec
                .get(i_is_jito)
                .map(|s| matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
        };
        by_node.insert(pubkey, info);
    }
    Ok(ValidatorMap { by_node })
}

fn non_empty(s: Option<&str>) -> Option<String> {
    s.map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_csv() {
        let csv = b"NodePubkey,Name,City,Country,Hosting,HostingCompanyId,IsJito\n\
                    11111111111111111111111111111111,BlueLotus,Frankfurt am Main,DE,23M GmbH,AS47447,true\n";
        let map = parse_csv(csv).expect("parse");
        assert_eq!(map.len(), 1);
        let pk: Pubkey = "11111111111111111111111111111111".parse().unwrap();
        let info = map.leader_for(&pk);
        assert_eq!(info.name.as_deref(), Some("BlueLotus"));
        assert_eq!(info.country.as_deref(), Some("DE"));
        assert!(info.is_jito);
    }

    #[test]
    fn unknown_leader_returns_pubkey_only() {
        let map = ValidatorMap::default();
        let pk: Pubkey = "11111111111111111111111111111111".parse().unwrap();
        let info = map.leader_for(&pk);
        assert_eq!(info.pubkey, pk.to_string());
        assert!(info.name.is_none());
        assert!(!info.is_jito);
    }
}
