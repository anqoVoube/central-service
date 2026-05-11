use anyhow::Context;
use std::{collections::HashSet, net::{IpAddr, SocketAddr}, path::PathBuf};

pub struct Config {
    pub mongo_uri: String,
    pub mongo_db: String,
    pub rpc_url: String,
    pub ws_bind: SocketAddr,
    pub whitelist_ips: HashSet<IpAddr>,
    pub poll_interval_secs: u64,
    pub wallet_keypair_b58: String,
    pub positions_log: PathBuf,
    pub alts_db_path: PathBuf,
    pub bans_db_path: PathBuf,
    pub lanes_db_path: PathBuf,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
        let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
        let rpc_url = std::env::var("RPC_URL").context("RPC_URL not set")?;
        let ws_bind = std::env::var("WS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:9001".into())
            .parse()
            .context("WS_BIND must be a socket address like 0.0.0.0:9001")?;
        let whitelist_ips = std::env::var("WHITELIST_IPS")
            .context("WHITELIST_IPS not set")?
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<IpAddr>())
            .collect::<Result<HashSet<_>, _>>()
            .context("WHITELIST_IPS contains an invalid IP")?;
        let poll_interval_secs = std::env::var("POLL_INTERVAL_SECS")
            .ok()
            .map(|s| s.parse::<u64>())
            .transpose()
            .context("POLL_INTERVAL_SECS must be a positive integer")?
            .unwrap_or(3600);
        let wallet_keypair_b58 =
            std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
        let positions_log = std::env::var("POSITIONS_LOG")
            .unwrap_or_else(|_| "positions.jsonl".into())
            .into();
        let alts_db_path = std::env::var("ALTS_DB_PATH")
            .unwrap_or_else(|_| "alts.db".into())
            .into();
        let bans_db_path = std::env::var("BANS_DB_PATH")
            .unwrap_or_else(|_| "bans.db".into())
            .into();
        let lanes_db_path = std::env::var("LANES_DB_PATH")
            .unwrap_or_else(|_| "lanes.db".into())
            .into();
        Ok(Self {
            mongo_uri,
            mongo_db,
            rpc_url,
            ws_bind,
            whitelist_ips,
            poll_interval_secs,
            wallet_keypair_b58,
            positions_log,
            alts_db_path,
            bans_db_path,
            lanes_db_path,
        })
    }
}
