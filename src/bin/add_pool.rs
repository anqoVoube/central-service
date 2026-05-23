//! One-shot manual pool insertion. Mongo-only — no on-chain ATA creation,
//! no CU measurement. Use this when you want a pool tracked by the bot but
//! the regular discovery pipeline hasn't (or won't) pick it up.
//!
//! What it does:
//!   1. Fetch the PumpFun pool account from RPC + parse it.
//!   2. Look up the base mint to read its decimals + token program.
//!   3. Best-effort Dexscreener fetch for token name / symbol / pair age.
//!   4. Insert a `PoolDoc` into Mongo (idempotent — duplicates rejected by
//!      the unique `pool` index).
//!   5. Default `is_unique = true` so the WS init filter ships this pool
//!      regardless of `pair_created_at_ms`. Override with `--unique=false`.
//!
//! What it does NOT do:
//!   - Create the wallet's base-mint ATA. Run `create_missing_atas` after,
//!     or wait for the next regular discovery pass.
//!   - Measure CU. Bot falls back to its static `CU_LIMIT_PUMP_FUN` until
//!     `measure_cu` (batch) or the auto-measure flow runs.
//!
//! Usage:
//!   cd ~/Work/central-service-seed && \
//!     ~/Work/central-service/target/release/add_pool <pool_pubkey>
//!   # Override is_unique:
//!   ./add_pool <pool_pubkey> --unique=false
//!
//! Requires `.env` (or env) with MONGO_URI, MONGO_DB. Optionally `RPC_URL`
//! to override Helius default.

use std::str::FromStr;

use anyhow::{anyhow, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

use central_service::pool::{pump_fun, AtaStatus, PoolAccounts, PoolDoc, PumpFunAccounts};

const DEFAULT_RPC: &str =
    "https://api.mainnet-beta.solana.com";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let pool_str = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .cloned()
        .context("usage: add_pool <pool_pubkey> [--unique=true|false]")?;
    let is_unique = parse_unique_flag(&args)?;

    let pool_pk = Pubkey::from_str(&pool_str).context("invalid pool pubkey")?;

    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC.to_owned());

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // Bail early if the pool already exists — keeps the output noise low
    // and avoids touching Mongo / Dexscreener when nothing would change.
    let client = mongodb::Client::with_uri_str(&mongo_uri)
        .await
        .context("mongo connect")?;
    let pools_coll = client
        .database(&mongo_db)
        .collection::<PoolDoc>("pools");
    if let Some(existing) = pools_coll
        .find_one(mongodb::bson::doc! { "pool": &pool_str })
        .await?
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let age = existing.pair_created_at_ms.map(|c| {
            let ms = now_ms.saturating_sub(c);
            format!("{} days", ms / (24 * 60 * 60 * 1_000))
        });
        println!("[add_pool] {pool_pk} already exists in Mongo — no insert performed");
        println!("  is_unique:           {}", existing.is_unique);
        println!("  ata_status:          {:?}", existing.ata_status);
        println!("  token_symbol:        {:?}", existing.token_symbol);
        println!("  pair_created_at_ms:  {:?}  ({})", existing.pair_created_at_ms, age.as_deref().unwrap_or("unknown"));
        println!("  compute_unit_limit:  {:?}", existing.compute_unit_limit);
        println!(
            "[add_pool] to flip is_unique=true on an existing pool, run: \
             `mark_unique {pool_pk}`"
        );
        return Ok(());
    }

    println!("[add_pool] fetching pool account from RPC…");
    let pool_acc = rpc
        .get_account(&pool_pk)
        .await
        .with_context(|| format!("getAccountInfo({pool_pk})"))?;
    let parsed = pump_fun::parse_pool(&pool_acc.data)
        .with_context(|| format!("parse_pool({pool_pk}) — is this really a PumpFun pAMM pool?"))?;
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
        .ok_or_else(|| anyhow!("mint {base_mint} data too short for decimals field"))?;

    println!("[add_pool] looking up token meta on Dexscreener (3s timeout)…");
    let (token_name, token_symbol, pair_created_at_ms) =
        fetch_pair_meta(&base_mint, &pool_pk).await;

    println!("[add_pool] resolved:");
    println!("  base_mint:           {base_mint}");
    println!("  quote_mint:          {}", parsed.quote_mint);
    println!("  pool_base_vault:     {}", parsed.pool_base_token_account);
    println!("  pool_quote_vault:    {}", parsed.pool_quote_token_account);
    println!("  coin_creator:        {}", parsed.coin_creator);
    println!("  base_token_program:  {token_program}");
    println!("  is_cashback:         {is_cashback}");
    println!("  token_decimals:      {token_decimals}");
    println!("  token_name:          {token_name:?}");
    println!("  token_symbol:        {token_symbol:?}");
    println!("  pair_created_at_ms:  {pair_created_at_ms:?}");
    println!("  is_unique:           {is_unique}");

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
        // Same default as discover.rs — bots get visibility immediately;
        // ATA creation is a separate manual step here (see module doc).
        ata_status: AtaStatus::Confirmed,
        ata_attempts: 0,
        token_name,
        token_symbol,
        pair_created_at_ms,
        compute_unit_limit: None,
        cu_measured_at: None,
        is_unique,
    };

    pools_coll
        .insert_one(&doc)
        .await
        .context("insert_one")?;
    println!("[add_pool] ✓ inserted {pool_pk}");
    println!(
        "[add_pool] next: run create_missing_atas to fund + create the wallet's base-mint ATA, \
         then measure_cu --force <pool> for a per-pool CU value (or wait for the next batch sweep)."
    );

    Ok(())
}

fn parse_unique_flag(args: &[String]) -> anyhow::Result<bool> {
    for a in args.iter().skip(1) {
        if let Some(v) = a.strip_prefix("--unique=") {
            return match v {
                "true" | "1" | "yes" => Ok(true),
                "false" | "0" | "no" => Ok(false),
                other => Err(anyhow!(
                    "--unique=<bool>: got {other:?}, expected true/false"
                )),
            };
        }
    }
    // Default: true — manually-added pools usually want the age filter
    // bypassed (otherwise why bother adding them manually?). Pass
    // `--unique=false` to fall back to the standard 7-day window.
    Ok(true)
}

/// Best-effort Dexscreener lookup — mirrors `central_service::discover::fetch_pair_meta`.
/// Kept inline here (rather than re-exporting) so this binary can be run
/// standalone without coupling to the discovery internals.
async fn fetch_pair_meta(
    mint: &Pubkey,
    pool: &Pubkey,
) -> (Option<String>, Option<String>, Option<i64>) {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{mint}");
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return (None, None, None),
    };
    let resp = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            eprintln!("[dexscreener] {url} → {}", r.status());
            return (None, None, None);
        }
        Err(e) => {
            eprintln!("[dexscreener] {url} failed: {e}");
            return (None, None, None);
        }
    };
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[dexscreener] {mint} parse failed: {e}");
            return (None, None, None);
        }
    };
    let mint_s = mint.to_string();
    let pool_s = pool.to_string();
    let pairs = match v.get("pairs").and_then(|x| x.as_array()) {
        Some(p) => p,
        None => return (None, None, None),
    };

    let pick_name_symbol = |p: &serde_json::Value| -> (Option<String>, Option<String>) {
        for side in ["baseToken", "quoteToken"] {
            let Some(t) = p.get(side) else { continue };
            let addr = t.get("address").and_then(|x| x.as_str()).unwrap_or("");
            if addr == mint_s {
                let name = t.get("name").and_then(|x| x.as_str()).map(str::to_owned);
                let symbol = t.get("symbol").and_then(|x| x.as_str()).map(str::to_owned);
                return (name, symbol);
            }
        }
        (None, None)
    };

    // Pass 1: exact pool match → reliable pair age.
    for p in pairs {
        let addr = p.get("pairAddress").and_then(|x| x.as_str()).unwrap_or("");
        if addr == pool_s {
            let (name, symbol) = pick_name_symbol(p);
            let created = p.get("pairCreatedAt").and_then(|x| x.as_i64());
            return (name, symbol, created);
        }
    }
    // Pass 2: no exact pool — keep name/symbol from the first mint match.
    for p in pairs {
        let (name, symbol) = pick_name_symbol(p);
        if name.is_some() || symbol.is_some() {
            return (name, symbol, None);
        }
    }
    (None, None, None)
}
