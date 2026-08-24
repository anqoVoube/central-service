//! transfer — send SOL from the configured wallet to an address.
//!
//! Deliberately dry-run by default, like its siblings in this directory. A
//! transfer is irreversible and there is no keep-set to protect you, so the
//! amount and destination are printed and checked before anything is signed.
//!
//! Env: `WALLET_KEYPAIR` (the sender), `RPC_URL`.
//!
//! Flags:
//!   --amount=<SOL>     required, e.g. `--amount=0.51`
//!   --address=<pubkey> required, the recipient
//!   --apply            actually send; without it this is a dry run
//!   --priority=<µL/CU> priority fee, default 100_000
//!
//! Examples:
//!   cargo run --release --bin transfer -- --amount=0.51 --address=Abc…   # dry run
//!   cargo run --release --bin transfer -- --amount=0.51 --address=Abc… --apply
//!
//! Both `--flag=value` and `--flag value` are accepted, because muscle memory
//! from the other tools in this directory produces the second form.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
/// Rent-exempt minimum for a 0-data system account. An account left below this
/// can be reaped by the runtime, taking the remainder with it.
const RENT_EXEMPT_MIN: u64 = 890_880;
const CU_LIMIT: u32 = 450;
const DEFAULT_CU_PRICE: u64 = 100_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let apply = args.iter().any(|a| a == "--apply");

    // Accepts `--name=value` and `--name value`.
    let arg = |name: &str| -> Option<String> {
        let eq = format!("--{name}=");
        if let Some(v) = args.iter().find_map(|a| a.strip_prefix(eq.as_str())) {
            return Some(v.to_owned());
        }
        let flag = format!("--{name}");
        args.iter()
            .position(|a| *a == flag)
            .and_then(|i| args.get(i + 1))
            .filter(|v| !v.starts_with("--"))
            .cloned()
    };

    let Some(amount_str) = arg("amount") else {
        bail!("--amount=<SOL> is required, e.g. --amount=0.51");
    };
    let Some(address_str) = arg("address") else {
        bail!("--address=<pubkey> is required");
    };

    let amount_sol: f64 = amount_str
        .parse()
        .with_context(|| format!("--amount={amount_str} is not a number"))?;
    if !(amount_sol.is_finite() && amount_sol > 0.0) {
        bail!("--amount must be a positive number, got {amount_sol}");
    }
    // Round rather than truncate: 0.51 is not exactly representable, and
    // truncating would silently send one lamport less than asked.
    let lamports = (amount_sol * LAMPORTS_PER_SOL).round() as u64;
    if lamports == 0 {
        bail!("--amount={amount_sol} rounds to 0 lamports");
    }

    let to = Pubkey::from_str(address_str.trim())
        .with_context(|| format!("--address={address_str} is not a valid pubkey"))?;

    let cu_price: u64 = arg("priority")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CU_PRICE);

    let wallet_keypair_b58 = std::env::var("WALLET_KEYPAIR").context("WALLET_KEYPAIR not set")?;
    let rpc_url = std::env::var("RPC_URL").context("RPC_URL not set")?;
    let kp = Keypair::from_base58_string(&wallet_keypair_b58);
    let from = kp.pubkey();

    if to == from {
        bail!("refusing to transfer to the sending wallet ({from})");
    }

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    let balance = rpc
        .get_balance(&from)
        .await
        .context("get_balance(sender)")?;
    // Priority fee plus the 5000-lamport base fee. Not exact — the base fee
    // scales with signature count, and there is one here — but it only has to
    // be right enough to catch a transfer that cannot pay for itself.
    let fee_estimate = 5_000 + ((CU_LIMIT as u128 * cu_price as u128) / 1_000_000) as u64;
    let after = balance.saturating_sub(lamports).saturating_sub(fee_estimate);

    println!("from      {from}");
    println!("to        {to}");
    println!(
        "amount    {:.9} SOL  ({lamports} lamports)",
        lamports as f64 / LAMPORTS_PER_SOL
    );
    println!(
        "balance   {:.9} SOL  ->  {:.9} SOL after (fee ~{fee_estimate} lamports)",
        balance as f64 / LAMPORTS_PER_SOL,
        after as f64 / LAMPORTS_PER_SOL
    );

    if lamports + fee_estimate > balance {
        bail!(
            "insufficient balance: need {} lamports (amount + fee), have {balance}",
            lamports + fee_estimate
        );
    }
    if after < RENT_EXEMPT_MIN {
        // Not fatal — draining a wallet on purpose is legitimate — but it must
        // be a deliberate choice rather than a surprise.
        println!(
            "WARNING   this leaves {after} lamports, below the {RENT_EXEMPT_MIN} rent-exempt \
             minimum; the account can be reaped and the remainder lost"
        );
    }

    if !apply {
        println!("\nDRY RUN — nothing sent. Re-run with --apply to execute.");
        return Ok(());
    }

    let ixs = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        system_instruction::transfer(&from, &to, lamports),
    ];
    let bh = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&from), &[&kp], bh);
    let sig = tx.signatures[0];

    // Preflight ON, unlike the batch tools: this is a single deliberate
    // transfer, so a simulation failure should stop it rather than be
    // discovered afterwards.
    //
    // `preflight_commitment` MUST match the commitment the blockhash came
    // from. It defaults to `finalized`, which lags confirmed by ~30 slots and
    // therefore has never heard of a blockhash we just fetched at confirmed —
    // preflight then fails with "Blockhash not found" on a perfectly valid
    // transaction.
    rpc.send_transaction_with_config(
        &tx,
        RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentLevel::Confirmed),
            ..Default::default()
        },
    )
    .await
    .context("send_transaction")?;
    println!("\nsent      {sig}");
    println!("solscan   https://solscan.io/tx/{sig}");

    // Confirm rather than fire-and-forget — the operator wants to know whether
    // the money moved, and there is only one transaction to wait on.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(statuses) = rpc.get_signature_statuses(&[sig]).await {
            if let Some(Some(st)) = statuses.value.first() {
                return match &st.err {
                    None => {
                        println!("status    CONFIRMED");
                        Ok(())
                    }
                    Some(e) => bail!("transaction reverted: {e:?}"),
                };
            }
        }
        if tokio::time::Instant::now() >= deadline {
            println!("status    UNKNOWN — not confirmed within 60s; check the signature above");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}
