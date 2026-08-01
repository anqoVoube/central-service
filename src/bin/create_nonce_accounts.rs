//! Create the bot's two durable-nonce accounts (buy + sell) in one tx and
//! print their pubkeys.
//!
//! The bot signs every swap with `WALLET_KEYPAIR` and uses a durable nonce as
//! the blockhash so all locations can fan out the same nonce and have exactly
//! one land. Two separate nonces so a buy and a sell on different pools can't
//! invalidate each other.
//!
//! Both accounts are created and initialized in a SINGLE transaction (4 ixs:
//! CreateAccount + InitializeNonceAccount, twice) so you either get both or
//! neither.
//!
//! AUTHORITY = the `WALLET_FOR_NONCE` wallet, which must be the SAME wallet
//! the bot signs with — `advance_nonce_account` is authorized by that key, so
//! a mismatch makes every swap fail. The generated nonce-account keypairs are
//! throwaway: they only sign this creation tx and are never needed again
//! (advance / withdraw / authorize are all authority-signed), which is why
//! only the pubkeys are printed.
//!
//! Env (.env in this directory):
//!   WALLET_FOR_NONCE  base58 secret key — pays rent AND becomes the nonce
//!                     authority. Use the bot's wallet.
//!   RPC_URL           optional; defaults to the Helius endpoint below.
//!
//! Run:
//!   cd ~/Work/central-service && cargo run --release --bin create_nonce_accounts
//!   # add --dry-run to print the plan + cost without sending

use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    nonce::State as NonceState,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};

const DEFAULT_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=e57668cb-43f4-4d35-9d83-fbb9c1d71ad2";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let dry_run = std::env::args().any(|a| a == "--dry-run");

    let secret = std::env::var("WALLET_FOR_NONCE")
        .context("WALLET_FOR_NONCE not set (base58 secret key of the bot's wallet)")?;
    let payer = Keypair::from_base58_string(secret.trim());
    let payer_pk = payer.pubkey();
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC.to_owned());

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // Fresh keypairs for the two nonce accounts. Only used to sign the
    // CreateAccount below; the authority governs them afterwards.
    let buy_nonce = Keypair::new();
    let sell_nonce = Keypair::new();

    let rent = rpc
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .await
        .context("get_minimum_balance_for_rent_exemption")?;
    let balance = rpc
        .get_balance(&payer_pk)
        .await
        .context("get_balance(payer)")?;

    println!("[nonce] payer/authority = {payer_pk}");
    println!(
        "[nonce] balance = {:.6} SOL | rent per nonce = {:.6} SOL | need ≈ {:.6} SOL (+fee)",
        balance as f64 / LAMPORTS_PER_SOL,
        rent as f64 / LAMPORTS_PER_SOL,
        (rent * 2) as f64 / LAMPORTS_PER_SOL,
    );
    println!("[nonce] would create buy={} sell={}", buy_nonce.pubkey(), sell_nonce.pubkey());

    if dry_run {
        println!("\nDRY RUN — nothing sent. Re-run without --dry-run to create.");
        return Ok(());
    }
    if balance < rent * 2 {
        anyhow::bail!(
            "payer has {:.6} SOL, needs ≥ {:.6} SOL for two rent-exempt nonce accounts",
            balance as f64 / LAMPORTS_PER_SOL,
            (rent * 2) as f64 / LAMPORTS_PER_SOL,
        );
    }

    // `create_nonce_account` returns [CreateAccount, InitializeNonceAccount].
    // Both pairs go in one tx → atomic.
    let mut ixs = system_instruction::create_nonce_account(
        &payer_pk,
        &buy_nonce.pubkey(),
        &payer_pk, // authority = the bot's wallet
        rent,
    );
    ixs.extend(system_instruction::create_nonce_account(
        &payer_pk,
        &sell_nonce.pubkey(),
        &payer_pk,
        rent,
    ));

    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;
    let mut tx = Transaction::new_with_payer(&ixs, Some(&payer_pk));
    // The new accounts must sign their own creation.
    tx.sign(&[&payer, &buy_nonce, &sell_nonce], blockhash);

    let sig = rpc
        .send_and_confirm_transaction(&tx)
        .await
        .context("send_and_confirm (create nonce accounts)")?;

    println!("\n[nonce] created ✅ sig={sig}");
    println!("\n─── add these to every bot's .env ───");
    println!("BUY_NONCE_PUBKEY={}", buy_nonce.pubkey());
    println!("SELL_NONCE_PUBKEY={}", sell_nonce.pubkey());
    println!(
        "\n[nonce] authority is {payer_pk} — the bot's WALLET_KEYPAIR MUST be this \
         same wallet or advance_nonce will fail on every swap."
    );

    Ok(())
}
