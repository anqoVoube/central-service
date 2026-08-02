//! Create ONE durable-nonce account and print its pubkey.
//!
//! The bot signs every swap with `WALLET_KEYPAIR` and uses a durable nonce as
//! the blockhash, so all locations can fan out against the same nonce and have
//! exactly one transaction land.
//!
//! A bot needs two nonces — one for buys, one for sells — so a buy and a sell
//! on different pools can't invalidate each other. This tool creates a SINGLE
//! account per run: run it twice (once per role), or once to replace just one
//! of an existing pair. One at a time also means a failed run costs at most one
//! account's rent instead of leaving a half-created pair.
//!
//! AUTHORITY = the `WALLET_FOR_NONCE` wallet, which must be the SAME wallet the
//! bot signs with — `advance_nonce_account` is authorized by that key, so a
//! mismatch makes every swap fail. The generated nonce-account keypair is
//! throwaway: it only signs this creation tx and is never needed again (advance
//! / withdraw / authorize are all authority-signed), which is why only the
//! pubkey is printed.
//!
//! Env (.env in this directory):
//!   WALLET_FOR_NONCE  base58 secret key — pays rent AND becomes the nonce
//!                     authority. Use the bot's wallet.
//!   RPC_URL           optional; defaults to the Helius endpoint below.
//!
//! Run:
//!   cd ~/Work/central-service && cargo run --release --bin create_nonce_accounts
//!   #   --buy | --sell   label the printed env line (default: prints the raw pubkey)
//!   #   --dry-run        print the plan + cost without sending

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

/// Which env var the created account is destined for. Affects the printed line
/// only — a nonce account is not buy- or sell-specific on chain.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Buy,
    Sell,
    Unspecified,
}

impl Role {
    fn label(self) -> &'static str {
        match self {
            Role::Buy => "buy",
            Role::Sell => "sell",
            Role::Unspecified => "unassigned",
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run");
    let role = match (
        args.iter().any(|a| a == "--buy"),
        args.iter().any(|a| a == "--sell"),
    ) {
        (true, true) => anyhow::bail!("pass at most one of --buy / --sell"),
        (true, false) => Role::Buy,
        (false, true) => Role::Sell,
        (false, false) => Role::Unspecified,
    };

    let secret = std::env::var("WALLET_FOR_NONCE")
        .context("WALLET_FOR_NONCE not set (base58 secret key of the bot's wallet)")?;
    let payer = Keypair::from_base58_string(secret.trim());
    let payer_pk = payer.pubkey();
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC.to_owned());

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // Fresh keypair for the nonce account. Only used to sign the CreateAccount
    // below; the authority governs it afterwards.
    let nonce = Keypair::new();

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
        "[nonce] balance = {:.6} SOL | rent = {:.6} SOL (+fee)",
        balance as f64 / LAMPORTS_PER_SOL,
        rent as f64 / LAMPORTS_PER_SOL,
    );
    println!(
        "[nonce] would create 1 account ({}) = {}",
        role.label(),
        nonce.pubkey()
    );

    if dry_run {
        println!("\nDRY RUN — nothing sent. Re-run without --dry-run to create.");
        return Ok(());
    }
    if balance < rent {
        anyhow::bail!(
            "payer has {:.6} SOL, needs ≥ {:.6} SOL for a rent-exempt nonce account",
            balance as f64 / LAMPORTS_PER_SOL,
            rent as f64 / LAMPORTS_PER_SOL,
        );
    }

    // `create_nonce_account` returns [CreateAccount, InitializeNonceAccount].
    let ixs = system_instruction::create_nonce_account(
        &payer_pk,
        &nonce.pubkey(),
        &payer_pk, // authority = the bot's wallet
        rent,
    );

    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash")?;
    let mut tx = Transaction::new_with_payer(&ixs, Some(&payer_pk));
    // The new account must sign its own creation.
    tx.sign(&[&payer, &nonce], blockhash);

    let sig = rpc
        .send_and_confirm_transaction(&tx)
        .await
        .context("send_and_confirm (create nonce account)")?;

    println!("\n[nonce] created ✅ sig={sig}");
    println!("\n─── add this to every bot's .env ───");
    match role {
        Role::Buy => println!("BUY_NONCE_PUBKEY={}", nonce.pubkey()),
        Role::Sell => println!("SELL_NONCE_PUBKEY={}", nonce.pubkey()),
        Role::Unspecified => {
            println!("{}", nonce.pubkey());
            println!(
                "\n[nonce] no role given — assign it as BUY_NONCE_PUBKEY or \
                 SELL_NONCE_PUBKEY (a nonce account is not buy/sell specific on \
                 chain). Re-run with --buy or --sell to print the exact line."
            );
        }
    }
    println!(
        "\n[nonce] authority is {payer_pk} — the bot's WALLET_KEYPAIR MUST be this \
         same wallet or advance_nonce will fail on every swap."
    );

    Ok(())
}
