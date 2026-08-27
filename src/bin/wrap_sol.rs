//! wrap_sol — wrap SOL into a wallet's WSOL account.
//!
//! Dry-run by default, like its siblings here.
//!
//! Exists because `copy_simple`'s buy path passes the WSOL ATA as an account
//! but never creates it and never wraps — it spends a pre-funded balance. On a
//! wallet that has never held WSOL, the first buy therefore fails on a missing
//! account, on chain, after the fee is paid, with preflight skipped. The
//! dashboard has a page for this, but it is behind a login, so a wallet that
//! nobody can log in for had no way to be prepared.
//!
//! Env (.env in this directory):
//!   WRAP_WALLET  base58 secret key of the wallet to wrap for.
//!   RPC_URL      optional; defaults to public mainnet-beta.
//!
//! Flags:
//!   --amount=<SOL>  required
//!   --apply         actually send; without it this is a dry run
//!
//! Unwrapping is `close_all_atas` or the dashboard page — this only wraps,
//! because that is the direction that blocks a deploy.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
/// Rent-exempt minimum for a 165-byte token account.
const TOKEN_ACCOUNT_RENT: u64 = 2_039_280;
/// Left unwrapped for fees and tips. Wrapping everything strands the wallet:
/// WSOL cannot pay a transaction fee.
const KEEP_SOL_MIN: u64 = 20_000_000;

fn find_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &Pubkey::from_str(ATA_PROGRAM).unwrap(),
    )
    .0
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let apply = args.iter().any(|a| a == "--apply");
    let amount_str = args
        .iter()
        .find_map(|a| a.strip_prefix("--amount="))
        .context("--amount=<SOL> is required")?;

    let amount_sol: f64 = amount_str.parse().context("--amount is not a number")?;
    if !(amount_sol.is_finite() && amount_sol > 0.0) {
        bail!("--amount must be positive, got {amount_sol}");
    }
    let lamports = (amount_sol * LAMPORTS_PER_SOL).round() as u64;

    let secret = std::env::var("WRAP_WALLET").context("WRAP_WALLET not set")?;
    let kp = Keypair::from_base58_string(secret.trim());
    let owner = kp.pubkey();
    let rpc_url =
        std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    let mint = Pubkey::from_str(WSOL_MINT)?;
    let token_prog = Pubkey::from_str(TOKEN_PROGRAM)?;
    let ata = find_ata(&owner, &mint, &token_prog);

    let balance = rpc.get_balance(&owner).await.context("get_balance")?;
    let exists = rpc.get_account(&ata).await.is_ok();
    // The ATA's rent is paid on top of the wrapped amount when it does not yet
    // exist, and is NOT part of the WSOL balance.
    let rent = if exists { 0 } else { TOKEN_ACCOUNT_RENT };
    let fee_est = 10_000;
    let after = balance
        .saturating_sub(lamports)
        .saturating_sub(rent)
        .saturating_sub(fee_est);

    println!("owner     {owner}");
    println!("wsol ata  {ata}  ({})", if exists { "exists" } else { "will be created" });
    println!("balance   {:.9} SOL", balance as f64 / LAMPORTS_PER_SOL);
    println!("wrapping  {:.9} SOL  ({lamports} lamports)", lamports as f64 / LAMPORTS_PER_SOL);
    if rent > 0 {
        println!("ata rent  {:.9} SOL (on top, not part of the WSOL balance)", rent as f64 / LAMPORTS_PER_SOL);
    }
    println!("left      {:.9} SOL for fees and tips", after as f64 / LAMPORTS_PER_SOL);

    if lamports + rent + fee_est > balance {
        bail!("insufficient balance: need {} lamports, have {balance}", lamports + rent + fee_est);
    }
    if after < KEEP_SOL_MIN {
        // WSOL cannot pay a transaction fee, so a wallet wrapped to the bone
        // holds funds it has no way to spend.
        bail!(
            "this leaves {:.9} SOL — under the {:.3} SOL needed for fees and tips. \
             WSOL cannot pay a fee, so the wallet would be stuck. Wrap less.",
            after as f64 / LAMPORTS_PER_SOL,
            KEEP_SOL_MIN as f64 / LAMPORTS_PER_SOL
        );
    }

    if !apply {
        println!("\nDRY RUN — nothing sent. Re-run with --apply.");
        return Ok(());
    }

    let sys = Pubkey::from_str(SYSTEM_PROGRAM)?;
    let mut transfer_data = Vec::with_capacity(12);
    transfer_data.extend_from_slice(&2u32.to_le_bytes());
    transfer_data.extend_from_slice(&lamports.to_le_bytes());

    let ixs = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(30_000),
        ComputeBudgetInstruction::set_compute_unit_price(100_000),
        // 1 = CreateIdempotent: succeeds whether or not the account is there.
        Instruction {
            program_id: Pubkey::from_str(ATA_PROGRAM)?,
            accounts: vec![
                AccountMeta::new(owner, true),
                AccountMeta::new(ata, false),
                AccountMeta::new_readonly(owner, false),
                AccountMeta::new_readonly(mint, false),
                AccountMeta::new_readonly(sys, false),
                AccountMeta::new_readonly(token_prog, false),
            ],
            data: vec![1u8],
        },
        Instruction {
            program_id: sys,
            accounts: vec![AccountMeta::new(owner, true), AccountMeta::new(ata, false)],
            data: transfer_data,
        },
        // 17 = SyncNative. Without it the lamports sit in the account but the
        // token balance still reads zero.
        Instruction {
            program_id: token_prog,
            accounts: vec![AccountMeta::new(ata, false)],
            data: vec![17u8],
        },
    ];

    let bh = rpc.get_latest_blockhash().await.context("blockhash")?;
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&owner), &[&kp], bh);
    let sig = tx.signatures[0];
    // Preflight ON: one deliberate transaction, so a simulation failure should
    // stop it rather than be discovered afterwards. The commitment must match
    // the blockhash's or preflight rejects a perfectly valid transaction.
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(st) = rpc.get_signature_statuses(&[sig]).await {
            if let Some(Some(s)) = st.value.first() {
                return match &s.err {
                    None => {
                        println!("status    CONFIRMED");
                        Ok(())
                    }
                    Some(e) => bail!("reverted: {e:?}"),
                };
            }
        }
        if tokio::time::Instant::now() >= deadline {
            println!("status    UNKNOWN after 60s — check the signature above");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}
