# central-service — copy-trading branch

**You are on branch `copy-trading`.** This service supports the copy-trading
bot repo (`supra-stop-take` on servers, `supra-stop-loss` locally), same branch. Deploy them together,
**central first** — the bot sends message variants an older central drops.

The bot's repo has the strategy documentation; this file covers central only.
Operational questions — `.env`, systemd, deploy order, troubleshooting — are
answered in ``DEPLOYMENT.md` in the bot repo (`supra-stop-take` on servers)`, which covers both repos.

## What central is for

One process, several jobs:

| job | file | notes |
|---|---|---|
| pool registry | `mongo.rs`, `pool/`, `discover.rs` | which accounts a pool has |
| ATA creation | `ata.rs` | bots cannot trade a pool without one |
| config | `fee_config.rs` | served to every bot, edited on the dashboard |
| WS hub | `ws.rs` | init + broadcasts, IP-whitelisted |
| stores | `orderflow.rs`, `copytrades.rs`, `alts.rs`, `bans.rs`, … | sled |

Bots hold no durable state worth keeping. Central does.

## Config is the important part

`fee_config.json` carries `copy_trading_v2`: the competitor wallet list, the
size/filter settings, the fee/tip variants, and the exit parameters. The
dashboard edits it at `/copy`; bots fetch it once at boot from
`GET /fee-config.json`.

`POST /fee-config.json` **validates before writing** (`CopyTradingV2::validate`)
— variants that do not sum to 100, a size outside (0,100], an unparseable
wallet are rejected rather than pushed to every location. Keep that invariant:
this file is read as gospel by every bot.

Saving broadcasts `ServerMsg::FeeConfigReload`, which makes bots exit for
systemd to restart them. There is no live re-read.

**`#[serde(default)]` only fills MISSING keys.** An existing `copy_trading_v2`
block keeps its stored values; new defaults in the code never reach it. To roll
out a new default, either set it on the page or delete the block and restart.

## sled + bincode — read this before touching a store

Every store serialises with **bincode, which is positional and not
self-describing**. Adding a field to a stored struct makes every existing row
undecodable. The symptom is nasty: the row count still sees the key while the
page shows nothing.

`copytrades.rs` and `orderflow.rs` therefore carry chains of legacy decoders
(`CopyTradeV2`, `CopyTradeV3`, `OrderflowRowV2`, …) and a `decode_*` helper
that tries newest-first. **If you add a field, add a fallback layer.**

## Never scan a tree on a polled endpoint

The dashboard polls `/orderflow.json` and `/copytrades.json` every few seconds.
`count()` used to be `db.iter().count()`, which in sled materialises every
*value* — at 64k rows that stalled the whole async runtime on a timer, freezing
the dashboard AND stopping new detections from being stored. Both stores now
keep an `AtomicUsize` maintained on insert and prune.

`orderflow::resolve_and_store` likewise shares ONE `RpcClient`. It used to
build one per detection, each with its own connection pool, held across a 12s
sleep — thousands of TLS handshakes a minute at live rates.

Rules of thumb here:

- `count()` and any per-poll aggregate must be O(1)
- long work goes in a spawned task, never inline in an axum handler
- `summary()` should walk the tree at most once, and must not call `count()`

## Retention quirks worth knowing

`orderflow.rs` **drops detections that never reached the chain**, except for
wallets in `KEEP_NOT_LANDED_DUMPERS`. With pre-block detection most detections
never land, so the visible rate is a fraction of the real one. Every row is
also delayed by `STATUS_CHECK_DELAY = 12s` before its status is resolved.

`MAX_ROWS = 1_000_000`, pruned oldest-first every `PRUNE_EVERY` inserts.

## Access control

Everything is IP-whitelisted via `WHITELIST_IPS`: the WS upgrade, and the
`GET /*.json` / `/*.bin` endpoints. **A bot whose IP is missing gets refused,
never receives `init`, and skips every opportunity with `pool_not_ready`** —
that is the first thing to check when a location trades nothing.

## .env

```bash
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=supra
RPC_URL=https://...                  # required
BLOCK_DETAIL_RPC_URL=https://...     # falls back to RPC_URL
WALLET_KEYPAIR=<base58>              # signs ATA creation, not trades
WHITELIST_IPS=<every bot ip>,<dashboard ip>
WS_BIND=0.0.0.0:9001
FEE_CONFIG_PATH=./fee_config.json
POSITIONS_LOG=./positions.jsonl
```

DB paths (`ORDERFLOW_DB_PATH`, `COPY_TRADES_DB_PATH`, …) all default to the
working directory and rarely need setting.

## Build

```bash
cargo build --release
sudo systemctl restart central-service
```

## Backups

The sled stores are **directories** — `tar` them, and stop the service first or
the snapshot can be torn. `orderflow.db` and `block_details.db` dominate the
size; `copytrades.db` and `fee_config.json` are the ones that matter for the
copy bot and are tiny.

## Do not

- commit `.env` or keypairs
- add a field to a sled-backed struct without a fallback decoder
- put a full tree scan on an endpoint the dashboard polls
- write config without validating it first
