# ExchangeCrawl

Local-first, read-only trading ledger archives built on
[OpenClaw CrawlKit](https://github.com/openclaw/crawlkit).

This repository builds two independent applications:

- `bybitcrawl` archives Bybit Unified Account transaction logs plus spot,
  derivatives, wallet, earn, and P2P history.
- `binancecrawl` archives Binance USDⓈ-M Futures income history plus spot,
  derivatives, wallet, Simple Earn, Auto-Invest, and C2C history.

Each application keeps its own config and SQLite archive. Exchange credentials,
signing, pagination, and response mapping stay in the Exchange adapter; archive
storage, checkpoints, local queries, and control metadata are shared.

## Coverage

Beyond the base account ledger, each binary archives independently
checkpointed streams. Immutable history is stored as append-only ledger
entries; mutable objects (open orders, positions, running plans) are stored as
append-only state observations queryable via `events`.

`bybitcrawl`:

- `ledger` — Unified Account transaction log
- `spot/fills`, `spot/orders` — spot executions and order state
- `futures/fills`, `futures/closed-pnl` — derivatives executions and realized PnL
- `futures/orders`, `futures/positions` — derivatives order and position state
- `asset/deposits`, `asset/withdrawals` — on-chain and internal wallet movements
- `earn/orders`, `earn/yield` — earn subscriptions/redemptions and yield history
- `p2p/orders` — P2P order history and state

`binancecrawl`:

- `ledger` — USDⓈ-M Futures income history
- `spot/fills`, `spot/orders` — spot executions and order state
- `futures/fills`, `futures/orders`, `futures/positions` — derivatives fills,
  order state, and position state
- `asset/deposits`, `asset/withdrawals` — wallet deposit and withdrawal history
- `earn/orders`, `earn/rewards` — Simple Earn subscriptions/redemptions and rewards
- `autoinvest/history`, `autoinvest/plans` — Auto-Invest executions and plan state
- `p2p/orders` — C2C order history and state

Every stream calls only official read/history endpoints.

## Development

Requirements: Go 1.26+.

```bash
go test ./...
go build -o bin/bybitcrawl ./cmd/bybitcrawl
go build -o bin/binancecrawl ./cmd/binancecrawl
```

## Quick start

Bybit HMAC:

```bash
export BYBIT_API_KEY="..."
export BYBIT_API_SECRET="..."
go run ./cmd/bybitcrawl init
go run ./cmd/bybitcrawl doctor --json
go run ./cmd/bybitcrawl sync
go run ./cmd/bybitcrawl entries --limit 20
```

Bybit RSA uses `BYBIT_API_PRIVATE_KEY_PATH` instead of
`BYBIT_API_SECRET`. Set `BYBIT_API_BASE_URL=https://api.bybit.id` when the
regional Indonesia endpoint is required. CLI configuration accepts only
official Exchange HTTPS origins so signed requests cannot be redirected to a
credential-collection host.

Binance:

```bash
export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
go run ./cmd/binancecrawl init
go run ./cmd/binancecrawl doctor --json
go run ./cmd/binancecrawl sync
go run ./cmd/binancecrawl search BTCUSDT --json
```

`sync --since` accepts an RFC3339 timestamp or `YYYY-MM-DD`. Without an
explicit start, the first sync reads seven days and later syncs overlap the
last successful checkpoint by 24 hours.

`entries` and `search` accept `--stream` for local stream-scoped queries.
`events` reads append-only mutable-object state transitions from the local
archive and supports account, stream, object, status, and time filters.

## Local-first and read-only

- ExchangeCrawl calls only official read endpoints.
- `entries`, `events`, `search`, and `status` use only local SQLite data.
- Config stores environment-variable names, never secret values.
- API keys, secrets, signed URLs, and private keys are never stored in the
  archive or emitted in command output.
- No archive publishing or remote sharing is enabled in V0.

## crawlctl scheduling

After both binaries are installed on `PATH`, create two independent refresh
jobs from their control manifests:

```bash
crawlctl init --app bybitcrawl --app binancecrawl
crawlctl install
crawlctl status
```

`bybitcrawl metadata --json` and `binancecrawl metadata --json` expose separate
app identities, database paths, and mutating `sync` commands to the scheduler.

See [SPEC.md](SPEC.md) for the behavioral contract and [CONTEXT.md](CONTEXT.md)
for domain language.
