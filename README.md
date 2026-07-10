# ExchangeCrawl

Local-first, read-only trading ledger archives built on
[OpenClaw CrawlKit](https://github.com/openclaw/crawlkit).

This repository builds two independent applications:

- `bybitcrawl` archives Bybit Unified Account transaction logs.
- `binancecrawl` archives Binance USDⓈ-M Futures income history.

Each application keeps its own config and SQLite archive. Exchange credentials,
signing, pagination, and response mapping stay in the Exchange adapter; archive
storage, checkpoints, local queries, and control metadata are shared.

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

## Local-first and read-only

- ExchangeCrawl calls only official read endpoints.
- `entries`, `search`, and `status` use only local SQLite data.
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
