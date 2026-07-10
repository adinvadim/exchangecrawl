# ExchangeCrawl V0 Specification

## Outcome

Build two local-first, read-only Exchange archive CLIs on top of
`github.com/openclaw/crawlkit`:

- `bybitcrawl`
- `binancecrawl`

Both binaries live in this repository and share one deep archive module. Each
binary keeps an independent config, SQLite database, credentials, app identity,
and `crawlctl` job.

## V0 scope

Archive the account ledger through official read-only endpoints:

- Bybit V5 `GET /v5/account/transaction-log`
- Binance USDⓈ-M Futures `GET /fapi/v1/income`

Deferred: orders, fills, balances, positions, TUI, snapshot sharing, remote
archives, and Exchange write actions.

## CLI interface

Both binaries expose the same commands:

```text
<app> init
<app> doctor [--json]
<app> sync [--account ID] [--since TIME] [--until TIME] [--json]
<app> entries [--account ID] [--symbol SYMBOL] [--type TYPE] [--since TIME] [--until TIME] [--limit N] [--json]
<app> search QUERY [--account ID] [--limit N] [--json]
<app> status [--json]
<app> metadata --json
<app> version
```

`TIME` accepts RFC3339 or `YYYY-MM-DD`. `entries` and `search` are strictly
local: they never authenticate or access the network. JSON decimals remain
strings and timestamps are RFC3339 UTC.

## Configuration and credentials

Use crawlkit platform paths. `init` writes a TOML file with no secret values.
An absent config behaves like one `primary` account with standard environment
variable names.

Each account has:

- stable lowercase `id`
- human `label`
- API key environment-variable name
- API secret environment-variable name
- Bybit only: optional RSA private-key-path environment-variable name

Default variables:

- Bybit: `BYBIT_API_KEY`, `BYBIT_API_SECRET`,
  `BYBIT_API_PRIVATE_KEY_PATH`; optional `BYBIT_API_BASE_URL`
- Binance: `BINANCE_API_KEY`, `BINANCE_API_SECRET`; optional
  `BINANCE_FUTURES_BASE_URL`

Credentials are resolved at request time. Secret values, auth headers, signed
URLs, and private-key contents must never be stored or emitted.

Bybit supports HMAC-SHA256 and RSA-SHA256 signing. Binance V0 supports
HMAC-SHA256. Signing must match the golden vectors in BybitBar.

## Archive module

The public Go interface is intentionally small:

```go
type Archive struct { /* hidden */ }

func Open(context.Context, Options) (*Archive, error)
func (*Archive) Sync(context.Context, SyncRequest) (SyncReport, error)
func (*Archive) Entries(context.Context, EntryQuery) ([]LedgerEntry, error)
func (*Archive) Search(context.Context, SearchQuery) ([]LedgerEntry, error)
func (*Archive) Status(context.Context) (Status, error)
func (*Archive) Close() error
```

The true-external seam is an internal Exchange adapter. The two production
adapters and a scripted test adapter satisfy it. Provider auth, response models,
pagination, rate-limit handling, and normalization remain adapter-owned.

## Ledger contract

Every row contains:

- Exchange and account identity
- stable provider-derived entry ID
- symbol, category, type, asset, side
- normalized amount plus optional fee, funding, cash flow, and balance
- order/trade IDs and provider info
- occurrence and observation times
- raw provider JSON for future remapping

Primary identity is `(exchange, account_id, entry_id)`. Sync is idempotent.
Binance scopes `tranId` by `incomeType` when deriving `entry_id`, matching the
provider's uniqueness guarantee.
SQLite uses WAL, file mode `0600`, schema versioning, deterministic query order,
and FTS5 over searchable ledger text.

## Sync behavior

- Sync all configured accounts unless `--account` narrows the request.
- First sync defaults to the previous seven days.
- Later sync overlaps the last successful checkpoint by 24 hours to catch late
  provider delivery.
- Explicit `--since` overrides the checkpoint; `--until` defaults to now.
- Split remote reads into windows no longer than seven days.
- Bybit follows every opaque `nextPageCursor` with page limit 50.
- Binance follows `page` pagination with page limit 1000.
- Upsert each page in one SQLite transaction.
- Advance an account checkpoint only after all its windows and pages succeed.
- An account failure does not erase already committed pages or other accounts.
  The report is degraded and the command exits non-zero.
- Bound pagination and reject repeated cursors/pages to avoid infinite loops.

## Control interface

`metadata --json` returns `crawlkit.control.v1` with independent identities,
paths, and commands for each binary. `status --json` returns a normalized
`control.Status` and database inventory. `doctor --json` reports config path,
database health, and credential presence only; never credential values.

## Verification

- Signing golden tests for Bybit HMAC/RSA and Binance HMAC.
- HTTP contract tests with `httptest.Server`; no live credentials or network.
- Tracer tests for windowing, pagination, idempotent upsert, account checkpoint
  safety, local query/search, control metadata, and secret redaction.
- `go test ./...`, `go vet ./...`, and builds for both commands.
