# ExchangeCrawl

## Language

**Exchange**: A trading venue read through its official REST interface. V0:
Bybit or Binance. Avoid: provider in user-facing text.

**Connected Account**: One credential identity on one Exchange, identified by a
stable local ID and label. Avoid: wallet, credential set.

**Ledger Entry**: An immutable or provider-correctable financial account event
stored in the local archive. Bybit transaction logs and Binance income rows map
to this term. Avoid: transaction when referring to the cross-Exchange model.

**Archive**: The local SQLite database containing Ledger Entries and sync
checkpoints. It is canonical for local queries. Avoid: cache.

**Checkpoint**: The end time of the last fully successful sync for one Connected
Account. It never advances on a partial account sync. Avoid: cursor, which is a
provider page token.

**Degraded Sync**: A sync in which at least one Connected Account failed while
another account or page succeeded. Avoid: partial success.

