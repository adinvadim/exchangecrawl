package archive

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
	"github.com/openclaw/crawlkit/state"
	"github.com/openclaw/crawlkit/store"
)

func TestOpenMigratesBinanceV1EntryIdentitiesWithoutDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	v1, err := store.Open(ctx, store.Options{
		Path: path, Schema: archiveV2Schema + state.Schema, SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	older := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	insertV1MigrationEntry(t, ctx, v1.DB(), "111", "TRANSFER", "1", "legacy-only", older, `{"incomeType":"TRANSFER","tranId":111}`)
	insertV1MigrationEntry(t, ctx, v1.DB(), "222", "COMMISSION", "-1", "legacy-stale", older, `{"incomeType":"COMMISSION","tranId":"222"}`)
	insertV1MigrationEntry(t, ctx, v1.DB(), "COMMISSION:222", "COMMISSION", "-2", "scoped-current", newer, `{"incomeType":"COMMISSION","tranId":"222"}`)
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}

	arc, err := Open(ctx, Options{
		Path:     path,
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Streams:  migrationStreams(migrationAdapter{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = arc.Close() })

	version, err := arc.store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %d, want %d", version, schemaVersion)
	}
	hasStream, err := databaseHasColumn(ctx, arc.store.DB(), "ledger_entries", "stream")
	if err != nil {
		t.Fatal(err)
	}
	if !hasStream {
		t.Fatal("v1 migration skipped the v2 to v3 stream step")
	}
	entries, err := arc.Entries(ctx, EntryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %#v, want two migrated identities", entries)
	}
	byID := make(map[string]model.LedgerEntry, len(entries))
	for _, entry := range entries {
		byID[entry.EntryID] = entry
	}
	if entry := byID["TRANSFER:111"]; entry.Info != "legacy-only" {
		t.Fatalf("renamed legacy entry = %#v", entry)
	}
	if entry := byID["COMMISSION:222"]; entry.Info != "scoped-current" || entry.Amount != "-2" {
		t.Fatalf("collision winner = %#v, want newest scoped entry", entry)
	}
	found, err := arc.Search(ctx, SearchQuery{Text: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].EntryID != "TRANSFER:111" {
		t.Fatalf("migrated FTS results = %#v", found)
	}
}

func TestOpenFreshArchiveCreatesSchemaV3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	arc, err := Open(ctx, Options{
		Path:     filepath.Join(t.TempDir(), "archive.db"),
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Streams:  migrationStreams(migrationAdapter{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = arc.Close() })
	version, err := arc.store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("schema version = %d, want 3", version)
	}
	for _, table := range []string{"ledger_entries", "state_transitions", "sync_state"} {
		var count int
		if err := arc.store.DB().QueryRowContext(ctx,
			`select count(*) from sqlite_schema where type = 'table' and name = ?`, table,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("table %q count = %d, want 1", table, count)
		}
	}
	hasStream, err := databaseHasColumn(ctx, arc.store.DB(), "ledger_entries", "stream")
	if err != nil {
		t.Fatal(err)
	}
	if !hasStream {
		t.Fatal("fresh ledger_entries has no stream column")
	}
}

func TestOpenMigratesV2StreamAndCheckpointAndReopensIdempotently(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	v2, err := store.Open(ctx, store.Options{
		Path: path, Schema: archiveV2Schema + state.Schema, SchemaVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	insertLegacyMigrationEntry(t, ctx, v2.DB(), "legacy", checkpoint.Add(-time.Hour))
	if _, err := v2.DB().ExecContext(ctx, `
insert into sync_state(source_name, entity_type, entity_id, value, updated_at)
values (?, 'account', 'primary', ?, ?)`,
		model.ExchangeBinance, checkpoint.Format(time.RFC3339Nano), checkpoint.Format(time.RFC3339Nano),
	); err != nil {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}

	adapter := &migrationSyncAdapter{}
	now := checkpoint.Add(2 * time.Hour)
	arc, err := Open(ctx, Options{
		Path:     path,
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Streams:  migrationStreams(adapter),
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	hasStream, err := databaseHasColumn(ctx, arc.store.DB(), "ledger_entries", "stream")
	if err != nil {
		t.Fatal(err)
	}
	if !hasStream {
		t.Fatal("migrated ledger_entries has no stream column")
	}
	var stream string
	if err := arc.store.DB().QueryRowContext(ctx, `select stream from ledger_entries where entry_id = 'legacy'`).Scan(&stream); err != nil {
		t.Fatal(err)
	}
	if stream != "ledger" {
		t.Fatalf("migrated stream = %q, want ledger", stream)
	}
	if _, err := arc.Sync(ctx, SyncRequest{}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.requests) != 1 {
		t.Fatalf("FetchPage requests = %d, want 1", len(adapter.requests))
	}
	if want := checkpoint.Add(-24 * time.Hour); !adapter.requests[0].Start.Equal(want) {
		t.Fatalf("migrated Checkpoint sync start = %s, want %s", adapter.requests[0].Start, want)
	}
	if err := arc.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, Options{
		Path:     path,
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Streams:  migrationStreams(&migrationSyncAdapter{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	var checkpointRows int
	if err := reopened.store.DB().QueryRowContext(ctx, `
select count(*) from sync_state
where source_name = ? and entity_type = 'ledger' and entity_id = 'primary'`, model.ExchangeBinance,
	).Scan(&checkpointRows); err != nil {
		t.Fatal(err)
	}
	if checkpointRows != 1 {
		t.Fatalf("ledger Checkpoint rows = %d, want 1", checkpointRows)
	}
}

func TestOpenMigratesBinanceV1EntriesAcrossKeysetBatches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	v1, err := store.Open(ctx, store.Options{
		Path: path, Schema: archiveV2Schema + state.Schema, SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	err = v1.WithTx(ctx, func(tx *sql.Tx) error {
		for index := range migrationBatchSize + 1 {
			entryID := fmt.Sprintf("%d", 10_000+index)
			insertV1MigrationEntry(t, ctx, tx, entryID, "TRANSFER", "1", "batch", observedAt,
				fmt.Sprintf(`{"incomeType":"TRANSFER","tranId":%s}`, entryID))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}

	arc, err := Open(ctx, Options{
		Path:     path,
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Streams:  migrationStreams(migrationAdapter{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = arc.Close() })
	entries, err := arc.Entries(ctx, EntryQuery{Limit: migrationBatchSize + 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != migrationBatchSize+1 {
		t.Fatalf("migrated entries = %d, want %d", len(entries), migrationBatchSize+1)
	}
	byID := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		byID[entry.EntryID] = struct{}{}
	}
	for _, entryID := range []string{"TRANSFER:10000", fmt.Sprintf("TRANSFER:%d", 10_000+migrationBatchSize)} {
		if _, ok := byID[entryID]; !ok {
			t.Fatalf("missing migrated boundary identity %q", entryID)
		}
	}
}

type migrationExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type migrationAdapter struct{}

func migrationStreams(adapter source.Adapter) []source.StreamBinding {
	return []source.StreamBinding{{Name: "ledger", Stream: "ledger", Adapter: adapter}}
}

func (migrationAdapter) Exchange() model.Exchange { return model.ExchangeBinance }

func (migrationAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (migrationAdapter) FetchPage(context.Context, source.PageRequest) (source.Page, error) {
	panic("migration test must not access the Exchange")
}

type migrationSyncAdapter struct {
	requests []source.PageRequest
}

func (*migrationSyncAdapter) Exchange() model.Exchange { return model.ExchangeBinance }

func (*migrationSyncAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (a *migrationSyncAdapter) FetchPage(_ context.Context, request source.PageRequest) (source.Page, error) {
	a.requests = append(a.requests, request)
	return source.Page{Done: true}, nil
}

func databaseHasColumn(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func insertLegacyMigrationEntry(t *testing.T, ctx context.Context, db migrationExecer, entryID string, occurredAt time.Time) {
	t.Helper()
	insertV1MigrationEntry(t, ctx, db, entryID, "TRANSFER", "1", "legacy", occurredAt.Add(time.Minute),
		`{"incomeType":"TRANSFER","tranId":"legacy"}`)
}

func insertV1MigrationEntry(
	t *testing.T,
	ctx context.Context,
	db migrationExecer,
	entryID string,
	entryType string,
	amount string,
	info string,
	observedAt time.Time,
	rawJSON string,
) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
insert into ledger_entries(
  exchange, account_id, account_label, entry_id, symbol, category, entry_type,
  asset, side, amount, fee, funding, cash_flow, balance, order_id, trade_id,
  info, occurred_at, observed_at, raw_json
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		model.ExchangeBinance, "primary", "Primary", entryID, "BTCUSDT", "income", entryType,
		"USDT", "", amount, "", "", amount, "", "", "", info,
		observedAt.Add(-time.Hour).UnixNano(), observedAt.UnixNano(), rawJSON,
	)
	if err != nil {
		t.Fatal(err)
	}
}

const archiveV2Schema = `
create table ledger_entries (
  id integer primary key,
  exchange text not null,
  account_id text not null,
  account_label text not null,
  entry_id text not null,
  symbol text not null,
  category text not null,
  entry_type text not null,
  asset text not null,
  side text not null,
  amount text not null,
  fee text not null,
  funding text not null,
  cash_flow text not null,
  balance text not null,
  order_id text not null,
  trade_id text not null,
  info text not null,
  occurred_at integer not null,
  observed_at integer not null,
  raw_json text not null,
  unique(exchange, account_id, entry_id)
);

create virtual table ledger_entries_fts using fts5(
  symbol,
  category,
  entry_type,
  asset,
  side,
  order_id,
  trade_id,
  info,
  content='ledger_entries',
  content_rowid='id'
);

create trigger ledger_entries_ai after insert on ledger_entries begin
  insert into ledger_entries_fts(
    rowid, symbol, category, entry_type, asset, side, order_id, trade_id, info
  ) values (
    new.id, new.symbol, new.category, new.entry_type, new.asset, new.side,
    new.order_id, new.trade_id, new.info
  );
end;

create trigger ledger_entries_ad after delete on ledger_entries begin
  insert into ledger_entries_fts(
    ledger_entries_fts, rowid, symbol, category, entry_type, asset, side,
    order_id, trade_id, info
  ) values (
    'delete', old.id, old.symbol, old.category, old.entry_type, old.asset,
    old.side, old.order_id, old.trade_id, old.info
  );
end;

create trigger ledger_entries_au after update on ledger_entries begin
  insert into ledger_entries_fts(
    ledger_entries_fts, rowid, symbol, category, entry_type, asset, side,
    order_id, trade_id, info
  ) values (
    'delete', old.id, old.symbol, old.category, old.entry_type, old.asset,
    old.side, old.order_id, old.trade_id, old.info
  );
  insert into ledger_entries_fts(
    rowid, symbol, category, entry_type, asset, side, order_id, trade_id, info
  ) values (
    new.id, new.symbol, new.category, new.entry_type, new.asset, new.side,
    new.order_id, new.trade_id, new.info
  );
end;
`
