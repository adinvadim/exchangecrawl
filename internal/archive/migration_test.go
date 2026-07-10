package archive

import (
	"context"
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
		Path: path, Schema: archiveSchema + state.Schema, SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	older := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	insertMigrationEntry(t, ctx, v1, "111", "TRANSFER", "1", "legacy-only", older, `{"incomeType":"TRANSFER","tranId":111}`)
	insertMigrationEntry(t, ctx, v1, "222", "COMMISSION", "-1", "legacy-stale", older, `{"incomeType":"COMMISSION","tranId":"222"}`)
	insertMigrationEntry(t, ctx, v1, "COMMISSION:222", "COMMISSION", "-2", "scoped-current", newer, `{"incomeType":"COMMISSION","tranId":"222"}`)
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}

	arc, err := Open(ctx, Options{
		Path:     path,
		Accounts: []source.Account{{ID: "primary", Label: "Primary"}},
		Adapter:  migrationAdapter{},
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

func insertMigrationEntry(
	t *testing.T,
	ctx context.Context,
	db *store.Store,
	entryID string,
	entryType string,
	amount string,
	info string,
	observedAt time.Time,
	rawJSON string,
) {
	t.Helper()
	_, err := db.DB().ExecContext(ctx, upsertLedgerEntry,
		model.ExchangeBinance,
		"primary",
		"Primary",
		entryID,
		"BTCUSDT",
		"income",
		entryType,
		"USDT",
		"",
		amount,
		"",
		"",
		amount,
		"",
		"",
		"",
		info,
		observedAt.Add(-time.Hour).UnixNano(),
		observedAt.UnixNano(),
		rawJSON,
	)
	if err != nil {
		t.Fatal(err)
	}
}

type migrationAdapter struct{}

func (migrationAdapter) Exchange() model.Exchange { return model.ExchangeBinance }

func (migrationAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (migrationAdapter) FetchPage(context.Context, source.PageRequest) (source.Page, error) {
	panic("migration test must not access the Exchange")
}
