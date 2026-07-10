package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/crawlkit/store"

	"github.com/adinvadim/exchangecrawl/internal/appconfig"
	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestRunMetadataExposesCrawlkitControlIdentity(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := Run(context.Background(), []string{"metadata", "--json"}, &stdout, &bytes.Buffer{}, Spec{
		ID:          "bybitcrawl",
		DisplayName: "Bybit Crawl",
		Description: "Local-first Bybit ledger archive.",
		Config: appconfig.Spec{
			AppID:          "bybitcrawl",
			EnvPrefix:      "BYBIT",
			DefaultBaseURL: "https://api.bybit.com",
			BaseURLEnv:     "BYBIT_API_BASE_URL",
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	var manifest control.Manifest
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		t.Fatalf("decode metadata: %v\n%s", err, stdout.String())
	}
	if manifest.ID != "bybitcrawl" || manifest.Binary.Name != "bybitcrawl" {
		t.Fatalf("manifest identity = %#v", manifest)
	}
	if manifest.Paths.DefaultConfig == "" || manifest.Paths.DefaultDatabase == "" {
		t.Fatalf("manifest paths = %#v", manifest.Paths)
	}
}

func TestRunSyncThenEntriesReadsOnlyLocalArchive(t *testing.T) {
	t.Parallel()

	adapter := &cliTestAdapter{}
	spec := Spec{
		ID:          "bybitcrawl",
		DisplayName: "Bybit Crawl",
		Description: "Local-first Bybit ledger archive.",
		Config: appconfig.Spec{
			AppID:          "bybitcrawl",
			EnvPrefix:      "BYBIT",
			DefaultBaseURL: "https://api.bybit.com",
			BaseURLEnv:     "BYBIT_API_BASE_URL",
		},
		NewAdapter: func(string) (source.Adapter, error) { return adapter, nil },
	}
	configPath := filepath.Join(t.TempDir(), "config.toml")
	dbPath := filepath.Join(t.TempDir(), "archive.db")

	if err := Run(t.Context(), []string{"--config", configPath, "init"}, &bytes.Buffer{}, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := Run(t.Context(), []string{"--config", configPath, "--db", dbPath, "sync", "--json"}, &bytes.Buffer{}, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("remote calls after sync = %d, want 1", adapter.calls)
	}

	var stdout bytes.Buffer
	if err := Run(t.Context(), []string{"--config", configPath, "--db", dbPath, "entries", "--json"}, &stdout, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("entries: %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("entries made a remote call; total = %d", adapter.calls)
	}
	var entries []model.LedgerEntry
	if err := json.Unmarshal(stdout.Bytes(), &entries); err != nil {
		t.Fatalf("decode entries: %v\n%s", err, stdout.String())
	}
	if len(entries) != 1 || entries[0].EntryID != "entry-1" {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestRunDoctorRejectsUnrelatedSQLite(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "unrelated.db")
	db, err := store.Open(t.Context(), store.Options{
		Path:          dbPath,
		Schema:        `create table unrelated(id integer primary key);`,
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("create unrelated SQLite: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close unrelated SQLite: %v", err)
	}

	spec := Spec{
		ID:          "bybitcrawl",
		DisplayName: "Bybit Crawl",
		Config: appconfig.Spec{
			AppID:          "bybitcrawl",
			EnvPrefix:      "BYBIT",
			DefaultBaseURL: "https://api.bybit.com",
			BaseURLEnv:     "BYBIT_API_BASE_URL",
		},
		NewAdapter: func(string) (source.Adapter, error) { return &cliTestAdapter{}, nil },
	}
	var stdout bytes.Buffer
	err = Run(t.Context(), []string{
		"--config", filepath.Join(t.TempDir(), "missing.toml"),
		"--db", dbPath,
		"doctor", "--json",
	}, &stdout, &bytes.Buffer{}, spec)
	if err == nil {
		t.Fatal("doctor accepted unrelated SQLite")
	}
	var report doctorReport
	if decodeErr := json.Unmarshal(stdout.Bytes(), &report); decodeErr != nil {
		t.Fatalf("decode doctor report: %v\n%s", decodeErr, stdout.String())
	}
	if !report.DatabasePresent || report.DatabaseReady || report.Ready {
		t.Fatalf("doctor report = %#v, want present but unhealthy database", report)
	}
}

type cliTestAdapter struct {
	calls int
}

func (a *cliTestAdapter) Exchange() model.Exchange { return model.ExchangeBybit }

func (a *cliTestAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (a *cliTestAdapter) FetchPage(_ context.Context, request source.PageRequest) (source.Page, error) {
	a.calls++
	return source.Page{
		Done: true,
		Entries: []model.LedgerEntry{{
			Exchange:     model.ExchangeBybit,
			AccountID:    request.Account.ID,
			AccountLabel: request.Account.Label,
			EntryID:      "entry-1",
			Symbol:       "BTCUSDT",
			Category:     "linear",
			Type:         "TRADE",
			Asset:        "USDT",
			Amount:       "1.25",
			OccurredAt:   time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
			ObservedAt:   time.Date(2026, 7, 10, 0, 1, 0, 0, time.UTC),
			RawJSON:      json.RawMessage(`{"id":"entry-1"}`),
		}},
	}, nil
}
