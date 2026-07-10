package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"

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
