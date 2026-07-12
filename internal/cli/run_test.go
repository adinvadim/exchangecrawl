package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"

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
		NewStreams: func(string) ([]source.StreamBinding, error) {
			return []source.StreamBinding{{Name: "ledger", Stream: "ledger", Adapter: adapter}}, nil
		},
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
	stdout.Reset()
	if err := Run(t.Context(), []string{"--config", configPath, "--db", dbPath, "entries", "--stream", "spot", "--json"}, &stdout, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("entries --stream: %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("entries --stream made a remote call; total = %d", adapter.calls)
	}
	entries = nil
	if err := json.Unmarshal(stdout.Bytes(), &entries); err != nil {
		t.Fatalf("decode filtered entries: %v\n%s", err, stdout.String())
	}
	if len(entries) != 0 {
		t.Fatalf("spot entries = %#v, want none", entries)
	}
}

func TestRunSyncThenEventsReadsOnlyLocalArchive(t *testing.T) {
	t.Parallel()

	adapter := &cliTestEventAdapter{}
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
		NewStreams: func(string) ([]source.StreamBinding, error) {
			return []source.StreamBinding{{Name: "withdrawal/pending", Stream: "withdrawal", Events: adapter}}, nil
		},
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
	if err := Run(t.Context(), []string{
		"--config", configPath, "--db", dbPath, "events",
		"--stream", "withdrawal", "--status", "Completed", "--json",
	}, &stdout, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("events: %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("events made a remote call; total = %d", adapter.calls)
	}
	var events []model.StateObservation
	if err := json.Unmarshal(stdout.Bytes(), &events); err != nil {
		t.Fatalf("decode events: %v\n%s", err, stdout.String())
	}
	if len(events) != 1 || events[0].ObjectID != "withdrawal-1" || events[0].AccountLabel != "Primary" {
		t.Fatalf("events = %#v", events)
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
		NewStreams: func(string) ([]source.StreamBinding, error) {
			return []source.StreamBinding{{Name: "ledger", Stream: "ledger", Adapter: &cliTestAdapter{}}}, nil
		},
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

func TestConfigureStreamsDisablesByModule(t *testing.T) {
	streams := cliTestStreams()
	disabled := false
	cfg := map[string]appconfig.StreamConfig{
		"spot": {Enabled: &disabled},
	}

	enabled, statuses, warnings := configureStreams(streams, cfg)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v", warnings)
	}
	if len(enabled) != 1 || enabled[0].Name != "transfer/withdrawals" {
		t.Fatalf("enabled = %#v", enabled)
	}
	if statuses[0].Name != "spot/fills" || statuses[0].Enabled {
		t.Fatalf("statuses = %#v", statuses)
	}
}

func TestConfigureStreamsDisablesByBinding(t *testing.T) {
	streams := cliTestStreams()
	disabled := false
	cfg := map[string]appconfig.StreamConfig{
		"spot/fills": {Enabled: &disabled},
	}

	enabled, statuses, warnings := configureStreams(streams, cfg)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v", warnings)
	}
	if len(enabled) != 2 || enabled[0].Name != "spot/orders" || enabled[1].Name != "transfer/withdrawals" {
		t.Fatalf("enabled = %#v", enabled)
	}
	if statuses[0].Name != "spot/fills" || statuses[0].Enabled {
		t.Fatalf("statuses = %#v", statuses)
	}
}

func TestConfigureStreamsBindingOverridesModule(t *testing.T) {
	streams := cliTestStreams()
	enabledValue, disabledValue := true, false
	cfg := map[string]appconfig.StreamConfig{
		"spot":       {Enabled: &disabledValue},
		"spot/fills": {Enabled: &enabledValue},
	}

	enabled, _, warnings := configureStreams(streams, cfg)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v", warnings)
	}
	if len(enabled) != 2 || enabled[0].Name != "spot/fills" || enabled[1].Name != "transfer/withdrawals" {
		t.Fatalf("enabled = %#v", enabled)
	}
}

func TestConfigureStreamsAppliesDurationOverrides(t *testing.T) {
	streams := cliTestStreams()
	cfg := map[string]appconfig.StreamConfig{
		"spot/fills": {
			InitialLookback:   "72h",
			CheckpointOverlap: "90m",
			MaxWindow:         "24h",
		},
	}

	enabled, statuses, warnings := configureStreams(streams, cfg)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v", warnings)
	}
	if got := enabled[0]; got.InitialLookback != 72*time.Hour || got.CheckpointOverlap != 90*time.Minute || got.MaxWindow != 24*time.Hour {
		t.Fatalf("configured binding = %#v", got)
	}
	if got := statuses[0]; got.InitialLookback != "72h0m0s" || got.CheckpointOverlap != "1h30m0s" || got.MaxWindow != "24h0m0s" {
		t.Fatalf("stream status = %#v", got)
	}
}

func TestConfigureStreamsWarnsForUnknownName(t *testing.T) {
	streams := cliTestStreams()
	cfg := map[string]appconfig.StreamConfig{
		"unknown": {},
	}

	enabled, _, warnings := configureStreams(streams, cfg)
	if len(enabled) != len(streams) {
		t.Fatalf("enabled = %d, want %d", len(enabled), len(streams))
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"unknown"`) {
		t.Fatalf("warnings = %v", warnings)
	}
}

func TestRunDoctorListsDisabledStreams(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte("[streams.ledger]\nenabled = false\nmax_window = \"24h\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := Spec{
		ID: "bybitcrawl",
		Config: appconfig.Spec{
			AppID: "bybitcrawl", EnvPrefix: "BYBIT", DefaultBaseURL: "https://api.bybit.com",
		},
		NewStreams: func(string) ([]source.StreamBinding, error) {
			return []source.StreamBinding{{Name: "ledger", Stream: "ledger", Adapter: &cliTestAdapter{}}}, nil
		},
	}
	var stdout bytes.Buffer
	if err := Run(t.Context(), []string{"--config", configPath, "doctor", "--json"}, &stdout, &bytes.Buffer{}, spec); err != nil {
		t.Fatalf("doctor: %v\n%s", err, stdout.String())
	}
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode doctor report: %v\n%s", err, stdout.String())
	}
	if len(report.Streams) != 1 || report.Streams[0].Name != "ledger" || report.Streams[0].Enabled || report.Streams[0].MaxWindow != "24h0m0s" {
		t.Fatalf("streams = %#v", report.Streams)
	}
}

func TestRunWarnsForUnknownConfiguredStream(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte("[streams.unknown]\nenabled = false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := Spec{
		ID: "bybitcrawl",
		Config: appconfig.Spec{
			AppID: "bybitcrawl", EnvPrefix: "BYBIT", DefaultBaseURL: "https://api.bybit.com",
		},
		NewStreams: func(string) ([]source.StreamBinding, error) {
			return []source.StreamBinding{{Name: "ledger", Stream: "ledger", Adapter: &cliTestAdapter{}}}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	if err := Run(t.Context(), []string{"--config", configPath, "doctor", "--json"}, &stdout, &stderr, spec); err != nil {
		t.Fatalf("doctor: %v\n%s", err, stdout.String())
	}
	if !strings.Contains(stderr.String(), `warning: configured stream "unknown"`) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestWriteEntriesSanitizesTextOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := writeEntries(&stdout, []model.LedgerEntry{{
		OccurredAt: time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
		AccountID:  "primary",
		Symbol:     "BTC\nUSDT",
		Type:       "TRADE\x1b[31m",
		Amount:     "1\t.25",
		Asset:      "USDT\u202e",
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range stdout.String() {
		if unicode.IsControl(r) && r != '\t' && r != '\n' || unicode.In(r, unicode.Cf) {
			t.Fatalf("text output contains unsafe control %U: %q", r, stdout.String())
		}
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Fatalf("entry injected extra output lines: %q", stdout.String())
	}
}

type cliTestAdapter struct {
	calls int
}

func cliTestStreams() []source.StreamBinding {
	adapter := &cliTestAdapter{}
	return []source.StreamBinding{
		{Name: "spot/fills", Stream: "spot", Adapter: adapter},
		{Name: "spot/orders", Stream: "spot", Adapter: adapter},
		{Name: "transfer/withdrawals", Stream: "transfer", Adapter: adapter},
	}
}

type cliTestEventAdapter struct {
	calls int
}

func (a *cliTestEventAdapter) Exchange() model.Exchange { return model.ExchangeBybit }

func (a *cliTestEventAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (a *cliTestEventAdapter) FetchEventPage(_ context.Context, request source.PageRequest) (source.EventPage, error) {
	a.calls++
	return source.EventPage{
		Done: true,
		Observations: []model.StateObservation{{
			Exchange:         model.ExchangeBybit,
			AccountID:        request.Account.ID,
			AccountLabel:     request.Account.Label,
			Stream:           "withdrawal",
			ObjectType:       "withdrawal",
			ObjectID:         "withdrawal-1",
			Status:           "Completed",
			StateFingerprint: "completed",
			OccurredAt:       time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			ObservedAt:       request.End.UTC(),
			RawJSON:          json.RawMessage(`{"id":"withdrawal-1"}`),
		}},
	}, nil
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
