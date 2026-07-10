package archive_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/archive"
	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestFirstSyncUsesPreviousSevenDays(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	entry := ledgerEntry("income-1", now.Add(-time.Hour), "first funding payment")
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBinance,
		pages:    []scriptedPage{{page: source.Page{Entries: []model.LedgerEntry{entry}, Done: true}}},
	}
	account := source.Account{ID: "primary", Label: "Primary", APIKeyEnv: "BINANCE_API_KEY", APISecretEnv: "BINANCE_API_SECRET"}
	arc := openArchive(t, ctx, adapter, []source.Account{account}, now)

	report, err := arc.Sync(ctx, archive.SyncRequest{})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if report.Degraded {
		t.Fatal("Sync() unexpectedly degraded")
	}
	if len(adapter.requests) != 1 {
		t.Fatalf("FetchPage() calls = %d, want 1", len(adapter.requests))
	}
	request := adapter.requests[0]
	if want := now.Add(-7 * 24 * time.Hour); !request.Start.Equal(want) {
		t.Errorf("request.Start = %s, want %s", request.Start, want)
	}
	if !request.End.Equal(now) {
		t.Errorf("request.End = %s, want %s", request.End, now)
	}

	entries, err := arc.Entries(ctx, archive.EntryQuery{})
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].EntryID != entry.EntryID {
		t.Fatalf("Entries() = %#v, want entry %q", entries, entry.EntryID)
	}
}

func TestSyncSplitsLongRangeIntoAscendingSevenDayWindows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	until := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	since := until.Add(-20 * 24 * time.Hour)
	adapter := &scriptedAdapter{exchange: model.ExchangeBybit}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, until)

	if _, err := arc.Sync(ctx, archive.SyncRequest{Since: &since, Until: &until}); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if len(adapter.requests) != 3 {
		t.Fatalf("FetchPage() calls = %d, want 3", len(adapter.requests))
	}
	want := [][2]time.Time{
		{since, since.Add(7 * 24 * time.Hour)},
		{since.Add(7 * 24 * time.Hour), since.Add(14 * 24 * time.Hour)},
		{since.Add(14 * 24 * time.Hour), until},
	}
	for i, request := range adapter.requests {
		if !request.Start.Equal(want[i][0]) || !request.End.Equal(want[i][1]) {
			t.Errorf("request %d window = [%s, %s], want [%s, %s]", i, request.Start, request.End, want[i][0], want[i][1])
		}
	}
}

func TestSyncFollowsOpaquePaginationCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBybit,
		pages: []scriptedPage{
			{page: source.Page{Entries: []model.LedgerEntry{ledgerEntryFor(model.ExchangeBybit, "first", now.Add(-2*time.Hour), "one")}, NextCursor: "opaque:2"}},
			{page: source.Page{Entries: []model.LedgerEntry{ledgerEntryFor(model.ExchangeBybit, "second", now.Add(-time.Hour), "two")}, Done: true}},
		},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, now)

	report, err := arc.Sync(ctx, archive.SyncRequest{})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if report.Pages != 2 || report.Entries != 2 {
		t.Fatalf("Sync() pages/entries = %d/%d, want 2/2", report.Pages, report.Entries)
	}
	if got := adapter.requests[1].Cursor; got != "opaque:2" {
		t.Errorf("second cursor = %q, want opaque:2", got)
	}
}

func TestSyncUpsertsProviderCorrectionsIdempotently(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	original := ledgerEntry("income-1", now.Add(-time.Hour), "original")
	corrected := original
	corrected.Amount = "2.50"
	corrected.Info = "corrected"
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBinance,
		pages: []scriptedPage{
			{page: source.Page{Entries: []model.LedgerEntry{original}, Done: true}},
			{page: source.Page{Entries: []model.LedgerEntry{corrected}, Done: true}},
		},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, now)

	if _, err := arc.Sync(ctx, archive.SyncRequest{}); err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	if _, err := arc.Sync(ctx, archive.SyncRequest{}); err != nil {
		t.Fatalf("second Sync() error = %v", err)
	}
	entries, err := arc.Entries(ctx, archive.EntryQuery{})
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Entries() count = %d, want 1", len(entries))
	}
	if entries[0].Amount != corrected.Amount || entries[0].Info != corrected.Info {
		t.Errorf("Entries()[0] = %#v, want provider correction", entries[0])
	}
}

func TestFailedAccountKeepsCommittedPagesButDoesNotAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	firstUntil := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	secondUntil := firstUntil.Add(2 * time.Hour)
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBinance,
		pages: []scriptedPage{
			{page: source.Page{Entries: []model.LedgerEntry{ledgerEntry("first", firstUntil.Add(-time.Hour), "first")}, Done: true}},
			{page: source.Page{Entries: []model.LedgerEntry{ledgerEntry("partial", firstUntil.Add(time.Hour), "committed before failure")}, NextCursor: "page:2"}},
			{err: errors.New("remote unavailable")},
		},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, firstUntil)

	if _, err := arc.Sync(ctx, archive.SyncRequest{}); err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	report, err := arc.Sync(ctx, archive.SyncRequest{Until: &secondUntil})
	if err == nil {
		t.Fatal("second Sync() error = nil, want Degraded Sync")
	}
	if !report.Degraded {
		t.Fatal("second Sync() did not report degradation")
	}
	entries, queryErr := arc.Entries(ctx, archive.EntryQuery{})
	if queryErr != nil {
		t.Fatalf("Entries() error = %v", queryErr)
	}
	if len(entries) != 2 {
		t.Fatalf("Entries() count = %d, want committed first page and partial page", len(entries))
	}
	status, statusErr := arc.Status(ctx)
	if statusErr != nil {
		t.Fatalf("Status() error = %v", statusErr)
	}
	checkpoint := status.Accounts[0].Checkpoint
	if checkpoint == nil || !checkpoint.Equal(firstUntil) {
		t.Fatalf("Checkpoint = %v, want %s", checkpoint, firstUntil)
	}
}

func TestEntriesAndSearchStayLocal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	funding := ledgerEntry("funding", now.Add(-time.Hour), "quarterly funding payment")
	commission := ledgerEntry("commission", now.Add(-2*time.Hour), "trading commission")
	commission.Symbol = "ETHUSDT"
	commission.Type = "COMMISSION"
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBinance,
		pages:    []scriptedPage{{page: source.Page{Entries: []model.LedgerEntry{funding, commission}, Done: true}}},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, now)
	if _, err := arc.Sync(ctx, archive.SyncRequest{}); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	remoteCalls := len(adapter.requests)

	entries, err := arc.Entries(ctx, archive.EntryQuery{Symbol: "ETHUSDT", Type: "COMMISSION"})
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].EntryID != commission.EntryID {
		t.Fatalf("Entries() = %#v, want commission", entries)
	}
	found, err := arc.Search(ctx, archive.SearchQuery{Text: "quarterly funding"})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(found) != 1 || found[0].EntryID != funding.EntryID {
		t.Fatalf("Search() = %#v, want funding", found)
	}
	if got := len(adapter.requests); got != remoteCalls {
		t.Fatalf("local queries made %d additional remote call(s)", got-remoteCalls)
	}
}

func TestSyncRejectsRepeatedPaginationCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBybit,
		pages: []scriptedPage{
			{page: source.Page{NextCursor: "same"}},
			{page: source.Page{NextCursor: "same"}},
		},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, now)

	report, err := arc.Sync(ctx, archive.SyncRequest{})
	if err == nil {
		t.Fatal("Sync() error = nil, want repeated cursor error")
	}
	if !report.Degraded {
		t.Fatal("Sync() did not report degradation")
	}
	if got := len(adapter.requests); got != 2 {
		t.Fatalf("FetchPage() calls = %d, want guard after 2", got)
	}
	status, statusErr := arc.Status(ctx)
	if statusErr != nil {
		t.Fatalf("Status() error = %v", statusErr)
	}
	if status.Accounts[0].Checkpoint != nil {
		t.Fatalf("Checkpoint = %v after repeated cursor", status.Accounts[0].Checkpoint)
	}
}

func TestLaterSyncOverlapsCheckpointByTwentyFourHours(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	firstUntil := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	secondUntil := firstUntil.Add(2 * time.Hour)
	adapter := &scriptedAdapter{
		exchange: model.ExchangeBinance,
		pages: []scriptedPage{
			{page: source.Page{Done: true}},
			{page: source.Page{Done: true}},
		},
	}
	arc := openArchive(t, ctx, adapter, []source.Account{{ID: "primary", Label: "Primary"}}, firstUntil)

	if _, err := arc.Sync(ctx, archive.SyncRequest{}); err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	if _, err := arc.Sync(ctx, archive.SyncRequest{Until: &secondUntil}); err != nil {
		t.Fatalf("second Sync() error = %v", err)
	}
	request := adapter.requests[1]
	if want := firstUntil.Add(-24 * time.Hour); !request.Start.Equal(want) {
		t.Errorf("second sync start = %s, want %s", request.Start, want)
	}
	if !request.End.Equal(secondUntil) {
		t.Errorf("second sync end = %s, want %s", request.End, secondUntil)
	}
}

func TestAccountFailureDoesNotStopOtherConnectedAccounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	adapter := &scriptedAdapter{exchange: model.ExchangeBinance}
	adapter.fetch = func(request source.PageRequest) (source.Page, error) {
		if request.Account.ID == "broken" {
			return source.Page{}, errors.New("broken account")
		}
		entry := ledgerEntry("healthy-entry", now.Add(-time.Hour), "healthy")
		entry.AccountID = "healthy"
		entry.AccountLabel = "Healthy"
		return source.Page{Entries: []model.LedgerEntry{entry}, Done: true}, nil
	}
	arc := openArchive(t, ctx, adapter, []source.Account{
		{ID: "broken", Label: "Broken"},
		{ID: "healthy", Label: "Healthy"},
	}, now)

	report, err := arc.Sync(ctx, archive.SyncRequest{})
	if err == nil || !report.Degraded {
		t.Fatalf("Sync() report/error = %#v/%v, want Degraded Sync", report, err)
	}
	entries, queryErr := arc.Entries(ctx, archive.EntryQuery{AccountID: "healthy"})
	if queryErr != nil {
		t.Fatalf("Entries() error = %v", queryErr)
	}
	if len(entries) != 1 || entries[0].EntryID != "healthy-entry" {
		t.Fatalf("healthy Entries() = %#v", entries)
	}
	status, statusErr := arc.Status(ctx)
	if statusErr != nil {
		t.Fatalf("Status() error = %v", statusErr)
	}
	if status.Accounts[0].Checkpoint != nil || status.Accounts[1].Checkpoint == nil {
		t.Fatalf("account Checkpoints = %#v", status.Accounts)
	}
}

type scriptedPage struct {
	page source.Page
	err  error
}

type scriptedAdapter struct {
	exchange model.Exchange
	status   source.CredentialStatus
	pages    []scriptedPage
	requests []source.PageRequest
	fetch    func(source.PageRequest) (source.Page, error)
}

func (a *scriptedAdapter) Exchange() model.Exchange { return a.exchange }

func (a *scriptedAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	if !a.status.Ready && len(a.status.Missing) == 0 {
		return source.CredentialStatus{Ready: true}
	}
	return a.status
}

func (a *scriptedAdapter) FetchPage(_ context.Context, request source.PageRequest) (source.Page, error) {
	a.requests = append(a.requests, request)
	if a.fetch != nil {
		return a.fetch(request)
	}
	if len(a.pages) == 0 {
		return source.Page{Done: true}, nil
	}
	next := a.pages[0]
	a.pages = a.pages[1:]
	return next.page, next.err
}

func openArchive(t *testing.T, ctx context.Context, adapter source.Adapter, accounts []source.Account, now time.Time) *archive.Archive {
	t.Helper()
	arc, err := archive.Open(ctx, archive.Options{
		Path:     filepath.Join(t.TempDir(), "archive.db"),
		Accounts: accounts,
		Adapter:  adapter,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := arc.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return arc
}

func ledgerEntry(id string, occurredAt time.Time, info string) model.LedgerEntry {
	return ledgerEntryFor(model.ExchangeBinance, id, occurredAt, info)
}

func ledgerEntryFor(exchange model.Exchange, id string, occurredAt time.Time, info string) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     exchange,
		AccountID:    "primary",
		AccountLabel: "Primary",
		EntryID:      id,
		Symbol:       "BTCUSDT",
		Category:     "income",
		Type:         "FUNDING_FEE",
		Asset:        "USDT",
		Side:         "",
		Amount:       "1.25",
		Funding:      "1.25",
		Info:         info,
		OccurredAt:   occurredAt.UTC(),
		ObservedAt:   occurredAt.Add(time.Minute).UTC(),
		RawJSON:      json.RawMessage(`{"id":"` + id + `"}`),
	}
}
