package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	defaultLookback   = 7 * 24 * time.Hour
	checkpointOverlap = 24 * time.Hour
	maxWindow         = 7 * 24 * time.Hour
	maxPagesPerWindow = 10_000
	checkpointType    = "account"
)

type SyncRequest struct {
	AccountID       string
	Since           *time.Time
	Until           *time.Time
	InitialLookback time.Duration
}

type SyncReport struct {
	Exchange   model.Exchange      `json:"exchange"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at"`
	Accounts   []AccountSyncReport `json:"accounts"`
	Pages      int                 `json:"pages"`
	Entries    int                 `json:"entries"`
	Degraded   bool                `json:"degraded"`
}

type AccountSyncReport struct {
	AccountID string    `json:"account_id"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Pages     int       `json:"pages"`
	Entries   int       `json:"entries"`
	Error     string    `json:"error,omitempty"`
}

type DegradedSyncError struct {
	Failures map[string]error
}

func (e *DegradedSyncError) Error() string {
	return fmt.Sprintf("Degraded Sync: %d Connected Account(s) failed", len(e.Failures))
}

func (a *Archive) Sync(ctx context.Context, request SyncRequest) (SyncReport, error) {
	startedAt := a.now().UTC()
	until := startedAt
	if request.Until != nil {
		until = request.Until.UTC()
	}
	accounts, err := a.selectedAccounts(request.AccountID)
	if err != nil {
		return SyncReport{}, err
	}
	report := SyncReport{Exchange: a.exchange(), StartedAt: startedAt}
	failures := make(map[string]error)

	for _, account := range accounts {
		accountReport, syncErr := a.syncAccount(ctx, account, request.Since, until, request.InitialLookback)
		report.Accounts = append(report.Accounts, accountReport)
		report.Pages += accountReport.Pages
		report.Entries += accountReport.Entries
		if syncErr != nil {
			accountReport.Error = syncErr.Error()
			report.Accounts[len(report.Accounts)-1] = accountReport
			failures[account.ID] = syncErr
		}
	}

	report.FinishedAt = a.now().UTC()
	report.Degraded = len(failures) > 0
	if report.Degraded {
		return report, &DegradedSyncError{Failures: failures}
	}
	return report, nil
}

func (a *Archive) selectedAccounts(accountID string) ([]source.Account, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return append([]source.Account(nil), a.accounts...), nil
	}
	for _, account := range a.accounts {
		if account.ID == accountID {
			return []source.Account{account}, nil
		}
	}
	return nil, fmt.Errorf("unknown Connected Account %q", accountID)
}

func (a *Archive) syncAccount(ctx context.Context, account source.Account, explicitSince *time.Time, until time.Time, initialLookback time.Duration) (AccountSyncReport, error) {
	start, err := a.syncStart(ctx, account.ID, explicitSince, until, initialLookback)
	result := AccountSyncReport{AccountID: account.ID, Start: start, End: until}
	if err != nil {
		return result, err
	}
	if !start.Before(until) {
		return result, fmt.Errorf("sync start %s must be before end %s", start.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	credentials := a.adapter.CheckCredentials(account)
	if !credentials.Ready {
		return result, fmt.Errorf("credentials unavailable: missing %s", strings.Join(credentials.Missing, ", "))
	}

	for windowStart := start; windowStart.Before(until); {
		windowEnd := windowStart.Add(maxWindow)
		if windowEnd.After(until) {
			windowEnd = until
		}
		pages, entries, err := a.syncWindow(ctx, account, windowStart, windowEnd)
		result.Pages += pages
		result.Entries += entries
		if err != nil {
			return result, err
		}
		windowStart = windowEnd
	}

	// A Checkpoint represents the whole Connected Account, not the last page
	// committed. Keeping it here makes retries overlap every incomplete window.
	if err := a.state.Set(ctx, string(a.exchange()), checkpointType, account.ID, until.Format(time.RFC3339Nano)); err != nil {
		return result, fmt.Errorf("write Checkpoint: %w", err)
	}
	return result, nil
}

func (a *Archive) syncStart(ctx context.Context, accountID string, explicitSince *time.Time, until time.Time, initialLookback time.Duration) (time.Time, error) {
	if explicitSince != nil {
		return explicitSince.UTC(), nil
	}
	record, ok, err := a.state.Get(ctx, string(a.exchange()), checkpointType, accountID)
	if err != nil {
		return time.Time{}, fmt.Errorf("read Checkpoint: %w", err)
	}
	if !ok {
		if initialLookback <= 0 {
			initialLookback = defaultLookback
		}
		return until.Add(-initialLookback), nil
	}
	checkpoint, err := time.Parse(time.RFC3339Nano, record.Value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse Checkpoint: %w", err)
	}
	return checkpoint.UTC().Add(-checkpointOverlap), nil
}

func (a *Archive) syncWindow(ctx context.Context, account source.Account, start, end time.Time) (int, int, error) {
	cursor := ""
	seen := map[string]struct{}{"": {}}
	pages := 0
	entries := 0
	for pages < maxPagesPerWindow {
		page, err := a.adapter.FetchPage(ctx, source.PageRequest{
			Account: account,
			Start:   start,
			End:     end,
			Cursor:  cursor,
		})
		if err != nil {
			return pages, entries, fmt.Errorf("fetch page for %s: %w", account.ID, err)
		}
		if err := a.upsertPage(ctx, account, page.Entries); err != nil {
			return pages, entries, err
		}
		pages++
		entries += len(page.Entries)
		if page.Done {
			return pages, entries, nil
		}
		// Provider pagination bugs must degrade the sync instead of spinning while
		// repeatedly committing the same idempotent rows.
		next := strings.TrimSpace(page.NextCursor)
		if next == "" {
			return pages, entries, errors.New("source returned an empty next cursor before completion")
		}
		if _, duplicate := seen[next]; duplicate {
			return pages, entries, fmt.Errorf("source repeated cursor %q", next)
		}
		seen[next] = struct{}{}
		cursor = next
	}
	return pages, entries, fmt.Errorf("source exceeded %d pages in one window", maxPagesPerWindow)
}

func (a *Archive) upsertPage(ctx context.Context, account source.Account, entries []model.LedgerEntry) error {
	return a.store.WithTx(ctx, func(tx *sql.Tx) error {
		for i := range entries {
			entry, err := a.normalizeEntry(account, entries[i])
			if err != nil {
				return fmt.Errorf("validate Ledger Entry %d: %w", i, err)
			}
			if _, err := tx.ExecContext(ctx, upsertLedgerEntry,
				entry.Exchange,
				entry.AccountID,
				entry.AccountLabel,
				entry.EntryID,
				entry.Symbol,
				entry.Category,
				entry.Type,
				entry.Asset,
				entry.Side,
				entry.Amount,
				entry.Fee,
				entry.Funding,
				entry.CashFlow,
				entry.Balance,
				entry.OrderID,
				entry.TradeID,
				entry.Info,
				entry.OccurredAt.UnixNano(),
				entry.ObservedAt.UnixNano(),
				string(entry.RawJSON),
			); err != nil {
				return fmt.Errorf("upsert Ledger Entry %q: %w", entry.EntryID, err)
			}
		}
		return nil
	})
}

func (a *Archive) normalizeEntry(account source.Account, entry model.LedgerEntry) (model.LedgerEntry, error) {
	if entry.Exchange == "" {
		entry.Exchange = a.exchange()
	}
	if entry.Exchange != a.exchange() {
		return model.LedgerEntry{}, fmt.Errorf("Exchange %q does not match adapter %q", entry.Exchange, a.exchange())
	}
	if entry.AccountID == "" {
		entry.AccountID = account.ID
	}
	if entry.AccountID != account.ID {
		return model.LedgerEntry{}, fmt.Errorf("Connected Account %q does not match request %q", entry.AccountID, account.ID)
	}
	if entry.AccountLabel == "" {
		entry.AccountLabel = account.Label
	}
	entry.EntryID = strings.TrimSpace(entry.EntryID)
	if entry.EntryID == "" {
		return model.LedgerEntry{}, errors.New("entry id is required")
	}
	if entry.OccurredAt.IsZero() {
		return model.LedgerEntry{}, errors.New("occurrence time is required")
	}
	entry.OccurredAt = entry.OccurredAt.UTC()
	if entry.ObservedAt.IsZero() {
		entry.ObservedAt = a.now().UTC()
	} else {
		entry.ObservedAt = entry.ObservedAt.UTC()
	}
	if len(entry.RawJSON) == 0 || !json.Valid(entry.RawJSON) {
		return model.LedgerEntry{}, errors.New("raw Exchange JSON is required and must be valid")
	}
	return entry, nil
}

const upsertLedgerEntry = `
insert into ledger_entries(
  exchange, account_id, account_label, entry_id, symbol, category, entry_type,
  asset, side, amount, fee, funding, cash_flow, balance, order_id, trade_id,
  info, occurred_at, observed_at, raw_json
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(exchange, account_id, entry_id) do update set
  account_label = excluded.account_label,
  symbol = excluded.symbol,
  category = excluded.category,
  entry_type = excluded.entry_type,
  asset = excluded.asset,
  side = excluded.side,
  amount = excluded.amount,
  fee = excluded.fee,
  funding = excluded.funding,
  cash_flow = excluded.cash_flow,
  balance = excluded.balance,
  order_id = excluded.order_id,
  trade_id = excluded.trade_id,
  info = excluded.info,
  occurred_at = excluded.occurred_at,
  observed_at = excluded.observed_at,
  raw_json = excluded.raw_json
`
