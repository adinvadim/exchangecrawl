package archive

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
)

type SyncRequest struct {
	AccountID       string
	Since           *time.Time
	Until           *time.Time
	InitialLookback time.Duration
}

type SyncReport struct {
	Exchange    model.Exchange      `json:"exchange"`
	StartedAt   time.Time           `json:"started_at"`
	FinishedAt  time.Time           `json:"finished_at"`
	Accounts    []AccountSyncReport `json:"accounts"`
	Pages       int                 `json:"pages"`
	Entries     int                 `json:"entries"`
	Transitions int                 `json:"transitions,omitempty"`
	Degraded    bool                `json:"degraded"`
}

type AccountSyncReport struct {
	AccountID   string    `json:"account_id"`
	Stream      string    `json:"stream,omitempty"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	Pages       int       `json:"pages"`
	Entries     int       `json:"entries"`
	Transitions int       `json:"transitions,omitempty"`
	Error       string    `json:"error,omitempty"`
}

type SyncTarget struct {
	AccountID string
	Stream    string
}

type DegradedSyncError struct {
	Failures map[SyncTarget]error
}

func (e *DegradedSyncError) Error() string {
	return fmt.Sprintf("Degraded Sync: %d Connected Account stream(s) failed", len(e.Failures))
}

func (a *Archive) Sync(ctx context.Context, request SyncRequest) (SyncReport, error) {
	startedAt := a.now().UTC()
	if err := validateArchiveTimestamp("current time", startedAt); err != nil {
		return SyncReport{}, err
	}
	until := startedAt
	if request.Until != nil {
		until = request.Until.UTC()
	}
	if err := validateArchiveTimestamp("sync end", until); err != nil {
		return SyncReport{}, err
	}
	if request.Since != nil {
		if err := validateArchiveTimestamp("sync start", request.Since.UTC()); err != nil {
			return SyncReport{}, err
		}
	}
	accounts, err := a.selectedAccounts(request.AccountID)
	if err != nil {
		return SyncReport{}, err
	}
	report := SyncReport{Exchange: a.exchange(), StartedAt: startedAt}
	failures := make(map[SyncTarget]error)

	for _, binding := range a.streams {
		for _, account := range accounts {
			accountReport, syncErr := a.syncAccountStream(ctx, binding, account, request.Since, until, request.InitialLookback)
			report.Accounts = append(report.Accounts, accountReport)
			report.Pages += accountReport.Pages
			report.Entries += accountReport.Entries
			report.Transitions += accountReport.Transitions
			if syncErr != nil {
				accountReport.Error = syncErr.Error()
				report.Accounts[len(report.Accounts)-1] = accountReport
				failures[SyncTarget{AccountID: account.ID, Stream: binding.Name}] = syncErr
			}
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

func (a *Archive) syncAccountStream(
	ctx context.Context,
	binding source.StreamBinding,
	account source.Account,
	explicitSince *time.Time,
	until time.Time,
	requestLookback time.Duration,
) (AccountSyncReport, error) {
	start, err := a.syncStart(ctx, binding, account.ID, explicitSince, until, requestLookback)
	reportStream := binding.Name
	// Preserve the V0 ledger report shape while identifying new stream targets.
	if len(a.streams) == 1 && binding.Name == "ledger" && binding.Stream == "ledger" {
		reportStream = ""
	}
	result := AccountSyncReport{AccountID: account.ID, Stream: reportStream, Start: start, End: until}
	if err != nil {
		return result, err
	}
	if err := validateArchiveTimestamp("sync start", start); err != nil {
		return result, err
	}
	if !start.Before(until) {
		return result, fmt.Errorf("sync start %s must be before end %s", start.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	credentials := streamCredentialStatus(binding, account)
	if !credentials.Ready {
		return result, fmt.Errorf("credentials unavailable: missing %s", strings.Join(credentials.Missing, ", "))
	}

	windowSize := binding.MaxWindow
	if windowSize == 0 {
		windowSize = maxWindow
	}
	for windowStart := start; windowStart.Before(until); {
		windowEnd := windowStart.Add(windowSize)
		if windowEnd.After(until) {
			windowEnd = until
		}
		pages, items, err := a.syncBindingWindow(ctx, binding, account, windowStart, windowEnd)
		result.Pages += pages
		if binding.Adapter != nil {
			result.Entries += items
		} else {
			result.Transitions += items
		}
		if err != nil {
			return result, err
		}
		windowStart = windowEnd
	}

	// A Checkpoint represents one Connected Account stream, not the last page
	// committed. Keeping it here makes retries overlap every incomplete window.
	if err := a.state.Set(ctx, string(a.exchange()), binding.Name, account.ID, until.Format(time.RFC3339Nano)); err != nil {
		return result, fmt.Errorf("write Checkpoint: %w", err)
	}
	return result, nil
}

func streamCredentialStatus(binding source.StreamBinding, account source.Account) source.CredentialStatus {
	if binding.Adapter != nil {
		return binding.Adapter.CheckCredentials(account)
	}
	return binding.Events.CheckCredentials(account)
}

func (a *Archive) syncStart(
	ctx context.Context,
	binding source.StreamBinding,
	accountID string,
	explicitSince *time.Time,
	until time.Time,
	requestLookback time.Duration,
) (time.Time, error) {
	if explicitSince != nil {
		return explicitSince.UTC(), nil
	}
	record, ok, err := a.state.Get(ctx, string(a.exchange()), binding.Name, accountID)
	if err != nil {
		return time.Time{}, fmt.Errorf("read Checkpoint: %w", err)
	}
	if !ok {
		lookback := binding.InitialLookback
		if lookback == 0 {
			lookback = requestLookback
		}
		if lookback <= 0 {
			lookback = defaultLookback
		}
		return until.Add(-lookback), nil
	}
	checkpoint, err := time.Parse(time.RFC3339Nano, record.Value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse Checkpoint: %w", err)
	}
	overlap := binding.CheckpointOverlap
	if overlap == 0 {
		overlap = checkpointOverlap
	}
	return checkpoint.UTC().Add(-overlap), nil
}

type pageEnvelope[T any] struct {
	items      []T
	nextCursor string
	done       bool
}

func (a *Archive) syncBindingWindow(
	ctx context.Context,
	binding source.StreamBinding,
	account source.Account,
	start time.Time,
	end time.Time,
) (int, int, error) {
	request := source.PageRequest{Account: account, Start: start, End: end}
	if binding.Adapter != nil {
		return syncWindow(ctx, request,
			func(ctx context.Context, request source.PageRequest) (pageEnvelope[model.LedgerEntry], error) {
				page, err := binding.Adapter.FetchPage(ctx, request)
				return pageEnvelope[model.LedgerEntry]{items: page.Entries, nextCursor: page.NextCursor, done: page.Done}, err
			},
			func(entries []model.LedgerEntry) error { return validatePageWindow(entries, start, end) },
			func(entry model.LedgerEntry) string { return strings.TrimSpace(entry.EntryID) },
			func(ctx context.Context, entries []model.LedgerEntry) (int, error) {
				return a.upsertPage(ctx, binding, account, entries)
			},
		)
	}
	return syncWindow(ctx, request,
		func(ctx context.Context, request source.PageRequest) (pageEnvelope[model.StateObservation], error) {
			page, err := binding.Events.FetchEventPage(ctx, request)
			if err == nil {
				observedAt := a.now().UTC()
				for index := range page.Observations {
					if page.Observations[index].ObservedAt.IsZero() {
						page.Observations[index].ObservedAt = observedAt
					}
				}
			}
			return pageEnvelope[model.StateObservation]{items: page.Observations, nextCursor: page.NextCursor, done: page.Done}, err
		},
		func(observations []model.StateObservation) error { return validateEventPage(observations) },
		func(observation model.StateObservation) string {
			return strings.Join([]string{
				strings.TrimSpace(observation.ObjectType),
				strings.TrimSpace(observation.ObjectID),
				strings.TrimSpace(observation.StateFingerprint),
			}, "\x00")
		},
		func(ctx context.Context, observations []model.StateObservation) (int, error) {
			return a.insertEventPage(ctx, binding, account, observations)
		},
	)
}

func syncWindow[T any](
	ctx context.Context,
	request source.PageRequest,
	fetch func(context.Context, source.PageRequest) (pageEnvelope[T], error),
	validate func([]T) error,
	identity func(T) string,
	commit func(context.Context, []T) (int, error),
) (int, int, error) {
	cursor := ""
	seen := map[string]struct{}{"": {}}
	seenPages := make(map[[sha256.Size]byte]struct{})
	pages := 0
	items := 0
	for pages < maxPagesPerWindow {
		request.Cursor = cursor
		page, err := fetch(ctx, request)
		if err != nil {
			return pages, items, fmt.Errorf("fetch page for %s: %w", request.Account.ID, err)
		}
		if err := validate(page.items); err != nil {
			return pages, items, err
		}
		// Empty pages are legitimate during multi-phase pagination: an adapter can
		// return zero items while it hands off between phases (e.g. Bybit category
		// linear -> inverse), and a quiet window can yield the same empty handoff
		// more than once. Every empty page hashes identically, so the duplicate
		// page-content guard must skip them or it would mistake a genuine
		// phase-handoff for a provider replay and permanently degrade the sync. A
		// real infinite loop of empty pages is still bounded below by the
		// repeated-cursor guard and the max-pages ceiling.
		if len(page.items) > 0 {
			fingerprint := pageIdentityFingerprint(page.items, identity)
			if _, duplicate := seenPages[fingerprint]; duplicate {
				return pages, items, errors.New("source repeated page content")
			}
			seenPages[fingerprint] = struct{}{}
		}
		committed, err := commit(ctx, page.items)
		if err != nil {
			return pages, items, err
		}
		pages++
		items += committed
		if page.done {
			return pages, items, nil
		}
		// Provider pagination bugs must degrade the sync instead of spinning while
		// repeatedly committing the same idempotent rows.
		next := page.nextCursor
		if strings.TrimSpace(next) == "" {
			return pages, items, errors.New("source returned an empty next cursor before completion")
		}
		if _, duplicate := seen[next]; duplicate {
			return pages, items, fmt.Errorf("source repeated cursor %q", next)
		}
		seen[next] = struct{}{}
		cursor = next
	}
	return pages, items, fmt.Errorf("source exceeded %d pages in one window", maxPagesPerWindow)
}

func validatePageWindow(entries []model.LedgerEntry, start, end time.Time) error {
	startMillis := start.UnixMilli()
	endMillis := end.UnixMilli()
	for index, entry := range entries {
		if entry.OccurredAt.IsZero() {
			return fmt.Errorf("source entry %d has no occurrence time", index)
		}
		if !fitsArchiveTimestamp(entry.OccurredAt) {
			return fmt.Errorf("source entry %d occurrence time is outside Archive timestamp range", index)
		}
		occurredMillis := entry.OccurredAt.UnixMilli()
		if occurredMillis < startMillis || occurredMillis > endMillis {
			return fmt.Errorf("source entry %d occurrence time is outside requested window", index)
		}
	}
	return nil
}

func validateEventPage(observations []model.StateObservation) error {
	for index, observation := range observations {
		if strings.TrimSpace(observation.ObjectType) == "" {
			return fmt.Errorf("source observation %d object type is required", index)
		}
		if strings.TrimSpace(observation.ObjectID) == "" {
			return fmt.Errorf("source observation %d object id is required", index)
		}
		if strings.TrimSpace(observation.Status) == "" {
			return fmt.Errorf("source observation %d status is required", index)
		}
		if strings.TrimSpace(observation.Stream) == "" {
			return fmt.Errorf("source observation %d stream is required", index)
		}
		if strings.TrimSpace(observation.StateFingerprint) == "" {
			return fmt.Errorf("source observation %d state fingerprint is required", index)
		}
		if observation.ObservedAt.IsZero() {
			return fmt.Errorf("source observation %d has no observation time", index)
		}
		if !fitsArchiveTimestamp(observation.ObservedAt) {
			return fmt.Errorf("source observation %d observation time is outside Archive timestamp range", index)
		}
		if !observation.OccurredAt.IsZero() && !fitsArchiveTimestamp(observation.OccurredAt) {
			return fmt.Errorf("source observation %d occurrence time is outside Archive timestamp range", index)
		}
	}
	return nil
}

func pageIdentityFingerprint[T any](items []T, identity func(T) string) [sha256.Size]byte {
	hash := sha256.New()
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(items)))
	_, _ = hash.Write(length[:])
	identities := make([]string, 0, len(items))
	for _, item := range items {
		identities = append(identities, identity(item))
	}
	sort.Strings(identities)
	for _, value := range identities {
		binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}

func (a *Archive) upsertPage(ctx context.Context, binding source.StreamBinding, account source.Account, entries []model.LedgerEntry) (int, error) {
	err := a.store.WithTx(ctx, func(tx *sql.Tx) error {
		for index := range entries {
			entry, err := a.normalizeEntry(account, entries[index])
			if err != nil {
				return fmt.Errorf("validate Ledger Entry %d: %w", index, err)
			}
			if _, err := tx.ExecContext(ctx, upsertLedgerEntry,
				entry.Exchange, entry.AccountID, entry.AccountLabel, binding.Stream,
				entry.EntryID, entry.Symbol, entry.Category, entry.Type, entry.Asset,
				entry.Side, entry.Amount, entry.Fee, entry.Funding, entry.CashFlow,
				entry.Balance, entry.OrderID, entry.TradeID, entry.Info,
				entry.OccurredAt.UnixNano(), entry.ObservedAt.UnixNano(), string(entry.RawJSON),
			); err != nil {
				return fmt.Errorf("upsert Ledger Entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(entries), nil
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
	if !fitsArchiveTimestamp(entry.OccurredAt) {
		return model.LedgerEntry{}, errors.New("occurrence time is outside Archive timestamp range")
	}
	entry.OccurredAt = entry.OccurredAt.UTC()
	if entry.ObservedAt.IsZero() {
		entry.ObservedAt = a.now().UTC()
	} else {
		entry.ObservedAt = entry.ObservedAt.UTC()
	}
	if !fitsArchiveTimestamp(entry.ObservedAt) {
		return model.LedgerEntry{}, errors.New("observation time is outside Archive timestamp range")
	}
	if len(entry.RawJSON) == 0 || !json.Valid(entry.RawJSON) {
		return model.LedgerEntry{}, errors.New("raw Exchange JSON is required and must be valid")
	}
	return entry, nil
}

func (a *Archive) insertEventPage(
	ctx context.Context,
	binding source.StreamBinding,
	account source.Account,
	observations []model.StateObservation,
) (int, error) {
	inserted := 0
	err := a.store.WithTx(ctx, func(tx *sql.Tx) error {
		for index := range observations {
			observation, err := a.normalizeObservation(binding, account, observations[index])
			if err != nil {
				return fmt.Errorf("validate State Observation %d: %w", index, err)
			}
			var latest string
			err = tx.QueryRowContext(ctx, `
select state_fingerprint
from state_transitions
where exchange = ? and account_id = ? and object_type = ? and object_id = ?
order by observed_at desc, id desc
limit 1`, observation.Exchange, observation.AccountID, observation.ObjectType, observation.ObjectID).Scan(&latest)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("read latest State Transition: %w", err)
			}
			if err == nil && latest == observation.StateFingerprint {
				continue
			}
			var occurredAt any
			if !observation.OccurredAt.IsZero() {
				occurredAt = observation.OccurredAt.UnixMilli()
			}
			result, err := tx.ExecContext(ctx, insertStateTransition,
				observation.Exchange, observation.AccountID, observation.Stream,
				observation.ObjectType, observation.ObjectID, observation.Status,
				observation.StateFingerprint, occurredAt, observation.ObservedAt.UnixMilli(),
				string(observation.RawJSON),
			)
			if err != nil {
				return fmt.Errorf("insert State Transition: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("count inserted State Transition: %w", err)
			}
			inserted += int(rows)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return inserted, nil
}

func (a *Archive) normalizeObservation(
	binding source.StreamBinding,
	account source.Account,
	observation model.StateObservation,
) (model.StateObservation, error) {
	if observation.Exchange == "" {
		observation.Exchange = a.exchange()
	}
	if observation.Exchange != a.exchange() {
		return model.StateObservation{}, fmt.Errorf("Exchange %q does not match adapter %q", observation.Exchange, a.exchange())
	}
	if observation.AccountID == "" {
		observation.AccountID = account.ID
	}
	if observation.AccountID != account.ID {
		return model.StateObservation{}, fmt.Errorf("Connected Account %q does not match request %q", observation.AccountID, account.ID)
	}
	if observation.AccountLabel == "" {
		observation.AccountLabel = account.Label
	}
	if observation.Stream != binding.Stream {
		return model.StateObservation{}, fmt.Errorf("stream %q does not match binding %q", observation.Stream, binding.Stream)
	}
	observation.ObjectType = strings.TrimSpace(observation.ObjectType)
	observation.ObjectID = strings.TrimSpace(observation.ObjectID)
	observation.Status = strings.TrimSpace(observation.Status)
	observation.StateFingerprint = strings.TrimSpace(observation.StateFingerprint)
	observation.ObservedAt = observation.ObservedAt.UTC()
	if !observation.OccurredAt.IsZero() {
		observation.OccurredAt = observation.OccurredAt.UTC()
	}
	if len(observation.RawJSON) == 0 || !json.Valid(observation.RawJSON) {
		return model.StateObservation{}, errors.New("raw Exchange JSON is required and must be valid")
	}
	return observation, nil
}

func fitsArchiveTimestamp(value time.Time) bool {
	return time.Unix(0, value.UnixNano()).Equal(value)
}

func validateArchiveTimestamp(name string, value time.Time) error {
	if !fitsArchiveTimestamp(value) {
		return fmt.Errorf("%s is outside Archive timestamp range", name)
	}
	return nil
}

const upsertLedgerEntry = `
insert into ledger_entries(
  exchange, account_id, account_label, stream, entry_id, symbol, category,
  entry_type, asset, side, amount, fee, funding, cash_flow, balance, order_id,
  trade_id, info, occurred_at, observed_at, raw_json
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(exchange, account_id, entry_id) do update set
  account_label = excluded.account_label,
  stream = excluded.stream,
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

const insertStateTransition = `
insert into state_transitions(
  exchange, account_id, stream, object_type, object_id, status,
  state_fingerprint, occurred_at, observed_at, raw_json
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict do nothing
`
