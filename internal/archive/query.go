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
	"github.com/openclaw/crawlkit/store"
)

const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
)

type EntryQuery struct {
	AccountID string
	Stream    string
	Symbol    string
	Type      string
	Since     *time.Time
	Until     *time.Time
	Limit     int
}

type SearchQuery struct {
	Text      string
	AccountID string
	Stream    string
	Limit     int
}

type EventQuery struct {
	AccountID  string
	Stream     string
	ObjectType string
	ObjectID   string
	Status     string
	Since      *time.Time
	Until      *time.Time
	Limit      int
}

type Status struct {
	Exchange        model.Exchange  `json:"exchange"`
	Path            string          `json:"path"`
	EntryCount      int64           `json:"entry_count"`
	TransitionCount int64           `json:"transition_count"`
	AccountCount    int             `json:"account_count"`
	LastSyncAt      *time.Time      `json:"last_sync_at,omitempty"`
	Accounts        []AccountStatus `json:"accounts"`
}

type AccountStatus struct {
	AccountID  string     `json:"account_id"`
	Label      string     `json:"label"`
	Stream     string     `json:"stream"`
	Checkpoint *time.Time `json:"checkpoint,omitempty"`
}

func (a *Archive) Entries(ctx context.Context, query EntryQuery) ([]model.LedgerEntry, error) {
	where := []string{"exchange = ?"}
	args := []any{a.exchange()}
	if value := strings.TrimSpace(query.AccountID); value != "" {
		where = append(where, "account_id = ?")
		args = append(args, value)
	}
	if value := strings.TrimSpace(query.Stream); value != "" {
		where = append(where, "stream = ?")
		args = append(args, value)
	}
	if value := strings.TrimSpace(query.Symbol); value != "" {
		where = append(where, "symbol = ?")
		args = append(args, value)
	}
	if value := strings.TrimSpace(query.Type); value != "" {
		where = append(where, "entry_type = ?")
		args = append(args, value)
	}
	if query.Since != nil {
		if err := validateArchiveTimestamp("entries start", query.Since.UTC()); err != nil {
			return nil, err
		}
		where = append(where, "occurred_at >= ?")
		args = append(args, query.Since.UTC().UnixNano())
	}
	if query.Until != nil {
		if err := validateArchiveTimestamp("entries end", query.Until.UTC()); err != nil {
			return nil, err
		}
		where = append(where, "occurred_at <= ?")
		args = append(args, query.Until.UTC().UnixNano())
	}
	limit, err := queryLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	args = append(args, limit)
	rows, err := a.store.DB().QueryContext(ctx, selectLedgerEntries+" where "+strings.Join(where, " and ")+ledgerOrder+" limit ?", args...)
	if err != nil {
		return nil, fmt.Errorf("query Ledger Entries: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows)
}

func (a *Archive) Search(ctx context.Context, query SearchQuery) ([]model.LedgerEntry, error) {
	terms := store.FTS5TokenQuery(query.Text)
	if terms == "" {
		return nil, errors.New("search query must contain a letter, number, or underscore")
	}
	limit, err := queryLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	where := []string{"ledger_entries_fts match ?", "e.exchange = ?"}
	args := []any{terms, a.exchange()}
	if value := strings.TrimSpace(query.AccountID); value != "" {
		where = append(where, "e.account_id = ?")
		args = append(args, value)
	}
	if value := strings.TrimSpace(query.Stream); value != "" {
		where = append(where, "e.stream = ?")
		args = append(args, value)
	}
	args = append(args, limit)
	rows, err := a.store.DB().QueryContext(ctx,
		selectLedgerEntriesFromFTS+" where "+strings.Join(where, " and ")+ledgerOrderWithAlias+" limit ?",
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("search Ledger Entries: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows)
}

func (a *Archive) Events(ctx context.Context, query EventQuery) ([]model.StateObservation, error) {
	where := []string{"exchange = ?"}
	args := []any{a.exchange()}
	filters := []struct {
		column string
		value  string
	}{
		{column: "account_id", value: query.AccountID},
		{column: "stream", value: query.Stream},
		{column: "object_type", value: query.ObjectType},
		{column: "object_id", value: query.ObjectID},
		{column: "status", value: query.Status},
	}
	for _, filter := range filters {
		if value := strings.TrimSpace(filter.value); value != "" {
			where = append(where, filter.column+" = ?")
			args = append(args, value)
		}
	}
	if query.Since != nil {
		if err := validateArchiveTimestamp("events start", query.Since.UTC()); err != nil {
			return nil, err
		}
		where = append(where, "observed_at >= ?")
		args = append(args, query.Since.UTC().UnixMilli())
	}
	if query.Until != nil {
		if err := validateArchiveTimestamp("events end", query.Until.UTC()); err != nil {
			return nil, err
		}
		where = append(where, "observed_at <= ?")
		args = append(args, query.Until.UTC().UnixMilli())
	}
	limit, err := queryLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	args = append(args, limit)
	rows, err := a.store.DB().QueryContext(ctx,
		selectStateTransitions+" where "+strings.Join(where, " and ")+" order by observed_at desc, id desc limit ?",
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query State Transitions: %w", err)
	}
	defer rows.Close()
	observations, err := scanObservations(rows)
	if err != nil {
		return nil, fmt.Errorf("query State Transitions: %w", err)
	}
	labels := make(map[string]string, len(a.accounts))
	for _, account := range a.accounts {
		labels[account.ID] = account.Label
	}
	for index := range observations {
		observations[index].AccountLabel = labels[observations[index].AccountID]
	}
	return observations, nil
}

func (a *Archive) Status(ctx context.Context) (Status, error) {
	status := Status{
		Exchange:     a.exchange(),
		Path:         a.store.Path(),
		AccountCount: len(a.accounts),
		Accounts:     make([]AccountStatus, 0, len(a.accounts)*len(a.streams)),
	}
	if err := a.store.DB().QueryRowContext(ctx, "select count(*) from ledger_entries where exchange = ?", a.exchange()).Scan(&status.EntryCount); err != nil {
		return Status{}, fmt.Errorf("count Ledger Entries: %w", err)
	}
	if err := a.store.DB().QueryRowContext(ctx, "select count(*) from state_transitions where exchange = ?", a.exchange()).Scan(&status.TransitionCount); err != nil {
		return Status{}, fmt.Errorf("count State Transitions: %w", err)
	}
	for _, account := range a.accounts {
		for _, binding := range a.streams {
			accountStatus := AccountStatus{AccountID: account.ID, Label: account.Label, Stream: binding.Name}
			record, ok, err := a.state.Get(ctx, string(a.exchange()), binding.Name, account.ID)
			if err != nil {
				return Status{}, fmt.Errorf("read Checkpoint for %s/%s: %w", account.ID, binding.Name, err)
			}
			if ok {
				checkpoint, err := time.Parse(time.RFC3339Nano, record.Value)
				if err != nil {
					return Status{}, fmt.Errorf("parse Checkpoint for %s/%s: %w", account.ID, binding.Name, err)
				}
				checkpoint = checkpoint.UTC()
				accountStatus.Checkpoint = &checkpoint
				if status.LastSyncAt == nil || checkpoint.After(*status.LastSyncAt) {
					latest := checkpoint
					status.LastSyncAt = &latest
				}
			}
			status.Accounts = append(status.Accounts, accountStatus)
		}
	}
	return status, nil
}

func scanObservations(rows *sql.Rows) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0)
	for rows.Next() {
		var observation model.StateObservation
		var occurredAt sql.NullInt64
		var observedAt int64
		var rawJSON string
		if err := rows.Scan(
			&observation.Exchange,
			&observation.AccountID,
			&observation.Stream,
			&observation.ObjectType,
			&observation.ObjectID,
			&observation.Status,
			&observation.StateFingerprint,
			&occurredAt,
			&observedAt,
			&rawJSON,
		); err != nil {
			return nil, err
		}
		if occurredAt.Valid {
			observation.OccurredAt = time.UnixMilli(occurredAt.Int64).UTC()
		}
		observation.ObservedAt = time.UnixMilli(observedAt).UTC()
		observation.RawJSON = json.RawMessage(rawJSON)
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return observations, nil
}

func queryLimit(value int) (int, error) {
	if value == 0 {
		return defaultQueryLimit, nil
	}
	if value < 0 || value > maxQueryLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", maxQueryLimit)
	}
	return value, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanEntry(row rowScanner) (model.LedgerEntry, error) {
	var entry model.LedgerEntry
	var occurredAt int64
	var observedAt int64
	var rawJSON string
	if err := row.Scan(
		&entry.Exchange,
		&entry.AccountID,
		&entry.AccountLabel,
		&entry.EntryID,
		&entry.Symbol,
		&entry.Category,
		&entry.Type,
		&entry.Asset,
		&entry.Side,
		&entry.Amount,
		&entry.Fee,
		&entry.Funding,
		&entry.CashFlow,
		&entry.Balance,
		&entry.OrderID,
		&entry.TradeID,
		&entry.Info,
		&occurredAt,
		&observedAt,
		&rawJSON,
	); err != nil {
		return model.LedgerEntry{}, err
	}
	entry.OccurredAt = time.Unix(0, occurredAt).UTC()
	entry.ObservedAt = time.Unix(0, observedAt).UTC()
	entry.RawJSON = json.RawMessage(rawJSON)
	return entry, nil
}

func scanEntries(rows *sql.Rows) ([]model.LedgerEntry, error) {
	entries := make([]model.LedgerEntry, 0)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

const selectLedgerEntries = `
select exchange, account_id, account_label, entry_id, symbol, category,
       entry_type, asset, side, amount, fee, funding, cash_flow, balance,
       order_id, trade_id, info, occurred_at, observed_at, raw_json
from ledger_entries`

const selectLedgerEntriesFromFTS = `
select e.exchange, e.account_id, e.account_label, e.entry_id, e.symbol,
       e.category, e.entry_type, e.asset, e.side, e.amount, e.fee, e.funding,
       e.cash_flow, e.balance, e.order_id, e.trade_id, e.info, e.occurred_at,
       e.observed_at, e.raw_json
from ledger_entries_fts
join ledger_entries e on e.id = ledger_entries_fts.rowid`

const selectStateTransitions = `
select exchange, account_id, stream, object_type, object_id, status,
       state_fingerprint, occurred_at, observed_at, raw_json
from state_transitions`

const ledgerOrder = " order by occurred_at desc, account_id asc, entry_id desc"
const ledgerOrderWithAlias = " order by e.occurred_at desc, e.account_id asc, e.entry_id desc"
