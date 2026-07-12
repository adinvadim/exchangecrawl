package bybit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	withdrawRecordPath        = "/v5/asset/withdraw/query-record"
	withdrawFilteredPageLimit = 10_000
	withdrawMaxWindow         = 30 * 24 * time.Hour
)

// WithdrawalsAdapter observes withdrawal state and archives terminal snapshots.
//
// A withdrawal mutates (SecurityCheck/Pending -> success/Reject/Fail/...), so it
// mirrors the SpotOrdersAdapter dual-role pattern: FetchEventPage records every
// observed snapshot as a StateObservation, while FetchPage exposes only terminal
// rows as stable, idempotent LedgerEntry records.
type WithdrawalsAdapter struct {
	base *Adapter
}

// NewWithdrawals builds a withdrawals adapter that shares the standard Bybit
// signing, retry, and base-URL validation implementation.
func NewWithdrawals(options Options) (*WithdrawalsAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &WithdrawalsAdapter{base: base}, nil
}

func (a *WithdrawalsAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *WithdrawalsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns only terminal withdrawal snapshots. Their stable withdrawId
// identity makes repeated history-window upserts idempotent. Non-terminal rows
// are skipped while draining nextPageCursor, guarded against repeated cursors.
func (a *WithdrawalsAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateWithdrawPageRequest(request); err != nil {
		return source.Page{}, err
	}
	cursor := request.Cursor
	seen := map[string]struct{}{cursor: {}}
	for pages := 0; pages < withdrawFilteredPageLimit; pages++ {
		page, observedAt, err := a.fetchWithdrawPage(ctx, request, cursor)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(page.Rows))
		for _, raw := range page.Rows {
			record, occurredAt, err := decodeWithdraw(raw)
			if err != nil {
				return source.Page{}, err
			}
			if !isTerminalWithdrawStatus(record.Status) {
				continue
			}
			entries = append(entries, normalizeTerminalWithdraw(raw, record, occurredAt, request.Account, observedAt))
		}
		if len(entries) > 0 || page.NextPageCursor == "" {
			return source.Page{Entries: entries, NextCursor: page.NextPageCursor, Done: page.NextPageCursor == ""}, nil
		}
		if _, duplicate := seen[page.NextPageCursor]; duplicate {
			return source.Page{}, errors.New("Bybit withdrawals repeated cursor")
		}
		seen[page.NextPageCursor] = struct{}{}
		cursor = page.NextPageCursor
	}
	return source.Page{}, fmt.Errorf("Bybit withdrawals exceeded %d filtered pages", withdrawFilteredPageLimit)
}

// FetchEventPage records every observed withdrawal snapshot, terminal or not, so
// the archive captures the full status progression of each withdrawal.
func (a *WithdrawalsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateWithdrawPageRequest(request); err != nil {
		return source.EventPage{}, err
	}
	page, observedAt, err := a.fetchWithdrawPage(ctx, request, request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	observations := make([]model.StateObservation, 0, len(page.Rows))
	for _, raw := range page.Rows {
		record, occurredAt, err := decodeWithdraw(raw)
		if err != nil {
			return source.EventPage{}, err
		}
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        request.Account.ID,
			AccountLabel:     request.Account.Label,
			Stream:           "asset",
			ObjectType:       "withdrawal",
			ObjectID:         record.WithdrawID,
			Status:           record.Status,
			StateFingerprint: withdrawFingerprint(record),
			Asset:            record.Coin,
			Amount:           record.Amount,
			OccurredAt:       occurredAt,
			ObservedAt:       observedAt.UTC(),
			RawJSON:          append(json.RawMessage(nil), raw...),
		})
	}
	next := page.NextPageCursor
	return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
}

func (a *WithdrawalsAdapter) fetchWithdrawPage(
	ctx context.Context,
	request source.PageRequest,
	cursor string,
) (withdrawResult, time.Time, error) {
	query := withdrawWindowQuery(request, 50)
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, withdrawRecordPath, query)
	if err != nil {
		return withdrawResult{}, time.Time{}, err
	}
	var page withdrawResult
	if err := json.Unmarshal(result, &page); err != nil {
		return withdrawResult{}, time.Time{}, errors.New("decode Bybit withdrawals result")
	}
	return page, observedAt, nil
}

func withdrawWindowQuery(request source.PageRequest, limit int) url.Values {
	return url.Values{
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(limit)},
	}
}

type withdrawResult struct {
	Rows           []json.RawMessage `json:"rows"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type withdrawRecord struct {
	WithdrawID   string `json:"withdrawId"`
	Coin         string `json:"coin"`
	Chain        string `json:"chain"`
	Amount       string `json:"amount"`
	TxID         string `json:"txID"`
	Status       string `json:"status"`
	ToAddress    string `json:"toAddress"`
	Tag          string `json:"tag"`
	WithdrawFee  string `json:"withdrawFee"`
	CreateTime   string `json:"createTime"`
	UpdateTime   string `json:"updateTime"`
	WithdrawType int    `json:"withdrawType"`
}

func decodeWithdraw(raw json.RawMessage) (withdrawRecord, time.Time, error) {
	var record withdrawRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return withdrawRecord{}, time.Time{}, errors.New("decode Bybit withdrawal")
	}
	if strings.TrimSpace(record.WithdrawID) == "" {
		return withdrawRecord{}, time.Time{}, errors.New("Bybit withdrawal has no withdrawId")
	}
	if strings.TrimSpace(record.Status) == "" {
		return withdrawRecord{}, time.Time{}, errors.New("Bybit withdrawal has no status")
	}
	occurredAt, err := parseWithdrawTimestamp(record.CreateTime, "withdrawal createTime")
	if err != nil {
		return withdrawRecord{}, time.Time{}, err
	}
	return record, occurredAt, nil
}

func normalizeTerminalWithdraw(
	raw json.RawMessage,
	record withdrawRecord,
	occurredAt time.Time,
	account source.Account,
	observedAt time.Time,
) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "withdraw:" + record.WithdrawID,
		Category:     "withdrawals",
		Type:         record.Status,
		Asset:        record.Coin,
		Side:         "withdraw",
		Amount:       record.Amount,
		Fee:          record.WithdrawFee,
		OrderID:      record.WithdrawID,
		TradeID:      record.TxID,
		Info:         withdrawTypeLabel(record.WithdrawType),
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}
}

// withdrawTypeLabel distinguishes on-chain from internal withdrawals without a
// second endpoint. Bybit encodes 0 as on-chain and 1 as an internal transfer.
func withdrawTypeLabel(withdrawType int) string {
	switch withdrawType {
	case 0:
		return "on-chain"
	case 1:
		return "internal"
	default:
		return "withdrawType-" + strconv.Itoa(withdrawType)
	}
}

// isTerminalWithdrawStatus reports whether a withdrawal has reached a Bybit-final
// state. success/Reject/Fail/CancelByUser are unambiguously final. Bybit also
// reports BlockchainConfirmed once funds have left and the on-chain transaction
// is confirmed; from Bybit's accounting perspective that row no longer mutates,
// so it is treated as terminal. Pre-final states (SecurityCheck, Pending,
// MoreInformationRequired, Unknown) are skipped by FetchPage.
func isTerminalWithdrawStatus(status string) bool {
	switch status {
	case "success", "Reject", "Fail", "CancelByUser", "BlockchainConfirmed":
		return true
	default:
		return false
	}
}

func withdrawFingerprint(record withdrawRecord) string {
	fields := []string{
		record.Status,
		record.UpdateTime,
		record.TxID,
	}
	hash := sha256.New()
	var length [8]byte
	for _, field := range fields {
		binary.LittleEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func parseWithdrawTimestamp(raw, field string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit %s is invalid", field)
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

func validateWithdrawPageRequest(request source.PageRequest) error {
	if strings.TrimSpace(request.Account.ID) == "" {
		return errors.New("Bybit Connected Account id is required")
	}
	if request.Start.IsZero() || request.End.IsZero() {
		return errors.New("Bybit page start and end are required")
	}
	if !request.Start.Before(request.End) {
		return errors.New("Bybit page start must be before end")
	}
	if request.End.Sub(request.Start) >= withdrawMaxWindow {
		return errors.New("Bybit withdrawals page range must be under 30 days")
	}
	return nil
}

var (
	_ source.Adapter      = (*WithdrawalsAdapter)(nil)
	_ source.EventAdapter = (*WithdrawalsAdapter)(nil)
)
