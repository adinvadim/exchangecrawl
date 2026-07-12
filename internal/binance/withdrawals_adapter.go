package binance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	withdrawHistoryPath = "/sapi/v1/capital/withdraw/history"
	withdrawPageLimit   = 1000
	withdrawMaxWindow   = 90 * 24 * time.Hour
	withdrawTimeLayout  = "2006-01-02 15:04:05"
	// withdrawMaxScanPages bounds the terminal adapter's internal offset scan so
	// a provider that keeps returning full non-terminal pages cannot loop forever.
	withdrawMaxScanPages = 100_000
)

// WithdrawalsAdapter archives Binance withdrawal lifecycle. Each withdrawal is a
// mutable object whose status walks 0/2/4 (interim) toward 3 Rejected or 6
// Completed (terminal); FetchEventPage records every observed state, while the
// adapter returned by Terminal records only settled withdrawals as immutable
// ledger entries.
type WithdrawalsAdapter struct {
	client *Adapter
}

var (
	_ source.EventAdapter = (*WithdrawalsAdapter)(nil)
	_ source.Adapter      = (*withdrawalsTerminalAdapter)(nil)
)

// NewWithdrawals builds a withdrawals adapter over the SAPI wallet endpoint. Like
// NewSpot it defaults to https://api.binance.com (not the fapi base URL) and
// reuses the shared signing, retry, and base-URL validation of *Adapter.
func NewWithdrawals(options Options) (*WithdrawalsAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &WithdrawalsAdapter{client: client}, nil
}

func (a *WithdrawalsAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *WithdrawalsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

// Terminal exposes the settled-withdrawal ledger view of the same stream. Their
// stable id-based identity makes repeated window upserts idempotent.
func (a *WithdrawalsAdapter) Terminal() source.Adapter {
	return &withdrawalsTerminalAdapter{withdrawals: a}
}

func (a *WithdrawalsAdapter) credentials(account source.Account) (string, string, error) {
	apiKey, ok := a.client.credential(account.APIKeyEnv, defaultAPIKeyEnv)
	if !ok {
		return "", "", fmt.Errorf("Binance credential environment variable %s is missing", envName(account.APIKeyEnv, defaultAPIKeyEnv))
	}
	secret, ok := a.client.credential(account.APISecretEnv, defaultAPISecretEnv)
	if !ok {
		return "", "", fmt.Errorf("Binance credential environment variable %s is missing", envName(account.APISecretEnv, defaultAPISecretEnv))
	}
	return apiKey, secret, nil
}

func validateWithdrawWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance withdrawals page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > withdrawMaxWindow {
		return errors.New("Binance withdrawals page window must not exceed 90 days")
	}
	return nil
}

// FetchEventPage records every observed withdrawal state, including interim ones.
func (a *WithdrawalsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateWithdrawWindow(request); err != nil {
		return source.EventPage{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	offset, err := decodeWithdrawCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	rows, err := a.fetchWithdrawRows(ctx, request, offset, apiKey, secret)
	if err != nil {
		return source.EventPage{}, err
	}
	observedAt := a.client.now().UTC()
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeWithdrawObservation(row, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, fmt.Errorf("normalize Binance withdrawal row %d: %w", index, err)
		}
		observations = append(observations, observation)
	}
	page := source.EventPage{Observations: observations, Done: len(rows) < withdrawPageLimit}
	if !page.Done {
		page.NextCursor = encodeWithdrawCursor(offset + withdrawPageLimit)
	}
	return page, nil
}

type withdrawalsTerminalAdapter struct {
	withdrawals *WithdrawalsAdapter
}

func (a *withdrawalsTerminalAdapter) Exchange() model.Exchange {
	return a.withdrawals.Exchange()
}

func (a *withdrawalsTerminalAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.withdrawals.CheckCredentials(account)
}

// FetchPage returns only settled withdrawal snapshots. When a full page holds no
// terminal rows it keeps scanning forward so callers never see empty pages, and
// the offset advances monotonically so the scan cannot repeat a cursor.
func (a *withdrawalsTerminalAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateWithdrawWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.withdrawals.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	offset, err := decodeWithdrawCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	observedAt := a.withdrawals.client.now().UTC()
	for scanned := 0; scanned < withdrawMaxScanPages; scanned++ {
		rows, err := a.withdrawals.fetchWithdrawRows(ctx, request, offset, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(rows))
		for index, row := range rows {
			if !isTerminalWithdrawStatus(row.Status) {
				continue
			}
			// The /sapi withdraw history endpoint windows on applyTime (creation),
			// so membership is decided on applyTime: drop rows applied outside
			// [start,end]. A late completeTime must never smuggle a foreign-window
			// withdrawal in, nor push an in-window one out.
			applyAt, err := parseWithdrawTime(row.ApplyTime, "applyTime")
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize terminal Binance withdrawal row %d: %w", index, err)
			}
			if applyAt.Before(request.Start) || applyAt.After(request.End) {
				continue
			}
			entry, err := normalizeTerminalWithdraw(row, request.Account, request.Start, request.End, observedAt)
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize terminal Binance withdrawal row %d: %w", index, err)
			}
			entries = append(entries, entry)
		}
		fullPage := len(rows) == withdrawPageLimit
		if len(entries) != 0 || !fullPage {
			page := source.Page{Entries: entries, Done: !fullPage}
			if fullPage {
				page.NextCursor = encodeWithdrawCursor(offset + withdrawPageLimit)
			}
			return page, nil
		}
		offset += withdrawPageLimit
	}
	return source.Page{}, fmt.Errorf("Binance withdrawal scan exceeded %d pages", withdrawMaxScanPages)
}

func (a *WithdrawalsAdapter) fetchWithdrawRows(
	ctx context.Context,
	request source.PageRequest,
	offset int,
	apiKey string,
	secret string,
) ([]withdrawRow, error) {
	query := url.Values{
		"startTime":  {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":    {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"offset":     {strconv.Itoa(offset)},
		"limit":      {strconv.Itoa(withdrawPageLimit)},
		"recvWindow": {strconv.Itoa(a.client.recvWindow)},
	}
	var rows []withdrawRow
	if err := a.client.signedGETOperation(ctx, withdrawHistoryPath, query, apiKey, secret, "withdraw history", &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func decodeWithdrawCursor(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	if value != strings.TrimSpace(value) {
		return 0, errors.New("Binance withdrawals cursor must be a non-negative decimal offset")
	}
	offset, err := strconv.Atoi(value)
	if err != nil || offset < 0 {
		return 0, errors.New("Binance withdrawals cursor must be a non-negative decimal offset")
	}
	return offset, nil
}

func encodeWithdrawCursor(offset int) string {
	return strconv.Itoa(offset)
}

type withdrawRow struct {
	ID              string          `json:"id"`
	Amount          string          `json:"amount"`
	TransactionFee  string          `json:"transactionFee"`
	Coin            string          `json:"coin"`
	Status          int             `json:"status"`
	Address         string          `json:"address"`
	TxID            string          `json:"txId"`
	ApplyTime       string          `json:"applyTime"`
	Network         string          `json:"network"`
	TransferType    int             `json:"transferType"`
	WithdrawOrderID string          `json:"withdrawOrderId"`
	Info            string          `json:"info"`
	ConfirmNo       int             `json:"confirmNo"`
	WalletType      int             `json:"walletType"`
	CompleteTime    string          `json:"completeTime"`
	RawJSON         json.RawMessage `json:"-"`
}

func (row *withdrawRow) UnmarshalJSON(data []byte) error {
	type wire withdrawRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = withdrawRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func withdrawStatusText(status int) string {
	switch status {
	case 0:
		return "EmailSent"
	case 1:
		return "Cancelled"
	case 2:
		return "AwaitingApproval"
	case 3:
		return "Rejected"
	case 4:
		return "Processing"
	case 5:
		return "Failure"
	case 6:
		return "Completed"
	default:
		return "Unknown"
	}
}

func isTerminalWithdrawStatus(status int) bool {
	switch status {
	case 1, 3, 5, 6: // Cancelled, Rejected, Failure, Completed
		return true
	default:
		return false
	}
}

func transferTypeText(transferType int) string {
	switch transferType {
	case 0:
		return "external"
	case 1:
		return "internal"
	default:
		return "unknown"
	}
}

// withdrawOccurredAt prefers the settlement time and falls back to the apply time
// while a withdrawal is still interim (completeTime is empty until it settles).
func withdrawOccurredAt(row withdrawRow) (time.Time, error) {
	if strings.TrimSpace(row.CompleteTime) != "" {
		return parseWithdrawTime(row.CompleteTime, "completeTime")
	}
	return parseWithdrawTime(row.ApplyTime, "applyTime")
}

// clampWithdrawTime constrains a settlement time to the requested creation
// window so a terminal snapshot never falls outside [start,end].
func clampWithdrawTime(value, start, end time.Time) time.Time {
	if value.Before(start) {
		return start
	}
	if value.After(end) {
		return end
	}
	return value
}

func parseWithdrawTime(raw, field string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("Binance withdrawal %s is empty", field)
	}
	parsed, err := time.ParseInLocation(withdrawTimeLayout, raw, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("Binance withdrawal %s is invalid", field)
	}
	if parsed.UnixMilli() <= 0 || parsed.UnixMilli() > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Binance withdrawal %s is out of range", field)
	}
	return parsed.UTC(), nil
}

func withdrawFingerprint(row withdrawRow) string {
	state := struct {
		Status       int    `json:"status"`
		TxID         string `json:"txId"`
		CompleteTime string `json:"completeTime"`
	}{
		Status:       row.Status,
		TxID:         strings.TrimSpace(row.TxID),
		CompleteTime: strings.TrimSpace(row.CompleteTime),
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)
	return hex.EncodeToString(fingerprint[:])
}

func normalizeWithdrawObservation(row withdrawRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	id := strings.TrimSpace(row.ID)
	if id == "" {
		return model.StateObservation{}, errors.New("withdrawal row has no id")
	}
	occurredAt, err := withdrawOccurredAt(row)
	if err != nil {
		return model.StateObservation{}, err
	}
	return model.StateObservation{
		Exchange:         model.ExchangeBinance,
		AccountID:        account.ID,
		AccountLabel:     account.Label,
		Stream:           "asset",
		ObjectType:       "withdrawal",
		ObjectID:         id,
		Status:           withdrawStatusText(row.Status),
		StateFingerprint: withdrawFingerprint(row),
		Asset:            strings.TrimSpace(row.Coin),
		Amount:           strings.TrimSpace(row.Amount),
		OccurredAt:       occurredAt,
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func normalizeTerminalWithdraw(row withdrawRow, account source.Account, start, end, observedAt time.Time) (model.LedgerEntry, error) {
	id := strings.TrimSpace(row.ID)
	if id == "" {
		return model.LedgerEntry{}, errors.New("withdrawal row has no id")
	}
	occurredAt, err := withdrawOccurredAt(row)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	// A settled withdrawal is windowed on applyTime (creation), but its settlement
	// clock (completeTime) can tick past the window's end. Clamp the recorded
	// OccurredAt into [start,end] so the terminal snapshot lands inside the creation
	// window that archive.validatePageWindow enforces; without this the first
	// 365-day backfill wedges the stream. completeTime stays verbatim in RawJSON.
	occurredAt = clampWithdrawTime(occurredAt, start, end)
	return model.LedgerEntry{
		Exchange:     model.ExchangeBinance,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "withdraw:" + id,
		Category:     "withdrawal",
		Type:         withdrawStatusText(row.Status),
		Asset:        strings.TrimSpace(row.Coin),
		Side:         "OUT",
		Amount:       strings.TrimSpace(row.Amount),
		Fee:          strings.TrimSpace(row.TransactionFee),
		CashFlow:     strings.TrimSpace(row.Amount),
		OrderID:      strings.TrimSpace(row.WithdrawOrderID),
		TradeID:      strings.TrimSpace(row.TxID),
		Info:         transferTypeText(row.TransferType),
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
