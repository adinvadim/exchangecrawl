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
	depositOnchainPath        = "/v5/asset/deposit/query-record"
	depositInternalPath       = "/v5/asset/deposit/query-internal-record"
	depositOnchainCursorKind  = "onchain:"
	depositInternalCursorKind = "internal:"
	depositPhaseOnchain       = "onchain"
	depositPhaseInternal      = "internal"
	depositsPageLimit         = 50
	depositsMaxWindow         = 30 * 24 * time.Hour
)

// DepositsAdapter observes deposit state from Bybit. Deposits stay mutable until
// terminal (status, confirmations, and depositType move), so each observation is
// a StateObservation rather than an immutable ledger entry. One adapter drains
// both the on-chain and internal-transfer endpoints under a single archive stream
// using a phase-prefixed cursor ("onchain:<cursor>" then "internal:<cursor>").
type DepositsAdapter struct {
	base *Adapter
}

// NewDeposits builds a deposits adapter that shares the standard Bybit signing,
// retry, and base-URL validation implementation.
func NewDeposits(options Options) (*DepositsAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &DepositsAdapter{base: base}, nil
}

func (a *DepositsAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *DepositsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchEventPage drains the on-chain endpoint first, then the internal-transfer
// endpoint. The two endpoints have separate id namespaces, so the phase prefix on
// the cursor keeps both provider cursors in one archive stream. Repeated provider
// cursors are rejected to avoid infinite pagination loops.
func (a *DepositsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateDepositsRequest(request); err != nil {
		return source.EventPage{}, err
	}
	phase, cursor, err := parseDepositsCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}

	if phase == depositPhaseInternal {
		page, observedAt, err := a.fetchDepositPage(ctx, request, depositInternalPath, cursor)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeInternalDeposits(page.Rows, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, err
		}
		next, err := depositNextCursor(depositInternalCursorKind, cursor, page.NextPageCursor)
		if err != nil {
			return source.EventPage{}, err
		}
		return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
	}

	page, observedAt, err := a.fetchDepositPage(ctx, request, depositOnchainPath, cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	observations, err := normalizeOnchainDeposits(page.Rows, request.Account, observedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	if page.NextPageCursor != "" {
		next, err := depositNextCursor(depositOnchainCursorKind, cursor, page.NextPageCursor)
		if err != nil {
			return source.EventPage{}, err
		}
		return source.EventPage{Observations: observations, NextCursor: next}, nil
	}

	// On-chain records are exhausted; drain the first internal page in the same
	// call so the archive stream flows straight into the internal phase.
	internal, internalObservedAt, err := a.fetchDepositPage(ctx, request, depositInternalPath, "")
	if err != nil {
		return source.EventPage{}, err
	}
	internalObservations, err := normalizeInternalDeposits(internal.Rows, request.Account, internalObservedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	observations = append(observations, internalObservations...)
	next, err := depositNextCursor(depositInternalCursorKind, "", internal.NextPageCursor)
	if err != nil {
		return source.EventPage{}, err
	}
	return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
}

func (a *DepositsAdapter) fetchDepositPage(
	ctx context.Context,
	request source.PageRequest,
	path string,
	cursor string,
) (depositResult, time.Time, error) {
	query := depositsWindowQuery(request, cursor)
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, path, query)
	if err != nil {
		return depositResult{}, time.Time{}, err
	}
	var page depositResult
	if err := json.Unmarshal(result, &page); err != nil {
		return depositResult{}, time.Time{}, errors.New("decode Bybit deposit result")
	}
	return page, observedAt, nil
}

func depositsWindowQuery(request source.PageRequest, cursor string) url.Values {
	query := url.Values{
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(depositsPageLimit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return query
}

func parseDepositsCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return depositPhaseOnchain, "", nil
	}
	if strings.HasPrefix(cursor, depositOnchainCursorKind) {
		return depositPhaseOnchain, strings.TrimPrefix(cursor, depositOnchainCursorKind), nil
	}
	if strings.HasPrefix(cursor, depositInternalCursorKind) {
		return depositPhaseInternal, strings.TrimPrefix(cursor, depositInternalCursorKind), nil
	}
	return "", "", errors.New("Bybit deposits cursor has invalid source prefix")
}

// depositNextCursor prefixes the provider cursor with its phase and rejects a
// provider that returns the same cursor it was handed, which would loop forever.
func depositNextCursor(kind, current, providerNext string) (string, error) {
	if providerNext == "" {
		return "", nil
	}
	if providerNext == current {
		return "", errors.New("Bybit deposits repeated cursor")
	}
	return kind + providerNext, nil
}

func validateDepositsRequest(request source.PageRequest) error {
	if strings.TrimSpace(request.Account.ID) == "" {
		return errors.New("Bybit Connected Account id is required")
	}
	if request.Start.IsZero() || request.End.IsZero() {
		return errors.New("Bybit page start and end are required")
	}
	if !request.Start.Before(request.End) {
		return errors.New("Bybit page start must be before end")
	}
	if request.End.Sub(request.Start) > depositsMaxWindow {
		return errors.New("Bybit deposits page range exceeds 30 days")
	}
	return nil
}

type depositResult struct {
	Rows           []json.RawMessage `json:"rows"`
	NextPageCursor string            `json:"nextPageCursor"`
}

// flexNumber decodes a JSON field that Bybit may emit as either a number or a
// string (deposit status and depositType) into its string form without loss.
type flexNumber string

func (n *flexNumber) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "null" {
		*n = ""
		return nil
	}
	*n = flexNumber(strings.Trim(text, `"`))
	return nil
}

type onchainDepositRecord struct {
	Coin          string     `json:"coin"`
	Amount        string     `json:"amount"`
	TxID          string     `json:"txID"`
	Status        flexNumber `json:"status"`
	SuccessAt     string     `json:"successAt"`
	Confirmations string     `json:"confirmations"`
	DepositType   flexNumber `json:"depositType"`
}

type internalDepositRecord struct {
	ID          string     `json:"id"`
	Coin        string     `json:"coin"`
	Amount      string     `json:"amount"`
	Status      flexNumber `json:"status"`
	CreatedTime string     `json:"createdTime"`
}

func normalizeOnchainDeposits(
	rows []json.RawMessage,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for _, raw := range rows {
		var record onchainDepositRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, errors.New("decode Bybit on-chain deposit")
		}
		if strings.TrimSpace(record.TxID) == "" {
			return nil, errors.New("Bybit on-chain deposit has no txID")
		}
		occurredAt, err := parseDepositTimestamp(record.SuccessAt, "on-chain deposit successAt")
		if err != nil {
			return nil, err
		}
		observations = append(observations, model.StateObservation{
			Exchange:     model.ExchangeBybit,
			AccountID:    account.ID,
			AccountLabel: account.Label,
			Stream:       "asset",
			ObjectType:   "deposit",
			ObjectID:     "deposit:onchain:" + record.TxID,
			Status:       string(record.Status),
			StateFingerprint: depositFingerprint(
				string(record.Status),
				record.Confirmations,
				string(record.DepositType),
			),
			Asset:      record.Coin,
			Amount:     record.Amount,
			OccurredAt: occurredAt,
			ObservedAt: observedAt.UTC(),
			RawJSON:    append(json.RawMessage(nil), raw...),
		})
	}
	return observations, nil
}

func normalizeInternalDeposits(
	rows []json.RawMessage,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for _, raw := range rows {
		var record internalDepositRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, errors.New("decode Bybit internal deposit")
		}
		if strings.TrimSpace(record.ID) == "" {
			return nil, errors.New("Bybit internal deposit has no id")
		}
		occurredAt, err := parseDepositTimestamp(record.CreatedTime, "internal deposit createdTime")
		if err != nil {
			return nil, err
		}
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        account.ID,
			AccountLabel:     account.Label,
			Stream:           "asset",
			ObjectType:       "deposit",
			ObjectID:         "deposit:internal:" + record.ID,
			Status:           string(record.Status),
			StateFingerprint: depositFingerprint(string(record.Status)),
			Asset:            record.Coin,
			Amount:           record.Amount,
			OccurredAt:       occurredAt,
			ObservedAt:       observedAt.UTC(),
			RawJSON:          append(json.RawMessage(nil), raw...),
		})
	}
	return observations, nil
}

// parseDepositTimestamp treats an empty or zero value as "not set yet" (on-chain
// deposits carry no successAt until they clear), leaving OccurredAt zero.
func parseDepositTimestamp(raw, field string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return time.Time{}, nil
	}
	timestampMillis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit %s is invalid", field)
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

func depositFingerprint(fields ...string) string {
	hash := sha256.New()
	var length [8]byte
	for _, field := range fields {
		binary.LittleEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

var _ source.EventAdapter = (*DepositsAdapter)(nil)
