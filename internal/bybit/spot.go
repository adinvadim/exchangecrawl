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
	spotExecutionPath     = "/v5/execution/list"
	spotOrderHistoryPath  = "/v5/order/history"
	spotOpenOrdersPath    = "/v5/order/realtime"
	spotHistoryCursorKind = "history:"
	spotOpenCursorKind    = "open:"
	spotRealtimeGrace     = 5 * time.Minute
	spotFilteredPageLimit = 10_000
)

// SpotFillsAdapter archives immutable spot executions.
type SpotFillsAdapter struct {
	base *Adapter
}

// SpotOrdersAdapter observes spot order state and archives terminal snapshots.
type SpotOrdersAdapter struct {
	base *Adapter
}

// NewSpot builds adapters that share the standard Bybit signing, retry, and
// base-URL validation implementation.
func NewSpot(options Options) (*SpotFillsAdapter, *SpotOrdersAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, nil, err
	}
	return &SpotFillsAdapter{base: base}, &SpotOrdersAdapter{base: base}, nil
}

func (a *SpotFillsAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *SpotFillsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

func (a *SpotFillsAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	query := spotWindowQuery(request, 100)
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, spotExecutionPath, query)
	if err != nil {
		return source.Page{}, err
	}
	var page spotResult
	if err := json.Unmarshal(result, &page); err != nil {
		return source.Page{}, errors.New("decode Bybit spot execution result")
	}
	entries := make([]model.LedgerEntry, 0, len(page.List))
	for _, raw := range page.List {
		entry, err := normalizeSpotExecution(raw, request.Account, observedAt)
		if err != nil {
			return source.Page{}, err
		}
		entries = append(entries, entry)
	}
	return source.Page{Entries: entries, NextCursor: page.NextPageCursor, Done: page.NextPageCursor == ""}, nil
}

func (a *SpotOrdersAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *SpotOrdersAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns only terminal order snapshots. Their stable order identity
// makes repeated history-window upserts idempotent.
func (a *SpotOrdersAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	cursor := request.Cursor
	seen := map[string]struct{}{cursor: {}}
	for pages := 0; pages < spotFilteredPageLimit; pages++ {
		page, observedAt, err := a.fetchHistoryPage(ctx, request, cursor)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(page.List))
		for _, raw := range page.List {
			record, _, err := decodeSpotOrder(raw)
			if err != nil {
				return source.Page{}, err
			}
			if !isTerminalSpotOrder(record.OrderStatus) {
				continue
			}
			createdAt, err := spotOrderCreatedAt(record)
			if err != nil {
				return source.Page{}, err
			}
			// The provider windows order history on creation time, so stamp the
			// terminal snapshot with createdTime (not the mutable updatedTime, which
			// stays in RawJSON). Clamp to [start,end] as defense-in-depth so a row
			// whose creation escaped the requested window never wedges the archive.
			if createdAt.Before(request.Start) || createdAt.After(request.End) {
				continue
			}
			entries = append(entries, normalizeTerminalSpotOrder(raw, record, createdAt, request.Account, observedAt))
		}
		if len(entries) > 0 || page.NextPageCursor == "" {
			return source.Page{Entries: entries, NextCursor: page.NextPageCursor, Done: page.NextPageCursor == ""}, nil
		}
		if _, duplicate := seen[page.NextPageCursor]; duplicate {
			return source.Page{}, errors.New("Bybit spot order history repeated cursor")
		}
		seen[page.NextPageCursor] = struct{}{}
		cursor = page.NextPageCursor
	}
	return source.Page{}, fmt.Errorf("Bybit spot order history exceeded %d filtered pages", spotFilteredPageLimit)
}

// FetchEventPage polls durable history first, then the live open-order snapshot.
// Adapter-owned cursor prefixes keep both provider cursors in one archive stream.
func (a *SpotOrdersAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validatePageRequest(request); err != nil {
		return source.EventPage{}, err
	}
	phase, cursor, err := parseSpotOrderCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	if phase == "open" {
		page, observedAt, err := a.fetchOpenPage(ctx, request, cursor)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeSpotOrders(page.List, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, err
		}
		next := ""
		if page.NextPageCursor != "" {
			next = spotOpenCursorKind + page.NextPageCursor
		}
		return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
	}

	history, historyObservedAt, err := a.fetchHistoryPage(ctx, request, cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	observations, err := normalizeSpotOrders(history.List, request.Account, historyObservedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	if history.NextPageCursor != "" {
		return source.EventPage{
			Observations: observations,
			NextCursor:   spotHistoryCursorKind + history.NextPageCursor,
		}, nil
	}
	if !shouldPollSpotRealtime(request, historyObservedAt) {
		return source.EventPage{Observations: observations, Done: true}, nil
	}

	open, openObservedAt, err := a.fetchOpenPage(ctx, request, "")
	if err != nil {
		return source.EventPage{}, err
	}
	openObservations, err := normalizeSpotOrders(open.List, request.Account, openObservedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	observations = append(observations, openObservations...)
	next := ""
	if open.NextPageCursor != "" {
		next = spotOpenCursorKind + open.NextPageCursor
	}
	return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
}

func shouldPollSpotRealtime(request source.PageRequest, observedAt time.Time) bool {
	return !observedAt.Before(request.Start) && !observedAt.After(request.End.Add(spotRealtimeGrace))
}

func (a *SpotOrdersAdapter) fetchHistoryPage(
	ctx context.Context,
	request source.PageRequest,
	cursor string,
) (spotResult, time.Time, error) {
	query := spotWindowQuery(request, 50)
	query.Del("cursor")
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return a.fetchOrderPage(ctx, request.Account, spotOrderHistoryPath, query)
}

func (a *SpotOrdersAdapter) fetchOpenPage(
	ctx context.Context,
	request source.PageRequest,
	cursor string,
) (spotResult, time.Time, error) {
	query := url.Values{"category": {"spot"}, "limit": {"50"}, "openOnly": {"1"}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return a.fetchOrderPage(ctx, request.Account, spotOpenOrdersPath, query)
}

func (a *SpotOrdersAdapter) fetchOrderPage(
	ctx context.Context,
	account source.Account,
	path string,
	query url.Values,
) (spotResult, time.Time, error) {
	result, observedAt, err := a.base.fetchResult(ctx, account, path, query)
	if err != nil {
		return spotResult{}, time.Time{}, err
	}
	var page spotResult
	if err := json.Unmarshal(result, &page); err != nil {
		return spotResult{}, time.Time{}, errors.New("decode Bybit spot order result")
	}
	return page, observedAt, nil
}

func spotWindowQuery(request source.PageRequest, limit int) url.Values {
	query := url.Values{
		"category":  {"spot"},
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(limit)},
	}
	if request.Cursor != "" {
		query.Set("cursor", request.Cursor)
	}
	return query
}

func parseSpotOrderCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "history", "", nil
	}
	if strings.HasPrefix(cursor, spotHistoryCursorKind) {
		return "history", strings.TrimPrefix(cursor, spotHistoryCursorKind), nil
	}
	if strings.HasPrefix(cursor, spotOpenCursorKind) {
		return "open", strings.TrimPrefix(cursor, spotOpenCursorKind), nil
	}
	return "", "", errors.New("Bybit spot order cursor has invalid source prefix")
}

type spotResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type spotExecutionRecord struct {
	Symbol        string `json:"symbol"`
	OrderID       string `json:"orderId"`
	Side          string `json:"side"`
	OrderType     string `json:"orderType"`
	ExecFee       string `json:"execFee"`
	ExecID        string `json:"execId"`
	ExecPrice     string `json:"execPrice"`
	ExecQty       string `json:"execQty"`
	ExecType      string `json:"execType"`
	ExecValue     string `json:"execValue"`
	ExecTime      string `json:"execTime"`
	FeeCurrency   string `json:"feeCurrency"`
	IsMaker       bool   `json:"isMaker"`
	OrderLinkID   string `json:"orderLinkId"`
	StopOrderType string `json:"stopOrderType"`
}

func normalizeSpotExecution(raw json.RawMessage, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	var record spotExecutionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.LedgerEntry{}, errors.New("decode Bybit spot execution")
	}
	if strings.TrimSpace(record.ExecID) == "" {
		return model.LedgerEntry{}, errors.New("Bybit spot execution has no execId")
	}
	occurredAt, err := parseSpotTimestamp(record.ExecTime, "execution execTime")
	if err != nil {
		return model.LedgerEntry{}, err
	}
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "spot:execution:" + record.ExecID,
		Symbol:       record.Symbol,
		Category:     "spot",
		Type:         record.ExecType,
		Asset:        record.FeeCurrency,
		Side:         record.Side,
		Amount:       record.ExecQty,
		Fee:          record.ExecFee,
		CashFlow:     record.ExecValue,
		OrderID:      record.OrderID,
		TradeID:      record.ExecID,
		Info:         record.OrderType,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}, nil
}

type spotOrderRecord struct {
	OrderID      string            `json:"orderId"`
	OrderLinkID  string            `json:"orderLinkId"`
	Symbol       string            `json:"symbol"`
	Side         string            `json:"side"`
	OrderType    string            `json:"orderType"`
	Price        string            `json:"price"`
	Qty          string            `json:"qty"`
	CumExecQty   string            `json:"cumExecQty"`
	CumExecValue string            `json:"cumExecValue"`
	CumExecFee   string            `json:"cumExecFee"`
	CumFeeDetail map[string]string `json:"cumFeeDetail"`
	OrderStatus  string            `json:"orderStatus"`
	CreatedTime  string            `json:"createdTime"`
	UpdatedTime  string            `json:"updatedTime"`
	AvgPrice     string            `json:"avgPrice"`
	LeavesQty    string            `json:"leavesQty"`
	FeeCurrency  string            `json:"feeCurrency"`
	CancelType   string            `json:"cancelType"`
	RejectReason string            `json:"rejectReason"`
}

func normalizeSpotOrders(
	rawOrders []json.RawMessage,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rawOrders))
	for _, raw := range rawOrders {
		record, occurredAt, err := decodeSpotOrder(raw)
		if err != nil {
			return nil, err
		}
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        account.ID,
			AccountLabel:     account.Label,
			Stream:           "spot",
			ObjectType:       "spot_order",
			ObjectID:         record.OrderID,
			Status:           record.OrderStatus,
			StateFingerprint: spotOrderFingerprint(record),
			Symbol:           record.Symbol,
			Asset:            record.FeeCurrency,
			Amount:           record.CumExecQty,
			OccurredAt:       occurredAt,
			ObservedAt:       observedAt.UTC(),
			RawJSON:          append(json.RawMessage(nil), raw...),
		})
	}
	return observations, nil
}

// spotOrderCreatedAt returns the order's creation time, the field the provider's
// history window filters on. Terminal ledger snapshots are stamped with it so
// each one lands in the window containing the order's creation, keeping the
// creation-time clamp lossless across checkpoint overlaps. It falls back to
// updatedTime only when createdTime is absent.
func spotOrderCreatedAt(record spotOrderRecord) (time.Time, error) {
	stamp := record.CreatedTime
	if strings.TrimSpace(stamp) == "" {
		stamp = record.UpdatedTime
	}
	return parseSpotTimestamp(stamp, "order createdTime")
}

func decodeSpotOrder(raw json.RawMessage) (spotOrderRecord, time.Time, error) {
	var record spotOrderRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return spotOrderRecord{}, time.Time{}, errors.New("decode Bybit spot order")
	}
	if strings.TrimSpace(record.OrderID) == "" {
		return spotOrderRecord{}, time.Time{}, errors.New("Bybit spot order has no orderId")
	}
	if strings.TrimSpace(record.OrderStatus) == "" {
		return spotOrderRecord{}, time.Time{}, errors.New("Bybit spot order has no orderStatus")
	}
	occurredAt, err := parseSpotTimestamp(record.UpdatedTime, "order updatedTime")
	if err != nil {
		return spotOrderRecord{}, time.Time{}, err
	}
	return record, occurredAt, nil
}

func spotOrderFingerprint(record spotOrderRecord) string {
	cumFeeDetail, _ := json.Marshal(record.CumFeeDetail)
	fields := []string{
		record.OrderStatus,
		record.Price,
		record.Qty,
		record.CumExecQty,
		record.CumExecValue,
		record.CumExecFee,
		string(cumFeeDetail),
		record.LeavesQty,
		record.AvgPrice,
		record.UpdatedTime,
		record.CancelType,
		record.RejectReason,
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

func normalizeTerminalSpotOrder(
	raw json.RawMessage,
	record spotOrderRecord,
	occurredAt time.Time,
	account source.Account,
	observedAt time.Time,
) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "spot:order:" + record.OrderID,
		Symbol:       record.Symbol,
		Category:     "spot",
		Type:         record.OrderStatus,
		Asset:        record.FeeCurrency,
		Side:         record.Side,
		Amount:       record.CumExecQty,
		Fee:          record.CumExecFee,
		CashFlow:     record.CumExecValue,
		OrderID:      record.OrderID,
		Info:         record.OrderType,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}
}

func isTerminalSpotOrder(status string) bool {
	switch status {
	case "Filled", "Cancelled", "PartiallyFilledCanceled", "Rejected", "Deactivated":
		return true
	default:
		return false
	}
}

func parseSpotTimestamp(raw, field string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit spot %s is invalid", field)
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var (
	_ source.Adapter      = (*SpotFillsAdapter)(nil)
	_ source.Adapter      = (*SpotOrdersAdapter)(nil)
	_ source.EventAdapter = (*SpotOrdersAdapter)(nil)
)
