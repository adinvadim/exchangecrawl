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
	futuresOrderHistoryPath  = "/v5/order/history"
	futuresOpenOrdersPath    = "/v5/order/realtime"
	futuresHistoryCursorKind = "history:"
	futuresOpenCursorKind    = "open:"
	futuresRealtimeGrace     = 5 * time.Minute
	futuresFilteredPageLimit = 10_000
)

// FuturesOrdersAdapter observes derivatives (linear + inverse) order state and
// archives terminal snapshots. It is a direct port of SpotOrdersAdapter to
// derivatives: it polls durable order history first, then the live open-order
// snapshot, iterating both the linear and inverse categories under one archive
// stream via phase- and category-prefixed cursors
// ("history:linear:<cursor>", "history:inverse:<cursor>", "open:linear:<cursor>",
// "open:inverse:<cursor>").
//
// Bybit only guarantees a 7-day window here, and even inside it retention is
// uneven: beyond 7 days only fully Filled orders remain queryable, and
// Cancelled/Rejected orders age out after roughly 24h. The poller must
// therefore run frequently so terminal snapshots are captured before the
// provider drops them.
type FuturesOrdersAdapter struct {
	base *Adapter
}

// NewFuturesOrders builds a derivatives order adapter that shares the standard
// Bybit signing, retry, and base-URL validation implementation.
func NewFuturesOrders(options Options) (*FuturesOrdersAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesOrdersAdapter{base: base}, nil
}

func (a *FuturesOrdersAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *FuturesOrdersAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns only terminal derivatives order snapshots. Their stable
// order identity makes repeated history-window upserts idempotent. It walks the
// linear category first, then the inverse category, filtering to terminal
// statuses and skipping pages that contain no terminal orders.
func (a *FuturesOrdersAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	category, cursor, err := parseFuturesTerminalCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	seen := map[string]struct{}{category + ":" + cursor: {}}
	for pages := 0; pages < futuresFilteredPageLimit; pages++ {
		page, observedAt, err := a.fetchHistoryPage(ctx, request, category, cursor)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(page.List))
		for _, raw := range page.List {
			record, _, err := decodeFuturesOrder(raw)
			if err != nil {
				return source.Page{}, err
			}
			if !isTerminalFuturesOrder(record.OrderStatus) {
				continue
			}
			createdAt, err := futuresOrderCreatedAt(record)
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
			entries = append(entries, normalizeTerminalFuturesOrder(raw, record, category, createdAt, request.Account, observedAt))
		}
		if len(entries) > 0 {
			next := futuresTerminalNextCursor(category, page.NextPageCursor)
			return source.Page{Entries: entries, NextCursor: next, Done: next == ""}, nil
		}
		if page.NextPageCursor != "" {
			key := category + ":" + page.NextPageCursor
			if _, duplicate := seen[key]; duplicate {
				return source.Page{}, errors.New("Bybit futures order history repeated cursor")
			}
			seen[key] = struct{}{}
			cursor = page.NextPageCursor
			continue
		}
		if category == futuresLinearCategory {
			category, cursor = futuresInverseCategory, ""
			key := category + ":" + cursor
			if _, duplicate := seen[key]; duplicate {
				return source.Page{}, errors.New("Bybit futures order history repeated cursor")
			}
			seen[key] = struct{}{}
			continue
		}
		return source.Page{Done: true}, nil
	}
	return source.Page{}, fmt.Errorf("Bybit futures order history exceeded %d filtered pages", futuresFilteredPageLimit)
}

// FetchEventPage polls durable history first (linear then inverse), then the
// live open-order snapshot for each category. Adapter-owned cursor prefixes
// keep every provider cursor in one archive stream.
func (a *FuturesOrdersAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validatePageRequest(request); err != nil {
		return source.EventPage{}, err
	}
	phase, category, cursor, err := parseFuturesOrderCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}

	if phase == "open" {
		page, observedAt, err := a.fetchOpenPage(ctx, request, category, cursor)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeFuturesOrders(page.List, category, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, err
		}
		next := futuresOpenNextCursor(category, page.NextPageCursor)
		return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
	}

	history, observedAt, err := a.fetchHistoryPage(ctx, request, category, cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	observations, err := normalizeFuturesOrders(history.List, category, request.Account, observedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	next, err := a.historyNextCursor(category, history.NextPageCursor, request, observedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
}

// historyNextCursor advances the event traversal: paginate the current category,
// then move linear -> inverse history, then (only when the window is fresh
// enough to hold live open orders) into the realtime open-order phase.
func (a *FuturesOrdersAdapter) historyNextCursor(
	category, providerCursor string,
	request source.PageRequest,
	observedAt time.Time,
) (string, error) {
	if providerCursor != "" {
		return futuresHistoryCursorKind + category + ":" + providerCursor, nil
	}
	if category == futuresLinearCategory {
		return futuresHistoryCursorKind + futuresInverseCategory + ":", nil
	}
	if shouldPollFuturesRealtime(request, observedAt) {
		return futuresOpenCursorKind + futuresLinearCategory + ":", nil
	}
	return "", nil
}

func shouldPollFuturesRealtime(request source.PageRequest, observedAt time.Time) bool {
	return !observedAt.Before(request.Start) && !observedAt.After(request.End.Add(futuresRealtimeGrace))
}

func futuresTerminalNextCursor(category, providerCursor string) string {
	if providerCursor != "" {
		return futuresHistoryCursorKind + category + ":" + providerCursor
	}
	if category == futuresLinearCategory {
		return futuresHistoryCursorKind + futuresInverseCategory + ":"
	}
	return ""
}

func futuresOpenNextCursor(category, providerCursor string) string {
	if providerCursor != "" {
		return futuresOpenCursorKind + category + ":" + providerCursor
	}
	if category == futuresLinearCategory {
		return futuresOpenCursorKind + futuresInverseCategory + ":"
	}
	return ""
}

func (a *FuturesOrdersAdapter) fetchHistoryPage(
	ctx context.Context,
	request source.PageRequest,
	category, cursor string,
) (spotResult, time.Time, error) {
	query := url.Values{
		"category":  {category},
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {"50"},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return a.fetchOrderPage(ctx, request.Account, futuresOrderHistoryPath, query)
}

func (a *FuturesOrdersAdapter) fetchOpenPage(
	ctx context.Context,
	request source.PageRequest,
	category, cursor string,
) (spotResult, time.Time, error) {
	query := url.Values{"category": {category}, "limit": {"50"}, "openOnly": {"1"}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return a.fetchOrderPage(ctx, request.Account, futuresOpenOrdersPath, query)
}

func (a *FuturesOrdersAdapter) fetchOrderPage(
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
		return spotResult{}, time.Time{}, errors.New("decode Bybit futures order result")
	}
	return page, observedAt, nil
}

func parseFuturesOrderCursor(cursor string) (phase, category, providerCursor string, err error) {
	if cursor == "" {
		return "history", futuresLinearCategory, "", nil
	}
	parts := strings.SplitN(cursor, ":", 3)
	if len(parts) != 3 {
		return "", "", "", errors.New("Bybit futures order cursor has invalid source prefix")
	}
	phase, category, providerCursor = parts[0], parts[1], parts[2]
	if phase != "history" && phase != "open" {
		return "", "", "", errors.New("Bybit futures order cursor has invalid source prefix")
	}
	if !isFuturesCategory(category) {
		return "", "", "", errors.New("Bybit futures order cursor has invalid category")
	}
	return phase, category, providerCursor, nil
}

func parseFuturesTerminalCursor(cursor string) (category, providerCursor string, err error) {
	if cursor == "" {
		return futuresLinearCategory, "", nil
	}
	phase, category, providerCursor, err := parseFuturesOrderCursor(cursor)
	if err != nil {
		return "", "", err
	}
	if phase != "history" {
		return "", "", errors.New("Bybit futures order terminal cursor must be a history cursor")
	}
	return category, providerCursor, nil
}

func isFuturesCategory(category string) bool {
	return category == futuresLinearCategory || category == futuresInverseCategory
}

func normalizeFuturesOrders(
	rawOrders []json.RawMessage,
	category string,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rawOrders))
	for _, raw := range rawOrders {
		record, occurredAt, err := decodeFuturesOrder(raw)
		if err != nil {
			return nil, err
		}
		objectID := futuresOrderIdentity(category, record.OrderID)
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        account.ID,
			AccountLabel:     account.Label,
			Stream:           "futures",
			ObjectType:       "futures_order",
			ObjectID:         objectID,
			Status:           record.OrderStatus,
			StateFingerprint: futuresOrderFingerprint(record),
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

// futuresOrderCreatedAt returns the order's creation time, the field the
// provider's history window filters on. Terminal ledger snapshots are stamped
// with it so each one lands in the window containing the order's creation,
// keeping the creation-time clamp lossless across checkpoint overlaps. It falls
// back to updatedTime only when createdTime is absent.
func futuresOrderCreatedAt(record spotOrderRecord) (time.Time, error) {
	stamp := record.CreatedTime
	if strings.TrimSpace(stamp) == "" {
		stamp = record.UpdatedTime
	}
	return parseFuturesTimestamp(stamp, "order createdTime")
}

func decodeFuturesOrder(raw json.RawMessage) (spotOrderRecord, time.Time, error) {
	var record spotOrderRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return spotOrderRecord{}, time.Time{}, errors.New("decode Bybit futures order")
	}
	if strings.TrimSpace(record.OrderID) == "" {
		return spotOrderRecord{}, time.Time{}, errors.New("Bybit futures order has no orderId")
	}
	if strings.TrimSpace(record.OrderStatus) == "" {
		return spotOrderRecord{}, time.Time{}, errors.New("Bybit futures order has no orderStatus")
	}
	occurredAt, err := parseFuturesTimestamp(record.UpdatedTime, "order updatedTime")
	if err != nil {
		return spotOrderRecord{}, time.Time{}, err
	}
	return record, occurredAt, nil
}

// futuresOrderFingerprint hashes the mutable execution progress fields called
// out by the stream spec: orderStatus, cumExecQty, cumExecValue, avgPrice,
// leavesQty, and updatedTime.
func futuresOrderFingerprint(record spotOrderRecord) string {
	fields := []string{
		record.OrderStatus,
		record.CumExecQty,
		record.CumExecValue,
		record.AvgPrice,
		record.LeavesQty,
		record.UpdatedTime,
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

func normalizeTerminalFuturesOrder(
	raw json.RawMessage,
	record spotOrderRecord,
	category string,
	occurredAt time.Time,
	account source.Account,
	observedAt time.Time,
) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      futuresOrderIdentity(category, record.OrderID),
		Symbol:       record.Symbol,
		Category:     category,
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

func futuresOrderIdentity(category, orderID string) string {
	return "futures-order:" + category + ":" + orderID
}

func isTerminalFuturesOrder(status string) bool {
	switch status {
	case "Filled", "Cancelled", "PartiallyFilledCanceled", "Rejected", "Deactivated":
		return true
	default:
		return false
	}
}

var (
	_ source.Adapter      = (*FuturesOrdersAdapter)(nil)
	_ source.EventAdapter = (*FuturesOrdersAdapter)(nil)
)
