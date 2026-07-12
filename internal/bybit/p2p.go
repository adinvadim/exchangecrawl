package bybit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	p2pHistoryPath       = "/v5/p2p/order/simplifyList"
	p2pPendingPath       = "/v5/p2p/order/pending/simplifyList"
	p2pHistoryCursorKind = "history:"
	p2pPendingCursorKind = "pending:"
	p2pPageSize          = 30
	p2pMaxWindow         = 30 * 24 * time.Hour
	p2pRealtimeGrace     = 5 * time.Minute
	p2pFilteredPageLimit = 10_000
)

// P2POrdersAdapter observes Bybit P2P order state and archives terminal
// snapshots. It is a dual-role adapter: FetchEventPage emits every observed
// state as a StateObservation, while FetchPage emits only terminal orders as
// stable LedgerEntry rows keyed by the immutable P2P order id.
//
// CAVEAT: the signed V5 P2P endpoints only work for Verified/Block Advertiser
// accounts. A plain retail account gets a normal Bybit API error, which flows
// back through the redacted remote-error path so the runner reports this one
// stream unhealthy without crashing the others.
type P2POrdersAdapter struct {
	base *Adapter
}

// NewP2P builds a P2P adapter that shares the standard Bybit signing, retry, and
// base-URL validation implementation via the signed POST helper.
func NewP2P(options Options) (*P2POrdersAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &P2POrdersAdapter{base: base}, nil
}

func (a *P2POrdersAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *P2POrdersAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns only terminal P2P order snapshots. Their stable order
// identity makes repeated history-window upserts idempotent.
func (a *P2POrdersAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateP2PPageRequest(request); err != nil {
		return source.Page{}, err
	}
	page, err := parseP2PPageCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	seen := map[int]struct{}{page: {}}
	for scanned := 0; scanned < p2pFilteredPageLimit; scanned++ {
		result, observedAt, err := a.fetchOrderPage(ctx, request, p2pHistoryPath, p2pWindowBody(request, page))
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(result.Items))
		for _, raw := range result.Items {
			record, occurredAt, err := decodeP2POrder(raw)
			if err != nil {
				return source.Page{}, err
			}
			if !isTerminalP2POrderStatus(record.Status) {
				continue
			}
			entries = append(entries, normalizeTerminalP2POrder(raw, record, occurredAt, request.Account, observedAt))
		}
		done := len(result.Items) < p2pPageSize
		if len(entries) > 0 || done {
			next := ""
			if !done {
				next = strconv.Itoa(page + 1)
			}
			return source.Page{Entries: entries, NextCursor: next, Done: done}, nil
		}
		nextPage := page + 1
		if _, duplicate := seen[nextPage]; duplicate {
			return source.Page{}, errors.New("Bybit P2P order history repeated cursor")
		}
		seen[nextPage] = struct{}{}
		page = nextPage
	}
	return source.Page{}, fmt.Errorf("Bybit P2P order history exceeded %d filtered pages", p2pFilteredPageLimit)
}

// FetchEventPage polls durable history first, then the live pending snapshot.
// Adapter-owned cursor prefixes keep both provider phases in one archive stream.
func (a *P2POrdersAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateP2PPageRequest(request); err != nil {
		return source.EventPage{}, err
	}
	phase, page, err := parseP2POrderCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	if phase == "pending" {
		result, observedAt, err := a.fetchOrderPage(ctx, request, p2pPendingPath, p2pPageBody(page))
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeP2POrders(result.Items, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, err
		}
		next := ""
		if len(result.Items) >= p2pPageSize {
			next = p2pPendingCursorKind + strconv.Itoa(page+1)
		}
		return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
	}

	history, historyObservedAt, err := a.fetchOrderPage(ctx, request, p2pHistoryPath, p2pWindowBody(request, page))
	if err != nil {
		return source.EventPage{}, err
	}
	observations, err := normalizeP2POrders(history.Items, request.Account, historyObservedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	if len(history.Items) >= p2pPageSize {
		return source.EventPage{
			Observations: observations,
			NextCursor:   p2pHistoryCursorKind + strconv.Itoa(page+1),
		}, nil
	}
	if !shouldPollP2PRealtime(request, historyObservedAt) {
		return source.EventPage{Observations: observations, Done: true}, nil
	}

	pending, pendingObservedAt, err := a.fetchOrderPage(ctx, request, p2pPendingPath, p2pPageBody(1))
	if err != nil {
		return source.EventPage{}, err
	}
	pendingObservations, err := normalizeP2POrders(pending.Items, request.Account, pendingObservedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	observations = append(observations, pendingObservations...)
	next := ""
	if len(pending.Items) >= p2pPageSize {
		next = p2pPendingCursorKind + "2"
	}
	return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
}

func shouldPollP2PRealtime(request source.PageRequest, observedAt time.Time) bool {
	return !observedAt.Before(request.Start) && !observedAt.After(request.End.Add(p2pRealtimeGrace))
}

func (a *P2POrdersAdapter) fetchOrderPage(
	ctx context.Context,
	request source.PageRequest,
	path string,
	body []byte,
) (p2pResult, time.Time, error) {
	result, observedAt, err := a.base.fetchResultPOST(ctx, request.Account, path, body)
	if err != nil {
		return p2pResult{}, time.Time{}, err
	}
	var page p2pResult
	if err := json.Unmarshal(result, &page); err != nil {
		return p2pResult{}, time.Time{}, errors.New("decode Bybit P2P order result")
	}
	return page, observedAt, nil
}

// p2pWindowBody builds a signed history request body. The V5 P2P window filters
// are string millisecond fields; page/size drive the numeric pagination.
func p2pWindowBody(request source.PageRequest, page int) []byte {
	body, _ := json.Marshal(map[string]any{
		"beginTime": strconv.FormatInt(request.Start.UnixMilli(), 10),
		"endTime":   strconv.FormatInt(request.End.UnixMilli(), 10),
		"page":      page,
		"size":      p2pPageSize,
	})
	return body
}

func p2pPageBody(page int) []byte {
	body, _ := json.Marshal(map[string]any{
		"page": page,
		"size": p2pPageSize,
	})
	return body
}

func validateP2PPageRequest(request source.PageRequest) error {
	if strings.TrimSpace(request.Account.ID) == "" {
		return errors.New("Bybit Connected Account id is required")
	}
	if request.Start.IsZero() || request.End.IsZero() {
		return errors.New("Bybit page start and end are required")
	}
	if !request.Start.Before(request.End) {
		return errors.New("Bybit page start must be before end")
	}
	if request.End.Sub(request.Start) > p2pMaxWindow {
		return errors.New("Bybit P2P page range exceeds thirty days")
	}
	return nil
}

func parseP2POrderCursor(cursor string) (string, int, error) {
	if cursor == "" {
		return "history", 1, nil
	}
	if strings.HasPrefix(cursor, p2pHistoryCursorKind) {
		page, err := parseP2PPageNumber(strings.TrimPrefix(cursor, p2pHistoryCursorKind))
		return "history", page, err
	}
	if strings.HasPrefix(cursor, p2pPendingCursorKind) {
		page, err := parseP2PPageNumber(strings.TrimPrefix(cursor, p2pPendingCursorKind))
		return "pending", page, err
	}
	return "", 0, errors.New("Bybit P2P order cursor has invalid source prefix")
}

func parseP2PPageCursor(cursor string) (int, error) {
	if cursor == "" {
		return 1, nil
	}
	return parseP2PPageNumber(cursor)
}

func parseP2PPageNumber(raw string) (int, error) {
	page, err := strconv.Atoi(raw)
	if err != nil || page < 1 {
		return 0, errors.New("Bybit P2P order cursor has invalid page")
	}
	return page, nil
}

type p2pResult struct {
	Count int               `json:"count"`
	Items []json.RawMessage `json:"items"`
}

type p2pOrderRecord struct {
	ID                  string `json:"id"`
	Side                int    `json:"side"`
	TokenID             string `json:"tokenId"`
	CurrencyID          string `json:"currencyId"`
	OrderType           string `json:"orderType"`
	Amount              string `json:"amount"`
	Price               string `json:"price"`
	Fee                 string `json:"fee"`
	NotifyTokenQuantity string `json:"notifyTokenQuantity"`
	NotifyTokenID       string `json:"notifyTokenId"`
	Status              int    `json:"status"`
	CreateDate          string `json:"createDate"`
}

func normalizeP2POrders(
	rawOrders []json.RawMessage,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rawOrders))
	for _, raw := range rawOrders {
		record, occurredAt, err := decodeP2POrder(raw)
		if err != nil {
			return nil, err
		}
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        account.ID,
			AccountLabel:     account.Label,
			Stream:           "p2p",
			ObjectType:       "p2p_order",
			ObjectID:         "p2p:" + record.ID,
			Status:           strconv.Itoa(record.Status),
			StateFingerprint: p2pOrderFingerprint(record),
			Symbol:           p2pSymbol(record),
			Asset:            record.TokenID,
			Amount:           record.NotifyTokenQuantity,
			OccurredAt:       occurredAt,
			ObservedAt:       observedAt.UTC(),
			RawJSON:          append(json.RawMessage(nil), raw...),
		})
	}
	return observations, nil
}

func normalizeTerminalP2POrder(
	raw json.RawMessage,
	record p2pOrderRecord,
	occurredAt time.Time,
	account source.Account,
	observedAt time.Time,
) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "p2p:" + record.ID,
		Symbol:       p2pSymbol(record),
		Category:     "p2p",
		Type:         strconv.Itoa(record.Status),
		Asset:        record.TokenID,
		Side:         p2pSideLabel(record.Side),
		Amount:       record.NotifyTokenQuantity,
		Fee:          record.Fee,
		CashFlow:     record.Amount,
		OrderID:      record.ID,
		Info:         record.Price,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}
}

func decodeP2POrder(raw json.RawMessage) (p2pOrderRecord, time.Time, error) {
	var record p2pOrderRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return p2pOrderRecord{}, time.Time{}, errors.New("decode Bybit P2P order")
	}
	if strings.TrimSpace(record.ID) == "" {
		return p2pOrderRecord{}, time.Time{}, errors.New("Bybit P2P order has no id")
	}
	occurredAt, err := parseP2PTimestamp(record.CreateDate, "order createDate")
	if err != nil {
		return p2pOrderRecord{}, time.Time{}, err
	}
	return record, occurredAt, nil
}

func p2pSymbol(record p2pOrderRecord) string {
	return record.TokenID + record.CurrencyID
}

// p2pSideLabel maps the P2P taker side (0 buy, 1 sell) to a stable label,
// falling back to the raw numeric code for any undocumented value.
func p2pSideLabel(side int) string {
	switch side {
	case 0:
		return "Buy"
	case 1:
		return "Sell"
	default:
		return strconv.Itoa(side)
	}
}

// isTerminalP2POrderStatus mirrors isTerminalSpotOrder for the numeric P2P
// lifecycle: 40 cancelled, 50 finished, 80 exception-cancelled are terminal;
// every other code (5, 10, 20, 30, 60, 70, 90, 100, 110) is still in flight.
func isTerminalP2POrderStatus(status int) bool {
	switch status {
	case 40, 50, 80:
		return true
	default:
		return false
	}
}

func p2pOrderFingerprint(record p2pOrderRecord) string {
	fields := []string{
		strconv.Itoa(record.Status),
		record.Amount,
		record.Price,
		record.Fee,
		record.NotifyTokenQuantity,
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

func parseP2PTimestamp(raw, field string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit P2P %s is invalid", field)
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var (
	_ source.Adapter      = (*P2POrdersAdapter)(nil)
	_ source.EventAdapter = (*P2POrdersAdapter)(nil)
)
