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
	earnFlexibleOrderPath   = "/v5/earn/order"
	earnFixedTermOrderPath  = "/v5/earn/fixed-term/order"
	earnFlexibleCursorKind  = "flexible:"
	earnFixedTermCursorKind = "fixedterm:"
	earnFlexibleCategory    = "FlexibleSaving"
	earnFixedTermCategory   = "FixedTermSaving"
	earnMillisThreshold     = 1_000_000_000_000
	earnFilteredPageLimit   = 10_000
)

// EarnOrdersAdapter drains Bybit Stake/Redeem order history across the flexible
// (/v5/earn/order) and fixed-term (/v5/earn/fixed-term/order) endpoints. It both
// observes order state (source.EventAdapter) and archives settled orders plus the
// immutable fixed-term yield rows as ledger entries (source.Adapter, terminal).
// A single adapter-owned phase cursor ("flexible:<cursor>" then
// "fixedterm:<cursor>") keeps both provider cursors in one archive stream.
type EarnOrdersAdapter struct {
	base *Adapter
}

// NewEarn builds the earn-orders adapter, sharing the standard Bybit signing,
// retry, and base-URL validation implementation. Requires the API key's "Earn"
// permission; a permission failure surfaces as a normal redacted remote error.
func NewEarn(options Options) (*EarnOrdersAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &EarnOrdersAdapter{base: base}, nil
}

func (a *EarnOrdersAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *EarnOrdersAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchEventPage observes every earn order (any status) as a mutable object,
// walking the flexible endpoint first and then the fixed-term endpoint.
func (a *EarnOrdersAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validatePageRequest(request); err != nil {
		return source.EventPage{}, err
	}
	phase, cursor, err := parseEarnCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	path, defaultCategory := earnPhaseEndpoint(phase)
	page, observedAt, err := a.fetchOrderPage(ctx, request, path, cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	observations, err := normalizeEarnOrders(page.List, defaultCategory, request.Account, observedAt)
	if err != nil {
		return source.EventPage{}, err
	}
	if page.NextPageCursor != "" {
		return source.EventPage{
			Observations: observations,
			NextCursor:   earnCursorKind(phase) + page.NextPageCursor,
		}, nil
	}
	if phase == "flexible" {
		// Flexible history is drained; hand off to the fixed-term endpoint.
		return source.EventPage{Observations: observations, NextCursor: earnFixedTermCursorKind}, nil
	}
	return source.EventPage{Observations: observations, Done: true}, nil
}

// FetchPage returns only settled (terminal) order snapshots plus, for settled
// fixed-term Redeem orders, the immutable yieldInfoList rows. Their stable
// identities make repeated history-window upserts idempotent.
func (a *EarnOrdersAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	phase, cursor, err := parseEarnCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	seen := map[string]struct{}{request.Cursor: {}}
	for pages := 0; pages < earnFilteredPageLimit; pages++ {
		path, defaultCategory := earnPhaseEndpoint(phase)
		page, observedAt, err := a.fetchOrderPage(ctx, request, path, cursor)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(page.List))
		for _, raw := range page.List {
			record, _, err := decodeEarnOrder(raw)
			if err != nil {
				return source.Page{}, err
			}
			if !isSettledEarnOrder(record.Status) {
				continue
			}
			category := earnCategory(record, defaultCategory)
			createdAt, err := earnOrderCreatedAt(record)
			if err != nil {
				return source.Page{}, err
			}
			// The provider windows order history on creation time, so stamp the
			// settled snapshot with createdAt (not the mutable updatedAt, which stays
			// in RawJSON). Clamp to [start,end] as defense-in-depth so a row whose
			// creation escaped the requested window never wedges the archive.
			if createdAt.Before(request.Start) || createdAt.After(request.End) {
				continue
			}
			entries = append(entries, normalizeSettledEarnOrder(raw, record, createdAt, category, request.Account, observedAt))
			if phase == "fixedterm" && isEarnRedeem(record.OrderType) {
				yieldEntries, err := normalizeEarnYields(record, category, request.Account, observedAt)
				if err != nil {
					return source.Page{}, err
				}
				for _, yieldEntry := range yieldEntries {
					// Immutable yield rows carry their own settlement time; keep only
					// those settling inside the window so the page stays wedge-free.
					if yieldEntry.OccurredAt.Before(request.Start) || yieldEntry.OccurredAt.After(request.End) {
						continue
					}
					entries = append(entries, yieldEntry)
				}
			}
		}

		next := earnNextCombinedCursor(phase, page.NextPageCursor)
		if len(entries) > 0 || next == "" {
			return source.Page{Entries: entries, NextCursor: next, Done: next == ""}, nil
		}
		if _, duplicate := seen[next]; duplicate {
			return source.Page{}, errors.New("Bybit earn order history repeated cursor")
		}
		seen[next] = struct{}{}
		phase, cursor, err = parseEarnCursor(next)
		if err != nil {
			return source.Page{}, err
		}
	}
	return source.Page{}, fmt.Errorf("Bybit earn order history exceeded %d filtered pages", earnFilteredPageLimit)
}

func (a *EarnOrdersAdapter) fetchOrderPage(
	ctx context.Context,
	request source.PageRequest,
	path string,
	cursor string,
) (earnResult, time.Time, error) {
	query := earnWindowQuery(request, 50, cursor)
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, path, query)
	if err != nil {
		return earnResult{}, time.Time{}, err
	}
	var page earnResult
	if err := json.Unmarshal(result, &page); err != nil {
		return earnResult{}, time.Time{}, errors.New("decode Bybit earn order result")
	}
	return page, observedAt, nil
}

func earnWindowQuery(request source.PageRequest, limit int, cursor string) url.Values {
	query := url.Values{
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(limit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return query
}

// earnNextCombinedCursor advances the phase cursor: within a phase it keeps
// draining provider pages; once the flexible endpoint is exhausted it hands off
// to the fixed-term phase; an exhausted fixed-term phase ends the stream.
func earnNextCombinedCursor(phase, providerCursor string) string {
	if providerCursor != "" {
		return earnCursorKind(phase) + providerCursor
	}
	if phase == "flexible" {
		return earnFixedTermCursorKind
	}
	return ""
}

func earnPhaseEndpoint(phase string) (string, string) {
	if phase == "fixedterm" {
		return earnFixedTermOrderPath, earnFixedTermCategory
	}
	return earnFlexibleOrderPath, earnFlexibleCategory
}

func earnCursorKind(phase string) string {
	if phase == "fixedterm" {
		return earnFixedTermCursorKind
	}
	return earnFlexibleCursorKind
}

func parseEarnCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "flexible", "", nil
	}
	if strings.HasPrefix(cursor, earnFlexibleCursorKind) {
		return "flexible", strings.TrimPrefix(cursor, earnFlexibleCursorKind), nil
	}
	if strings.HasPrefix(cursor, earnFixedTermCursorKind) {
		return "fixedterm", strings.TrimPrefix(cursor, earnFixedTermCursorKind), nil
	}
	return "", "", errors.New("Bybit earn order cursor has invalid source prefix")
}

type earnResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type earnOrderRecord struct {
	Category      string            `json:"category"`
	Coin          string            `json:"coin"`
	OrderID       string            `json:"orderId"`
	OrderLinkID   string            `json:"orderLinkId"`
	OrderType     string            `json:"orderType"`
	OrderValue    string            `json:"orderValue"`
	Amount        string            `json:"amount"`
	Quantity      string            `json:"quantity"`
	Status        string            `json:"status"`
	CreatedAt     string            `json:"createdAt"`
	UpdatedAt     string            `json:"updatedAt"`
	YieldInfoList []json.RawMessage `json:"yieldInfoList"`
}

type earnYieldRow struct {
	Coin      string `json:"coin"`
	Amount    string `json:"amount"`
	CreatedAt string `json:"createdAt"`
}

func normalizeEarnOrders(
	rawOrders []json.RawMessage,
	defaultCategory string,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rawOrders))
	for _, raw := range rawOrders {
		record, occurredAt, err := decodeEarnOrder(raw)
		if err != nil {
			return nil, err
		}
		category := earnCategory(record, defaultCategory)
		observations = append(observations, model.StateObservation{
			Exchange:         model.ExchangeBybit,
			AccountID:        account.ID,
			AccountLabel:     account.Label,
			Stream:           "earn",
			ObjectType:       "earn_order",
			ObjectID:         "earn-order:" + category + ":" + record.OrderID,
			Status:           record.Status,
			StateFingerprint: earnOrderFingerprint(record),
			Asset:            record.Coin,
			Amount:           earnValue(record),
			OccurredAt:       occurredAt,
			ObservedAt:       observedAt.UTC(),
			RawJSON:          append(json.RawMessage(nil), raw...),
		})
	}
	return observations, nil
}

// earnOrderCreatedAt returns the order's creation time, the field the provider's
// history window filters on. Settled ledger snapshots are stamped with it so each
// one lands in the window containing the order's creation, keeping the
// creation-time clamp lossless across checkpoint overlaps. It falls back to
// updatedAt only when createdAt is absent.
func earnOrderCreatedAt(record earnOrderRecord) (time.Time, error) {
	stamp := record.CreatedAt
	if strings.TrimSpace(stamp) == "" {
		stamp = record.UpdatedAt
	}
	return parseEarnTimestamp(stamp, "order createdAt")
}

func decodeEarnOrder(raw json.RawMessage) (earnOrderRecord, time.Time, error) {
	var record earnOrderRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return earnOrderRecord{}, time.Time{}, errors.New("decode Bybit earn order")
	}
	if strings.TrimSpace(record.OrderID) == "" {
		return earnOrderRecord{}, time.Time{}, errors.New("Bybit earn order has no orderId")
	}
	if strings.TrimSpace(record.Status) == "" {
		return earnOrderRecord{}, time.Time{}, errors.New("Bybit earn order has no status")
	}
	stamp := record.UpdatedAt
	if strings.TrimSpace(stamp) == "" {
		stamp = record.CreatedAt
	}
	occurredAt, err := parseEarnTimestamp(stamp, "order updatedAt")
	if err != nil {
		return earnOrderRecord{}, time.Time{}, err
	}
	return record, occurredAt, nil
}

func normalizeSettledEarnOrder(
	raw json.RawMessage,
	record earnOrderRecord,
	occurredAt time.Time,
	category string,
	account source.Account,
	observedAt time.Time,
) model.LedgerEntry {
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "earn-order:" + category + ":" + record.OrderID,
		Category:     "earn",
		Type:         record.Status,
		Asset:        record.Coin,
		Side:         record.OrderType,
		Amount:       earnValue(record),
		OrderID:      record.OrderID,
		Info:         category,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}
}

// normalizeEarnYields turns the settled fixed-term Redeem yieldInfoList rows into
// immutable ledger entries. Their identity embeds the parent order, settlement
// time, and amount so re-observing the same order upserts idempotently.
func normalizeEarnYields(
	record earnOrderRecord,
	category string,
	account source.Account,
	observedAt time.Time,
) ([]model.LedgerEntry, error) {
	entries := make([]model.LedgerEntry, 0, len(record.YieldInfoList))
	for _, raw := range record.YieldInfoList {
		var row earnYieldRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, errors.New("decode Bybit earn yield row")
		}
		if strings.TrimSpace(row.CreatedAt) == "" || strings.TrimSpace(row.Amount) == "" {
			// A yield row without a settlement time or amount has no stable identity.
			continue
		}
		occurredAt, err := parseEarnTimestamp(row.CreatedAt, "yield createdAt")
		if err != nil {
			return nil, err
		}
		coin := row.Coin
		if strings.TrimSpace(coin) == "" {
			coin = record.Coin
		}
		entries = append(entries, model.LedgerEntry{
			Exchange:     model.ExchangeBybit,
			AccountID:    account.ID,
			AccountLabel: account.Label,
			EntryID:      "earn-order-yield:" + record.OrderID + ":" + row.CreatedAt + ":" + row.Amount,
			Category:     "earn",
			Type:         "Yield",
			Asset:        coin,
			Amount:       row.Amount,
			OrderID:      record.OrderID,
			Info:         category,
			OccurredAt:   occurredAt,
			ObservedAt:   observedAt.UTC(),
			RawJSON:      append(json.RawMessage(nil), raw...),
		})
	}
	return entries, nil
}

func earnOrderFingerprint(record earnOrderRecord) string {
	fields := []string{
		record.Status,
		earnValue(record),
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

func earnCategory(record earnOrderRecord, fallback string) string {
	if category := strings.TrimSpace(record.Category); category != "" {
		return category
	}
	return fallback
}

func earnValue(record earnOrderRecord) string {
	if value := strings.TrimSpace(record.OrderValue); value != "" {
		return record.OrderValue
	}
	if value := strings.TrimSpace(record.Amount); value != "" {
		return record.Amount
	}
	return record.Quantity
}

func isSettledEarnOrder(status string) bool {
	switch status {
	case "Success", "Complete", "Completed", "Failed", "Fail":
		return true
	default:
		return false
	}
}

func isEarnRedeem(orderType string) bool {
	return strings.EqualFold(strings.TrimSpace(orderType), "Redeem")
}

// parseEarnTimestamp accepts either second- or millisecond-precision Unix
// timestamps (Bybit earn history reports seconds) and returns a UTC time.
func parseEarnTimestamp(raw, field string) (time.Time, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 0 {
		return time.Time{}, fmt.Errorf("Bybit earn %s is invalid", field)
	}
	millis := value
	if value < earnMillisThreshold {
		millis = value * 1000
	}
	if millis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit earn %s is invalid", field)
	}
	return time.UnixMilli(millis).UTC(), nil
}

var (
	_ source.Adapter      = (*EarnOrdersAdapter)(nil)
	_ source.EventAdapter = (*EarnOrdersAdapter)(nil)
)
