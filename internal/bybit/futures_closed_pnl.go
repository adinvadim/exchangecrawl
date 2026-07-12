package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	closedPnLPath          = "/v5/position/closed-pnl"
	closedPnLLimit         = 100
	closedPnLLinearPrefix  = "linear:"
	closedPnLInversePrefix = "inverse:"
)

// closedPnLCategories are polled in order. Each closed-pnl row is a terminal,
// write-once position-close record, so plain LedgerEntry archiving is safe.
var closedPnLCategories = []string{"linear", "inverse"}

// FuturesClosedPnLAdapter archives immutable Bybit closed-PnL rows across the
// linear and inverse contract categories, reusing the shared signing, retry,
// and base-URL validation implementation of *Adapter.
type FuturesClosedPnLAdapter struct {
	base *Adapter
}

// NewFuturesClosedPnL builds an adapter that shares the standard Bybit signing,
// retry, and base-URL validation implementation.
func NewFuturesClosedPnL(options Options) (*FuturesClosedPnLAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesClosedPnLAdapter{base: base}, nil
}

func (a *FuturesClosedPnLAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *FuturesClosedPnLAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns one provider page for the category selected by the cursor.
// The adapter walks linear rows first, then inverse rows, encoding both the
// active category and the provider cursor in its own cursor so the two Bybit
// categories share a single checkpointed archive stream.
func (a *FuturesClosedPnLAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	category, providerCursor, err := parseClosedPnLCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	query := closedPnLWindowQuery(request, category, providerCursor)
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, closedPnLPath, query)
	if err != nil {
		return source.Page{}, err
	}
	var page closedPnLResult
	if err := json.Unmarshal(result, &page); err != nil {
		return source.Page{}, errors.New("decode Bybit closed pnl result")
	}
	entries := make([]model.LedgerEntry, 0, len(page.List))
	for _, raw := range page.List {
		entry, err := normalizeClosedPnL(raw, category, request.Account, observedAt)
		if err != nil {
			return source.Page{}, err
		}
		entries = append(entries, entry)
	}
	next, err := nextClosedPnLCursor(category, providerCursor, page.NextPageCursor)
	if err != nil {
		return source.Page{}, err
	}
	return source.Page{Entries: entries, NextCursor: next, Done: next == ""}, nil
}

// nextClosedPnLCursor advances within a category while the provider offers more
// pages, then hands off from linear to inverse, and finally reports completion.
// A provider cursor that repeats the one just sent is rejected to avoid loops.
func nextClosedPnLCursor(category, current, providerNext string) (string, error) {
	if providerNext != "" {
		if providerNext == current {
			return "", errors.New("Bybit closed pnl repeated cursor")
		}
		return categoryCursorPrefix(category) + providerNext, nil
	}
	if category == "linear" {
		return closedPnLInversePrefix, nil
	}
	return "", nil
}

func categoryCursorPrefix(category string) string {
	if category == "inverse" {
		return closedPnLInversePrefix
	}
	return closedPnLLinearPrefix
}

func parseClosedPnLCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "linear", "", nil
	}
	if strings.HasPrefix(cursor, closedPnLLinearPrefix) {
		return "linear", strings.TrimPrefix(cursor, closedPnLLinearPrefix), nil
	}
	if strings.HasPrefix(cursor, closedPnLInversePrefix) {
		return "inverse", strings.TrimPrefix(cursor, closedPnLInversePrefix), nil
	}
	return "", "", errors.New("Bybit closed pnl cursor has invalid source prefix")
}

func closedPnLWindowQuery(request source.PageRequest, category, cursor string) url.Values {
	query := url.Values{
		"category":  {category},
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(closedPnLLimit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	return query
}

type closedPnLResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type closedPnLRecord struct {
	Symbol        string `json:"symbol"`
	OrderID       string `json:"orderId"`
	Side          string `json:"side"`
	Qty           string `json:"qty"`
	OrderPrice    string `json:"orderPrice"`
	OrderType     string `json:"orderType"`
	ExecType      string `json:"execType"`
	ClosedSize    string `json:"closedSize"`
	CumEntryValue string `json:"cumEntryValue"`
	AvgEntryPrice string `json:"avgEntryPrice"`
	CumExitValue  string `json:"cumExitValue"`
	AvgExitPrice  string `json:"avgExitPrice"`
	ClosedPnl     string `json:"closedPnl"`
	FillCount     string `json:"fillCount"`
	Leverage      string `json:"leverage"`
	OpenFee       string `json:"openFee"`
	CloseFee      string `json:"closeFee"`
	CreatedTime   string `json:"createdTime"`
	UpdatedTime   string `json:"updatedTime"`
}

// normalizeClosedPnL maps one provider row into a LedgerEntry. Closed-pnl rows
// carry no id of their own, and a single orderId can emit several rows (partial
// closes), so the identity is composed from category, orderId, updatedTime, and
// closedSize. All decimal fields are preserved verbatim as strings.
func normalizeClosedPnL(
	raw json.RawMessage,
	category string,
	account source.Account,
	observedAt time.Time,
) (model.LedgerEntry, error) {
	var record closedPnLRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.LedgerEntry{}, errors.New("decode Bybit closed pnl entry")
	}
	if strings.TrimSpace(record.OrderID) == "" {
		return model.LedgerEntry{}, errors.New("Bybit closed pnl entry has no orderId")
	}
	if strings.TrimSpace(record.ClosedSize) == "" {
		return model.LedgerEntry{}, errors.New("Bybit closed pnl entry has no closedSize")
	}
	occurredAt, err := parseClosedPnLTimestamp(record.UpdatedTime)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	entryID := strings.Join([]string{
		"futures-pnl",
		category,
		record.OrderID,
		record.UpdatedTime,
		record.ClosedSize,
	}, ":")
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      entryID,
		Symbol:       record.Symbol,
		Category:     category,
		Type:         record.ExecType,
		Side:         record.Side,
		Amount:       record.ClosedSize,
		Fee:          record.CloseFee,
		CashFlow:     record.ClosedPnl,
		OrderID:      record.OrderID,
		Info:         record.OrderType,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}, nil
}

func parseClosedPnLTimestamp(raw string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, errors.New("Bybit closed pnl entry has invalid updatedTime")
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var _ source.Adapter = (*FuturesClosedPnLAdapter)(nil)
