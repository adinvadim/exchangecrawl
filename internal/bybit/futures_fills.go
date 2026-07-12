package bybit

import (
	"context"
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
	futuresExecutionPath   = "/v5/execution/list"
	futuresLinearCategory  = "linear"
	futuresInverseCategory = "inverse"
)

// FuturesFillsAdapter archives immutable derivatives executions (fills) across
// both Bybit contract categories. It reuses the shared Bybit signing, retry, and
// base-URL validation implementation and drains linear then inverse fills through
// a single compound cursor ("linear|<nextPageCursor>" then "inverse|<nextPageCursor>").
type FuturesFillsAdapter struct {
	base *Adapter
}

// NewFuturesFills builds a futures fills adapter that shares the standard Bybit
// signing, retry, and base-URL validation implementation.
func NewFuturesFills(options Options) (*FuturesFillsAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesFillsAdapter{base: base}, nil
}

func (a *FuturesFillsAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *FuturesFillsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage returns one page of derivatives fills. A single adapter drains both
// contract categories: it first walks every linear page, then switches to inverse
// via the "inverse|" cursor, then reports Done. The compound cursor keeps both
// provider pagination streams in one checkpointed archive stream. A provider that
// echoes the same cursor is rejected to avoid an infinite pagination loop.
func (a *FuturesFillsAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	category, providerCursor, err := parseFuturesFillsCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	query := futuresFillsWindowQuery(request, category, providerCursor, 100)
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, futuresExecutionPath, query)
	if err != nil {
		return source.Page{}, err
	}
	var page spotResult
	if err := json.Unmarshal(result, &page); err != nil {
		return source.Page{}, errors.New("decode Bybit futures execution result")
	}
	entries := make([]model.LedgerEntry, 0, len(page.List))
	for _, raw := range page.List {
		entry, err := normalizeFuturesExecution(raw, category, request.Account, observedAt)
		if err != nil {
			return source.Page{}, err
		}
		entries = append(entries, entry)
	}

	if next := page.NextPageCursor; next != "" {
		if next == providerCursor {
			return source.Page{}, errors.New("Bybit futures fills repeated cursor")
		}
		return source.Page{Entries: entries, NextCursor: category + "|" + next}, nil
	}
	if category == futuresLinearCategory {
		// Linear pages are exhausted; advance to the inverse category.
		return source.Page{Entries: entries, NextCursor: futuresInverseCategory + "|"}, nil
	}
	return source.Page{Entries: entries, NextCursor: "", Done: true}, nil
}

func futuresFillsWindowQuery(request source.PageRequest, category, providerCursor string, limit int) url.Values {
	query := url.Values{
		"category":  {category},
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(limit)},
	}
	if providerCursor != "" {
		query.Set("cursor", providerCursor)
	}
	return query
}

// parseFuturesFillsCursor splits an adapter-owned compound cursor into the
// contract category and the provider's opaque page cursor. An empty request
// cursor starts the first (linear) category with no provider cursor.
func parseFuturesFillsCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return futuresLinearCategory, "", nil
	}
	separator := strings.IndexByte(cursor, '|')
	if separator < 0 {
		return "", "", errors.New("Bybit futures fills cursor has invalid source prefix")
	}
	category := cursor[:separator]
	if category != futuresLinearCategory && category != futuresInverseCategory {
		return "", "", errors.New("Bybit futures fills cursor has invalid source prefix")
	}
	return category, cursor[separator+1:], nil
}

type futuresExecutionRecord struct {
	Symbol      string `json:"symbol"`
	OrderID     string `json:"orderId"`
	OrderLinkID string `json:"orderLinkId"`
	Side        string `json:"side"`
	OrderType   string `json:"orderType"`
	ExecFee     string `json:"execFee"`
	ExecID      string `json:"execId"`
	ExecPrice   string `json:"execPrice"`
	ExecQty     string `json:"execQty"`
	ExecType    string `json:"execType"`
	ExecValue   string `json:"execValue"`
	ExecTime    string `json:"execTime"`
	FeeCurrency string `json:"feeCurrency"`
	ClosedSize  string `json:"closedSize"`
	IsMaker     bool   `json:"isMaker"`
}

func normalizeFuturesExecution(
	raw json.RawMessage,
	category string,
	account source.Account,
	observedAt time.Time,
) (model.LedgerEntry, error) {
	var record futuresExecutionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.LedgerEntry{}, errors.New("decode Bybit futures execution")
	}
	if strings.TrimSpace(record.ExecID) == "" {
		return model.LedgerEntry{}, errors.New("Bybit futures execution has no execId")
	}
	occurredAt, err := parseFuturesTimestamp(record.ExecTime, "execution execTime")
	if err != nil {
		return model.LedgerEntry{}, err
	}
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "futures-fill:" + category + ":" + record.ExecID,
		Symbol:       record.Symbol,
		Category:     category,
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

func parseFuturesTimestamp(raw, field string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, fmt.Errorf("Bybit futures %s is invalid", field)
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var _ source.Adapter = (*FuturesFillsAdapter)(nil)
