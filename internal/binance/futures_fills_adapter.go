package binance

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	futuresExchangeInfoPath = "/fapi/v1/exchangeInfo"
	futuresUserTradesPath   = "/fapi/v1/userTrades"
	futuresFillsPageLimit   = 1000
	// USDS-M userTrades accepts a startTime/endTime span of at most 7 days; the
	// caller's window is capped here so a single sweep never straddles the limit.
	futuresFillsMaxWindow = 7 * 24 * time.Hour
)

// FuturesFillsAdapter archives the immutable USDS-M futures fill ledger from
// GET /fapi/v1/userTrades. That endpoint requires a symbol, so this adapter
// mirrors the proven spot pattern: it caches the exchangeInfo symbol set, then
// sweeps symbols with a JSON cursor {symbol, fromId}, paging each symbol by
// fromId until a short page arrives before advancing to the next symbol.
//
// Binance retains futures userTrades history for roughly six months, so a full
// backfill must run inside that retention window; older fills are unavailable
// from the provider. fromId cannot be combined with startTime/endTime, so
// within-symbol paging is fromId-only and rows are filtered against the
// requested window client-side.
type FuturesFillsAdapter struct {
	client *Adapter

	symbolsMu sync.Mutex
	symbols   []string
}

var _ source.Adapter = (*FuturesFillsAdapter)(nil)

// NewFuturesFills builds a USDS-M futures fill adapter over a fapi-configured
// client, reusing its signing, retry, and base-URL validation.
func NewFuturesFills(options Options) (*FuturesFillsAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesFillsAdapter{client: client}, nil
}

func (a *FuturesFillsAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *FuturesFillsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *FuturesFillsAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateFuturesFillsWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	symbols, err := a.listedSymbols(ctx)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeFuturesFillCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	return a.fetchFillPage(ctx, request, symbols, cursor, apiKey, secret)
}

func (a *FuturesFillsAdapter) credentials(account source.Account) (string, string, error) {
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

func validateFuturesFillsWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance futures fills page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > futuresFillsMaxWindow {
		return errors.New("Binance futures fills page window must not exceed 7 days")
	}
	return nil
}

type futuresExchangeInfo struct {
	Symbols []struct {
		Symbol string `json:"symbol"`
	} `json:"symbols"`
}

func (a *FuturesFillsAdapter) listedSymbols(ctx context.Context) ([]string, error) {
	a.symbolsMu.Lock()
	defer a.symbolsMu.Unlock()
	if len(a.symbols) != 0 {
		return append([]string(nil), a.symbols...), nil
	}
	var response futuresExchangeInfo
	if err := a.client.publicGET(ctx, futuresExchangeInfoPath, nil, "futures exchange info", &response); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(response.Symbols))
	for _, item := range response.Symbols {
		symbol := strings.TrimSpace(item.Symbol)
		if symbol != "" {
			seen[symbol] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, errors.New("Binance futures exchange info returned no symbols")
	}
	a.symbols = make([]string, 0, len(seen))
	for symbol := range seen {
		a.symbols = append(a.symbols, symbol)
	}
	sort.Strings(a.symbols)
	return append([]string(nil), a.symbols...), nil
}

type futuresFillCursor struct {
	Symbol string `json:"s,omitempty"`
	FromID string `json:"i,omitempty"`
}

func encodeFuturesFillCursor(cursor futuresFillCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeFuturesFillCursor(value string) (futuresFillCursor, error) {
	if value == "" {
		return futuresFillCursor{}, nil
	}
	if value != strings.TrimSpace(value) {
		return futuresFillCursor{}, errors.New("Binance futures fills cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return futuresFillCursor{}, errors.New("Binance futures fills cursor is invalid")
	}
	var cursor futuresFillCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return futuresFillCursor{}, errors.New("Binance futures fills cursor is invalid")
	}
	return cursor, nil
}

func futuresFillSymbolIndex(symbols []string, symbol string) (int, error) {
	if symbol == "" {
		return 0, nil
	}
	index := sort.SearchStrings(symbols, symbol)
	if index == len(symbols) || symbols[index] != symbol {
		return 0, errors.New("Binance futures fills cursor references an unavailable symbol")
	}
	return index, nil
}

type futuresFillRow struct {
	Symbol          string          `json:"symbol"`
	ID              flexibleID      `json:"id"`
	OrderID         flexibleID      `json:"orderId"`
	Side            string          `json:"side"`
	PositionSide    string          `json:"positionSide"`
	Price           string          `json:"price"`
	Qty             string          `json:"qty"`
	QuoteQty        string          `json:"quoteQty"`
	RealizedPnl     string          `json:"realizedPnl"`
	Commission      string          `json:"commission"`
	CommissionAsset string          `json:"commissionAsset"`
	Time            int64           `json:"time"`
	Buyer           bool            `json:"buyer"`
	Maker           bool            `json:"maker"`
	RawJSON         json.RawMessage `json:"-"`
}

func (row *futuresFillRow) UnmarshalJSON(data []byte) error {
	type wire futuresFillRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = futuresFillRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func (a *FuturesFillsAdapter) fetchFillPage(
	ctx context.Context,
	request source.PageRequest,
	symbols []string,
	cursor futuresFillCursor,
	apiKey string,
	secret string,
) (source.Page, error) {
	index, err := futuresFillSymbolIndex(symbols, cursor.Symbol)
	if err != nil {
		return source.Page{}, err
	}
	for index < len(symbols) {
		symbol := symbols[index]
		query := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(futuresFillsPageLimit)}, "recvWindow": {strconv.Itoa(a.client.recvWindow)}}
		if cursor.FromID == "" {
			query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
			query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
		} else {
			query.Set("fromId", cursor.FromID)
		}
		var rows []futuresFillRow
		if err := a.client.signedGETOperation(ctx, futuresUserTradesPath, query, apiKey, secret, "futures user trades", &rows); err != nil {
			return source.Page{}, err
		}
		observedAt := a.client.now().UTC()
		entries := make([]model.LedgerEntry, 0, len(rows))
		pastWindow := false
		for rowIndex, row := range rows {
			entry, err := normalizeFuturesFill(row, request.Account, observedAt)
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize Binance futures fill row %d: %w", rowIndex, err)
			}
			if entry.OccurredAt.After(request.End) {
				pastWindow = true
				continue
			}
			if !entry.OccurredAt.Before(request.Start) {
				entries = append(entries, entry)
			}
		}
		if len(rows) == futuresFillsPageLimit && !pastWindow {
			nextID, err := nextSpotID(string(rows[len(rows)-1].ID))
			if err != nil {
				return source.Page{}, fmt.Errorf("paginate Binance futures fills: %w", err)
			}
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeFuturesFillCursor(futuresFillCursor{Symbol: symbol, FromID: nextID})}, nil
			}
			cursor = futuresFillCursor{Symbol: symbol, FromID: nextID}
			continue
		}
		index++
		cursor = futuresFillCursor{}
		if len(entries) != 0 {
			page := source.Page{Entries: entries, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeFuturesFillCursor(futuresFillCursor{Symbol: symbols[index]})
			}
			return page, nil
		}
	}
	return source.Page{Done: true}, nil
}

func normalizeFuturesFill(row futuresFillRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	tradeID := strings.TrimSpace(string(row.ID))
	symbol := strings.TrimSpace(row.Symbol)
	if tradeID == "" || symbol == "" {
		return model.LedgerEntry{}, errors.New("futures fill row has no symbol or trade id")
	}
	if row.Time <= 0 || row.Time > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("futures fill row has invalid occurrence time")
	}
	side := strings.ToUpper(strings.TrimSpace(row.Side))
	if side == "" {
		side = "SELL"
		if row.Buyer {
			side = "BUY"
		}
	}
	liquidity := "taker"
	if row.Maker {
		liquidity = "maker"
	}
	// positionSide (LONG/SHORT/BOTH) and realizedPnl are futures-only fields with
	// no dedicated LedgerEntry column; they are preserved verbatim in RawJSON.
	info := liquidity
	if positionSide := strings.ToUpper(strings.TrimSpace(row.PositionSide)); positionSide != "" {
		info = liquidity + " " + positionSide
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: "futures-fill:" + symbol + ":" + tradeID, Symbol: symbol, Category: "futures", Type: "fill",
		Asset: strings.TrimSpace(row.CommissionAsset), Side: side, Amount: strings.TrimSpace(row.Qty),
		Fee: strings.TrimSpace(row.Commission), CashFlow: strings.TrimSpace(row.QuoteQty),
		OrderID: strings.TrimSpace(string(row.OrderID)), TradeID: tradeID,
		Info: info, OccurredAt: time.UnixMilli(row.Time).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
