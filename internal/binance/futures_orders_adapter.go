package binance

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	futuresOpenOrdersPath  = "/fapi/v1/openOrders"
	futuresAllOrdersPath   = "/fapi/v1/allOrders"
	futuresOrdersPageLimit = 1000
	// USDS-M allOrders accepts a startTime/endTime span of at most 7 days; the
	// caller's window is capped here so a single sweep never straddles the limit.
	futuresOrdersMaxWindow   = 7 * 24 * time.Hour
	futuresOrderHistoryPhase = "history"
)

// FuturesOrdersAdapter observes the USDS-M futures order lifecycle. Because an
// order is a mutable object, FetchEventPage records StateObservations: it first
// snapshots the open set from GET /fapi/v1/openOrders (which needs no symbol),
// then sweeps every listed symbol through GET /fapi/v1/allOrders using an
// orderId cursor, mirroring the proven spot order pattern. TerminalOrders wraps
// the same sweep as a source.Adapter that emits an immutable LedgerEntry only
// for terminal statuses (FILLED, CANCELED, EXPIRED, EXPIRED_IN_MATCH, REJECTED).
//
// allOrders requires a symbol and retains history for a bounded window, so the
// caller's time span is capped at 7 days. orderId paging cannot be combined
// with startTime/endTime, so within-symbol paging is orderId-only and rows are
// filtered against the requested window client-side.
type FuturesOrdersAdapter struct {
	client *Adapter

	symbolsMu sync.Mutex
	symbols   []string
}

var (
	_ source.EventAdapter = (*FuturesOrdersAdapter)(nil)
	_ source.Adapter      = (*futuresTerminalOrderAdapter)(nil)
)

// NewFuturesOrders builds a USDS-M futures order adapter over a fapi-configured
// client, reusing its signing, retry, and base-URL validation.
func NewFuturesOrders(options Options) (*FuturesOrdersAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesOrdersAdapter{client: client}, nil
}

func (a *FuturesOrdersAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *FuturesOrdersAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *FuturesOrdersAdapter) credentials(account source.Account) (string, string, error) {
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

func validateFuturesOrdersWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance futures orders page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > futuresOrdersMaxWindow {
		return errors.New("Binance futures orders page window must not exceed 7 days")
	}
	return nil
}

// listedSymbols reuses the shared futuresExchangeInfo type, futuresExchangeInfoPath
// const, and publicGET helper (defined alongside the futures fills adapter) so the
// symbol enumeration is not re-implemented; only this adapter's own cache is local.
func (a *FuturesOrdersAdapter) listedSymbols(ctx context.Context) ([]string, error) {
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

type futuresOrderCursor struct {
	Phase  string   `json:"p,omitempty"`
	Symbol string   `json:"s,omitempty"`
	FromID string   `json:"i,omitempty"`
	Skip   []string `json:"x,omitempty"`
}

func encodeFuturesOrderCursor(cursor futuresOrderCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeFuturesOrderCursor(value string) (futuresOrderCursor, error) {
	if value == "" {
		return futuresOrderCursor{}, nil
	}
	if value != strings.TrimSpace(value) {
		return futuresOrderCursor{}, errors.New("Binance futures orders cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return futuresOrderCursor{}, errors.New("Binance futures orders cursor is invalid")
	}
	var cursor futuresOrderCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return futuresOrderCursor{}, errors.New("Binance futures orders cursor is invalid")
	}
	return cursor, nil
}

func futuresOrderSymbolIndex(symbols []string, symbol string) (int, error) {
	if symbol == "" {
		return 0, nil
	}
	index := sort.SearchStrings(symbols, symbol)
	if index == len(symbols) || symbols[index] != symbol {
		return 0, errors.New("Binance futures orders cursor references an unavailable symbol")
	}
	return index, nil
}

type futuresOrderRow struct {
	Symbol        string          `json:"symbol"`
	OrderID       flexibleID      `json:"orderId"`
	ClientOrderID string          `json:"clientOrderId"`
	Price         string          `json:"price"`
	AvgPrice      string          `json:"avgPrice"`
	OrigQty       string          `json:"origQty"`
	ExecutedQty   string          `json:"executedQty"`
	CumQuote      string          `json:"cumQuote"`
	Status        string          `json:"status"`
	TimeInForce   string          `json:"timeInForce"`
	Type          string          `json:"type"`
	Side          string          `json:"side"`
	PositionSide  string          `json:"positionSide"`
	StopPrice     string          `json:"stopPrice"`
	ReduceOnly    bool            `json:"reduceOnly"`
	ClosePosition bool            `json:"closePosition"`
	OrigType      string          `json:"origType"`
	Time          int64           `json:"time"`
	UpdateTime    int64           `json:"updateTime"`
	RawJSON       json.RawMessage `json:"-"`
}

func (row *futuresOrderRow) UnmarshalJSON(data []byte) error {
	type wire futuresOrderRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = futuresOrderRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func (a *FuturesOrdersAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateFuturesOrdersWindow(request); err != nil {
		return source.EventPage{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	symbols, err := a.listedSymbols(ctx)
	if err != nil {
		return source.EventPage{}, err
	}
	cursor, err := decodeFuturesOrderCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	if cursor.Phase == "" {
		query := url.Values{"recvWindow": {strconv.Itoa(a.client.recvWindow)}}
		var rows []futuresOrderRow
		if err := a.client.signedGETOperation(ctx, futuresOpenOrdersPath, query, apiKey, secret, "futures open orders", &rows); err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeFuturesOrderObservations(rows, request.Account, a.client.now().UTC())
		if err != nil {
			return source.EventPage{}, err
		}
		cursor = futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: symbols[0]}
		if len(observations) != 0 {
			cursor.Skip = make([]string, 0, len(observations))
			for _, observation := range observations {
				cursor.Skip = append(cursor.Skip, spotObservationIdentity(observation))
			}
			return source.EventPage{Observations: observations, NextCursor: encodeFuturesOrderCursor(cursor)}, nil
		}
	}
	if cursor.Phase != futuresOrderHistoryPhase {
		return source.EventPage{}, errors.New("Binance futures orders cursor has an invalid phase")
	}
	return a.fetchOrderEventPage(ctx, request, symbols, cursor, apiKey, secret)
}

func (a *FuturesOrdersAdapter) fetchOrderEventPage(
	ctx context.Context,
	request source.PageRequest,
	symbols []string,
	cursor futuresOrderCursor,
	apiKey string,
	secret string,
) (source.EventPage, error) {
	index, err := futuresOrderSymbolIndex(symbols, cursor.Symbol)
	if err != nil {
		return source.EventPage{}, err
	}
	skip := make(map[string]struct{}, len(cursor.Skip))
	for _, identity := range cursor.Skip {
		skip[identity] = struct{}{}
	}
	for index < len(symbols) {
		symbol := symbols[index]
		rows, nextID, pastWindow, err := a.fetchOrderRows(ctx, request, symbol, cursor.FromID, apiKey, secret)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeFuturesOrderObservations(rows, request.Account, a.client.now().UTC())
		if err != nil {
			return source.EventPage{}, err
		}
		filtered := observations[:0]
		for _, observation := range observations {
			if observation.OccurredAt.After(request.End) || observation.OccurredAt.Before(request.Start) {
				continue
			}
			if _, duplicate := skip[spotObservationIdentity(observation)]; !duplicate {
				filtered = append(filtered, observation)
			}
		}
		if nextID != "" && !pastWindow {
			next := futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: symbol, FromID: nextID, Skip: cursor.Skip}
			if len(filtered) != 0 {
				return source.EventPage{Observations: filtered, NextCursor: encodeFuturesOrderCursor(next)}, nil
			}
			cursor = next
			continue
		}
		index++
		cursor.FromID = ""
		if len(filtered) != 0 {
			page := source.EventPage{Observations: filtered, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeFuturesOrderCursor(futuresOrderCursor{
					Phase: futuresOrderHistoryPhase, Symbol: symbols[index], Skip: cursor.Skip,
				})
			}
			return page, nil
		}
	}
	return source.EventPage{Done: true}, nil
}

func (a *FuturesOrdersAdapter) fetchOrderRows(
	ctx context.Context,
	request source.PageRequest,
	symbol string,
	fromID string,
	apiKey string,
	secret string,
) ([]futuresOrderRow, string, bool, error) {
	query := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(futuresOrdersPageLimit)}, "recvWindow": {strconv.Itoa(a.client.recvWindow)}}
	if fromID == "" {
		query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
		query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
	} else {
		query.Set("orderId", fromID)
	}
	var rows []futuresOrderRow
	if err := a.client.signedGETOperation(ctx, futuresAllOrdersPath, query, apiKey, secret, "futures all orders", &rows); err != nil {
		return nil, "", false, err
	}
	pastWindow := false
	for _, row := range rows {
		// orderId pagination is creation-ordered; updateTime can move independently
		// when an older order stays open longer than newer orders.
		if row.Time > request.End.UnixMilli() {
			pastWindow = true
			break
		}
	}
	if len(rows) != futuresOrdersPageLimit {
		return rows, "", pastWindow, nil
	}
	nextID, err := nextSpotID(string(rows[len(rows)-1].OrderID))
	if err != nil {
		return nil, "", false, fmt.Errorf("paginate Binance futures orders: %w", err)
	}
	return rows, nextID, pastWindow, nil
}

func normalizeFuturesOrderObservations(rows []futuresOrderRow, account source.Account, observedAt time.Time) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeFuturesOrderObservation(row, account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance futures order row %d: %w", index, err)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func normalizeFuturesOrderObservation(row futuresOrderRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	symbol := strings.TrimSpace(row.Symbol)
	orderID := strings.TrimSpace(string(row.OrderID))
	status := strings.TrimSpace(row.Status)
	if symbol == "" || orderID == "" {
		return model.StateObservation{}, errors.New("order row has no symbol or order id")
	}
	if status == "" {
		return model.StateObservation{}, errors.New("order row has no status")
	}
	if row.UpdateTime <= 0 || row.UpdateTime > maxArchivedUnixMS {
		return model.StateObservation{}, errors.New("order row has invalid update time")
	}
	// The fingerprint captures exactly the fields that mutate over an order's life
	// so a resnapshot with identical execution state produces the same fingerprint.
	state := struct {
		Status      string `json:"status"`
		ExecutedQty string `json:"executedQty"`
		CumQuote    string `json:"cumQuote"`
		AvgPrice    string `json:"avgPrice"`
		UpdateTime  int64  `json:"updateTime"`
	}{
		Status: status, ExecutedQty: row.ExecutedQty, CumQuote: row.CumQuote,
		AvgPrice: row.AvgPrice, UpdateTime: row.UpdateTime,
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)
	return model.StateObservation{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		Stream: "futures", ObjectType: "order", ObjectID: "futures-order:" + symbol + ":" + orderID, Status: status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]), Symbol: symbol, Amount: strings.TrimSpace(row.ExecutedQty),
		OccurredAt: time.UnixMilli(row.UpdateTime).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

type futuresTerminalOrderAdapter struct {
	orders *FuturesOrdersAdapter
}

// TerminalOrders exposes the same allOrders sweep as an immutable ledger over
// terminal order statuses only, producing a stable futures-order LedgerEntry.
func (a *FuturesOrdersAdapter) TerminalOrders() source.Adapter {
	return &futuresTerminalOrderAdapter{orders: a}
}

func (a *futuresTerminalOrderAdapter) Exchange() model.Exchange {
	return a.orders.Exchange()
}

func (a *futuresTerminalOrderAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.orders.CheckCredentials(account)
}

func (a *futuresTerminalOrderAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateFuturesOrdersWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.orders.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	symbols, err := a.orders.listedSymbols(ctx)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeFuturesOrderCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	if cursor.Phase != "" && cursor.Phase != futuresOrderHistoryPhase {
		return source.Page{}, errors.New("Binance futures terminal-order cursor has an invalid phase")
	}
	index, err := futuresOrderSymbolIndex(symbols, cursor.Symbol)
	if err != nil {
		return source.Page{}, err
	}
	for index < len(symbols) {
		symbol := symbols[index]
		rows, nextID, pastWindow, err := a.orders.fetchOrderRows(ctx, request, symbol, cursor.FromID, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(rows))
		for rowIndex, row := range rows {
			if !isTerminalFuturesOrderStatus(strings.TrimSpace(row.Status)) {
				continue
			}
			entry, err := normalizeTerminalFuturesOrder(row, request.Account, a.orders.client.now().UTC())
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize terminal Binance futures order row %d: %w", rowIndex, err)
			}
			if !entry.OccurredAt.Before(request.Start) && !entry.OccurredAt.After(request.End) {
				entries = append(entries, entry)
			}
		}
		if nextID != "" && !pastWindow {
			next := futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: symbol, FromID: nextID}
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeFuturesOrderCursor(next)}, nil
			}
			cursor = next
			continue
		}
		index++
		cursor.FromID = ""
		if len(entries) != 0 {
			page := source.Page{Entries: entries, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeFuturesOrderCursor(futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: symbols[index]})
			}
			return page, nil
		}
	}
	return source.Page{Done: true}, nil
}

func isTerminalFuturesOrderStatus(status string) bool {
	switch status {
	case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED":
		return true
	default:
		return false
	}
}

func normalizeTerminalFuturesOrder(row futuresOrderRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	observation, err := normalizeFuturesOrderObservation(row, account, observedAt)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	// positionSide (LONG/SHORT/BOTH) has no dedicated LedgerEntry column and is
	// preserved verbatim in RawJSON; Info carries the order type for readability.
	info := strings.TrimSpace(row.Type)
	if positionSide := strings.ToUpper(strings.TrimSpace(row.PositionSide)); positionSide != "" {
		info = strings.TrimSpace(info + " " + positionSide)
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: observation.ObjectID, Symbol: observation.Symbol, Category: "futures", Type: observation.Status,
		Side: strings.TrimSpace(row.Side), Amount: strings.TrimSpace(row.ExecutedQty),
		CashFlow: strings.TrimSpace(row.CumQuote), OrderID: strings.TrimSpace(string(row.OrderID)),
		Info: info, OccurredAt: observation.OccurredAt, ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
