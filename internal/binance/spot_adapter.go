package binance

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	defaultSpotBaseURL    = "https://api.binance.com"
	spotExchangeInfoPath  = "/api/v3/exchangeInfo"
	spotTradesPath        = "/api/v3/myTrades"
	spotOrdersPath        = "/api/v3/allOrders"
	spotOpenOrdersPath    = "/api/v3/openOrders"
	spotPageLimit         = 1000
	spotMaxWindow         = 24 * time.Hour
	spotOrderHistoryPhase = "history"
)

type SpotAdapter struct {
	client *Adapter

	symbolsMu sync.Mutex
	symbols   []string
}

var (
	_ source.Adapter      = (*SpotAdapter)(nil)
	_ source.EventAdapter = (*SpotAdapter)(nil)
)

func NewSpot(options Options) (*SpotAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &SpotAdapter{client: client}, nil
}

func (a *SpotAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *SpotAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *SpotAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateSpotWindow(request); err != nil {
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
	cursor, err := decodeSpotCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	return a.fetchTradePage(ctx, request, symbols, cursor, apiKey, secret)
}

func (a *SpotAdapter) credentials(account source.Account) (string, string, error) {
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

func validateSpotWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance Spot page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > spotMaxWindow {
		return errors.New("Binance Spot page window must not exceed 24 hours")
	}
	return nil
}

type spotExchangeInfo struct {
	Symbols []struct {
		Symbol string `json:"symbol"`
	} `json:"symbols"`
}

func (a *SpotAdapter) listedSymbols(ctx context.Context) ([]string, error) {
	a.symbolsMu.Lock()
	defer a.symbolsMu.Unlock()
	if len(a.symbols) != 0 {
		return append([]string(nil), a.symbols...), nil
	}
	var response spotExchangeInfo
	if err := a.client.publicGET(ctx, spotExchangeInfoPath, nil, "spot exchange info", &response); err != nil {
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
		return nil, errors.New("Binance Spot exchange info returned no symbols")
	}
	a.symbols = make([]string, 0, len(seen))
	for symbol := range seen {
		a.symbols = append(a.symbols, symbol)
	}
	sort.Strings(a.symbols)
	return append([]string(nil), a.symbols...), nil
}

func (a *Adapter) publicGET(ctx context.Context, path string, query url.Values, operation string, out any) error {
	if query == nil {
		query = url.Values{}
	}
	return a.getJSON(ctx, operation, func(time.Time) (*http.Request, []string, error) {
		endpoint := *a.baseURL
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, nil, err
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "binancecrawl/0")
		return request, nil, nil
	}, out)
}

type spotCursor struct {
	Phase  string   `json:"p,omitempty"`
	Symbol string   `json:"s,omitempty"`
	FromID string   `json:"i,omitempty"`
	Skip   []string `json:"x,omitempty"`
}

func encodeSpotCursor(cursor spotCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeSpotCursor(value string) (spotCursor, error) {
	if value == "" {
		return spotCursor{}, nil
	}
	if value != strings.TrimSpace(value) {
		return spotCursor{}, errors.New("Binance Spot cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return spotCursor{}, errors.New("Binance Spot cursor is invalid")
	}
	var cursor spotCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return spotCursor{}, errors.New("Binance Spot cursor is invalid")
	}
	return cursor, nil
}

func cursorSymbolIndex(symbols []string, symbol string) (int, error) {
	if symbol == "" {
		return 0, nil
	}
	index := sort.SearchStrings(symbols, symbol)
	if index == len(symbols) || symbols[index] != symbol {
		return 0, errors.New("Binance Spot cursor references an unavailable symbol")
	}
	return index, nil
}

type spotTradeRow struct {
	Symbol          string          `json:"symbol"`
	ID              flexibleID      `json:"id"`
	OrderID         flexibleID      `json:"orderId"`
	Price           string          `json:"price"`
	Qty             string          `json:"qty"`
	QuoteQty        string          `json:"quoteQty"`
	Commission      string          `json:"commission"`
	CommissionAsset string          `json:"commissionAsset"`
	Time            int64           `json:"time"`
	IsBuyer         bool            `json:"isBuyer"`
	IsMaker         bool            `json:"isMaker"`
	RawJSON         json.RawMessage `json:"-"`
}

func (row *spotTradeRow) UnmarshalJSON(data []byte) error {
	type wire spotTradeRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = spotTradeRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func (a *SpotAdapter) fetchTradePage(
	ctx context.Context,
	request source.PageRequest,
	symbols []string,
	cursor spotCursor,
	apiKey string,
	secret string,
) (source.Page, error) {
	index, err := cursorSymbolIndex(symbols, cursor.Symbol)
	if err != nil {
		return source.Page{}, err
	}
	for index < len(symbols) {
		symbol := symbols[index]
		query := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(spotPageLimit)}, "recvWindow": {strconv.Itoa(a.client.recvWindow)}}
		if cursor.FromID == "" {
			query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
			query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
		} else {
			query.Set("fromId", cursor.FromID)
		}
		var rows []spotTradeRow
		if err := a.client.signedGETOperation(ctx, spotTradesPath, query, apiKey, secret, "spot trades", &rows); err != nil {
			return source.Page{}, err
		}
		observedAt := a.client.now().UTC()
		entries := make([]model.LedgerEntry, 0, len(rows))
		pastWindow := false
		for rowIndex, row := range rows {
			entry, err := normalizeSpotTrade(row, request.Account, observedAt)
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize Binance Spot trade row %d: %w", rowIndex, err)
			}
			if entry.OccurredAt.After(request.End) {
				pastWindow = true
				continue
			}
			if !entry.OccurredAt.Before(request.Start) {
				entries = append(entries, entry)
			}
		}
		if len(rows) == spotPageLimit && !pastWindow {
			nextID, err := nextSpotID(string(rows[len(rows)-1].ID))
			if err != nil {
				return source.Page{}, fmt.Errorf("paginate Binance Spot trades: %w", err)
			}
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeSpotCursor(spotCursor{Symbol: symbol, FromID: nextID})}, nil
			}
			cursor = spotCursor{Symbol: symbol, FromID: nextID}
			continue
		}
		index++
		cursor = spotCursor{}
		if len(entries) != 0 {
			page := source.Page{Entries: entries, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeSpotCursor(spotCursor{Symbol: symbols[index]})
			}
			return page, nil
		}
	}
	return source.Page{Done: true}, nil
}

func normalizeSpotTrade(row spotTradeRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	tradeID := strings.TrimSpace(string(row.ID))
	symbol := strings.TrimSpace(row.Symbol)
	if tradeID == "" || symbol == "" {
		return model.LedgerEntry{}, errors.New("trade row has no symbol or trade id")
	}
	if row.Time <= 0 || row.Time > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("trade row has invalid occurrence time")
	}
	side := "SELL"
	if row.IsBuyer {
		side = "BUY"
	}
	liquidity := "taker"
	if row.IsMaker {
		liquidity = "maker"
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: "spot-fill:" + symbol + ":" + tradeID, Symbol: symbol, Category: "spot", Type: "fill",
		Asset: strings.TrimSpace(row.CommissionAsset), Side: side, Amount: strings.TrimSpace(row.Qty),
		Fee: strings.TrimSpace(row.Commission), CashFlow: strings.TrimSpace(row.QuoteQty),
		OrderID: strings.TrimSpace(string(row.OrderID)), TradeID: tradeID,
		Info: liquidity, OccurredAt: time.UnixMilli(row.Time).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func nextSpotID(value string) (string, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || id == ^uint64(0) {
		return "", errors.New("last row has invalid id")
	}
	return strconv.FormatUint(id+1, 10), nil
}

type spotOrderRow struct {
	Symbol                  string          `json:"symbol"`
	OrderID                 flexibleID      `json:"orderId"`
	ClientOrderID           string          `json:"clientOrderId"`
	Price                   string          `json:"price"`
	OrigQty                 string          `json:"origQty"`
	ExecutedQty             string          `json:"executedQty"`
	CumulativeQuoteQty      string          `json:"cummulativeQuoteQty"`
	Status                  string          `json:"status"`
	TimeInForce             string          `json:"timeInForce"`
	Type                    string          `json:"type"`
	Side                    string          `json:"side"`
	StopPrice               string          `json:"stopPrice"`
	Time                    int64           `json:"time"`
	UpdateTime              int64           `json:"updateTime"`
	IsWorking               bool            `json:"isWorking"`
	WorkingTime             int64           `json:"workingTime"`
	SelfTradePreventionMode string          `json:"selfTradePreventionMode"`
	RawJSON                 json.RawMessage `json:"-"`
}

func (row *spotOrderRow) UnmarshalJSON(data []byte) error {
	type wire spotOrderRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = spotOrderRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func (a *SpotAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateSpotWindow(request); err != nil {
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
	cursor, err := decodeSpotCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	if cursor.Phase == "" {
		query := url.Values{"recvWindow": {strconv.Itoa(a.client.recvWindow)}}
		var rows []spotOrderRow
		if err := a.client.signedGETOperation(ctx, spotOpenOrdersPath, query, apiKey, secret, "spot open orders", &rows); err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeSpotOrderObservations(rows, request.Account, a.client.now().UTC())
		if err != nil {
			return source.EventPage{}, err
		}
		cursor = spotCursor{Phase: spotOrderHistoryPhase, Symbol: symbols[0]}
		if len(observations) != 0 {
			cursor.Skip = make([]string, 0, len(observations))
			for _, observation := range observations {
				cursor.Skip = append(cursor.Skip, spotObservationIdentity(observation))
			}
			return source.EventPage{Observations: observations, NextCursor: encodeSpotCursor(cursor)}, nil
		}
	}
	if cursor.Phase != spotOrderHistoryPhase {
		return source.EventPage{}, errors.New("Binance Spot order cursor has an invalid phase")
	}
	return a.fetchOrderEventPage(ctx, request, symbols, cursor, apiKey, secret)
}

func (a *SpotAdapter) fetchOrderEventPage(
	ctx context.Context,
	request source.PageRequest,
	symbols []string,
	cursor spotCursor,
	apiKey string,
	secret string,
) (source.EventPage, error) {
	index, err := cursorSymbolIndex(symbols, cursor.Symbol)
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
		observations, err := normalizeSpotOrderObservations(rows, request.Account, a.client.now().UTC())
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
			next := spotCursor{Phase: spotOrderHistoryPhase, Symbol: symbol, FromID: nextID, Skip: cursor.Skip}
			if len(filtered) != 0 {
				return source.EventPage{Observations: filtered, NextCursor: encodeSpotCursor(next)}, nil
			}
			cursor = next
			continue
		}
		index++
		cursor.FromID = ""
		if len(filtered) != 0 {
			page := source.EventPage{Observations: filtered, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeSpotCursor(spotCursor{
					Phase: spotOrderHistoryPhase, Symbol: symbols[index], Skip: cursor.Skip,
				})
			}
			return page, nil
		}
	}
	return source.EventPage{Done: true}, nil
}

func (a *SpotAdapter) fetchOrderRows(
	ctx context.Context,
	request source.PageRequest,
	symbol string,
	fromID string,
	apiKey string,
	secret string,
) ([]spotOrderRow, string, bool, error) {
	query := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(spotPageLimit)}, "recvWindow": {strconv.Itoa(a.client.recvWindow)}}
	if fromID == "" {
		query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
		query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
	} else {
		query.Set("orderId", fromID)
	}
	var rows []spotOrderRow
	if err := a.client.signedGETOperation(ctx, spotOrdersPath, query, apiKey, secret, "spot orders", &rows); err != nil {
		return nil, "", false, err
	}
	pastWindow := false
	for _, row := range rows {
		// orderId pagination is creation-ordered; updateTime can move independently
		// when an older order remains open longer than newer orders.
		if row.Time > request.End.UnixMilli() {
			pastWindow = true
			break
		}
	}
	if len(rows) != spotPageLimit {
		return rows, "", pastWindow, nil
	}
	nextID, err := nextSpotID(string(rows[len(rows)-1].OrderID))
	if err != nil {
		return nil, "", false, fmt.Errorf("paginate Binance Spot orders: %w", err)
	}
	return rows, nextID, pastWindow, nil
}

func normalizeSpotOrderObservations(rows []spotOrderRow, account source.Account, observedAt time.Time) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeSpotOrderObservation(row, account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance Spot order row %d: %w", index, err)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func normalizeSpotOrderObservation(row spotOrderRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
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
	state := struct {
		Status             string `json:"status"`
		ClientOrderID      string `json:"clientOrderId"`
		Price              string `json:"price"`
		OrigQty            string `json:"origQty"`
		ExecutedQty        string `json:"executedQty"`
		CumulativeQuoteQty string `json:"cummulativeQuoteQty"`
		StopPrice          string `json:"stopPrice"`
		UpdateTime         int64  `json:"updateTime"`
		IsWorking          bool   `json:"isWorking"`
		WorkingTime        int64  `json:"workingTime"`
	}{
		Status: status, ClientOrderID: row.ClientOrderID, Price: row.Price, OrigQty: row.OrigQty,
		ExecutedQty: row.ExecutedQty, CumulativeQuoteQty: row.CumulativeQuoteQty,
		StopPrice: row.StopPrice, UpdateTime: row.UpdateTime, IsWorking: row.IsWorking, WorkingTime: row.WorkingTime,
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)
	return model.StateObservation{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		Stream: "spot", ObjectType: "order", ObjectID: symbol + ":" + orderID, Status: status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]), Symbol: symbol, Amount: strings.TrimSpace(row.ExecutedQty),
		OccurredAt: time.UnixMilli(row.UpdateTime).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func spotObservationIdentity(observation model.StateObservation) string {
	return observation.ObjectType + "\x00" + observation.ObjectID + "\x00" + observation.StateFingerprint
}

type spotTerminalOrderAdapter struct {
	spot *SpotAdapter
}

var _ source.Adapter = (*spotTerminalOrderAdapter)(nil)

func (a *SpotAdapter) TerminalOrders() source.Adapter {
	return &spotTerminalOrderAdapter{spot: a}
}

func (a *spotTerminalOrderAdapter) Exchange() model.Exchange {
	return a.spot.Exchange()
}

func (a *spotTerminalOrderAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.spot.CheckCredentials(account)
}

func (a *spotTerminalOrderAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateSpotWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.spot.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	symbols, err := a.spot.listedSymbols(ctx)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeSpotCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	if cursor.Phase != "" && cursor.Phase != spotOrderHistoryPhase {
		return source.Page{}, errors.New("Binance Spot terminal-order cursor has an invalid phase")
	}
	index, err := cursorSymbolIndex(symbols, cursor.Symbol)
	if err != nil {
		return source.Page{}, err
	}
	for index < len(symbols) {
		symbol := symbols[index]
		rows, nextID, pastWindow, err := a.spot.fetchOrderRows(ctx, request, symbol, cursor.FromID, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		entries := make([]model.LedgerEntry, 0, len(rows))
		for rowIndex, row := range rows {
			if !isTerminalSpotOrderStatus(strings.TrimSpace(row.Status)) {
				continue
			}
			entry, err := normalizeTerminalSpotOrder(row, request.Account, a.spot.client.now().UTC())
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize terminal Binance Spot order row %d: %w", rowIndex, err)
			}
			if !entry.OccurredAt.Before(request.Start) && !entry.OccurredAt.After(request.End) {
				entries = append(entries, entry)
			}
		}
		if nextID != "" && !pastWindow {
			next := spotCursor{Phase: spotOrderHistoryPhase, Symbol: symbol, FromID: nextID}
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeSpotCursor(next)}, nil
			}
			cursor = next
			continue
		}
		index++
		cursor.FromID = ""
		if len(entries) != 0 {
			page := source.Page{Entries: entries, Done: index == len(symbols)}
			if !page.Done {
				page.NextCursor = encodeSpotCursor(spotCursor{Phase: spotOrderHistoryPhase, Symbol: symbols[index]})
			}
			return page, nil
		}
	}
	return source.Page{Done: true}, nil
}

func isTerminalSpotOrderStatus(status string) bool {
	switch status {
	case "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH":
		return true
	default:
		return false
	}
}

func normalizeTerminalSpotOrder(row spotOrderRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	observation, err := normalizeSpotOrderObservation(row, account, observedAt)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: "spot-order:" + observation.ObjectID, Symbol: observation.Symbol, Category: "spot", Type: observation.Status,
		Side: strings.TrimSpace(row.Side), Amount: strings.TrimSpace(row.ExecutedQty),
		CashFlow: strings.TrimSpace(row.CumulativeQuoteQty), OrderID: strings.TrimSpace(string(row.OrderID)),
		Info: strings.TrimSpace(row.Type), OccurredAt: observation.OccurredAt, ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
