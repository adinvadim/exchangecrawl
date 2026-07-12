package binance

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestSpotFetchEventPageCapturesOpenAndHistoricalOrderStates(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case spotExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case spotOpenOrdersPath:
			assertSpotSignature(t, request, "fake-api-secret")
			if request.URL.Query().Get("symbol") != "" {
				t.Errorf("openOrders symbol = %q, want all symbols", request.URL.Query().Get("symbol"))
			}
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","orderId":29,"clientOrderId":"fake-client-order","price":"61234.56000000","origQty":"0.00200000","executedQty":"0.00100000","cummulativeQuoteQty":"61.23456000","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000","time":1699999000000,"updateTime":1699999999000,"isWorking":true,"workingTime":1699999000000,"selfTradePreventionMode":"EXPIRE_MAKER"}]`))
		case spotOrdersPath:
			assertSpotSignature(t, request, "fake-api-secret")
			query := request.URL.Query()
			for key, want := range map[string]string{
				"symbol": "BTCUSDT", "startTime": strconv.FormatInt(start.UnixMilli(), 10),
				"endTime": strconv.FormatInt(fixedNow.UnixMilli(), 10), "limit": "1000",
				"recvWindow": "5000", "timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
			} {
				if got := query.Get(key); got != want {
					t.Errorf("allOrders %s = %q, want %q", key, got, want)
				}
			}
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","orderId":29,"clientOrderId":"fake-client-order","price":"61234.56000000","origQty":"0.00200000","executedQty":"0.00200000","cummulativeQuoteQty":"122.46912000","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000","time":1699999000000,"updateTime":1699999999500,"isWorking":false,"workingTime":1699999000000,"selfTradePreventionMode":"EXPIRE_MAKER"}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	spot := newTestSpotAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}
	openPage, err := spot.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch open orders: %v", err)
	}
	if openPage.Done || openPage.NextCursor == "" || len(openPage.Observations) != 1 {
		t.Fatalf("open page = done %t cursor %q observations %d", openPage.Done, openPage.NextCursor, len(openPage.Observations))
	}
	open := openPage.Observations[0]
	if open.ObjectType != "order" || open.ObjectID != "BTCUSDT:29" || open.Status != "PARTIALLY_FILLED" {
		t.Fatalf("open identity = %#v", open)
	}
	if open.Stream != "spot" || open.Amount != "0.00100000" || open.StateFingerprint == "" {
		t.Fatalf("open state = %#v", open)
	}

	request.Cursor = openPage.NextCursor
	historyPage, err := spot.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch order history: %v", err)
	}
	if !historyPage.Done || len(historyPage.Observations) != 1 {
		t.Fatalf("history page = done %t observations %d", historyPage.Done, len(historyPage.Observations))
	}
	terminal := historyPage.Observations[0]
	if terminal.Status != "FILLED" || terminal.Amount != "0.00200000" {
		t.Fatalf("terminal state = %#v", terminal)
	}
	if terminal.StateFingerprint == open.StateFingerprint {
		t.Fatal("mutable order states share a fingerprint")
	}
	if !json.Valid(terminal.RawJSON) || !strings.Contains(string(terminal.RawJSON), `"status":"FILLED"`) {
		t.Fatalf("raw JSON = %s", terminal.RawJSON)
	}
}

func TestSpotTerminalOrdersProduceStableLedgerEntries(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case spotExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"ETHUSDT"}]}`))
		case spotOrdersPath:
			_, _ = response.Write([]byte(`[{"symbol":"ETHUSDT","orderId":41,"clientOrderId":"fake-terminal-order","price":"2500.00000000","origQty":"0.50000000","executedQty":"0.50000000","cummulativeQuoteQty":"1250.00000000","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"SELL","stopPrice":"0.00000000","time":1699999000000,"updateTime":1699999999000,"isWorking":false,"workingTime":1699999000000,"selfTradePreventionMode":"NONE"}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	spot := newTestSpotAdapter(t, server, fixedNow)
	terminal := spot.TerminalOrders()
	page, err := terminal.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch terminal orders: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 {
		t.Fatalf("page = done %t entries %d", page.Done, len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.EntryID != "spot-order:ETHUSDT:41" || entry.Type != "FILLED" || entry.Amount != "0.50000000" {
		t.Fatalf("entry = %#v", entry)
	}
	if entry.CashFlow != "1250.00000000" {
		t.Fatalf("cash flow = %q", entry.CashFlow)
	}
	if entry.Side != "SELL" || entry.OrderID != "41" || !entry.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) {
		t.Fatalf("provider fields = %#v", entry)
	}
}

func TestSpotEndpointsReturnStructuredErrors(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{spotExchangeInfoPath, spotTradesPath, spotOpenOrdersPath, spotOrdersPath} {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path == spotExchangeInfoPath && endpoint != spotExchangeInfoPath {
					_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
					return
				}
				response.WriteHeader(http.StatusBadRequest)
				_, _ = response.Write([]byte(`{"code":-1022,"msg":"fake request rejected"}`))
			}))
			t.Cleanup(server.Close)
			spot := newTestSpotAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
			request := source.PageRequest{
				Account: source.Account{ID: "primary", Label: "Primary"},
				Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
			}
			var err error
			switch endpoint {
			case spotExchangeInfoPath:
				_, err = spot.FetchPage(t.Context(), request)
			case spotTradesPath:
				_, err = spot.FetchPage(t.Context(), request)
			case spotOpenOrdersPath:
				_, err = spot.FetchEventPage(t.Context(), request)
			case spotOrdersPath:
				request.Cursor = encodeSpotCursor(spotCursor{Phase: spotOrderHistoryPhase, Symbol: "BTCUSDT"})
				_, err = spot.FetchEventPage(t.Context(), request)
			}
			var apiError *APIError
			if !errors.As(err, &apiError) || apiError.Code != -1022 {
				t.Fatalf("error = %T %v", err, err)
			}
		})
	}
}

func TestSpotExchangeInfoRejectsMalformedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"symbols":`))
	}))
	t.Cleanup(server.Close)
	spot := newTestSpotAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := spot.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: time.UnixMilli(1_699_999_000_000), End: time.UnixMilli(1_700_000_000_000),
	})
	var requestError *RequestError
	if !errors.As(err, &requestError) {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestSpotTradePaginationAdvancesFromTradeID(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var tradeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case spotExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case spotTradesPath:
			tradeRequests++
			if tradeRequests == 1 {
				if request.URL.Query().Get("fromId") != "" {
					t.Errorf("first fromId = %q", request.URL.Query().Get("fromId"))
				}
				rows := make([]map[string]any, spotPageLimit)
				for index := range rows {
					rows[index] = spotTradeFixture(index + 1)
				}
				_ = json.NewEncoder(response).Encode(rows)
				return
			}
			if got := request.URL.Query().Get("fromId"); got != "1001" {
				t.Errorf("second fromId = %q, want 1001", got)
			}
			if request.URL.Query().Get("startTime") != "" || request.URL.Query().Get("endTime") != "" {
				t.Error("fromId page also sent a time range")
			}
			_ = json.NewEncoder(response).Encode([]map[string]any{spotTradeFixture(1001)})
		}
	}))
	t.Cleanup(server.Close)

	spot := newTestSpotAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: fixedNow,
	}
	first, err := spot.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Entries) != spotPageLimit {
		t.Fatalf("first = done %t cursor %q entries %d", first.Done, first.NextCursor, len(first.Entries))
	}
	request.Cursor = first.NextCursor
	second, err := spot.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Entries) != 1 || second.Entries[0].TradeID != "1001" {
		t.Fatalf("second = done %t entries %#v", second.Done, second.Entries)
	}
}

func TestSpotOrderPaginationAdvancesFromOrderID(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var orderRequests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case spotExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case spotOpenOrdersPath:
			_, _ = response.Write([]byte(`[]`))
		case spotOrdersPath:
			orderRequests++
			if orderRequests == 1 {
				rows := make([]map[string]any, spotPageLimit)
				for index := range rows {
					rows[index] = spotOrderFixture(index+1, "FILLED")
				}
				_ = json.NewEncoder(response).Encode(rows)
				return
			}
			if got := request.URL.Query().Get("orderId"); got != "1001" {
				t.Errorf("second orderId = %q, want 1001", got)
			}
			if request.URL.Query().Get("startTime") != "" || request.URL.Query().Get("endTime") != "" {
				t.Error("orderId page also sent a time range")
			}
			_ = json.NewEncoder(response).Encode([]map[string]any{spotOrderFixture(1001, "FILLED")})
		}
	}))
	t.Cleanup(server.Close)

	spot := newTestSpotAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: fixedNow,
	}
	first, err := spot.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Observations) != spotPageLimit {
		t.Fatalf("first = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	request.Cursor = first.NextCursor
	second, err := spot.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Observations) != 1 || second.Observations[0].ObjectID != "BTCUSDT:1001" {
		t.Fatalf("second = done %t observations %#v", second.Done, second.Observations)
	}
}

func TestSpotFetchPageSignsTradeRequestAndNormalizesFill(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-time.Hour)
	end := fixedNow.Add(-time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case spotExchangeInfoPath:
			if request.URL.Query().Get("signature") != "" || request.Header.Get("X-MBX-APIKEY") != "" {
				t.Error("exchangeInfo request was authenticated")
			}
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case spotTradesPath:
			if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
				t.Errorf("API key header = %q", got)
			}
			query := request.URL.Query()
			for key, want := range map[string]string{
				"symbol": "BTCUSDT", "startTime": strconv.FormatInt(start.UnixMilli(), 10),
				"endTime": strconv.FormatInt(end.UnixMilli(), 10), "limit": "1000",
				"recvWindow": "5000", "timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
			} {
				if got := query.Get(key); got != want {
					t.Errorf("query %s = %q, want %q", key, got, want)
				}
			}
			assertSpotSignature(t, request, "fake-api-secret")
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","id":17,"orderId":29,"orderListId":-1,"price":"61234.56000000","qty":"0.00100000","quoteQty":"61.23456000","commission":"0.00000100","commissionAsset":"BTC","time":1699999999000,"isBuyer":true,"isMaker":false,"isBestMatch":true}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
			http.NotFound(response, request)
		}
	}))
	t.Cleanup(server.Close)

	spot := newTestSpotAdapter(t, server, fixedNow)
	page, err := spot.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: end,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 1 {
		t.Fatalf("page = done %t cursor %q entries %d", page.Done, page.NextCursor, len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.Exchange != model.ExchangeBinance || entry.EntryID != "spot-fill:BTCUSDT:17" {
		t.Fatalf("identity = %#v", entry)
	}
	if entry.Category != "spot" || entry.Type != "fill" || entry.Symbol != "BTCUSDT" {
		t.Fatalf("classification = %#v", entry)
	}
	if entry.Amount != "0.00100000" || entry.Fee != "0.00000100" || entry.CashFlow != "61.23456000" || entry.Asset != "BTC" {
		t.Fatalf("amounts = %#v", entry)
	}
	if entry.Side != "BUY" || entry.OrderID != "29" || entry.TradeID != "17" {
		t.Fatalf("provider fields = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) || !entry.ObservedAt.Equal(fixedNow) {
		t.Fatalf("times = %s / %s", entry.OccurredAt, entry.ObservedAt)
	}
	if !json.Valid(entry.RawJSON) || !strings.Contains(string(entry.RawJSON), `"quoteQty":"61.23456000"`) {
		t.Fatalf("raw JSON = %s", entry.RawJSON)
	}
}

func newTestSpotAdapter(t *testing.T, server *httptest.Server, now time.Time) *SpotAdapter {
	t.Helper()
	adapter, err := NewSpot(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new spot adapter: %v", err)
	}
	return adapter
}

func assertSpotSignature(t *testing.T, request *http.Request, secret string) {
	t.Helper()
	rawQuery := request.URL.RawQuery
	signatureIndex := strings.LastIndex(rawQuery, "&signature=")
	if signatureIndex == -1 {
		t.Fatalf("raw query has no trailing signature: %q", rawQuery)
	}
	unsigned := rawQuery[:signatureIndex]
	if got, want := request.URL.Query().Get("signature"), sign(secret, unsigned); got != want {
		t.Errorf("signature = %q, want %q", got, want)
	}
}

func spotTradeFixture(id int) map[string]any {
	return map[string]any{
		"symbol": "BTCUSDT", "id": id, "orderId": id + 10_000,
		"price": "60000.00000000", "qty": "0.00100000", "quoteQty": "60.00000000",
		"commission": "0.00000100", "commissionAsset": "BTC", "time": int64(1_699_999_999_000),
		"isBuyer": true, "isMaker": false, "isBestMatch": true,
	}
}

func spotOrderFixture(id int, status string) map[string]any {
	return map[string]any{
		"symbol": "BTCUSDT", "orderId": id, "clientOrderId": "fake-order-" + strconv.Itoa(id),
		"price": "60000.00000000", "origQty": "0.00100000", "executedQty": "0.00100000",
		"cummulativeQuoteQty": "60.00000000", "status": status, "timeInForce": "GTC",
		"type": "LIMIT", "side": "BUY", "stopPrice": "0.00000000", "time": int64(1_699_999_000_000),
		"updateTime": int64(1_699_999_999_000), "isWorking": false, "workingTime": int64(1_699_999_000_000),
		"selfTradePreventionMode": "NONE",
	}
}
