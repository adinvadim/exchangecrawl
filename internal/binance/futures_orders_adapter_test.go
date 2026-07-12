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

func TestFuturesOrdersFetchEventPageCapturesOpenAndHistoricalStates(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case futuresOpenOrdersPath:
			assertSpotSignature(t, request, "fake-api-secret")
			if request.URL.Query().Get("symbol") != "" {
				t.Errorf("openOrders symbol = %q, want all symbols", request.URL.Query().Get("symbol"))
			}
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","orderId":29,"clientOrderId":"fake-client-order","price":"61234.56","avgPrice":"0.00000","origQty":"0.002","executedQty":"0.001","cumQuote":"61.23456","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","positionSide":"LONG","stopPrice":"0","time":1699999000000,"updateTime":1699999999000}]`))
		case futuresAllOrdersPath:
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
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","orderId":29,"clientOrderId":"fake-client-order","price":"61234.56","avgPrice":"61234.56","origQty":"0.002","executedQty":"0.002","cumQuote":"122.46912","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","positionSide":"LONG","stopPrice":"0","time":1699999000000,"updateTime":1699999999500}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}
	openPage, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch open orders: %v", err)
	}
	if openPage.Done || openPage.NextCursor == "" || len(openPage.Observations) != 1 {
		t.Fatalf("open page = done %t cursor %q observations %d", openPage.Done, openPage.NextCursor, len(openPage.Observations))
	}
	open := openPage.Observations[0]
	if open.ObjectType != "order" || open.ObjectID != "futures-order:BTCUSDT:29" || open.Status != "PARTIALLY_FILLED" {
		t.Fatalf("open identity = %#v", open)
	}
	if open.Stream != "futures" || open.Amount != "0.001" || open.StateFingerprint == "" {
		t.Fatalf("open state = %#v", open)
	}
	if !open.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) || !open.ObservedAt.Equal(fixedNow) {
		t.Fatalf("open times = %s / %s", open.OccurredAt, open.ObservedAt)
	}

	request.Cursor = openPage.NextCursor
	historyPage, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch order history: %v", err)
	}
	if !historyPage.Done || len(historyPage.Observations) != 1 {
		t.Fatalf("history page = done %t observations %d", historyPage.Done, len(historyPage.Observations))
	}
	terminal := historyPage.Observations[0]
	if terminal.Status != "FILLED" || terminal.Amount != "0.002" {
		t.Fatalf("terminal state = %#v", terminal)
	}
	if terminal.StateFingerprint == open.StateFingerprint {
		t.Fatal("mutable order states share a fingerprint")
	}
	if !json.Valid(terminal.RawJSON) || !strings.Contains(string(terminal.RawJSON), `"status":"FILLED"`) {
		t.Fatalf("raw JSON = %s", terminal.RawJSON)
	}
}

func TestFuturesOrdersSkipsAlreadyReportedOpenOrder(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-time.Hour)
	// The open snapshot and the history sweep return the exact same open order
	// state; the history phase must dedup it via the cursor Skip set.
	openRow := `{"symbol":"BTCUSDT","orderId":29,"clientOrderId":"fake-client-order","price":"61234.56","avgPrice":"0.00000","origQty":"0.002","executedQty":"0.001","cumQuote":"61.23456","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","positionSide":"LONG","stopPrice":"0","time":1699999000000,"updateTime":1699999999000}`
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case futuresOpenOrdersPath:
			_, _ = response.Write([]byte("[" + openRow + "]"))
		case futuresAllOrdersPath:
			_, _ = response.Write([]byte("[" + openRow + "]"))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: start, End: fixedNow}
	openPage, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch open orders: %v", err)
	}
	request.Cursor = openPage.NextCursor
	historyPage, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch order history: %v", err)
	}
	if !historyPage.Done || len(historyPage.Observations) != 0 {
		t.Fatalf("history page = done %t observations %d (duplicate not skipped)", historyPage.Done, len(historyPage.Observations))
	}
}

func TestFuturesOrdersTerminalOrdersProduceStableLedgerEntries(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"ETHUSDT"}]}`))
		case futuresAllOrdersPath:
			// Two rows: one still open (must be excluded), one terminal.
			_, _ = response.Write([]byte(`[{"symbol":"ETHUSDT","orderId":40,"clientOrderId":"fake-open","price":"2500","avgPrice":"0","origQty":"0.5","executedQty":"0","cumQuote":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"SELL","positionSide":"SHORT","stopPrice":"0","time":1699999000000,"updateTime":1699999500000},{"symbol":"ETHUSDT","orderId":41,"clientOrderId":"fake-terminal","price":"2500","avgPrice":"2500","origQty":"0.5","executedQty":"0.5","cumQuote":"1250","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"SELL","positionSide":"SHORT","stopPrice":"0","time":1699999000000,"updateTime":1699999999000}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, fixedNow)
	terminal := orders.TerminalOrders()
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
	if entry.Exchange != model.ExchangeBinance || entry.EntryID != "futures-order:ETHUSDT:41" || entry.Type != "FILLED" || entry.Amount != "0.5" {
		t.Fatalf("entry = %#v", entry)
	}
	if entry.Category != "futures" || entry.CashFlow != "1250" || entry.Side != "SELL" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.OrderID != "41" || entry.Info != "LIMIT SHORT" || !entry.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) {
		t.Fatalf("provider fields = %#v", entry)
	}
}

func TestFuturesOrdersPaginationAdvancesFromOrderID(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var orderRequests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case futuresOpenOrdersPath:
			_, _ = response.Write([]byte(`[]`))
		case futuresAllOrdersPath:
			orderRequests++
			if orderRequests == 1 {
				rows := make([]map[string]any, futuresOrdersPageLimit)
				for index := range rows {
					rows[index] = futuresOrderFixture(index+1, "FILLED")
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
			_ = json.NewEncoder(response).Encode([]map[string]any{futuresOrderFixture(1001, "FILLED")})
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour), End: fixedNow,
	}
	first, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Observations) != futuresOrdersPageLimit {
		t.Fatalf("first = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Observations) != 1 || second.Observations[0].ObjectID != "futures-order:BTCUSDT:1001" {
		t.Fatalf("second = done %t observations %#v", second.Done, second.Observations)
	}
}

func TestFuturesOrdersRejectsWindowOverSevenDays(t *testing.T) {
	t.Parallel()

	orders := newTestFuturesOrdersAdapter(t, httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})), time.UnixMilli(1_700_000_000_000).UTC())
	start := time.UnixMilli(1_700_000_000_000).UTC()
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: start, End: start.Add(futuresOrdersMaxWindow + time.Millisecond)}
	if _, err := orders.FetchEventPage(t.Context(), request); err == nil || !strings.Contains(err.Error(), "7 days") {
		t.Fatalf("event window error = %v", err)
	}
	if _, err := orders.TerminalOrders().FetchPage(t.Context(), request); err == nil || !strings.Contains(err.Error(), "7 days") {
		t.Fatalf("ledger window error = %v", err)
	}
}

func TestFuturesOrdersCursorRejectsUnknownSymbolAndPhase(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == futuresExchangeInfoPath {
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
			return
		}
		t.Errorf("unexpected path %q", request.URL.Path)
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow}

	request.Cursor = encodeFuturesOrderCursor(futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: "DOGEUSDT"})
	if _, err := orders.FetchEventPage(t.Context(), request); err == nil || !strings.Contains(err.Error(), "unavailable symbol") {
		t.Fatalf("unknown symbol error = %v", err)
	}

	request.Cursor = encodeFuturesOrderCursor(futuresOrderCursor{Phase: "bogus", Symbol: "BTCUSDT"})
	if _, err := orders.TerminalOrders().FetchPage(t.Context(), request); err == nil || !strings.Contains(err.Error(), "invalid phase") {
		t.Fatalf("invalid phase error = %v", err)
	}

	if _, err := decodeFuturesOrderCursor("not base64!!"); err == nil {
		t.Fatal("malformed cursor accepted")
	}
}

func TestFuturesOrdersEndpointsReturnStructuredErrors(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{futuresExchangeInfoPath, futuresOpenOrdersPath, futuresAllOrdersPath} {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path == futuresExchangeInfoPath && endpoint != futuresExchangeInfoPath {
					_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
					return
				}
				response.WriteHeader(http.StatusBadRequest)
				_, _ = response.Write([]byte(`{"code":-1022,"msg":"fake request rejected"}`))
			}))
			t.Cleanup(server.Close)
			orders := newTestFuturesOrdersAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
			request := source.PageRequest{
				Account: source.Account{ID: "primary", Label: "Primary"},
				Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
			}
			var err error
			switch endpoint {
			case futuresExchangeInfoPath, futuresOpenOrdersPath:
				_, err = orders.FetchEventPage(t.Context(), request)
			case futuresAllOrdersPath:
				request.Cursor = encodeFuturesOrderCursor(futuresOrderCursor{Phase: futuresOrderHistoryPhase, Symbol: "BTCUSDT"})
				_, err = orders.FetchEventPage(t.Context(), request)
			}
			var apiError *APIError
			if !errors.As(err, &apiError) || apiError.Code != -1022 {
				t.Fatalf("error = %T %v", err, err)
			}
		})
	}
}

func TestFuturesOrdersErrorMessagesRedactSecrets(t *testing.T) {
	t.Parallel()

	// The provider echoes the signed URL (key + signature) back in its message;
	// the adapter must strip those before surfacing the error.
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == futuresExchangeInfoPath {
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
			return
		}
		apiKey := request.Header.Get("X-MBX-APIKEY")
		signature := request.URL.Query().Get("signature")
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"rejected key ` + apiKey + ` signature ` + signature + `"}`))
	}))
	t.Cleanup(server.Close)

	orders := newTestFuturesOrdersAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := orders.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	})
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-key") {
		t.Fatalf("message leaked the api key: %q", apiError.Message)
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("message not redacted: %q", apiError.Message)
	}
}

func newTestFuturesOrdersAdapter(t *testing.T, server *httptest.Server, now time.Time) *FuturesOrdersAdapter {
	t.Helper()
	adapter, err := NewFuturesOrders(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	return adapter
}

func futuresOrderFixture(id int, status string) map[string]any {
	return map[string]any{
		"symbol": "BTCUSDT", "orderId": id, "clientOrderId": "fake-order-" + strconv.Itoa(id),
		"price": "60000", "avgPrice": "60000", "origQty": "0.001", "executedQty": "0.001",
		"cumQuote": "60", "status": status, "timeInForce": "GTC", "type": "LIMIT", "side": "BUY",
		"positionSide": "BOTH", "stopPrice": "0", "time": int64(1_699_999_000_000),
		"updateTime": int64(1_699_999_999_000),
	}
}
