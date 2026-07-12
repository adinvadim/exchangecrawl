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

func TestFuturesFillsFetchPageSignsRequestAndNormalizesFill(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-time.Hour)
	end := fixedNow.Add(-time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			if request.URL.Query().Get("signature") != "" || request.Header.Get("X-MBX-APIKEY") != "" {
				t.Error("exchangeInfo request was authenticated")
			}
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case futuresUserTradesPath:
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
			_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","id":698759,"orderId":25851813,"side":"SELL","positionSide":"SHORT","price":"7819.01","qty":"0.002","quoteQty":"15.63802","realizedPnl":"-0.91539999","commission":"0.07819010","commissionAsset":"USDT","time":1699999999000,"buyer":false,"maker":true}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
			http.NotFound(response, request)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestFuturesFillsAdapter(t, server, fixedNow)
	page, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: end,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 1 {
		t.Fatalf("page = done %t cursor %q entries %d", page.Done, page.NextCursor, len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.Exchange != model.ExchangeBinance || entry.EntryID != "futures-fill:BTCUSDT:698759" {
		t.Fatalf("identity = %#v", entry)
	}
	if entry.Category != "futures" || entry.Type != "fill" || entry.Symbol != "BTCUSDT" {
		t.Fatalf("classification = %#v", entry)
	}
	if entry.Amount != "0.002" || entry.Fee != "0.07819010" || entry.CashFlow != "15.63802" || entry.Asset != "USDT" {
		t.Fatalf("amounts = %#v", entry)
	}
	if entry.Side != "SELL" || entry.OrderID != "25851813" || entry.TradeID != "698759" || entry.Info != "maker SHORT" {
		t.Fatalf("provider fields = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) || !entry.ObservedAt.Equal(fixedNow) {
		t.Fatalf("times = %s / %s", entry.OccurredAt, entry.ObservedAt)
	}
	if !json.Valid(entry.RawJSON) || !strings.Contains(string(entry.RawJSON), `"realizedPnl":"-0.91539999"`) {
		t.Fatalf("raw JSON = %s", entry.RawJSON)
	}
}

func TestFuturesFillsPaginationAdvancesFromTradeID(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var tradeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
		case futuresUserTradesPath:
			tradeRequests++
			if tradeRequests == 1 {
				if request.URL.Query().Get("fromId") != "" {
					t.Errorf("first fromId = %q", request.URL.Query().Get("fromId"))
				}
				rows := make([]map[string]any, futuresFillsPageLimit)
				for index := range rows {
					rows[index] = futuresFillFixture(index + 1)
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
			_ = json.NewEncoder(response).Encode([]map[string]any{futuresFillFixture(1001)})
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestFuturesFillsAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: fixedNow,
	}
	first, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Entries) != futuresFillsPageLimit {
		t.Fatalf("first = done %t cursor %q entries %d", first.Done, first.NextCursor, len(first.Entries))
	}
	request.Cursor = first.NextCursor
	second, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Entries) != 1 || second.Entries[0].TradeID != "1001" {
		t.Fatalf("second = done %t entries %#v", second.Done, second.Entries)
	}
}

func TestFuturesFillsSweepsSymbolsAndFiltersWindow(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := time.UnixMilli(1_699_999_500_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case futuresExchangeInfoPath:
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"},{"symbol":"ETHUSDT"}]}`))
		case futuresUserTradesPath:
			switch request.URL.Query().Get("symbol") {
			case "BTCUSDT":
				// One in-window fill plus one before the window (must be dropped)
				// and one after the window (must be dropped).
				_, _ = response.Write([]byte(`[` +
					`{"symbol":"BTCUSDT","id":1,"orderId":10,"side":"BUY","positionSide":"BOTH","price":"1","qty":"1","quoteQty":"1","realizedPnl":"0","commission":"0","commissionAsset":"USDT","time":1699999000000,"buyer":true,"maker":false},` +
					`{"symbol":"BTCUSDT","id":2,"orderId":11,"side":"BUY","positionSide":"BOTH","price":"1","qty":"1","quoteQty":"1","realizedPnl":"0","commission":"0","commissionAsset":"USDT","time":1699999600000,"buyer":true,"maker":false},` +
					`{"symbol":"BTCUSDT","id":3,"orderId":12,"side":"SELL","positionSide":"BOTH","price":"1","qty":"1","quoteQty":"1","realizedPnl":"0","commission":"0","commissionAsset":"USDT","time":1700000600000,"buyer":false,"maker":false}` +
					`]`))
			case "ETHUSDT":
				_, _ = response.Write([]byte(`[{"symbol":"ETHUSDT","id":9,"orderId":90,"side":"SELL","positionSide":"BOTH","price":"1","qty":"1","quoteQty":"1","realizedPnl":"0","commission":"0","commissionAsset":"USDT","time":1699999700000,"buyer":false,"maker":true}]`))
			default:
				t.Errorf("unexpected symbol %q", request.URL.Query().Get("symbol"))
			}
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestFuturesFillsAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: start, End: fixedNow}

	first, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Entries) != 1 {
		t.Fatalf("first = done %t cursor %q entries %#v", first.Done, first.NextCursor, first.Entries)
	}
	if first.Entries[0].EntryID != "futures-fill:BTCUSDT:2" {
		t.Fatalf("in-window entry = %#v", first.Entries[0])
	}

	request.Cursor = first.NextCursor
	second, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Entries) != 1 || second.Entries[0].EntryID != "futures-fill:ETHUSDT:9" {
		t.Fatalf("second = done %t entries %#v", second.Done, second.Entries)
	}
}

func TestFuturesFillsRejectsInvalidWindow(t *testing.T) {
	t.Parallel()

	adapter := newTestFuturesFillsAdapter(t, httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})), time.UnixMilli(1_700_000_000_000).UTC())
	cases := map[string]source.PageRequest{
		"missing bounds": {Account: source.Account{ID: "primary"}},
		"reversed":       {Account: source.Account{ID: "primary"}, Start: time.UnixMilli(1_700_000_000_000), End: time.UnixMilli(1_699_999_000_000)},
		"too wide":       {Account: source.Account{ID: "primary"}, Start: time.UnixMilli(1_700_000_000_000).Add(-8 * 24 * time.Hour), End: time.UnixMilli(1_700_000_000_000)},
	}
	for name, request := range cases {
		request := request
		t.Run(name, func(t *testing.T) {
			if _, err := adapter.FetchPage(t.Context(), request); err == nil {
				t.Fatal("expected window validation error")
			}
		})
	}
}

func TestFuturesFillsRejectsRepeatedCursorSymbol(t *testing.T) {
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

	adapter := newTestFuturesFillsAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
		Cursor: encodeFuturesFillCursor(futuresFillCursor{Symbol: "UNLISTEDUSDT"}),
	}
	if _, err := adapter.FetchPage(t.Context(), request); err == nil {
		t.Fatal("expected unavailable-symbol cursor error")
	}
}

func TestFuturesFillsRedactSecretsInErrors(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == futuresExchangeInfoPath {
			_, _ = response.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`))
			return
		}
		// Echo the signed query, api key, and signature back inside the provider
		// error message to prove they are stripped before surfacing.
		signature := request.URL.Query().Get("signature")
		apiKey := request.Header.Get("X-MBX-APIKEY")
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"rejected key=` + apiKey + ` signature=` + signature + `"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestFuturesFillsAdapter(t, server, fixedNow)
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-key") || strings.Contains(apiError.Message, "fake-api-secret") {
		t.Fatalf("error leaked a credential: %q", apiError.Message)
	}
	if strings.Contains(apiError.Message, "signature=") && !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("error did not redact signature: %q", apiError.Message)
	}
}

func TestFuturesFillsExchangeInfoRejectsMalformedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"symbols":`))
	}))
	t.Cleanup(server.Close)
	adapter := newTestFuturesFillsAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: time.UnixMilli(1_699_999_000_000), End: time.UnixMilli(1_700_000_000_000),
	})
	var requestError *RequestError
	if !errors.As(err, &requestError) {
		t.Fatalf("error = %T %v", err, err)
	}
}

func newTestFuturesFillsAdapter(t *testing.T, server *httptest.Server, now time.Time) *FuturesFillsAdapter {
	t.Helper()
	adapter, err := NewFuturesFills(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}
	return adapter
}

func futuresFillFixture(id int) map[string]any {
	return map[string]any{
		"symbol": "BTCUSDT", "id": id, "orderId": id + 10_000,
		"side": "BUY", "positionSide": "BOTH", "price": "60000.0", "qty": "0.001", "quoteQty": "60.0",
		"realizedPnl": "0", "commission": "0.024", "commissionAsset": "USDT",
		"time": int64(1_699_999_999_000), "buyer": true, "maker": false,
	}
}
