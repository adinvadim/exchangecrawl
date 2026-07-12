package bybit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFuturesFillsSignsPaginatesAndNormalizes(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-futures-key"
		secret = "fake-futures-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != futuresExecutionPath {
			t.Errorf("path = %q, want %q", r.URL.Path, futuresExecutionPath)
		}
		wantQuery := url.Values{
			"category":  {"linear"},
			"startTime": {"1700000000000"},
			"endTime":   {"1700000060000"},
			"limit":     {"100"},
		}.Encode()
		if r.URL.RawQuery != wantQuery {
			t.Errorf("query = %q, want %q", r.URL.RawQuery, wantQuery)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		_, _ = fmt.Fprint(w, `{
          "retCode":0,"retMsg":"OK","result":{
            "nextPageCursor":"fake-next-fill-cursor",
            "list":[{
              "symbol":"BTCUSDT","orderId":"fake-order-1","side":"Buy",
              "orderType":"Limit","execFee":"0.000000000000000001",
              "execId":"fake-exec-1","execPrice":"30000.01",
              "execQty":"0.000000000000000123","execType":"Trade",
              "execValue":"0.003690000000000001","execTime":"1700000030000",
              "feeCurrency":"USDT","closedSize":"0","isMaker":true,
              "futureField":"preserved"
            }]
          }
        }`)
	}))
	t.Cleanup(server.Close)

	fills, err := NewFuturesFills(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}
	page, err := fills.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch futures fills: %v", err)
	}
	if page.Done || page.NextCursor != "linear|fake-next-fill-cursor" || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.EntryID != "futures-fill:linear:fake-exec-1" || entry.TradeID != "fake-exec-1" || entry.OrderID != "fake-order-1" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "linear" || entry.Type != "Trade" || entry.Symbol != "BTCUSDT" || entry.Side != "Buy" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "0.000000000000000123" || entry.Fee != "0.000000000000000001" || entry.CashFlow != "0.003690000000000001" {
		t.Fatalf("entry decimals = %#v", entry)
	}
	if !entry.ObservedAt.Equal(requestTime) || !entry.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) {
		t.Fatalf("entry timestamps = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "preserved") {
		t.Fatalf("raw JSON did not preserve unknown field: %s", entry.RawJSON)
	}
}

func TestFuturesFillsDrainsLinearThenInverse(t *testing.T) {
	t.Parallel()

	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		category := r.URL.Query().Get("category")
		cursor := r.URL.Query().Get("cursor")
		switch {
		case category == "linear" && cursor == "":
			_, _ = fmt.Fprint(w, futuresExecResponse("linear-next", validFuturesExec("linear-exec-1")))
		case category == "linear" && cursor == "linear-next":
			_, _ = fmt.Fprint(w, futuresExecResponse("", validFuturesExec("linear-exec-2")))
		case category == "inverse" && cursor == "":
			_, _ = fmt.Fprint(w, futuresExecResponse("inverse-next", validFuturesExec("inverse-exec-1")))
		case category == "inverse" && cursor == "inverse-next":
			_, _ = fmt.Fprint(w, futuresExecResponse("", validFuturesExec("inverse-exec-2")))
		default:
			t.Errorf("unexpected request category=%q cursor=%q", category, cursor)
		}
	}))
	t.Cleanup(server.Close)

	fills, err := NewFuturesFills(spotTestOptions(server, requestTime, "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}

	request := spotPageRequest("")
	first, err := fills.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done || first.NextCursor != "linear|linear-next" || len(first.Entries) != 1 ||
		first.Entries[0].EntryID != "futures-fill:linear:linear-exec-1" {
		t.Fatalf("first page = %#v", first)
	}

	request.Cursor = first.NextCursor
	second, err := fills.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.Done || second.NextCursor != "inverse|" || len(second.Entries) != 1 ||
		second.Entries[0].EntryID != "futures-fill:linear:linear-exec-2" {
		t.Fatalf("second page = %#v", second)
	}

	request.Cursor = second.NextCursor
	third, err := fills.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if third.Done || third.NextCursor != "inverse|inverse-next" || len(third.Entries) != 1 ||
		third.Entries[0].EntryID != "futures-fill:inverse:inverse-exec-1" {
		t.Fatalf("third page = %#v", third)
	}

	request.Cursor = third.NextCursor
	fourth, err := fills.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("fourth page: %v", err)
	}
	if !fourth.Done || fourth.NextCursor != "" || len(fourth.Entries) != 1 ||
		fourth.Entries[0].EntryID != "futures-fill:inverse:inverse-exec-2" {
		t.Fatalf("fourth page = %#v", fourth)
	}
}

func TestFuturesFillsRejectsRepeatedProviderCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, futuresExecResponse("stuck-cursor", validFuturesExec("exec-1")))
	}))
	t.Cleanup(server.Close)

	fills, err := NewFuturesFills(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}
	_, err = fills.FetchPage(context.Background(), spotPageRequest("linear|stuck-cursor"))
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestFuturesFillsRejectsInvalidCursorAndWindow(t *testing.T) {
	t.Parallel()

	fills, err := NewFuturesFills(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}

	_, err = fills.FetchPage(context.Background(), spotPageRequest("provider-cursor-without-prefix"))
	if err == nil || !strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("cursor error = %v", err)
	}

	_, err = fills.FetchPage(context.Background(), spotPageRequest("spot|leaked-cursor"))
	if err == nil || !strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("unknown category error = %v", err)
	}

	wide := spotPageRequest("")
	wide.End = wide.Start.Add(8 * 24 * time.Hour)
	_, err = fills.FetchPage(context.Background(), wide)
	if err == nil || !strings.Contains(err.Error(), "exceeds seven days") {
		t.Fatalf("window error = %v", err)
	}
}

func TestFuturesFillsErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-futures-key"
		secret = "fake-futures-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"rejected key %s and secret %s","result":{}}`, apiKey, secret)
	}))
	t.Cleanup(server.Close)

	fills, err := NewFuturesFills(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), apiKey, secret))
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}
	_, err = fills.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil {
		t.Fatalf("expected API error")
	}
	message := err.Error()
	if !strings.Contains(message, "Bybit API error 10003") || !strings.Contains(message, "[REDACTED]") {
		t.Fatalf("error = %q, want redacted API error", message)
	}
	if strings.Contains(message, apiKey) || strings.Contains(message, secret) {
		t.Fatalf("error message leaked a secret: %q", message)
	}
}

func TestFuturesFillsRejectsMissingExecID(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, futuresExecResponse("", `{"symbol":"BTCUSDT","execTime":"1700000030000"}`))
	}))
	t.Cleanup(server.Close)

	fills, err := NewFuturesFills(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures fills adapter: %v", err)
	}
	_, err = fills.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "no execId") {
		t.Fatalf("error = %v, want missing execId", err)
	}
}

func futuresExecResponse(nextCursor string, rows ...string) string {
	return fmt.Sprintf(
		`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":[%s]}}`,
		nextCursor, strings.Join(rows, ","),
	)
}

func validFuturesExec(execID string) string {
	return fmt.Sprintf(`{
      "symbol":"BTCUSDT","orderId":"order-%[1]s","side":"Buy","orderType":"Limit",
      "execFee":"0.01","execId":%[1]q,"execPrice":"30000","execQty":"0.001",
      "execType":"Trade","execValue":"30","execTime":"1700000030000","feeCurrency":"USDT"
    }`, execID)
}
