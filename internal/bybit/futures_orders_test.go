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

	"github.com/adinvadim/exchangecrawl/internal/source"
)

// futureWindowNow keeps the request window fresh (inside the realtime grace) so
// the event traversal reaches the open-order phase.
const futuresWindowNow = 1_700_000_123_456

func TestFuturesOrdersTraversesCategoriesAndPhases(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-fut-key"
		secret = "fake-fut-secret"
	)
	requestTime := time.UnixMilli(futuresWindowNow).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		category := r.URL.Query().Get("category")
		switch r.URL.Path {
		case futuresOrderHistoryPath:
			wantQuery := url.Values{
				"category":  {category},
				"startTime": {"1700000000000"},
				"endTime":   {"1700000060000"},
				"limit":     {"50"},
			}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("history query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("hist-"+category, "Filled", "history")))
		case futuresOpenOrdersPath:
			wantQuery := url.Values{"category": {category}, "limit": {"50"}, "openOnly": {"1"}}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("open query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("open-"+category, "New", "open")))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewFuturesOrders(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}

	// history:linear -> history:inverse
	request := spotPageRequest("")
	first, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("history linear: %v", err)
	}
	if first.Done || first.NextCursor != "history:inverse:" || len(first.Observations) != 1 {
		t.Fatalf("history linear page = %#v", first)
	}
	obs := first.Observations[0]
	if obs.ObjectID != "futures-order:linear:hist-linear" || obs.Stream != "futures" || obs.ObjectType != "futures_order" {
		t.Fatalf("observation identity = %#v", obs)
	}
	if obs.Status != "Filled" || obs.StateFingerprint == "" || obs.Amount != "1.000000000000000001" {
		t.Fatalf("observation normalization = %#v", obs)
	}
	if !obs.ObservedAt.Equal(requestTime) || !obs.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) {
		t.Fatalf("observation timestamps = %#v", obs)
	}
	if !containsJSONField(obs.RawJSON, "futureField", "history") {
		t.Fatalf("raw JSON not preserved: %s", obs.RawJSON)
	}

	// history:inverse -> open:linear (window is fresh)
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("history inverse: %v", err)
	}
	if second.Done || second.NextCursor != "open:linear:" || second.Observations[0].ObjectID != "futures-order:inverse:hist-inverse" {
		t.Fatalf("history inverse page = %#v", second)
	}

	// open:linear -> open:inverse
	request.Cursor = second.NextCursor
	third, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("open linear: %v", err)
	}
	if third.Done || third.NextCursor != "open:inverse:" || third.Observations[0].ObjectID != "futures-order:linear:open-linear" {
		t.Fatalf("open linear page = %#v", third)
	}

	// open:inverse -> done
	request.Cursor = third.NextCursor
	fourth, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("open inverse: %v", err)
	}
	if !fourth.Done || fourth.NextCursor != "" || fourth.Observations[0].ObjectID != "futures-order:inverse:open-inverse" {
		t.Fatalf("open inverse page = %#v", fourth)
	}
}

func TestFuturesOrdersPaginateWithinCategory(t *testing.T) {
	t.Parallel()

	var historyCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != futuresOrderHistoryPath {
			t.Errorf("path = %q, want history", r.URL.Path)
		}
		historyCalls++
		if historyCalls == 1 {
			_, _ = fmt.Fprint(w, futuresOrderResponse("fake-next", futuresOrder("hist-1", "Filled", "p1")))
			return
		}
		if got := r.URL.Query().Get("cursor"); got != "fake-next" {
			t.Errorf("cursor = %q, want fake-next", got)
		}
		_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("hist-2", "Cancelled", "p2")))
	}))
	t.Cleanup(server.Close)

	orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	request := spotPageRequest("")
	first, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor != "history:linear:fake-next" || len(first.Observations) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	// Second linear page exhausts the category, so the traversal moves to inverse.
	if second.NextCursor != "history:inverse:" || second.Observations[0].ObjectID != "futures-order:linear:hist-2" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestFuturesOrdersSkipRealtimeForHistoricalWindow(t *testing.T) {
	t.Parallel()

	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path+"?"+r.URL.Query().Get("category"))
		if r.URL.Path != futuresOrderHistoryPath {
			t.Errorf("path = %q, want history", r.URL.Path)
		}
		_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("hist", "Filled", "old")))
	}))
	t.Cleanup(server.Close)

	// Now is far past the window end + grace, so the open phase must be skipped.
	orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(1_700_100_000_000), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	request := spotPageRequest("")
	first, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("history linear: %v", err)
	}
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("history inverse: %v", err)
	}
	if !second.Done || second.NextCursor != "" {
		t.Fatalf("historical window should end after inverse history: %#v", second)
	}
	if strings.Join(paths, ",") != futuresOrderHistoryPath+"?linear,"+futuresOrderHistoryPath+"?inverse" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestFuturesTerminalOrdersBecomeStableLedgerEntries(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != futuresOrderHistoryPath {
			t.Errorf("path = %q, want history", r.URL.Path)
		}
		switch r.URL.Query().Get("category") {
		case "linear":
			// Open orders are filtered out; only the terminal one is archived.
			_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"nextPageCursor":"","list":[
              {"orderId":"open-order","orderStatus":"New","updatedTime":"1700000030000"},
              {"orderId":"filled-order","symbol":"BTCUSDT","side":"Buy","orderType":"Limit",
               "orderStatus":"Filled","cumExecQty":"1.234567890123456789",
               "cumExecValue":"40000.123456789012345678","cumExecFee":"0.000000000000000001",
               "feeCurrency":"USDT","updatedTime":"1700000040000","futureField":"terminal"}
            ]}}`)
		default:
			_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("inv-cancelled", "Cancelled", "inv")))
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	request := spotPageRequest("")
	linear, err := orders.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("linear terminal page: %v", err)
	}
	if linear.NextCursor != "history:inverse:" || len(linear.Entries) != 1 {
		t.Fatalf("linear page = %#v", linear)
	}
	entry := linear.Entries[0]
	if entry.EntryID != "futures-order:linear:filled-order" || entry.OrderID != "filled-order" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "linear" || entry.Type != "Filled" || entry.Symbol != "BTCUSDT" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "1.234567890123456789" || entry.CashFlow != "40000.123456789012345678" || entry.Fee != "0.000000000000000001" {
		t.Fatalf("entry decimals = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "terminal") {
		t.Fatalf("terminal raw JSON = %s", entry.RawJSON)
	}

	request.Cursor = linear.NextCursor
	inverse, err := orders.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("inverse terminal page: %v", err)
	}
	if !inverse.Done || inverse.NextCursor != "" || len(inverse.Entries) != 1 {
		t.Fatalf("inverse page = %#v", inverse)
	}
	if inverse.Entries[0].EntryID != "futures-order:inverse:inv-cancelled" || inverse.Entries[0].Category != "inverse" {
		t.Fatalf("inverse entry = %#v", inverse.Entries[0])
	}
}

func TestFuturesTerminalOrdersAdvanceCategoryPastNonterminalPages(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("category") {
		case "linear":
			// No terminal orders in linear; FetchPage must advance to inverse in one call.
			_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("open-only", "New", "n")))
		default:
			_, _ = fmt.Fprint(w, futuresOrderResponse("", futuresOrder("inv-filled", "Filled", "f")))
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("terminal page: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 || page.Entries[0].EntryID != "futures-order:inverse:inv-filled" {
		t.Fatalf("page = %#v", page)
	}
}

func TestFuturesTerminalOrdersClampToCreationWindow(t *testing.T) {
	t.Parallel()

	start := time.UnixMilli(1_700_000_000_000).UTC()
	end := time.UnixMilli(1_700_000_060_000).UTC()
	tests := []struct {
		name        string
		createdTime string
		updatedTime string
		archived    bool
		occurredAt  time.Time
	}{
		{
			name:        "settled after window end but created inside window",
			createdTime: "1700000030000",
			updatedTime: "1700000090000",
			archived:    true,
			occurredAt:  time.UnixMilli(1_700_000_030_000).UTC(),
		},
		{
			name:        "created before window start is dropped",
			createdTime: "1699999000000",
			updatedTime: "1700000030000",
			archived:    false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			order := fmt.Sprintf(`{"orderId":"lin-filled","symbol":"BTCUSDT","side":"Buy",
              "orderType":"Limit","orderStatus":"Filled","cumExecQty":"1","cumExecValue":"2",
              "cumExecFee":"0","feeCurrency":"USDT","createdTime":%q,"updatedTime":%q}`,
				test.createdTime, test.updatedTime)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("category") == "linear" {
					_, _ = fmt.Fprint(w, futuresOrderResponse("", order))
					return
				}
				_, _ = fmt.Fprint(w, futuresOrderResponse("", ""))
			}))
			t.Cleanup(server.Close)
			orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new futures orders adapter: %v", err)
			}
			page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
			if err != nil {
				t.Fatalf("fetch terminal orders: %v", err)
			}
			assertEntriesWithinWindow(t, page.Entries, start, end)
			if test.archived {
				if len(page.Entries) != 1 {
					t.Fatalf("want 1 archived entry, got %#v", page.Entries)
				}
				if !page.Entries[0].OccurredAt.Equal(test.occurredAt) {
					t.Fatalf("OccurredAt = %s, want %s", page.Entries[0].OccurredAt, test.occurredAt)
				}
			} else if len(page.Entries) != 0 {
				t.Fatalf("want entry dropped, got %#v", page.Entries)
			}
		})
	}
}

func TestFuturesOrdersRejectInvalidCursorsAndWindows(t *testing.T) {
	t.Parallel()

	orders, err := NewFuturesOrders(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}

	_, err = orders.FetchEventPage(context.Background(), spotPageRequest("provider-cursor-without-prefix"))
	if err == nil || !strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("event cursor error = %v", err)
	}

	_, err = orders.FetchEventPage(context.Background(), spotPageRequest("history:options:abc"))
	if err == nil || !strings.Contains(err.Error(), "invalid category") {
		t.Fatalf("category error = %v", err)
	}

	// A terminal (Adapter) cursor must be a history cursor, never an open one.
	_, err = orders.FetchPage(context.Background(), spotPageRequest("open:linear:abc"))
	if err == nil || !strings.Contains(err.Error(), "must be a history cursor") {
		t.Fatalf("terminal cursor error = %v", err)
	}

	// Windows wider than seven days are rejected before any request is signed.
	wide := spotPageRequest("")
	wide.End = wide.Start.Add(8 * 24 * time.Hour)
	if _, err := orders.FetchEventPage(context.Background(), wide); err == nil || !strings.Contains(err.Error(), "seven days") {
		t.Fatalf("window error = %v", err)
	}
}

func TestFuturesOrdersRedactSecretsInErrors(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-fut-secret-key"
		secret = "fake-fut-secret-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signature := r.Header.Get("X-BAPI-SIGN")
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"invalid key %s sign %s","result":{}}`, apiKey, signature)
	}))
	t.Cleanup(server.Close)

	orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), apiKey, secret))
	if err != nil {
		t.Fatalf("new futures orders adapter: %v", err)
	}
	_, err = orders.FetchEventPage(context.Background(), spotPageRequest(""))
	if err == nil {
		t.Fatal("expected API error")
	}
	message := err.Error()
	if !strings.Contains(message, "Bybit API error 10003") {
		t.Fatalf("error = %v, want code 10003", message)
	}
	if strings.Contains(message, apiKey) {
		t.Fatalf("error leaked api key: %v", message)
	}
	if !strings.Contains(message, "[REDACTED]") {
		t.Fatalf("error did not redact secrets: %v", message)
	}
}

func TestFuturesOrdersReturnEndpointErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		fetch   func(*FuturesOrdersAdapter) error
		want    string
	}{
		{
			name: "history malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":"invalid"}`)
			},
			fetch: func(orders *FuturesOrdersAdapter) error {
				_, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "decode Bybit futures order result",
		},
		{
			name: "open orders HTTP error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == futuresOrderHistoryPath {
					_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"nextPageCursor":"","list":[]}}`)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
			},
			fetch: func(orders *FuturesOrdersAdapter) error {
				// Walk to the open phase: linear history, inverse history, then open.
				request := spotPageRequest("open:linear:")
				_, err := orders.FetchEventPage(context.Background(), request)
				return err
			},
			want: "Bybit HTTP status 400",
		},
		{
			name: "terminal invalid timestamp",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"nextPageCursor":"","list":[
                  {"orderId":"bad","orderStatus":"Filled","updatedTime":"not-a-number"}]}}`)
			},
			fetch: func(orders *FuturesOrdersAdapter) error {
				_, err := orders.FetchPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "Bybit futures order updatedTime is invalid",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(test.handler)
			t.Cleanup(server.Close)
			orders, err := NewFuturesOrders(spotTestOptions(server, time.UnixMilli(futuresWindowNow), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new futures orders adapter: %v", err)
			}
			err = test.fetch(orders)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestFuturesOrderFingerprintTracksMutableFields(t *testing.T) {
	t.Parallel()

	base := spotOrderRecord{
		OrderStatus: "New", CumExecQty: "0", CumExecValue: "0",
		AvgPrice: "0", LeavesQty: "2", UpdatedTime: "1700000030000",
	}
	for _, mutate := range []func(*spotOrderRecord){
		func(r *spotOrderRecord) { r.OrderStatus = "PartiallyFilled" },
		func(r *spotOrderRecord) { r.CumExecQty = "0.000000000000000001" },
		func(r *spotOrderRecord) { r.CumExecValue = "0.000000000000000001" },
		func(r *spotOrderRecord) { r.AvgPrice = "30000.01" },
		func(r *spotOrderRecord) { r.LeavesQty = "1.999999999999999999" },
		func(r *spotOrderRecord) { r.UpdatedTime = "1700000030001" },
	} {
		changed := base
		mutate(&changed)
		if futuresOrderFingerprint(base) == futuresOrderFingerprint(changed) {
			t.Fatalf("fingerprint ignored a mutable field: %#v", changed)
		}
	}
}

func futuresOrderResponse(nextCursor, order string) string {
	list := "[]"
	if order != "" {
		list = "[" + order + "]"
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":%s}}`, nextCursor, list)
}

func futuresOrder(orderID, status, marker string) string {
	return fmt.Sprintf(`{
      "orderId":%q,"symbol":"BTCUSDT","side":"Buy","orderType":"Limit",
      "qty":"1.000000000000000001","cumExecQty":"1.000000000000000001",
      "cumExecValue":"40000.01","cumExecFee":"0.000000000000000001",
      "orderStatus":%q,"updatedTime":"1700000030000","leavesQty":"0",
      "avgPrice":"40000.01","feeCurrency":"USDT","futureField":%q
    }`, orderID, status, marker)
}

var (
	_ source.Adapter      = (*FuturesOrdersAdapter)(nil)
	_ source.EventAdapter = (*FuturesOrdersAdapter)(nil)
)
