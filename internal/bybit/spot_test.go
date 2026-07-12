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

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

// assertEntriesWithinWindow mirrors archive.validatePageWindow's clamp: every
// terminal ledger entry must occur inside [start,end] or the sync wedges.
func assertEntriesWithinWindow(t *testing.T, entries []model.LedgerEntry, start, end time.Time) {
	t.Helper()
	for index, entry := range entries {
		if entry.OccurredAt.Before(start) || entry.OccurredAt.After(end) {
			t.Fatalf("entry %d OccurredAt %s escapes window [%s,%s]", index, entry.OccurredAt, start, end)
		}
	}
}

func TestSpotFillsSignsPaginatesAndNormalizes(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-spot-key"
		secret = "fake-spot-secret"
		cursor = "fake-fill-cursor"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != spotExecutionPath {
			t.Errorf("path = %q, want %q", r.URL.Path, spotExecutionPath)
		}
		wantQuery := url.Values{
			"category":  {"spot"},
			"startTime": {"1700000000000"},
			"endTime":   {"1700000060000"},
			"limit":     {"100"},
			"cursor":    {cursor},
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
              "feeCurrency":"USDT","isMaker":true,"futureField":"preserved"
            }]
          }
        }`)
	}))
	t.Cleanup(server.Close)

	fills, _, err := NewSpot(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	page, err := fills.FetchPage(context.Background(), spotPageRequest(cursor))
	if err != nil {
		t.Fatalf("fetch spot fills: %v", err)
	}
	if page.Done || page.NextCursor != "fake-next-fill-cursor" || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.EntryID != "spot:execution:fake-exec-1" || entry.TradeID != "fake-exec-1" || entry.OrderID != "fake-order-1" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "spot" || entry.Type != "Trade" || entry.Symbol != "BTCUSDT" || entry.Side != "Buy" {
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

func TestSpotOrdersPollsHistoryAndOpenOrders(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-orders-key"
		secret = "fake-orders-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		paths = append(paths, r.URL.Path)
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		switch r.URL.Path {
		case spotOrderHistoryPath:
			wantQuery := url.Values{
				"category": {"spot"}, "startTime": {"1700000000000"},
				"endTime": {"1700000060000"}, "limit": {"50"},
			}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("history query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("", `{
              "orderId":"fake-order-filled","symbol":"ETHUSDT","side":"Sell",
              "orderType":"Market","qty":"1.000000000000000001",
              "cumExecQty":"1.000000000000000001","cumExecValue":"3000.01",
              "cumExecFee":"0.000000000000000001","orderStatus":"Filled",
              "updatedTime":"1700000040000","leavesQty":"0","avgPrice":"3000.01",
              "feeCurrency":"USDT","futureField":"history"
            }`))
		case spotOpenOrdersPath:
			wantQuery := url.Values{"category": {"spot"}, "limit": {"50"}, "openOnly": {"1"}}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("open query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("", `{
              "orderId":"fake-order-new","symbol":"ETHUSDT","side":"Buy",
              "orderType":"Limit","qty":"2.000000000000000001",
              "cumExecQty":"0.000000000000000001","cumExecValue":"0",
              "cumExecFee":"0","orderStatus":"New","updatedTime":"1700000050000",
              "leavesQty":"2.000000000000000000","avgPrice":"","feeCurrency":"USDT"
            }`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, orders, err := NewSpot(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	page, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch spot orders: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != spotOrderHistoryPath+","+spotOpenOrdersPath {
		t.Fatalf("paths = %v", paths)
	}
	history, open := page.Observations[0], page.Observations[1]
	if history.ObjectType != "spot_order" || history.ObjectID != "fake-order-filled" || history.Status != "Filled" {
		t.Fatalf("history observation = %#v", history)
	}
	if history.Stream != "spot" || history.Amount != "1.000000000000000001" || history.StateFingerprint == "" {
		t.Fatalf("history normalization = %#v", history)
	}
	if open.ObjectID != "fake-order-new" || open.Status != "New" || open.StateFingerprint == history.StateFingerprint {
		t.Fatalf("open observation = %#v", open)
	}
	if !containsJSONField(history.RawJSON, "futureField", "history") {
		t.Fatalf("history raw JSON = %s", history.RawJSON)
	}
}

func TestSpotOrdersSkipRealtimeForHistoricalWindow(t *testing.T) {
	t.Parallel()

	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path != spotOrderHistoryPath {
			t.Errorf("path = %q, want %q", r.URL.Path, spotOrderHistoryPath)
		}
		_, _ = fmt.Fprint(w, spotOrderResponse("", validSpotOrder("fake-history", "Filled")))
	}))
	t.Cleanup(server.Close)

	_, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_100_000_000), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	page, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch historical spot orders: %v", err)
	}
	if !page.Done || len(page.Observations) != 1 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != spotOrderHistoryPath {
		t.Fatalf("paths = %v", paths)
	}
}

func TestSpotOrdersMapsHistoryAndOpenCursors(t *testing.T) {
	t.Parallel()

	var historyCalls, openCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case spotOrderHistoryPath:
			historyCalls++
			if historyCalls == 1 {
				_, _ = fmt.Fprint(w, spotOrderResponse("fake-history-next", validSpotOrder("fake-history-1", "Filled")))
				return
			}
			if got := r.URL.Query().Get("cursor"); got != "fake-history-next" {
				t.Errorf("history cursor = %q", got)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("", validSpotOrder("fake-history-2", "Cancelled")))
		case spotOpenOrdersPath:
			openCalls++
			if openCalls == 1 {
				_, _ = fmt.Fprint(w, spotOrderResponse("fake-open-next", validSpotOrder("fake-open-1", "New")))
				return
			}
			if got := r.URL.Query().Get("cursor"); got != "fake-open-next" {
				t.Errorf("open cursor = %q", got)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("", validSpotOrder("fake-open-2", "PartiallyFilled")))
		}
	}))
	t.Cleanup(server.Close)

	_, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	request := spotPageRequest("")
	first, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done || first.NextCursor != "history:fake-history-next" {
		t.Fatalf("first cursor = %#v", first)
	}
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.Done || second.NextCursor != "open:fake-open-next" || len(second.Observations) != 2 {
		t.Fatalf("second cursor = %#v", second)
	}
	request.Cursor = second.NextCursor
	third, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if !third.Done || third.NextCursor != "" || len(third.Observations) != 1 {
		t.Fatalf("third cursor = %#v", third)
	}
}

func TestSpotTerminalOrdersBecomeStableLedgerEntries(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{
          "retCode":0,"result":{"nextPageCursor":"","list":[
            {"orderId":"fake-open","orderStatus":"New","updatedTime":"1700000030000"},
            {"orderId":"fake-terminal","symbol":"SOLUSDT","side":"Sell","orderType":"Limit",
             "orderStatus":"PartiallyFilledCanceled","cumExecQty":"1.234567890123456789",
             "cumExecValue":"200.123456789012345678","cumExecFee":"0.000000000000000001",
             "feeCurrency":"USDT","updatedTime":"1700000040000","futureField":"terminal"}
          ]}
        }`)
	}))
	t.Cleanup(server.Close)

	_, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal orders: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.EntryID != "spot:order:fake-terminal" || entry.OrderID != "fake-terminal" || entry.Type != "PartiallyFilledCanceled" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Amount != "1.234567890123456789" || entry.CashFlow != "200.123456789012345678" || entry.Fee != "0.000000000000000001" {
		t.Fatalf("entry decimals = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "terminal") {
		t.Fatalf("terminal raw JSON = %s", entry.RawJSON)
	}
}

func TestSpotTerminalOrdersSkipNonterminalOnlyPages(t *testing.T) {
	t.Parallel()

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			_, _ = fmt.Fprint(w, spotOrderResponse("fake-next-1", validSpotOrder("fake-new", "New")))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "fake-next-1" {
				t.Errorf("cursor = %q, want fake-next-1", got)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("fake-next-2", validSpotOrder("fake-partial", "PartiallyFilled")))
		case 3:
			if got := r.URL.Query().Get("cursor"); got != "fake-next-2" {
				t.Errorf("cursor = %q, want fake-next-2", got)
			}
			_, _ = fmt.Fprint(w, spotOrderResponse("", validSpotOrder("fake-filled", "Filled")))
		default:
			t.Fatalf("unexpected request %d", calls)
		}
	}))
	t.Cleanup(server.Close)

	_, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal orders: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 || page.Entries[0].EntryID != "spot:order:fake-filled" {
		t.Fatalf("page = %#v", page)
	}
}

func TestSpotTerminalOrdersClampToCreationWindow(t *testing.T) {
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
			order := fmt.Sprintf(`{"orderId":"fake-terminal","symbol":"BTCUSDT","side":"Buy",
              "orderType":"Limit","orderStatus":"Filled","cumExecQty":"1","cumExecValue":"2",
              "cumExecFee":"0","feeCurrency":"USDT","createdTime":%q,"updatedTime":%q}`,
				test.createdTime, test.updatedTime)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, spotOrderResponse("", order))
			}))
			t.Cleanup(server.Close)
			_, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new spot adapters: %v", err)
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

func TestSpotEndpointErrorsAreReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		fetch   func(*SpotFillsAdapter, *SpotOrdersAdapter) error
		want    string
	}{
		{
			name: "execution API error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":10003,"retMsg":"fake invalid key","result":{}}`)
			},
			fetch: func(fills *SpotFillsAdapter, _ *SpotOrdersAdapter) error {
				_, err := fills.FetchPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "Bybit API error 10003",
		},
		{
			name: "history malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":"invalid"}`)
			},
			fetch: func(_ *SpotFillsAdapter, orders *SpotOrdersAdapter) error {
				_, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "decode Bybit spot order result",
		},
		{
			name: "open orders HTTP error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == spotOrderHistoryPath {
					_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"nextPageCursor":"","list":[]}}`)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
			},
			fetch: func(_ *SpotFillsAdapter, orders *SpotOrdersAdapter) error {
				_, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "Bybit HTTP status 400",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(test.handler)
			t.Cleanup(server.Close)
			fills, orders, err := NewSpot(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new spot adapters: %v", err)
			}
			err = test.fetch(fills, orders)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSpotOrderFingerprintTracksMutableFields(t *testing.T) {
	t.Parallel()

	base := spotOrderRecord{
		OrderStatus: "New", Price: "100", Qty: "2", CumExecQty: "0",
		CumFeeDetail: map[string]string{"USDT": "0"}, UpdatedTime: "1700000030000",
	}
	changedAmount := base
	changedAmount.CumExecQty = "0.000000000000000001"
	changedPrice := base
	changedPrice.Price = "101"
	changedFeeDetail := base
	changedFeeDetail.CumFeeDetail = map[string]string{"USDT": "0.000000000000000001"}
	changedTime := base
	changedTime.UpdatedTime = "1700000030001"
	if spotOrderFingerprint(base) == spotOrderFingerprint(changedAmount) {
		t.Fatal("fingerprint ignored cumulative execution quantity")
	}
	if spotOrderFingerprint(base) == spotOrderFingerprint(changedPrice) {
		t.Fatal("fingerprint ignored amended price")
	}
	if spotOrderFingerprint(base) == spotOrderFingerprint(changedFeeDetail) {
		t.Fatal("fingerprint ignored cumulative fee detail")
	}
	if spotOrderFingerprint(base) == spotOrderFingerprint(changedTime) {
		t.Fatal("fingerprint ignored provider update time")
	}
}

func TestSpotOrdersRejectInvalidCursorAndNormalization(t *testing.T) {
	t.Parallel()

	_, orders, err := NewSpot(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new spot adapters: %v", err)
	}
	_, err = orders.FetchEventPage(context.Background(), spotPageRequest("provider-cursor-without-prefix"))
	if err == nil || !strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("cursor error = %v", err)
	}

	_, _, err = decodeSpotOrder([]byte(`{"orderId":"fake-order","orderStatus":"New","updatedTime":"invalid"}`))
	if err == nil || !strings.Contains(err.Error(), "updatedTime") {
		t.Fatalf("normalization error = %v", err)
	}
}

func spotTestOptions(server *httptest.Server, now time.Time, apiKey, secret string) Options {
	return Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			"FAKE_BYBIT_KEY":    apiKey,
			"FAKE_BYBIT_SECRET": secret,
		}),
	}
}

func spotPageRequest(cursor string) source.PageRequest {
	return source.PageRequest{
		Account: source.Account{
			ID: "fake-account", Label: "Fake Account",
			APIKeyEnv: "FAKE_BYBIT_KEY", APISecretEnv: "FAKE_BYBIT_SECRET",
		},
		Start:  time.UnixMilli(1_700_000_000_000).UTC(),
		End:    time.UnixMilli(1_700_000_060_000).UTC(),
		Cursor: cursor,
	}
}

func assertSpotSigned(t *testing.T, request *http.Request, apiKey, secret string, now time.Time) {
	t.Helper()
	assertHeader(t, request.Header, "X-BAPI-API-KEY", apiKey)
	assertHeader(t, request.Header, "X-BAPI-TIMESTAMP", fmt.Sprint(now.UnixMilli()))
	assertHeader(t, request.Header, "X-BAPI-RECV-WINDOW", "5000")
	payload := fmt.Sprint(now.UnixMilli()) + apiKey + "5000" + request.URL.RawQuery
	assertHeader(t, request.Header, "X-BAPI-SIGN", hmacHex(secret, payload))
}

func spotOrderResponse(nextCursor, order string) string {
	list := "[]"
	if order != "" {
		list = "[" + order + "]"
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":%s}}`, nextCursor, list)
}

func validSpotOrder(orderID, status string) string {
	return fmt.Sprintf(`{"orderId":%q,"orderStatus":%q,"updatedTime":"1700000030000"}`, orderID, status)
}
