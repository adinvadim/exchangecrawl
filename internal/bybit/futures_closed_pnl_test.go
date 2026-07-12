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

func TestFuturesClosedPnLSignsWindowsAndNormalizes(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-pnl-key"
		secret = "fake-pnl-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != closedPnLPath {
			t.Errorf("path = %q, want %q", r.URL.Path, closedPnLPath)
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
            "nextPageCursor":"",
            "list":[{
              "symbol":"BTCUSDT","orderId":"fake-order-1","side":"Sell",
              "orderType":"Market","execType":"Trade",
              "closedSize":"0.000000000000000123","qty":"0.000000000000000123",
              "orderPrice":"30000.01","avgEntryPrice":"29000.01",
              "avgExitPrice":"30000.02","cumEntryValue":"3.480000000000000001",
              "cumExitValue":"3.690000000000000001","closedPnl":"0.210000000000000001",
              "openFee":"0.000000000000000002","closeFee":"0.000000000000000003",
              "leverage":"10","fillCount":"1",
              "createdTime":"1700000020000","updatedTime":"1700000030000",
              "futureField":"preserved"
            }]
          }
        }`)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewFuturesClosedPnL(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new closed pnl adapter: %v", err)
	}
	page, err := adapter.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch closed pnl: %v", err)
	}
	// linear exhausted (empty provider cursor) hands off to inverse.
	if page.Done || page.NextCursor != closedPnLInversePrefix || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	wantID := "futures-pnl:linear:fake-order-1:1700000030000:0.000000000000000123"
	if entry.EntryID != wantID || entry.OrderID != "fake-order-1" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "linear" || entry.Type != "Trade" || entry.Symbol != "BTCUSDT" || entry.Side != "Sell" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "0.000000000000000123" || entry.Fee != "0.000000000000000003" || entry.CashFlow != "0.210000000000000001" {
		t.Fatalf("entry decimals = %#v", entry)
	}
	if entry.Info != "Market" {
		t.Fatalf("entry info = %#v", entry)
	}
	if !entry.ObservedAt.Equal(requestTime) || !entry.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) {
		t.Fatalf("entry timestamps = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "preserved") {
		t.Fatalf("raw JSON did not preserve unknown field: %s", entry.RawJSON)
	}
}

func TestFuturesClosedPnLWalksLinearThenInverse(t *testing.T) {
	t.Parallel()

	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var categories []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		category := r.URL.Query().Get("category")
		categories = append(categories, category+"|"+r.URL.Query().Get("cursor"))
		switch {
		case category == "linear" && r.URL.Query().Get("cursor") == "":
			_, _ = fmt.Fprint(w, closedPnLResponse("linear-next", closedPnLRow("fake-linear-1", "1")))
		case category == "linear" && r.URL.Query().Get("cursor") == "linear-next":
			_, _ = fmt.Fprint(w, closedPnLResponse("", closedPnLRow("fake-linear-2", "2")))
		case category == "inverse" && r.URL.Query().Get("cursor") == "":
			_, _ = fmt.Fprint(w, closedPnLResponse("inverse-next", closedPnLRow("fake-inverse-1", "3")))
		case category == "inverse" && r.URL.Query().Get("cursor") == "inverse-next":
			_, _ = fmt.Fprint(w, closedPnLResponse("", closedPnLRow("fake-inverse-2", "4")))
		default:
			t.Errorf("unexpected request category=%q cursor=%q", category, r.URL.Query().Get("cursor"))
		}
	}))
	t.Cleanup(server.Close)

	adapter, err := NewFuturesClosedPnL(spotTestOptions(server, requestTime, "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new closed pnl adapter: %v", err)
	}

	request := spotPageRequest("")
	var ids []string
	cursors := []string{}
	for pages := 0; pages < 10; pages++ {
		page, err := adapter.FetchPage(context.Background(), request)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, entry := range page.Entries {
			ids = append(ids, entry.EntryID)
		}
		if page.Done {
			if page.NextCursor != "" {
				t.Fatalf("done page has cursor %q", page.NextCursor)
			}
			break
		}
		cursors = append(cursors, page.NextCursor)
		request.Cursor = page.NextCursor
	}

	wantIDs := []string{
		"futures-pnl:linear:fake-linear-1:1700000030000:1",
		"futures-pnl:linear:fake-linear-2:1700000030000:2",
		"futures-pnl:inverse:fake-inverse-1:1700000030000:3",
		"futures-pnl:inverse:fake-inverse-2:1700000030000:4",
	}
	if strings.Join(ids, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("ids = %v, want %v", ids, wantIDs)
	}
	wantCursors := []string{"linear:linear-next", closedPnLInversePrefix, "inverse:inverse-next"}
	if strings.Join(cursors, ",") != strings.Join(wantCursors, ",") {
		t.Fatalf("cursors = %v, want %v", cursors, wantCursors)
	}
	wantCalls := []string{"linear|", "linear|linear-next", "inverse|", "inverse|inverse-next"}
	if strings.Join(categories, ",") != strings.Join(wantCalls, ",") {
		t.Fatalf("calls = %v, want %v", categories, wantCalls)
	}
}

func TestFuturesClosedPnLRejectsRepeatedCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Provider echoes back the cursor it was handed, which would loop forever.
		_, _ = fmt.Fprint(w, closedPnLResponse("stuck-cursor", closedPnLRow("fake-loop", "1")))
	}))
	t.Cleanup(server.Close)

	adapter, err := NewFuturesClosedPnL(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new closed pnl adapter: %v", err)
	}
	request := spotPageRequest("linear:stuck-cursor")
	_, err = adapter.FetchPage(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestFuturesClosedPnLEnforcesWindowBounds(t *testing.T) {
	t.Parallel()

	adapter, err := NewFuturesClosedPnL(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new closed pnl adapter: %v", err)
	}
	request := spotPageRequest("")
	request.End = request.Start.Add(8 * 24 * time.Hour)
	if _, err := adapter.FetchPage(context.Background(), request); err == nil ||
		!strings.Contains(err.Error(), "seven days") {
		t.Fatalf("window error = %v, want seven days", err)
	}

	if _, _, err := parseClosedPnLCursor("provider-cursor-without-prefix"); err == nil ||
		!strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("cursor error = %v", err)
	}
}

func TestFuturesClosedPnLRejectsMissingIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  string
		want string
	}{
		{
			name: "no orderId",
			row:  `{"orderId":"","closedSize":"1","updatedTime":"1700000030000"}`,
			want: "no orderId",
		},
		{
			name: "no closedSize",
			row:  `{"orderId":"fake","closedSize":"","updatedTime":"1700000030000"}`,
			want: "no closedSize",
		},
		{
			name: "invalid updatedTime",
			row:  `{"orderId":"fake","closedSize":"1","updatedTime":"nope"}`,
			want: "invalid updatedTime",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeClosedPnL([]byte(test.row), "linear", source.Account{ID: "fake"}, time.Now())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestFuturesClosedPnLRedactsSecretsInErrors(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-secret-key"
		secret = "fake-secret-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Remote error message echoes the api key and signature; both must be redacted.
		signature := r.Header.Get("X-BAPI-SIGN")
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"invalid key %s sign %s","result":{}}`, apiKey, signature)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewFuturesClosedPnL(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), apiKey, secret))
	if err != nil {
		t.Fatalf("new closed pnl adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), spotPageRequest(""))
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

func TestFuturesClosedPnLReturnsEndpointErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{
			name: "malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":"invalid"}`)
			},
			want: "decode Bybit closed pnl result",
		},
		{
			name: "HTTP error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
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
			adapter, err := NewFuturesClosedPnL(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new closed pnl adapter: %v", err)
			}
			_, err = adapter.FetchPage(context.Background(), spotPageRequest(""))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func closedPnLResponse(nextCursor, row string) string {
	list := "[]"
	if row != "" {
		list = "[" + row + "]"
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":%s}}`, nextCursor, list)
}

func closedPnLRow(orderID, closedSize string) string {
	return fmt.Sprintf(
		`{"symbol":"BTCUSDT","orderId":%q,"side":"Sell","orderType":"Market","execType":"Trade","closedSize":%q,"closedPnl":"0.01","closeFee":"0.001","updatedTime":"1700000030000"}`,
		orderID, closedSize,
	)
}
