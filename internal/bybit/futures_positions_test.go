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

func TestFuturesPositionsSignsDrainsScopesAndNormalizes(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-positions-key"
		secret = "fake-positions-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != positionListPath {
			t.Errorf("path = %q, want %q", r.URL.Path, positionListPath)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		queries = append(queries, r.URL.RawQuery)
		switch {
		case r.URL.Query().Get("category") == "linear" && r.URL.Query().Get("settleCoin") == "USDT":
			_, _ = fmt.Fprint(w, futuresPositionResponse("", futuresPositionRow("BTCUSDT", "Buy", "0.5", "Normal")))
		case r.URL.Query().Get("category") == "linear" && r.URL.Query().Get("settleCoin") == "USDC":
			_, _ = fmt.Fprint(w, futuresPositionResponse("", futuresPositionRow("ETHPERP", "Sell", "1.25", "Normal")))
		case r.URL.Query().Get("category") == "inverse":
			_, _ = fmt.Fprint(w, futuresPositionResponse("", futuresPositionRow("BTCUSD", "Buy", "100", "Normal")))
		default:
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
	}))
	t.Cleanup(server.Close)

	positions, err := NewFuturesPositions(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}

	// Scope 0: linear / USDT.
	first, err := positions.FetchEventPage(context.Background(), futuresPositionRequest(""))
	if err != nil {
		t.Fatalf("fetch scope 0: %v", err)
	}
	if first.Done || first.NextCursor != "scope:1" || len(first.Observations) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	got := first.Observations[0]
	if got.ObjectType != "position" || got.ObjectID != "futures-position:linear:BTCUSDT:0" {
		t.Fatalf("observation identity = %#v", got)
	}
	if got.Stream != "futures" || got.Status != "Normal" || got.Symbol != "BTCUSDT" || got.Asset != "USDT" {
		t.Fatalf("observation classification = %#v", got)
	}
	if got.Amount != "0.5" || got.StateFingerprint == "" {
		t.Fatalf("observation state = %#v", got)
	}
	if !got.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) || !got.ObservedAt.Equal(requestTime) {
		t.Fatalf("observation timestamps = %#v", got)
	}
	if !containsJSONField(got.RawJSON, "markPrice", "30001") || !containsJSONField(got.RawJSON, "unrealisedPnl", "0.5") {
		t.Fatalf("raw JSON dropped excluded fields: %s", got.RawJSON)
	}

	// Scope 1: linear / USDC.
	second, err := positions.FetchEventPage(context.Background(), futuresPositionRequest(first.NextCursor))
	if err != nil {
		t.Fatalf("fetch scope 1: %v", err)
	}
	if second.Done || second.NextCursor != "scope:2" || len(second.Observations) != 1 {
		t.Fatalf("second page = %#v", second)
	}
	if second.Observations[0].ObjectID != "futures-position:linear:ETHPERP:0" || second.Observations[0].Asset != "USDC" {
		t.Fatalf("scope 1 observation = %#v", second.Observations[0])
	}

	// Scope 2: inverse (terminal).
	third, err := positions.FetchEventPage(context.Background(), futuresPositionRequest(second.NextCursor))
	if err != nil {
		t.Fatalf("fetch scope 2: %v", err)
	}
	if !third.Done || third.NextCursor != "" || len(third.Observations) != 1 {
		t.Fatalf("third page = %#v", third)
	}
	if third.Observations[0].ObjectID != "futures-position:inverse:BTCUSD:0" || third.Observations[0].Asset != "" {
		t.Fatalf("scope 2 observation = %#v", third.Observations[0])
	}

	want := []string{
		url.Values{"category": {"linear"}, "limit": {futuresPositionQueryLimit}, "settleCoin": {"USDT"}}.Encode(),
		url.Values{"category": {"linear"}, "limit": {futuresPositionQueryLimit}, "settleCoin": {"USDC"}}.Encode(),
		url.Values{"category": {"inverse"}, "limit": {futuresPositionQueryLimit}}.Encode(),
	}
	if strings.Join(queries, "|") != strings.Join(want, "|") {
		t.Fatalf("queries = %v, want %v", queries, want)
	}
}

func TestFuturesPositionsPaginatesWithinScope(t *testing.T) {
	t.Parallel()

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("settleCoin") != "USDT" {
			// Later scopes are empty for this test.
			_, _ = fmt.Fprint(w, futuresPositionResponse(""))
			return
		}
		calls++
		switch calls {
		case 1:
			if got := r.URL.Query().Get("cursor"); got != "" {
				t.Errorf("first page cursor = %q, want empty", got)
			}
			_, _ = fmt.Fprint(w, futuresPositionResponse("page-2", futuresPositionRow("BTCUSDT", "Buy", "0.5", "Normal")))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "page-2" {
				t.Errorf("second page cursor = %q, want page-2", got)
			}
			_, _ = fmt.Fprint(w, futuresPositionResponse("", futuresPositionRow("SOLUSDT", "Sell", "3", "Normal")))
		default:
			t.Errorf("unexpected USDT call %d", calls)
		}
	}))
	t.Cleanup(server.Close)

	positions, err := NewFuturesPositions(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}
	page, err := positions.FetchEventPage(context.Background(), futuresPositionRequest(""))
	if err != nil {
		t.Fatalf("fetch scope 0: %v", err)
	}
	if page.Done || page.NextCursor != "scope:1" || len(page.Observations) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if page.Observations[0].ObjectID != "futures-position:linear:BTCUSDT:0" ||
		page.Observations[1].ObjectID != "futures-position:linear:SOLUSDT:0" {
		t.Fatalf("observations = %#v", page.Observations)
	}
	if calls != 2 {
		t.Fatalf("USDT calls = %d, want 2", calls)
	}
}

func TestFuturesPositionsRejectsRepeatedCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, futuresPositionResponse("loop", futuresPositionRow("BTCUSDT", "Buy", "0.5", "Normal")))
	}))
	t.Cleanup(server.Close)

	positions, err := NewFuturesPositions(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}
	_, err = positions.FetchEventPage(context.Background(), futuresPositionRequest(""))
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestFuturesPositionsSkipsEmptyScopes(t *testing.T) {
	t.Parallel()

	var paths int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths++
		if r.URL.Query().Get("category") == "inverse" {
			_, _ = fmt.Fprint(w, futuresPositionResponse("", futuresPositionRow("BTCUSD", "Buy", "100", "Normal")))
			return
		}
		_, _ = fmt.Fprint(w, futuresPositionResponse(""))
	}))
	t.Cleanup(server.Close)

	positions, err := NewFuturesPositions(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}
	page, err := positions.FetchEventPage(context.Background(), futuresPositionRequest(""))
	if err != nil {
		t.Fatalf("fetch positions: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 1 {
		t.Fatalf("page = %#v", page)
	}
	if page.Observations[0].ObjectID != "futures-position:inverse:BTCUSD:0" {
		t.Fatalf("observation = %#v", page.Observations[0])
	}
	if paths != 3 {
		t.Fatalf("requests = %d, want 3 (one per scope)", paths)
	}
}

func TestFuturesPositionFingerprintExcludesLivePricing(t *testing.T) {
	t.Parallel()

	base := futuresPositionRecord{
		Side: "Buy", Size: "0.5", AvgPrice: "30000.5", Leverage: "10",
		PositionStatus: "Normal", CumRealisedPnl: "1.5", MarkPrice: "30001", UnrealisedPnl: "0.5",
	}
	movedMark := base
	movedMark.MarkPrice = "31000"
	movedMark.UnrealisedPnl = "500"
	if futuresPositionFingerprint(base) != futuresPositionFingerprint(movedMark) {
		t.Fatal("fingerprint reacted to markPrice/unrealisedPnl changes")
	}

	changed := map[string]func(*futuresPositionRecord){
		"side":           func(r *futuresPositionRecord) { r.Side = "Sell" },
		"size":           func(r *futuresPositionRecord) { r.Size = "0.6" },
		"avgPrice":       func(r *futuresPositionRecord) { r.AvgPrice = "30000.6" },
		"leverage":       func(r *futuresPositionRecord) { r.Leverage = "20" },
		"positionStatus": func(r *futuresPositionRecord) { r.PositionStatus = "Liq" },
		"cumRealisedPnl": func(r *futuresPositionRecord) { r.CumRealisedPnl = "2.0" },
	}
	for field, mutate := range changed {
		mutated := base
		mutate(&mutated)
		if futuresPositionFingerprint(base) == futuresPositionFingerprint(mutated) {
			t.Fatalf("fingerprint ignored %s change", field)
		}
	}
}

func TestFuturesPositionsErrorsAndRedaction(t *testing.T) {
	t.Parallel()

	const secret = "fake-super-secret"

	tests := []struct {
		name    string
		cursor  string
		handler http.HandlerFunc
		want    string
		absent  string
	}{
		{
			name: "API error redacts secret",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"invalid key %s","result":{}}`, secret)
			},
			want:   "Bybit API error 10003",
			absent: secret,
		},
		{
			name: "malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":"invalid"}`)
			},
			want: "decode Bybit futures position result",
		},
		{
			name:    "invalid cursor prefix",
			cursor:  "provider-cursor-without-prefix",
			handler: func(w http.ResponseWriter, r *http.Request) { t.Error("must not call API on bad cursor") },
			want:    "invalid source prefix",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(test.handler)
			t.Cleanup(server.Close)
			positions, err := NewFuturesPositions(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", secret))
			if err != nil {
				t.Fatalf("new futures positions adapter: %v", err)
			}
			_, err = positions.FetchEventPage(context.Background(), futuresPositionRequest(test.cursor))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if test.absent != "" && strings.Contains(err.Error(), test.absent) {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestFuturesPositionsRejectMissingAccountAndTimestamp(t *testing.T) {
	t.Parallel()

	positions, err := NewFuturesPositions(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}
	_, err = positions.FetchEventPage(context.Background(), source.PageRequest{})
	if err == nil || !strings.Contains(err.Error(), "Account id is required") {
		t.Fatalf("account error = %v", err)
	}

	_, err = normalizeFuturesPosition(
		[]byte(`{"symbol":"BTCUSDT","updatedTime":"not-a-number"}`),
		futuresPositionScopes[0],
		source.Account{ID: "fake-account"},
		time.UnixMilli(1_700_000_123_456).UTC(),
	)
	if err == nil || !strings.Contains(err.Error(), "updatedTime") {
		t.Fatalf("timestamp error = %v", err)
	}
}

func futuresPositionRequest(cursor string) source.PageRequest {
	return source.PageRequest{
		Account: source.Account{
			ID: "fake-account", Label: "Fake Account",
			APIKeyEnv: "FAKE_BYBIT_KEY", APISecretEnv: "FAKE_BYBIT_SECRET",
		},
		Cursor: cursor,
	}
}

func futuresPositionResponse(nextCursor string, rows ...string) string {
	return fmt.Sprintf(
		`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":[%s]}}`,
		nextCursor, strings.Join(rows, ","),
	)
}

func futuresPositionRow(symbol, side, size, status string) string {
	return fmt.Sprintf(
		`{"symbol":%q,"side":%q,"size":%q,"avgPrice":"30000.5","leverage":"10",`+
			`"positionStatus":%q,"positionIdx":0,"positionValue":"15000.25",`+
			`"cumRealisedPnl":"1.5","markPrice":"30001","unrealisedPnl":"0.5",`+
			`"updatedTime":"1700000030000"}`,
		symbol, side, size, status,
	)
}
