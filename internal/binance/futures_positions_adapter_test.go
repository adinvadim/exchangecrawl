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

func TestFuturesPositionsFetchEventPageNormalizesSnapshot(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != futuresPositionsPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			http.NotFound(response, request)
			return
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
			t.Errorf("API key header = %q", got)
		}
		query := request.URL.Query()
		if got := query.Get("recvWindow"); got != "5000" {
			t.Errorf("recvWindow = %q, want 5000", got)
		}
		if got := query.Get("timestamp"); got != strconv.FormatInt(fixedNow.UnixMilli(), 10) {
			t.Errorf("timestamp = %q", got)
		}
		assertSpotSignature(t, request, "fake-api-secret")
		_, _ = response.Write([]byte(`[
			{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.500","entryPrice":"60000.0","breakEvenPrice":"60012.0","markPrice":"61000.0","unRealizedProfit":"500.0","liquidationPrice":"40000.0","isolatedMargin":"0","marginAsset":"USDT","leverage":"10","updateTime":1699999999000},
			{"symbol":"ETHUSDT","positionSide":"SHORT","positionAmt":"0.000","entryPrice":"0.0","breakEvenPrice":"0.0","markPrice":"3000.0","unRealizedProfit":"0.0","liquidationPrice":"0","isolatedMargin":"0","marginAsset":"USDT","leverage":"20","updateTime":1699999000000}
		]`))
	}))

	adapter := newTestFuturesPositionsAdapter(t, server, fixedNow)
	page, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour), End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch positions: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 2 {
		t.Fatalf("page = done %t cursor %q observations %d", page.Done, page.NextCursor, len(page.Observations))
	}

	open := page.Observations[0]
	if open.Exchange != model.ExchangeBinance || open.Stream != "futures" || open.ObjectType != "position" {
		t.Fatalf("open classification = %#v", open)
	}
	if open.ObjectID != "futures-position:BTCUSDT:LONG" || open.Status != "OPEN" {
		t.Fatalf("open identity = %#v", open)
	}
	if open.Symbol != "BTCUSDT" || open.Asset != "USDT" || open.Amount != "0.500" || open.StateFingerprint == "" {
		t.Fatalf("open state = %#v", open)
	}
	if !open.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) || !open.ObservedAt.Equal(fixedNow) {
		t.Fatalf("open times = %s / %s", open.OccurredAt, open.ObservedAt)
	}
	if !json.Valid(open.RawJSON) || !strings.Contains(string(open.RawJSON), `"markPrice":"61000.0"`) {
		t.Fatalf("open raw JSON = %s", open.RawJSON)
	}

	flat := page.Observations[1]
	if flat.ObjectID != "futures-position:ETHUSDT:SHORT" || flat.Status != "FLAT" || flat.Amount != "0.000" {
		t.Fatalf("flat state = %#v", flat)
	}
}

func TestFuturesPositionsFingerprintExcludesMarkAndProfit(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	account := source.Account{ID: "primary"}

	base := futuresPositionRow{
		Symbol: "BTCUSDT", PositionSide: "BOTH", PositionAmt: "0.500", EntryPrice: "60000.0",
		BreakEvenPrice: "60012.0", Leverage: "10", IsolatedMargin: "0", MarginAsset: "USDT",
		UpdateTime: 1_699_999_999_000,
	}
	baseObservation, err := normalizeFuturesPosition(base, account, fixedNow)
	if err != nil {
		t.Fatalf("normalize base: %v", err)
	}

	// markPrice / unRealizedProfit are not struct fields and only live in
	// RawJSON, so they cannot affect the fingerprint by construction; a row
	// identical in the durable terms must fingerprint identically.
	sameTerms := base
	sameTerms.UpdateTime = 1_700_000_500_000
	sameObservation, err := normalizeFuturesPosition(sameTerms, account, fixedNow)
	if err != nil {
		t.Fatalf("normalize same terms: %v", err)
	}
	if sameObservation.StateFingerprint != baseObservation.StateFingerprint {
		t.Fatalf("fingerprint churned on markPrice-equivalent change: %q vs %q",
			sameObservation.StateFingerprint, baseObservation.StateFingerprint)
	}

	changed := base
	changed.EntryPrice = "60500.0"
	changedObservation, err := normalizeFuturesPosition(changed, account, fixedNow)
	if err != nil {
		t.Fatalf("normalize changed: %v", err)
	}
	if changedObservation.StateFingerprint == baseObservation.StateFingerprint {
		t.Fatal("fingerprint did not change on entryPrice change")
	}
}

func TestFuturesPositionsRejectsCursor(t *testing.T) {
	t.Parallel()

	adapter := newTestFuturesPositionsAdapter(t, httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		t.Error("cursor request reached the server")
		http.NotFound(response, nil)
	})), time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
		Cursor: "anything",
	})
	if err == nil || !strings.Contains(err.Error(), "does not paginate") {
		t.Fatalf("cursor error = %v", err)
	}
}

func TestFuturesPositionsRejectsInvertedWindow(t *testing.T) {
	t.Parallel()

	adapter := newTestFuturesPositionsAdapter(t, httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		t.Error("inverted-window request reached the server")
		http.NotFound(response, nil)
	})), time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_700_000_000_000).UTC(), End: time.UnixMilli(1_699_999_000_000).UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "end before it starts") {
		t.Fatalf("window error = %v", err)
	}
}

func TestFuturesPositionsRedactsSecretsInErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		// The provider echoes back credential material; it must never survive
		// into the structured error we surface.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"signature for fake-api-key was rejected"}`))
	}))

	adapter := newTestFuturesPositionsAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-key") || !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("message leaked credential material: %q", apiError.Message)
	}
}

func TestFuturesPositionsRejectsMissingCredentials(t *testing.T) {
	t.Parallel()

	adapter, err := NewFuturesPositions(Options{
		BaseURL: "https://fapi.binance.com", HTTPClient: http.DefaultClient,
		Now:       func() time.Time { return time.UnixMilli(1_700_000_000_000).UTC() },
		LookupEnv: mapLookup(map[string]string{}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), defaultAPIKeyEnv) {
		t.Fatalf("credential error = %v", err)
	}
}

func newTestFuturesPositionsAdapter(t *testing.T, server *httptest.Server, now time.Time) *FuturesPositionsAdapter {
	t.Helper()
	t.Cleanup(server.Close)
	adapter, err := NewFuturesPositions(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new futures positions adapter: %v", err)
	}
	return adapter
}
