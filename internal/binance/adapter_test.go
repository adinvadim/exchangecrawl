package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestHMACSignatureMatchesBybitBarGoldenVector(t *testing.T) {
	t.Parallel()

	query := "symbol=BTCUSDT&timestamp=1700000000000&recvWindow=5000"
	if got, want := sign("mysecret", query), "943a8c63f0a712588329984a033f6a7079a08171a0e206725b4d6cdcb542b6df"; got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}

func TestFetchPageSignsIncomeRequestAndNormalizesEntry(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := time.UnixMilli(1_699_900_000_000).UTC()
	end := time.UnixMilli(1_699_999_999_999).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", request.Method)
		}
		if request.URL.Path != incomePath {
			t.Errorf("path = %q, want %q", request.URL.Path, incomePath)
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "api-key" {
			t.Errorf("API key header = %q, want api-key", got)
		}

		query := request.URL.Query()
		wantValues := map[string]string{
			"startTime":  strconv.FormatInt(start.UnixMilli(), 10),
			"endTime":    strconv.FormatInt(end.UnixMilli(), 10),
			"page":       "1",
			"limit":      "1000",
			"timestamp":  strconv.FormatInt(fixedNow.UnixMilli(), 10),
			"recvWindow": "5000",
		}
		for key, want := range wantValues {
			if got := query.Get(key); got != want {
				t.Errorf("query %s = %q, want %q", key, got, want)
			}
		}

		rawQuery := request.URL.RawQuery
		signatureIndex := strings.LastIndex(rawQuery, "&signature=")
		if signatureIndex == -1 {
			t.Errorf("raw query has no trailing signature: %q", rawQuery)
		} else {
			unsigned := rawQuery[:signatureIndex]
			if got, want := query.Get("signature"), sign("api-secret", unsigned); got != want {
				t.Errorf("signature = %q, want %q over %q", got, want, unsigned)
			}
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`[{"symbol":"BTCUSDT","incomeType":"COMMISSION","income":"-0.12500000","asset":"USDT","info":"maker commission","time":1699999999000,"tranId":9689322392,"tradeId":"2059192"}]`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestAdapter(t, server, fixedNow, map[string]string{
		"BINANCE_MAIN_KEY":    "api-key",
		"BINANCE_MAIN_SECRET": "api-secret",
	})
	account := source.Account{
		ID:           "main",
		Label:        "Main",
		APIKeyEnv:    "BINANCE_MAIN_KEY",
		APISecretEnv: "BINANCE_MAIN_SECRET",
	}

	page, err := adapter.FetchPage(t.Context(), source.PageRequest{Account: account, Start: start, End: end})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if !page.Done || page.NextCursor != "" {
		t.Fatalf("pagination = done %t, cursor %q; want done with empty cursor", page.Done, page.NextCursor)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.Exchange != model.ExchangeBinance || entry.AccountID != "main" || entry.AccountLabel != "Main" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.EntryID != "9689322392" || entry.Category != "income" || entry.Type != "COMMISSION" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "-0.12500000" || entry.Fee != "-0.12500000" || entry.CashFlow != "-0.12500000" {
		t.Fatalf("entry amounts = %#v", entry)
	}
	if entry.Symbol != "BTCUSDT" || entry.Asset != "USDT" || entry.TradeID != "2059192" {
		t.Fatalf("entry provider fields = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000).UTC()) || !entry.ObservedAt.Equal(fixedNow) {
		t.Fatalf("entry times = occurred %s observed %s", entry.OccurredAt, entry.ObservedAt)
	}
	if !json.Valid(entry.RawJSON) || !strings.Contains(string(entry.RawJSON), `"tranId":9689322392`) {
		t.Fatalf("raw JSON = %s", entry.RawJSON)
	}
}

func TestFetchPageReturnsRedactedStructuredAPIError(t *testing.T) {
	t.Parallel()

	signatureChannel := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		echoedSignature := request.URL.Query().Get("signature")
		signatureChannel <- echoedSignature
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{
			"code": -1022,
			"msg":  fmt.Sprintf("invalid request %s api-key api-secret %s", request.URL.String(), echoedSignature),
		})
	}))
	t.Cleanup(server.Close)

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	adapter := newTestAdapter(t, server, fixedNow, map[string]string{
		defaultAPIKeyEnv:    "api-key",
		defaultAPISecretEnv: "api-secret",
	})
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
	})
	if err == nil {
		t.Fatal("fetch page succeeded, want API error")
	}
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("error type = %T, want *APIError", err)
	}
	if apiError.StatusCode != http.StatusBadRequest || apiError.Code != -1022 {
		t.Fatalf("API error = %#v", apiError)
	}
	echoedSignature := <-signatureChannel
	for _, secret := range []string{"api-key", "api-secret", echoedSignature, "signature="} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

func TestFetchPageSanitizesRemoteErrorControlCharacters(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{
			"code": -1022,
			"msg":  "invalid\n\t\x1b[31m\u202erequest",
		})
	}))
	t.Cleanup(server.Close)

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	adapter := newTestAdapter(t, server, fixedNow, map[string]string{
		defaultAPIKeyEnv:    "api-key",
		defaultAPISecretEnv: "api-secret",
	})
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
	})
	if err == nil {
		t.Fatal("fetch page succeeded, want API error")
	}
	for _, r := range err.Error() {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			t.Fatalf("error contains control character %U: %q", r, err)
		}
	}
}

func TestNormalizeIncomeErrorDoesNotEchoRemoteTransactionID(t *testing.T) {
	t.Parallel()

	_, err := normalizeIncome(incomeRow{TranID: "remote\n\x1b[31m", Time: 0}, source.Account{}, time.Now())
	if err == nil {
		t.Fatal("normalizeIncome() expected occurrence-time error")
	}
	if strings.Contains(err.Error(), "remote") || strings.ContainsAny(err.Error(), "\n\x1b") {
		t.Fatalf("error echoes remote transaction id: %q", err)
	}
}

func TestFetchPageRetriesTransientResponseWithFreshSignature(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var requests atomic.Int32
	timestamps := make(chan string, 2)
	signatures := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		timestamps <- request.URL.Query().Get("timestamp")
		signatures <- request.URL.Query().Get("signature")
		if requests.Add(1) == 1 {
			response.Header().Set("Retry-After", "2")
			response.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(response).Encode(map[string]any{"code": -1003, "msg": "rate limited"})
			return
		}
		_ = json.NewEncoder(response).Encode([]map[string]any{incomeFixture("retry-1", "1.0", "FUNDING_FEE")})
	}))
	t.Cleanup(server.Close)

	var nowCalls atomic.Int64
	sleeps := make(chan time.Duration, maxRetries)
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now: func() time.Time {
			return fixedNow.Add(time.Duration(nowCalls.Add(1)-1) * time.Second)
		},
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv:    "api-key",
			defaultAPISecretEnv: "api-secret",
		}),
		Sleep: func(ctx context.Context, duration time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				sleeps <- duration
				return nil
			}
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	page, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].EntryID != "retry-1" {
		t.Fatalf("entries = %#v", page.Entries)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
	if delay := <-sleeps; delay != 2*time.Second {
		t.Fatalf("retry delay = %s, want 2s", delay)
	}
	firstTimestamp, secondTimestamp := <-timestamps, <-timestamps
	if firstTimestamp == secondTimestamp {
		t.Fatalf("retry reused timestamp %q", firstTimestamp)
	}
	firstSignature, secondSignature := <-signatures, <-signatures
	if firstSignature == secondSignature {
		t.Fatalf("retry reused signature %q", firstSignature)
	}
}

func TestFetchPageDoesNotRetryPermanentClientError(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var sleeps atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{"code": -1022, "msg": "bad signature"})
	}))
	t.Cleanup(server.Close)

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return fixedNow },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv:    "api-key",
			defaultAPISecretEnv: "api-secret",
		}),
		Sleep: func(context.Context, time.Duration) error {
			sleeps.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
	})
	if err == nil {
		t.Fatal("fetch page succeeded, want API error")
	}
	if requests.Load() != 1 || sleeps.Load() != 0 {
		t.Fatalf("requests = %d, sleeps = %d; want 1, 0", requests.Load(), sleeps.Load())
	}
}

func TestFetchPageStopsAfterThreeRetries(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var sleeps atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		response.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(response).Encode(map[string]any{"code": -1000, "msg": "unavailable"})
	}))
	t.Cleanup(server.Close)

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return fixedNow },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv:    "api-key",
			defaultAPISecretEnv: "api-secret",
		}),
		Sleep: func(context.Context, time.Duration) error {
			sleeps.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
	})
	if err == nil {
		t.Fatal("fetch page succeeded, want API error")
	}
	if requests.Load() != maxRetries+1 || sleeps.Load() != maxRetries {
		t.Fatalf("requests = %d, sleeps = %d; want %d, %d", requests.Load(), sleeps.Load(), maxRetries+1, maxRetries)
	}
}

func TestRetryDelayClampsUntrustedRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	for _, value := range []string{"999999999999999999", now.Add(100 * 365 * 24 * time.Hour).Format(http.TimeFormat)} {
		if got := retryDelay(value, now, 0); got != maxRetryDelay {
			t.Fatalf("retryDelay(%q) = %s, want %s", value, got, maxRetryDelay)
		}
	}
}

func TestFetchPageUsesDecimalCursorAndAdvancesFullPage(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		page := request.URL.Query().Get("page")
		var rows []map[string]any
		switch page {
		case "7":
			rows = make([]map[string]any, pageLimit)
			for index := range rows {
				rows[index] = incomeFixture(strconv.Itoa(10_000+index), "1.00000000", "FUNDING_FEE")
			}
		case "8":
			rows = []map[string]any{incomeFixture("10000", "2.00000000", "FUNDING_FEE")}
		default:
			t.Errorf("page = %q, want 7 or 8", page)
			rows = []map[string]any{}
		}
		_ = json.NewEncoder(response).Encode(rows)
	}))
	t.Cleanup(server.Close)

	adapter := newTestAdapter(t, server, fixedNow, map[string]string{
		defaultAPIKeyEnv:    "api-key",
		defaultAPISecretEnv: "api-secret",
	})
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour),
		End:     fixedNow,
		Cursor:  "7",
	}
	first, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch first page: %v", err)
	}
	if first.Done || first.NextCursor != "8" || len(first.Entries) != pageLimit {
		t.Fatalf("first page = done %t cursor %q entries %d", first.Done, first.NextCursor, len(first.Entries))
	}
	if first.Entries[0].Funding != "1.00000000" {
		t.Fatalf("first funding = %q", first.Entries[0].Funding)
	}

	request.Cursor = first.NextCursor
	second, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch second page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Entries) != 1 {
		t.Fatalf("second page = done %t cursor %q entries %d", second.Done, second.NextCursor, len(second.Entries))
	}
	if first.Entries[0].EntryID != second.Entries[0].EntryID {
		t.Fatalf("corrected entry ID changed: %q != %q", first.Entries[0].EntryID, second.Entries[0].EntryID)
	}
	if second.Entries[0].Amount != "2.00000000" {
		t.Fatalf("corrected amount = %q", second.Entries[0].Amount)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}

func TestCheckCredentialsReportsOnlyEnvironmentVariableNames(t *testing.T) {
	t.Parallel()

	adapter, err := New(Options{LookupEnv: mapLookup(map[string]string{
		"CUSTOM_KEY": "present-key",
	})})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	status := adapter.CheckCredentials(source.Account{APIKeyEnv: "CUSTOM_KEY", APISecretEnv: "CUSTOM_SECRET"})
	if status.Ready {
		t.Fatal("credentials ready, want missing secret")
	}
	if len(status.Missing) != 1 || status.Missing[0] != "CUSTOM_SECRET" {
		t.Fatalf("missing = %#v, want CUSTOM_SECRET", status.Missing)
	}
}

func TestNewRejectsRemoteOrUninjectedPlainHTTP(t *testing.T) {
	t.Parallel()

	tests := []Options{
		{BaseURL: "http://example.com", HTTPClient: &http.Client{}},
		{BaseURL: "http://127.0.0.1:8080"},
	}
	for _, options := range tests {
		if _, err := New(options); err == nil {
			t.Errorf("New(%q) succeeded, want HTTP rejection", options.BaseURL)
		}
	}
}

func newTestAdapter(t *testing.T, server *httptest.Server, now time.Time, environment map[string]string) *Adapter {
	t.Helper()
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return now },
		LookupEnv:  mapLookup(environment),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	return adapter
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func incomeFixture(transactionID, income, incomeType string) map[string]any {
	return map[string]any{
		"symbol":     "BTCUSDT",
		"incomeType": incomeType,
		"income":     income,
		"asset":      "USDT",
		"info":       "fixture",
		"time":       int64(1_699_999_999_000),
		"tranId":     transactionID,
		"tradeId":    "trade-1",
	}
}
