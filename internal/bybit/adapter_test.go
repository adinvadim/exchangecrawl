package bybit

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestFetchPageSignsRequestAndNormalizesLedgerEntry(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "test-api-key"
		secret = "test-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	start := time.UnixMilli(1_700_000_000_000).UTC()
	end := time.UnixMilli(1_700_000_060_000).UTC()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != transactionLogPath {
			t.Errorf("path = %q, want %q", r.URL.Path, transactionLogPath)
		}
		wantQuery := url.Values{
			"accountType": {"UNIFIED"},
			"startTime":   {"1700000000000"},
			"endTime":     {"1700000060000"},
			"limit":       {"50"},
		}.Encode()
		if r.URL.RawQuery != wantQuery {
			t.Errorf("query = %q, want %q", r.URL.RawQuery, wantQuery)
		}
		assertHeader(t, r.Header, "X-BAPI-API-KEY", apiKey)
		assertHeader(t, r.Header, "X-BAPI-TIMESTAMP", "1700000123456")
		assertHeader(t, r.Header, "X-BAPI-RECV-WINDOW", "5000")
		assertHeader(t, r.Header, "X-BAPI-SIGN", hmacHex(secret, "1700000123456"+apiKey+"5000"+wantQuery))
		if got := r.Header.Get("X-BAPI-SIGN-TYPE"); got != "" {
			t.Errorf("X-BAPI-SIGN-TYPE = %q, want empty", got)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "nextPageCursor": "21963%3A1%2C14954%3A1",
                "list": [{
                    "id": "entry-1",
                    "symbol": "XRPUSDT",
                    "category": "linear",
                    "side": "Buy",
                    "transactionTime": "1672128000000",
                    "type": "SETTLEMENT",
                    "transSubType": "movePosition",
                    "currency": "USDT",
                    "funding": "-0.003676",
                    "fee": "0.00000000",
                    "cashFlow": "1.25",
                    "change": "1.246324",
                    "cashBalance": "5086.55825002",
                    "tradeId": "trade-1",
                    "orderId": "order-1",
                    "futureField": "preserved"
                }]
            },
            "time": 1672132481405
        }`)
	}))
	t.Cleanup(server.Close)

	env := map[string]string{
		"BYBIT_TEST_KEY":    apiKey,
		"BYBIT_TEST_SECRET": secret,
	}
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return requestTime },
		LookupEnv:  mapLookup(env),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	account := source.Account{
		ID:           "main",
		Label:        "Main",
		APIKeyEnv:    "BYBIT_TEST_KEY",
		APISecretEnv: "BYBIT_TEST_SECRET",
	}

	page, err := adapter.FetchPage(context.Background(), source.PageRequest{
		Account: account,
		Start:   start,
		End:     end,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if adapter.Exchange() != model.ExchangeBybit {
		t.Fatalf("exchange = %q, want %q", adapter.Exchange(), model.ExchangeBybit)
	}
	if page.Done {
		t.Fatal("page is done with a next cursor")
	}
	if want := "21963%3A1%2C14954%3A1"; page.NextCursor != want {
		t.Fatalf("next cursor = %q, want %q", page.NextCursor, want)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(page.Entries))
	}

	entry := page.Entries[0]
	if entry.Exchange != model.ExchangeBybit || entry.AccountID != "main" || entry.AccountLabel != "Main" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.EntryID != "entry-1" || entry.Symbol != "XRPUSDT" || entry.Category != "linear" || entry.Type != "SETTLEMENT" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Asset != "USDT" || entry.Side != "Buy" || entry.Amount != "1.246324" {
		t.Fatalf("entry amount = %#v", entry)
	}
	if entry.Fee != "0.00000000" || entry.Funding != "-0.003676" || entry.CashFlow != "1.25" || entry.Balance != "5086.55825002" {
		t.Fatalf("entry financial fields = %#v", entry)
	}
	if entry.OrderID != "order-1" || entry.TradeID != "trade-1" || entry.Info != "movePosition" {
		t.Fatalf("entry provider fields = %#v", entry)
	}
	if want := time.UnixMilli(1_672_128_000_000).UTC(); !entry.OccurredAt.Equal(want) {
		t.Fatalf("occurred at = %s, want %s", entry.OccurredAt, want)
	}
	if !entry.ObservedAt.Equal(requestTime) {
		t.Fatalf("observed at = %s, want %s", entry.ObservedAt, requestTime)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "preserved") {
		t.Fatalf("raw JSON did not preserve unknown field: %s", entry.RawJSON)
	}
}

func TestFetchPageMapsOpaqueCursor(t *testing.T) {
	t.Parallel()

	const cursor = "21963%3A1%2C14954%3A1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cursor"); got != cursor {
			t.Errorf("cursor = %q, want %q", got, cursor)
		}
		_, _ = fmt.Fprint(w, `{
            "retCode": 0,
            "retMsg": "OK",
            "result": {"nextPageCursor": "", "list": []}
        }`)
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return time.UnixMilli(1_700_000_123_456) },
		LookupEnv: mapLookup(map[string]string{
			"KEY":    "api-key",
			"SECRET": "api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	page, err := adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
		Cursor:  cursor,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if !page.Done || page.NextCursor != "" {
		t.Fatalf("page cursor state = %#v, want done", page)
	}
}

func TestFetchPageReturnsSanitizedEnvelopeError(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "sensitive-api-key"
		secret = "sensitive-api-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		message := strings.Join([]string{
			"invalid credentials",
			apiKey,
			secret,
			r.Header.Get("X-BAPI-SIGN"),
			r.URL.String(),
		}, " ")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"retCode": 10003,
			"retMsg":  message,
			"result":  map[string]any{},
		})
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return time.UnixMilli(1_700_000_123_456) },
		LookupEnv: mapLookup(map[string]string{
			"KEY":    apiKey,
			"SECRET": secret,
		}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	if err == nil {
		t.Fatal("fetch page succeeded, want API error")
	}
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != 10003 {
		t.Fatalf("error = %T %v, want API error 10003", err, err)
	}
	for _, sensitive := range []string{apiKey, secret, server.URL, "startTime="} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf("error contains sensitive request data %q: %v", sensitive, err)
		}
	}
}

func TestFetchPageRejectsMalformedSuccessEnvelope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		LookupEnv:  mapLookup(map[string]string{"KEY": "api-key", "SECRET": "api-secret"}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	if err == nil || !strings.Contains(err.Error(), "decode Bybit response") {
		t.Fatalf("error = %v, want malformed envelope error", err)
	}
}

func TestCheckCredentialsAcceptsHMACOrRSAWithoutExposingValues(t *testing.T) {
	t.Parallel()

	account := source.Account{
		ID:                "main",
		APIKeyEnv:         "KEY",
		APISecretEnv:      "SECRET",
		PrivateKeyPathEnv: "RSA_PATH",
	}
	tests := []struct {
		name        string
		env         map[string]string
		ready       bool
		wantMissing []string
	}{
		{
			name:        "missing",
			env:         map[string]string{},
			wantMissing: []string{"KEY", "SECRET", "RSA_PATH"},
		},
		{
			name:  "HMAC",
			env:   map[string]string{"KEY": "key-value", "SECRET": "secret-value"},
			ready: true,
		},
		{
			name:  "RSA",
			env:   map[string]string{"KEY": "key-value", "RSA_PATH": "/private/key.pem"},
			ready: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Options{BaseURL: "https://api.bybit.test", LookupEnv: mapLookup(test.env)})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			status := adapter.CheckCredentials(account)
			if status.Ready != test.ready || !reflect.DeepEqual(status.Missing, test.wantMissing) {
				t.Fatalf("credential status = %#v, want ready=%t missing=%v", status, test.ready, test.wantMissing)
			}
			serialized, err := json.Marshal(status)
			if err != nil {
				t.Fatalf("marshal status: %v", err)
			}
			for _, value := range test.env {
				if strings.Contains(string(serialized), value) {
					t.Fatalf("credential status contains value %q: %s", value, serialized)
				}
			}
		})
	}
}

func TestCustomHMACAccountDoesNotInheritGlobalRSAPrivateKey(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "custom-api-key"
		secret = "custom-api-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-BAPI-SIGN-TYPE"); got != "" {
			t.Errorf("X-BAPI-SIGN-TYPE = %q, want HMAC", got)
		}
		wantSignature := hmacHex(secret, strconv.FormatInt(requestTime.UnixMilli(), 10)+apiKey+"5000"+r.URL.RawQuery)
		assertHeader(t, r.Header, "X-BAPI-SIGN", wantSignature)
		_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[]}}`)
	}))
	t.Cleanup(server.Close)

	readFileCalled := false
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return requestTime },
		LookupEnv: mapLookup(map[string]string{
			"CUSTOM_KEY":                 apiKey,
			"CUSTOM_SECRET":              secret,
			"BYBIT_API_PRIVATE_KEY_PATH": "/global/private.pem",
		}),
		ReadFile: func(string) ([]byte, error) {
			readFileCalled = true
			return nil, errors.New("must not read global RSA key")
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	account := source.Account{ID: "custom", APIKeyEnv: "CUSTOM_KEY", APISecretEnv: "CUSTOM_SECRET"}
	if status := adapter.CheckCredentials(account); !status.Ready {
		t.Fatalf("credential status = %#v, want ready", status)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: account,
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if readFileCalled {
		t.Fatal("custom HMAC account read the global RSA private key")
	}
}

func TestNewRejectsPlainHTTPBaseURLFromEnvironment(t *testing.T) {
	t.Parallel()

	_, err := New(Options{LookupEnv: mapLookup(map[string]string{
		"BYBIT_API_BASE_URL": "http://api.bybit.test",
	})})
	if err == nil {
		t.Fatal("new adapter accepted plaintext production base URL")
	}
}

func TestNewAllowsPlainHTTPOnlyForInjectedLoopback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		baseURL    string
		httpClient *http.Client
		wantError  bool
	}{
		{name: "injected IPv4 loopback", baseURL: "http://127.0.0.1:8080", httpClient: &http.Client{}},
		{name: "injected localhost", baseURL: "http://localhost:8080", httpClient: &http.Client{}},
		{name: "loopback without injected client", baseURL: "http://127.0.0.1:8080", wantError: true},
		{name: "arbitrary host with injected client", baseURL: "http://api.bybit.test", httpClient: &http.Client{}, wantError: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(Options{BaseURL: test.baseURL, HTTPClient: test.httpClient})
			if (err != nil) != test.wantError {
				t.Fatalf("New() error = %v, wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestFetchPageUsesInjectedRSAPrivateKey(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	privateKeyPEM := mustMarshalPKCS8(t, privateKey)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeader(t, r.Header, "X-BAPI-SIGN-TYPE", "2")
		signature, err := base64.StdEncoding.DecodeString(r.Header.Get("X-BAPI-SIGN"))
		if err != nil {
			t.Errorf("decode signature: %v", err)
		}
		payload := "1700000123456api-key5000" + r.URL.RawQuery
		digest := sha256.Sum256([]byte(payload))
		if err := rsa.VerifyPKCS1v15(&privateKey.PublicKey, crypto.SHA256, digest[:], signature); err != nil {
			t.Errorf("verify request signature: %v", err)
		}
		_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[]}}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return requestTime },
		LookupEnv: mapLookup(map[string]string{
			"KEY":      "api-key",
			"SECRET":   "ignored-hmac-secret",
			"RSA_PATH": "/virtual/private.pem",
		}),
		ReadFile: func(path string) ([]byte, error) {
			if path != "/virtual/private.pem" {
				t.Fatalf("read path = %q", path)
			}
			return privateKeyPEM, nil
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{
			ID:                "main",
			APIKeyEnv:         "KEY",
			APISecretEnv:      "SECRET",
			PrivateKeyPathEnv: "RSA_PATH",
		},
		Start: time.UnixMilli(1_700_000_000_000),
		End:   time.UnixMilli(1_700_000_060_000),
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
}

func TestFetchPageRetriesRateLimitsWithRetryAfterAndFreshSignature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		firstReply func(http.ResponseWriter)
	}{
		{
			name: "HTTP 429",
			firstReply: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprint(w, `{"retCode":10006,"retMsg":"rate limit"}`)
			},
		},
		{
			name: "Bybit retCode 10006",
			firstReply: func(w http.ResponseWriter) {
				_, _ = fmt.Fprint(w, `{"retCode":10006,"retMsg":"rate limit","result":{}}`)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var attempts int
			var timestamps, signatures []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				timestamps = append(timestamps, r.Header.Get("X-BAPI-TIMESTAMP"))
				signatures = append(signatures, r.Header.Get("X-BAPI-SIGN"))
				if attempts == 1 {
					w.Header().Set("Retry-After", "2")
					test.firstReply(w)
					return
				}
				_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[]}}`)
			}))
			t.Cleanup(server.Close)

			baseTime := time.UnixMilli(1_700_000_123_456).UTC()
			nowCalls := 0
			var sleeps []time.Duration
			adapter, err := New(Options{
				BaseURL:    server.URL,
				HTTPClient: server.Client(),
				Now: func() time.Time {
					result := baseTime.Add(time.Duration(nowCalls) * time.Second)
					nowCalls++
					return result
				},
				LookupEnv: mapLookup(map[string]string{"KEY": "api-key", "SECRET": "api-secret"}),
				Sleep: func(ctx context.Context, delay time.Duration) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					sleeps = append(sleeps, delay)
					return nil
				},
			})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.FetchPage(context.Background(), source.PageRequest{
				Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
				Start:   time.UnixMilli(1_700_000_000_000),
				End:     time.UnixMilli(1_700_000_060_000),
			})
			if err != nil {
				t.Fatalf("fetch page: %v", err)
			}
			if attempts != 2 || !reflect.DeepEqual(sleeps, []time.Duration{2 * time.Second}) {
				t.Fatalf("attempts/sleeps = %d/%v, want 2/[2s]", attempts, sleeps)
			}
			if len(timestamps) != 2 || timestamps[0] == timestamps[1] {
				t.Fatalf("timestamps = %v, want fresh timestamp per attempt", timestamps)
			}
			if len(signatures) != 2 || signatures[0] == signatures[1] {
				t.Fatalf("signatures = %v, want fresh signature per attempt", signatures)
			}
		})
	}
}

func TestFetchPageBoundsRetryableResponsesToThreeRetries(t *testing.T) {
	t.Parallel()

	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	var sleeps int
	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		LookupEnv:  mapLookup(map[string]string{"KEY": "api-key", "SECRET": "api-secret"}),
		Sleep: func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	var httpError *HTTPError
	if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %T %v, want HTTP 503", err, err)
	}
	if attempts != maxRequestRetries+1 || sleeps != maxRequestRetries {
		t.Fatalf("attempts/sleeps = %d/%d, want %d/%d", attempts, sleeps, maxRequestRetries+1, maxRequestRetries)
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

func TestFetchPageDoesNotRetryPermanentFailure(t *testing.T) {
	t.Parallel()

	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		_, _ = fmt.Fprint(w, `{"retCode":10003,"retMsg":"invalid key","result":{}}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		LookupEnv:  mapLookup(map[string]string{"KEY": "api-key", "SECRET": "api-secret"}),
		Sleep: func(context.Context, time.Duration) error {
			t.Fatal("permanent failure requested a retry sleep")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != 10003 {
		t.Fatalf("error = %T %v, want API error 10003", err, err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestNormalizeEntryRejectsTimestampOutsideSQLiteNanosecondRange(t *testing.T) {
	t.Parallel()

	_, err := normalizeEntry(json.RawMessage(`{"id":"entry-1","transactionTime":"9223372036854775807"}`), source.Account{}, time.Now())
	if err == nil {
		t.Fatal("normalizeEntry() accepted timestamp outside SQLite nanosecond range")
	}
}

func TestFetchPageDoesNotForwardSignedHeadersAcrossRedirect(t *testing.T) {
	t.Parallel()

	var redirected bool
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
		if r.Header.Get("X-BAPI-API-KEY") != "" || r.Header.Get("X-BAPI-SIGN") != "" {
			t.Errorf("redirect received signed headers")
		}
		_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"list":[]}}`)
	}))
	t.Cleanup(sink.Close)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL+transactionLogPath, http.StatusFound)
	}))
	t.Cleanup(server.Close)

	adapter, err := New(Options{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		Now:        func() time.Time { return time.UnixMilli(1_700_000_123_456) },
		LookupEnv:  mapLookup(map[string]string{"KEY": "api-key", "SECRET": "api-secret"}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), source.PageRequest{
		Account: source.Account{ID: "main", APIKeyEnv: "KEY", APISecretEnv: "SECRET"},
		Start:   time.UnixMilli(1_700_000_000_000),
		End:     time.UnixMilli(1_700_000_060_000),
	})
	var httpError *HTTPError
	if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusFound {
		t.Fatalf("error = %T %v, want HTTP 302", err, err)
	}
	if redirected {
		t.Fatal("signed request followed redirect")
	}
}

func assertHeader(t *testing.T, header http.Header, name, want string) {
	t.Helper()
	if got := header.Get(name); got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

func hmacHex(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func containsJSONField(raw []byte, name, want string) bool {
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	got, ok := value[name].(string)
	return ok && got == want
}
