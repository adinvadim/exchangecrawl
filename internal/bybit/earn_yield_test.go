package bybit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// earnYieldDigestFixture pins the exact pipe-joined concatenation the EntryID
// digest is computed over. If the field set, order, or separator ever changes
// this fixture must change too, which is the point: the archived identity of
// every historical row must never silently drift.
const earnYieldDigestFixture = "USDT|USDT|1700000030000|1000.500000000000000001|0.123456780000000001|0.0512"

func TestEarnYieldSignsPaginatesAndNormalizes(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-earn-key"
		secret = "fake-earn-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != earnYieldPath {
			t.Errorf("path = %q, want %q", r.URL.Path, earnYieldPath)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		cursor := r.URL.Query().Get("cursor")
		cursors = append(cursors, cursor)
		if cursor == "" {
			// First page: window bounds must be carried, no cursor yet.
			wantQuery := url.Values{
				"startTime": {"1700000000000"},
				"endTime":   {"1700000060000"},
				"limit":     {"50"},
			}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("first query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{
              "nextPageCursor":"fake-earn-next",
              "list":[{
                "coin":"USDT","yieldCoin":"USDT","createdAt":"1700000030000",
                "effectiveAmount":"1000.500000000000000001",
                "pnl":"0.123456780000000001","apy":"0.0512","futureField":"preserved"
              }]
            }}`)
			return
		}
		if cursor != "fake-earn-next" {
			t.Errorf("second cursor = %q, want fake-earn-next", cursor)
		}
		_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{
          "nextPageCursor":"",
          "list":[{
            "coin":"MNT","yieldCoin":"MNT","createdAt":"1700000045000",
            "effectiveAmount":"42.000000000000000001",
            "pnl":"0.000000000000000009","apy":"0.09"
          }]
        }}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewEarnYield(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new earn yield adapter: %v", err)
	}
	page, err := adapter.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch earn yield: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(cursors, ",") != ",fake-earn-next" {
		t.Fatalf("cursors = %v", cursors)
	}

	first := page.Entries[0]
	wantDigest := fmt.Sprintf("earn-yield:%x", sha256.Sum256([]byte(earnYieldDigestFixture)))
	if first.EntryID != wantDigest {
		t.Fatalf("entry id = %q, want %q", first.EntryID, wantDigest)
	}
	if first.Category != "earn" || first.Type != "yield" || first.Symbol != "USDT" || first.Asset != "USDT" {
		t.Fatalf("entry classification = %#v", first)
	}
	if first.Amount != "0.123456780000000001" || first.CashFlow != "0.123456780000000001" {
		t.Fatalf("entry amount decimals = %#v", first)
	}
	if first.Balance != "1000.500000000000000001" || first.Info != "0.0512" {
		t.Fatalf("entry principal/apy = %#v", first)
	}
	if !first.ObservedAt.Equal(requestTime) || !first.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) {
		t.Fatalf("entry timestamps = %#v", first)
	}
	if !containsJSONField(first.RawJSON, "futureField", "preserved") {
		t.Fatalf("raw JSON did not preserve unknown field: %s", first.RawJSON)
	}

	// A different row yields a different synthesized identity.
	if page.Entries[1].EntryID == first.EntryID {
		t.Fatalf("distinct rows collided on entry id %q", first.EntryID)
	}
}

func TestEarnYieldEntryIDMatchesDocumentedDigest(t *testing.T) {
	t.Parallel()

	record := earnYieldRecord{
		CoinName:        "USDT",
		YieldCoinName:   "USDT",
		CreatedAt:       "1700000030000",
		EffectiveAmount: "1000.500000000000000001",
		Pnl:             "0.123456780000000001",
		Apy:             "0.0512",
	}
	want := fmt.Sprintf("earn-yield:%x", sha256.Sum256([]byte(earnYieldDigestFixture)))
	if got := earnYieldEntryID(record); got != want {
		t.Fatalf("entry id = %q, want %q", got, want)
	}
}

func TestEarnYieldRejectsRepeatedCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always advertise the same next cursor to simulate a provider loop.
		_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{
          "nextPageCursor":"loop",
          "list":[{"coin":"USDT","yieldCoin":"USDT","createdAt":"1700000030000",
            "effectiveAmount":"1","pnl":"0.1","apy":"0.05"}]
        }}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewEarnYield(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new earn yield adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestEarnYieldInvalidTimestampRejected(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[
          {"coin":"USDT","yieldCoin":"USDT","createdAt":"not-a-time",
           "effectiveAmount":"1","pnl":"0.1","apy":"0.05"}
        ]}}`)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewEarnYield(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new earn yield adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "createdAt") {
		t.Fatalf("error = %v, want createdAt validation", err)
	}
}

func TestEarnYieldRedactsSecretsInErrors(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "sensitive-earn-key"
		secret = "sensitive-earn-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		message := strings.Join([]string{
			"invalid credentials",
			apiKey,
			secret,
			r.Header.Get("X-BAPI-SIGN"),
			r.URL.String(),
		}, " ")
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":%q,"result":{}}`, message)
	}))
	t.Cleanup(server.Close)

	adapter, err := NewEarnYield(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new earn yield adapter: %v", err)
	}
	_, err = adapter.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "Bybit API error 10003") {
		t.Fatalf("error = %v, want API error 10003", err)
	}
	for _, sensitive := range []string{apiKey, secret, server.URL, "startTime="} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf("error leaked sensitive data %q: %v", sensitive, err)
		}
	}
}
