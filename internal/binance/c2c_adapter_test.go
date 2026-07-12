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

func newTestC2CAdapter(t *testing.T, server *httptest.Server, now time.Time) *C2CAdapter {
	t.Helper()
	adapter, err := NewC2C(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new c2c adapter: %v", err)
	}
	return adapter
}

func c2cOrderFixture(orderNumber, tradeType, status string, createTime int64) map[string]any {
	return map[string]any{
		"orderNumber": orderNumber, "advNo": "adv-" + orderNumber, "tradeType": tradeType,
		"asset": "USDT", "fiat": "RUB", "fiatSymbol": "P", "amount": "100.00000000",
		"totalPrice": "9500.00", "unitPrice": "95.00", "orderStatus": status,
		"createTime": createTime, "commission": "0.10000000",
		"counterPartNickName": "ab***", "advertisementRole": "TAKER",
	}
}

func c2cHistoryBody(t *testing.T, total int, rows ...map[string]any) string {
	t.Helper()
	if rows == nil {
		rows = []map[string]any{}
	}
	payload := map[string]any{"code": "000000", "message": "success", "success": true, "total": total, "data": rows}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode c2c body: %v", err)
	}
	return string(encoded)
}

// TestC2CFetchPageSignsRequestAndNormalizesTerminalOrder asserts the terminal
// ledger role signs each tradeType call and normalizes provider fields.
func TestC2CFetchPageSignsRequestAndNormalizesTerminalOrder(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	occurred := start.Add(time.Hour)
	var tradeTypes []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != c2cOrderHistoryPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			http.NotFound(response, request)
			return
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
			t.Errorf("API key header = %q", got)
		}
		assertSpotSignature(t, request, "fake-api-secret")
		query := request.URL.Query()
		tradeTypes = append(tradeTypes, query.Get("tradeType"))
		for key, want := range map[string]string{
			"startTimestamp": strconv.FormatInt(start.UnixMilli(), 10),
			"endTimestamp":   strconv.FormatInt(fixedNow.UnixMilli(), 10),
			"page":           "1", "rows": "100", "recvWindow": "5000",
			"timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
		} {
			if got := query.Get(key); got != want {
				t.Errorf("query %s = %q, want %q", key, got, want)
			}
		}
		if query.Get("tradeType") == "BUY" {
			_, _ = response.Write([]byte(c2cHistoryBody(t, 1,
				c2cOrderFixture("20219644646554779648", "BUY", "COMPLETED", occurred.UnixMilli()))))
			return
		}
		_, _ = response.Write([]byte(c2cHistoryBody(t, 0)))
	}))
	t.Cleanup(server.Close)

	adapter := newTestC2CAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}
	page, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if page.NextCursor != "sell:1" || page.Done {
		t.Fatalf("buy page = cursor %q done %t", page.NextCursor, page.Done)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("buy entries = %d", len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.Exchange != model.ExchangeBinance || entry.EntryID != "p2p:20219644646554779648" {
		t.Fatalf("identity = %#v", entry)
	}
	if entry.Category != "p2p" || entry.Type != "COMPLETED" || entry.Symbol != "USDTRUB" {
		t.Fatalf("classification = %#v", entry)
	}
	if entry.Side != "BUY" || entry.Asset != "USDT" || entry.Amount != "100.00000000" {
		t.Fatalf("amounts = %#v", entry)
	}
	if entry.Fee != "0.10000000" || entry.CashFlow != "9500.00" || entry.OrderID != "20219644646554779648" {
		t.Fatalf("provider fields = %#v", entry)
	}
	if entry.Info != "TAKER" || !entry.OccurredAt.Equal(occurred) || !entry.ObservedAt.Equal(fixedNow) {
		t.Fatalf("times/info = %#v", entry)
	}
	if !json.Valid(entry.RawJSON) || !strings.Contains(string(entry.RawJSON), `"orderStatus":"COMPLETED"`) {
		t.Fatalf("raw JSON = %s", entry.RawJSON)
	}

	// The SELL phase is empty, so advancing the cursor completes the stream.
	request.Cursor = page.NextCursor
	sellPage, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch sell page: %v", err)
	}
	if !sellPage.Done || len(sellPage.Entries) != 0 {
		t.Fatalf("sell page = done %t entries %d", sellPage.Done, len(sellPage.Entries))
	}
	if len(tradeTypes) != 2 || tradeTypes[0] != "BUY" || tradeTypes[1] != "SELL" {
		t.Fatalf("tradeType calls = %v", tradeTypes)
	}
}

// TestC2CFetchPageDropsNonTerminalAndOutOfWindow verifies the ledger role keeps
// only terminal statuses within the requested window.
func TestC2CFetchPageDropsNonTerminalAndOutOfWindow(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	inWindow := start.Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("tradeType") == "BUY" {
			_, _ = response.Write([]byte(c2cHistoryBody(t, 3,
				c2cOrderFixture("terminal", "BUY", "COMPLETED", inWindow.UnixMilli()),
				c2cOrderFixture("pending", "BUY", "TRADING", inWindow.UnixMilli()),
				c2cOrderFixture("stale", "BUY", "COMPLETED", start.Add(-time.Hour).UnixMilli()))))
			return
		}
		_, _ = response.Write([]byte(c2cHistoryBody(t, 0)))
	}))
	t.Cleanup(server.Close)

	adapter := newTestC2CAdapter(t, server, fixedNow)
	page, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: start, End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].EntryID != "p2p:terminal" {
		t.Fatalf("entries = %#v", page.Entries)
	}
}

// TestC2CFetchEventPageRecordsEveryStatus asserts the event role captures both
// terminal and non-terminal statuses with distinct fingerprints.
func TestC2CFetchEventPageRecordsEveryStatus(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	occurred := start.Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("tradeType") == "BUY" {
			_, _ = response.Write([]byte(c2cHistoryBody(t, 2,
				c2cOrderFixture("open", "BUY", "TRADING", occurred.UnixMilli()),
				c2cOrderFixture("done", "BUY", "COMPLETED", occurred.UnixMilli()))))
			return
		}
		_, _ = response.Write([]byte(c2cHistoryBody(t, 0)))
	}))
	t.Cleanup(server.Close)

	adapter := newTestC2CAdapter(t, server, fixedNow)
	page, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch event page: %v", err)
	}
	if page.NextCursor != "sell:1" || page.Done || len(page.Observations) != 2 {
		t.Fatalf("event page = cursor %q done %t observations %d", page.NextCursor, page.Done, len(page.Observations))
	}
	open := page.Observations[0]
	if open.Stream != "p2p" || open.ObjectType != "p2p_order" || open.ObjectID != "p2p:open" || open.Status != "TRADING" {
		t.Fatalf("open observation = %#v", open)
	}
	if open.Symbol != "USDTRUB" || open.Amount != "100.00000000" || open.StateFingerprint == "" {
		t.Fatalf("open state = %#v", open)
	}
	if page.Observations[1].StateFingerprint == open.StateFingerprint {
		t.Fatal("distinct statuses share a fingerprint")
	}
	if !open.OccurredAt.Equal(occurred) {
		t.Fatalf("occurred at = %s", open.OccurredAt)
	}
}

// TestC2CFetchPagePaginatesWithinPhase walks multiple pages of one tradeType
// before rolling over to the next phase.
func TestC2CFetchPagePaginatesWithinPhase(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	occurred := start.Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if query.Get("tradeType") == "BUY" {
			switch query.Get("page") {
			case "1":
				rows := make([]map[string]any, c2cPageRows)
				for index := range rows {
					rows[index] = c2cOrderFixture("buy-"+strconv.Itoa(index), "BUY", "COMPLETED", occurred.UnixMilli())
				}
				_, _ = response.Write([]byte(c2cHistoryBody(t, c2cPageRows+1, rows...)))
			case "2":
				_, _ = response.Write([]byte(c2cHistoryBody(t, c2cPageRows+1,
					c2cOrderFixture("buy-last", "BUY", "COMPLETED", occurred.UnixMilli()))))
			default:
				t.Errorf("unexpected BUY page %q", query.Get("page"))
			}
			return
		}
		_, _ = response.Write([]byte(c2cHistoryBody(t, 0)))
	}))
	t.Cleanup(server.Close)

	adapter := newTestC2CAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: start, End: fixedNow}
	first, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor != "buy:2" || first.Done || len(first.Entries) != c2cPageRows {
		t.Fatalf("first = cursor %q done %t entries %d", first.NextCursor, first.Done, len(first.Entries))
	}
	request.Cursor = first.NextCursor
	second, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if second.NextCursor != "sell:1" || second.Done || len(second.Entries) != 1 {
		t.Fatalf("second = cursor %q done %t entries %d", second.NextCursor, second.Done, len(second.Entries))
	}
	if second.Entries[0].EntryID != "p2p:buy-last" {
		t.Fatalf("second entry = %#v", second.Entries[0])
	}
}

// TestC2CRejectsInvalidCursorsAndWindows covers cursor validation and the
// 30-day window bound.
func TestC2CRejectsInvalidCursorsAndWindows(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(c2cHistoryBody(t, 0)))
	}))
	t.Cleanup(server.Close)
	adapter := newTestC2CAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())

	valid := source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_000_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	}

	if _, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: valid.Account,
		Start:   time.UnixMilli(1_600_000_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	}); err == nil {
		t.Fatal("oversized window accepted")
	}

	for _, cursor := range []string{"bogus", "buy:0", "buy:", " buy:1", "buy:x", "hold:1"} {
		request := valid
		request.Cursor = cursor
		if _, err := adapter.FetchPage(t.Context(), request); err == nil {
			t.Fatalf("cursor %q accepted", cursor)
		}
		if _, err := adapter.FetchEventPage(t.Context(), request); err == nil {
			t.Fatalf("event cursor %q accepted", cursor)
		}
	}
}

// TestC2CErrorsRedactSecrets asserts a rejected request surfaces a structured
// APIError whose message never echoes the signature, key, or secret.
func TestC2CErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var leaked string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		// Echo the real signature back inside the provider message to prove redaction.
		leaked = request.URL.Query().Get("signature")
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"Signature ` + leaked + ` for key fake-api-key is invalid"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestC2CAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-24 * time.Hour), End: fixedNow,
	}
	_, err := adapter.FetchPage(t.Context(), request)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if leaked == "" {
		t.Fatal("test server never observed a signature")
	}
	for _, secret := range []string{leaked, "fake-api-key", "fake-api-secret"} {
		if strings.Contains(apiError.Message, secret) {
			t.Fatalf("error message leaked %q: %s", secret, apiError.Message)
		}
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("message not redacted: %s", apiError.Message)
	}

	// The event role shares the same signed path and redaction.
	if _, err := adapter.FetchEventPage(t.Context(), request); !errors.As(err, &apiError) {
		t.Fatalf("event error = %T %v", err, err)
	}
}

// TestC2CMissingCredentialsReported guards the credential preflight.
func TestC2CMissingCredentialsReported(t *testing.T) {
	t.Parallel()

	adapter, err := NewC2C(Options{
		BaseURL: "https://api.binance.com",
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key",
		}),
	})
	if err != nil {
		t.Fatalf("new c2c adapter: %v", err)
	}
	status := adapter.CheckCredentials(source.Account{ID: "primary"})
	if status.Ready || len(status.Missing) != 1 || status.Missing[0] != defaultAPISecretEnv {
		t.Fatalf("credential status = %#v", status)
	}
	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	_, err = adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
	})
	if err == nil || !strings.Contains(err.Error(), defaultAPISecretEnv) {
		t.Fatalf("missing-secret error = %v", err)
	}
}
