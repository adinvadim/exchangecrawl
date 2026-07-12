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

func TestWithdrawalsEventPageNormalizesAndPaginates(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-withdraw-key"
		secret = "fake-withdraw-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != withdrawRecordPath {
			t.Errorf("path = %q, want %q", r.URL.Path, withdrawRecordPath)
		}
		wantQuery := url.Values{
			"startTime": {"1700000000000"},
			"endTime":   {"1700000060000"},
			"limit":     {"50"},
		}.Encode()
		if r.URL.RawQuery != wantQuery {
			t.Errorf("query = %q, want %q", r.URL.RawQuery, wantQuery)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		_, _ = fmt.Fprint(w, `{
          "retCode":0,"retMsg":"OK","result":{
            "nextPageCursor":"fake-next-withdraw-cursor",
            "rows":[{
              "withdrawId":"fake-withdraw-1","coin":"USDT","chain":"ETH",
              "amount":"12.000000000000000001","txID":"0xfake",
              "status":"Pending","toAddress":"0xdead","tag":"",
              "withdrawFee":"1.000000000000000001","createTime":"1700000030000",
              "updateTime":"1700000031000","withdrawType":0,"futureField":"preserved"
            }]
          }
        }`)
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	page, err := withdrawals.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch withdrawals event page: %v", err)
	}
	if page.Done || page.NextCursor != "fake-next-withdraw-cursor" || len(page.Observations) != 1 {
		t.Fatalf("page = %#v", page)
	}
	obs := page.Observations[0]
	if obs.ObjectType != "withdrawal" || obs.ObjectID != "fake-withdraw-1" || obs.Status != "Pending" {
		t.Fatalf("observation identity = %#v", obs)
	}
	if obs.Stream != "asset" || obs.Asset != "USDT" || obs.Amount != "12.000000000000000001" || obs.StateFingerprint == "" {
		t.Fatalf("observation normalization = %#v", obs)
	}
	if !obs.ObservedAt.Equal(requestTime) || !obs.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) {
		t.Fatalf("observation timestamps = %#v", obs)
	}
	if !containsJSONField(obs.RawJSON, "futureField", "preserved") {
		t.Fatalf("raw JSON did not preserve unknown field: %s", obs.RawJSON)
	}
}

func TestWithdrawalsEventPagePassesProviderCursor(t *testing.T) {
	t.Parallel()

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_, _ = fmt.Fprint(w, withdrawResponse("fake-cursor-2", validWithdraw("fake-w1", "Pending")))
			return
		}
		if got := r.URL.Query().Get("cursor"); got != "fake-cursor-2" {
			t.Errorf("cursor = %q, want fake-cursor-2", got)
		}
		_, _ = fmt.Fprint(w, withdrawResponse("", validWithdraw("fake-w2", "success")))
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	request := spotPageRequest("")
	first, err := withdrawals.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first event page: %v", err)
	}
	if first.Done || first.NextCursor != "fake-cursor-2" || len(first.Observations) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	request.Cursor = first.NextCursor
	second, err := withdrawals.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second event page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Observations) != 1 {
		t.Fatalf("second page = %#v", second)
	}
}

func TestWithdrawalsTerminalNormalizesAndFilters(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{
          "retCode":0,"result":{"nextPageCursor":"","rows":[
            {"withdrawId":"fake-pending","status":"Pending","createTime":"1700000030000"},
            {"withdrawId":"fake-done","coin":"BTC","chain":"BTC","amount":"0.500000000000000001",
             "txID":"0xdone","status":"success","withdrawFee":"0.000000000000000001",
             "createTime":"1700000040000","updateTime":"1700000041000","withdrawType":1,
             "futureField":"terminal"}
          ]}
        }`)
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	page, err := withdrawals.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal withdrawals: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.EntryID != "withdraw:fake-done" || entry.OrderID != "fake-done" || entry.TradeID != "0xdone" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "withdrawals" || entry.Type != "success" || entry.Asset != "BTC" || entry.Side != "withdraw" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "0.500000000000000001" || entry.Fee != "0.000000000000000001" || entry.Info != "internal" {
		t.Fatalf("entry decimals/type = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(1_700_000_040_000).UTC()) {
		t.Fatalf("entry occurredAt = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "terminal") {
		t.Fatalf("terminal raw JSON = %s", entry.RawJSON)
	}
}

func TestWithdrawalsTerminalSkipsNonterminalOnlyPages(t *testing.T) {
	t.Parallel()

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			_, _ = fmt.Fprint(w, withdrawResponse("fake-next-1", validWithdraw("fake-pending", "Pending")))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "fake-next-1" {
				t.Errorf("cursor = %q, want fake-next-1", got)
			}
			_, _ = fmt.Fprint(w, withdrawResponse("fake-next-2", validWithdraw("fake-check", "SecurityCheck")))
		case 3:
			if got := r.URL.Query().Get("cursor"); got != "fake-next-2" {
				t.Errorf("cursor = %q, want fake-next-2", got)
			}
			_, _ = fmt.Fprint(w, withdrawResponse("", validWithdraw("fake-final", "BlockchainConfirmed")))
		default:
			t.Fatalf("unexpected request %d", calls)
		}
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	page, err := withdrawals.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal withdrawals: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 || page.Entries[0].EntryID != "withdraw:fake-final" {
		t.Fatalf("page = %#v", page)
	}
}

func TestWithdrawalsTerminalRejectsRepeatedCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always report a non-terminal row and the same next cursor to force a loop.
		_, _ = fmt.Fprint(w, withdrawResponse("fake-loop-cursor", validWithdraw("fake-pending", "Pending")))
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	request := spotPageRequest("fake-loop-cursor")
	_, err = withdrawals.FetchPage(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestWithdrawalsWindowBounds(t *testing.T) {
	t.Parallel()

	withdrawals, err := NewWithdrawals(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	start := time.UnixMilli(1_700_000_000_000).UTC()
	request := source.PageRequest{
		Account: source.Account{ID: "fake-account", APIKeyEnv: "FAKE_BYBIT_KEY", APISecretEnv: "FAKE_BYBIT_SECRET"},
		Start:   start,
		End:     start.Add(30 * 24 * time.Hour),
	}
	if _, err := withdrawals.FetchPage(context.Background(), request); err == nil || !strings.Contains(err.Error(), "under 30 days") {
		t.Fatalf("FetchPage window error = %v", err)
	}
	if _, err := withdrawals.FetchEventPage(context.Background(), request); err == nil || !strings.Contains(err.Error(), "under 30 days") {
		t.Fatalf("FetchEventPage window error = %v", err)
	}
}

func TestWithdrawalsErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-withdraw-key"
		secret = "fake-withdraw-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"rejected key %s secret %s","result":{}}`, apiKey, secret)
	}))
	t.Cleanup(server.Close)

	withdrawals, err := NewWithdrawals(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), apiKey, secret))
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	_, err = withdrawals.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "Bybit API error 10003") {
		t.Fatalf("error = %v, want Bybit API error 10003", err)
	}
	if strings.Contains(err.Error(), apiKey) || strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error did not redact credentials: %v", err)
	}
}

func TestIsTerminalWithdrawStatus(t *testing.T) {
	t.Parallel()

	terminal := []string{"success", "Reject", "Fail", "CancelByUser", "BlockchainConfirmed"}
	for _, status := range terminal {
		if !isTerminalWithdrawStatus(status) {
			t.Errorf("status %q should be terminal", status)
		}
	}
	nonTerminal := []string{"SecurityCheck", "Pending", "MoreInformationRequired", "Unknown", ""}
	for _, status := range nonTerminal {
		if isTerminalWithdrawStatus(status) {
			t.Errorf("status %q should not be terminal", status)
		}
	}
}

func TestWithdrawFingerprintTracksMutableFields(t *testing.T) {
	t.Parallel()

	base := withdrawRecord{WithdrawID: "fake", Status: "Pending", UpdateTime: "1700000030000", TxID: ""}
	changedStatus := base
	changedStatus.Status = "success"
	changedUpdate := base
	changedUpdate.UpdateTime = "1700000030001"
	changedTx := base
	changedTx.TxID = "0xfake"
	if withdrawFingerprint(base) == withdrawFingerprint(changedStatus) {
		t.Fatal("fingerprint ignored status")
	}
	if withdrawFingerprint(base) == withdrawFingerprint(changedUpdate) {
		t.Fatal("fingerprint ignored updateTime")
	}
	if withdrawFingerprint(base) == withdrawFingerprint(changedTx) {
		t.Fatal("fingerprint ignored txID")
	}
}

func TestWithdrawalsRejectInvalidRecords(t *testing.T) {
	t.Parallel()

	if _, _, err := decodeWithdraw([]byte(`{"withdrawId":"","status":"success","createTime":"1700000030000"}`)); err == nil ||
		!strings.Contains(err.Error(), "withdrawId") {
		t.Fatalf("missing withdrawId error = %v", err)
	}
	if _, _, err := decodeWithdraw([]byte(`{"withdrawId":"fake","status":"","createTime":"1700000030000"}`)); err == nil ||
		!strings.Contains(err.Error(), "status") {
		t.Fatalf("missing status error = %v", err)
	}
	if _, _, err := decodeWithdraw([]byte(`{"withdrawId":"fake","status":"success","createTime":"invalid"}`)); err == nil ||
		!strings.Contains(err.Error(), "createTime") {
		t.Fatalf("invalid createTime error = %v", err)
	}
}

func withdrawResponse(nextCursor, row string) string {
	rows := "[]"
	if row != "" {
		rows = "[" + row + "]"
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"rows":%s}}`, nextCursor, rows)
}

func validWithdraw(withdrawID, status string) string {
	return fmt.Sprintf(
		`{"withdrawId":%q,"coin":"USDT","amount":"1","status":%q,"createTime":"1700000030000","updateTime":"1700000031000","withdrawType":0}`,
		withdrawID, status,
	)
}
