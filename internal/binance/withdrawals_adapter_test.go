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

func newTestWithdrawalsAdapter(t *testing.T, server *httptest.Server, now time.Time) *WithdrawalsAdapter {
	t.Helper()
	adapter, err := NewWithdrawals(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new withdrawals adapter: %v", err)
	}
	return adapter
}

func withdrawFixture(id string, status int) map[string]any {
	return map[string]any{
		"id": id, "amount": "8.91000000", "transactionFee": "0.00400000", "coin": "USDT",
		"status": status, "address": "0xfakeaddress", "txId": "0xfaketx" + id,
		"applyTime": "2023-03-23 16:52:02", "network": "ETH", "transferType": 0,
		"withdrawOrderId": "WITHDRAW-" + id, "info": "", "confirmNo": 3, "walletType": 1,
		"completeTime": "2023-03-23 16:52:41",
	}
}

func withdrawRequest() source.PageRequest {
	return source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   time.UnixMilli(1_679_000_000_000).UTC(),
		End:     time.UnixMilli(1_679_600_000_000).UTC(),
	}
}

func TestWithdrawalsFetchEventPageSignsAndNormalizes(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	request := withdrawRequest()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, r *http.Request) {
		if r.URL.Path != withdrawHistoryPath {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.NotFound(response, r)
			return
		}
		if got := r.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
			t.Errorf("API key header = %q", got)
		}
		query := r.URL.Query()
		for key, want := range map[string]string{
			"startTime": strconv.FormatInt(request.Start.UnixMilli(), 10),
			"endTime":   strconv.FormatInt(request.End.UnixMilli(), 10),
			"offset":    "0", "limit": "1000", "recvWindow": "5000",
			"timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
		} {
			if got := query.Get(key); got != want {
				t.Errorf("query %s = %q, want %q", key, got, want)
			}
		}
		assertSpotSignature(t, r, "fake-api-secret")
		rows := []map[string]any{
			withdrawFixture("abc123", 6),
			func() map[string]any { // interim row: no completeTime yet
				row := withdrawFixture("def456", 4)
				row["completeTime"] = ""
				return row
			}(),
		}
		_ = json.NewEncoder(response).Encode(rows)
	}))
	t.Cleanup(server.Close)

	adapter := newTestWithdrawalsAdapter(t, server, fixedNow)
	page, err := adapter.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch event page: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 2 {
		t.Fatalf("page = done %t cursor %q observations %d", page.Done, page.NextCursor, len(page.Observations))
	}
	completed := page.Observations[0]
	if completed.Exchange != model.ExchangeBinance || completed.Stream != "asset" || completed.ObjectType != "withdrawal" {
		t.Fatalf("identity = %#v", completed)
	}
	if completed.ObjectID != "abc123" || completed.Status != "Completed" || completed.Asset != "USDT" || completed.Amount != "8.91000000" {
		t.Fatalf("completed = %#v", completed)
	}
	if !completed.OccurredAt.Equal(time.Date(2023, 3, 23, 16, 52, 41, 0, time.UTC)) {
		t.Fatalf("completed occurredAt = %s", completed.OccurredAt)
	}
	if !completed.ObservedAt.Equal(fixedNow) || completed.StateFingerprint == "" {
		t.Fatalf("completed observed = %s fingerprint %q", completed.ObservedAt, completed.StateFingerprint)
	}
	if !json.Valid(completed.RawJSON) || !strings.Contains(string(completed.RawJSON), `"status":6`) {
		t.Fatalf("raw JSON = %s", completed.RawJSON)
	}

	interim := page.Observations[1]
	if interim.Status != "Processing" {
		t.Fatalf("interim status = %q", interim.Status)
	}
	if !interim.OccurredAt.Equal(time.Date(2023, 3, 23, 16, 52, 2, 0, time.UTC)) {
		t.Fatalf("interim falls back to applyTime, got %s", interim.OccurredAt)
	}
	if interim.StateFingerprint == completed.StateFingerprint {
		t.Fatal("distinct withdrawal states share a fingerprint")
	}
}

func TestWithdrawalsTerminalKeepsSettledEntriesOnly(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, r *http.Request) {
		rows := []map[string]any{
			withdrawFixture("settled1", 6),  // Completed -> terminal
			withdrawFixture("pending1", 2),  // Awaiting Approval -> interim, skipped
			withdrawFixture("rejected1", 3), // Rejected -> terminal
		}
		_ = json.NewEncoder(response).Encode(rows)
	}))
	t.Cleanup(server.Close)

	adapter := newTestWithdrawalsAdapter(t, server, fixedNow)
	page, err := adapter.Terminal().FetchPage(t.Context(), withdrawRequest())
	if err != nil {
		t.Fatalf("fetch terminal page: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 2 {
		t.Fatalf("page = done %t cursor %q entries %d", page.Done, page.NextCursor, len(page.Entries))
	}
	first := page.Entries[0]
	if first.EntryID != "withdraw:settled1" || first.Category != "withdrawal" || first.Type != "Completed" {
		t.Fatalf("first entry = %#v", first)
	}
	if first.Asset != "USDT" || first.Amount != "8.91000000" || first.Fee != "0.00400000" || first.CashFlow != "8.91000000" {
		t.Fatalf("first amounts = %#v", first)
	}
	if first.Side != "OUT" || first.TradeID != "0xfaketxsettled1" || first.OrderID != "WITHDRAW-settled1" || first.Info != "external" {
		t.Fatalf("first provider fields = %#v", first)
	}
	if !first.OccurredAt.Equal(time.Date(2023, 3, 23, 16, 52, 41, 0, time.UTC)) {
		t.Fatalf("first occurredAt = %s", first.OccurredAt)
	}
	if page.Entries[1].EntryID != "withdraw:rejected1" || page.Entries[1].Type != "Rejected" {
		t.Fatalf("second entry = %#v", page.Entries[1])
	}
}

func TestWithdrawalsTerminalWindowsOnApplyTime(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	request := withdrawRequest()

	cases := []struct {
		name         string
		applyTime    string
		completeTime string
		wantEntries  int
		wantOccurred time.Time
	}{
		{
			// Wedge bug #2: applied inside the window but settled after the window's
			// end. The row must be archived, and its OccurredAt clamped into the
			// window so archive.validatePageWindow accepts it instead of wedging.
			name:         "completed after window end is clamped and archived",
			applyTime:    "2023-03-23 16:52:02", // inside [start,end]
			completeTime: "2023-03-24 00:00:00", // after request.End
			wantEntries:  1,
			wantOccurred: request.End,
		},
		{
			// Membership is decided on applyTime (the provider's filter field): a
			// withdrawal applied before the window start is dropped even though it
			// happens to settle inside the window.
			name:         "applied before window start is dropped",
			applyTime:    "2023-03-01 00:00:00", // before request.Start
			completeTime: "2023-03-23 16:52:41", // inside [start,end]
			wantEntries:  0,
		},
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				row := withdrawFixture("clamp1", 6) // Completed -> terminal
				row["applyTime"] = testCase.applyTime
				row["completeTime"] = testCase.completeTime
				_ = json.NewEncoder(response).Encode([]map[string]any{row})
			}))
			t.Cleanup(server.Close)

			adapter := newTestWithdrawalsAdapter(t, server, fixedNow)
			page, err := adapter.Terminal().FetchPage(t.Context(), request)
			if err != nil {
				t.Fatalf("fetch terminal page: %v", err)
			}
			if len(page.Entries) != testCase.wantEntries {
				t.Fatalf("entries = %d, want %d", len(page.Entries), testCase.wantEntries)
			}
			if testCase.wantEntries == 0 {
				return
			}
			entry := page.Entries[0]
			if !entry.OccurredAt.Equal(testCase.wantOccurred) {
				t.Fatalf("occurredAt = %s, want %s", entry.OccurredAt, testCase.wantOccurred)
			}
			// The clamped OccurredAt must satisfy archive.validatePageWindow.
			if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
				t.Fatalf("occurredAt %s outside window [%s, %s]", entry.OccurredAt, request.Start, request.End)
			}
			// completeTime is preserved verbatim in RawJSON despite the clamp.
			if !strings.Contains(string(entry.RawJSON), testCase.completeTime) {
				t.Fatalf("completeTime not preserved in RawJSON: %s", entry.RawJSON)
			}
		})
	}
}

func TestWithdrawalsPaginatesByOffset(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, r *http.Request) {
		requests++
		offset := r.URL.Query().Get("offset")
		if requests == 1 {
			if offset != "0" {
				t.Errorf("first offset = %q, want 0", offset)
			}
			rows := make([]map[string]any, withdrawPageLimit)
			for index := range rows {
				rows[index] = withdrawFixture("full-"+strconv.Itoa(index), 6)
			}
			_ = json.NewEncoder(response).Encode(rows)
			return
		}
		if offset != strconv.Itoa(withdrawPageLimit) {
			t.Errorf("second offset = %q, want %d", offset, withdrawPageLimit)
		}
		_ = json.NewEncoder(response).Encode([]map[string]any{withdrawFixture("tail", 6)})
	}))
	t.Cleanup(server.Close)

	adapter := newTestWithdrawalsAdapter(t, server, fixedNow)
	request := withdrawRequest()
	first, err := adapter.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor != strconv.Itoa(withdrawPageLimit) || len(first.Observations) != withdrawPageLimit {
		t.Fatalf("first = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	request.Cursor = first.NextCursor
	second, err := adapter.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Observations) != 1 || second.Observations[0].ObjectID != "tail" {
		t.Fatalf("second = done %t cursor %q observations %#v", second.Done, second.NextCursor, second.Observations)
	}
}

func TestWithdrawalsRejectsBadWindowsAndCursor(t *testing.T) {
	t.Parallel()

	adapter := newTestWithdrawalsAdapter(t, httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("no request expected for rejected input")
	})), time.UnixMilli(1_700_000_000_000).UTC())

	base := source.Account{ID: "primary"}
	cases := []struct {
		name    string
		request source.PageRequest
	}{
		{"end before start", source.PageRequest{Account: base, Start: time.UnixMilli(2_000), End: time.UnixMilli(1_000)}},
		{"zero times", source.PageRequest{Account: base}},
		{
			"window exceeds 90 days",
			source.PageRequest{Account: base, Start: time.UnixMilli(0).UTC(), End: time.UnixMilli(0).Add(withdrawMaxWindow + time.Hour).UTC()},
		},
		{
			"invalid cursor",
			source.PageRequest{Account: base, Start: time.UnixMilli(1_679_000_000_000).UTC(), End: time.UnixMilli(1_679_600_000_000).UTC(), Cursor: "-5"},
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := adapter.FetchEventPage(t.Context(), testCase.request); err == nil {
				t.Fatal("expected event error")
			}
			if _, err := adapter.Terminal().FetchPage(t.Context(), testCase.request); err == nil {
				t.Fatal("expected terminal error")
			}
		})
	}
}

func TestWithdrawalsErrorRedactsSecret(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		// Provider echoes the signing secret; the adapter must redact it.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"signature for fake-api-secret is not valid"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestWithdrawalsAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchEventPage(t.Context(), withdrawRequest())
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-secret") {
		t.Fatalf("secret leaked in error message: %q", apiError.Message)
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("expected redaction marker, got %q", apiError.Message)
	}
}
