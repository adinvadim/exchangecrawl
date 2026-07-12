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

const earnRowTime = int64(1_699_999_000_000)

func TestEarnFetchEventPageWalksEveryPhase(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assertEarnSignedWindow(t, request, start, fixedNow)
		switch request.URL.Path {
		case earnFlexibleSubscriptionPath:
			_, _ = response.Write(earnResultJSON(earnFlexibleSubscriptionFixture(26055, "SUCCESS")))
		case earnLockedSubscriptionPath:
			_, _ = response.Write(earnResultJSON(earnLockedSubscriptionFixture(26057, "PURCHASING")))
		case earnFlexibleRedemptionPath:
			_, _ = response.Write(earnResultJSON(earnFlexibleRedemptionFixture(1121343, "SUCCESS")))
		case earnLockedRedemptionPath:
			_, _ = response.Write(earnResultJSON(earnLockedRedemptionFixture(1121344, "PAID")))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	earn := newTestEarnAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}

	wantObjects := []struct {
		objectID   string
		objectType string
		status     string
		amount     string
		asset      string
	}{
		{"earn-sub:flexible:26055", "earn_subscription", "SUCCESS", "100.00000000", "USDT"},
		{"earn-sub:locked:26057", "earn_subscription", "PURCHASING", "21.05000000", "BNB"},
		{"earn-redeem:flexible:1121343", "earn_redemption", "SUCCESS", "10.54000000", "USDT"},
		{"earn-redeem:locked:1121344", "earn_redemption", "PAID", "21.05000000", "BNB"},
	}

	var fingerprints []string
	for phase := 0; phase < len(wantObjects); phase++ {
		page, err := earn.FetchEventPage(t.Context(), request)
		if err != nil {
			t.Fatalf("phase %d: %v", phase, err)
		}
		if len(page.Observations) != 1 {
			t.Fatalf("phase %d observations = %d", phase, len(page.Observations))
		}
		observation := page.Observations[0]
		want := wantObjects[phase]
		if observation.ObjectID != want.objectID || observation.ObjectType != want.objectType {
			t.Fatalf("phase %d identity = %#v, want %+v", phase, observation, want)
		}
		if observation.Status != want.status || observation.Amount != want.amount || observation.Asset != want.asset {
			t.Fatalf("phase %d state = %#v, want %+v", phase, observation, want)
		}
		if observation.Stream != "earn" || observation.Exchange != model.ExchangeBinance {
			t.Fatalf("phase %d stream = %#v", phase, observation)
		}
		if observation.StateFingerprint == "" {
			t.Fatalf("phase %d has no fingerprint", phase)
		}
		if !observation.OccurredAt.Equal(time.UnixMilli(earnRowTime)) || !observation.ObservedAt.Equal(fixedNow) {
			t.Fatalf("phase %d times = %s / %s", phase, observation.OccurredAt, observation.ObservedAt)
		}
		if !json.Valid(observation.RawJSON) {
			t.Fatalf("phase %d raw json = %s", phase, observation.RawJSON)
		}
		fingerprints = append(fingerprints, observation.StateFingerprint)

		isLast := phase == len(wantObjects)-1
		if page.Done != isLast {
			t.Fatalf("phase %d done = %t", phase, page.Done)
		}
		if isLast {
			if page.NextCursor != "" {
				t.Fatalf("final page has cursor %q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" {
			t.Fatalf("phase %d has no next cursor", phase)
		}
		request.Cursor = page.NextCursor
	}

	// Fingerprint keys on status only: SUCCESS phases must share, others differ.
	if fingerprints[0] != fingerprints[2] {
		t.Fatal("equal SUCCESS statuses produced different fingerprints")
	}
	if fingerprints[0] == fingerprints[1] {
		t.Fatal("SUCCESS and PURCHASING share a fingerprint")
	}
}

func TestEarnFetchEventPagePaginatesWithinPhase(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	var flexRequests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case earnFlexibleSubscriptionPath:
			flexRequests++
			current := request.URL.Query().Get("current")
			if request.URL.Query().Get("size") != strconv.Itoa(earnPageSize) {
				t.Errorf("size = %q, want %d", request.URL.Query().Get("size"), earnPageSize)
			}
			if flexRequests == 1 {
				if current != "1" {
					t.Errorf("first current = %q, want 1", current)
				}
				rows := make([]map[string]any, earnPageSize)
				for index := range rows {
					rows[index] = earnFlexibleSubscriptionFixture(index+1, "SUCCESS")
				}
				_, _ = response.Write(earnResultJSON(rows...))
				return
			}
			if current != "2" {
				t.Errorf("second current = %q, want 2", current)
			}
			_, _ = response.Write(earnResultJSON(earnFlexibleSubscriptionFixture(9001, "SUCCESS")))
		default:
			_, _ = response.Write(earnResultJSON())
		}
	}))
	t.Cleanup(server.Close)

	earn := newTestEarnAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}

	first, err := earn.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor == "" || len(first.Observations) != earnPageSize {
		t.Fatalf("first = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	cursor, err := decodeEarnCursor(first.NextCursor)
	if err != nil {
		t.Fatalf("decode next cursor: %v", err)
	}
	if cursor.Phase != "flex-sub" || cursor.Page != 2 {
		t.Fatalf("next cursor = %+v, want flex-sub page 2", cursor)
	}

	request.Cursor = first.NextCursor
	second, err := earn.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Observations) != 1 || second.Observations[0].ObjectID != "earn-sub:flexible:9001" {
		t.Fatalf("second = %#v", second.Observations)
	}
	// The short page 2 ends flex-sub and advances to the next phase.
	if second.Done || second.NextCursor == "" {
		t.Fatalf("second page = done %t cursor %q", second.Done, second.NextCursor)
	}
	advanced, err := decodeEarnCursor(second.NextCursor)
	if err != nil {
		t.Fatalf("decode advanced cursor: %v", err)
	}
	if advanced.Phase != "locked-sub" || advanced.Page != 1 {
		t.Fatalf("advanced cursor = %+v, want locked-sub page 1", advanced)
	}
}

func TestEarnTerminalOrdersKeepOnlySettledRecords(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case earnFlexibleSubscriptionPath:
			assertSpotSignature(t, request, "fake-api-secret")
			_, _ = response.Write(earnResultJSON(
				earnFlexibleSubscriptionFixture(700, "SUCCESS"),
				earnFlexibleSubscriptionFixture(701, "PURCHASING"),
			))
		default:
			_, _ = response.Write(earnResultJSON())
		}
	}))
	t.Cleanup(server.Close)

	earn := newTestEarnAdapter(t, server, fixedNow)
	terminal := earn.TerminalOrders()
	page, err := terminal.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch terminal earn: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("entries = %d, want 1 (PURCHASING dropped)", len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.EntryID != "earn-sub:flexible:700" || entry.Category != "earn" || entry.Type != "subscription" {
		t.Fatalf("identity = %#v", entry)
	}
	if entry.Side != "SUBSCRIBE" || entry.Info != "SUCCESS" || entry.OrderID != "700" {
		t.Fatalf("classification = %#v", entry)
	}
	if entry.Amount != "100.00000000" || entry.CashFlow != "100.00000000" || entry.Asset != "USDT" {
		t.Fatalf("amounts = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(earnRowTime)) || !entry.ObservedAt.Equal(fixedNow) {
		t.Fatalf("times = %s / %s", entry.OccurredAt, entry.ObservedAt)
	}
	if !json.Valid(entry.RawJSON) || !strings.Contains(string(entry.RawJSON), `"purchaseId":700`) {
		t.Fatalf("raw json = %s", entry.RawJSON)
	}
}

func TestEarnTerminalRedemptionEntryIdentity(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == earnLockedRedemptionPath {
			_, _ = response.Write(earnResultJSON(earnLockedRedemptionFixture(555, "PAID")))
			return
		}
		_, _ = response.Write(earnResultJSON())
	}))
	t.Cleanup(server.Close)

	earn := newTestEarnAdapter(t, server, fixedNow)
	page, err := earn.TerminalOrders().FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch terminal redemption: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 {
		t.Fatalf("page = done %t entries %d", page.Done, len(page.Entries))
	}
	entry := page.Entries[0]
	if entry.EntryID != "earn-redeem:locked:555" || entry.Type != "redemption" || entry.Side != "REDEEM" {
		t.Fatalf("entry = %#v", entry)
	}
}

func TestEarnWindowBounds(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("window validation must not reach the network")
	}))
	t.Cleanup(server.Close)
	earn := newTestEarnAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())

	base := time.UnixMilli(1_700_000_000_000).UTC()
	cases := map[string]source.PageRequest{
		"missing times": {Account: source.Account{ID: "primary"}},
		"end before start": {
			Account: source.Account{ID: "primary"}, Start: base, End: base.Add(-time.Hour),
		},
		"over 30 days": {
			Account: source.Account{ID: "primary"}, Start: base.Add(-earnMaxWindow - time.Hour), End: base,
		},
	}
	for name, request := range cases {
		request := request
		t.Run(name, func(t *testing.T) {
			if _, err := earn.FetchEventPage(t.Context(), request); err == nil {
				t.Fatal("FetchEventPage accepted an invalid window")
			}
			if _, err := earn.TerminalOrders().FetchPage(t.Context(), request); err == nil {
				t.Fatal("FetchPage accepted an invalid window")
			}
		})
	}
}

func TestEarnErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		secret := request.URL.Query().Get("signature")
		response.WriteHeader(http.StatusBadRequest)
		// Echo the request signature back inside the message to prove redaction.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"rejected signature ` + secret + ` for fake-api-secret"}`))
	}))
	t.Cleanup(server.Close)

	earn := newTestEarnAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := earn.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-secret") {
		t.Fatalf("error leaked secret: %q", apiError.Message)
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("error was not redacted: %q", apiError.Message)
	}
}

func TestEarnRejectsInvalidCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("cursor validation must not reach the network")
	}))
	t.Cleanup(server.Close)
	earn := newTestEarnAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	request := source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_999_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
		Cursor: "not-a-cursor!!",
	}
	if _, err := earn.FetchEventPage(t.Context(), request); err == nil {
		t.Fatal("accepted a malformed cursor")
	}
}

func newTestEarnAdapter(t *testing.T, server *httptest.Server, now time.Time) *EarnAdapter {
	t.Helper()
	adapter, err := NewEarn(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}
	return adapter
}

func assertEarnSignedWindow(t *testing.T, request *http.Request, start, end time.Time) {
	t.Helper()
	assertSpotSignature(t, request, "fake-api-secret")
	query := request.URL.Query()
	if got, want := query.Get("startTime"), strconv.FormatInt(start.UnixMilli(), 10); got != want {
		t.Errorf("startTime = %q, want %q", got, want)
	}
	if got, want := query.Get("endTime"), strconv.FormatInt(end.UnixMilli(), 10); got != want {
		t.Errorf("endTime = %q, want %q", got, want)
	}
	if got := query.Get("recvWindow"); got != "5000" {
		t.Errorf("recvWindow = %q, want 5000", got)
	}
}

func earnResultJSON(rows ...map[string]any) []byte {
	if rows == nil {
		rows = []map[string]any{}
	}
	data, _ := json.Marshal(map[string]any{"rows": rows, "total": len(rows)})
	return data
}

func earnFlexibleSubscriptionFixture(purchaseID int, status string) map[string]any {
	return map[string]any{
		"amount": "100.00000000", "asset": "USDT", "time": earnRowTime,
		"purchaseId": purchaseID, "type": "AUTO", "sourceAccount": "SPOT", "status": status,
	}
}

func earnLockedSubscriptionFixture(purchaseID int, status string) map[string]any {
	return map[string]any{
		"positionId": "3001", "purchaseId": purchaseID, "time": earnRowTime,
		"asset": "BNB", "amount": "21.05000000", "lockPeriod": "30", "type": "NORMAL", "status": status,
	}
}

func earnFlexibleRedemptionFixture(redeemID int, status string) map[string]any {
	return map[string]any{
		"amount": "10.54000000", "asset": "USDT", "time": earnRowTime,
		"projectId": "USDT001", "redeemId": redeemID, "destAccount": "SPOT", "status": status,
	}
}

func earnLockedRedemptionFixture(redeemID int, status string) map[string]any {
	return map[string]any{
		"positionId": "3001", "redeemId": redeemID, "time": earnRowTime, "asset": "BNB",
		"lockPeriod": "30", "amount": "21.05000000", "type": "MATURE", "deliverDate": "1699999900000", "status": status,
	}
}
