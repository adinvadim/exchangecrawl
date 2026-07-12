package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestDepositsFetchEventPageSignsRequestAndNormalizes(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-30 * 24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != depositHisrecPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			return
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
			t.Errorf("API key header = %q", got)
		}
		assertSpotSignature(t, request, "fake-api-secret")
		query := request.URL.Query()
		for key, want := range map[string]string{
			"startTime": strconv.FormatInt(start.UnixMilli(), 10),
			"endTime":   strconv.FormatInt(fixedNow.UnixMilli(), 10),
			"offset":    "0", "limit": "1000", "recvWindow": "5000",
			"timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
		} {
			if got := query.Get(key); got != want {
				t.Errorf("query %s = %q, want %q", key, got, want)
			}
		}
		_, _ = response.Write([]byte(`[{"id":"769800519366885376","amount":"0.00100000","coin":"BTC","network":"BTC","status":1,"address":"fake-address","addressTag":"","txId":"fake-tx","insertTime":1699999999000,"transferType":0,"confirmTimes":"1/1","unlockConfirm":0,"walletType":0,"completeTime":"1699999999500","travelRuleStatus":0}]`))
	}))
	t.Cleanup(server.Close)

	deposits := newTestDepositsAdapter(t, server, fixedNow)
	page, err := deposits.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow,
	})
	if err != nil {
		t.Fatalf("fetch deposits: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 1 {
		t.Fatalf("page = done %t cursor %q observations %d", page.Done, page.NextCursor, len(page.Observations))
	}
	observation := page.Observations[0]
	if observation.Exchange != model.ExchangeBinance || observation.Stream != "asset" {
		t.Fatalf("stream identity = %#v", observation)
	}
	if observation.ObjectType != "deposit" || observation.ObjectID != "deposit:769800519366885376" || observation.Status != "1" {
		t.Fatalf("object identity = %#v", observation)
	}
	if observation.Asset != "BTC" || observation.Amount != "0.00100000" || observation.StateFingerprint == "" {
		t.Fatalf("state = %#v", observation)
	}
	if !observation.OccurredAt.Equal(time.UnixMilli(1_699_999_999_000)) || !observation.ObservedAt.Equal(fixedNow) {
		t.Fatalf("times = %s / %s", observation.OccurredAt, observation.ObservedAt)
	}
	if !json.Valid(observation.RawJSON) || !strings.Contains(string(observation.RawJSON), `"transferType":0`) {
		t.Fatalf("raw JSON must retain transferType: %s", observation.RawJSON)
	}
}

func TestDepositsFingerprintTracksMutableProgress(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	pending := depositRow{ID: "1", InsertTime: 1699999999000, Status: "0", ConfirmTimes: "0/1"}
	credited := depositRow{ID: "1", InsertTime: 1699999999000, Status: "1", ConfirmTimes: "1/1", CompleteTime: "1699999999500"}
	account := source.Account{ID: "primary"}
	first, err := normalizeDeposit(pending, account, fixedNow)
	if err != nil {
		t.Fatalf("normalize pending: %v", err)
	}
	second, err := normalizeDeposit(credited, account, fixedNow)
	if err != nil {
		t.Fatalf("normalize credited: %v", err)
	}
	if first.ObjectID != second.ObjectID {
		t.Fatalf("object id changed across states: %q != %q", first.ObjectID, second.ObjectID)
	}
	if first.StateFingerprint == second.StateFingerprint {
		t.Fatal("mutable deposit states share a fingerprint")
	}
}

func TestDepositsPaginationAdvancesByOffset(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 1 {
			if got := request.URL.Query().Get("offset"); got != "0" {
				t.Errorf("first offset = %q, want 0", got)
			}
			rows := make([]map[string]any, depositPageLimit)
			for index := range rows {
				rows[index] = depositFixture(index + 1)
			}
			_ = json.NewEncoder(response).Encode(rows)
			return
		}
		if got := request.URL.Query().Get("offset"); got != strconv.Itoa(depositPageLimit) {
			t.Errorf("second offset = %q, want %d", got, depositPageLimit)
		}
		_ = json.NewEncoder(response).Encode([]map[string]any{depositFixture(depositPageLimit + 1)})
	}))
	t.Cleanup(server.Close)

	deposits := newTestDepositsAdapter(t, server, fixedNow)
	request := source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-30 * 24 * time.Hour), End: fixedNow,
	}
	first, err := deposits.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.NextCursor != strconv.Itoa(depositPageLimit) || len(first.Observations) != depositPageLimit {
		t.Fatalf("first = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	request.Cursor = first.NextCursor
	second, err := deposits.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Observations) != 1 {
		t.Fatalf("second = done %t cursor %q observations %d", second.Done, second.NextCursor, len(second.Observations))
	}
	if second.Observations[0].ObjectID != "deposit:"+strconv.Itoa(depositPageLimit+1) {
		t.Fatalf("second object id = %q", second.Observations[0].ObjectID)
	}
}

func TestDepositsFiltersOutsideWindow(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := time.UnixMilli(1_699_990_000_000).UTC()
	end := time.UnixMilli(1_699_999_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`[
			{"id":"before","amount":"1","coin":"BTC","status":1,"insertTime":1699980000000,"confirmTimes":"1/1"},
			{"id":"inside","amount":"1","coin":"BTC","status":1,"insertTime":1699995000000,"confirmTimes":"1/1"},
			{"id":"after","amount":"1","coin":"BTC","status":1,"insertTime":1699999999000,"confirmTimes":"1/1"}
		]`))
	}))
	t.Cleanup(server.Close)

	deposits := newTestDepositsAdapter(t, server, fixedNow)
	page, err := deposits.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: start, End: end,
	})
	if err != nil {
		t.Fatalf("fetch deposits: %v", err)
	}
	if !page.Done || len(page.Observations) != 1 || page.Observations[0].ObjectID != "deposit:inside" {
		t.Fatalf("window filter = %#v", page.Observations)
	}
}

func TestDepositsRejectsWindowLongerThanNinetyDays(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("adapter must reject oversized window before calling the API")
	}))
	t.Cleanup(server.Close)

	deposits := newTestDepositsAdapter(t, server, fixedNow)
	_, err := deposits.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-91 * 24 * time.Hour), End: fixedNow,
	})
	if err == nil || !strings.Contains(err.Error(), "90 days") {
		t.Fatalf("error = %v, want 90-day window rejection", err)
	}
}

func TestDepositsRejectsInvalidCursor(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("adapter must reject an invalid cursor before calling the API")
	}))
	t.Cleanup(server.Close)

	deposits := newTestDepositsAdapter(t, server, fixedNow)
	for _, cursor := range []string{" 1", "-1", "abc"} {
		_, err := deposits.FetchEventPage(t.Context(), source.PageRequest{
			Account: source.Account{ID: "primary"},
			Start:   fixedNow.Add(-time.Hour), End: fixedNow, Cursor: cursor,
		})
		if err == nil {
			t.Fatalf("cursor %q was accepted", cursor)
		}
	}
}

func TestDepositsReturnsRedactedStructuredAPIError(t *testing.T) {
	t.Parallel()

	signatureChannel := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		echoedSignature := request.URL.Query().Get("signature")
		signatureChannel <- echoedSignature
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{
			"code": -1022,
			"msg":  fmt.Sprintf("invalid request %s fake-api-key fake-api-secret %s", request.URL.String(), echoedSignature),
		})
	}))
	t.Cleanup(server.Close)

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	deposits := newTestDepositsAdapter(t, server, fixedNow)
	_, err := deposits.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary", Label: "Primary"},
		Start:   fixedNow.Add(-time.Hour), End: fixedNow,
	})
	if err == nil {
		t.Fatal("fetch deposits succeeded, want API error")
	}
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	echoedSignature := <-signatureChannel
	for _, secret := range []string{"fake-api-key", "fake-api-secret", echoedSignature, "signature="} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

func TestDepositsRejectsMissingCredentials(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("adapter must not call the API without credentials")
	}))
	t.Cleanup(server.Close)

	deposits, err := NewDeposits(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return fixedNow },
		LookupEnv: mapLookup(map[string]string{defaultAPIKeyEnv: "fake-api-key"}),
	})
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}
	_, err = deposits.FetchEventPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
	})
	if err == nil || !strings.Contains(err.Error(), defaultAPISecretEnv) {
		t.Fatalf("error = %v, want missing secret env", err)
	}
}

func newTestDepositsAdapter(t *testing.T, server *httptest.Server, now time.Time) *DepositsAdapter {
	t.Helper()
	adapter, err := NewDeposits(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}
	return adapter
}

func depositFixture(id int) map[string]any {
	return map[string]any{
		"id": strconv.Itoa(id), "amount": "0.00100000", "coin": "BTC", "network": "BTC",
		"status": 1, "address": "fake-address", "txId": "fake-tx-" + strconv.Itoa(id),
		"insertTime": int64(1_699_999_000_000), "transferType": 0, "confirmTimes": "1/1",
		"completeTime": "1699999500000", "travelRuleStatus": 0,
	}
}
