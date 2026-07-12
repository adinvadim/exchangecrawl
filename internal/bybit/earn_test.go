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
)

func TestEarnOrdersObservePhasesAndCursors(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-earn-key"
		secret = "fake-earn-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var flexCalls, fixedCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		switch r.URL.Path {
		case earnFlexibleOrderPath:
			flexCalls++
			if flexCalls == 1 {
				wantQuery := url.Values{
					"startTime": {"1700000000000"}, "endTime": {"1700000060000"}, "limit": {"50"},
				}.Encode()
				if r.URL.RawQuery != wantQuery {
					t.Errorf("flexible query = %q, want %q", r.URL.RawQuery, wantQuery)
				}
				_, _ = fmt.Fprint(w, earnResponse("flex-next", `{
                  "category":"FlexibleSaving","coin":"USDT","orderId":"fake-flex-1",
                  "orderType":"Stake","orderValue":"5.000000000000000001",
                  "status":"Processing","createdAt":"1700000030","updatedAt":"1700000030",
                  "futureField":"flex"
                }`))
				return
			}
			if got := r.URL.Query().Get("cursor"); got != "flex-next" {
				t.Errorf("flexible cursor = %q, want flex-next", got)
			}
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"OnChain","coin":"ETH","orderId":"fake-flex-2",
              "orderType":"Redeem","orderValue":"2","status":"Success",
              "updatedAt":"1700000035"
            }`))
		case earnFixedTermOrderPath:
			fixedCalls++
			if got := r.URL.Query().Get("cursor"); got != "" {
				t.Errorf("fixed-term cursor = %q, want empty", got)
			}
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"FixedTermSaving","coin":"BTC","orderId":"fake-fixed-1",
              "orderType":"Redeem","amount":"1.000000000000000002","status":"Success",
              "updatedAt":"1700000040"
            }`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	earn, err := NewEarn(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}

	request := spotPageRequest("")
	first, err := earn.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first event page: %v", err)
	}
	if first.Done || first.NextCursor != "flexible:flex-next" || len(first.Observations) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	obs := first.Observations[0]
	if obs.Stream != "earn" || obs.ObjectType != "earn_order" || obs.ObjectID != "earn-order:FlexibleSaving:fake-flex-1" {
		t.Fatalf("observation identity = %#v", obs)
	}
	if obs.Status != "Processing" || obs.Asset != "USDT" || obs.Amount != "5.000000000000000001" || obs.StateFingerprint == "" {
		t.Fatalf("observation normalization = %#v", obs)
	}
	if !obs.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) || !obs.ObservedAt.Equal(requestTime) {
		t.Fatalf("observation timestamps = %#v", obs)
	}
	if !containsJSONField(obs.RawJSON, "futureField", "flex") {
		t.Fatalf("observation raw JSON = %s", obs.RawJSON)
	}

	request.Cursor = first.NextCursor
	second, err := earn.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second event page: %v", err)
	}
	if second.Done || second.NextCursor != "fixedterm:" || len(second.Observations) != 1 {
		t.Fatalf("second page = %#v", second)
	}
	if second.Observations[0].ObjectID != "earn-order:OnChain:fake-flex-2" {
		t.Fatalf("second observation = %#v", second.Observations[0])
	}

	request.Cursor = second.NextCursor
	third, err := earn.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("third event page: %v", err)
	}
	if !third.Done || third.NextCursor != "" || len(third.Observations) != 1 {
		t.Fatalf("third page = %#v", third)
	}
	if third.Observations[0].ObjectID != "earn-order:FixedTermSaving:fake-fixed-1" {
		t.Fatalf("third observation = %#v", third.Observations[0])
	}
	if flexCalls != 2 || fixedCalls != 1 {
		t.Fatalf("calls flex=%d fixed=%d", flexCalls, fixedCalls)
	}
}

func TestEarnTerminalOrdersAndFixedTermYields(t *testing.T) {
	t.Parallel()

	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case earnFlexibleOrderPath:
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"FlexibleSaving","coin":"USDT","orderId":"fake-flex-pending",
              "orderType":"Stake","orderValue":"9","status":"Processing","updatedAt":"1700000030"
            }`, `{
              "category":"FlexibleSaving","coin":"USDT","orderId":"fake-flex-success",
              "orderType":"Stake","orderValue":"5.000000000000000001","status":"Success",
              "updatedAt":"1700000031","futureField":"flex-terminal"
            }`))
		case earnFixedTermOrderPath:
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"FixedTermSaving","coin":"BTC","orderId":"fake-fixed-redeem",
              "orderType":"Redeem","amount":"1.000000000000000002","status":"Success",
              "updatedAt":"1700000040",
              "yieldInfoList":[
                {"coin":"BTC","amount":"0.000000000000000010","createdAt":"1700000041","futureField":"yield"},
                {"coin":"BTC","amount":"0.000000000000000020","createdAt":"1700000042"}
              ]
            }`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	earn, err := NewEarn(spotTestOptions(server, requestTime, "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}

	first, err := earn.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("terminal flexible page: %v", err)
	}
	if first.Done || first.NextCursor != "fixedterm:" || len(first.Entries) != 1 {
		t.Fatalf("first page = %#v", first)
	}
	order := first.Entries[0]
	if order.EntryID != "earn-order:FlexibleSaving:fake-flex-success" || order.Category != "earn" || order.Type != "Success" {
		t.Fatalf("settled order identity = %#v", order)
	}
	if order.Side != "Stake" || order.Asset != "USDT" || order.Amount != "5.000000000000000001" || order.OrderID != "fake-flex-success" {
		t.Fatalf("settled order fields = %#v", order)
	}
	if !order.OccurredAt.Equal(time.UnixMilli(1_700_000_031_000).UTC()) || !containsJSONField(order.RawJSON, "futureField", "flex-terminal") {
		t.Fatalf("settled order raw/time = %#v", order)
	}

	request := spotPageRequest("")
	request.Cursor = first.NextCursor
	second, err := earn.FetchPage(context.Background(), request)
	if err != nil {
		t.Fatalf("terminal fixed-term page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Entries) != 3 {
		t.Fatalf("second page = %#v", second)
	}
	redeem := second.Entries[0]
	if redeem.EntryID != "earn-order:FixedTermSaving:fake-fixed-redeem" || redeem.Amount != "1.000000000000000002" {
		t.Fatalf("redeem order = %#v", redeem)
	}
	yieldOne, yieldTwo := second.Entries[1], second.Entries[2]
	if yieldOne.EntryID != "earn-order-yield:fake-fixed-redeem:1700000041:0.000000000000000010" {
		t.Fatalf("yield one identity = %#v", yieldOne)
	}
	if yieldOne.Type != "Yield" || yieldOne.Asset != "BTC" || yieldOne.Amount != "0.000000000000000010" || yieldOne.OrderID != "fake-fixed-redeem" {
		t.Fatalf("yield one fields = %#v", yieldOne)
	}
	if !yieldOne.OccurredAt.Equal(time.UnixMilli(1_700_000_041_000).UTC()) || !containsJSONField(yieldOne.RawJSON, "futureField", "yield") {
		t.Fatalf("yield one raw/time = %#v", yieldOne)
	}
	if yieldTwo.EntryID != "earn-order-yield:fake-fixed-redeem:1700000042:0.000000000000000020" {
		t.Fatalf("yield two identity = %#v", yieldTwo)
	}
}

func TestEarnTerminalDrainsNonSettledPhases(t *testing.T) {
	t.Parallel()

	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case earnFlexibleOrderPath:
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"FlexibleSaving","coin":"USDT","orderId":"fake-flex-pending",
              "orderType":"Stake","orderValue":"9","status":"Pending","updatedAt":"1700000030"
            }`))
		case earnFixedTermOrderPath:
			_, _ = fmt.Fprint(w, earnResponse("", `{
              "category":"FixedTermSaving","coin":"BTC","orderId":"fake-fixed-fail",
              "orderType":"Stake","amount":"1","status":"Failed","updatedAt":"1700000040"
            }`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	earn, err := NewEarn(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}
	page, err := earn.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("terminal page: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 || page.Entries[0].EntryID != "earn-order:FixedTermSaving:fake-fixed-fail" {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != earnFlexibleOrderPath+","+earnFixedTermOrderPath {
		t.Fatalf("paths = %v", paths)
	}
}

func TestEarnOrdersRejectInvalidCursorAndWindow(t *testing.T) {
	t.Parallel()

	earn, err := NewEarn(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}

	_, err = earn.FetchEventPage(context.Background(), spotPageRequest("provider-cursor-without-prefix"))
	if err == nil || !strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("cursor error = %v", err)
	}

	wide := spotPageRequest("")
	wide.End = wide.Start.Add(8 * 24 * time.Hour)
	_, err = earn.FetchPage(context.Background(), wide)
	if err == nil || !strings.Contains(err.Error(), "seven days") {
		t.Fatalf("window error = %v", err)
	}
}

func TestEarnEndpointErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-earn-key"
		secret = "fake-earn-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"earn permission denied for %s using %s","result":{}}`, apiKey, secret)
	}))
	t.Cleanup(server.Close)

	earn, err := NewEarn(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), apiKey, secret))
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}
	_, err = earn.FetchEventPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "Bybit API error 10003") {
		t.Fatalf("error = %v, want API error", err)
	}
	if strings.Contains(err.Error(), apiKey) || strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error did not redact: %v", err)
	}
}

func TestEarnMalformedResultIsReported(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"retCode":0,"result":"invalid"}`)
	}))
	t.Cleanup(server.Close)

	earn, err := NewEarn(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new earn adapter: %v", err)
	}
	_, err = earn.FetchPage(context.Background(), spotPageRequest(""))
	if err == nil || !strings.Contains(err.Error(), "decode Bybit earn order result") {
		t.Fatalf("error = %v", err)
	}
}

func TestEarnTerminalOrdersClampToCreationWindow(t *testing.T) {
	t.Parallel()

	start := time.UnixMilli(1_700_000_000_000).UTC()
	end := time.UnixMilli(1_700_000_060_000).UTC()
	tests := []struct {
		name       string
		createdAt  string
		updatedAt  string
		archived   bool
		occurredAt time.Time
	}{
		{
			name:       "settled after window end but created inside window",
			createdAt:  "1700000030",
			updatedAt:  "1700000090",
			archived:   true,
			occurredAt: time.UnixMilli(1_700_000_030_000).UTC(),
		},
		{
			name:      "created before window start is dropped",
			createdAt: "1699999000",
			updatedAt: "1700000030",
			archived:  false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			order := fmt.Sprintf(`{"category":"FlexibleSaving","coin":"USDT",
              "orderId":"fake-flex-success","orderType":"Stake","orderValue":"5",
              "status":"Success","createdAt":%q,"updatedAt":%q}`, test.createdAt, test.updatedAt)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == earnFlexibleOrderPath {
					_, _ = fmt.Fprint(w, earnResponse("", order))
					return
				}
				_, _ = fmt.Fprint(w, earnResponse(""))
			}))
			t.Cleanup(server.Close)
			earn, err := NewEarn(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
			if err != nil {
				t.Fatalf("new earn adapter: %v", err)
			}
			page, err := earn.FetchPage(context.Background(), spotPageRequest(""))
			if err != nil {
				t.Fatalf("fetch terminal orders: %v", err)
			}
			assertEntriesWithinWindow(t, page.Entries, start, end)
			if test.archived {
				if len(page.Entries) != 1 {
					t.Fatalf("want 1 archived entry, got %#v", page.Entries)
				}
				if !page.Entries[0].OccurredAt.Equal(test.occurredAt) {
					t.Fatalf("OccurredAt = %s, want %s", page.Entries[0].OccurredAt, test.occurredAt)
				}
			} else if len(page.Entries) != 0 {
				t.Fatalf("want entry dropped, got %#v", page.Entries)
			}
		})
	}
}

func earnResponse(nextCursor string, orders ...string) string {
	list := "[" + strings.Join(orders, ",") + "]"
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":%s}}`, nextCursor, list)
}
