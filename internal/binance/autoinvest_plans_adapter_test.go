package binance

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func autoInvestPlanFixture(planID int, planType, status, targetAsset string) map[string]any {
	return map[string]any{
		"planId": planID, "planType": planType, "status": status,
		"targetAsset": targetAsset, "sourceAsset": "USDT",
		"totalInvestedInUSD": "1000.00000000", "planValueInUSD": "1050.00000000",
		"pnlInUSD": "50.00000000", "roi": "0.05000000",
		"nextExecutionDateTime": int64(1_699_999_999_000), "lastUpdatedDateTime": int64(1_699_999_000_000),
	}
}

func newTestAutoInvestPlansAdapter(t *testing.T, server *httptest.Server, now time.Time) *AutoInvestPlansAdapter {
	t.Helper()
	adapter, err := NewAutoInvestPlans(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new auto-invest plans adapter: %v", err)
	}
	return adapter
}

func TestAutoInvestPlansWalksPlanTypesAndNormalizes(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	var seenPlanTypes []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != autoInvestPlanListPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			http.NotFound(response, request)
			return
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
			t.Errorf("API key header = %q", got)
		}
		assertSpotSignature(t, request, "fake-api-secret")
		planType := request.URL.Query().Get("planType")
		seenPlanTypes = append(seenPlanTypes, planType)
		switch planType {
		case "SINGLE":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"plan": []map[string]any{autoInvestPlanFixture(100001, "SINGLE", "ONGOING", "BTC")},
			})
		case "PORTFOLIO":
			_ = json.NewEncoder(response).Encode(map[string]any{"plan": []map[string]any{}})
		case "INDEX":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"plan": []map[string]any{autoInvestPlanFixture(100002, "INDEX", "PAUSED", "ETH")},
			})
		default:
			t.Errorf("unexpected planType %q", planType)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestPlansAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}}

	first, err := adapter.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch first page: %v", err)
	}
	if first.Done || first.NextCursor == "" || len(first.Observations) != 1 {
		t.Fatalf("first page = done %t cursor %q observations %d", first.Done, first.NextCursor, len(first.Observations))
	}
	single := first.Observations[0]
	if single.Exchange != model.ExchangeBinance || single.Stream != "autoinvest" {
		t.Fatalf("single stream = %#v", single)
	}
	if single.ObjectType != "autoinvest-plan" || single.ObjectID != "autoinvest-plan:100001" || single.Status != "ONGOING" {
		t.Fatalf("single identity = %#v", single)
	}
	if single.Asset != "BTC" || single.Amount != "1000.00000000" || single.StateFingerprint == "" {
		t.Fatalf("single state = %#v", single)
	}
	if !single.OccurredAt.Equal(time.UnixMilli(1_699_999_000_000)) || !single.ObservedAt.Equal(fixedNow) {
		t.Fatalf("single times = %s / %s", single.OccurredAt, single.ObservedAt)
	}
	if !json.Valid(single.RawJSON) || !strings.Contains(string(single.RawJSON), `"planId":100001`) {
		t.Fatalf("single raw JSON = %s", single.RawJSON)
	}

	// The second page must resume from the cursor, skip the empty PORTFOLIO
	// plan type internally, and terminate on INDEX.
	request.Cursor = first.NextCursor
	second, err := adapter.FetchEventPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch second page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Observations) != 1 {
		t.Fatalf("second page = done %t cursor %q observations %d", second.Done, second.NextCursor, len(second.Observations))
	}
	index := second.Observations[0]
	if index.ObjectID != "autoinvest-plan:100002" || index.Status != "PAUSED" || index.Asset != "ETH" {
		t.Fatalf("index observation = %#v", index)
	}
	if index.StateFingerprint == single.StateFingerprint {
		t.Fatal("distinct plans share a fingerprint")
	}

	if strings.Join(seenPlanTypes, ",") != "SINGLE,PORTFOLIO,INDEX" {
		t.Fatalf("planType walk = %v", seenPlanTypes)
	}
}

func TestAutoInvestPlansAllEmptyIsDone(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != autoInvestPlanListPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			return
		}
		_, _ = response.Write([]byte(`{"plan":[]}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestPlansAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	page, err := adapter.FetchEventPage(t.Context(), source.PageRequest{Account: source.Account{ID: "primary"}})
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 0 {
		t.Fatalf("page = done %t cursor %q observations %d", page.Done, page.NextCursor, len(page.Observations))
	}
}

func TestAutoInvestPlansRejectsInvalidCursor(t *testing.T) {
	t.Parallel()

	for _, cursor := range []string{"not base64!!", encodeBogusPlanTypeCursor(t), " leading-space"} {
		cursor := cursor
		adapter := newTestAutoInvestPlansAdapter(t, httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			t.Error("adapter issued a request for an invalid cursor")
			response.WriteHeader(http.StatusInternalServerError)
		})), time.UnixMilli(1_700_000_000_000).UTC())
		_, err := adapter.FetchEventPage(t.Context(), source.PageRequest{
			Account: source.Account{ID: "primary"}, Cursor: cursor,
		})
		if err == nil {
			t.Fatalf("cursor %q accepted", cursor)
		}
	}
}

func encodeBogusPlanTypeCursor(t *testing.T) string {
	t.Helper()
	data, err := json.Marshal(autoInvestPlansCursor{PlanType: "MARGIN"})
	if err != nil {
		t.Fatal(err)
	}
	// Mirror the adapter encoding so only the plan type is unknown.
	return base64.RawURLEncoding.EncodeToString(data)
}

func TestAutoInvestPlansRedactsSecretInErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != autoInvestPlanListPath {
			t.Errorf("unexpected path %q", request.URL.Path)
			return
		}
		response.WriteHeader(http.StatusBadRequest)
		// Echo the signed secret back inside the provider message to prove
		// the adapter strips it before surfacing the error.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"signature invalid for secret fake-api-secret and key fake-api-key"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestPlansAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchEventPage(t.Context(), source.PageRequest{Account: source.Account{ID: "primary"}})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Message, "fake-api-secret") || strings.Contains(apiError.Message, "fake-api-key") {
		t.Fatalf("error message leaked a credential: %q", apiError.Message)
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("error message was not redacted: %q", apiError.Message)
	}
}

func TestAutoInvestPlansRequiresCredentials(t *testing.T) {
	t.Parallel()

	adapter, err := NewAutoInvestPlans(Options{
		BaseURL: "https://api.binance.com", LookupEnv: mapLookup(map[string]string{}),
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if _, err := adapter.FetchEventPage(t.Context(), source.PageRequest{Account: source.Account{ID: "primary"}}); err == nil {
		t.Fatal("missing credentials accepted")
	}
}
