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

func TestAutoInvestFetchPageNormalizesAndCrossesPhases(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case autoInvestHistoryPath:
			assertSpotSignature(t, request, "fake-api-secret")
			if got := request.Header.Get("X-MBX-APIKEY"); got != "fake-api-key" {
				t.Errorf("API key header = %q", got)
			}
			query := request.URL.Query()
			for key, want := range map[string]string{
				"startTime": strconv.FormatInt(start.UnixMilli(), 10),
				"endTime":   strconv.FormatInt(fixedNow.UnixMilli(), 10),
				"current":   "1", "size": "100", "recvWindow": "5000",
				"timestamp": strconv.FormatInt(fixedNow.UnixMilli(), 10),
			} {
				if got := query.Get(key); got != want {
					t.Errorf("history %s = %q, want %q", key, got, want)
				}
			}
			_, _ = response.Write([]byte(`{"total":1,"list":[{"id":91827364,"targetAsset":"BTC","planType":"SINGLE","planName":"BTC","planId":1234,"transactionDateTime":1699999000000,"transactionStatus":"SUCCESS","failedType":"NON","sourceAsset":"USDT","sourceAssetAmount":"15.00000000","targetAssetAmount":"0.00025000","sourceWallet":"SPOT_WALLET","executionPrice":"60000","executionType":"RECURRING","transactionFee":"0.02000000","transactionFeeUnit":"USDT","executionAssetType":"CRYPTO"}]}`))
		case autoInvestRedeemPath:
			assertSpotSignature(t, request, "fake-api-secret")
			if got := request.URL.Query().Get("current"); got != "1" {
				t.Errorf("redeem current = %q, want 1", got)
			}
			_, _ = response.Write([]byte(`[{"indexId":77,"indexName":"DEFI","redemptionId":20180,"status":"SUCCESS","asset":"USDT","amount":"12.34000000","redemptionDateTime":1699999500000,"transactionFee":"0.01000000","transactionFeeUnit":"USDT"}]`))
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}

	first, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch tx page: %v", err)
	}
	if first.Done || first.NextCursor != "redeem:1" || len(first.Entries) != 1 {
		t.Fatalf("first = done %t cursor %q entries %d", first.Done, first.NextCursor, len(first.Entries))
	}
	tx := first.Entries[0]
	if tx.Exchange != model.ExchangeBinance || tx.EntryID != "autoinvest-tx:91827364" {
		t.Fatalf("tx identity = %#v", tx)
	}
	if tx.Category != "autoinvest" || tx.Type != "execution" || tx.Info != "SUCCESS" || tx.Side != "BUY" {
		t.Fatalf("tx classification = %#v", tx)
	}
	if tx.Asset != "BTC" || tx.Amount != "0.00025000" || tx.CashFlow != "15.00000000" || tx.Fee != "0.02000000" {
		t.Fatalf("tx amounts = %#v", tx)
	}
	if tx.OrderID != "1234" || !tx.OccurredAt.Equal(time.UnixMilli(1699999000000)) || !tx.ObservedAt.Equal(fixedNow) {
		t.Fatalf("tx provider fields = %#v", tx)
	}
	if !json.Valid(tx.RawJSON) || !strings.Contains(string(tx.RawJSON), `"executionPrice":"60000"`) {
		t.Fatalf("tx raw JSON = %s", tx.RawJSON)
	}

	request.Cursor = first.NextCursor
	second, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("fetch redeem page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Entries) != 1 {
		t.Fatalf("second = done %t cursor %q entries %d", second.Done, second.NextCursor, len(second.Entries))
	}
	redeem := second.Entries[0]
	if redeem.EntryID != "autoinvest-redeem:20180" || redeem.Type != "redemption" || redeem.Side != "SELL" {
		t.Fatalf("redeem identity = %#v", redeem)
	}
	if redeem.Asset != "USDT" || redeem.Amount != "12.34000000" || redeem.Fee != "0.01000000" || redeem.OrderID != "77" {
		t.Fatalf("redeem amounts = %#v", redeem)
	}
	if redeem.Info != "SUCCESS" || !redeem.OccurredAt.Equal(time.UnixMilli(1699999500000)) {
		t.Fatalf("redeem provider fields = %#v", redeem)
	}
}

func TestAutoInvestFetchPagePaginatesAndTerminates(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-24 * time.Hour)
	historyCurrents := map[string]int{}
	redeemCurrents := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case autoInvestHistoryPath:
			current := request.URL.Query().Get("current")
			historyCurrents[current]++
			if current == "1" {
				rows := make([]map[string]any, autoInvestPageSize)
				for index := range rows {
					rows[index] = autoInvestTxFixture(index + 1)
				}
				_ = json.NewEncoder(response).Encode(map[string]any{"total": autoInvestPageSize + 1, "list": rows})
				return
			}
			_ = json.NewEncoder(response).Encode(map[string]any{
				"total": autoInvestPageSize + 1, "list": []map[string]any{autoInvestTxFixture(101)},
			})
		case autoInvestRedeemPath:
			redeemCurrents[request.URL.Query().Get("current")]++
			_ = json.NewEncoder(response).Encode([]map[string]any{autoInvestRedeemFixture(555)})
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary"}, Start: start, End: fixedNow}

	var txCount, redeemCount int
	cursors := []string{}
	for iterations := 0; ; iterations++ {
		if iterations > 10 {
			t.Fatalf("crawl did not terminate; cursors = %v", cursors)
		}
		page, err := adapter.FetchPage(t.Context(), request)
		if err != nil {
			t.Fatalf("fetch page: %v", err)
		}
		for _, entry := range page.Entries {
			switch entry.Type {
			case "execution":
				txCount++
			case "redemption":
				redeemCount++
			default:
				t.Fatalf("unexpected entry type %q", entry.Type)
			}
		}
		if page.Done {
			break
		}
		if page.NextCursor == "" || page.NextCursor == request.Cursor {
			t.Fatalf("non-advancing cursor %q -> %q", request.Cursor, page.NextCursor)
		}
		cursors = append(cursors, page.NextCursor)
		request.Cursor = page.NextCursor
	}

	if txCount != autoInvestPageSize+1 || redeemCount != 1 {
		t.Fatalf("counts = tx %d redeem %d", txCount, redeemCount)
	}
	if historyCurrents["1"] != 1 || historyCurrents["2"] != 1 || redeemCurrents["1"] != 1 {
		t.Fatalf("page fan-out = history %v redeem %v", historyCurrents, redeemCurrents)
	}
}

func TestAutoInvestFetchPageEnforcesWindowBounds(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()

	t.Run("window too wide", func(t *testing.T) {
		t.Parallel()
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			t.Errorf("adapter called the network for an invalid window: %q", request.URL.Path)
		}))
		t.Cleanup(server.Close)
		adapter := newTestAutoInvestAdapter(t, server, fixedNow)
		_, err := adapter.FetchPage(t.Context(), source.PageRequest{
			Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-31 * 24 * time.Hour), End: fixedNow,
		})
		if err == nil || !strings.Contains(err.Error(), "30 days") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("out-of-window rows dropped", func(t *testing.T) {
		t.Parallel()
		start := fixedNow.Add(-time.Hour)
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case autoInvestHistoryPath:
				inWindow := autoInvestTxFixture(1)
				inWindow["transactionDateTime"] = start.Add(30 * time.Minute).UnixMilli()
				stale := autoInvestTxFixture(2)
				stale["transactionDateTime"] = start.Add(-time.Hour).UnixMilli()
				_ = json.NewEncoder(response).Encode(map[string]any{"total": 2, "list": []map[string]any{inWindow, stale}})
			case autoInvestRedeemPath:
				_, _ = response.Write([]byte(`[]`))
			default:
				t.Errorf("unexpected path %q", request.URL.Path)
			}
		}))
		t.Cleanup(server.Close)
		adapter := newTestAutoInvestAdapter(t, server, fixedNow)
		page, err := adapter.FetchPage(t.Context(), source.PageRequest{
			Account: source.Account{ID: "primary"}, Start: start, End: fixedNow,
		})
		if err != nil {
			t.Fatalf("fetch page: %v", err)
		}
		// The tx phase ends on this short page but hands off to the redeem phase
		// via a cursor (see TestAutoInvestFetchPageNormalizesAndCrossesPhases and
		// TestAutoInvestFetchPagePaginatesAndTerminates, which pin that the redeem
		// page is fetched exactly once on the following call), so this first call
		// is not Done. What this case verifies is that the stale row is dropped:
		// only the in-window entry survives.
		if page.Done || page.NextCursor != "redeem:1" || len(page.Entries) != 1 || page.Entries[0].EntryID != "autoinvest-tx:1" {
			t.Fatalf("page = done %t cursor %q entries %#v", page.Done, page.NextCursor, page.Entries)
		}
	})
}

func TestAutoInvestFetchPageRejectsInvalidCursor(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		t.Errorf("adapter called the network for an invalid cursor: %q", request.URL.Path)
	}))
	t.Cleanup(server.Close)
	adapter := newTestAutoInvestAdapter(t, server, fixedNow)
	for _, cursor := range []string{"tx:0", "bogus:1", "tx", " tx:1", "tx:-1", "redeem:x"} {
		request := source.PageRequest{
			Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow, Cursor: cursor,
		}
		if _, err := adapter.FetchPage(t.Context(), request); err == nil {
			t.Errorf("cursor %q was accepted", cursor)
		}
	}
}

func TestAutoInvestFetchPageRedactsSecretsInErrors(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		// A hostile/echoing provider that reflects the signed query back in its message.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"rejected signature fake-api-secret for key fake-api-key"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestAutoInvestAdapter(t, server, fixedNow)
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"}, Start: fixedNow.Add(-time.Hour), End: fixedNow,
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(apiError.Error(), "fake-api-secret") || strings.Contains(apiError.Error(), "fake-api-key") {
		t.Fatalf("error leaked a credential: %q", apiError.Error())
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("message did not redact secrets: %q", apiError.Message)
	}
}

func newTestAutoInvestAdapter(t *testing.T, server *httptest.Server, now time.Time) *AutoInvestAdapter {
	t.Helper()
	adapter, err := NewAutoInvest(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new auto-invest adapter: %v", err)
	}
	return adapter
}

func autoInvestTxFixture(id int) map[string]any {
	return map[string]any{
		"id": id, "targetAsset": "BTC", "planType": "SINGLE", "planName": "BTC", "planId": 1234,
		"transactionDateTime": int64(1_699_999_000_000), "transactionStatus": "SUCCESS", "failedType": "NON",
		"sourceAsset": "USDT", "sourceAssetAmount": "15.00000000", "targetAssetAmount": "0.00025000",
		"sourceWallet": "SPOT_WALLET", "executionPrice": "60000", "executionType": "RECURRING",
		"transactionFee": "0.02000000", "transactionFeeUnit": "USDT", "executionAssetType": "CRYPTO",
	}
}

func autoInvestRedeemFixture(id int) map[string]any {
	return map[string]any{
		"indexId": 77, "indexName": "DEFI", "redemptionId": id, "status": "SUCCESS",
		"asset": "USDT", "amount": "12.34000000", "redemptionDateTime": int64(1_699_999_500_000),
		"transactionFee": "0.01000000", "transactionFeeUnit": "USDT",
	}
}
