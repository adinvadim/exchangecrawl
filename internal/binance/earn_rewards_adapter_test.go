package binance

import (
	"crypto/sha256"
	"encoding/hex"
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

func newTestEarnRewardsAdapter(t *testing.T, server *httptest.Server, now time.Time) *EarnRewardsAdapter {
	t.Helper()
	adapter, err := NewEarnRewards(Options{
		BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now },
		LookupEnv: mapLookup(map[string]string{
			defaultAPIKeyEnv: "fake-api-key", defaultAPISecretEnv: "fake-api-secret",
		}),
	})
	if err != nil {
		t.Fatalf("new earn rewards adapter: %v", err)
	}
	return adapter
}

func lockedRewardFixture(id int, timeMs int64) map[string]any {
	return map[string]any{
		"positionId": id, "asset": "BNB", "lockPeriod": "30",
		"amount": "0.001000" + strconv.Itoa(id%10), "time": timeMs,
	}
}

// TestEarnRewardsWalksFlexiblePhasesThenLockedSweep drives the full cursor walk:
// three flexible types (one empty), window-bound filtering, then a paginated
// locked sweep. It also asserts normalization of both reward shapes.
func TestEarnRewardsWalksFlexiblePhasesThenLockedSweep(t *testing.T) {
	t.Parallel()

	fixedNow := time.UnixMilli(1_700_000_000_000).UTC()
	start := fixedNow.Add(-3 * 24 * time.Hour)
	inWindow := int64(1_699_900_000_000)

	var flexibleTypes []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		assertSpotSignature(t, request, "fake-api-secret")
		for key, want := range map[string]string{
			"startTime": strconv.FormatInt(start.UnixMilli(), 10),
			"endTime":   strconv.FormatInt(fixedNow.UnixMilli(), 10),
			"size":      strconv.Itoa(earnRewardsPageSize), "recvWindow": "5000",
		} {
			if got := query.Get(key); got != want {
				t.Errorf("%s %s = %q, want %q", request.URL.Path, key, got, want)
			}
		}
		switch request.URL.Path {
		case earnFlexibleRewardsPath:
			rewardType := query.Get("type")
			flexibleTypes = append(flexibleTypes, rewardType)
			if query.Get("current") != "" {
				t.Errorf("flexible request paginated with current=%q", query.Get("current"))
			}
			switch rewardType {
			case "BONUS":
				_, _ = response.Write([]byte(`{"total":1,"rows":[{"asset":"USDT","rewards":"0.00006408","projectId":"USDT001","type":"BONUS","time":1699900000000}]}`))
			case "REALTIME":
				_, _ = response.Write([]byte(`{"total":0,"rows":[]}`))
			case "REWARDS":
				_, _ = response.Write([]byte(`{"total":2,"rows":[` +
					`{"asset":"BNB","rewards":"0.10000000","projectId":"BNB001","type":"REWARDS","time":1699900000000},` +
					`{"asset":"BNB","rewards":"9.99999999","projectId":"BNB001","type":"REWARDS","time":1700500000000}]}`))
			default:
				t.Errorf("unexpected flexible type %q", rewardType)
			}
		case earnLockedRewardsPath:
			if query.Get("type") != "" {
				t.Errorf("locked request sent a type param: %q", query.Get("type"))
			}
			switch query.Get("current") {
			case "1":
				rows := make([]map[string]any, earnRewardsPageSize)
				for index := range rows {
					rows[index] = lockedRewardFixture(index+1, inWindow)
				}
				_ = json.NewEncoder(response).Encode(map[string]any{"total": earnRewardsPageSize + 1, "rows": rows})
			case "2":
				_ = json.NewEncoder(response).Encode(map[string]any{
					"total": earnRewardsPageSize + 1, "rows": []map[string]any{lockedRewardFixture(9999, inWindow)},
				})
			default:
				t.Errorf("unexpected locked current=%q", query.Get("current"))
			}
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	adapter := newTestEarnRewardsAdapter(t, server, fixedNow)
	request := source.PageRequest{Account: source.Account{ID: "primary", Label: "Primary"}, Start: start, End: fixedNow}

	// Page 1: flexible BONUS.
	page1, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if page1.Done || page1.NextCursor == "" || len(page1.Entries) != 1 {
		t.Fatalf("page 1 = done %t cursor %q entries %d", page1.Done, page1.NextCursor, len(page1.Entries))
	}
	bonus := page1.Entries[0]
	if bonus.Exchange != model.ExchangeBinance || bonus.Category != "earn" || bonus.Type != "flexible" {
		t.Fatalf("bonus classification = %#v", bonus)
	}
	if bonus.Asset != "USDT" || bonus.Amount != "0.00006408" || bonus.CashFlow != "0.00006408" {
		t.Fatalf("bonus amounts = %#v", bonus)
	}
	if bonus.Info != "BONUS" || bonus.OrderID != "USDT001" {
		t.Fatalf("bonus provider fields = %#v", bonus)
	}
	if !strings.HasPrefix(bonus.EntryID, "earn-reward:flexible:BONUS:") {
		t.Fatalf("bonus entry id = %q", bonus.EntryID)
	}
	if !bonus.OccurredAt.Equal(time.UnixMilli(inWindow)) || !bonus.ObservedAt.Equal(fixedNow) {
		t.Fatalf("bonus times = %s / %s", bonus.OccurredAt, bonus.ObservedAt)
	}
	if !json.Valid(bonus.RawJSON) || !strings.Contains(string(bonus.RawJSON), `"projectId":"USDT001"`) {
		t.Fatalf("bonus raw = %s", bonus.RawJSON)
	}

	// Page 2: REALTIME is empty, so the adapter skips to REWARDS in one call and
	// drops the out-of-window row.
	request.Cursor = page1.NextCursor
	page2, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if page2.Done || page2.NextCursor == "" || len(page2.Entries) != 1 {
		t.Fatalf("page 2 = done %t cursor %q entries %d", page2.Done, page2.NextCursor, len(page2.Entries))
	}
	if page2.Entries[0].Amount != "0.10000000" {
		t.Fatalf("page 2 kept the wrong row: %#v", page2.Entries[0])
	}

	// Page 3: locked sweep, first page is full so pagination continues.
	request.Cursor = page2.NextCursor
	page3, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if page3.Done || page3.NextCursor == "" || len(page3.Entries) != earnRewardsPageSize {
		t.Fatalf("page 3 = done %t cursor %q entries %d", page3.Done, page3.NextCursor, len(page3.Entries))
	}
	locked := page3.Entries[0]
	if locked.Category != "earn" || locked.Type != "locked" || locked.Asset != "BNB" {
		t.Fatalf("locked classification = %#v", locked)
	}
	if locked.Info != "30" || locked.OrderID != "1" {
		t.Fatalf("locked provider fields = %#v", locked)
	}
	if !strings.HasPrefix(locked.EntryID, "earn-reward:locked:") {
		t.Fatalf("locked entry id = %q", locked.EntryID)
	}

	// Page 4: last locked page is short, so the stream terminates.
	request.Cursor = page3.NextCursor
	page4, err := adapter.FetchPage(t.Context(), request)
	if err != nil {
		t.Fatalf("page 4: %v", err)
	}
	if !page4.Done || len(page4.Entries) != 1 || page4.Entries[0].OrderID != "9999" {
		t.Fatalf("page 4 = done %t entries %#v", page4.Done, page4.Entries)
	}

	if strings.Join(flexibleTypes, ",") != "BONUS,REALTIME,REWARDS" {
		t.Fatalf("flexible types queried in order %q", strings.Join(flexibleTypes, ","))
	}
}

// TestEarnRewardDigestInputOrderIsPinned locks the exact field order fed into
// the synthesized identity digest for both reward shapes.
func TestEarnRewardDigestInputOrderIsPinned(t *testing.T) {
	t.Parallel()

	observedAt := time.UnixMilli(1_700_000_000_000).UTC()
	account := source.Account{ID: "primary"}

	flexible, err := normalizeFlexibleReward(earnFlexibleRewardRow{
		Asset: "USDT", Rewards: "0.00006408", ProjectID: "USDT001", Type: "BONUS", Time: 1_699_900_000_000,
	}, "BONUS", account, observedAt)
	if err != nil {
		t.Fatalf("normalize flexible: %v", err)
	}
	flexibleSum := sha256.Sum256([]byte("USDT001|USDT|BONUS|1699900000000|0.00006408"))
	wantFlexible := "earn-reward:flexible:BONUS:" + hex.EncodeToString(flexibleSum[:])
	if flexible.EntryID != wantFlexible {
		t.Fatalf("flexible entry id = %q, want %q", flexible.EntryID, wantFlexible)
	}

	locked, err := normalizeLockedReward(earnLockedRewardRow{
		PositionID: "123123", Asset: "BNB", LockPeriod: "30", Amount: "21312.23223", Time: 1_699_900_000_000,
	}, account, observedAt)
	if err != nil {
		t.Fatalf("normalize locked: %v", err)
	}
	lockedSum := sha256.Sum256([]byte("123123|BNB|30|1699900000000|21312.23223"))
	wantLocked := "earn-reward:locked:" + hex.EncodeToString(lockedSum[:])
	if locked.EntryID != wantLocked {
		t.Fatalf("locked entry id = %q, want %q", locked.EntryID, wantLocked)
	}
}

func TestEarnRewardsRejectsBadWindowAndCursor(t *testing.T) {
	t.Parallel()

	adapter := newTestEarnRewardsAdapter(t, httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		t.Error("no request expected")
	})), time.UnixMilli(1_700_000_000_000).UTC())
	base := source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_000_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	}

	cases := map[string]source.PageRequest{
		"reversed window":  {Account: base.Account, Start: base.End, End: base.Start},
		"window too large": {Account: base.Account, Start: base.End.Add(-earnRewardsMaxWindow - time.Hour), End: base.End},
		"padded cursor":    {Account: base.Account, Start: base.Start, End: base.End, Cursor: " abc"},
		"unknown phase":    {Account: base.Account, Start: base.Start, End: base.End, Cursor: encodeEarnRewardsCursor(earnRewardsCursor{Phase: "flexible:MYSTERY"})},
	}
	for name, request := range cases {
		request := request
		t.Run(name, func(t *testing.T) {
			if _, err := adapter.FetchPage(t.Context(), request); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestEarnRewardsRedactsSecretsInErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		// The provider echoes the signature/secret back inside its message; the
		// adapter must not surface it.
		_, _ = response.Write([]byte(`{"code":-1022,"msg":"signature for fake-api-secret is invalid"}`))
	}))
	t.Cleanup(server.Close)

	adapter := newTestEarnRewardsAdapter(t, server, time.UnixMilli(1_700_000_000_000).UTC())
	_, err := adapter.FetchPage(t.Context(), source.PageRequest{
		Account: source.Account{ID: "primary"},
		Start:   time.UnixMilli(1_699_990_000_000).UTC(), End: time.UnixMilli(1_700_000_000_000).UTC(),
	})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Code != -1022 {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(err.Error(), "fake-api-secret") {
		t.Fatalf("error leaked the secret: %q", err.Error())
	}
	if !strings.Contains(apiError.Message, "[REDACTED]") {
		t.Fatalf("message not redacted: %q", apiError.Message)
	}
}
