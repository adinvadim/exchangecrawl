package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestP2POrdersPollsHistoryAndPending(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-p2p-key"
		secret = "fake-p2p-secret"
	)
	requestTime := time.UnixMilli(1_700_000_030_000).UTC()
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		paths = append(paths, r.URL.Path)
		body := assertP2PSigned(t, r, apiKey, secret, requestTime)
		switch r.URL.Path {
		case p2pHistoryPath:
			if got := string(body); got != `{"beginTime":"1700000000000","endTime":"1700000060000","page":1,"size":30}` {
				t.Errorf("history body = %s", got)
			}
			_, _ = fmt.Fprint(w, p2pResponse(1, `{
              "id":"fake-p2p-finished","side":1,"tokenId":"USDT","currencyId":"USD",
              "amount":"100.000000000000000001","price":"1.01","fee":"0.000000000000000001",
              "notifyTokenQuantity":"99.000000000000000001","status":50,
              "createDate":"1700000040000","futureField":"history"
            }`))
		case p2pPendingPath:
			if got := string(body); got != `{"page":1,"size":30}` {
				t.Errorf("pending body = %s", got)
			}
			_, _ = fmt.Fprint(w, p2pResponse(1, validP2POrder("fake-p2p-pending", 20)))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewP2P(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	page, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch p2p orders: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != p2pHistoryPath+","+p2pPendingPath {
		t.Fatalf("paths = %v", paths)
	}
	history, pending := page.Observations[0], page.Observations[1]
	if history.Stream != "p2p" || history.ObjectType != "p2p_order" || history.ObjectID != "p2p:fake-p2p-finished" {
		t.Fatalf("history identity = %#v", history)
	}
	if history.Status != "50" || history.Symbol != "USDTUSD" || history.Asset != "USDT" {
		t.Fatalf("history classification = %#v", history)
	}
	if history.Amount != "99.000000000000000001" || history.StateFingerprint == "" {
		t.Fatalf("history normalization = %#v", history)
	}
	if !history.OccurredAt.Equal(time.UnixMilli(1_700_000_040_000).UTC()) || !history.ObservedAt.Equal(requestTime) {
		t.Fatalf("history timestamps = %#v", history)
	}
	if !containsJSONField(history.RawJSON, "futureField", "history") {
		t.Fatalf("history raw JSON = %s", history.RawJSON)
	}
	if pending.ObjectID != "p2p:fake-p2p-pending" || pending.Status != "20" || pending.StateFingerprint == history.StateFingerprint {
		t.Fatalf("pending observation = %#v", pending)
	}
}

func TestP2POrdersSkipPendingForHistoricalWindow(t *testing.T) {
	t.Parallel()

	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path != p2pHistoryPath {
			t.Errorf("path = %q, want %q", r.URL.Path, p2pHistoryPath)
		}
		_, _ = fmt.Fprint(w, p2pResponse(1, validP2POrder("fake-history", 50)))
	}))
	t.Cleanup(server.Close)

	orders, err := NewP2P(spotTestOptions(server, time.UnixMilli(1_700_100_000_000), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	page, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch historical p2p orders: %v", err)
	}
	if !page.Done || len(page.Observations) != 1 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != p2pHistoryPath {
		t.Fatalf("paths = %v", paths)
	}
}

func TestP2POrdersMapsHistoryAndPendingCursors(t *testing.T) {
	t.Parallel()

	requestTime := time.UnixMilli(1_700_000_030_000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := assertP2PSigned(t, r, "fake-key", "fake-secret", requestTime)
		page := p2pRequestPage(t, body)
		switch r.URL.Path {
		case p2pHistoryPath:
			if page == 1 {
				_, _ = fmt.Fprint(w, fullP2PPage("fake-history-1", 20))
				return
			}
			if page != 2 {
				t.Errorf("history page = %d, want 2", page)
			}
			_, _ = fmt.Fprint(w, p2pResponse(1, validP2POrder("fake-history-2", 50)))
		case p2pPendingPath:
			if page != 1 {
				t.Errorf("pending page = %d, want 1", page)
			}
			_, _ = fmt.Fprint(w, p2pResponse(1, validP2POrder("fake-pending", 20)))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewP2P(spotTestOptions(server, requestTime, "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	request := spotPageRequest("")
	first, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done || first.NextCursor != "history:2" || len(first.Observations) != p2pPageSize {
		t.Fatalf("first page = %#v", first)
	}
	request.Cursor = first.NextCursor
	second, err := orders.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Observations) != 2 {
		t.Fatalf("second page = %#v", second)
	}
	if second.Observations[0].ObjectID != "p2p:fake-history-2" || second.Observations[1].ObjectID != "p2p:fake-pending" {
		t.Fatalf("second observations = %#v", second.Observations)
	}
}

func TestP2PTerminalOrdersBecomeStableLedgerEntries(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != p2pHistoryPath {
			t.Errorf("path = %q, want %q", r.URL.Path, p2pHistoryPath)
		}
		_, _ = fmt.Fprint(w, p2pResponse(2,
			validP2POrder("fake-active", 20),
			`{"id":"fake-terminal","side":0,"tokenId":"USDC","currencyId":"EUR","amount":"250.000000000000000001",
              "price":"0.91","fee":"0.000000000000000001","notifyTokenQuantity":"249.000000000000000001",
              "status":40,"createDate":"1700000040000","futureField":"terminal"}`))
	}))
	t.Cleanup(server.Close)

	orders, err := NewP2P(spotTestOptions(server, time.UnixMilli(1_700_000_030_000), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal p2p orders: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.EntryID != "p2p:fake-terminal" || entry.OrderID != "fake-terminal" || entry.Type != "40" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Category != "p2p" || entry.Symbol != "USDCEUR" || entry.Asset != "USDC" || entry.Side != "Buy" {
		t.Fatalf("entry classification = %#v", entry)
	}
	if entry.Amount != "249.000000000000000001" || entry.CashFlow != "250.000000000000000001" || entry.Fee != "0.000000000000000001" || entry.Info != "0.91" {
		t.Fatalf("entry decimals = %#v", entry)
	}
	if !entry.OccurredAt.Equal(time.UnixMilli(1_700_000_040_000).UTC()) {
		t.Fatalf("entry occurredAt = %#v", entry)
	}
	if !containsJSONField(entry.RawJSON, "futureField", "terminal") {
		t.Fatalf("terminal raw JSON = %s", entry.RawJSON)
	}
}

func TestP2PTerminalOrdersSkipNonterminalOnlyPages(t *testing.T) {
	t.Parallel()

	var pages []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := assertP2PSigned(t, r, "fake-key", "fake-secret", time.UnixMilli(1_700_000_030_000).UTC())
		page := p2pRequestPage(t, body)
		pages = append(pages, page)
		switch page {
		case 1:
			_, _ = fmt.Fprint(w, fullP2PPage("fake-active", 20))
		case 2:
			_, _ = fmt.Fprint(w, p2pResponse(1, validP2POrder("fake-finished", 50)))
		default:
			t.Errorf("unexpected page %d", page)
		}
	}))
	t.Cleanup(server.Close)

	orders, err := NewP2P(spotTestOptions(server, time.UnixMilli(1_700_000_030_000), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	page, err := orders.FetchPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch terminal p2p orders: %v", err)
	}
	if !page.Done || len(page.Entries) != 1 || page.Entries[0].EntryID != "p2p:fake-finished" {
		t.Fatalf("page = %#v", page)
	}
	if len(pages) != 2 || pages[0] != 1 || pages[1] != 2 {
		t.Fatalf("requested pages = %v", pages)
	}
}

func TestP2PWindowBoundsRejectWideRange(t *testing.T) {
	t.Parallel()

	orders, err := NewP2P(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	request := spotPageRequest("")
	request.End = request.Start.Add(p2pMaxWindow + time.Hour)
	if _, err := orders.FetchPage(context.Background(), request); err == nil || !strings.Contains(err.Error(), "exceeds thirty days") {
		t.Fatalf("FetchPage window error = %v", err)
	}
	if _, err := orders.FetchEventPage(context.Background(), request); err == nil || !strings.Contains(err.Error(), "exceeds thirty days") {
		t.Fatalf("FetchEventPage window error = %v", err)
	}
}

func TestP2PEndpointErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	const apiKey = "fake-secret-key-value"
	tests := []struct {
		name    string
		handler http.HandlerFunc
		fetch   func(*P2POrdersAdapter) error
		want    string
		absent  string
	}{
		{
			name: "advertiser API error redacts key",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"denied for key %s","result":{}}`, apiKey)
			},
			fetch: func(orders *P2POrdersAdapter) error {
				_, err := orders.FetchPage(context.Background(), spotPageRequest(""))
				return err
			},
			want:   "[REDACTED]",
			absent: apiKey,
		},
		{
			name: "malformed result",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":"invalid"}`)
			},
			fetch: func(orders *P2POrdersAdapter) error {
				_, err := orders.FetchEventPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "decode Bybit P2P order result",
		},
		{
			name: "HTTP error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			},
			fetch: func(orders *P2POrdersAdapter) error {
				_, err := orders.FetchPage(context.Background(), spotPageRequest(""))
				return err
			},
			want: "Bybit HTTP status 400",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(test.handler)
			t.Cleanup(server.Close)
			orders, err := NewP2P(spotTestOptions(server, time.UnixMilli(1_700_000_030_000), apiKey, "fake-secret"))
			if err != nil {
				t.Fatalf("new p2p adapter: %v", err)
			}
			err = test.fetch(orders)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if test.absent != "" && strings.Contains(err.Error(), test.absent) {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestP2POrderFingerprintTracksMutableFields(t *testing.T) {
	t.Parallel()

	base := p2pOrderRecord{
		Status: 20, Amount: "100", Price: "1.0", Fee: "0.5", NotifyTokenQuantity: "99",
	}
	changedStatus := base
	changedStatus.Status = 50
	changedAmount := base
	changedAmount.Amount = "100.000000000000000001"
	changedPrice := base
	changedPrice.Price = "1.01"
	changedFee := base
	changedFee.Fee = "0.000000000000000001"
	changedQuantity := base
	changedQuantity.NotifyTokenQuantity = "99.000000000000000001"
	for _, changed := range []p2pOrderRecord{changedStatus, changedAmount, changedPrice, changedFee, changedQuantity} {
		if p2pOrderFingerprint(base) == p2pOrderFingerprint(changed) {
			t.Fatalf("fingerprint ignored a mutable field: %#v", changed)
		}
	}
}

func TestP2POrdersRejectInvalidCursorAndNormalization(t *testing.T) {
	t.Parallel()

	orders, err := NewP2P(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new p2p adapter: %v", err)
	}
	if _, err := orders.FetchEventPage(context.Background(), spotPageRequest("provider-cursor-without-prefix")); err == nil ||
		!strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("event cursor error = %v", err)
	}
	if _, err := orders.FetchEventPage(context.Background(), spotPageRequest("history:0")); err == nil ||
		!strings.Contains(err.Error(), "invalid page") {
		t.Fatalf("event page error = %v", err)
	}
	if _, err := orders.FetchPage(context.Background(), spotPageRequest("not-a-number")); err == nil ||
		!strings.Contains(err.Error(), "invalid page") {
		t.Fatalf("ledger cursor error = %v", err)
	}
	if _, _, err := decodeP2POrder([]byte(`{"id":"fake","status":50,"createDate":"invalid"}`)); err == nil ||
		!strings.Contains(err.Error(), "createDate") {
		t.Fatalf("normalization error = %v", err)
	}
	if _, _, err := decodeP2POrder([]byte(`{"status":50,"createDate":"1700000030000"}`)); err == nil ||
		!strings.Contains(err.Error(), "no id") {
		t.Fatalf("missing id error = %v", err)
	}
}

func assertP2PSigned(t *testing.T, request *http.Request, apiKey, secret string, now time.Time) []byte {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	assertHeader(t, request.Header, "Content-Type", "application/json")
	assertHeader(t, request.Header, "X-BAPI-API-KEY", apiKey)
	assertHeader(t, request.Header, "X-BAPI-TIMESTAMP", fmt.Sprint(now.UnixMilli()))
	assertHeader(t, request.Header, "X-BAPI-RECV-WINDOW", "5000")
	payload := fmt.Sprint(now.UnixMilli()) + apiKey + "5000" + string(body)
	assertHeader(t, request.Header, "X-BAPI-SIGN", hmacHex(secret, payload))
	return body
}

func p2pResponse(count int, items ...string) string {
	return fmt.Sprintf(`{"retCode":0,"retMsg":"SUCCESS","result":{"count":%d,"items":[%s]}}`, count, strings.Join(items, ","))
}

func validP2POrder(id string, status int) string {
	return fmt.Sprintf(
		`{"id":%q,"side":1,"tokenId":"USDT","currencyId":"USD","amount":"100","price":"1.0","fee":"0.5","notifyTokenQuantity":"99","status":%d,"createDate":"1700000030000"}`,
		id, status,
	)
}

func fullP2PPage(prefix string, status int) string {
	items := make([]string, p2pPageSize)
	for i := range items {
		items[i] = validP2POrder(fmt.Sprintf("%s-%d", prefix, i), status)
	}
	return p2pResponse(p2pPageSize, items...)
}

func p2pRequestPage(t *testing.T, body []byte) int {
	t.Helper()
	var request struct {
		Page int `json:"page"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("decode request page: %v", err)
	}
	return request.Page
}
