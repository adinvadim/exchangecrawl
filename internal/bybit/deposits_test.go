package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDepositsDrainsOnchainThenInternal(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-deposit-key"
		secret = "fake-deposit-secret"
	)
	requestTime := time.UnixMilli(1_700_000_123_456).UTC()
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		paths = append(paths, r.URL.Path)
		assertSpotSigned(t, r, apiKey, secret, requestTime)
		switch r.URL.Path {
		case depositOnchainPath:
			wantQuery := url.Values{
				"startTime": {"1700000000000"}, "endTime": {"1700000060000"}, "limit": {"50"},
			}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("on-chain query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{
              "nextPageCursor":"",
              "rows":[{
                "coin":"USDT","chain":"ETH","amount":"10.000000000000000001",
                "txID":"fake-tx-1","status":3,"successAt":"1700000030000",
                "confirmations":"12","depositType":0,"futureField":"onchain"
              }]
            }}`)
		case depositInternalPath:
			wantQuery := url.Values{
				"startTime": {"1700000000000"}, "endTime": {"1700000060000"}, "limit": {"50"},
			}.Encode()
			if r.URL.RawQuery != wantQuery {
				t.Errorf("internal query = %q, want %q", r.URL.RawQuery, wantQuery)
			}
			_, _ = fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{
              "nextPageCursor":"",
              "rows":[{
                "id":"fake-internal-1","coin":"BTC","amount":"0.000000000000000001",
                "status":2,"createdTime":"1700000040000","futureField":"internal"
              }]
            }}`)
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	deposits, err := NewDeposits(spotTestOptions(server, requestTime, apiKey, secret))
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}
	page, err := deposits.FetchEventPage(context.Background(), spotPageRequest(""))
	if err != nil {
		t.Fatalf("fetch deposits: %v", err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Observations) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if strings.Join(paths, ",") != depositOnchainPath+","+depositInternalPath {
		t.Fatalf("paths = %v", paths)
	}

	onchain, internal := page.Observations[0], page.Observations[1]
	if onchain.Stream != "asset" || onchain.ObjectType != "deposit" {
		t.Fatalf("on-chain stream classification = %#v", onchain)
	}
	if onchain.ObjectID != "deposit:onchain:fake-tx-1" || onchain.Status != "3" || onchain.Asset != "USDT" {
		t.Fatalf("on-chain identity = %#v", onchain)
	}
	if onchain.Amount != "10.000000000000000001" || onchain.StateFingerprint == "" {
		t.Fatalf("on-chain normalization = %#v", onchain)
	}
	if !onchain.OccurredAt.Equal(time.UnixMilli(1_700_000_030_000).UTC()) || !onchain.ObservedAt.Equal(requestTime) {
		t.Fatalf("on-chain timestamps = %#v", onchain)
	}
	if !containsJSONField(onchain.RawJSON, "futureField", "onchain") {
		t.Fatalf("on-chain raw JSON = %s", onchain.RawJSON)
	}

	if internal.ObjectID != "deposit:internal:fake-internal-1" || internal.Status != "2" || internal.Asset != "BTC" {
		t.Fatalf("internal identity = %#v", internal)
	}
	if !internal.OccurredAt.Equal(time.UnixMilli(1_700_000_040_000).UTC()) {
		t.Fatalf("internal timestamp = %#v", internal)
	}
	if internal.StateFingerprint == onchain.StateFingerprint {
		t.Fatalf("internal and on-chain fingerprints collided")
	}
	if !containsJSONField(internal.RawJSON, "futureField", "internal") {
		t.Fatalf("internal raw JSON = %s", internal.RawJSON)
	}
}

func TestDepositsPaginatesAcrossPhases(t *testing.T) {
	t.Parallel()

	var onchainCalls, internalCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case depositOnchainPath:
			onchainCalls++
			if onchainCalls == 1 {
				_, _ = fmt.Fprint(w, depositResponse("oc-2", validOnchainDeposit("fake-tx-1", 2)))
				return
			}
			if got := r.URL.Query().Get("cursor"); got != "oc-2" {
				t.Errorf("on-chain cursor = %q, want oc-2", got)
			}
			_, _ = fmt.Fprint(w, depositResponse("", validOnchainDeposit("fake-tx-2", 3)))
		case depositInternalPath:
			internalCalls++
			if internalCalls == 1 {
				_, _ = fmt.Fprint(w, depositResponse("in-2", validInternalDeposit("fake-internal-1", 1)))
				return
			}
			if got := r.URL.Query().Get("cursor"); got != "in-2" {
				t.Errorf("internal cursor = %q, want in-2", got)
			}
			_, _ = fmt.Fprint(w, depositResponse("", validInternalDeposit("fake-internal-2", 2)))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	deposits, err := NewDeposits(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}

	request := spotPageRequest("")
	first, err := deposits.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done || first.NextCursor != "onchain:oc-2" || len(first.Observations) != 1 {
		t.Fatalf("first page = %#v", first)
	}

	request.Cursor = first.NextCursor
	second, err := deposits.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	// On-chain exhausts and the same call drains the first internal page.
	if second.Done || second.NextCursor != "internal:in-2" || len(second.Observations) != 2 {
		t.Fatalf("second page = %#v", second)
	}
	if second.Observations[0].ObjectID != "deposit:onchain:fake-tx-2" {
		t.Fatalf("second on-chain observation = %#v", second.Observations[0])
	}
	if second.Observations[1].ObjectID != "deposit:internal:fake-internal-1" {
		t.Fatalf("second internal observation = %#v", second.Observations[1])
	}

	request.Cursor = second.NextCursor
	third, err := deposits.FetchEventPage(context.Background(), request)
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if !third.Done || third.NextCursor != "" || len(third.Observations) != 1 {
		t.Fatalf("third page = %#v", third)
	}
	if third.Observations[0].ObjectID != "deposit:internal:fake-internal-2" {
		t.Fatalf("third observation = %#v", third.Observations[0])
	}
}

func TestDepositsRejectsRepeatedProviderCursor(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The provider echoes back the cursor it was handed, which would loop.
		_, _ = fmt.Fprint(w, depositResponse("oc-loop", validOnchainDeposit("fake-tx-1", 2)))
	}))
	t.Cleanup(server.Close)

	deposits, err := NewDeposits(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), "fake-key", "fake-secret"))
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}
	request := spotPageRequest("onchain:oc-loop")
	_, err = deposits.FetchEventPage(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error = %v, want repeated cursor", err)
	}
}

func TestDepositsRejectsWindowAndCursor(t *testing.T) {
	t.Parallel()

	deposits, err := NewDeposits(Options{BaseURL: "https://api.bybit.test"})
	if err != nil {
		t.Fatalf("new deposits adapter: %v", err)
	}

	wide := spotPageRequest("")
	wide.End = wide.Start.Add(31 * 24 * time.Hour)
	if _, err := deposits.FetchEventPage(context.Background(), wide); err == nil ||
		!strings.Contains(err.Error(), "exceeds 30 days") {
		t.Fatalf("window error = %v, want exceeds 30 days", err)
	}

	if _, err := deposits.FetchEventPage(context.Background(), spotPageRequest("bogus-prefix")); err == nil ||
		!strings.Contains(err.Error(), "invalid source prefix") {
		t.Fatalf("cursor error = %v, want invalid source prefix", err)
	}
}

func TestDepositsErrorsRedactSecrets(t *testing.T) {
	t.Parallel()

	const (
		apiKey = "fake-deposit-key"
		secret = "fake-deposit-secret"
	)
	tests := []struct {
		name    string
		handler http.HandlerFunc
		cursor  string
		want    string
		absent  string
	}{
		{
			name: "API error message is sanitized",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// A hostile retMsg echoing the secret must never survive into the error.
				_, _ = fmt.Fprintf(w, `{"retCode":10003,"retMsg":"bad key %s","result":{}}`, secret)
			},
			want:   "Bybit API error 10003",
			absent: secret,
		},
		{
			name: "malformed result is reported without leaking",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"retCode":0,"result":{"rows":"invalid"}}`)
			},
			want: "decode Bybit deposit result",
		},
		{
			name: "HTTP failures propagate",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
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
			deposits, err := NewDeposits(spotTestOptions(server, time.UnixMilli(1_700_000_123_456), apiKey, secret))
			if err != nil {
				t.Fatalf("new deposits adapter: %v", err)
			}
			_, err = deposits.FetchEventPage(context.Background(), spotPageRequest(test.cursor))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if test.absent != "" && strings.Contains(err.Error(), test.absent) {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestDepositFingerprintTracksMutableFields(t *testing.T) {
	t.Parallel()

	base := depositFingerprint("2", "6", "0")
	if base == depositFingerprint("3", "6", "0") {
		t.Fatal("fingerprint ignored status change")
	}
	if base == depositFingerprint("2", "12", "0") {
		t.Fatal("fingerprint ignored confirmations change")
	}
	if base == depositFingerprint("2", "6", "10") {
		t.Fatal("fingerprint ignored depositType change")
	}
	// Field boundaries must not collide across concatenations.
	if depositFingerprint("26", "0") == depositFingerprint("2", "60") {
		t.Fatal("fingerprint ignored field boundaries")
	}
}

func TestDepositsNormalizeStringStatusAndPendingTimestamp(t *testing.T) {
	t.Parallel()

	// Bybit may emit status/depositType as strings; a not-yet-cleared deposit has
	// no successAt, which must leave OccurredAt zero rather than error.
	observations, err := normalizeOnchainDeposits(
		[]json.RawMessage{[]byte(`{
          "coin":"USDT","amount":"5","txID":"fake-tx-pending",
          "status":"1","successAt":"0","confirmations":"0","depositType":"0"
        }`)},
		spotPageRequest("").Account,
		time.UnixMilli(1_700_000_123_456).UTC(),
	)
	if err != nil {
		t.Fatalf("normalize pending deposit: %v", err)
	}
	if len(observations) != 1 {
		t.Fatalf("observations = %#v", observations)
	}
	if observations[0].Status != "1" || !observations[0].OccurredAt.IsZero() {
		t.Fatalf("pending observation = %#v", observations[0])
	}

	if _, err := normalizeOnchainDeposits(
		[]json.RawMessage{[]byte(`{"coin":"USDT","amount":"5","status":3}`)},
		spotPageRequest("").Account,
		time.Now(),
	); err == nil || !strings.Contains(err.Error(), "no txID") {
		t.Fatalf("missing txID error = %v", err)
	}
}

func depositResponse(nextCursor, row string) string {
	rows := "[]"
	if row != "" {
		rows = "[" + row + "]"
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"rows":%s}}`, nextCursor, rows)
}

func validOnchainDeposit(txID string, status int) string {
	return fmt.Sprintf(
		`{"coin":"USDT","amount":"1","txID":%q,"status":%d,"successAt":"1700000030000","confirmations":"6","depositType":0}`,
		txID, status,
	)
}

func validInternalDeposit(id string, status int) string {
	return fmt.Sprintf(
		`{"id":%q,"coin":"USDT","amount":"1","status":%d,"createdTime":"1700000030000"}`,
		id, status,
	)
}
