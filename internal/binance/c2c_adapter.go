package binance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	c2cDefaultBaseURL   = "https://api.binance.com"
	c2cOrderHistoryPath = "/sapi/v1/c2c/orderMatch/listUserOrderHistory"
	c2cPageRows         = 100
	c2cMaxWindow        = 30 * 24 * time.Hour
	c2cMaxPages         = 1000
	c2cPhaseBuy         = "buy"
	c2cPhaseSell        = "sell"
)

// C2CAdapter archives Binance C2C (P2P) order history. Because C2C history
// exposes non-terminal statuses, it plays two roles keyed on the stable
// orderNumber: FetchEventPage records every observed state and FetchPage
// promotes only terminal orders into immutable ledger entries.
//
// Binance docs conflict on whether tradeType is required, so each page position
// is fetched twice (tradeType=BUY then SELL) through a phase cursor and deduped
// by orderNumber. That is correct whether the server honors tradeType (BUY and
// SELL sets are disjoint) or ignores it (identical rows collapse on the stable
// EntryID/ObjectID during upsert).
type C2CAdapter struct {
	client *Adapter
}

var (
	_ source.Adapter      = (*C2CAdapter)(nil)
	_ source.EventAdapter = (*C2CAdapter)(nil)
)

// NewC2C builds a C2C adapter that reuses the standard Binance signing, retry,
// and base-URL validation. C2C history lives on the sapi host.
func NewC2C(options Options) (*C2CAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = c2cDefaultBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &C2CAdapter{client: client}, nil
}

func (a *C2CAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *C2CAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *C2CAdapter) credentials(account source.Account) (string, string, error) {
	apiKey, ok := a.client.credential(account.APIKeyEnv, defaultAPIKeyEnv)
	if !ok {
		return "", "", fmt.Errorf("Binance credential environment variable %s is missing", envName(account.APIKeyEnv, defaultAPIKeyEnv))
	}
	secret, ok := a.client.credential(account.APISecretEnv, defaultAPISecretEnv)
	if !ok {
		return "", "", fmt.Errorf("Binance credential environment variable %s is missing", envName(account.APISecretEnv, defaultAPISecretEnv))
	}
	return apiKey, secret, nil
}

func validateC2CWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance C2C page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > c2cMaxWindow {
		return errors.New("Binance C2C page window must not exceed 30 days")
	}
	return nil
}

// FetchPage promotes terminal C2C orders into immutable ledger entries. The
// stable "p2p:<orderNumber>" EntryID makes overlapping re-polls idempotent.
func (a *C2CAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateC2CWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeC2CCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	seen := make(map[string]struct{}, c2cMaxPages)
	for pages := 0; pages < c2cMaxPages; pages++ {
		key := encodeC2CCursor(cursor)
		if _, duplicate := seen[key]; duplicate {
			return source.Page{}, errors.New("Binance C2C order history repeated cursor")
		}
		seen[key] = struct{}{}
		rows, observedAt, phaseDone, err := a.fetchOrderRows(ctx, request, cursor, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		entries, err := a.terminalEntries(rows, request, observedAt)
		if err != nil {
			return source.Page{}, err
		}
		next := nextC2CCursor(cursor, phaseDone)
		if len(entries) != 0 {
			return source.Page{Entries: entries, NextCursor: next, Done: next == ""}, nil
		}
		if next == "" {
			return source.Page{Done: true}, nil
		}
		if cursor, err = decodeC2CCursor(next); err != nil {
			return source.Page{}, err
		}
	}
	return source.Page{}, fmt.Errorf("Binance C2C order history exceeded %d pages", c2cMaxPages)
}

// FetchEventPage records every observed C2C order state, including non-terminal
// statuses, as StateObservations fingerprinted over the mutable fields.
func (a *C2CAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateC2CWindow(request); err != nil {
		return source.EventPage{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	cursor, err := decodeC2CCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	seen := make(map[string]struct{}, c2cMaxPages)
	for pages := 0; pages < c2cMaxPages; pages++ {
		key := encodeC2CCursor(cursor)
		if _, duplicate := seen[key]; duplicate {
			return source.EventPage{}, errors.New("Binance C2C order history repeated cursor")
		}
		seen[key] = struct{}{}
		rows, observedAt, phaseDone, err := a.fetchOrderRows(ctx, request, cursor, apiKey, secret)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := a.orderObservations(rows, request, observedAt)
		if err != nil {
			return source.EventPage{}, err
		}
		next := nextC2CCursor(cursor, phaseDone)
		if len(observations) != 0 {
			return source.EventPage{Observations: observations, NextCursor: next, Done: next == ""}, nil
		}
		if next == "" {
			return source.EventPage{Done: true}, nil
		}
		if cursor, err = decodeC2CCursor(next); err != nil {
			return source.EventPage{}, err
		}
	}
	return source.EventPage{}, fmt.Errorf("Binance C2C order history exceeded %d pages", c2cMaxPages)
}

func (a *C2CAdapter) fetchOrderRows(
	ctx context.Context,
	request source.PageRequest,
	cursor c2cCursor,
	apiKey string,
	secret string,
) ([]c2cOrderRow, time.Time, bool, error) {
	query := url.Values{
		"tradeType":      {c2cTradeType(cursor.phase)},
		"startTimestamp": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTimestamp":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"page":           {strconv.Itoa(cursor.page)},
		"rows":           {strconv.Itoa(c2cPageRows)},
		"recvWindow":     {strconv.Itoa(a.client.recvWindow)},
	}
	var response c2cOrderHistoryResponse
	if err := a.client.signedGETOperation(ctx, c2cOrderHistoryPath, query, apiKey, secret, "c2c order history", &response); err != nil {
		return nil, time.Time{}, false, err
	}
	observedAt := a.client.now().UTC()
	// A short page or reaching the server-reported total ends this tradeType.
	phaseDone := len(response.Data) < c2cPageRows || cursor.page*c2cPageRows >= response.Total
	return response.Data, observedAt, phaseDone, nil
}

func (a *C2CAdapter) terminalEntries(
	rows []c2cOrderRow,
	request source.PageRequest,
	observedAt time.Time,
) ([]model.LedgerEntry, error) {
	entries := make([]model.LedgerEntry, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		if !isTerminalC2COrderStatus(strings.TrimSpace(row.OrderStatus)) {
			continue
		}
		entry, err := normalizeC2COrder(row, request.Account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance C2C order row %d: %w", index, err)
		}
		if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
			continue
		}
		if _, duplicate := seen[entry.EntryID]; duplicate {
			continue
		}
		seen[entry.EntryID] = struct{}{}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (a *C2CAdapter) orderObservations(
	rows []c2cOrderRow,
	request source.PageRequest,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		observation, err := normalizeC2CObservation(row, request.Account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance C2C order row %d: %w", index, err)
		}
		if observation.OccurredAt.Before(request.Start) || observation.OccurredAt.After(request.End) {
			continue
		}
		if _, duplicate := seen[observation.ObjectID]; duplicate {
			continue
		}
		seen[observation.ObjectID] = struct{}{}
		observations = append(observations, observation)
	}
	return observations, nil
}

type c2cCursor struct {
	phase string
	page  int
}

func encodeC2CCursor(cursor c2cCursor) string {
	return cursor.phase + ":" + strconv.Itoa(cursor.page)
}

func decodeC2CCursor(value string) (c2cCursor, error) {
	if value == "" {
		return c2cCursor{phase: c2cPhaseBuy, page: 1}, nil
	}
	if value != strings.TrimSpace(value) {
		return c2cCursor{}, errors.New("Binance C2C cursor is invalid")
	}
	phase, rawPage, found := strings.Cut(value, ":")
	if !found || (phase != c2cPhaseBuy && phase != c2cPhaseSell) {
		return c2cCursor{}, errors.New("Binance C2C cursor is invalid")
	}
	page, err := strconv.Atoi(rawPage)
	if err != nil || page < 1 {
		return c2cCursor{}, errors.New("Binance C2C cursor is invalid")
	}
	return c2cCursor{phase: phase, page: page}, nil
}

func nextC2CCursor(cursor c2cCursor, phaseDone bool) string {
	if !phaseDone {
		return encodeC2CCursor(c2cCursor{phase: cursor.phase, page: cursor.page + 1})
	}
	if cursor.phase == c2cPhaseBuy {
		return encodeC2CCursor(c2cCursor{phase: c2cPhaseSell, page: 1})
	}
	return ""
}

func c2cTradeType(phase string) string {
	if phase == c2cPhaseSell {
		return "SELL"
	}
	return "BUY"
}

type c2cOrderHistoryResponse struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Data    []c2cOrderRow `json:"data"`
	Total   int           `json:"total"`
	Success bool          `json:"success"`
}

type c2cOrderRow struct {
	OrderNumber         string          `json:"orderNumber"`
	AdvNo               string          `json:"advNo"`
	TradeType           string          `json:"tradeType"`
	Asset               string          `json:"asset"`
	Fiat                string          `json:"fiat"`
	FiatSymbol          string          `json:"fiatSymbol"`
	Amount              string          `json:"amount"`
	TotalPrice          string          `json:"totalPrice"`
	UnitPrice           string          `json:"unitPrice"`
	OrderStatus         string          `json:"orderStatus"`
	CreateTime          int64           `json:"createTime"`
	Commission          string          `json:"commission"`
	CounterPartNickName string          `json:"counterPartNickName"`
	AdvertisementRole   string          `json:"advertisementRole"`
	RawJSON             json.RawMessage `json:"-"`
}

func (row *c2cOrderRow) UnmarshalJSON(data []byte) error {
	type wire c2cOrderRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = c2cOrderRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func c2cOrderIdentity(row c2cOrderRow) (orderNumber, status string, occurredAt time.Time, err error) {
	orderNumber = strings.TrimSpace(row.OrderNumber)
	status = strings.TrimSpace(row.OrderStatus)
	if orderNumber == "" {
		return "", "", time.Time{}, errors.New("c2c order row has no order number")
	}
	if status == "" {
		return "", "", time.Time{}, errors.New("c2c order row has no status")
	}
	if row.CreateTime <= 0 || row.CreateTime > maxArchivedUnixMS {
		return "", "", time.Time{}, errors.New("c2c order row has invalid create time")
	}
	return orderNumber, status, time.UnixMilli(row.CreateTime).UTC(), nil
}

func normalizeC2COrder(row c2cOrderRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	orderNumber, status, occurredAt, err := c2cOrderIdentity(row)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	asset := strings.TrimSpace(row.Asset)
	return model.LedgerEntry{
		Exchange:     model.ExchangeBinance,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      "p2p:" + orderNumber,
		Symbol:       c2cSymbol(asset, strings.TrimSpace(row.Fiat)),
		Category:     "p2p",
		Type:         status,
		Asset:        asset,
		Side:         strings.TrimSpace(row.TradeType),
		Amount:       strings.TrimSpace(row.Amount),
		Fee:          strings.TrimSpace(row.Commission),
		CashFlow:     strings.TrimSpace(row.TotalPrice),
		OrderID:      orderNumber,
		Info:         strings.TrimSpace(row.AdvertisementRole),
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func normalizeC2CObservation(row c2cOrderRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	orderNumber, status, occurredAt, err := c2cOrderIdentity(row)
	if err != nil {
		return model.StateObservation{}, err
	}
	asset := strings.TrimSpace(row.Asset)
	return model.StateObservation{
		Exchange:         model.ExchangeBinance,
		AccountID:        account.ID,
		AccountLabel:     account.Label,
		Stream:           "p2p",
		ObjectType:       "p2p_order",
		ObjectID:         "p2p:" + orderNumber,
		Status:           status,
		StateFingerprint: c2cFingerprint(status, row.Amount, row.TotalPrice, row.Commission),
		Symbol:           c2cSymbol(asset, strings.TrimSpace(row.Fiat)),
		Asset:            asset,
		Amount:           strings.TrimSpace(row.Amount),
		OccurredAt:       occurredAt,
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func c2cSymbol(asset, fiat string) string {
	return asset + fiat
}

func c2cFingerprint(status, amount, totalPrice, commission string) string {
	state := struct {
		OrderStatus string `json:"orderStatus"`
		Amount      string `json:"amount"`
		TotalPrice  string `json:"totalPrice"`
		Commission  string `json:"commission"`
	}{
		OrderStatus: status,
		Amount:      strings.TrimSpace(amount),
		TotalPrice:  strings.TrimSpace(totalPrice),
		Commission:  strings.TrimSpace(commission),
	}
	data, _ := json.Marshal(state)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func isTerminalC2COrderStatus(status string) bool {
	switch status {
	case "COMPLETED", "CANCELLED", "CANCELLED_BY_SYSTEM":
		return true
	default:
		return false
	}
}
