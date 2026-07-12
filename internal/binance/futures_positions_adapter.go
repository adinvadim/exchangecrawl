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

const futuresPositionsPath = "/fapi/v3/positionRisk"

// FuturesPositionsAdapter observes the current USDⓈ-M futures position snapshot.
//
// /fapi/v3/positionRisk is an unpaginated, current-state read: one signed GET
// yields every position at once, so each poll returns a single EventPage that is
// immediately Done. Positions are mutable objects, so they are modeled as
// model.StateObservation with a fingerprint over the durable position terms
// (positionAmt, entryPrice, breakEvenPrice, leverage, isolatedMargin). markPrice
// and unRealizedProfit are intentionally excluded from the fingerprint so a
// stationary position does not churn a new transition on every poll; the full
// provider row is still preserved verbatim in RawJSON.
type FuturesPositionsAdapter struct {
	client *Adapter
}

var _ source.EventAdapter = (*FuturesPositionsAdapter)(nil)

// NewFuturesPositions builds a positions adapter that reuses the standard
// Binance futures (fapi) signing, retry, and base-URL validation client.
func NewFuturesPositions(options Options) (*FuturesPositionsAdapter, error) {
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesPositionsAdapter{client: client}, nil
}

func (a *FuturesPositionsAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *FuturesPositionsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *FuturesPositionsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateFuturesPositionsWindow(request); err != nil {
		return source.EventPage{}, err
	}
	// positionRisk is a single unpaginated snapshot: no page ever emits a
	// NextCursor, so any cursor here would only be a stale value that could
	// drive an infinite re-poll loop.
	if strings.TrimSpace(request.Cursor) != "" {
		return source.EventPage{}, errors.New("Binance futures positions snapshot does not paginate")
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	query := url.Values{"recvWindow": {strconv.Itoa(a.client.recvWindow)}}
	var rows []futuresPositionRow
	if err := a.client.signedGETOperation(ctx, futuresPositionsPath, query, apiKey, secret, "futures positions", &rows); err != nil {
		return source.EventPage{}, err
	}
	observedAt := a.client.now().UTC()
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeFuturesPosition(row, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, fmt.Errorf("normalize Binance futures position row %d: %w", index, err)
		}
		observations = append(observations, observation)
	}
	return source.EventPage{Observations: observations, Done: true}, nil
}

func (a *FuturesPositionsAdapter) credentials(account source.Account) (string, string, error) {
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

// validateFuturesPositionsWindow keeps the snapshot defensive without filtering:
// a position's current state is always relevant regardless of when it last
// changed, so the request window is not applied to updateTime. Only an inverted
// window is rejected as an obviously malformed request.
func validateFuturesPositionsWindow(request source.PageRequest) error {
	if !request.Start.IsZero() && !request.End.IsZero() && request.End.Before(request.Start) {
		return errors.New("Binance futures positions window must not end before it starts")
	}
	return nil
}

type futuresPositionRow struct {
	Symbol         string          `json:"symbol"`
	PositionSide   string          `json:"positionSide"`
	PositionAmt    string          `json:"positionAmt"`
	EntryPrice     string          `json:"entryPrice"`
	BreakEvenPrice string          `json:"breakEvenPrice"`
	Leverage       string          `json:"leverage"`
	IsolatedMargin string          `json:"isolatedMargin"`
	MarginAsset    string          `json:"marginAsset"`
	UpdateTime     int64           `json:"updateTime"`
	RawJSON        json.RawMessage `json:"-"`
}

func (row *futuresPositionRow) UnmarshalJSON(data []byte) error {
	type wire futuresPositionRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = futuresPositionRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeFuturesPosition(row futuresPositionRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	symbol := strings.TrimSpace(row.Symbol)
	positionSide := strings.TrimSpace(row.PositionSide)
	if symbol == "" || positionSide == "" {
		return model.StateObservation{}, errors.New("position row has no symbol or position side")
	}
	if row.UpdateTime < 0 || row.UpdateTime > maxArchivedUnixMS {
		return model.StateObservation{}, errors.New("position row has invalid update time")
	}
	// Fingerprint only the durable position terms; markPrice and
	// unRealizedProfit move every poll and would otherwise force a fresh
	// transition on an otherwise unchanged position.
	state := struct {
		PositionAmt    string `json:"positionAmt"`
		EntryPrice     string `json:"entryPrice"`
		BreakEvenPrice string `json:"breakEvenPrice"`
		Leverage       string `json:"leverage"`
		IsolatedMargin string `json:"isolatedMargin"`
	}{
		PositionAmt:    strings.TrimSpace(row.PositionAmt),
		EntryPrice:     strings.TrimSpace(row.EntryPrice),
		BreakEvenPrice: strings.TrimSpace(row.BreakEvenPrice),
		Leverage:       strings.TrimSpace(row.Leverage),
		IsolatedMargin: strings.TrimSpace(row.IsolatedMargin),
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)
	status := "OPEN"
	if isZeroDecimal(row.PositionAmt) {
		status = "FLAT"
	}
	observation := model.StateObservation{
		Exchange:         model.ExchangeBinance,
		AccountID:        account.ID,
		AccountLabel:     account.Label,
		Stream:           "futures",
		ObjectType:       "position",
		ObjectID:         "futures-position:" + symbol + ":" + positionSide,
		Status:           status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]),
		Symbol:           symbol,
		Asset:            strings.TrimSpace(row.MarginAsset),
		Amount:           strings.TrimSpace(row.PositionAmt),
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), row.RawJSON...),
	}
	if row.UpdateTime > 0 {
		observation.OccurredAt = time.UnixMilli(row.UpdateTime).UTC()
	}
	return observation, nil
}

// isZeroDecimal reports whether a provider decimal string represents zero
// without parsing it to a float (which would lose precision on real balances).
func isZeroDecimal(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	for _, r := range value {
		if r != '0' && r != '.' && r != '-' && r != '+' {
			return false
		}
	}
	return true
}
