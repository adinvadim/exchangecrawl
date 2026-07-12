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
	depositHisrecPath = "/sapi/v1/capital/deposit/hisrec"
	depositPageLimit  = 1000
	depositMaxWindow  = 90 * 24 * time.Hour
)

// DepositsAdapter observes Binance capital deposit records (on-chain and
// internal) as mutable-object snapshots. It reuses the shared *Adapter client
// for signing, retry, and base-URL validation.
type DepositsAdapter struct {
	client *Adapter
}

var _ source.EventAdapter = (*DepositsAdapter)(nil)

// NewDeposits builds a deposits adapter against the SAPI host. Like NewSpot it
// defaults to the spot base URL (https://api.binance.com) rather than the
// futures host, because deposit history lives under /sapi.
func NewDeposits(options Options) (*DepositsAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &DepositsAdapter{client: client}, nil
}

func (a *DepositsAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *DepositsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *DepositsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateDepositWindow(request); err != nil {
		return source.EventPage{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	offset, err := parseDepositCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}

	query := url.Values{}
	query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
	query.Set("offset", strconv.Itoa(offset))
	query.Set("limit", strconv.Itoa(depositPageLimit))
	query.Set("recvWindow", strconv.Itoa(a.client.recvWindow))

	var rows []depositRow
	if err := a.client.signedGETOperation(ctx, depositHisrecPath, query, apiKey, secret, "capital deposits", &rows); err != nil {
		return source.EventPage{}, err
	}

	observedAt := a.client.now().UTC()
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeDeposit(row, request.Account, observedAt)
		if err != nil {
			return source.EventPage{}, fmt.Errorf("normalize Binance deposit row %d: %w", index, err)
		}
		if observation.OccurredAt.Before(request.Start) || observation.OccurredAt.After(request.End) {
			continue
		}
		observations = append(observations, observation)
	}

	page := source.EventPage{Observations: observations, Done: len(rows) < depositPageLimit}
	if !page.Done {
		nextOffset := offset + len(rows)
		// offset+limit pagination is strictly monotonic; guard anyway so a
		// crafted or overflowing cursor can never loop on the same page.
		if nextOffset <= offset {
			return source.EventPage{}, errors.New("Binance deposits pagination did not advance")
		}
		page.NextCursor = strconv.Itoa(nextOffset)
	}
	return page, nil
}

func (a *DepositsAdapter) credentials(account source.Account) (string, string, error) {
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

func validateDepositWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance deposits page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > depositMaxWindow {
		return errors.New("Binance deposits page window must not exceed 90 days")
	}
	return nil
}

func parseDepositCursor(cursor string) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	if cursor != strings.TrimSpace(cursor) {
		return 0, errors.New("Binance deposits cursor must be a non-negative decimal integer")
	}
	offset, err := strconv.Atoi(cursor)
	if err != nil || offset < 0 {
		return 0, errors.New("Binance deposits cursor must be a non-negative decimal integer")
	}
	return offset, nil
}

// depositRow mirrors one /sapi/v1/capital/deposit/hisrec record. Numeric-or-string
// provider fields use flexibleID so decimal precision is preserved as strings and
// schema drift (number vs string) never breaks decoding.
type depositRow struct {
	ID               string          `json:"id"`
	Amount           string          `json:"amount"`
	Coin             string          `json:"coin"`
	Network          string          `json:"network"`
	Status           flexibleID      `json:"status"`
	TxID             string          `json:"txId"`
	InsertTime       int64           `json:"insertTime"`
	TransferType     flexibleID      `json:"transferType"`
	ConfirmTimes     string          `json:"confirmTimes"`
	TravelRuleStatus flexibleID      `json:"travelRuleStatus"`
	CompleteTime     flexibleID      `json:"completeTime"`
	RawJSON          json.RawMessage `json:"-"`
}

func (row *depositRow) UnmarshalJSON(data []byte) error {
	type wire depositRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = depositRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeDeposit(row depositRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	id := strings.TrimSpace(row.ID)
	if id == "" {
		return model.StateObservation{}, errors.New("deposit row has no id")
	}
	if row.InsertTime <= 0 || row.InsertTime > maxArchivedUnixMS {
		return model.StateObservation{}, errors.New("deposit row has invalid insert time")
	}
	status := strings.TrimSpace(string(row.Status))
	if status == "" {
		return model.StateObservation{}, errors.New("deposit row has no status")
	}

	// A deposit is mutable until fully credited: status, confirmation progress,
	// travel-rule review, and completion time all change over an object's life.
	state := struct {
		Status           string `json:"status"`
		ConfirmTimes     string `json:"confirmTimes"`
		TravelRuleStatus string `json:"travelRuleStatus"`
		CompleteTime     string `json:"completeTime"`
	}{
		Status:           status,
		ConfirmTimes:     strings.TrimSpace(row.ConfirmTimes),
		TravelRuleStatus: strings.TrimSpace(string(row.TravelRuleStatus)),
		CompleteTime:     strings.TrimSpace(string(row.CompleteTime)),
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)

	return model.StateObservation{
		Exchange:     model.ExchangeBinance,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		Stream:       "asset",
		ObjectType:   "deposit",
		ObjectID:     "deposit:" + id,
		// transferType (0 external / 1 internal) is not part of Status; it stays
		// in RawJSON. Status carries the provider deposit status code so a single
		// stream covers on-chain and internal deposits without a sub-stream.
		Status:           status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]),
		Asset:            strings.TrimSpace(row.Coin),
		Amount:           strings.TrimSpace(row.Amount),
		OccurredAt:       time.UnixMilli(row.InsertTime).UTC(),
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
