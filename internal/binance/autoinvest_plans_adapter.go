package binance

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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

const autoInvestPlanListPath = "/sapi/v1/lending/auto-invest/plan/list"

// autoInvestPlanTypes is the fixed, ordered set of plan categories the Binance
// Auto-Invest plan/list endpoint accepts. The adapter walks them one per page.
var autoInvestPlanTypes = []string{"SINGLE", "PORTFOLIO", "INDEX"}

// AutoInvestPlansAdapter snapshots mutable Binance Auto-Invest plan objects
// (ONGOING/PAUSED/REMOVED with running PnL) into StateObservations. It reuses
// the shared *Adapter for signing, retry, and base-URL validation; it never
// re-implements signing and only calls the read-only plan/list endpoint.
type AutoInvestPlansAdapter struct {
	client *Adapter
}

var _ source.EventAdapter = (*AutoInvestPlansAdapter)(nil)

// NewAutoInvestPlans builds an Auto-Invest plans adapter. The plan/list endpoint
// lives on the sapi host, so the base URL defaults to the spot API origin rather
// than the futures origin.
func NewAutoInvestPlans(options Options) (*AutoInvestPlansAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &AutoInvestPlansAdapter{client: client}, nil
}

func (a *AutoInvestPlansAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *AutoInvestPlansAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *AutoInvestPlansAdapter) credentials(account source.Account) (string, string, error) {
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

// FetchEventPage walks the planType values in a fixed order, one planType per
// page. There is no time window and no provider pagination: each planType is a
// single signed request. The cursor names the next planType to fetch and always
// advances, so the walk terminates after at most len(autoInvestPlanTypes) pages.
func (a *AutoInvestPlansAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	index, err := decodeAutoInvestPlansCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	for index < len(autoInvestPlanTypes) {
		planType := autoInvestPlanTypes[index]
		rows, err := a.fetchPlans(ctx, planType, apiKey, secret)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeAutoInvestPlans(rows, request.Account, a.client.now().UTC())
		if err != nil {
			return source.EventPage{}, err
		}
		index++
		if len(observations) != 0 {
			page := source.EventPage{Observations: observations, Done: index == len(autoInvestPlanTypes)}
			if !page.Done {
				page.NextCursor = encodeAutoInvestPlansCursor(index)
			}
			return page, nil
		}
	}
	return source.EventPage{Done: true}, nil
}

func (a *AutoInvestPlansAdapter) fetchPlans(ctx context.Context, planType, apiKey, secret string) ([]autoInvestPlanRow, error) {
	query := url.Values{
		"planType":   {planType},
		"recvWindow": {strconv.Itoa(a.client.recvWindow)},
	}
	var response autoInvestPlanListResponse
	if err := a.client.signedGETOperation(ctx, autoInvestPlanListPath, query, apiKey, secret, "auto-invest plan list", &response); err != nil {
		return nil, err
	}
	return response.Plan, nil
}

type autoInvestPlansCursor struct {
	PlanType string `json:"t"`
}

func encodeAutoInvestPlansCursor(index int) string {
	data, _ := json.Marshal(autoInvestPlansCursor{PlanType: autoInvestPlanTypes[index]})
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeAutoInvestPlansCursor resolves a cursor to the index of the next planType
// to fetch. An empty cursor starts at the first planType. Unknown or malformed
// cursors are rejected so a corrupt cursor can never drive an unbounded walk.
func decodeAutoInvestPlansCursor(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	if value != strings.TrimSpace(value) {
		return 0, errors.New("Binance Auto-Invest plans cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, errors.New("Binance Auto-Invest plans cursor is invalid")
	}
	var cursor autoInvestPlansCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return 0, errors.New("Binance Auto-Invest plans cursor is invalid")
	}
	for index, planType := range autoInvestPlanTypes {
		if planType == cursor.PlanType {
			return index, nil
		}
	}
	return 0, errors.New("Binance Auto-Invest plans cursor references an unknown plan type")
}

type autoInvestPlanListResponse struct {
	Plan []autoInvestPlanRow `json:"plan"`
}

type autoInvestPlanRow struct {
	PlanID                flexibleID      `json:"planId"`
	PlanType              string          `json:"planType"`
	Status                string          `json:"status"`
	TargetAsset           string          `json:"targetAsset"`
	SourceAsset           string          `json:"sourceAsset"`
	TotalInvestedInUSD    string          `json:"totalInvestedInUSD"`
	PlanValueInUSD        string          `json:"planValueInUSD"`
	PnlInUSD              string          `json:"pnlInUSD"`
	ROI                   string          `json:"roi"`
	NextExecutionDateTime int64           `json:"nextExecutionDateTime"`
	LastUpdatedDateTime   int64           `json:"lastUpdatedDateTime"`
	RawJSON               json.RawMessage `json:"-"`
}

func (row *autoInvestPlanRow) UnmarshalJSON(data []byte) error {
	type wire autoInvestPlanRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = autoInvestPlanRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeAutoInvestPlans(rows []autoInvestPlanRow, account source.Account, observedAt time.Time) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeAutoInvestPlan(row, account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance Auto-Invest plan row %d: %w", index, err)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func normalizeAutoInvestPlan(row autoInvestPlanRow, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	planID := strings.TrimSpace(string(row.PlanID))
	status := strings.TrimSpace(row.Status)
	if planID == "" {
		return model.StateObservation{}, errors.New("plan row has no plan id")
	}
	if status == "" {
		return model.StateObservation{}, errors.New("plan row has no status")
	}
	if row.LastUpdatedDateTime <= 0 || row.LastUpdatedDateTime > maxArchivedUnixMS {
		return model.StateObservation{}, errors.New("plan row has invalid last-updated time")
	}
	state := struct {
		Status                string `json:"status"`
		TotalInvestedInUSD    string `json:"totalInvestedInUSD"`
		PlanValueInUSD        string `json:"planValueInUSD"`
		PnlInUSD              string `json:"pnlInUSD"`
		ROI                   string `json:"roi"`
		NextExecutionDateTime int64  `json:"nextExecutionDateTime"`
	}{
		Status:                status,
		TotalInvestedInUSD:    strings.TrimSpace(row.TotalInvestedInUSD),
		PlanValueInUSD:        strings.TrimSpace(row.PlanValueInUSD),
		PnlInUSD:              strings.TrimSpace(row.PnlInUSD),
		ROI:                   strings.TrimSpace(row.ROI),
		NextExecutionDateTime: row.NextExecutionDateTime,
	}
	stateJSON, _ := json.Marshal(state)
	fingerprint := sha256.Sum256(stateJSON)
	return model.StateObservation{
		Exchange:         model.ExchangeBinance,
		AccountID:        account.ID,
		AccountLabel:     account.Label,
		Stream:           "autoinvest",
		ObjectType:       "autoinvest-plan",
		ObjectID:         "autoinvest-plan:" + planID,
		Status:           status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]),
		Asset:            strings.TrimSpace(row.TargetAsset),
		Amount:           strings.TrimSpace(row.TotalInvestedInUSD),
		OccurredAt:       time.UnixMilli(row.LastUpdatedDateTime).UTC(),
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
