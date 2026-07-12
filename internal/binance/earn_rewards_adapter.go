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

const (
	earnFlexibleRewardsPath = "/sapi/v1/simple-earn/flexible/history/rewardsRecord"
	earnLockedRewardsPath   = "/sapi/v1/simple-earn/locked/history/rewardsRecord"
	earnRewardsMaxWindow    = 7 * 24 * time.Hour
	earnRewardsPageSize     = 100
	earnLockedPhase         = "locked"
	earnFlexiblePhasePrefix = "flexible:"
)

// earnRewardsPhases is the pinned iteration order the cursor walks within one time
// window: the flexible endpoint has no pagination and demands a mandatory
// `type`, so it is queried once per required type before the locked endpoint is
// swept page by page. Locked is always last so a flexible phase never terminates
// the stream on its own.
var earnRewardsPhases = []string{
	earnFlexiblePhasePrefix + "BONUS",
	earnFlexiblePhasePrefix + "REALTIME",
	earnFlexiblePhasePrefix + "REWARDS",
	earnLockedPhase,
}

// EarnRewardsAdapter archives realized Simple Earn reward payouts (flexible and
// locked) as immutable ledger entries. It reuses the shared *Adapter client for
// signing and retry; it never calls a write endpoint.
type EarnRewardsAdapter struct {
	client *Adapter
}

var _ source.Adapter = (*EarnRewardsAdapter)(nil)

// NewEarnRewards builds the earn rewards adapter. The Simple Earn history lives
// on the sapi host, which shares the spot origin (api.binance.com), so an empty
// BaseURL defaults there rather than to the futures host.
func NewEarnRewards(options Options) (*EarnRewardsAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &EarnRewardsAdapter{client: client}, nil
}

func (a *EarnRewardsAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *EarnRewardsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *EarnRewardsAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateEarnRewardsWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeEarnRewardsCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	phaseIndex, err := earnRewardsPhaseIndex(cursor.Phase)
	if err != nil {
		return source.Page{}, err
	}
	lockedPage := cursor.Page
	for phaseIndex < len(earnRewardsPhases) {
		phase := earnRewardsPhases[phaseIndex]
		if phase == earnLockedPhase {
			if lockedPage < 1 {
				lockedPage = 1
			}
			entries, hasMore, err := a.fetchLockedRewards(ctx, request, lockedPage, apiKey, secret)
			if err != nil {
				return source.Page{}, err
			}
			if hasMore {
				next := encodeEarnRewardsCursor(earnRewardsCursor{Phase: earnLockedPhase, Page: lockedPage + 1})
				if next == request.Cursor {
					return source.Page{}, errors.New("Binance earn rewards cursor did not advance")
				}
				if len(entries) != 0 {
					return source.Page{Entries: entries, NextCursor: next}, nil
				}
				lockedPage++
				continue
			}
			return source.Page{Entries: entries, Done: true}, nil
		}
		rewardType := strings.TrimPrefix(phase, earnFlexiblePhasePrefix)
		entries, err := a.fetchFlexibleRewards(ctx, request, rewardType, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		phaseIndex++
		if len(entries) != 0 {
			// A flexible phase is never the last phase, so the locked sweep is
			// still pending: hand back a cursor rather than marking Done.
			return source.Page{
				Entries:    entries,
				NextCursor: encodeEarnRewardsCursor(earnRewardsCursor{Phase: earnRewardsPhases[phaseIndex]}),
			}, nil
		}
	}
	return source.Page{Done: true}, nil
}

func (a *EarnRewardsAdapter) credentials(account source.Account) (string, string, error) {
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

func validateEarnRewardsWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance earn rewards page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > earnRewardsMaxWindow {
		return errors.New("Binance earn rewards page window must not exceed 7 days")
	}
	return nil
}

type earnRewardsCursor struct {
	Phase string `json:"p,omitempty"`
	Page  int    `json:"c,omitempty"`
}

func encodeEarnRewardsCursor(cursor earnRewardsCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeEarnRewardsCursor(value string) (earnRewardsCursor, error) {
	if value == "" {
		return earnRewardsCursor{}, nil
	}
	if value != strings.TrimSpace(value) {
		return earnRewardsCursor{}, errors.New("Binance earn rewards cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return earnRewardsCursor{}, errors.New("Binance earn rewards cursor is invalid")
	}
	var cursor earnRewardsCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return earnRewardsCursor{}, errors.New("Binance earn rewards cursor is invalid")
	}
	if cursor.Page < 0 {
		return earnRewardsCursor{}, errors.New("Binance earn rewards cursor is invalid")
	}
	return cursor, nil
}

func earnRewardsPhaseIndex(phase string) (int, error) {
	if phase == "" {
		return 0, nil
	}
	for index, known := range earnRewardsPhases {
		if known == phase {
			return index, nil
		}
	}
	return 0, errors.New("Binance earn rewards cursor references an unknown phase")
}

type earnRewardsEnvelope[Row any] struct {
	Rows []Row `json:"rows"`
}

type earnFlexibleRewardRow struct {
	Asset     string          `json:"asset"`
	Rewards   string          `json:"rewards"`
	ProjectID string          `json:"projectId"`
	Type      string          `json:"type"`
	Time      int64           `json:"time"`
	RawJSON   json.RawMessage `json:"-"`
}

func (row *earnFlexibleRewardRow) UnmarshalJSON(data []byte) error {
	type wire earnFlexibleRewardRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = earnFlexibleRewardRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

type earnLockedRewardRow struct {
	PositionID flexibleID      `json:"positionId"`
	Asset      string          `json:"asset"`
	LockPeriod string          `json:"lockPeriod"`
	Amount     string          `json:"amount"`
	Time       int64           `json:"time"`
	RawJSON    json.RawMessage `json:"-"`
}

func (row *earnLockedRewardRow) UnmarshalJSON(data []byte) error {
	type wire earnLockedRewardRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = earnLockedRewardRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func (a *EarnRewardsAdapter) fetchFlexibleRewards(
	ctx context.Context,
	request source.PageRequest,
	rewardType string,
	apiKey string,
	secret string,
) ([]model.LedgerEntry, error) {
	query := url.Values{
		"type":       {rewardType},
		"startTime":  {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":    {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"size":       {strconv.Itoa(earnRewardsPageSize)},
		"recvWindow": {strconv.Itoa(a.client.recvWindow)},
	}
	var response earnRewardsEnvelope[earnFlexibleRewardRow]
	if err := a.client.signedGETOperation(ctx, earnFlexibleRewardsPath, query, apiKey, secret, "simple earn flexible rewards", &response); err != nil {
		return nil, err
	}
	observedAt := a.client.now().UTC()
	entries := make([]model.LedgerEntry, 0, len(response.Rows))
	for index, row := range response.Rows {
		entry, err := normalizeFlexibleReward(row, rewardType, request.Account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance flexible earn reward row %d: %w", index, err)
		}
		if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (a *EarnRewardsAdapter) fetchLockedRewards(
	ctx context.Context,
	request source.PageRequest,
	page int,
	apiKey string,
	secret string,
) ([]model.LedgerEntry, bool, error) {
	query := url.Values{
		"startTime":  {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":    {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"current":    {strconv.Itoa(page)},
		"size":       {strconv.Itoa(earnRewardsPageSize)},
		"recvWindow": {strconv.Itoa(a.client.recvWindow)},
	}
	var response earnRewardsEnvelope[earnLockedRewardRow]
	if err := a.client.signedGETOperation(ctx, earnLockedRewardsPath, query, apiKey, secret, "simple earn locked rewards", &response); err != nil {
		return nil, false, err
	}
	observedAt := a.client.now().UTC()
	entries := make([]model.LedgerEntry, 0, len(response.Rows))
	for index, row := range response.Rows {
		entry, err := normalizeLockedReward(row, request.Account, observedAt)
		if err != nil {
			return nil, false, fmt.Errorf("normalize Binance locked earn reward row %d: %w", index, err)
		}
		if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, len(response.Rows) == earnRewardsPageSize, nil
}

func normalizeFlexibleReward(row earnFlexibleRewardRow, rewardType string, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	asset := strings.TrimSpace(row.Asset)
	rewards := strings.TrimSpace(row.Rewards)
	projectID := strings.TrimSpace(row.ProjectID)
	if asset == "" || rewards == "" {
		return model.LedgerEntry{}, errors.New("flexible earn reward row has no asset or rewards amount")
	}
	if row.Time <= 0 || row.Time > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("flexible earn reward row has invalid occurrence time")
	}
	// No provider id exists for a reward row, so synthesize a deterministic
	// digest. The input order is pinned by TestEarnRewardDigestInputOrderIsPinned.
	digest := earnDigest(projectID, asset, rewardType, strconv.FormatInt(row.Time, 10), rewards)
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID:  "earn-reward:flexible:" + rewardType + ":" + digest,
		Category: "earn", Type: "flexible", Asset: asset,
		Amount: rewards, CashFlow: rewards, OrderID: projectID, Info: rewardType,
		OccurredAt: time.UnixMilli(row.Time).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func normalizeLockedReward(row earnLockedRewardRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	asset := strings.TrimSpace(row.Asset)
	amount := strings.TrimSpace(row.Amount)
	positionID := strings.TrimSpace(string(row.PositionID))
	lockPeriod := strings.TrimSpace(row.LockPeriod)
	if asset == "" || amount == "" {
		return model.LedgerEntry{}, errors.New("locked earn reward row has no asset or amount")
	}
	if row.Time <= 0 || row.Time > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("locked earn reward row has invalid occurrence time")
	}
	// No provider id exists for a reward row, so synthesize a deterministic
	// digest. The input order is pinned by TestEarnRewardDigestInputOrderIsPinned.
	digest := earnDigest(positionID, asset, lockPeriod, strconv.FormatInt(row.Time, 10), amount)
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID:  "earn-reward:locked:" + digest,
		Category: "earn", Type: "locked", Asset: asset,
		Amount: amount, CashFlow: amount, OrderID: positionID, Info: lockPeriod,
		OccurredAt: time.UnixMilli(row.Time).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

// earnDigest joins the pinned identity fields with a delimiter that cannot occur
// inside the decimal, asset, or id values and hashes them, yielding a stable,
// unique key for an otherwise id-less reward payout.
func earnDigest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}
