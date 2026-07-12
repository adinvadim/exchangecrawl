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
	earnFlexibleSubscriptionPath = "/sapi/v1/simple-earn/flexible/history/subscriptionRecord"
	earnLockedSubscriptionPath   = "/sapi/v1/simple-earn/locked/history/subscriptionRecord"
	earnFlexibleRedemptionPath   = "/sapi/v1/simple-earn/flexible/history/redemptionRecord"
	earnLockedRedemptionPath     = "/sapi/v1/simple-earn/locked/history/redemptionRecord"

	earnPageSize  = 100
	earnMaxWindow = 30 * 24 * time.Hour
	// earnMaxPage bounds intra-call pagination so a misbehaving endpoint that
	// ignores the time window cannot page forever.
	earnMaxPage = 10_000
)

// earnPhase describes one of the four Simple Earn history endpoints the adapter
// walks in a fixed order. purchaseId/redeemId namespaces are per-endpoint, so a
// two-level identity prefix (action + product) is required for uniqueness.
type earnPhase struct {
	name    string // stable cursor token
	path    string
	action  string // "subscription" | "redemption"
	product string // "flexible" | "locked"
}

func (p earnPhase) objectPrefix() string {
	if p.action == "redemption" {
		return "earn-redeem:" + p.product + ":"
	}
	return "earn-sub:" + p.product + ":"
}

func (p earnPhase) objectType() string {
	return "earn_" + p.action
}

var earnPhases = []earnPhase{
	{name: "flex-sub", path: earnFlexibleSubscriptionPath, action: "subscription", product: "flexible"},
	{name: "locked-sub", path: earnLockedSubscriptionPath, action: "subscription", product: "locked"},
	{name: "flex-redeem", path: earnFlexibleRedemptionPath, action: "redemption", product: "flexible"},
	{name: "locked-redeem", path: earnLockedRedemptionPath, action: "redemption", product: "locked"},
}

// EarnAdapter archives Binance Simple Earn subscription and redemption history.
// It observes transitioning object states (PURCHASING -> SUCCESS/FAILED) as
// StateObservations and, via TerminalOrders, the terminal states as immutable
// LedgerEntries. It reuses the standard Binance signing, retry, and base-URL
// validation of *Adapter rather than re-implementing them.
type EarnAdapter struct {
	client *Adapter
}

// earnTerminalAdapter emits only terminal Simple Earn records as LedgerEntries.
type earnTerminalAdapter struct {
	earn *EarnAdapter
}

var (
	_ source.EventAdapter = (*EarnAdapter)(nil)
	_ source.Adapter      = (*earnTerminalAdapter)(nil)
)

// NewEarn builds an EarnAdapter that talks to the sapi host (api.binance.com by
// default, matching NewSpot).
func NewEarn(options Options) (*EarnAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &EarnAdapter{client: client}, nil
}

func (a *EarnAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *EarnAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

// TerminalOrders returns the immutable-history view of Simple Earn records.
func (a *EarnAdapter) TerminalOrders() source.Adapter {
	return &earnTerminalAdapter{earn: a}
}

func (a *earnTerminalAdapter) Exchange() model.Exchange {
	return a.earn.Exchange()
}

func (a *earnTerminalAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.earn.CheckCredentials(account)
}

func (a *EarnAdapter) credentials(account source.Account) (string, string, error) {
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

func validateEarnWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance Simple Earn page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > earnMaxWindow {
		return errors.New("Binance Simple Earn page window must not exceed 30 days")
	}
	return nil
}

// FetchEventPage walks the four Simple Earn history endpoints in a fixed order,
// paging each with a page-based cursor, and reports every record's current state.
func (a *EarnAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateEarnWindow(request); err != nil {
		return source.EventPage{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.EventPage{}, err
	}
	cursor, err := decodeEarnCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	index, err := phaseIndex(cursor.Phase)
	if err != nil {
		return source.EventPage{}, err
	}
	page := cursor.Page
	seen := map[string]struct{}{}
	for index < len(earnPhases) {
		phase := earnPhases[index]
		if err := markEarnCursor(seen, phase.name, page); err != nil {
			return source.EventPage{}, err
		}
		rows, err := a.fetchPhaseRows(ctx, request, phase, page, apiKey, secret)
		if err != nil {
			return source.EventPage{}, err
		}
		observations, err := normalizeEarnObservations(rows, phase, request.Account, a.client.now().UTC())
		if err != nil {
			return source.EventPage{}, err
		}
		filtered := observations[:0]
		for _, observation := range observations {
			if observation.OccurredAt.Before(request.Start) || observation.OccurredAt.After(request.End) {
				continue
			}
			filtered = append(filtered, observation)
		}
		if len(rows) == earnPageSize {
			page++
			if len(filtered) != 0 {
				return source.EventPage{Observations: filtered, NextCursor: encodeEarnCursor(earnCursor{Phase: phase.name, Page: page})}, nil
			}
			continue
		}
		index++
		page = 1
		if len(filtered) != 0 {
			done := index == len(earnPhases)
			result := source.EventPage{Observations: filtered, Done: done}
			if !done {
				result.NextCursor = encodeEarnCursor(earnCursor{Phase: earnPhases[index].name, Page: 1})
			}
			return result, nil
		}
	}
	return source.EventPage{Done: true}, nil
}

// FetchPage walks the same four endpoints but keeps only terminal records, whose
// stable per-endpoint identity makes repeated window upserts idempotent.
func (a *earnTerminalAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateEarnWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.earn.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	cursor, err := decodeEarnCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	index, err := phaseIndex(cursor.Phase)
	if err != nil {
		return source.Page{}, err
	}
	page := cursor.Page
	seen := map[string]struct{}{}
	for index < len(earnPhases) {
		phase := earnPhases[index]
		if err := markEarnCursor(seen, phase.name, page); err != nil {
			return source.Page{}, err
		}
		rows, err := a.earn.fetchPhaseRows(ctx, request, phase, page, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		observedAt := a.earn.client.now().UTC()
		entries := make([]model.LedgerEntry, 0, len(rows))
		for rowIndex, row := range rows {
			if !isTerminalEarnStatus(strings.TrimSpace(row.Status)) {
				continue
			}
			entry, err := normalizeEarnEntry(row, phase, request.Account, observedAt)
			if err != nil {
				return source.Page{}, fmt.Errorf("normalize Binance Simple Earn %s row %d: %w", phase.name, rowIndex, err)
			}
			if !entry.OccurredAt.Before(request.Start) && !entry.OccurredAt.After(request.End) {
				entries = append(entries, entry)
			}
		}
		if len(rows) == earnPageSize {
			page++
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeEarnCursor(earnCursor{Phase: phase.name, Page: page})}, nil
			}
			continue
		}
		index++
		page = 1
		if len(entries) != 0 {
			done := index == len(earnPhases)
			result := source.Page{Entries: entries, Done: done}
			if !done {
				result.NextCursor = encodeEarnCursor(earnCursor{Phase: earnPhases[index].name, Page: 1})
			}
			return result, nil
		}
	}
	return source.Page{Done: true}, nil
}

func (a *EarnAdapter) fetchPhaseRows(
	ctx context.Context,
	request source.PageRequest,
	phase earnPhase,
	page int,
	apiKey string,
	secret string,
) ([]earnRow, error) {
	query := url.Values{
		"current":    {strconv.Itoa(page)},
		"size":       {strconv.Itoa(earnPageSize)},
		"startTime":  {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":    {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"recvWindow": {strconv.Itoa(a.client.recvWindow)},
	}
	var response earnResult
	if err := a.client.signedGETOperation(ctx, phase.path, query, apiKey, secret, "simple earn "+phase.name, &response); err != nil {
		return nil, err
	}
	return response.Rows, nil
}

type earnResult struct {
	Rows  []earnRow  `json:"rows"`
	Total flexibleID `json:"total"`
}

type earnRow struct {
	Amount     string          `json:"amount"`
	Asset      string          `json:"asset"`
	Time       int64           `json:"time"`
	Status     string          `json:"status"`
	Type       string          `json:"type"`
	PurchaseID flexibleID      `json:"purchaseId"`
	RedeemID   flexibleID      `json:"redeemId"`
	PositionID flexibleID      `json:"positionId"`
	ProjectID  string          `json:"projectId"`
	LockPeriod string          `json:"lockPeriod"`
	RawJSON    json.RawMessage `json:"-"`
}

func (row *earnRow) UnmarshalJSON(data []byte) error {
	type wire earnRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = earnRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func earnRowID(row earnRow, phase earnPhase) string {
	if phase.action == "redemption" {
		return strings.TrimSpace(string(row.RedeemID))
	}
	return strings.TrimSpace(string(row.PurchaseID))
}

func normalizeEarnObservations(rows []earnRow, phase earnPhase, account source.Account, observedAt time.Time) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for index, row := range rows {
		observation, err := normalizeEarnObservation(row, phase, account, observedAt)
		if err != nil {
			return nil, fmt.Errorf("normalize Binance Simple Earn %s row %d: %w", phase.name, index, err)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func normalizeEarnObservation(row earnRow, phase earnPhase, account source.Account, observedAt time.Time) (model.StateObservation, error) {
	id := earnRowID(row, phase)
	if id == "" {
		return model.StateObservation{}, errors.New("earn row has no identifier")
	}
	status := strings.TrimSpace(row.Status)
	if status == "" {
		return model.StateObservation{}, errors.New("earn row has no status")
	}
	if row.Time <= 0 || row.Time > maxArchivedUnixMS {
		return model.StateObservation{}, errors.New("earn row has invalid occurrence time")
	}
	fingerprint := sha256.Sum256([]byte(status))
	return model.StateObservation{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		Stream: "earn", ObjectType: phase.objectType(), ObjectID: phase.objectPrefix() + id, Status: status,
		StateFingerprint: hex.EncodeToString(fingerprint[:]),
		Asset:            strings.TrimSpace(row.Asset), Amount: strings.TrimSpace(row.Amount),
		OccurredAt: time.UnixMilli(row.Time).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

func normalizeEarnEntry(row earnRow, phase earnPhase, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	observation, err := normalizeEarnObservation(row, phase, account, observedAt)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	side := "SUBSCRIBE"
	if phase.action == "redemption" {
		side = "REDEEM"
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: observation.ObjectID, Category: "earn", Type: phase.action,
		Asset: observation.Asset, Side: side, Amount: observation.Amount, CashFlow: observation.Amount,
		OrderID: earnRowID(row, phase), Info: observation.Status,
		OccurredAt: observation.OccurredAt, ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

// isTerminalEarnStatus reports whether a Simple Earn record has settled. Records
// in a transitioning status (PURCHASING, PENDING) are not yet immutable.
func isTerminalEarnStatus(status string) bool {
	switch status {
	case "SUCCESS", "FAILED", "PAID":
		return true
	default:
		return false
	}
}

type earnCursor struct {
	Phase string `json:"p"`
	Page  int    `json:"n"`
}

func encodeEarnCursor(cursor earnCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeEarnCursor(value string) (earnCursor, error) {
	if value == "" {
		return earnCursor{Phase: earnPhases[0].name, Page: 1}, nil
	}
	if value != strings.TrimSpace(value) {
		return earnCursor{}, errors.New("Binance Simple Earn cursor is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return earnCursor{}, errors.New("Binance Simple Earn cursor is invalid")
	}
	var cursor earnCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return earnCursor{}, errors.New("Binance Simple Earn cursor is invalid")
	}
	if cursor.Page < 1 {
		return earnCursor{}, errors.New("Binance Simple Earn cursor is invalid")
	}
	if _, err := phaseIndex(cursor.Phase); err != nil {
		return earnCursor{}, err
	}
	return cursor, nil
}

func phaseIndex(name string) (int, error) {
	for index, phase := range earnPhases {
		if phase.name == name {
			return index, nil
		}
	}
	return 0, errors.New("Binance Simple Earn cursor has an invalid phase")
}

// markEarnCursor records a visited (phase, page) so a bug or a window-ignoring
// endpoint that re-serves the same page cannot loop forever.
func markEarnCursor(seen map[string]struct{}, phase string, page int) error {
	if page > earnMaxPage {
		return fmt.Errorf("Binance Simple Earn %s exceeded %d pages", phase, earnMaxPage)
	}
	key := phase + ":" + strconv.Itoa(page)
	if _, duplicate := seen[key]; duplicate {
		return errors.New("Binance Simple Earn repeated cursor")
	}
	seen[key] = struct{}{}
	return nil
}
