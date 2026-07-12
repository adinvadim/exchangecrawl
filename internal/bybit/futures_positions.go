package bybit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	positionListPath            = "/v5/position/list"
	futuresPositionCursorPrefix = "scope:"
	futuresPositionQueryLimit   = "200"
	futuresPositionPageLimit    = 1_000
)

// futuresPositionScope is one (category, settleCoin) slice of the position list.
// Bybit exposes derivatives positions per settlement coin, so a single snapshot
// poll must drain each scope independently.
type futuresPositionScope struct {
	category   string
	settleCoin string
}

// futuresPositionScopes is the fixed, ordered set of scopes the adapter drains.
// The order is stable so the phase cursor stays meaningful across polls.
var futuresPositionScopes = []futuresPositionScope{
	{category: "linear", settleCoin: "USDT"},
	{category: "linear", settleCoin: "USDC"},
	{category: "inverse"},
}

// FuturesPositionsAdapter observes current derivatives positions as mutable
// objects. It carries no history, so it ignores the request window and relies on
// StateFingerprint dedup to only archive real position changes.
type FuturesPositionsAdapter struct {
	base *Adapter
}

// NewFuturesPositions builds a positions adapter that reuses the standard Bybit
// signing, retry, and base-URL validation implementation.
func NewFuturesPositions(options Options) (*FuturesPositionsAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &FuturesPositionsAdapter{base: base}, nil
}

func (a *FuturesPositionsAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *FuturesPositionsAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchEventPage returns the current position snapshot for one scope per call,
// advancing through the fixed scope list with a phase cursor. Start/End are
// intentionally ignored: this is a current-state poll, not a backfill.
func (a *FuturesPositionsAdapter) FetchEventPage(ctx context.Context, request source.PageRequest) (source.EventPage, error) {
	if err := validateFuturesPositionRequest(request); err != nil {
		return source.EventPage{}, err
	}
	scopeIndex, err := parseFuturesPositionCursor(request.Cursor)
	if err != nil {
		return source.EventPage{}, err
	}
	for scopeIndex < len(futuresPositionScopes) {
		scope := futuresPositionScopes[scopeIndex]
		observations, err := a.drainScope(ctx, request.Account, scope)
		if err != nil {
			return source.EventPage{}, err
		}
		scopeIndex++
		if len(observations) != 0 {
			done := scopeIndex == len(futuresPositionScopes)
			page := source.EventPage{Observations: observations, Done: done}
			if !done {
				page.NextCursor = encodeFuturesPositionCursor(scopeIndex)
			}
			return page, nil
		}
	}
	return source.EventPage{Done: true}, nil
}

// drainScope reads every page of one scope, rejecting a repeated provider cursor
// so a misbehaving upstream cannot spin the poll forever.
func (a *FuturesPositionsAdapter) drainScope(
	ctx context.Context,
	account source.Account,
	scope futuresPositionScope,
) ([]model.StateObservation, error) {
	var observations []model.StateObservation
	cursor := ""
	seen := map[string]struct{}{}
	for pages := 0; pages < futuresPositionPageLimit; pages++ {
		result, observedAt, err := a.fetchScopePage(ctx, account, scope, cursor)
		if err != nil {
			return nil, err
		}
		scoped, err := normalizeFuturesPositions(result.List, scope, account, observedAt)
		if err != nil {
			return nil, err
		}
		observations = append(observations, scoped...)
		if result.NextPageCursor == "" {
			return observations, nil
		}
		if _, duplicate := seen[result.NextPageCursor]; duplicate {
			return nil, errors.New("Bybit futures position list repeated cursor")
		}
		seen[result.NextPageCursor] = struct{}{}
		cursor = result.NextPageCursor
	}
	return nil, fmt.Errorf("Bybit futures position list exceeded %d pages", futuresPositionPageLimit)
}

func (a *FuturesPositionsAdapter) fetchScopePage(
	ctx context.Context,
	account source.Account,
	scope futuresPositionScope,
	cursor string,
) (futuresPositionResult, time.Time, error) {
	query := url.Values{
		"category": {scope.category},
		"limit":    {futuresPositionQueryLimit},
	}
	if scope.settleCoin != "" {
		query.Set("settleCoin", scope.settleCoin)
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	result, observedAt, err := a.base.fetchResult(ctx, account, positionListPath, query)
	if err != nil {
		return futuresPositionResult{}, time.Time{}, err
	}
	var page futuresPositionResult
	if err := json.Unmarshal(result, &page); err != nil {
		return futuresPositionResult{}, time.Time{}, errors.New("decode Bybit futures position result")
	}
	return page, observedAt, nil
}

func validateFuturesPositionRequest(request source.PageRequest) error {
	if strings.TrimSpace(request.Account.ID) == "" {
		return errors.New("Bybit Connected Account id is required")
	}
	return nil
}

func parseFuturesPositionCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	if !strings.HasPrefix(cursor, futuresPositionCursorPrefix) {
		return 0, errors.New("Bybit futures position cursor has invalid source prefix")
	}
	index, err := strconv.Atoi(strings.TrimPrefix(cursor, futuresPositionCursorPrefix))
	if err != nil || index < 1 || index >= len(futuresPositionScopes) {
		return 0, errors.New("Bybit futures position cursor has invalid scope")
	}
	return index, nil
}

func encodeFuturesPositionCursor(scopeIndex int) string {
	return futuresPositionCursorPrefix + strconv.Itoa(scopeIndex)
}

type futuresPositionResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type futuresPositionRecord struct {
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	Size           string `json:"size"`
	AvgPrice       string `json:"avgPrice"`
	Leverage       string `json:"leverage"`
	PositionStatus string `json:"positionStatus"`
	PositionIdx    int    `json:"positionIdx"`
	PositionValue  string `json:"positionValue"`
	CumRealisedPnl string `json:"cumRealisedPnl"`
	UnrealisedPnl  string `json:"unrealisedPnl"`
	MarkPrice      string `json:"markPrice"`
	UpdatedTime    string `json:"updatedTime"`
}

func normalizeFuturesPositions(
	rows []json.RawMessage,
	scope futuresPositionScope,
	account source.Account,
	observedAt time.Time,
) ([]model.StateObservation, error) {
	observations := make([]model.StateObservation, 0, len(rows))
	for _, raw := range rows {
		observation, err := normalizeFuturesPosition(raw, scope, account, observedAt)
		if err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func normalizeFuturesPosition(
	raw json.RawMessage,
	scope futuresPositionScope,
	account source.Account,
	observedAt time.Time,
) (model.StateObservation, error) {
	var record futuresPositionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.StateObservation{}, errors.New("decode Bybit futures position")
	}
	symbol := strings.TrimSpace(record.Symbol)
	if symbol == "" {
		return model.StateObservation{}, errors.New("Bybit futures position has no symbol")
	}
	occurredAt, err := parseFuturesPositionTimestamp(record.UpdatedTime)
	if err != nil {
		return model.StateObservation{}, err
	}
	objectID := fmt.Sprintf("futures-position:%s:%s:%d", scope.category, symbol, record.PositionIdx)
	return model.StateObservation{
		Exchange:         model.ExchangeBybit,
		AccountID:        account.ID,
		AccountLabel:     account.Label,
		Stream:           "futures",
		ObjectType:       "position",
		ObjectID:         objectID,
		Status:           record.PositionStatus,
		StateFingerprint: futuresPositionFingerprint(record),
		Symbol:           symbol,
		Asset:            scope.settleCoin,
		Amount:           record.Size,
		OccurredAt:       occurredAt,
		ObservedAt:       observedAt.UTC(),
		RawJSON:          append(json.RawMessage(nil), raw...),
	}, nil
}

// futuresPositionFingerprint hashes only the durable position identity fields.
// markPrice and unrealisedPnl are deliberately excluded so a live-moving mark
// price does not manufacture a new observation on every poll; those fields still
// land in RawJSON.
func futuresPositionFingerprint(record futuresPositionRecord) string {
	fields := []string{
		record.Side,
		record.Size,
		record.AvgPrice,
		record.Leverage,
		record.PositionStatus,
		record.CumRealisedPnl,
	}
	hash := sha256.New()
	var length [8]byte
	for _, field := range fields {
		binary.LittleEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func parseFuturesPositionTimestamp(raw string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, errors.New("Bybit futures position updatedTime is invalid")
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var _ source.EventAdapter = (*FuturesPositionsAdapter)(nil)
