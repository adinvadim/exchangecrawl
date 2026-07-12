package bybit

import (
	"context"
	"crypto/sha256"
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
	earnYieldPath      = "/v5/earn/hold-to-earn/yield-history"
	earnYieldPageLimit = 10_000
)

// EarnYieldAdapter archives Bybit Hold-to-Earn yield distributions.
//
// Each row is an already-distributed, immutable yield fact, so the stream is a
// pure LedgerEntry source. Rows carry no provider id, so the EntryID is a
// deterministic SHA-256 digest over the normalized fields (see
// earnYieldEntryID). Bybit only serves roughly the last three months of yield
// history: yield older than the InitialLookback window is unrecoverable.
type EarnYieldAdapter struct {
	base *Adapter
}

// NewEarnYield builds an EarnYieldAdapter that shares the standard Bybit
// signing, retry, and base-URL validation implementation.
func NewEarnYield(options Options) (*EarnYieldAdapter, error) {
	base, err := New(options)
	if err != nil {
		return nil, err
	}
	return &EarnYieldAdapter{base: base}, nil
}

func (a *EarnYieldAdapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *EarnYieldAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.base.CheckCredentials(account)
}

// FetchPage walks the provider cursor to completion within the request window,
// rejecting repeated cursors so a misbehaving endpoint cannot loop forever.
func (a *EarnYieldAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	cursor := request.Cursor
	seen := map[string]struct{}{}
	if cursor != "" {
		seen[cursor] = struct{}{}
	}
	var entries []model.LedgerEntry
	for pages := 0; pages < earnYieldPageLimit; pages++ {
		page, observedAt, err := a.fetchYieldPage(ctx, request, cursor)
		if err != nil {
			return source.Page{}, err
		}
		for _, raw := range page.List {
			entry, err := normalizeEarnYield(raw, request.Account, observedAt)
			if err != nil {
				return source.Page{}, err
			}
			entries = append(entries, entry)
		}
		if page.NextPageCursor == "" {
			return source.Page{Entries: entries, Done: true}, nil
		}
		if _, duplicate := seen[page.NextPageCursor]; duplicate {
			return source.Page{}, errors.New("Bybit earn yield history repeated cursor")
		}
		seen[page.NextPageCursor] = struct{}{}
		cursor = page.NextPageCursor
	}
	return source.Page{}, fmt.Errorf("Bybit earn yield history exceeded %d pages", earnYieldPageLimit)
}

func (a *EarnYieldAdapter) fetchYieldPage(
	ctx context.Context,
	request source.PageRequest,
	cursor string,
) (earnYieldResult, time.Time, error) {
	query := url.Values{
		"startTime": {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":     {"50"},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	result, observedAt, err := a.base.fetchResult(ctx, request.Account, earnYieldPath, query)
	if err != nil {
		return earnYieldResult{}, time.Time{}, err
	}
	var page earnYieldResult
	if err := json.Unmarshal(result, &page); err != nil {
		return earnYieldResult{}, time.Time{}, errors.New("decode Bybit earn yield result")
	}
	return page, observedAt, nil
}

type earnYieldResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type earnYieldRecord struct {
	CoinName        string `json:"coin"`
	YieldCoinName   string `json:"yieldCoin"`
	CreatedAt       string `json:"createdAt"`
	EffectiveAmount string `json:"effectiveAmount"`
	Pnl             string `json:"pnl"`
	Apy             string `json:"apy"`
}

func normalizeEarnYield(raw json.RawMessage, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	var record earnYieldRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.LedgerEntry{}, errors.New("decode Bybit earn yield entry")
	}
	occurredAt, err := parseEarnYieldTimestamp(record.CreatedAt)
	if err != nil {
		return model.LedgerEntry{}, err
	}
	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      earnYieldEntryID(record),
		Symbol:       record.CoinName,
		Category:     "earn",
		Type:         "yield",
		Asset:        record.YieldCoinName,
		Amount:       record.Pnl,
		CashFlow:     record.Pnl,
		Balance:      record.EffectiveAmount,
		Info:         record.Apy,
		OccurredAt:   occurredAt,
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}, nil
}

// earnYieldEntryID synthesizes a stable identity for a row that carries no
// provider id. The digest is over the exact pipe-joined concatenation
//
//	coin | yieldCoin | createdAt | effectiveAmount | pnl | apy
//
// using the raw provider field values. The field set and order are pinned by a
// test fixture so the identity never silently drifts.
func earnYieldEntryID(record earnYieldRecord) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		record.CoinName,
		record.YieldCoinName,
		record.CreatedAt,
		record.EffectiveAmount,
		record.Pnl,
		record.Apy,
	}, "|")))
	return fmt.Sprintf("earn-yield:%x", digest[:])
}

func parseEarnYieldTimestamp(raw string) (time.Time, error) {
	timestampMillis, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || timestampMillis < 0 || timestampMillis > maxArchivedUnixMS {
		return time.Time{}, errors.New("Bybit earn yield createdAt is invalid")
	}
	return time.UnixMilli(timestampMillis).UTC(), nil
}

var _ source.Adapter = (*EarnYieldAdapter)(nil)
