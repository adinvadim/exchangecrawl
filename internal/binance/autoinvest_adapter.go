package binance

import (
	"context"
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
	autoInvestHistoryPath = "/sapi/v1/lending/auto-invest/history/list"
	autoInvestRedeemPath  = "/sapi/v1/lending/auto-invest/redeem/history"
	autoInvestPageSize    = 100
	autoInvestMaxWindow   = 30 * 24 * time.Hour
	autoInvestPhaseTx     = "tx"
	autoInvestPhaseRedeem = "redeem"
)

// AutoInvestAdapter archives Binance Auto-Invest (DCA) history as immutable
// LedgerEntry records. Per-execution subscriptions and index-plan redemptions
// each carry a provider-assigned unique id and an outcome fixed at write time,
// so they never mutate once observed. Both endpoints live on the sapi host.
type AutoInvestAdapter struct {
	client *Adapter
}

var _ source.Adapter = (*AutoInvestAdapter)(nil)

// NewAutoInvest builds an Auto-Invest history adapter. It defaults to the sapi
// host (api.binance.com) and reuses the shared signed-request client so signing
// and retry are never re-implemented here.
func NewAutoInvest(options Options) (*AutoInvestAdapter, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		options.BaseURL = defaultSpotBaseURL
	}
	client, err := New(options)
	if err != nil {
		return nil, err
	}
	return &AutoInvestAdapter{client: client}, nil
}

func (a *AutoInvestAdapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *AutoInvestAdapter) CheckCredentials(account source.Account) source.CredentialStatus {
	return a.client.CheckCredentials(account)
}

func (a *AutoInvestAdapter) credentials(account source.Account) (string, string, error) {
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

func validateAutoInvestWindow(request source.PageRequest) error {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return errors.New("Binance Auto-Invest page requires a valid start and end time")
	}
	if request.End.Sub(request.Start) > autoInvestMaxWindow {
		return errors.New("Binance Auto-Invest page window must not exceed 30 days")
	}
	return nil
}

// FetchPage walks the two Auto-Invest history feeds as a single ordered stream:
// subscription executions first ("tx" phase), then index redemptions ("redeem"
// phase). Each phase is page-based (current/size, size capped at 100) and ends
// when the provider returns a short page, at which point the cursor advances to
// the next phase or reports Done. Page numbers strictly increase, so a replayed
// cursor can never reproduce an earlier one and the walk cannot loop forever.
func (a *AutoInvestAdapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validateAutoInvestWindow(request); err != nil {
		return source.Page{}, err
	}
	apiKey, secret, err := a.credentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	phase, page, err := parseAutoInvestCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}

	for {
		if phase == autoInvestPhaseTx {
			entries, full, err := a.fetchTxPage(ctx, request, page, apiKey, secret)
			if err != nil {
				return source.Page{}, err
			}
			if full {
				page++
				if len(entries) != 0 {
					return source.Page{Entries: entries, NextCursor: encodeAutoInvestCursor(autoInvestPhaseTx, page)}, nil
				}
				continue
			}
			phase, page = autoInvestPhaseRedeem, 1
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeAutoInvestCursor(autoInvestPhaseRedeem, page)}, nil
			}
			continue
		}

		entries, full, err := a.fetchRedeemPage(ctx, request, page, apiKey, secret)
		if err != nil {
			return source.Page{}, err
		}
		if full {
			page++
			if len(entries) != 0 {
				return source.Page{Entries: entries, NextCursor: encodeAutoInvestCursor(autoInvestPhaseRedeem, page)}, nil
			}
			continue
		}
		return source.Page{Entries: entries, Done: true}, nil
	}
}

func (a *AutoInvestAdapter) fetchTxPage(
	ctx context.Context,
	request source.PageRequest,
	page uint64,
	apiKey string,
	secret string,
) ([]model.LedgerEntry, bool, error) {
	query := autoInvestQuery(request, page, a.client.recvWindow)
	var response autoInvestTxResponse
	if err := a.client.signedGETOperation(ctx, autoInvestHistoryPath, query, apiKey, secret, "auto-invest history", &response); err != nil {
		return nil, false, err
	}
	observedAt := a.client.now().UTC()
	entries := make([]model.LedgerEntry, 0, len(response.List))
	for index, row := range response.List {
		entry, err := normalizeAutoInvestTx(row, request.Account, observedAt)
		if err != nil {
			return nil, false, fmt.Errorf("normalize Binance Auto-Invest execution row %d: %w", index, err)
		}
		if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, len(response.List) == autoInvestPageSize, nil
}

func (a *AutoInvestAdapter) fetchRedeemPage(
	ctx context.Context,
	request source.PageRequest,
	page uint64,
	apiKey string,
	secret string,
) ([]model.LedgerEntry, bool, error) {
	query := autoInvestQuery(request, page, a.client.recvWindow)
	var rows []autoInvestRedeemRow
	if err := a.client.signedGETOperation(ctx, autoInvestRedeemPath, query, apiKey, secret, "auto-invest redemptions", &rows); err != nil {
		return nil, false, err
	}
	observedAt := a.client.now().UTC()
	entries := make([]model.LedgerEntry, 0, len(rows))
	for index, row := range rows {
		entry, err := normalizeAutoInvestRedeem(row, request.Account, observedAt)
		if err != nil {
			return nil, false, fmt.Errorf("normalize Binance Auto-Invest redemption row %d: %w", index, err)
		}
		if entry.OccurredAt.Before(request.Start) || entry.OccurredAt.After(request.End) {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, len(rows) == autoInvestPageSize, nil
}

func autoInvestQuery(request source.PageRequest, page uint64, recvWindow int) url.Values {
	return url.Values{
		"startTime":  {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":    {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"current":    {strconv.FormatUint(page, 10)},
		"size":       {strconv.Itoa(autoInvestPageSize)},
		"recvWindow": {strconv.Itoa(recvWindow)},
	}
}

func encodeAutoInvestCursor(phase string, page uint64) string {
	return phase + ":" + strconv.FormatUint(page, 10)
}

func parseAutoInvestCursor(value string) (string, uint64, error) {
	if strings.TrimSpace(value) == "" {
		return autoInvestPhaseTx, 1, nil
	}
	if value != strings.TrimSpace(value) {
		return "", 0, errors.New("Binance Auto-Invest cursor is invalid")
	}
	phase, rawPage, found := strings.Cut(value, ":")
	if !found || (phase != autoInvestPhaseTx && phase != autoInvestPhaseRedeem) {
		return "", 0, errors.New("Binance Auto-Invest cursor is invalid")
	}
	page, err := strconv.ParseUint(rawPage, 10, 32)
	if err != nil || page == 0 {
		return "", 0, errors.New("Binance Auto-Invest cursor is invalid")
	}
	return phase, page, nil
}

type autoInvestTxResponse struct {
	Total int               `json:"total"`
	List  []autoInvestTxRow `json:"list"`
}

type autoInvestTxRow struct {
	ID                  flexibleID      `json:"id"`
	TargetAsset         string          `json:"targetAsset"`
	PlanType            string          `json:"planType"`
	PlanName            string          `json:"planName"`
	PlanID              flexibleID      `json:"planId"`
	TransactionDateTime int64           `json:"transactionDateTime"`
	TransactionStatus   string          `json:"transactionStatus"`
	FailedType          string          `json:"failedType"`
	SourceAsset         string          `json:"sourceAsset"`
	SourceAssetAmount   string          `json:"sourceAssetAmount"`
	TargetAssetAmount   string          `json:"targetAssetAmount"`
	SourceWallet        string          `json:"sourceWallet"`
	ExecutionPrice      string          `json:"executionPrice"`
	ExecutionType       string          `json:"executionType"`
	TransactionFee      string          `json:"transactionFee"`
	TransactionFeeUnit  string          `json:"transactionFeeUnit"`
	ExecutionAssetType  string          `json:"executionAssetType"`
	RawJSON             json.RawMessage `json:"-"`
}

func (row *autoInvestTxRow) UnmarshalJSON(data []byte) error {
	type wire autoInvestTxRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = autoInvestTxRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeAutoInvestTx(row autoInvestTxRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	id := strings.TrimSpace(string(row.ID))
	if id == "" {
		return model.LedgerEntry{}, errors.New("auto-invest execution row has no id")
	}
	if row.TransactionDateTime <= 0 || row.TransactionDateTime > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("auto-invest execution row has invalid occurrence time")
	}
	status := strings.ToUpper(strings.TrimSpace(row.TransactionStatus))
	if status == "" {
		return model.LedgerEntry{}, errors.New("auto-invest execution row has no transaction status")
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: "autoinvest-tx:" + id, Category: "autoinvest", Type: "execution",
		Asset: strings.TrimSpace(row.TargetAsset), Side: "BUY",
		Amount: strings.TrimSpace(row.TargetAssetAmount), Fee: strings.TrimSpace(row.TransactionFee),
		CashFlow: strings.TrimSpace(row.SourceAssetAmount), OrderID: strings.TrimSpace(string(row.PlanID)),
		Info: status, OccurredAt: time.UnixMilli(row.TransactionDateTime).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}

type autoInvestRedeemRow struct {
	IndexID            flexibleID      `json:"indexId"`
	IndexName          string          `json:"indexName"`
	RedemptionID       flexibleID      `json:"redemptionId"`
	Status             string          `json:"status"`
	Asset              string          `json:"asset"`
	Amount             string          `json:"amount"`
	RedemptionDateTime int64           `json:"redemptionDateTime"`
	TransactionFee     string          `json:"transactionFee"`
	TransactionFeeUnit string          `json:"transactionFeeUnit"`
	RawJSON            json.RawMessage `json:"-"`
}

func (row *autoInvestRedeemRow) UnmarshalJSON(data []byte) error {
	type wire autoInvestRedeemRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = autoInvestRedeemRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeAutoInvestRedeem(row autoInvestRedeemRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	id := strings.TrimSpace(string(row.RedemptionID))
	if id == "" {
		return model.LedgerEntry{}, errors.New("auto-invest redemption row has no redemption id")
	}
	if row.RedemptionDateTime <= 0 || row.RedemptionDateTime > maxArchivedUnixMS {
		return model.LedgerEntry{}, errors.New("auto-invest redemption row has invalid occurrence time")
	}
	status := strings.ToUpper(strings.TrimSpace(row.Status))
	if status == "" {
		return model.LedgerEntry{}, errors.New("auto-invest redemption row has no status")
	}
	return model.LedgerEntry{
		Exchange: model.ExchangeBinance, AccountID: account.ID, AccountLabel: account.Label,
		EntryID: "autoinvest-redeem:" + id, Category: "autoinvest", Type: "redemption",
		Asset: strings.TrimSpace(row.Asset), Side: "SELL",
		Amount: strings.TrimSpace(row.Amount), Fee: strings.TrimSpace(row.TransactionFee),
		OrderID: strings.TrimSpace(string(row.IndexID)), Info: status,
		OccurredAt: time.UnixMilli(row.RedemptionDateTime).UTC(), ObservedAt: observedAt.UTC(),
		RawJSON: append(json.RawMessage(nil), row.RawJSON...),
	}, nil
}
