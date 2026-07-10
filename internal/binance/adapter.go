package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

const (
	defaultBaseURL      = "https://fapi.binance.com"
	defaultAPIKeyEnv    = "BINANCE_API_KEY"
	defaultAPISecretEnv = "BINANCE_API_SECRET"
	baseURLEnv          = "BINANCE_FUTURES_BASE_URL"
	incomePath          = "/fapi/v1/income"
	pageLimit           = 1000
	defaultRecvWindow   = 5000
	maxResponseBytes    = 8 << 20
	maxRetries          = 3
	baseRetryDelay      = time.Second
)

type Options struct {
	BaseURL    string
	HTTPClient *http.Client
	Now        func() time.Time
	LookupEnv  func(string) (string, bool)
	Sleep      func(context.Context, time.Duration) error
	RecvWindow int
}

type Adapter struct {
	baseURL    *url.URL
	httpClient *http.Client
	now        func() time.Time
	lookupEnv  func(string) (string, bool)
	sleep      func(context.Context, time.Duration) error
	recvWindow int
}

var _ source.Adapter = (*Adapter)(nil)

// APIError is a provider response stripped of request URLs and credentials.
type APIError struct {
	StatusCode int    `json:"status_code"`
	Code       int    `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Binance API error %d (HTTP %d)", e.Code, e.StatusCode)
	}
	return fmt.Sprintf("Binance API error %d (HTTP %d): %s", e.Code, e.StatusCode, e.Message)
}

type RequestError struct {
	Operation string
}

func (e *RequestError) Error() string {
	return "Binance " + e.Operation + " request failed"
}

func New(options Options) (*Adapter, error) {
	lookupEnv := options.LookupEnv
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}

	baseURL := strings.TrimSpace(options.BaseURL)
	if baseURL == "" {
		if configured, ok := lookupEnv(baseURLEnv); ok && strings.TrimSpace(configured) != "" {
			baseURL = strings.TrimSpace(configured)
		} else {
			baseURL = defaultBaseURL
		}
	}
	parsedBaseURL, err := parseBaseURL(baseURL, options.HTTPClient != nil)
	if err != nil {
		return nil, err
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	clientCopy := *httpClient
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	httpClient = &clientCopy
	now := options.Now
	if now == nil {
		now = time.Now
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	recvWindow := options.RecvWindow
	if recvWindow == 0 {
		recvWindow = defaultRecvWindow
	}
	if recvWindow < 1 {
		return nil, errors.New("Binance receive window must be positive")
	}

	return &Adapter{
		baseURL:    parsedBaseURL,
		httpClient: httpClient,
		now:        now,
		lookupEnv:  lookupEnv,
		sleep:      sleep,
		recvWindow: recvWindow,
	}, nil
}

func (a *Adapter) Exchange() model.Exchange {
	return model.ExchangeBinance
}

func (a *Adapter) CheckCredentials(account source.Account) source.CredentialStatus {
	missing := make([]string, 0, 2)
	if _, ok := a.credential(account.APIKeyEnv, defaultAPIKeyEnv); !ok {
		missing = append(missing, envName(account.APIKeyEnv, defaultAPIKeyEnv))
	}
	if _, ok := a.credential(account.APISecretEnv, defaultAPISecretEnv); !ok {
		missing = append(missing, envName(account.APISecretEnv, defaultAPISecretEnv))
	}
	return source.CredentialStatus{Ready: len(missing) == 0, Missing: missing}
}

func (a *Adapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if request.Start.IsZero() || request.End.IsZero() || request.End.Before(request.Start) {
		return source.Page{}, errors.New("Binance income page requires a valid start and end time")
	}
	pageNumber, err := parseCursor(request.Cursor)
	if err != nil {
		return source.Page{}, err
	}
	apiKey, ok := a.credential(request.Account.APIKeyEnv, defaultAPIKeyEnv)
	if !ok {
		return source.Page{}, fmt.Errorf("Binance credential environment variable %s is missing", envName(request.Account.APIKeyEnv, defaultAPIKeyEnv))
	}
	secret, ok := a.credential(request.Account.APISecretEnv, defaultAPISecretEnv)
	if !ok {
		return source.Page{}, fmt.Errorf("Binance credential environment variable %s is missing", envName(request.Account.APISecretEnv, defaultAPISecretEnv))
	}

	query := url.Values{}
	query.Set("startTime", strconv.FormatInt(request.Start.UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(request.End.UnixMilli(), 10))
	query.Set("page", strconv.FormatUint(pageNumber, 10))
	query.Set("limit", strconv.Itoa(pageLimit))
	query.Set("recvWindow", strconv.Itoa(a.recvWindow))

	var rows []incomeRow
	if err := a.signedGET(ctx, incomePath, query, apiKey, secret, &rows); err != nil {
		return source.Page{}, err
	}

	observedAt := a.now().UTC()
	entries := make([]model.LedgerEntry, 0, len(rows))
	for index, row := range rows {
		entry, err := normalizeIncome(row, request.Account, observedAt)
		if err != nil {
			return source.Page{}, fmt.Errorf("normalize Binance income row %d: %w", index, err)
		}
		entries = append(entries, entry)
	}

	result := source.Page{Entries: entries, Done: len(rows) < pageLimit}
	if !result.Done {
		result.NextCursor = strconv.FormatUint(pageNumber+1, 10)
	}
	return result, nil
}

func (a *Adapter) signedGET(
	ctx context.Context,
	path string,
	query url.Values,
	apiKey string,
	secret string,
	out any,
) error {
	for attempt := 0; ; attempt++ {
		attemptedAt := a.now().UTC()
		query.Set("timestamp", strconv.FormatInt(attemptedAt.UnixMilli(), 10))
		unsignedQuery := query.Encode()
		signature := sign(secret, unsignedQuery)
		endpoint := *a.baseURL
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
		endpoint.RawQuery = unsignedQuery + "&signature=" + signature

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return &RequestError{Operation: "income"}
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "binancecrawl/0")
		request.Header.Set("X-MBX-APIKEY", apiKey)

		response, err := a.httpClient.Do(request)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return &RequestError{Operation: "income"}
		}

		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		_ = response.Body.Close()
		var responseErr error
		switch {
		case readErr != nil || len(body) > maxResponseBytes:
			responseErr = &RequestError{Operation: "income response"}
		case response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices:
			responseErr = decodeAPIError(response.StatusCode, body, endpoint.String(), endpoint.RawQuery, apiKey, secret, signature)
		case json.Unmarshal(body, out) != nil:
			var providerError apiErrorResponse
			if json.Unmarshal(body, &providerError) == nil && providerError.Code != 0 {
				responseErr = newAPIError(response.StatusCode, providerError, endpoint.String(), endpoint.RawQuery, apiKey, secret, signature)
			} else {
				responseErr = &RequestError{Operation: "income response decode"}
			}
		default:
			return nil
		}

		if attempt >= maxRetries || !retryableResponse(response.StatusCode, responseErr) {
			return responseErr
		}
		if err := a.sleep(ctx, retryDelay(response.Header.Get("Retry-After"), attemptedAt, attempt)); err != nil {
			return err
		}
	}
}

type apiErrorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func decodeAPIError(statusCode int, body []byte, sensitive ...string) error {
	var response apiErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return &APIError{StatusCode: statusCode, Message: "unexpected error response"}
	}
	return newAPIError(statusCode, response, sensitive...)
}

func newAPIError(statusCode int, response apiErrorResponse, sensitive ...string) error {
	return &APIError{
		StatusCode: statusCode,
		Code:       response.Code,
		Message:    redact(response.Msg, sensitive...),
	}
}

func redact(message string, sensitive ...string) string {
	for _, value := range sensitive {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return ' '
		}
		return r
	}, message)
	message = strings.TrimSpace(message)
	if len(message) > 1024 {
		message = message[:1024]
	}
	return message
}

func retryableResponse(statusCode int, err error) bool {
	if statusCode == http.StatusTeapot || statusCode == http.StatusTooManyRequests || statusCode >= 500 && statusCode <= 599 {
		return true
	}
	var apiError *APIError
	return errors.As(err, &apiError) && apiError.Code == -1003
}

func retryDelay(header string, now time.Time, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(header); err == nil {
		if delay := retryAt.Sub(now); delay > 0 {
			return delay
		}
		return 0
	}
	return baseRetryDelay << attempt
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type flexibleID string

func (id *flexibleID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*id = ""
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		*id = flexibleID(value)
		return nil
	}
	var value json.Number
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*id = flexibleID(value.String())
	return nil
}

type incomeRow struct {
	Symbol     string          `json:"symbol"`
	IncomeType string          `json:"incomeType"`
	Income     string          `json:"income"`
	Asset      string          `json:"asset"`
	Info       string          `json:"info"`
	Time       int64           `json:"time"`
	TranID     flexibleID      `json:"tranId"`
	TradeID    flexibleID      `json:"tradeId"`
	RawJSON    json.RawMessage `json:"-"`
}

func (row *incomeRow) UnmarshalJSON(data []byte) error {
	type wire incomeRow
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*row = incomeRow(decoded)
	row.RawJSON = append(row.RawJSON[:0], data...)
	return nil
}

func normalizeIncome(row incomeRow, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	entryID := strings.TrimSpace(string(row.TranID))
	if entryID == "" {
		return model.LedgerEntry{}, errors.New("income row has no transaction id")
	}
	if row.Time <= 0 {
		return model.LedgerEntry{}, fmt.Errorf("income transaction %s has no occurrence time", entryID)
	}

	incomeType := strings.TrimSpace(row.IncomeType)
	entry := model.LedgerEntry{
		Exchange:     model.ExchangeBinance,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      entryID,
		Symbol:       strings.TrimSpace(row.Symbol),
		Category:     "income",
		Type:         incomeType,
		Asset:        strings.TrimSpace(row.Asset),
		Amount:       strings.TrimSpace(row.Income),
		CashFlow:     strings.TrimSpace(row.Income),
		TradeID:      strings.TrimSpace(string(row.TradeID)),
		Info:         strings.TrimSpace(row.Info),
		OccurredAt:   time.UnixMilli(row.Time).UTC(),
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), row.RawJSON...),
	}
	switch strings.ToUpper(incomeType) {
	case "COMMISSION":
		entry.Fee = entry.Amount
	case "FUNDING_FEE":
		entry.Funding = entry.Amount
	}
	return entry, nil
}

func parseCursor(cursor string) (uint64, error) {
	if strings.TrimSpace(cursor) == "" {
		return 1, nil
	}
	if cursor != strings.TrimSpace(cursor) {
		return 0, errors.New("Binance page cursor must be a positive decimal integer")
	}
	page, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil || page == 0 {
		return 0, errors.New("Binance page cursor must be a positive decimal integer")
	}
	return page, nil
}

func (a *Adapter) credential(configuredName, defaultName string) (string, bool) {
	value, ok := a.lookupEnv(envName(configuredName, defaultName))
	value = strings.TrimSpace(value)
	return value, ok && value != ""
}

func envName(configured, fallback string) string {
	if value := strings.TrimSpace(configured); value != "" {
		return value
	}
	return fallback
}

func parseBaseURL(raw string, allowLoopbackHTTP bool) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Binance futures base URL must be an origin URL")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && allowLoopbackHTTP && isLoopbackHost(parsed.Hostname())) {
		return nil, errors.New("Binance futures base URL must use HTTPS")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return nil, errors.New("Binance futures base URL must not contain a path")
	}
	parsed.Path = ""
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
