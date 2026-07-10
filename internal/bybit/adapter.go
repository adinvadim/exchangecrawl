package bybit

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
	defaultBaseURL       = "https://api.bybit.com"
	defaultRecvWindow    = 5000
	transactionLogPath   = "/v5/account/transaction-log"
	maxResponseBodyBytes = 8 << 20
	maxRequestRetries    = 3
	baseRetryDelay       = 250 * time.Millisecond
)

// Options supplies process dependencies. Nil functions and zero values use
// production defaults; tests can replace every source of I/O and time.
type Options struct {
	BaseURL    string
	HTTPClient *http.Client
	Now        func() time.Time
	LookupEnv  func(string) (string, bool)
	ReadFile   func(string) ([]byte, error)
	Sleep      func(context.Context, time.Duration) error
	RecvWindow int
}

type Adapter struct {
	baseURL    *url.URL
	httpClient *http.Client
	now        func() time.Time
	lookupEnv  func(string) (string, bool)
	readFile   func(string) ([]byte, error)
	sleep      func(context.Context, time.Duration) error
	recvWindow int
}

type APIError struct {
	Code    int
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Bybit API error %d", e.Code)
	}
	return fmt.Sprintf("Bybit API error %d: %s", e.Code, e.Message)
}

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("Bybit HTTP status %d", e.StatusCode)
}

func New(options Options) (*Adapter, error) {
	lookupEnv := options.LookupEnv
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}

	baseURL := strings.TrimSpace(options.BaseURL)
	if baseURL == "" {
		if configured, ok := lookupEnv("BYBIT_API_BASE_URL"); ok {
			baseURL = strings.TrimSpace(configured)
		}
	}
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	allowTestHTTP := options.HTTPClient != nil
	parsedBaseURL, err := parseBaseURL(baseURL, allowTestHTTP)
	if err != nil {
		return nil, err
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	httpClientCopy := *httpClient
	httpClientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	readFile := options.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	recvWindow := options.RecvWindow
	if recvWindow <= 0 {
		recvWindow = defaultRecvWindow
	}

	return &Adapter{
		baseURL:    parsedBaseURL,
		httpClient: &httpClientCopy,
		now:        now,
		lookupEnv:  lookupEnv,
		readFile:   readFile,
		sleep:      sleep,
		recvWindow: recvWindow,
	}, nil
}

func (a *Adapter) Exchange() model.Exchange {
	return model.ExchangeBybit
}

func (a *Adapter) CheckCredentials(account source.Account) source.CredentialStatus {
	names := credentialEnvNames(account)
	var missing []string
	if !a.envPresent(names.apiKey) {
		missing = append(missing, names.apiKey)
	}
	if !a.envPresent(names.secret) && (names.privateKeyPath == "" || !a.envPresent(names.privateKeyPath)) {
		missing = append(missing, names.secret)
		if names.privateKeyPath != "" {
			missing = append(missing, names.privateKeyPath)
		}
	}
	return source.CredentialStatus{Ready: len(missing) == 0, Missing: missing}
}

func (a *Adapter) FetchPage(ctx context.Context, request source.PageRequest) (source.Page, error) {
	if err := validatePageRequest(request); err != nil {
		return source.Page{}, err
	}
	credentials, err := a.resolveCredentials(request.Account)
	if err != nil {
		return source.Page{}, err
	}
	query := url.Values{
		"accountType": {"UNIFIED"},
		"startTime":   {strconv.FormatInt(request.Start.UnixMilli(), 10)},
		"endTime":     {strconv.FormatInt(request.End.UnixMilli(), 10)},
		"limit":       {"50"},
	}
	if request.Cursor != "" {
		query.Set("cursor", request.Cursor)
	}
	encodedQuery := query.Encode()

	for attempt := 0; attempt <= maxRequestRetries; attempt++ {
		observedAt := a.now().UTC()
		timestamp := strconv.FormatInt(observedAt.UnixMilli(), 10)
		payload := timestamp + credentials.apiKey + strconv.Itoa(a.recvWindow) + encodedQuery
		signature, signType, err := credentials.signer.sign(payload)
		if err != nil {
			return source.Page{}, err
		}

		requestURL := *a.baseURL
		requestURL.Path = transactionLogPath
		requestURL.RawQuery = encodedQuery
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
		if err != nil {
			return source.Page{}, errors.New("build Bybit request")
		}
		httpRequest.Header.Set("Accept", "application/json")
		httpRequest.Header.Set("User-Agent", "exchangecrawl/0")
		httpRequest.Header.Set("X-BAPI-API-KEY", credentials.apiKey)
		httpRequest.Header.Set("X-BAPI-TIMESTAMP", timestamp)
		httpRequest.Header.Set("X-BAPI-RECV-WINDOW", strconv.Itoa(a.recvWindow))
		httpRequest.Header.Set("X-BAPI-SIGN", signature)
		if signType != "" {
			httpRequest.Header.Set("X-BAPI-SIGN-TYPE", signType)
		}

		response, err := a.httpClient.Do(httpRequest)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return source.Page{}, ctxErr
			}
			return source.Page{}, errors.New("send Bybit request: transport failed")
		}
		body, readErr := readResponseBody(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			return source.Page{}, readErr
		}

		if response.StatusCode == http.StatusTooManyRequests ||
			(response.StatusCode >= http.StatusInternalServerError && response.StatusCode < 600) {
			if attempt < maxRequestRetries {
				if err := a.waitBeforeRetry(ctx, response.Header.Get("Retry-After"), observedAt, attempt); err != nil {
					return source.Page{}, err
				}
				continue
			}
			return source.Page{}, &HTTPError{StatusCode: response.StatusCode}
		}
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return source.Page{}, &HTTPError{StatusCode: response.StatusCode}
		}

		var envelope transactionLogEnvelope
		if err := json.Unmarshal(body, &envelope); err != nil {
			return source.Page{}, errors.New("decode Bybit response")
		}
		if envelope.RetCode == nil {
			return source.Page{}, errors.New("decode Bybit response: missing retCode")
		}
		if *envelope.RetCode == 10006 && attempt < maxRequestRetries {
			if err := a.waitBeforeRetry(ctx, response.Header.Get("Retry-After"), observedAt, attempt); err != nil {
				return source.Page{}, err
			}
			continue
		}
		if *envelope.RetCode != 0 {
			redactions := append([]string(nil), credentials.redactions...)
			redactions = append(redactions, signature, requestURL.String(), requestURL.RequestURI())
			return source.Page{}, &APIError{
				Code:    *envelope.RetCode,
				Message: sanitizeRemoteMessage(envelope.RetMsg, redactions...),
			}
		}
		if envelope.Result == nil {
			return source.Page{}, errors.New("decode Bybit response: missing result")
		}

		entries := make([]model.LedgerEntry, 0, len(envelope.Result.List))
		for _, raw := range envelope.Result.List {
			entry, err := normalizeEntry(raw, request.Account, observedAt)
			if err != nil {
				return source.Page{}, err
			}
			entries = append(entries, entry)
		}
		nextCursor := envelope.Result.NextPageCursor
		return source.Page{
			Entries:    entries,
			NextCursor: nextCursor,
			Done:       nextCursor == "",
		}, nil
	}
	return source.Page{}, errors.New("Bybit request exhausted retries")
}

type resolvedCredentials struct {
	apiKey     string
	signer     requestSigner
	redactions []string
}

type envNames struct {
	apiKey         string
	secret         string
	privateKeyPath string
}

func credentialEnvNames(account source.Account) envNames {
	privateKeyPath := strings.TrimSpace(account.PrivateKeyPathEnv)
	if strings.TrimSpace(account.APIKeyEnv) == "" && strings.TrimSpace(account.APISecretEnv) == "" && privateKeyPath == "" {
		privateKeyPath = "BYBIT_API_PRIVATE_KEY_PATH"
	}
	return envNames{
		apiKey:         valueOrDefault(account.APIKeyEnv, "BYBIT_API_KEY"),
		secret:         valueOrDefault(account.APISecretEnv, "BYBIT_API_SECRET"),
		privateKeyPath: privateKeyPath,
	}
}

func (a *Adapter) resolveCredentials(account source.Account) (resolvedCredentials, error) {
	names := credentialEnvNames(account)
	apiKey, ok := a.nonEmptyEnv(names.apiKey)
	if !ok {
		return resolvedCredentials{}, fmt.Errorf("missing Bybit credential environment variable %s", names.apiKey)
	}

	if names.privateKeyPath != "" {
		if privateKeyPath, ok := a.nonEmptyEnv(names.privateKeyPath); ok {
			privateKeyPEM, err := a.readFile(privateKeyPath)
			if err != nil {
				return resolvedCredentials{}, fmt.Errorf("read Bybit RSA key from %s", names.privateKeyPath)
			}
			signer, err := newRSASigner(privateKeyPEM)
			if err != nil {
				return resolvedCredentials{}, err
			}
			return resolvedCredentials{
				apiKey:     apiKey,
				signer:     signer,
				redactions: []string{apiKey, privateKeyPath},
			}, nil
		}
	}

	secret, ok := a.nonEmptyEnv(names.secret)
	if !ok {
		if names.privateKeyPath != "" {
			return resolvedCredentials{}, fmt.Errorf(
				"missing Bybit signing credential environment variable %s or %s",
				names.secret,
				names.privateKeyPath,
			)
		}
		return resolvedCredentials{}, fmt.Errorf("missing Bybit signing credential environment variable %s", names.secret)
	}
	signer, err := newHMACSigner(secret)
	if err != nil {
		return resolvedCredentials{}, err
	}
	return resolvedCredentials{
		apiKey:     apiKey,
		signer:     signer,
		redactions: []string{apiKey, secret},
	}, nil
}

type transactionLogEnvelope struct {
	RetCode *int                  `json:"retCode"`
	RetMsg  string                `json:"retMsg"`
	Result  *transactionLogResult `json:"result"`
}

type transactionLogResult struct {
	List           []json.RawMessage `json:"list"`
	NextPageCursor string            `json:"nextPageCursor"`
}

type transactionLogRecord struct {
	ID              string `json:"id"`
	Symbol          string `json:"symbol"`
	Category        string `json:"category"`
	Side            string `json:"side"`
	TransactionTime string `json:"transactionTime"`
	Type            string `json:"type"`
	TransSubType    string `json:"transSubType"`
	Currency        string `json:"currency"`
	Funding         string `json:"funding"`
	Fee             string `json:"fee"`
	CashFlow        string `json:"cashFlow"`
	Change          string `json:"change"`
	CashBalance     string `json:"cashBalance"`
	TradeID         string `json:"tradeId"`
	OrderID         string `json:"orderId"`
}

func normalizeEntry(raw json.RawMessage, account source.Account, observedAt time.Time) (model.LedgerEntry, error) {
	var record transactionLogRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.LedgerEntry{}, errors.New("decode Bybit transaction log entry")
	}
	if strings.TrimSpace(record.ID) == "" {
		return model.LedgerEntry{}, errors.New("Bybit transaction log entry has no id")
	}
	timestampMillis, err := strconv.ParseInt(record.TransactionTime, 10, 64)
	if err != nil || timestampMillis < 0 {
		return model.LedgerEntry{}, errors.New("Bybit transaction log entry has invalid transactionTime")
	}

	return model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    account.ID,
		AccountLabel: account.Label,
		EntryID:      record.ID,
		Symbol:       record.Symbol,
		Category:     record.Category,
		Type:         record.Type,
		Asset:        record.Currency,
		Side:         record.Side,
		Amount:       record.Change,
		Fee:          record.Fee,
		Funding:      record.Funding,
		CashFlow:     record.CashFlow,
		Balance:      record.CashBalance,
		OrderID:      record.OrderID,
		TradeID:      record.TradeID,
		Info:         record.TransSubType,
		OccurredAt:   time.UnixMilli(timestampMillis).UTC(),
		ObservedAt:   observedAt.UTC(),
		RawJSON:      append(json.RawMessage(nil), raw...),
	}, nil
}

func parseBaseURL(raw string, allowHTTP bool) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	validScheme := parsed != nil && (parsed.Scheme == "https" ||
		(allowHTTP && parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname())))
	if err != nil || !validScheme || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("Bybit base URL must be an HTTP(S) origin")
	}
	parsed.Path = ""
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (a *Adapter) waitBeforeRetry(ctx context.Context, retryAfter string, now time.Time, attempt int) error {
	delay := retryDelay(retryAfter, now, attempt)
	if err := a.sleep(ctx, delay); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("wait to retry Bybit request: %w", err)
	}
	return nil
}

func retryDelay(retryAfter string, now time.Time, attempt int) time.Duration {
	retryAfter = strings.TrimSpace(retryAfter)
	if seconds, err := strconv.ParseInt(retryAfter, 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(retryAfter); err == nil {
		if delay := deadline.Sub(now); delay > 0 {
			return delay
		}
		return 0
	}
	return baseRetryDelay * time.Duration(1<<attempt)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validatePageRequest(request source.PageRequest) error {
	if strings.TrimSpace(request.Account.ID) == "" {
		return errors.New("Bybit Connected Account id is required")
	}
	if request.Start.IsZero() || request.End.IsZero() {
		return errors.New("Bybit page start and end are required")
	}
	if !request.Start.Before(request.End) {
		return errors.New("Bybit page start must be before end")
	}
	if request.End.Sub(request.Start) > 7*24*time.Hour {
		return errors.New("Bybit page range exceeds seven days")
	}
	return nil
}

func readResponseBody(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxResponseBodyBytes+1))
	if err != nil {
		return nil, errors.New("read Bybit response")
	}
	if len(body) > maxResponseBodyBytes {
		return nil, errors.New("Bybit response exceeds size limit")
	}
	return body, nil
}

func sanitizeRemoteMessage(message string, redactions ...string) string {
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, message)
	for _, value := range redactions {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	message = strings.TrimSpace(message)
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

func (a *Adapter) envPresent(name string) bool {
	_, ok := a.nonEmptyEnv(name)
	return ok
}

func (a *Adapter) nonEmptyEnv(name string) (string, bool) {
	value, ok := a.lookupEnv(name)
	return value, ok && strings.TrimSpace(value) != ""
}

func valueOrDefault(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

var _ source.Adapter = (*Adapter)(nil)
