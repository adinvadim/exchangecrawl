package appconfig

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	crawlconfig "github.com/openclaw/crawlkit/config"

	"github.com/adinvadim/exchangecrawl/internal/source"
)

const schemaVersion = 1

var (
	accountIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_]*$`)
	envNamePattern   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type Spec struct {
	AppID               string
	EnvPrefix           string
	DefaultBaseURL      string
	BaseURLEnv          string
	SupportsPrivateKey  bool
	AllowedBaseURLHosts []string
}

type Config struct {
	Version         int             `toml:"version" json:"version"`
	DBPath          string          `toml:"db_path" json:"db_path"`
	BaseURL         string          `toml:"base_url" json:"base_url"`
	InitialLookback string          `toml:"initial_lookback" json:"initial_lookback"`
	Accounts        []AccountConfig `toml:"accounts" json:"accounts"`
}

type AccountConfig struct {
	ID                string `toml:"id" json:"id"`
	Label             string `toml:"label" json:"label"`
	APIKeyEnv         string `toml:"api_key_env" json:"api_key_env"`
	APISecretEnv      string `toml:"api_secret_env" json:"api_secret_env"`
	PrivateKeyPathEnv string `toml:"private_key_path_env,omitempty" json:"private_key_path_env,omitempty"`
}

func Default(spec Spec) (Config, error) {
	app, err := appFor(spec)
	if err != nil {
		return Config{}, err
	}
	paths, err := app.DefaultPaths()
	if err != nil {
		return Config{}, err
	}
	prefix := strings.ToUpper(strings.TrimSpace(spec.EnvPrefix))
	account := AccountConfig{
		ID:           "primary",
		Label:        "Primary",
		APIKeyEnv:    prefix + "_API_KEY",
		APISecretEnv: prefix + "_API_SECRET",
	}
	if spec.SupportsPrivateKey {
		account.PrivateKeyPathEnv = prefix + "_API_PRIVATE_KEY_PATH"
	}
	return Config{
		Version:         schemaVersion,
		DBPath:          paths.DBPath,
		BaseURL:         spec.DefaultBaseURL,
		InitialLookback: "168h",
		Accounts:        []AccountConfig{account},
	}, nil
}

func DefaultPath(spec Spec) (string, error) {
	app, err := appFor(spec)
	if err != nil {
		return "", err
	}
	paths, err := app.DefaultPaths()
	if err != nil {
		return "", err
	}
	return paths.ConfigPath, nil
}

func Load(spec Spec, path string) (Config, string, error) {
	defaults, err := Default(spec)
	if err != nil {
		return Config{}, "", err
	}
	resolvedPath, err := resolveConfigPath(spec, path)
	if err != nil {
		return Config{}, "", err
	}
	cfg := defaults
	if err := crawlconfig.LoadTOML(resolvedPath, &cfg); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, resolvedPath, err
	}
	if override := strings.TrimSpace(os.Getenv(spec.BaseURLEnv)); override != "" {
		cfg.BaseURL = override
	}
	if err := cfg.Resolve(spec); err != nil {
		return Config{}, resolvedPath, err
	}
	return cfg, resolvedPath, nil
}

func WriteStarter(spec Spec, path string) (string, error) {
	resolvedPath, err := resolveConfigPath(spec, path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(resolvedPath); err == nil {
		return resolvedPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	cfg, err := Default(spec)
	if err != nil {
		return "", err
	}
	if err := crawlconfig.WriteTOML(resolvedPath, cfg, 0o600); err != nil {
		return "", err
	}
	return resolvedPath, nil
}

func (c *Config) Resolve(spec Spec) error {
	if c.Version == 0 {
		c.Version = schemaVersion
	}
	if c.Version != schemaVersion {
		return fmt.Errorf("unsupported config version %d", c.Version)
	}
	if strings.TrimSpace(c.DBPath) == "" {
		return errors.New("db_path is required")
	}
	c.DBPath = absolutePath(c.DBPath)
	baseURL, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil {
		return errors.New("base_url must be an HTTPS origin")
	}
	if baseURL.Path != "" && baseURL.Path != "/" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return errors.New("base_url must be an HTTPS origin")
	}
	if baseURL.Port() != "" && baseURL.Port() != "443" {
		return errors.New("base_url must use the default HTTPS port")
	}
	if !baseURLHostAllowed(spec, baseURL.Hostname()) {
		return fmt.Errorf("base_url host is not allowed for %s", spec.AppID)
	}
	c.BaseURL = strings.TrimRight(baseURL.String(), "/")
	lookback, err := time.ParseDuration(strings.TrimSpace(c.InitialLookback))
	if err != nil || lookback <= 0 {
		return fmt.Errorf("initial_lookback must be a positive duration: %q", c.InitialLookback)
	}
	if len(c.Accounts) == 0 {
		return errors.New("at least one account is required")
	}
	seen := make(map[string]struct{}, len(c.Accounts))
	for index := range c.Accounts {
		account := &c.Accounts[index]
		account.ID = strings.ToLower(strings.TrimSpace(account.ID))
		if !accountIDPattern.MatchString(account.ID) {
			return fmt.Errorf("invalid account id %q", account.ID)
		}
		if _, ok := seen[account.ID]; ok {
			return fmt.Errorf("duplicate account id %q", account.ID)
		}
		seen[account.ID] = struct{}{}
		account.Label = strings.TrimSpace(account.Label)
		if account.Label == "" {
			account.Label = account.ID
		}
		if err := validateEnvName("api_key_env", account.APIKeyEnv); err != nil {
			return fmt.Errorf("account %s: %w", account.ID, err)
		}
		if err := validateEnvName("api_secret_env", account.APISecretEnv); err != nil {
			return fmt.Errorf("account %s: %w", account.ID, err)
		}
		if account.PrivateKeyPathEnv != "" {
			if !spec.SupportsPrivateKey {
				return fmt.Errorf("account %s: private_key_path_env is not supported", account.ID)
			}
			if err := validateEnvName("private_key_path_env", account.PrivateKeyPathEnv); err != nil {
				return fmt.Errorf("account %s: %w", account.ID, err)
			}
		}
	}
	return nil
}

func baseURLHostAllowed(spec Spec, host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	allowed := append([]string(nil), spec.AllowedBaseURLHosts...)
	if parsed, err := url.Parse(spec.DefaultBaseURL); err == nil {
		allowed = append(allowed, parsed.Hostname())
	}
	for _, candidate := range allowed {
		if host == strings.ToLower(strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}

func (c Config) SourceAccounts() []source.Account {
	accounts := make([]source.Account, 0, len(c.Accounts))
	for _, account := range c.Accounts {
		accounts = append(accounts, source.Account{
			ID:                account.ID,
			Label:             account.Label,
			APIKeyEnv:         account.APIKeyEnv,
			APISecretEnv:      account.APISecretEnv,
			PrivateKeyPathEnv: account.PrivateKeyPathEnv,
		})
	}
	return accounts
}

func (c Config) Lookback() time.Duration {
	duration, _ := time.ParseDuration(c.InitialLookback)
	return duration
}

func appFor(spec Spec) (crawlconfig.App, error) {
	id := strings.TrimSpace(spec.AppID)
	if id == "" {
		return crawlconfig.App{}, errors.New("app id is required")
	}
	return crawlconfig.App{
		Name:          id,
		ConfigEnv:     strings.ToUpper(strings.ReplaceAll(id, "-", "_")) + "_CONFIG",
		LegacyBaseDir: "~/." + id,
		PlatformDirs:  true,
	}, nil
}

func resolveConfigPath(spec Spec, path string) (string, error) {
	app, err := appFor(spec)
	if err != nil {
		return "", err
	}
	resolved, err := app.ResolveConfigPath(path)
	if err != nil {
		return "", err
	}
	return absolutePath(resolved), nil
}

func validateEnvName(field, value string) error {
	if !envNamePattern.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("%s must name an environment variable", field)
	}
	return nil
}

func absolutePath(path string) string {
	expanded := crawlconfig.ExpandHome(path)
	absolute, err := filepath.Abs(expanded)
	if err != nil {
		return expanded
	}
	return absolute
}
