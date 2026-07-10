package appconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultNeverContainsCredentialValues(t *testing.T) {
	t.Setenv("BYBIT_API_KEY", "sensitive-key")
	t.Setenv("BYBIT_API_SECRET", "sensitive-secret")

	cfg, err := Default(bybitSpec())
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Accounts[0].APIKeyEnv; got != "BYBIT_API_KEY" {
		t.Fatalf("APIKeyEnv = %q", got)
	}
	if got := cfg.Accounts[0].APISecretEnv; got != "BYBIT_API_SECRET" {
		t.Fatalf("APISecretEnv = %q", got)
	}
}

func TestResolveCanonicalizesAccountsAndRejectsDuplicates(t *testing.T) {
	cfg, err := Default(bybitSpec())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Accounts = []AccountConfig{
		{ID: "MAIN", APIKeyEnv: "BYBIT_MAIN_API_KEY", APISecretEnv: "BYBIT_MAIN_API_SECRET"},
		{ID: "main", APIKeyEnv: "BYBIT_OTHER_API_KEY", APISecretEnv: "BYBIT_OTHER_API_SECRET"},
	}
	if err := cfg.Resolve(bybitSpec()); err == nil {
		t.Fatal("expected duplicate account error")
	}
}

func TestWriteStarterUsesPrivateFileAndNoSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	written, err := WriteStarter(bybitSpec(), path)
	if err != nil {
		t.Fatal(err)
	}
	if written != path {
		t.Fatalf("written path = %q", written)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %o", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if contains(text, "sensitive") {
		t.Fatal("starter config contains a credential value")
	}
}

func TestLoadAppliesBaseURLOverride(t *testing.T) {
	t.Setenv("BYBIT_API_BASE_URL", "https://api.bybit.id")
	path := filepath.Join(t.TempDir(), "missing.toml")
	cfg, _, err := Load(bybitSpec(), path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != "https://api.bybit.id" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
}

func TestResolveRejectsUnlistedBaseURLHost(t *testing.T) {
	cfg, err := Default(bybitSpec())
	if err != nil {
		t.Fatal(err)
	}
	cfg.BaseURL = "https://collector.example"
	if err := cfg.Resolve(bybitSpec()); err == nil {
		t.Fatal("expected unlisted host error")
	}
}

func TestResolveDoesNotEchoCredentialsFromInvalidBaseURL(t *testing.T) {
	cfg, err := Default(bybitSpec())
	if err != nil {
		t.Fatal(err)
	}
	cfg.BaseURL = "https://user:sensitive-password@api.bybit.com?token=sensitive-token"
	err = cfg.Resolve(bybitSpec())
	if err == nil {
		t.Fatal("expected invalid origin error")
	}
	for _, secret := range []string{"sensitive-password", "sensitive-token", "user:"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

func bybitSpec() Spec {
	return Spec{
		AppID:               "bybitcrawl",
		EnvPrefix:           "BYBIT",
		DefaultBaseURL:      "https://api.bybit.com",
		BaseURLEnv:          "BYBIT_API_BASE_URL",
		SupportsPrivateKey:  true,
		AllowedBaseURLHosts: []string{"api.bybit.com", "api.bybit.id"},
	}
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
