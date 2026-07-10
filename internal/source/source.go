package source

import (
	"context"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
)

// Account describes one Connected Account without containing secret values.
type Account struct {
	ID                string `json:"id"`
	Label             string `json:"label"`
	APIKeyEnv         string `json:"api_key_env"`
	APISecretEnv      string `json:"api_secret_env"`
	PrivateKeyPathEnv string `json:"private_key_path_env,omitempty"`
}

// CredentialStatus reports credential presence without exposing values.
type CredentialStatus struct {
	Ready   bool     `json:"ready"`
	Missing []string `json:"missing,omitempty"`
}

type PageRequest struct {
	Account Account
	Start   time.Time
	End     time.Time
	Cursor  string
}

type Page struct {
	Entries    []model.LedgerEntry
	NextCursor string
	Done       bool
}

// Adapter is the true-external seam implemented by each Exchange source.
type Adapter interface {
	Exchange() model.Exchange
	CheckCredentials(Account) CredentialStatus
	FetchPage(context.Context, PageRequest) (Page, error)
}
