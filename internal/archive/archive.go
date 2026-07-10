// Package archive owns local Ledger Entry persistence, incremental sync, and
// strictly local query behavior shared by both Exchange crawlers.
package archive

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
	"github.com/openclaw/crawlkit/state"
	"github.com/openclaw/crawlkit/store"
)

const schemaVersion = 1

type Options struct {
	Path     string
	Accounts []source.Account
	Adapter  source.Adapter
	Now      func() time.Time
}

type Archive struct {
	store    *store.Store
	state    *state.Store
	adapter  source.Adapter
	accounts []source.Account
	now      func() time.Time
}

func Open(ctx context.Context, options Options) (*Archive, error) {
	if options.Adapter == nil {
		return nil, errors.New("source adapter is required")
	}
	if len(options.Accounts) == 0 {
		return nil, errors.New("at least one Connected Account is required")
	}
	if err := validateAccounts(options.Accounts); err != nil {
		return nil, err
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	db, err := store.Open(ctx, store.Options{
		Path:          options.Path,
		Schema:        archiveSchema + state.Schema,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("open Archive: %w", err)
	}
	return &Archive{
		store:    db,
		state:    state.NewWithClock(db.DB(), now),
		adapter:  options.Adapter,
		accounts: append([]source.Account(nil), options.Accounts...),
		now:      now,
	}, nil
}

func (a *Archive) Close() error {
	if a == nil || a.store == nil {
		return nil
	}
	return a.store.Close()
}

func validateAccounts(accounts []source.Account) error {
	seen := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		id := strings.TrimSpace(account.ID)
		if id == "" {
			return errors.New("Connected Account id is required")
		}
		if id != account.ID || strings.ToLower(id) != id {
			return fmt.Errorf("Connected Account id %q must be trimmed lowercase", account.ID)
		}
		if strings.TrimSpace(account.Label) == "" {
			return fmt.Errorf("Connected Account %q label is required", id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate Connected Account id %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (a *Archive) exchange() model.Exchange {
	return a.adapter.Exchange()
}
