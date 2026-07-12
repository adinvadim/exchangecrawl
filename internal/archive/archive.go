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

const schemaVersion = 3

type Options struct {
	Path           string
	Accounts       []source.Account
	Streams        []source.StreamBinding
	Now            func() time.Time
	ReadOnly       bool
	CheckIntegrity bool
}

type Archive struct {
	store          *store.Store
	state          *state.Store
	streams        []source.StreamBinding
	sourceExchange model.Exchange
	accounts       []source.Account
	now            func() time.Time
}

func Open(ctx context.Context, options Options) (*Archive, error) {
	streams, exchange, err := validateStreams(options.Streams)
	if err != nil {
		return nil, err
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
	var db *store.Store
	if options.ReadOnly {
		db, err = store.OpenReadOnly(ctx, options.Path)
	} else {
		db, err = store.Open(ctx, store.Options{
			Path:   options.Path,
			Schema: archiveSchema + state.Schema,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("open Archive: %w", err)
	}
	if !options.ReadOnly {
		if err := migrateSchema(ctx, db); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open Archive: %w", err)
		}
	}
	if options.ReadOnly {
		if options.CheckIntegrity {
			err = checkDatabase(ctx, db)
		} else {
			err = checkSchema(ctx, db)
		}
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open read-only Archive: %w", err)
		}
	}
	return &Archive{
		store:          db,
		state:          state.NewWithClock(db.DB(), now),
		streams:        streams,
		sourceExchange: exchange,
		accounts:       append([]source.Account(nil), options.Accounts...),
		now:            now,
	}, nil
}

func validateStreams(streams []source.StreamBinding) ([]source.StreamBinding, model.Exchange, error) {
	if len(streams) == 0 {
		return nil, "", errors.New("at least one source stream is required")
	}
	validated := append([]source.StreamBinding(nil), streams...)
	seen := make(map[string]struct{}, len(streams))
	var exchange model.Exchange
	for index, binding := range validated {
		name := strings.TrimSpace(binding.Name)
		if name == "" {
			return nil, "", fmt.Errorf("source stream %d name is required", index)
		}
		if name != binding.Name {
			return nil, "", fmt.Errorf("source stream name %q must be trimmed", binding.Name)
		}
		if _, ok := seen[name]; ok {
			return nil, "", fmt.Errorf("duplicate source stream name %q", name)
		}
		seen[name] = struct{}{}
		stream := strings.TrimSpace(binding.Stream)
		if stream == "" {
			return nil, "", fmt.Errorf("source stream %q storage stream is required", name)
		}
		if stream != binding.Stream {
			return nil, "", fmt.Errorf("source stream %q storage stream must be trimmed", name)
		}
		if (binding.Adapter == nil) == (binding.Events == nil) {
			return nil, "", fmt.Errorf("source stream %q must configure exactly one adapter kind", name)
		}
		if binding.InitialLookback < 0 || binding.CheckpointOverlap < 0 || binding.MaxWindow < 0 {
			return nil, "", fmt.Errorf("source stream %q durations must not be negative", name)
		}
		bindingExchange := streamExchange(binding)
		if bindingExchange == "" {
			return nil, "", fmt.Errorf("source stream %q Exchange is required", name)
		}
		if exchange == "" {
			exchange = bindingExchange
		} else if bindingExchange != exchange {
			return nil, "", fmt.Errorf("source stream %q Exchange %q does not match %q", name, bindingExchange, exchange)
		}
	}
	return validated, exchange, nil
}

func streamExchange(binding source.StreamBinding) model.Exchange {
	if binding.Adapter != nil {
		return binding.Adapter.Exchange()
	}
	return binding.Events.Exchange()
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
	return a.sourceExchange
}
