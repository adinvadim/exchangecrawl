package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/openclaw/crawlkit/store"
)

func migrateSchema(ctx context.Context, db *store.Store) error {
	if _, err := db.DB().ExecContext(ctx, `create table if not exists schema_migrations(version integer not null)`); err != nil {
		return fmt.Errorf("prepare Archive schema version: %w", err)
	}
	return db.WithTx(ctx, func(tx *sql.Tx) error {
		var current int
		if err := tx.QueryRowContext(ctx, `select coalesce(max(version), 0) from schema_migrations`).Scan(&current); err != nil {
			return fmt.Errorf("read Archive schema version: %w", err)
		}
		if current > schemaVersion {
			return fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
		}
		if current == schemaVersion {
			return nil
		}
		switch current {
		case 0:
			// New Archive: the current schema was created by store.Open.
		case 1:
			if err := migrateBinanceEntryIDs(ctx, tx); err != nil {
				return fmt.Errorf("migrate Archive schema 1 to 2: %w", err)
			}
		default:
			return fmt.Errorf("unsupported Archive schema version %d", current)
		}
		if _, err := tx.ExecContext(ctx, `delete from schema_migrations`); err != nil {
			return fmt.Errorf("clear Archive schema version: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `insert into schema_migrations(version) values(?)`, schemaVersion); err != nil {
			return fmt.Errorf("write Archive schema version: %w", err)
		}
		return nil
	})
}

type binanceMigrationRow struct {
	id         int64
	accountID  string
	entryID    string
	observedAt int64
	rawJSON    string
}

func migrateBinanceEntryIDs(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
select id, account_id, entry_id, observed_at, raw_json
from ledger_entries
where exchange = ?
order by id`, model.ExchangeBinance)
	if err != nil {
		return fmt.Errorf("read Binance Ledger Entries: %w", err)
	}
	var entries []binanceMigrationRow
	for rows.Next() {
		var entry binanceMigrationRow
		if err := rows.Scan(&entry.id, &entry.accountID, &entry.entryID, &entry.observedAt, &entry.rawJSON); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read Binance Ledger Entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read Binance Ledger Entries: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close Binance migration rows: %w", err)
	}

	byIdentity := make(map[string]binanceMigrationRow, len(entries))
	for _, entry := range entries {
		byIdentity[migrationIdentity(entry.accountID, entry.entryID)] = entry
	}
	for _, entry := range entries {
		canonicalID, legacyID, err := scopedBinanceEntryID(entry.rawJSON)
		if err != nil {
			return fmt.Errorf("derive Binance Ledger Entry identity for row %d: %w", entry.id, err)
		}
		if entry.entryID == canonicalID {
			continue
		}
		if entry.entryID != legacyID {
			return fmt.Errorf("Binance Ledger Entry row %d has an inconsistent identity", entry.id)
		}

		legacyKey := migrationIdentity(entry.accountID, entry.entryID)
		canonicalKey := migrationIdentity(entry.accountID, canonicalID)
		if existing, ok := byIdentity[canonicalKey]; ok && existing.id != entry.id {
			if existing.observedAt >= entry.observedAt {
				if _, err := tx.ExecContext(ctx, `delete from ledger_entries where id = ?`, entry.id); err != nil {
					return fmt.Errorf("remove superseded Binance Ledger Entry: %w", err)
				}
				delete(byIdentity, legacyKey)
				continue
			}
			if _, err := tx.ExecContext(ctx, `delete from ledger_entries where id = ?`, existing.id); err != nil {
				return fmt.Errorf("replace superseded Binance Ledger Entry: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `update ledger_entries set entry_id = ? where id = ?`, canonicalID, entry.id); err != nil {
			return fmt.Errorf("scope Binance Ledger Entry identity: %w", err)
		}
		delete(byIdentity, legacyKey)
		entry.entryID = canonicalID
		byIdentity[canonicalKey] = entry
	}
	return nil
}

func scopedBinanceEntryID(rawJSON string) (canonical string, legacy string, err error) {
	decoder := json.NewDecoder(strings.NewReader(rawJSON))
	decoder.UseNumber()
	var payload struct {
		IncomeType string `json:"incomeType"`
		TranID     any    `json:"tranId"`
	}
	if err := decoder.Decode(&payload); err != nil {
		return "", "", errors.New("invalid Binance raw JSON")
	}
	incomeType := strings.ToUpper(strings.TrimSpace(payload.IncomeType))
	if incomeType == "" {
		return "", "", errors.New("Binance raw JSON has no income type")
	}
	switch value := payload.TranID.(type) {
	case json.Number:
		legacy = strings.TrimSpace(value.String())
	case string:
		legacy = strings.TrimSpace(value)
	default:
		return "", "", errors.New("Binance raw JSON has an invalid transaction id")
	}
	if legacy == "" {
		return "", "", errors.New("Binance raw JSON has no transaction id")
	}
	return incomeType + ":" + legacy, legacy, nil
}

func migrationIdentity(accountID, entryID string) string {
	return accountID + "\x00" + entryID
}
