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
		for version := current; version < schemaVersion; version++ {
			switch version {
			case 0:
				// New Archive: the current schema was created by store.Open.
			case 1:
				if err := migrateBinanceEntryIDs(ctx, tx); err != nil {
					return fmt.Errorf("migrate Archive schema 1 to 2: %w", err)
				}
			case 2:
				if err := migrateV2ToV3(ctx, tx); err != nil {
					return fmt.Errorf("migrate Archive schema 2 to 3: %w", err)
				}
			default:
				return fmt.Errorf("unsupported Archive schema version %d", version)
			}
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

func migrateV2ToV3(ctx context.Context, tx *sql.Tx) error {
	hasStream, err := ledgerEntriesHaveStream(ctx, tx)
	if err != nil {
		return err
	}
	if !hasStream {
		if _, err := tx.ExecContext(ctx, `alter table ledger_entries add column stream text not null default 'ledger'`); err != nil {
			return fmt.Errorf("add Ledger Entry stream: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `create index if not exists idx_ledger_entries_stream_order
on ledger_entries(exchange, stream, occurred_at desc)`); err != nil {
		return fmt.Errorf("index Ledger Entry streams: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
insert into sync_state(source_name, entity_type, entity_id, value, updated_at)
select source_name, 'ledger', entity_id, value, updated_at
from sync_state
where entity_type = 'account'
on conflict(source_name, entity_type, entity_id) do nothing`); err != nil {
		return fmt.Errorf("copy legacy Checkpoints: %w", err)
	}
	return nil
}

func ledgerEntriesHaveStream(ctx context.Context, tx *sql.Tx) (bool, error) {
	rows, err := tx.QueryContext(ctx, `pragma table_info(ledger_entries)`)
	if err != nil {
		return false, fmt.Errorf("inspect Ledger Entry columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("inspect Ledger Entry columns: %w", err)
		}
		if name == "stream" {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("inspect Ledger Entry columns: %w", err)
	}
	return false, nil
}

type binanceMigrationRow struct {
	id         int64
	accountID  string
	entryID    string
	observedAt int64
	rawJSON    string
}

const migrationBatchSize = 100

func migrateBinanceEntryIDs(ctx context.Context, tx *sql.Tx) error {
	var afterID int64
	for {
		entries, err := readBinanceMigrationBatch(ctx, tx, afterID)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		for _, entry := range entries {
			if err := migrateBinanceEntryID(ctx, tx, entry); err != nil {
				return err
			}
		}
		afterID = entries[len(entries)-1].id
	}
}

func readBinanceMigrationBatch(ctx context.Context, tx *sql.Tx, afterID int64) ([]binanceMigrationRow, error) {
	rows, err := tx.QueryContext(ctx, `
select id, account_id, entry_id, observed_at, raw_json
from ledger_entries
where exchange = ? and id > ?
order by id
limit ?`, model.ExchangeBinance, afterID, migrationBatchSize)
	if err != nil {
		return nil, fmt.Errorf("read Binance Ledger Entries: %w", err)
	}
	defer rows.Close()
	var entries []binanceMigrationRow
	for rows.Next() {
		var entry binanceMigrationRow
		if err := rows.Scan(&entry.id, &entry.accountID, &entry.entryID, &entry.observedAt, &entry.rawJSON); err != nil {
			return nil, fmt.Errorf("read Binance Ledger Entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Binance Ledger Entries: %w", err)
	}
	return entries, nil
}

func migrateBinanceEntryID(ctx context.Context, tx *sql.Tx, entry binanceMigrationRow) error {
	canonicalID, legacyID, err := scopedBinanceEntryID(entry.rawJSON)
	if err != nil {
		return fmt.Errorf("derive Binance Ledger Entry identity for row %d: %w", entry.id, err)
	}
	if entry.entryID == canonicalID {
		return nil
	}
	if entry.entryID != legacyID {
		return fmt.Errorf("Binance Ledger Entry row %d has an inconsistent identity", entry.id)
	}

	var existingID int64
	var existingObservedAt int64
	err = tx.QueryRowContext(ctx, `
select id, observed_at
from ledger_entries
where exchange = ? and account_id = ? and entry_id = ?`,
		model.ExchangeBinance, entry.accountID, canonicalID,
	).Scan(&existingID, &existingObservedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return fmt.Errorf("find scoped Binance Ledger Entry: %w", err)
	case existingObservedAt >= entry.observedAt:
		if _, err := tx.ExecContext(ctx, `delete from ledger_entries where id = ?`, entry.id); err != nil {
			return fmt.Errorf("remove superseded Binance Ledger Entry: %w", err)
		}
		return nil
	default:
		if _, err := tx.ExecContext(ctx, `delete from ledger_entries where id = ?`, existingID); err != nil {
			return fmt.Errorf("replace superseded Binance Ledger Entry: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `update ledger_entries set entry_id = ? where id = ?`, canonicalID, entry.id); err != nil {
		return fmt.Errorf("scope Binance Ledger Entry identity: %w", err)
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
