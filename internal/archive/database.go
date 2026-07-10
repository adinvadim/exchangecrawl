package archive

import (
	"context"
	"fmt"

	"github.com/openclaw/crawlkit/store"
)

var requiredSchemaQueries = []struct {
	name  string
	query string
}{
	{
		name: "ledger_entries",
		query: `select exchange, account_id, account_label, entry_id, symbol,
category, entry_type, asset, side, amount, fee, funding, cash_flow, balance,
order_id, trade_id, info, occurred_at, observed_at, raw_json
from ledger_entries where 0`,
	},
	{
		name: "ledger_entries_fts",
		query: `select rowid, symbol, category, entry_type, asset, side, order_id,
trade_id, info from ledger_entries_fts where 0`,
	},
	{
		name: "sync_state",
		query: `select source_name, entity_type, entity_id, value, updated_at
from sync_state where 0`,
	},
}

func checkDatabase(ctx context.Context, db *store.Store) error {
	if err := checkSchema(ctx, db); err != nil {
		return err
	}

	rows, err := db.DB().QueryContext(ctx, `pragma quick_check`)
	if err != nil {
		return fmt.Errorf("run Archive quick check: %w", err)
	}
	defer rows.Close()
	resultCount := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("read Archive quick check: %w", err)
		}
		resultCount++
		if result != "ok" {
			return fmt.Errorf("Archive quick check failed: %s", result)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read Archive quick check: %w", err)
	}
	if resultCount == 0 {
		return fmt.Errorf("Archive quick check returned no result")
	}
	return nil
}

func checkSchema(ctx context.Context, db *store.Store) error {
	version, err := db.SchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("read Archive schema version: %w", err)
	}
	if version != schemaVersion {
		return fmt.Errorf("Archive schema version is %d, want %d", version, schemaVersion)
	}
	for _, required := range requiredSchemaQueries {
		rows, err := db.DB().QueryContext(ctx, required.query)
		if err != nil {
			return fmt.Errorf("validate required Archive schema %s: %w", required.name, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("validate required Archive schema %s: %w", required.name, err)
		}
	}

	return nil
}
