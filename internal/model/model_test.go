package model_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/model"
)

func TestLedgerEntryJSONKeepsDecimalStringsAndUTCFields(t *testing.T) {
	t.Parallel()

	entry := model.LedgerEntry{
		Exchange:     model.ExchangeBybit,
		AccountID:    "primary",
		AccountLabel: "Primary",
		EntryID:      "entry-1",
		Amount:       "0.100000000000000001",
		OccurredAt:   time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		ObservedAt:   time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC),
		RawJSON:      json.RawMessage(`{"provider":"value"}`),
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if value["amount"] != entry.Amount {
		t.Fatalf("amount = %#v, want decimal string %q", value["amount"], entry.Amount)
	}
	if value["occurred_at"] != "2026-07-10T12:00:00Z" {
		t.Fatalf("occurred_at = %#v", value["occurred_at"])
	}
	raw, ok := value["raw_json"].(map[string]any)
	if !ok || raw["provider"] != "value" {
		t.Fatalf("raw_json = %#v, want embedded provider object", value["raw_json"])
	}
}
