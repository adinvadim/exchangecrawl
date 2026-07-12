package model

import (
	"encoding/json"
	"time"
)

// Exchange identifies the trading venue that produced a Ledger Entry.
type Exchange string

const (
	ExchangeBybit   Exchange = "bybit"
	ExchangeBinance Exchange = "binance"
)

// LedgerEntry is the stable cross-Exchange representation stored in an Archive.
// Decimal values remain strings so archiving never loses provider precision.
type LedgerEntry struct {
	Exchange     Exchange        `json:"exchange"`
	AccountID    string          `json:"account_id"`
	AccountLabel string          `json:"account_label"`
	EntryID      string          `json:"entry_id"`
	Symbol       string          `json:"symbol"`
	Category     string          `json:"category"`
	Type         string          `json:"type"`
	Asset        string          `json:"asset"`
	Side         string          `json:"side"`
	Amount       string          `json:"amount"`
	Fee          string          `json:"fee"`
	Funding      string          `json:"funding"`
	CashFlow     string          `json:"cash_flow"`
	Balance      string          `json:"balance"`
	OrderID      string          `json:"order_id"`
	TradeID      string          `json:"trade_id"`
	Info         string          `json:"info"`
	OccurredAt   time.Time       `json:"occurred_at"`
	ObservedAt   time.Time       `json:"observed_at"`
	RawJSON      json.RawMessage `json:"raw_json"`
}

// StateObservation is one observed state of a mutable Exchange object.
// Decimal values remain strings so archiving never loses provider precision.
type StateObservation struct {
	Exchange         Exchange        `json:"exchange"`
	AccountID        string          `json:"account_id"`
	AccountLabel     string          `json:"account_label"`
	Stream           string          `json:"stream"`
	ObjectType       string          `json:"object_type"`
	ObjectID         string          `json:"object_id"`
	Status           string          `json:"status"`
	StateFingerprint string          `json:"state_fingerprint"`
	Symbol           string          `json:"symbol"`
	Asset            string          `json:"asset"`
	Amount           string          `json:"amount"`
	OccurredAt       time.Time       `json:"occurred_at,omitempty"`
	ObservedAt       time.Time       `json:"observed_at"`
	RawJSON          json.RawMessage `json:"raw_json"`
}
