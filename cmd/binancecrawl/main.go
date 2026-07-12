package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/appconfig"
	"github.com/adinvadim/exchangecrawl/internal/binance"
	"github.com/adinvadim/exchangecrawl/internal/cli"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

var version = "dev"

func main() {
	spec := cli.Spec{
		ID:          "binancecrawl",
		DisplayName: "Binance Crawl",
		Description: "Local-first Binance Futures and Spot archive.",
		SymbolName:  "chart.xyaxis.line",
		AccentColor: "#F0B90B",
		Version:     version,
		Config: appconfig.Spec{
			AppID:               "binancecrawl",
			EnvPrefix:           "BINANCE",
			DefaultBaseURL:      "https://fapi.binance.com",
			BaseURLEnv:          "BINANCE_FUTURES_BASE_URL",
			AllowedBaseURLHosts: []string{"fapi.binance.com"},
		},
		NewStreams: func(baseURL string) ([]source.StreamBinding, error) {
			const day = 24 * time.Hour
			adapter, err := binance.New(binance.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			spot, err := binance.NewSpot(binance.Options{})
			if err != nil {
				return nil, err
			}
			// Futures adapters use the configured fapi base URL.
			futFills, err := binance.NewFuturesFills(binance.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futOrders, err := binance.NewFuturesOrders(binance.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futPositions, err := binance.NewFuturesPositions(binance.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			// SAPI adapters self-default to https://api.binance.com; never pass the fapi URL.
			deposits, err := binance.NewDeposits(binance.Options{})
			if err != nil {
				return nil, err
			}
			withdrawals, err := binance.NewWithdrawals(binance.Options{})
			if err != nil {
				return nil, err
			}
			earn, err := binance.NewEarn(binance.Options{})
			if err != nil {
				return nil, err
			}
			earnRewards, err := binance.NewEarnRewards(binance.Options{})
			if err != nil {
				return nil, err
			}
			autoInvest, err := binance.NewAutoInvest(binance.Options{})
			if err != nil {
				return nil, err
			}
			autoInvestPlans, err := binance.NewAutoInvestPlans(binance.Options{})
			if err != nil {
				return nil, err
			}
			c2c, err := binance.NewC2C(binance.Options{})
			if err != nil {
				return nil, err
			}
			return []source.StreamBinding{
				{Name: "ledger", Stream: "ledger", Adapter: adapter},
				{
					Name: "spot/fills", Stream: "spot", Adapter: spot,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: day,
				},
				{
					Name: "spot/orders", Stream: "spot", Events: spot,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: day,
				},
				{
					Name: "spot/orders-terminal", Stream: "spot", Adapter: spot.TerminalOrders(),
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: day,
				},
				{
					Name: "futures/fills", Stream: "futures", Adapter: futFills,
					InitialLookback: 180 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/orders", Stream: "futures", Events: futOrders,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/orders-terminal", Stream: "futures", Adapter: futOrders.TerminalOrders(),
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{Name: "futures/positions", Stream: "futures", Events: futPositions},
				{
					Name: "asset/deposits", Stream: "asset", Events: deposits,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 90 * day,
				},
				{
					Name: "asset/withdrawals", Stream: "asset", Events: withdrawals,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 89 * day,
				},
				{
					Name: "asset/withdrawals-terminal", Stream: "asset", Adapter: withdrawals.Terminal(),
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 89 * day,
				},
				{
					Name: "earn/orders", Stream: "earn", Events: earn,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 30 * day,
				},
				{
					Name: "earn/orders-terminal", Stream: "earn", Adapter: earn.TerminalOrders(),
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 30 * day,
				},
				{
					Name: "earn/rewards", Stream: "earn", Adapter: earnRewards,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "p2p/orders", Stream: "p2p", Events: c2c,
					InitialLookback: 180 * day, CheckpointOverlap: 7 * day,
					MaxWindow: 30 * day,
				},
				{
					Name: "p2p/orders-terminal", Stream: "p2p", Adapter: c2c,
					InitialLookback: 180 * day, CheckpointOverlap: 7 * day,
					MaxWindow: 30 * day,
				},
				{
					Name: "autoinvest/history", Stream: "autoinvest", Adapter: autoInvest,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 30 * day,
				},
				{Name: "autoinvest/plans", Stream: "autoinvest", Events: autoInvestPlans},
			}, nil
		},
	}
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, spec); err != nil {
		fmt.Fprintln(os.Stderr, "binancecrawl:", err)
		os.Exit(1)
	}
}
