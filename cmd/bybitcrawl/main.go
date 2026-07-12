package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/adinvadim/exchangecrawl/internal/appconfig"
	"github.com/adinvadim/exchangecrawl/internal/bybit"
	"github.com/adinvadim/exchangecrawl/internal/cli"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

var version = "dev"

func main() {
	spec := cli.Spec{
		ID:          "bybitcrawl",
		DisplayName: "Bybit Crawl",
		Description: "Local-first Bybit Unified Account ledger archive.",
		SymbolName:  "chart.xyaxis.line",
		AccentColor: "#F7A600",
		Version:     version,
		Config: appconfig.Spec{
			AppID:              "bybitcrawl",
			EnvPrefix:          "BYBIT",
			DefaultBaseURL:     "https://api.bybit.com",
			BaseURLEnv:         "BYBIT_API_BASE_URL",
			SupportsPrivateKey: true,
			AllowedBaseURLHosts: []string{
				"api.bybit.com", "api.bytick.com", "api.bybit.id", "api.bybit.nl",
				"api.bybit.tr", "api.bybit.kz", "api.bybitgeorgia.ge", "api.bybit.ae",
				"api.bybit.eu", "api.moneypartners.co.jp", "api2.moneypartners.co.jp",
				"api3.moneypartners.co.jp",
			},
		},
		NewStreams: func(baseURL string) ([]source.StreamBinding, error) {
			const day = 24 * time.Hour
			adapter, err := bybit.New(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			spotFills, spotOrders, err := bybit.NewSpot(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futFills, err := bybit.NewFuturesFills(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futPnl, err := bybit.NewFuturesClosedPnL(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futOrders, err := bybit.NewFuturesOrders(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			futPositions, err := bybit.NewFuturesPositions(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			deposits, err := bybit.NewDeposits(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			withdrawals, err := bybit.NewWithdrawals(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			earnOrders, err := bybit.NewEarn(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			earnYield, err := bybit.NewEarnYield(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			p2pOrders, err := bybit.NewP2P(bybit.Options{BaseURL: baseURL})
			if err != nil {
				return nil, err
			}
			return []source.StreamBinding{
				{Name: "ledger", Stream: "ledger", Adapter: adapter},
				{
					Name: "spot/fills", Stream: "spot", Adapter: spotFills,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "spot/orders", Stream: "spot", Events: spotOrders,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "spot/orders-terminal", Stream: "spot", Adapter: spotOrders,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/fills", Stream: "futures", Adapter: futFills,
					InitialLookback: 730 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/closed-pnl", Stream: "futures", Adapter: futPnl,
					InitialLookback: 730 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/orders", Stream: "futures", Events: futOrders,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "futures/orders-terminal", Stream: "futures", Adapter: futOrders,
					InitialLookback: 7 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{Name: "futures/positions", Stream: "futures", Events: futPositions},
				{
					Name: "asset/deposits", Stream: "asset", Events: deposits,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 30 * day,
				},
				{
					Name: "asset/withdrawals", Stream: "asset", Events: withdrawals,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 29 * day,
				},
				{
					Name: "asset/withdrawals-terminal", Stream: "asset", Adapter: withdrawals,
					InitialLookback: 365 * day, CheckpointOverlap: day,
					MaxWindow: 29 * day,
				},
				{
					Name: "earn/orders", Stream: "earn", Events: earnOrders,
					InitialLookback: 90 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "earn/orders-terminal", Stream: "earn", Adapter: earnOrders,
					InitialLookback: 90 * day, CheckpointOverlap: day,
					MaxWindow: 7 * day,
				},
				{
					Name: "earn/yield", Stream: "earn", Adapter: earnYield,
					InitialLookback: 90 * day, CheckpointOverlap: day,
					MaxWindow: 30 * day,
				},
				{
					Name: "p2p/orders", Stream: "p2p", Events: p2pOrders,
					InitialLookback: 180 * day, CheckpointOverlap: 7 * day,
					MaxWindow: 30 * day,
				},
				{
					Name: "p2p/orders-terminal", Stream: "p2p", Adapter: p2pOrders,
					InitialLookback: 180 * day, CheckpointOverlap: 7 * day,
					MaxWindow: 30 * day,
				},
			}, nil
		},
	}
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, spec); err != nil {
		fmt.Fprintln(os.Stderr, "bybitcrawl:", err)
		os.Exit(1)
	}
}
