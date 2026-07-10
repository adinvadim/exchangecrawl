package main

import (
	"context"
	"fmt"
	"os"

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
		Description: "Local-first Binance USDⓈ-M Futures ledger archive.",
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
		NewAdapter: func(baseURL string) (source.Adapter, error) {
			return binance.New(binance.Options{BaseURL: baseURL})
		},
	}
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, spec); err != nil {
		fmt.Fprintln(os.Stderr, "binancecrawl:", err)
		os.Exit(1)
	}
}
