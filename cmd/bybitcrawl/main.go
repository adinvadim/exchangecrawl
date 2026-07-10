package main

import (
	"context"
	"fmt"
	"os"

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
		},
		NewAdapter: func(baseURL string) (source.Adapter, error) {
			return bybit.New(bybit.Options{BaseURL: baseURL})
		},
	}
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, spec); err != nil {
		fmt.Fprintln(os.Stderr, "bybitcrawl:", err)
		os.Exit(1)
	}
}
