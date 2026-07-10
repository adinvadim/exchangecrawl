package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/openclaw/crawlkit/config"
	"github.com/openclaw/crawlkit/control"

	"github.com/adinvadim/exchangecrawl/internal/appconfig"
	"github.com/adinvadim/exchangecrawl/internal/archive"
	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, spec Spec) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if strings.TrimSpace(spec.ID) == "" || spec.Config.AppID != spec.ID {
		return errors.New("crawler specification has inconsistent app identity")
	}
	if len(args) == 0 {
		printUsage(stdout, spec)
		return nil
	}

	global := flag.NewFlagSet(spec.ID, flag.ContinueOnError)
	global.SetOutput(stderr)
	configPath := global.String("config", "", "config file path")
	dbPath := global.String("db", "", "database path override")
	versionFlag := global.Bool("version", false, "print version")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage(stdout, spec)
			return nil
		}
		return err
	}
	if *versionFlag {
		fmt.Fprintln(stdout, versionString(spec))
		return nil
	}
	rest := global.Args()
	if len(rest) == 0 || rest[0] == "help" {
		printUsage(stdout, spec)
		return nil
	}
	command, commandArgs := rest[0], rest[1:]
	switch command {
	case "version":
		fmt.Fprintln(stdout, versionString(spec))
		return nil
	case "metadata":
		return runMetadata(stdout, spec, commandArgs)
	case "init":
		if len(commandArgs) != 0 {
			return errors.New("init takes no arguments")
		}
		path, err := appconfig.WriteStarter(spec.Config, *configPath)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "wrote %s\n", path)
		return nil
	}

	cfg, resolvedConfigPath, err := appconfig.Load(spec.Config, *configPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) != "" {
		cfg.DBPath, err = filepath.Abs(config.ExpandHome(*dbPath))
		if err != nil {
			return err
		}
	}
	if spec.NewAdapter == nil {
		return errors.New("crawler source adapter factory is required")
	}
	adapter, err := spec.NewAdapter(cfg.BaseURL)
	if err != nil {
		return err
	}

	switch command {
	case "doctor":
		return runDoctor(ctx, stdout, spec, resolvedConfigPath, cfg, adapter, commandArgs)
	case "status":
		return runStatus(ctx, stdout, spec, resolvedConfigPath, cfg, adapter, commandArgs)
	case "sync":
		return runSync(ctx, stdout, cfg, adapter, commandArgs)
	case "entries":
		return runEntries(ctx, stdout, cfg, adapter, commandArgs)
	case "search":
		return runSearch(ctx, stdout, cfg, adapter, commandArgs)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func runMetadata(stdout io.Writer, spec Spec, args []string) error {
	fs := flag.NewFlagSet("metadata", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	_ = fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("metadata takes flags only")
	}
	defaults, err := appconfig.Default(spec.Config)
	if err != nil {
		return err
	}
	configPath, err := appconfig.DefaultPath(spec.Config)
	if err != nil {
		return err
	}
	paths := control.Paths{
		DefaultConfig:   configPath,
		ConfigEnv:       strings.ToUpper(strings.ReplaceAll(spec.ID, "-", "_")) + "_CONFIG",
		DefaultDatabase: defaults.DBPath,
		DefaultLogs:     filepath.Join(filepath.Dir(defaults.DBPath), "logs"),
	}
	return writeJSON(stdout, controlManifest(spec, paths))
}

type doctorReport struct {
	AppID           string          `json:"app_id"`
	ConfigPath      string          `json:"config_path"`
	DatabasePath    string          `json:"database_path"`
	DatabasePresent bool            `json:"database_present"`
	DatabaseReady   bool            `json:"database_ready"`
	Accounts        []doctorAccount `json:"accounts"`
	Ready           bool            `json:"ready"`
}

type doctorAccount struct {
	ID      string   `json:"id"`
	Label   string   `json:"label"`
	Ready   bool     `json:"ready"`
	Missing []string `json:"missing,omitempty"`
}

func runDoctor(ctx context.Context, stdout io.Writer, spec Spec, configPath string, cfg appconfig.Config, adapter source.Adapter, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("doctor takes flags only")
	}
	report := doctorReport{
		AppID:        spec.ID,
		ConfigPath:   configPath,
		DatabasePath: cfg.DBPath,
		Ready:        true,
	}
	for _, account := range cfg.SourceAccounts() {
		status := adapter.CheckCredentials(account)
		report.Accounts = append(report.Accounts, doctorAccount{
			ID:      account.ID,
			Label:   account.Label,
			Ready:   status.Ready,
			Missing: append([]string(nil), status.Missing...),
		})
		if !status.Ready {
			report.Ready = false
		}
	}
	if _, err := os.Stat(cfg.DBPath); err == nil {
		report.DatabasePresent = true
		if checkErr := checkArchiveDatabase(ctx, cfg, adapter); checkErr == nil {
			report.DatabaseReady = true
		} else {
			report.Ready = false
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		report.Ready = false
	}
	if *jsonOut {
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(stdout, "%s: ready=%t database=%s present=%t\n", spec.ID, report.Ready, report.DatabasePath, report.DatabasePresent)
		for _, account := range report.Accounts {
			fmt.Fprintf(stdout, "%s: credentials ready=%t", account.ID, account.Ready)
			if len(account.Missing) > 0 {
				fmt.Fprintf(stdout, " missing=%s", strings.Join(account.Missing, ","))
			}
			fmt.Fprintln(stdout)
		}
	}
	if !report.Ready {
		return errors.New("doctor found missing credentials or an unreadable database")
	}
	return nil
}

func runStatus(ctx context.Context, stdout io.Writer, spec Spec, configPath string, cfg appconfig.Config, adapter source.Adapter, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "print normalized crawlkit status JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("status takes flags only")
	}
	status := archive.Status{
		Exchange:     adapter.Exchange(),
		Path:         cfg.DBPath,
		AccountCount: len(cfg.Accounts),
	}
	if _, err := os.Stat(cfg.DBPath); err == nil {
		arc, err := openReadOnlyArchive(ctx, cfg, adapter)
		if err != nil {
			return err
		}
		defer arc.Close()
		status, err = arc.Status(ctx)
		if err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	normalized := normalizedStatus(spec, configPath, status)
	if *jsonOut {
		return writeJSON(stdout, normalized)
	}
	fmt.Fprintln(stdout, normalized.Summary)
	if normalized.LastSyncAt != "" {
		fmt.Fprintf(stdout, "last sync: %s\n", normalized.LastSyncAt)
	}
	return nil
}

func normalizedStatus(spec Spec, configPath string, status archive.Status) control.Status {
	counts := []control.Count{
		control.NewCount("ledger_entries", "Ledger Entries", status.EntryCount),
		control.NewCount("accounts", "Connected Accounts", int64(status.AccountCount)),
	}
	result := control.NewStatus(spec.ID, fmt.Sprintf("%d Ledger Entries across %d Connected Accounts", status.EntryCount, status.AccountCount))
	result.State = "empty"
	result.ConfigPath = configPath
	result.DatabasePath = status.Path
	result.Counts = counts
	if status.LastSyncAt != nil {
		result.State = "current"
		result.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
	}
	database := control.SQLiteDatabase("primary", spec.DisplayName+" archive", "archive", status.Path, true, counts)
	result.DatabaseBytes = database.Bytes
	result.Databases = []control.Database{database}
	return result
}

func runSync(ctx context.Context, stdout io.Writer, cfg appconfig.Config, adapter source.Adapter, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	sinceRaw := fs.String("since", "", "RFC3339 or YYYY-MM-DD start")
	untilRaw := fs.String("until", "", "RFC3339 or YYYY-MM-DD end")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("sync takes flags only")
	}
	since, err := optionalTime(*sinceRaw)
	if err != nil {
		return err
	}
	until, err := optionalTime(*untilRaw)
	if err != nil {
		return err
	}
	arc, err := openArchive(ctx, cfg, adapter)
	if err != nil {
		return err
	}
	defer arc.Close()
	report, syncErr := arc.Sync(ctx, archive.SyncRequest{
		AccountID:       strings.ToLower(strings.TrimSpace(*accountID)),
		Since:           since,
		Until:           until,
		InitialLookback: cfg.Lookback(),
	})
	if *jsonOut {
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(stdout, "%s: %d entries in %d pages\n", report.Exchange, report.Entries, report.Pages)
		for _, account := range report.Accounts {
			fmt.Fprintf(stdout, "%s: entries=%d pages=%d", account.AccountID, account.Entries, account.Pages)
			if account.Error != "" {
				fmt.Fprintf(stdout, " error=%s", safeText(account.Error))
			}
			fmt.Fprintln(stdout)
		}
	}
	return syncErr
}

func runEntries(ctx context.Context, stdout io.Writer, cfg appconfig.Config, adapter source.Adapter, args []string) error {
	fs := flag.NewFlagSet("entries", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	symbol := fs.String("symbol", "", "Exchange symbol")
	entryType := fs.String("type", "", "Ledger Entry type")
	sinceRaw := fs.String("since", "", "RFC3339 or YYYY-MM-DD start")
	untilRaw := fs.String("until", "", "RFC3339 or YYYY-MM-DD end")
	limit := fs.Int("limit", 100, "maximum rows")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("entries takes flags only")
	}
	since, err := optionalTime(*sinceRaw)
	if err != nil {
		return err
	}
	until, err := optionalTime(*untilRaw)
	if err != nil {
		return err
	}
	arc, err := openExistingArchive(ctx, cfg, adapter)
	if err != nil {
		return err
	}
	defer arc.Close()
	entries, err := arc.Entries(ctx, archive.EntryQuery{
		AccountID: strings.ToLower(strings.TrimSpace(*accountID)),
		Symbol:    strings.ToUpper(strings.TrimSpace(*symbol)),
		Type:      strings.ToUpper(strings.TrimSpace(*entryType)),
		Since:     since,
		Until:     until,
		Limit:     *limit,
	})
	if err != nil {
		return err
	}
	return writeEntries(stdout, entries, *jsonOut)
}

func runSearch(ctx context.Context, stdout io.Writer, cfg appconfig.Config, adapter source.Adapter, args []string) error {
	query, flagArgs := searchArgs(args)
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	limit := fs.Int("limit", 100, "maximum rows")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if query == "" {
		if fs.NArg() != 1 {
			return errors.New("search requires exactly one query")
		}
		query = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return errors.New("search requires exactly one query")
	}
	arc, err := openExistingArchive(ctx, cfg, adapter)
	if err != nil {
		return err
	}
	defer arc.Close()
	entries, err := arc.Search(ctx, archive.SearchQuery{
		Text:      query,
		AccountID: strings.ToLower(strings.TrimSpace(*accountID)),
		Limit:     *limit,
	})
	if err != nil {
		return err
	}
	return writeEntries(stdout, entries, *jsonOut)
}

func searchArgs(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
}

func writeEntries(stdout io.Writer, entries []model.LedgerEntry, jsonOut bool) error {
	if jsonOut {
		return writeJSON(stdout, entries)
	}
	for _, entry := range entries {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\n",
			entry.OccurredAt.UTC().Format(time.RFC3339), safeText(entry.AccountID),
			safeText(entry.Symbol), safeText(entry.Type), safeText(entry.Amount), safeText(entry.Asset))
	}
	return nil
}

func safeText(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return ' '
		}
		return r
	}, value)
	return strings.TrimSpace(value)
}

func openArchive(ctx context.Context, cfg appconfig.Config, adapter source.Adapter) (*archive.Archive, error) {
	return archive.Open(ctx, archive.Options{
		Path:     cfg.DBPath,
		Accounts: cfg.SourceAccounts(),
		Adapter:  adapter,
	})
}

func openExistingArchive(ctx context.Context, cfg appconfig.Config, adapter source.Adapter) (*archive.Archive, error) {
	if _, err := os.Stat(cfg.DBPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Archive does not exist at %s; run sync first", cfg.DBPath)
		}
		return nil, err
	}
	return openReadOnlyArchive(ctx, cfg, adapter)
}

func openReadOnlyArchive(ctx context.Context, cfg appconfig.Config, adapter source.Adapter) (*archive.Archive, error) {
	return archive.Open(ctx, archive.Options{
		Path:     cfg.DBPath,
		Accounts: cfg.SourceAccounts(),
		Adapter:  adapter,
		ReadOnly: true,
	})
}

func checkArchiveDatabase(ctx context.Context, cfg appconfig.Config, adapter source.Adapter) error {
	arc, err := archive.Open(ctx, archive.Options{
		Path:           cfg.DBPath,
		Accounts:       cfg.SourceAccounts(),
		Adapter:        adapter,
		ReadOnly:       true,
		CheckIntegrity: true,
	})
	if err != nil {
		return err
	}
	return arc.Close()
}

func optionalTime(value string) (*time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parsed, err := parseTime(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func writeJSON(stdout io.Writer, value any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printUsage(stdout io.Writer, spec Spec) {
	fmt.Fprintf(stdout, `%s keeps a local, read-only Exchange ledger Archive.

Usage:
  %s [--config PATH] [--db PATH] <command> [flags]

Commands:
  init       Write a secret-free starter config.
  doctor     Check config, credential presence, and database health.
  sync       Read Ledger Entries from the Exchange.
  entries    List local Ledger Entries.
  search     Search the local Archive.
  status     Show local Archive status.
  metadata   Print crawlkit control metadata.
  version    Print version.
`, spec.ID, spec.ID)
}

func versionString(spec Spec) string {
	if strings.TrimSpace(spec.Version) == "" {
		return "dev"
	}
	return spec.Version
}
