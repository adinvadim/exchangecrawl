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
	"sort"
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
	if spec.NewStreams == nil {
		return errors.New("crawler source stream factory is required")
	}
	availableStreams, err := spec.NewStreams(cfg.BaseURL)
	if err != nil {
		return err
	}
	if len(availableStreams) == 0 {
		return errors.New("crawler source stream registry is empty")
	}
	streams, streamStatuses, streamWarnings := configureStreams(availableStreams, cfg.Streams)
	for _, warning := range streamWarnings {
		fmt.Fprintln(stderr, warning)
	}
	if command == "doctor" {
		return runDoctor(ctx, stdout, spec, resolvedConfigPath, cfg, streams, streamStatuses, commandArgs)
	}
	if len(streams) == 0 {
		return errors.New("all source streams are disabled")
	}

	switch command {
	case "status":
		return runStatus(ctx, stdout, spec, resolvedConfigPath, cfg, streams, commandArgs)
	case "sync":
		return runSync(ctx, stdout, cfg, streams, commandArgs)
	case "entries":
		return runEntries(ctx, stdout, cfg, streams, commandArgs)
	case "events":
		return runEvents(ctx, stdout, cfg, streams, commandArgs)
	case "search":
		return runSearch(ctx, stdout, cfg, streams, commandArgs)
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
	Streams         []doctorStream  `json:"streams"`
	Ready           bool            `json:"ready"`
}

type doctorAccount struct {
	ID      string   `json:"id"`
	Label   string   `json:"label"`
	Ready   bool     `json:"ready"`
	Missing []string `json:"missing,omitempty"`
}

type doctorStream struct {
	Name              string `json:"name"`
	Stream            string `json:"stream"`
	Enabled           bool   `json:"enabled"`
	InitialLookback   string `json:"initial_lookback,omitempty"`
	CheckpointOverlap string `json:"checkpoint_overlap,omitempty"`
	MaxWindow         string `json:"max_window,omitempty"`
}

func runDoctor(ctx context.Context, stdout io.Writer, spec Spec, configPath string, cfg appconfig.Config, streams []source.StreamBinding, streamStatuses []doctorStream, args []string) error {
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
		Streams:      append([]doctorStream(nil), streamStatuses...),
		Ready:        true,
	}
	for _, account := range cfg.SourceAccounts() {
		status := streamCredentialStatus(streams, account)
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
		if checkErr := checkArchiveDatabase(ctx, cfg, streams); checkErr == nil {
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
		for _, stream := range report.Streams {
			fmt.Fprintf(stdout, "stream %s: module=%s enabled=%t", safeText(stream.Name), safeText(stream.Stream), stream.Enabled)
			if stream.InitialLookback != "" {
				fmt.Fprintf(stdout, " initial_lookback=%s", stream.InitialLookback)
			}
			if stream.CheckpointOverlap != "" {
				fmt.Fprintf(stdout, " checkpoint_overlap=%s", stream.CheckpointOverlap)
			}
			if stream.MaxWindow != "" {
				fmt.Fprintf(stdout, " max_window=%s", stream.MaxWindow)
			}
			fmt.Fprintln(stdout)
		}
	}
	if !report.Ready {
		return errors.New("doctor found missing credentials or an unreadable database")
	}
	return nil
}

func configureStreams(available []source.StreamBinding, configured map[string]appconfig.StreamConfig) ([]source.StreamBinding, []doctorStream, []string) {
	knownNames := make(map[string]struct{}, len(available)*2)
	for _, binding := range available {
		knownNames[binding.Name] = struct{}{}
		knownNames[binding.Stream] = struct{}{}
	}
	unknownNames := make([]string, 0)
	for name := range configured {
		if _, ok := knownNames[name]; !ok {
			unknownNames = append(unknownNames, name)
		}
	}
	sort.Strings(unknownNames)
	warnings := make([]string, 0, len(unknownNames))
	for _, name := range unknownNames {
		warnings = append(warnings, fmt.Sprintf("warning: configured stream %q does not match a binding or module", name))
	}

	enabled := make([]source.StreamBinding, 0, len(available))
	statuses := make([]doctorStream, 0, len(available))
	for _, original := range available {
		binding := original
		streamConfig, ok := configured[binding.Name]
		if !ok {
			streamConfig, ok = configured[binding.Stream]
		}
		isEnabled := true
		if ok {
			isEnabled = streamConfig.IsEnabled()
			applyStreamDuration(streamConfig.InitialLookback, &binding.InitialLookback)
			applyStreamDuration(streamConfig.CheckpointOverlap, &binding.CheckpointOverlap)
			applyStreamDuration(streamConfig.MaxWindow, &binding.MaxWindow)
		}
		statuses = append(statuses, doctorStream{
			Name:              binding.Name,
			Stream:            binding.Stream,
			Enabled:           isEnabled,
			InitialLookback:   formatStreamDuration(binding.InitialLookback),
			CheckpointOverlap: formatStreamDuration(binding.CheckpointOverlap),
			MaxWindow:         formatStreamDuration(binding.MaxWindow),
		})
		if isEnabled {
			enabled = append(enabled, binding)
		}
	}
	return enabled, statuses, warnings
}

func applyStreamDuration(value string, target *time.Duration) {
	if strings.TrimSpace(value) == "" {
		return
	}
	duration, _ := time.ParseDuration(value)
	*target = duration
}

func formatStreamDuration(value time.Duration) string {
	if value == 0 {
		return ""
	}
	return value.String()
}

func streamCredentialStatus(streams []source.StreamBinding, account source.Account) source.CredentialStatus {
	status := source.CredentialStatus{Ready: true}
	seen := make(map[string]struct{})
	for _, binding := range streams {
		var current source.CredentialStatus
		if binding.Adapter != nil {
			current = binding.Adapter.CheckCredentials(account)
		} else if binding.Events != nil {
			current = binding.Events.CheckCredentials(account)
		} else {
			status.Ready = false
			continue
		}
		if !current.Ready {
			status.Ready = false
		}
		for _, missing := range current.Missing {
			if _, ok := seen[missing]; ok {
				continue
			}
			seen[missing] = struct{}{}
			status.Missing = append(status.Missing, missing)
		}
	}
	return status
}

func runStatus(ctx context.Context, stdout io.Writer, spec Spec, configPath string, cfg appconfig.Config, streams []source.StreamBinding, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "print normalized crawlkit status JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("status takes flags only")
	}
	if len(streams) == 0 {
		return errors.New("crawler source stream registry is empty")
	}
	status := archive.Status{
		Exchange:     streamExchange(streams[0]),
		Path:         cfg.DBPath,
		AccountCount: len(cfg.Accounts),
	}
	if _, err := os.Stat(cfg.DBPath); err == nil {
		arc, err := openReadOnlyArchive(ctx, cfg, streams)
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
	} else {
		for _, account := range cfg.SourceAccounts() {
			for _, binding := range streams {
				status.Accounts = append(status.Accounts, archive.AccountStatus{
					AccountID: account.ID, Label: account.Label, Stream: binding.Name,
				})
			}
		}
	}
	normalized := normalizedStatus(spec, configPath, status)
	if *jsonOut {
		return writeJSON(stdout, normalized)
	}
	fmt.Fprintln(stdout, normalized.Summary)
	if normalized.LastSyncAt != "" {
		fmt.Fprintf(stdout, "last sync: %s\n", normalized.LastSyncAt)
	}
	for _, account := range status.Accounts {
		fmt.Fprintf(stdout, "%s/%s: checkpoint=", safeText(account.AccountID), safeText(account.Stream))
		if account.Checkpoint == nil {
			fmt.Fprintln(stdout, "never")
		} else {
			fmt.Fprintln(stdout, account.Checkpoint.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

func streamExchange(binding source.StreamBinding) model.Exchange {
	if binding.Adapter != nil {
		return binding.Adapter.Exchange()
	}
	if binding.Events != nil {
		return binding.Events.Exchange()
	}
	return ""
}

func normalizedStatus(spec Spec, configPath string, status archive.Status) control.Status {
	counts := []control.Count{
		control.NewCount("ledger_entries", "Ledger Entries", status.EntryCount),
		control.NewCount("state_transitions", "State Transitions", status.TransitionCount),
		control.NewCount("accounts", "Connected Accounts", int64(status.AccountCount)),
	}
	result := control.NewStatus(spec.ID, fmt.Sprintf("%d Ledger Entries and %d State Transitions across %d Connected Accounts", status.EntryCount, status.TransitionCount, status.AccountCount))
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

func runSync(ctx context.Context, stdout io.Writer, cfg appconfig.Config, streams []source.StreamBinding, args []string) error {
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
	arc, err := openArchive(ctx, cfg, streams)
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
		if report.Transitions == 0 {
			fmt.Fprintf(stdout, "%s: %d entries in %d pages\n", report.Exchange, report.Entries, report.Pages)
		} else {
			fmt.Fprintf(stdout, "%s: %d entries and %d transitions in %d pages\n", report.Exchange, report.Entries, report.Transitions, report.Pages)
		}
		for _, account := range report.Accounts {
			target := account.AccountID
			if len(streams) > 1 {
				target += "/" + account.Stream
			}
			fmt.Fprintf(stdout, "%s: entries=%d", safeText(target), account.Entries)
			if account.Transitions > 0 {
				fmt.Fprintf(stdout, " transitions=%d", account.Transitions)
			}
			fmt.Fprintf(stdout, " pages=%d", account.Pages)
			if account.Error != "" {
				fmt.Fprintf(stdout, " error=%s", safeText(account.Error))
			}
			fmt.Fprintln(stdout)
		}
	}
	return syncErr
}

func runEntries(ctx context.Context, stdout io.Writer, cfg appconfig.Config, streams []source.StreamBinding, args []string) error {
	fs := flag.NewFlagSet("entries", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	stream := fs.String("stream", "", "storage stream")
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
	arc, err := openExistingArchive(ctx, cfg, streams)
	if err != nil {
		return err
	}
	defer arc.Close()
	entries, err := arc.Entries(ctx, archive.EntryQuery{
		AccountID: strings.ToLower(strings.TrimSpace(*accountID)),
		Stream:    strings.TrimSpace(*stream),
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

func runEvents(ctx context.Context, stdout io.Writer, cfg appconfig.Config, streams []source.StreamBinding, args []string) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	stream := fs.String("stream", "", "storage stream")
	objectType := fs.String("object-type", "", "mutable object type")
	objectID := fs.String("object-id", "", "Exchange object id")
	status := fs.String("status", "", "raw Exchange object status")
	sinceRaw := fs.String("since", "", "RFC3339 or YYYY-MM-DD observation start")
	untilRaw := fs.String("until", "", "RFC3339 or YYYY-MM-DD observation end")
	limit := fs.Int("limit", 100, "maximum rows")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("events takes flags only")
	}
	since, err := optionalTime(*sinceRaw)
	if err != nil {
		return err
	}
	until, err := optionalTime(*untilRaw)
	if err != nil {
		return err
	}
	arc, err := openExistingArchive(ctx, cfg, streams)
	if err != nil {
		return err
	}
	defer arc.Close()
	events, err := arc.Events(ctx, archive.EventQuery{
		AccountID:  strings.ToLower(strings.TrimSpace(*accountID)),
		Stream:     strings.TrimSpace(*stream),
		ObjectType: strings.TrimSpace(*objectType),
		ObjectID:   strings.TrimSpace(*objectID),
		Status:     strings.TrimSpace(*status),
		Since:      since,
		Until:      until,
		Limit:      *limit,
	})
	if err != nil {
		return err
	}
	return writeEvents(stdout, events, *jsonOut)
}

func runSearch(ctx context.Context, stdout io.Writer, cfg appconfig.Config, streams []source.StreamBinding, args []string) error {
	query, flagArgs := searchArgs(args)
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.String("account", "", "Connected Account id")
	stream := fs.String("stream", "", "storage stream")
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
	arc, err := openExistingArchive(ctx, cfg, streams)
	if err != nil {
		return err
	}
	defer arc.Close()
	entries, err := arc.Search(ctx, archive.SearchQuery{
		Text:      query,
		AccountID: strings.ToLower(strings.TrimSpace(*accountID)),
		Stream:    strings.TrimSpace(*stream),
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

func writeEvents(stdout io.Writer, events []model.StateObservation, jsonOut bool) error {
	if jsonOut {
		return writeJSON(stdout, events)
	}
	for _, event := range events {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\n",
			event.ObservedAt.UTC().Format(time.RFC3339), safeText(event.AccountID),
			safeText(event.Stream), safeText(event.ObjectType), safeText(event.ObjectID), safeText(event.Status))
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

func openArchive(ctx context.Context, cfg appconfig.Config, streams []source.StreamBinding) (*archive.Archive, error) {
	return archive.Open(ctx, archive.Options{
		Path:     cfg.DBPath,
		Accounts: cfg.SourceAccounts(),
		Streams:  streams,
	})
}

func openExistingArchive(ctx context.Context, cfg appconfig.Config, streams []source.StreamBinding) (*archive.Archive, error) {
	if _, err := os.Stat(cfg.DBPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Archive does not exist at %s; run sync first", cfg.DBPath)
		}
		return nil, err
	}
	return openReadOnlyArchive(ctx, cfg, streams)
}

func openReadOnlyArchive(ctx context.Context, cfg appconfig.Config, streams []source.StreamBinding) (*archive.Archive, error) {
	return archive.Open(ctx, archive.Options{
		Path:     cfg.DBPath,
		Accounts: cfg.SourceAccounts(),
		Streams:  streams,
		ReadOnly: true,
	})
}

func checkArchiveDatabase(ctx context.Context, cfg appconfig.Config, streams []source.StreamBinding) error {
	arc, err := archive.Open(ctx, archive.Options{
		Path:           cfg.DBPath,
		Accounts:       cfg.SourceAccounts(),
		Streams:        streams,
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
  events     List local State Transitions.
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
