package cli

import (
	"github.com/openclaw/crawlkit/control"

	"github.com/adinvadim/exchangecrawl/internal/appconfig"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

type Spec struct {
	ID          string
	DisplayName string
	Description string
	SymbolName  string
	AccentColor string
	Version     string
	Config      appconfig.Spec
	NewAdapter  func(baseURL string) (source.Adapter, error)
}

func controlManifest(spec Spec, paths control.Paths) control.Manifest {
	manifest := control.NewManifest(spec.ID, spec.DisplayName, spec.ID)
	manifest.Description = spec.Description
	manifest.Branding = control.Branding{
		SymbolName:  spec.SymbolName,
		AccentColor: spec.AccentColor,
	}
	manifest.Paths = paths
	manifest.Capabilities = []string{"metadata", "status", "doctor", "sync", "entries", "search"}
	manifest.Privacy = control.Privacy{
		ContainsPrivateMessages: false,
		ExportsSecrets:          false,
		LocalOnlyScopes:         []string{"credentials", "private-financial-data", "sqlite"},
	}
	manifest.Commands = map[string]control.Command{
		"status":  {Title: "Status", Argv: []string{spec.ID, "status", "--json"}, JSON: true},
		"doctor":  {Title: "Doctor", Argv: []string{spec.ID, "doctor", "--json"}, JSON: true},
		"sync":    {Title: "Sync ledger", Argv: []string{spec.ID, "sync", "--json"}, JSON: true, Mutates: true},
		"entries": {Title: "Recent ledger entries", Argv: []string{spec.ID, "entries", "--json"}, JSON: true},
	}
	return manifest
}
