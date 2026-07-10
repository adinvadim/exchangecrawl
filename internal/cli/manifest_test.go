package cli

import (
	"testing"

	"github.com/openclaw/crawlkit/control"
)

func TestControlManifestKeepsCrawlerIdentityAndReadOnlyQueries(t *testing.T) {
	t.Parallel()

	manifest := controlManifest(Spec{
		ID:          "bybitcrawl",
		DisplayName: "Bybit Crawl",
		Description: "Local-first Bybit ledger archive.",
	}, control.Paths{
		DefaultConfig:   "/config/bybitcrawl.toml",
		DefaultDatabase: "/data/bybitcrawl.db",
	})

	if manifest.SchemaVersion != control.SchemaVersion || manifest.ID != "bybitcrawl" {
		t.Fatalf("manifest identity = %#v", manifest)
	}
	if manifest.Commands["sync"].Mutates != true {
		t.Fatal("sync command must be marked mutating")
	}
	if manifest.Commands["entries"].Mutates {
		t.Fatal("entries command must be read-only")
	}
	if manifest.Privacy.ExportsSecrets {
		t.Fatal("manifest must state that secrets are not exported")
	}
}
