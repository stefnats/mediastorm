package datastore

import (
	"testing"

	"github.com/pressly/goose/v3"
)

// Startup discovery follows the complete embedded migration set. Tests for a
// specific migration or a refused downgrade retain their explicit versions.
func latestEmbeddedMigrationVersion(t *testing.T) int64 {
	t.Helper()
	entries, err := embedMigrations.ReadDir("migrations")
	if err != nil {
		t.Fatal("read embedded migrations", err)
	}
	var latest int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		version, err := goose.NumericComponent(entry.Name())
		if err != nil {
			t.Fatal("read embedded migration version", entry.Name(), err)
		}
		latest = max(latest, version)
	}
	if latest == 0 {
		t.Fatal("embedded migration set is empty")
	}
	return latest
}
