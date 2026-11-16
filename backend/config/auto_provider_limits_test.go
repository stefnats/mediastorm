package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAutoProviderLimitsDefaultOffAndPersistOnlyExplicitOptIn(t *testing.T) {
	if DefaultSettings().Live.AutoAdoptProviderLimits {
		t.Fatal("default enabled")
	}
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(`{"live":{"maxStreams":3}}`), 0600); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(path)
	s, err := manager.Load()
	if err != nil || s.Live.AutoAdoptProviderLimits {
		t.Fatal("legacy settings activated policy", err)
	}
	s.Live.AutoAdoptProviderLimits = true
	if err := manager.Save(s); err != nil {
		t.Fatal(err)
	}
	again, err := NewManager(path).Load()
	if err != nil || !again.Live.AutoAdoptProviderLimits || again.Live.MaxStreams != 3 {
		t.Fatal("opt-in/manual fallback not retained", err)
	}
	again.Live.AutoAdoptProviderLimits = false
	if err := manager.Save(again); err != nil {
		t.Fatal(err)
	}
	again, err = manager.Load()
	if err != nil || again.Live.AutoAdoptProviderLimits {
		t.Fatal("opt-out not retained", err)
	}
}
