package config

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

func securityWritePlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "windows" {
		t.Skip("native write port unavailable on this platform")
	}
}
func TestSecurityPreflightOwnsCallerSliceMapAndNormalization(t *testing.T) {
	original := DefaultSettings()
	original.UsenetEngines = []UsenetEngineSettings{{Name: "one", Type: "nzbdav", Enabled: true, Config: map[string]string{"synthetic": "A"}, AllowedProfiles: []string{"one"}}, {Name: "two", Type: "nzbdav", Enabled: true}}
	before, _ := json.Marshal(original)
	owned, err := preflightSettings(NewManager(filepath.Join(t.TempDir(), "settings.json")), original)
	if err != nil {
		t.Fatal("pure preflight failed", err)
	}
	if !owned.UsenetEngines[0].Enabled || owned.UsenetEngines[1].Enabled {
		t.Fatal("accepted normalization changed its established engine choice")
	}
	owned.UsenetEngines[0].Config["synthetic"] = "B"
	owned.UsenetEngines[0].AllowedProfiles[0] = "two"
	after, _ := json.Marshal(original)
	if !bytes.Equal(before, after) {
		t.Fatal("preflight mutated caller slice/map backing storage")
	}
}
func TestSecurityPreflightBoundsCloneBeforeCompositeAllocation(t *testing.T) {
	for _, kind := range []string{"slice", "map"} {
		t.Run(kind, func(t *testing.T) {
			value := DefaultSettings()
			if kind == "slice" {
				value.UsenetEngines = make([]UsenetEngineSettings, 100001)
			} else {
				value.UsenetEngines = []UsenetEngineSettings{{Config: map[string]string{}}}
				for i := 0; i < 100001; i++ {
					value.UsenetEngines[0].Config[strconv.Itoa(i)] = "synthetic"
				}
			}
			directory := t.TempDir()
			if _, err := preflightSettings(NewManager(filepath.Join(directory, "settings.json")), value); err == nil {
				t.Fatal("unbounded composite passed pure preflight")
			}
			names, _ := os.ReadDir(directory)
			if len(names) != 0 {
				t.Fatal("preflight created guard or settings files")
			}
		})
	}
}
func TestSecurityOrdinarySaveSerializationAndRawPathErrors(t *testing.T) {
	securityWritePlatform(t)
	directory := t.TempDir()
	manager := NewManager(filepath.Join(directory, "settings.json"))
	settings := DefaultSettings()
	settings.UsenetEngines = []UsenetEngineSettings{{Name: "one", Enabled: true}, {Name: "two", Enabled: true}}
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- NewManager(manager.path).Save(settings) }()
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal("successful ordinary Save/Save serialization became denial", err)
		}
	}
	if !settings.UsenetEngines[1].Enabled {
		t.Fatal("accepted Save changed caller's shared input array")
	}
	raw, err := os.ReadFile(manager.path)
	if err != nil {
		t.Fatal(err)
	}
	var actual Settings
	if json.Unmarshal(raw, &actual) != nil || actual.UsenetEngines[1].Enabled {
		t.Fatal("saved established normalization not applied")
	}
	if _, err = os.Stat(manager.path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("owned settings temp survived successful rename")
	}
	if err = NewManager("").Save(settings); err == nil || err.Error() != "config path not set" {
		t.Fatal("empty raw path error changed")
	}
	invalid := filepath.Join(directory, "settings.json", "child.json")
	if err = NewManager(invalid).Save(settings); err == nil {
		t.Fatal("EnsureDir regular-file parent was silently normalized")
	}
	if !reflect.DeepEqual(settings.UsenetEngines, []UsenetEngineSettings{{Name: "one", Enabled: true}, {Name: "two", Enabled: true}}) {
		t.Fatal("failed Save mutated caller")
	}
}
func TestSecurityBootstrapDataReadDoesNotLoadMigrateOrCreate(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "settings.json")
	if value, exists, err := ReadManagedBootstrap(context.Background(), NewManager(path)); err != nil || exists || value.Database.URL != "" {
		t.Fatal("missing bootstrap data did not preserve default selection")
	}
	if names, _ := os.ReadDir(directory); len(names) != 0 {
		t.Fatal("pure bootstrap read created default or guard metadata")
	}
	original := []byte(`{"database":{"url":"synthetic-only","maxOpenConns":1},"security":{"enforceProfilePins":true},"unknown":{"preserved":true}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	value, exists, err := ReadManagedBootstrap(context.Background(), NewManager(path))
	if err != nil || !exists || value.Database.URL != "synthetic-only" || value.Security.EnforceProfilePins {
		t.Fatal("bootstrap DATA became loaded policy/settings")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(original, after) {
		t.Fatal("pure bootstrap read normalized original bytes")
	}
}
func TestSecurityAdvancedCapabilityMatchesPlatformWithoutLegacyShutdown(t *testing.T) {
	if recordingPolicySupported() != (runtime.GOOS == "linux") {
		t.Fatal("advanced availability did not match actual platform port")
	}
	if runtime.GOOS != "linux" {
		if observation, err := (&ManagedSecurity{}).CapturePolicy(context.Background()); observation != nil || err == nil {
			t.Fatal("unsupported advanced policy became ready")
		}
	}
}
