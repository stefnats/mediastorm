//go:build linux

package settingscommit

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSettingsNativeLinuxRejectsSymlinkAndRenamedWitness(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	o, err := OpenLockObjects(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	alias := filepath.Join(t.TempDir(), "alias")
	if err = os.Symlink(filepath.Join(o.directory, "commit-barrier.lock"), alias); err != nil {
		t.Fatal(err)
	}
	if file, err := nativeOpen(alias, false); err == nil {
		file.Close()
		t.Fatal("native operation followed a symbolic link")
	}
	if err = os.Rename(filepath.Join(o.directory, "mutation-turn.lock"), filepath.Join(o.directory, "old-turn")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(o.directory, "mutation-turn.lock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if hold, err := o.acquire(t.Context(), true); err == nil {
		hold.close()
		t.Fatal("fresh named object replaced original witness")
	}
}
func TestSettingsNativeLinuxProcessHelper(t *testing.T) {
	path := os.Getenv("MEDIASTORM_SYNTHETIC_SETTINGS_LOCK")
	if path == "" {
		return
	}
	f, err := nativeOpen(path, false)
	if err != nil {
		t.Fatal("open own process fixture")
	}
	defer f.Close()
	err = nativeLock(f, true, true)
	if err == nil {
		nativeUnlock(f)
		t.Fatal("independent process bypassed original EX")
	}
	if !nativeConflict(err) {
		t.Fatal("native conflict returned unrelated error")
	}
}
func TestSettingsNativeLinuxIndependentProcessCannotBypassGuard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	o, err := OpenLockObjects(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	guard, err := TryUnarmedWrite(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSettingsNativeLinuxProcessHelper$", "-test.timeout=10s")
	command.Env = append(os.Environ(), "MEDIASTORM_SYNTHETIC_SETTINGS_LOCK="+filepath.Join(o.directory, "commit-barrier.lock"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("independent native process did not observe exclusion: %s", output)
	}
}
