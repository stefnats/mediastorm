//go:build windows

package settingscommit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSettingsNativeWindowsIndependentHandlesAndPendingWriter(t *testing.T) {
	o, err := OpenLockObjects(t.Context(), filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	a, err := o.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	b, err := o.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	a.close()
	if writer, err := o.acquire(t.Context(), true); err == nil {
		writer.close()
		t.Fatal("closing one handle released another shared lock")
	}
	b.close()
	first, err := TryUnarmedWrite(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	done := make(chan error, 1)
	go func() {
		next, err := TryUnarmedWrite(t.Context(), o)
		if next != nil {
			next.Close()
		}
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("pending Windows turn returned before actual release")
	case <-time.After(30 * time.Millisecond):
	}
	first.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual pending LockFileEx did not complete")
	}
}
func TestSettingsNativeWindowsWitnessCannotBeReplacedOrArmedAdopted(t *testing.T) {
	o, err := OpenLockObjects(t.Context(), filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if err = os.Rename(filepath.Join(o.directory, "commit-barrier.lock"), filepath.Join(o.directory, "old-lock")); err == nil {
		t.Fatal("immutable held Windows file allowed replacement")
	}
	guard, err := TryUnarmedWrite(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.writeRecord(record{Version: 1, Family: nativeFamily, Mode: "armed", Source: "synthetic"}); err != nil {
		t.Fatal(err)
	}
	guard.Close()
	if next, err := TryUnarmedWrite(t.Context(), o); err == nil {
		next.Close()
		t.Fatal("armed Windows root became free legacy")
	}
}
