//go:build darwin

package settingscommit

import (
	"path/filepath"
	"testing"
)

func TestSettingsNativeDarwinIndependentDescriptionsKeepOtherSharedHold(t *testing.T) {
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
		t.Fatal("closing another OFD released the actual shared lock")
	}
	b.close()
	writer, err := TryUnarmedWrite(t.Context(), o)
	if err != nil {
		t.Fatal("ordinary Darwin write unavailable", err)
	}
	writer.Close()
}
func TestSettingsNativeDarwinArmedRootCannotUseLegacyWrite(t *testing.T) {
	o, err := OpenLockObjects(t.Context(), filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
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
		t.Fatal("armed Darwin root became free legacy")
	}
}
