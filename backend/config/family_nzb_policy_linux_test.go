//go:build linux

package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"novastream/internal/settingscommit"
)

func familyNZBNative(t *testing.T) (*securityPGFixture, *FamilyNZBRoot) {
	t.Helper()
	f := securityPG(t)
	f.settings.Cache.Directory = "cache"
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	r, err := CaptureFamilyNZBWorkingDirectory(t.Context(), f.manager)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	root, err := f.security.PrepareFamilyNZBRoot(t.Context(), r)
	if err != nil || root != filepath.Join(r.cwdName, "cache", "streams") {
		t.Fatal("actual original relative cache preparation", err)
	}
	return f, r
}
func familyNZBFence(t *testing.T, f *securityPGFixture, r *FamilyNZBRoot) (*FamilyNZBPolicyObservation, *FamilyNZBPolicyFence, *settingscommit.ProgressFileHold) {
	t.Helper()
	o, err := f.security.CaptureFamilyNZBPolicy(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	h, err := f.coordinator.TryProgressFileHold(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	fence, err := f.security.BindFamilyNZBPolicy(t.Context(), o, h)
	if err != nil {
		t.Fatal(err)
	}
	return o, fence, h
}
func TestFamilyNZBOriginalNativePolicy(t *testing.T) {
	t.Run("same-actual-domain-root-and-fence", func(t *testing.T) {
		f, r := familyNZBNative(t)
		_, fence, h := familyNZBFence(t, f, r)
		values, err := fence.Values()
		if err != nil || values.MetadataRoot != filepath.Join(r.cwdName, "cache", "streams") || values.EnforceProfilePins != f.settings.Security.EnforceProfilePins {
			t.Fatal("original native values", err)
		}
		if info, err := r.cwd.Stat(); err != nil || !os.SameFile(info, r.cwdInfo) {
			t.Fatal("original cwd descriptor absent", err)
		}
		if err = h.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = fence.Values(); err == nil {
			t.Fatal("closed actual native hold retained DATA")
		}
	})
	t.Run("foreign-copy-zero-and-expired-denied", func(t *testing.T) {
		f, r := familyNZBNative(t)
		o, fence, h := familyNZBFence(t, f, r)
		copyRoot := *r
		copyObservation := *o
		copyFence := *fence
		if _, err := f.security.CaptureFamilyNZBPolicy(t.Context(), &copyRoot); err == nil {
			t.Fatal("copied physical root accepted")
		}
		if _, err := f.security.BindFamilyNZBPolicy(t.Context(), &copyObservation, h); err == nil {
			t.Fatal("copied policy accepted")
		}
		if _, err := copyFence.Values(); err == nil {
			t.Fatal("copied fence accepted")
		}
		if _, err := (&FamilyNZBPolicyFence{}).Values(); err == nil {
			t.Fatal("zero fence accepted")
		}
		for _, v := range []any{r, *r, o, *o, fence, *fence} {
			b, err := json.Marshal(v)
			if err != nil || string(b) != "null" || fmt.Sprintf("%#v", v) == "" {
				t.Fatal("opaque policy representation", err)
			}
		}
		var imported FamilyNZBRoot
		if json.Unmarshal([]byte("{}"), &imported) == nil {
			t.Fatal("root imported from JSON")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := f.security.CaptureFamilyNZBPolicy(ctx, r); err == nil {
			t.Fatal("cancelled operation accepted")
		}
		if _, err := f.security.CaptureFamilyNZBPolicy(t.Context(), r); err != nil {
			t.Fatal("cancelled local query retired original root", err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := r.cwd.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("actual original cwd not closed", err)
		}
		if _, err := f.security.CaptureFamilyNZBPolicy(t.Context(), r); err == nil {
			t.Fatal("closed root renewed")
		}
	})
	t.Run("refused-native-save-retires-original", func(t *testing.T) {
		f, r := familyNZBNative(t)
		old, _, hold := familyNZBFence(t, f, r)
		f.settings.Security.EnforceProfilePins = false
		if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
			t.Fatal("actual held native writer did not refuse", err)
		}
		if _, err := f.security.BindFamilyNZBPolicy(t.Context(), old, hold); err == nil {
			t.Fatal("old source policy survived refused save attempt")
		}
		fresh, err := f.security.CaptureFamilyNZBPolicy(t.Context(), r)
		if err != nil {
			t.Fatal("new source could not observe actual unchanged native file", err)
		}
		fence, err := f.security.BindFamilyNZBPolicy(t.Context(), fresh, hold)
		if err != nil {
			t.Fatal(err)
		}
		values, err := fence.Values()
		if err != nil || !values.EnforceProfilePins {
			t.Fatal("refused write was treated as committed", err)
		}
		if err = hold.Close(); err != nil {
			t.Fatal(err)
		}
		if err = f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		_, next, nextHold := familyNZBFence(t, f, r)
		defer nextHold.Close()
		values, err = next.Values()
		if err != nil || values.EnforceProfilePins {
			t.Fatal("new source did not read actual successful same-cache policy", err)
		}
		if _, err = f.security.BindFamilyNZBPolicy(t.Context(), fresh, nextHold); err == nil {
			t.Fatal("prior source policy renewed after real save")
		}
	})
	t.Run("physical-config-and-root-change-denied", func(t *testing.T) {
		// Actual Linux cwd/native names are used, never PWD or a fake resolver.
		f := securityPG(t)
		t.Chdir(filepath.Dir(f.manager.path))
		f.manager.path = filepath.Base(f.manager.path)
		f.settings.Cache.Directory = "cache"
		if err := f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		r, err := CaptureFamilyNZBWorkingDirectory(t.Context(), f.manager)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if _, err = f.security.PrepareFamilyNZBRoot(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		originalAbsolute := r.nativeAbsolute
		other := t.TempDir()
		if err = os.WriteFile(filepath.Join(other, f.manager.path), []byte(`{"cache":{"directory":"FOREIGN"}}`), 0600); err != nil {
			t.Fatal(err)
		}
		t.Chdir(other)
		if _, err = f.security.CaptureFamilyNZBPolicy(t.Context(), r); err == nil {
			t.Fatal("fresh cwd replaced original native domain")
		}
		if r.nativeAbsolute != originalAbsolute {
			t.Fatal("native absolute target adopted a fresh cwd")
		}
		t.Chdir(r.cwdName)
		f.settings.Cache.Directory = "changed-cache"
		if err = f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		if _, err = f.security.PrepareFamilyNZBRoot(t.Context(), r); err == nil {
			t.Fatal("repeated preparation returned stale cache DATA")
		}
		f.settings.Cache.Directory = "cache"
		if err = f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		if _, err = f.security.PrepareFamilyNZBRoot(t.Context(), r); err == nil {
			t.Fatal("restored raw cache revived failed original root")
		}
		cwdRoot, err := CaptureFamilyNZBWorkingDirectory(t.Context(), f.manager)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cwdRoot.Close() })
		if _, err = f.security.PrepareFamilyNZBRoot(t.Context(), cwdRoot); err != nil {
			t.Fatal(err)
		}
		originalName := cwdRoot.cwdName
		retiredName := originalName + ".original-nzb-cwd"
		moved, replaced := false, false
		restore := func() {
			if replaced {
				_ = os.Remove(originalName)
				replaced = false
			}
			if moved {
				_ = os.Rename(retiredName, originalName)
				moved = false
			}
		}
		t.Cleanup(restore)
		if err = os.Rename(originalName, retiredName); err != nil {
			t.Fatal(err)
		}
		moved = true
		if err = os.Mkdir(originalName, 0700); err != nil {
			t.Fatal(err)
		}
		replaced = true
		local, cancelLocal := context.WithCancel(t.Context())
		observed := checkFamilyNZBDirectory(local, cwdRoot)
		if !errors.Is(observed, errFamilyNZBRootChanged) {
			t.Fatal("actual named cwd identity change was not observed", observed)
		}
		cancelLocal()
		if cwdRoot.observeDirectoryResult(observed) == nil || cwdRoot.ctx.Err() == nil {
			t.Fatal("late local cancellation erased known physical identity change")
		}
		if _, err = cwdRoot.cwd.Stat(); err != nil {
			t.Fatal("genuine original directory descriptor missing", err)
		}
		restore()
		if _, err = f.security.CaptureFamilyNZBPolicy(t.Context(), cwdRoot); err == nil {
			t.Fatal("restored name revived observed replaced cwd")
		}
		// Invalid UTF8 is a real filesystem name; it must fail before receipt.
		bad := filepath.Join(t.TempDir(), string([]byte{'b', 0xff}))
		if err = os.Mkdir(bad, 0700); err != nil {
			t.Fatal(err)
		}
		t.Chdir(bad)
		if actual, err := CaptureFamilyNZBWorkingDirectory(t.Context(), NewManager("settings.json")); err == nil || actual != nil {
			if actual != nil {
				_ = actual.Close()
			}
			t.Fatal("invalid UTF8 cwd published original receipt")
		}
	})
}
