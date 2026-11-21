//go:build linux

package config

import (
	"context"
	"encoding/json"
	"errors"
	"novastream/internal/settingscommit"
	"os"
	"path/filepath"
	"testing"
)

func TestSABPublicationNativePolicy(t *testing.T) {
	for _, name := range []string{"captured-stage-and-domain-only", "configuration-replacement-aba", "writer-attempt-retires-original", "malformed-or-wrong-domain-denied", "copied-zero-closed-observation-denied"} {
		t.Run(name, func(t *testing.T) {
			f := securityPG(t)
			enabled := true
			f.settings.SABnzbd.Enabled = &enabled
			f.settings.SABnzbd.FallbackHost = "http://synthetic.invalid"
			f.settings.SABnzbd.FallbackAPIKey = "synthetic-unused-key"
			f.settings.SABnzbd.ImportStageTargetRoot = t.TempDir()
			f.settings.SABnzbd.ImportStageMaxBytes = 32 << 20
			if e := f.manager.Save(f.settings); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original, e := f.security.CaptureSABPublicationPolicy(ctx)
			if e != nil {
				t.Fatal(e)
			}
			hold, e := f.coordinator.TryProgressFileHold(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer hold.Close()
			fence, e := f.security.BindSABPublicationPolicy(ctx, original, hold)
			if e != nil {
				t.Fatal(e)
			}
			values, e := fence.Values()
			if e != nil || values.StageRoot != f.settings.SABnzbd.ImportStageTargetRoot || values.MaxBytes != 32<<20 || len(values.SourceFingerprint) != 64 || values.EnforceProfilePins != f.settings.Security.EnforceProfilePins {
				t.Fatal("actual native values missing", e)
			}
			switch name {
			case "captured-stage-and-domain-only":
				if e = hold.Close(); e != nil {
					t.Fatal(e)
				}
				if _, e = fence.Values(); e == nil {
					t.Fatal("closed native hold retained values")
				}
			case "configuration-replacement-aba":
				raw, e := os.ReadFile(f.manager.path)
				if e != nil {
					t.Fatal(e)
				}
				next := filepath.Join(filepath.Dir(f.manager.path), "replacement.json")
				if e = os.WriteFile(next, raw, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(next, f.manager.path); e != nil {
					t.Fatal(e)
				}
				if _, e = f.security.BindSABPublicationPolicy(ctx, original, hold); e == nil {
					t.Fatal("same bytes replaced original native inode")
				}
			case "writer-attempt-retires-original":
				e = f.manager.Save(f.settings)
				if !errors.Is(e, settingscommit.ErrBusy) {
					t.Fatal("actual native writer did not encounter original read hold", e)
				}
				if _, e = f.security.BindSABPublicationPolicy(ctx, original, hold); e == nil {
					t.Fatal("writer attempt renewed original")
				}
			case "malformed-or-wrong-domain-denied":
				if e = os.WriteFile(f.manager.path, []byte(`{"security":{"enforceProfilePins":false},"sabnzbd":{"enabled":true},"database":{"url":"postgresql://synthetic.invalid/foreign"}}`), 0600); e != nil {
					t.Fatal(e)
				}
				if _, e = f.security.CaptureSABPublicationPolicy(ctx); e == nil {
					t.Fatal("incomplete or foreign native file gained defaults")
				}
				altered := f.settings
				altered.Database.URL = "postgresql://synthetic.invalid/foreign"
				wire, e := json.Marshal(altered)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(f.manager.path, wire, 0600); e != nil {
					t.Fatal(e)
				}
				if _, e = f.security.CaptureSABPublicationPolicy(ctx); e == nil {
					t.Fatal("well-formed foreign PG domain borrowed native authority")
				}

			case "copied-zero-closed-observation-denied":
				copied := *original
				if _, e = f.security.BindSABPublicationPolicy(ctx, &copied, hold); e == nil {
					t.Fatal("copied native observation accepted")
				}
				if _, e = (&SABPublicationPolicyFence{}).Values(); e == nil {
					t.Fatal("zero fence accepted")
				}
				cancel()
				if _, e = fence.Values(); e == nil {
					t.Fatal("original cancellation retained fence")
				}
			}
		})
	}
}
