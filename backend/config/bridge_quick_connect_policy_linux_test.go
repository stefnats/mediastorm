//go:build linux

package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"novastream/internal/settingscommit"
)

func quickConnectPolicyPG(t *testing.T) *securityPGFixture {
	t.Helper()
	f := securityPG(t)
	f.settings.JellyfinBridge.Enabled = true
	f.settings.JellyfinBridge.QuickConnect = true
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	return f
}
func TestQuickConnectPolicyOriginalSnapshot(t *testing.T) {
	for _, kind := range []string{"enabled", "disabled", "copied-observation", "copied-receipt", "busy-save", "original-cancel", "durable-policy", "cancelled-request"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPolicyPG(t)
			if kind == "disabled" {
				f.settings.JellyfinBridge.QuickConnect = false
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if kind == "cancelled-request" {
				cancel()
			}
			original, err := f.security.CaptureBridgeQuickConnectPolicy(ctx)
			if kind == "cancelled-request" {
				if err == nil || original != nil {
					t.Fatal("cancelled policy capture")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			hold, err := f.coordinator.TryProgressFileHold(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer hold.Close()
			if kind == "copied-observation" {
				copyOfOriginal := *original
				if fence, e := f.security.BindBridgeQuickConnectPolicy(ctx, &copyOfOriginal, hold); e == nil || fence != nil {
					t.Fatal("copied observation accepted")
				}
				return
			}
			fence, err := f.security.BindBridgeQuickConnectPolicy(ctx, original, hold)
			if err != nil {
				t.Fatal(err)
			}
			values, err := fence.Values()
			if err != nil || !values.BridgeEnabled || values.QuickConnectEnabled != (kind != "disabled") {
				t.Fatal("actual original flag snapshot", err)
			}
			if kind == "original-cancel" {
				cancel()
				if _, err = fence.Values(); err == nil {
					t.Fatal("cancelled original retained fence")
				}
				return
			}
			if kind == "busy-save" {
				if err = f.manager.Save(f.settings); err == nil {
					t.Fatal("native holder did not exclude actual Save")
				}
				if _, err = fence.Handoff(time.Now().Add(time.Minute)); err == nil {
					t.Fatal("retired policy attempt handed off")
				}
				return
			}
			receipt, err := fence.Handoff(time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "copied-receipt" {
				copyOfReceipt := *receipt
				if _, err = f.security.BindBridgeQuickConnectPolicyReceipt(ctx, &copyOfReceipt, hold); err == nil {
					t.Fatal("copied durable policy accepted")
				}
				return
			}
			if kind == "durable-policy" {
				_ = hold.Close()
				cancel()
				fresh, e := f.coordinator.TryProgressFileHold(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer fresh.Close()
				bound, e := f.security.BindBridgeQuickConnectPolicyReceipt(t.Context(), receipt, fresh)
				if e != nil {
					t.Fatal("distinct durable receipt depended on old HTTP ctx", e)
				}
				if _, e = bound.Values(); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
func TestQuickConnectPolicyMalformedAndLifetime(t *testing.T) {
	for _, kind := range []string{"missing-flag", "string-flag", "alias-flag", "oversized", "expired-handoff", "foreign-hold", "closed-security", "receipt-cannot-renew"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPolicyPG(t)
			if kind == "missing-flag" || kind == "string-flag" || kind == "alias-flag" || kind == "oversized" {
				raw, err := os.ReadFile(f.manager.path)
				if err != nil {
					t.Fatal(err)
				}
				var root map[string]json.RawMessage
				if json.Unmarshal(raw, &root) != nil {
					t.Fatal("own settings JSON")
				}
				var bridge map[string]json.RawMessage
				if json.Unmarshal(root["jellyfinBridge"], &bridge) != nil {
					t.Fatal("own bridge JSON")
				}
				switch kind {
				case "missing-flag":
					delete(bridge, "quickConnect")
				case "string-flag":
					bridge["quickConnect"] = json.RawMessage(`"true"`)
				case "alias-flag":
					bridge["QuickConnect"] = json.RawMessage(`true`)
				}
				root["jellyfinBridge"], _ = json.Marshal(bridge)
				raw, _ = json.Marshal(root)
				if kind == "oversized" {
					raw = []byte(strings.Repeat(" ", managedSnapshotLimit+1))
				}
				if err = os.WriteFile(f.manager.path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if original, err := f.security.CaptureBridgeQuickConnectPolicy(t.Context()); err == nil || original != nil {
					t.Fatal("invalid original QC flag authorized", kind)
				}
				return
			}
			original, err := f.security.CaptureBridgeQuickConnectPolicy(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			hold, err := f.coordinator.TryProgressFileHold(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer hold.Close()
			if kind == "foreign-hold" {
				objects, e := settingscommit.OpenLockObjects(t.Context(), filepath.Join(t.TempDir(), "foreign-settings.json"))
				if e != nil {
					t.Fatal(e)
				}
				defer objects.Close()
				foreign, e := settingscommit.NewForPool(t.Context(), f.pool, objects)
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				guard, e := foreign.TryWrite(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				if e = foreign.Arm(guard); e != nil {
					_ = guard.Close()
					t.Fatal(e)
				}
				_ = guard.Close()
				foreignHold, e := foreign.TryProgressFileHold(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer foreignHold.Close()
				if _, err = f.security.BindBridgeQuickConnectPolicy(t.Context(), original, foreignHold); err == nil {
					t.Fatal("genuine foreign native hold accepted")
				}
				return
			}
			fence, err := f.security.BindBridgeQuickConnectPolicy(t.Context(), original, hold)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "expired-handoff" {
				if _, err = fence.Handoff(time.Now().Add(-time.Second)); err == nil {
					t.Fatal("expired handoff accepted")
				}
				return
			}
			if kind == "closed-security" {
				_ = f.security.Close()
				if _, err = fence.Values(); err == nil {
					t.Fatal("closed managed owner retained values")
				}
				return
			}
			receipt, err := fence.Handoff(time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			bound, err := f.security.BindBridgeQuickConnectPolicyReceipt(t.Context(), receipt, hold)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = bound.Handoff(time.Now().Add(2 * time.Minute)); err == nil {
				t.Fatal("durable policy receipt renewed")
			}
		})
	}
}
