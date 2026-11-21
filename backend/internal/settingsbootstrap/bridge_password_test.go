//go:build linux

package settingsbootstrap

import (
	"testing"

	"novastream/internal/datastore"
	"novastream/services/profileunlock"
)

func TestBridgePasswordStartupOriginalDependencies(t *testing.T) {
	for _, kind := range []string{"ready", "before-finish", "nil-startup", "copied-startup", "nil-store", "foreign-store", "nil-pins", "foreign-pins", "closed"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressStartupPG(t, kind != "before-finish")
			startup, store, pins := f.startup, f.store, f.pins
			switch kind {
			case "nil-startup":
				startup = nil
			case "copied-startup":
				// Copy only the public pointer's scalar identity, never mutexes.
				startup = &Startup{self: f.startup, ctx: f.startup.ctx}
			case "nil-store":
				store = nil
			case "nil-pins":
				pins = nil
			case "foreign-store", "foreign-pins":
				foreign, err := datastore.New(t.Context(), f.boot.raw)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(foreign.Close)
				if kind == "foreign-store" {
					store = foreign
				} else {
					pins = profileunlock.NewPGProfilePins(foreign)
				}
			case "closed":
				_ = startup.Close()
			}
			adapter, err := startup.NewBridgePasswordLogin(store, pins)
			if kind == "ready" {
				if err != nil || adapter == nil {
					t.Fatal("original managed startup constructor", err)
				}
			} else if err == nil || adapter != nil {
				t.Fatal("foreign/unready startup constructed a password runtime", kind)
			}
			if kind == "before-finish" {
				f.startup.mu.Lock()
				finishing := f.startup.finishing
				f.startup.mu.Unlock()
				if finishing {
					t.Fatal("password constructor silently started Finish")
				}
			}
		})
	}
}
