//go:build linux

package settingsbootstrap

import (
	"novastream/internal/datastore"
	"testing"
)

func TestShareLinkProvenanceStartup(t *testing.T) {
	for _, kind := range []string{"ready", "before-finish", "nil-startup", "copied-startup", "foreign-store", "closed"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressStartupPG(t, kind != "before-finish")
			startup, store := f.startup, f.store
			switch kind {
			case "nil-startup":
				startup = nil
			case "copied-startup":
				startup = &Startup{self: f.startup, ctx: f.startup.ctx}
			case "foreign-store":
				foreign, err := datastore.New(t.Context(), f.boot.raw)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(foreign.Close)
				store = foreign
			case "closed":
				_ = startup.Close()
			}
			creator, err := startup.NewShareLinkCreator(store)
			if kind == "ready" {
				if err != nil || creator == nil || creator.Context().Err() != nil {
					t.Fatal("original finished startup rejected", err)
				}
			} else if err == nil || creator != nil {
				t.Fatal("foreign/unready startup admitted", kind)
			}
			if kind == "before-finish" {
				f.startup.mu.Lock()
				started := f.startup.finishing
				f.startup.mu.Unlock()
				if started {
					t.Fatal("constructor started Finish")
				}
			}
		})
	}
}
