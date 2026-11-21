//go:build linux

package settingsbootstrap

import (
	"context"
	"novastream/internal/datastore"
	"testing"
	"time"
)

func TestSharePlaybackStartupComposition(t *testing.T) {
	for _, kind := range []string{"original-finished", "before-finish", "nil-startup", "copied-startup", "foreign-store", "single-owned-runtime", "close-cancels-joins", "late-construction-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressStartupPG(t, kind != "before-finish")
			startup, store := f.startup, f.store
			switch kind {
			case "nil-startup":
				startup = nil
			case "copied-startup":
				startup = &Startup{self: f.startup, ctx: f.startup.ctx}
			case "foreign-store":
				var err error
				store, err = datastore.New(t.Context(), f.boot.raw)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				original, err := f.startup.NewShareLinkRedeemer(f.store)
				if err != nil {
					t.Fatal(err)
				}
				if original == nil {
					t.Fatal("original missing")
				}
			case "late-construction-denied":
				if err := startup.Close(); err != nil {
					t.Fatal(err)
				}
			}
			runtime, err := startup.NewShareLinkRedeemer(store)
			valid := kind == "original-finished" || kind == "single-owned-runtime" || kind == "close-cancels-joins"
			if !valid {
				if err == nil || runtime != nil {
					t.Fatal("unready/foreign startup selected runtime", kind)
				}
				if kind == "before-finish" {
					f.startup.mu.Lock()
					started := f.startup.finishing
					f.startup.mu.Unlock()
					if started {
						t.Fatal("constructor started Finish")
					}
				}
				return
			}
			if err != nil || runtime == nil || !runtime.MatchesStore(store) {
				t.Fatal("original finished runtime unavailable", err)
			}
			next, e := startup.NewShareLinkRedeemer(store)
			if e != nil || next != runtime {
				t.Fatal("factory did not preserve single original runtime", e)
			}
			if kind == "close-cancels-joins" {
				done := make(chan error, 1)
				go func() { done <- startup.Close() }()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("startup close failed to join runtime")
				}
				if runtime.Context().Err() == nil || runtime.MatchesStore(store) {
					t.Fatal("closed startup left runtime ready")
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if _, e := runtime.Redeem(nil, ""); e == nil {
					t.Fatal("closed runtime admitted public redemption")
				}
				_ = ctx
			}
		})
	}
}
