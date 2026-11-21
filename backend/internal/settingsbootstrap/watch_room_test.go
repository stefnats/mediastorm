//go:build linux

package settingsbootstrap

import (
	"net/http/httptest"
	"novastream/internal/datastore"
	"testing"
	"time"
)

func TestWatchRoomStartupFactory(t *testing.T) {
	for _, kind := range []string{"original-finished", "before-finish", "nil-startup", "copied-startup", "foreign-store", "single-owned-runtime", "close-joins-original-work"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressStartupPG(t, kind != "before-finish")
			startup, store := f.startup, f.store
			if kind == "nil-startup" {
				startup = nil
			}
			if kind == "copied-startup" {
				startup = &Startup{self: f.startup, ctx: f.startup.ctx, source: f.startup.source, manager: f.startup.manager, security: f.startup.security}
			}
			if kind == "foreign-store" {
				var e error
				store, e = datastore.New(t.Context(), f.boot.raw)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(store.Close)
			}
			authority, e := startup.NewWatchRoomAuthority(store)
			denied := kind == "before-finish" || kind == "nil-startup" || kind == "copied-startup" || kind == "foreign-store"
			if denied {
				if e == nil || authority != nil {
					t.Fatal("unready/foreign/copied startup selected room producer", kind)
				}
				return
			}
			if e != nil || authority == nil || !authority.MatchesStore(f.store) {
				t.Fatal("original finished factory", e)
			}
			if kind == "single-owned-runtime" {
				for _, forms := range [][]string{{"192.0.2.8:10", "[::ffff:192.0.2.8]:20", "[::ffff:c000:208]:30", "192.0.2.8:40", "[::ffff:192.0.2.8]:50"}, {"[2001:db8::8]:10", "[2001:0db8:0000::8]:20", "[2001:db8:0:0:0:0:0:8]:30", "[2001:db8::8]:40", "[2001:db8::8]:50"}, {"malformed-one", "malformed-two", "::1%lo", "[::1%lo]:40", "malformed-five"}} {
					entries := make([]*datastore.WatchRoomHTTPEntry, 0, 4)
					for i, form := range forms {
						request := httptest.NewRequest("POST", "http://synthetic.test/api/users/profile/watch-rooms", nil)
						request.RemoteAddr = form
						request.Header.Set("X-Forwarded-For", form)
						entry, err := authority.OpenHTTPEntry(request)
						if i < 4 {
							if err != nil || entry == nil {
								t.Fatal("actual canonical peer admission", form, err)
							}
							entries = append(entries, entry)
							t.Cleanup(entry.Close)
						} else if err == nil || entry != nil {
							if entry != nil {
								entry.Close()
							}
							t.Fatal("actual peer spelling/port bypassed bounded admission", form)
						}
					}
					for _, entry := range entries {
						entry.Close()
					}
				}
				again, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil || again != authority {
					t.Fatal("factory created second runtime", e)
				}
				return
			}
			if kind == "close-joins-original-work" {
				request := httptest.NewRequest("POST", "http://synthetic.test/api/users/profile/watch-rooms", nil)
				entry, e := authority.OpenHTTPEntry(request)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(entry.Close)
				done := make(chan struct{})
				t.Cleanup(func() { entry.Close(); <-done })
				go func() {
					defer close(done)
					if e := f.startup.Close(); e != nil {
						t.Error(e)
					}
				}()
				select {
				case <-entry.Context().Done():
				case <-time.After(5 * time.Second):
					t.Fatal("owned original entry not cancelled")
				}
				select {
				case <-done:
					t.Fatal("Startup.Close returned before original ingress ended")
				default:
				}
				entry.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Startup.Close did not join original entry")
				}
			}
		})
	}
}
