//go:build linux

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"novastream/internal/parentauth"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
)

func TestFamilyInstallationOriginalUsersAndGuard(t *testing.T) {
	t.Run("same-original-users-parent-guard", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		if f.installed.ParentFiles.parent != f.usersHandler.parentAuthorization || !f.usersHandler.parentAuthorization.FamilyMediaReady(f.store) || f.installed.deps.Pins != f.pins {
			t.Fatal("selected preparation replaced actual original guard/Pins")
		}
	})
	t.Run("one-shared-failed-password-budget", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		r, release := f.localRequest(t, "GET")
		defer release()
		r.Header.Set(parentauth.PasswordHeader, "synthetic-wrong-parent-password")
		for n := 0; n < 10; n++ {
			w := httptest.NewRecorder()
			var allowed bool
			if n%2 == 0 {
				allowed = f.usersHandler.parentAuthorization.Require(w, r)
			} else {
				allowed = f.installed.ParentFiles.parent.RequireFresh(w, r)
			}
			if allowed || w.Code != http.StatusForbidden {
				t.Fatal("actual wrong-password budget changed before tenth failure", n, w.Code)
			}
		}
		for _, guard := range []*parentauth.Guard{f.usersHandler.parentAuthorization, f.installed.ParentFiles.parent} {
			w := httptest.NewRecorder()
			if guard.RequireFresh(w, r) || w.Code != 429 || w.Header().Get("Retry-After") == "" {
				t.Fatal("selected parent consumer has a second credential budget", w.Code)
			}
		}
	})
	t.Run("same-installation-for-both-concrete-sources", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		i := f.installed
		if i.localRaw.installation != i || i.jellyfinRaw.installation != i || i.jellyfinRaw.store != f.store || i.ParentFiles.local != f.local || i.deps.Parent != f.usersHandler.parentAuthorization {
			t.Fatal("known source constructors installed a different original boundary")
		}
	})
	t.Run("missing-original-users-guard-denied", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		users := NewUsersHandler(f.users)
		users.SetPGProfilePins(f.pins)
		if i, e := users.PrepareFamilyMediaConsumers(f.original, f.store, f.pins, f.local, f.manager, f.remote); i != nil || e == nil {
			if i != nil {
				i.Close()
			}
			t.Fatal("missing original guard prepared selected consumers")
		}
		if f.wire.requests.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("failed preparation opened actual sources")
		}
	})
	t.Run("foreign-parent-account-store-denied", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		other := newPublicPinCarrierFixture(t)
		account, e := accounts.NewServiceWithStore(other.store)
		if e != nil {
			t.Fatal(e)
		}
		users := NewUsersHandler(f.users, account)
		users.SetPGProfilePins(f.pins)
		if i, e := users.PrepareFamilyMediaConsumers(f.original, f.store, f.pins, f.local, f.manager, f.remote); i != nil || e == nil {
			if i != nil {
				i.Close()
			}
			t.Fatal("matching logical foreign account became original guard")
		}
		if !f.installed.valid() || f.installed.ParentFiles.parent != f.usersHandler.parentAuthorization {
			t.Fatal("denied foreign preparation changed original")
		}
	})
	t.Run("missing-or-foreign-profile-pins-denied", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		other := newPublicPinCarrierFixture(t)
		original := f.usersHandler.pgPins
		defer f.usersHandler.SetPGProfilePins(original)
		for _, pins := range []*profileunlock.PGProfilePins{nil, other.pins} {
			// Select that same facade on the actual UsersHandler too; an early
			// pointer mismatch must not mask the actual foreign-store check.
			f.usersHandler.SetPGProfilePins(pins)
			if i, e := f.usersHandler.PrepareFamilyMediaConsumers(f.original, f.store, pins, f.local, f.manager, f.remote); i != nil || e == nil {
				if i != nil {
					i.Close()
				}
				t.Fatal("foreign/nil Pins prepared original installation")
			}
		}
	})
	t.Run("prepared-inactive-local-wires-without-opening", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.installed.localRaw.opener}
		f.installed.localRaw.opener = observer
		r, release := f.localRequest(t, "GET")
		defer release()
		w := newFamilyKnownRecorder()
		aborted := familyKnownServe(f.installed.ServeLocalRaw, w, r)
		familyKnownDenied(t, w, aborted)
		if observer.readers.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("inactive genuine source opened actual FD/reader")
		}
		if e := f.local.Activate(f.original); e != nil {
			t.Fatal(e)
		}
		fresh, stop := f.localRequest(t, "GET")
		defer stop()
		healthy := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeLocalRaw, healthy, fresh) || healthy.Code != 200 || healthy.Body.Len() != len(f.data) {
			t.Fatal("fresh genuine activated consumer denied", healthy.Code)
		}
	})
	t.Run("closed-concrete-local-service-rejects-installation", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		if e := f.local.Close(); e != nil {
			t.Fatal(e)
		}
		if i, e := f.usersHandler.PrepareFamilyMediaConsumers(f.original, f.store, f.pins, f.local, f.manager, f.remote); i != nil || e == nil {
			if i != nil {
				i.Close()
			}
			t.Fatal("closed actual service still installable")
		}
		w := newFamilyKnownRecorder()
		r, release := f.localRequest(t, "GET")
		defer release()
		familyKnownDenied(t, w, familyKnownServe(f.installed.ServeLocalRaw, w, r))
	})
	t.Run("factory-starts-no-source-or-background-work", func(t *testing.T) {
		f := newFamilyKnownFixture(t, false)
		if f.wire.requests.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 || f.installed.localRaw.life.Err() != nil {
			t.Fatal("preparation activated source IO")
		}
		// A cancelled original cannot construct a replacement runtime or revive this
		// prepared service. No authenticated request is synthesized from its context.
		cancelled, cancel := context.WithCancel(f.original)
		cancel()
		if i, e := f.usersHandler.PrepareFamilyMediaConsumers(cancelled, f.store, f.pins, f.local, f.manager, f.remote); i != nil || e == nil {
			if i != nil {
				i.Close()
			}
			t.Fatal("cancelled original created replacement installation")
		}
	})
}
