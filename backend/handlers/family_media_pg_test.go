package handlers

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

func familyActualPGInstall(t *testing.T, f *publicPinCarrierFixture) *FamilyMediaInstallation {
	t.Helper()
	local, err := localmedia.NewService(f.store, nil, "/synthetic-family-never-run")
	if err != nil {
		t.Fatal(err)
	}
	account, err := accounts.NewServiceWithStore(f.store)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := InstallFamilyMedia(FamilyMediaDependencies{Store: f.store, Pins: f.pins, Local: local, Parent: parentauth.New(account)})
	if err != nil || installed == nil || installed.Boundary == nil || installed.ParentFiles == nil {
		t.Fatal("actual PG family core dependencies did not construct", err)
	}
	return installed
}
func familyActualPGChild(t *testing.T, f *publicPinCarrierFixture) string {
	t.Helper()
	child, err := f.users.CreateForAccount("actor", "Synthetic protected child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), "UPDATE users SET is_kids_profile=true WHERE id=$1", child.ID); err != nil {
		t.Fatal(err)
	}
	return child.ID
}

func TestFamilyHTTPActualPGActiveAdultRequiresUnlockAndChildCannotBecomeGenericOwner(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	child := familyActualPGChild(t, f)
	installed := familyActualPGInstall(t, f)
	// No optional global PIN policy is a dependency of this constructor. The
	// actual current PG adult unlock remains mandatory even without middleware.
	r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release()
	w := httptest.NewRecorder()
	if q, ok := installed.Boundary.Begin(w, r, ""); ok || q != nil || w.Code != 403 {
		t.Fatal("NoPin adult became family bypass")
	}
	f.setPIN(t, "1357")
	r2, release2 := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release2()
	w = httptest.NewRecorder()
	if q, ok := installed.Boundary.Begin(w, r2, ""); ok || q != nil || w.Code != 403 {
		t.Fatal("locked adult became family bypass")
	}
	unlock := f.issue(t, r2)
	r2.Header.Set(profileunlock.Header, unlock)
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r2, "")
	if !ok {
		t.Fatal("actual current adult PG unlock denied")
	}
	defer q.Close()
	if q.owner.original.Mode() != profileunlock.FamilyMediaActiveAdult || q.owner.request != nil {
		t.Fatal("actual PG owner replaced by synthetic request")
	}
	fresh, stop := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer stop()
	if q.CheckRequest(fresh, f.profile.ID) == nil {
		t.Fatal("fresh HTTP consumer borrowed original worker PIN presentation")
	}
	if q.Check(q.Context()) != nil {
		t.Fatal("missing fresh unlock revoked original owner")
	}
	fresh.Header.Set(profileunlock.Header, unlock)
	if q.CheckRequest(fresh, f.profile.ID) != nil {
		t.Fatal("fresh same-owner consumer with current PIN denied")
	}
	foreign, stopForeign := f.request(t, "GET", "/?profileId="+child)
	defer stopForeign()
	foreign.Header.Set(profileunlock.Header, unlock)
	w = httptest.NewRecorder()
	if childOwner, yes := installed.Boundary.Begin(w, foreign, ""); yes || childOwner != nil || w.Code != 403 {
		t.Fatal("adult unlock minted generic child authority")
	}
	if q.Handoff(r2) != nil {
		t.Fatal("actual original PG worker handoff denied")
	}
	if q.Handoff(r2) == nil {
		t.Fatal("original PG worker handoff consumed twice")
	}
	release2() // Original request retirement is expected AFTER successful adoption.
	if q.Check(q.Context()) != nil {
		t.Fatal("adopted worker lost original PG owner after request returned")
	}
	if _, err := f.users.SetPin(f.profile.ID, "2468"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
		t.Fatal(err)
	}
	if q.Check(context.Background()) == nil {
		t.Fatal("original HTTP worker adopted later PIN generation after ABA")
	}
}

func TestFamilyHTTPActualPGPendingOpenKeepsAdmissionUntilRealFDCloses(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release()
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r, "")
	if !ok {
		t.Fatal("actual inactive original owner denied")
	}
	t.Cleanup(q.Close)
	var held []*profileunlock.FamilyMediaOwner
	t.Cleanup(func() {
		for _, owner := range held {
			owner.Close()
		}
	})
	for range 31 {
		owner, err := f.pins.CaptureFamilyMediaOwner(r, f.profile.ID)
		if err != nil {
			t.Fatal("fill actual bounded account admission", err)
		}
		held = append(held, owner)
	}
	path := filepath.Join(t.TempDir(), "synthetic-held-input.bin")
	if err := os.WriteFile(path, []byte("Synthetic inactive adult input."), 0600); err != nil {
		t.Fatal(err)
	}
	entered, openGate, opening := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var gateOnce sync.Once
	t.Cleanup(func() { gateOnce.Do(func() { close(openGate) }) })
	var actual *os.File
	go func() {
		defer close(opening)
		body, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) {
			close(entered)
			<-openGate
			var e error
			actual, e = os.Open(path)
			return actual, e
		})
		if body != nil || err == nil {
			t.Error("late opener transferred canceled file")
		}
	}()
	familyWait(t, entered, "actual FD opener did not enter")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, q.Context().Done(), "HTTP owner did not cancel pending opener")
	if extra, err := f.pins.CaptureFamilyMediaOwner(r, f.profile.ID); extra != nil || !errors.Is(err, profileunlock.ErrFamilyMediaBusy) {
		if extra != nil {
			extra.Close()
		}
		t.Fatal("pending original opener refunded actual PG admission", err)
	}
	familyPending(t, closed, "pending opener had no cleanup barrier")
	gateOnce.Do(func() { close(openGate) })
	familyWait(t, opening, "actual opener did not finish")
	familyWait(t, closed, "actual FD cleanup did not join")
	if actual == nil {
		t.Fatal("actual synthetic FD never opened")
	}
	if _, err := actual.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("actual FD still live after admission refunded")
	}
	next, err := f.pins.CaptureFamilyMediaOwner(r, f.profile.ID)
	if err != nil {
		t.Fatal("actual FD close did not release admission", err)
	}
	next.Close()
}

func TestFamilyHTTPActualPGOriginalRequestCancellationCannotLeavePendingAuthority(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r, "")
	if !ok {
		release()
		t.Fatal("actual original owner denied")
	}
	defer q.Close()
	release()
	familyWait(t, q.Context().Done(), "original request cancellation did not reach HTTP owner")
	called := false
	if body, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) { called = true; return nil, nil }); body != nil || err == nil || called {
		t.Fatal("canceled pending request opened a source")
	}
	if q.Handoff(r) == nil {
		t.Fatal("canceled original request adopted replacement worker")
	}
}

func TestFamilyHTTPInstallerRejectsMissingAndForeignPGDependenciesWithoutPublication(t *testing.T) {
	if value, err := InstallFamilyMedia(FamilyMediaDependencies{}); value != nil || err == nil {
		t.Fatal("missing factory dependencies became legacy allow")
	}
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	foreign := profileunlock.NewPGProfilePins(&datastore.DataStore{})
	deps := FamilyMediaDependencies{Store: f.store, Pins: foreign, Local: installed.ParentFiles.local, Parent: installed.ParentFiles.parent}
	if value, err := InstallFamilyMedia(deps); value != nil || err == nil {
		t.Fatal("foreign PG facade accepted by installer")
	}
	if installed.Boundary.boundary.original == nil || installed.Boundary.boundary.pin != nil || installed.ParentFiles.hlsManager() != nil {
		t.Fatal("factory omitted original port or activated unrelated PIN/HLS runtime")
	}
	// The separate actual-file parent path is constructed, never replaced by
	// the generic adult owner. No HTTP route or media source is installed here.
	if installed.ParentFiles.pins != f.pins || installed.ParentFiles.store != f.store {
		t.Fatal("child parent-file factory changed current original dependencies")
	}
}
