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

	"novastream/services/profileunlock"
)

func TestFamilyConsumerActualPGFreshPINAndOriginalSessionCannotBeReplaced(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	familyActualPGChild(t, f)
	f.setPIN(t, "1357")
	installed := familyActualPGInstall(t, f)
	original, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release()
	unlock := f.issue(t, original)
	original.Header.Set(profileunlock.Header, unlock)
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("actual active adult original request denied")
	}
	t.Cleanup(q.Close)
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	expires := r.ExpiresAt()
	release()
	if r.Check(r.Context()) != nil {
		t.Fatal("actual adopted original lost authority at HTTP return")
	}
	fresh, stop := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer stop()
	if c, _, e := r.beginConsumer(httptest.NewRecorder(), fresh, f.profile.ID); c != nil || e == nil {
		t.Fatal("fresh consumer borrowed worker PIN")
	}
	if r.Check(r.Context()) != nil {
		t.Fatal("omitted consumer PIN revoked valid original")
	}
	fresh.Header.Set(profileunlock.Header, unlock)
	c, checked, err := r.beginConsumer(httptest.NewRecorder(), fresh, f.profile.ID)
	if err != nil || checked == nil {
		t.Fatal("actual same-original fresh consumer denied", err)
	}
	if !c.ExpiresAt().Equal(expires) {
		t.Fatal("fresh consumer changed original expiry")
	}
	c.Close()
	newSession, err := f.sessions.Create("actor", false, "synthetic-replacement", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	foreign, stopForeign, err := f.pins.Authenticate(httptest.NewRequest("GET", "/?profileId="+f.profile.ID, nil), newSession.Token, "synthetic-client")
	if err != nil {
		t.Fatal(err)
	}
	defer stopForeign()
	foreign.Header.Set(profileunlock.Header, f.issue(t, foreign))
	if borrowed, _, e := r.beginConsumer(httptest.NewRecorder(), foreign, f.profile.ID); borrowed != nil || e == nil {
		t.Fatal("replacement authenticated session adopted original worker")
	}
	if r.Check(r.Context()) != nil {
		t.Fatal("foreign session request retired valid original")
	}
	if _, err = f.users.SetPin(f.profile.ID, "2468"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.users.SetPin(f.profile.ID, "1357"); err != nil {
		t.Fatal(err)
	}
	if r.Check(context.Background()) == nil {
		t.Fatal("runtime adopted original profile PIN after ABA")
	}
	familyWait(t, r.Context().Done(), "actual PIN generation change did not cancel original runtime")
}

func TestFamilyConsumerActualPGOriginalRefundWaitsForLateConsumerFD(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	original, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release()
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("actual inactive original denied")
	}
	t.Cleanup(q.Close)
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	fresh, stop := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer stop()
	var held []*profileunlock.FamilyMediaOwner
	t.Cleanup(func() {
		for _, o := range held {
			o.Close()
		}
	})
	for range 31 {
		o, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
		if e != nil {
			t.Fatal("fill actual PG owner budget", e)
		}
		held = append(held, o)
	}
	c, _, err := r.beginConsumer(httptest.NewRecorder(), fresh, f.profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "synthetic-consumer-range.bin")
	if err = os.WriteFile(path, []byte("Synthetic actual consumer FD."), 0600); err != nil {
		t.Fatal(err)
	}
	entered, gate, opened := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var gateOnce sync.Once
	t.Cleanup(func() { gateOnce.Do(func() { close(gate) }) })
	var actual *os.File
	go func() {
		defer close(opened)
		body, e := c.OpenBody(func(context.Context) (io.ReadCloser, error) {
			close(entered)
			<-gate
			var openErr error
			actual, openErr = os.Open(path)
			return actual, openErr
		})
		if body != nil || e == nil {
			t.Error("late actual consumer source published")
		}
	}()
	familyWait(t, entered, "actual consumer FD opener not blocked")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, r.Context().Done(), "original owner failed to cancel pending consumer")
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		if extra != nil {
			extra.Close()
		}
		t.Fatal("late consumer opener refunded actual original PG slot", e)
	}
	familyPending(t, closed, "actual consumer opener was not joined")
	gateOnce.Do(func() { close(gate) })
	familyWait(t, opened, "actual consumer FD open did not retire")
	familyWait(t, closed, "actual consumer FD close did not refund original")
	if actual == nil {
		t.Fatal("actual synthetic consumer FD never opened")
	}
	if _, e := actual.Stat(); !errors.Is(e, os.ErrClosed) {
		t.Fatal("actual consumer FD was not closed before refund")
	}
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); e != nil {
		t.Fatal("actual FD completion did not refund PG slot", e)
	} else {
		extra.Close()
	}
}

func TestFamilyConsumerActualPGProcessCompletionBarrierRetainsOriginalAdmission(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	original, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer release()
	q, ok := installed.Boundary.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("actual inactive original denied")
	}
	t.Cleanup(q.Close)
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	fresh, stop := f.request(t, "GET", "/?profileId="+f.profile.ID)
	defer stop()
	var held []*profileunlock.FamilyMediaOwner
	t.Cleanup(func() {
		for _, o := range held {
			o.Close()
		}
	})
	for range 31 {
		o, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, o)
	}
	actualDone, stopped := make(chan struct{}), make(chan struct{})
	var doneOnce sync.Once
	t.Cleanup(func() { doneOnce.Do(func() { close(actualDone) }) })
	if _, err = r.openCleanup(actualDone, func(context.Context) (func(), error) { return func() { close(stopped) }, nil }); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	familyWait(t, stopped, "actual cleanup stop not called")
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		if extra != nil {
			extra.Close()
		}
		t.Fatal("stop signal refunded original PG slot before real completion", e)
	}
	familyPending(t, closed, "runtime ignored actual process/pump/output done barrier")
	doneOnce.Do(func() { close(actualDone) })
	familyWait(t, closed, "actual resource barrier did not release original")
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); e != nil {
		t.Fatal("actual completion did not refund original admission", e)
	} else {
		extra.Close()
	}
}
