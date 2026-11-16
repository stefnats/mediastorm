package handlers

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"novastream/services/playback"
)

type rootVODTraceKey struct{}

func rootVODAdoptOwner(t *testing.T) *vodOwnerContext {
	t.Helper()
	f := newVODTestState(t, vodAccountActive, false)
	w := httptest.NewRecorder()
	owner, ok := f.boundary.requestOwner(w, f.request("GET", "/internal?profileId=profile", ""), vodAdopt, "profile")
	if !ok || owner == nil {
		t.Fatalf("synthetic current adult/PIN fixture rejected: %d", w.Code)
	}
	t.Cleanup(owner.release)
	return owner
}

func rootVODAdoptHandler(t *testing.T, owner *vodOwnerContext) (*PrequeueHandler, string, string) {
	t.Helper()
	store := playback.NewPrequeueStore(time.Hour)
	entry, _ := store.Create("tmdb:movie:42", "Synthetic adopted movie", "profile", "movie", 2026, nil, "details")
	path := "/synthetic/adopted-context.mkv"
	store.Update(entry.ID, func(e *playback.PrequeueEntry) {
		e.Status = playback.PrequeueStatusReady
		e.StreamPath = path
		e.MigrationAdopted = true
		e.Duration = 17
	})
	return &PrequeueHandler{store: store, vodBoundary: owner.issuer}, entry.ID, path
}

func TestVODContextActualAdoptedProbeRetainsItsExisting45SecondLimit(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	h, id, path := rootVODAdoptHandler(t, owner)
	called := false
	h.fullProber = fallbackFullProber{run: func(ctx context.Context, actual string) (*VideoFullResult, error) {
		called = true
		if actual != path {
			t.Fatal("actual adopted probe received another source")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 45*time.Second {
			t.Fatal("owner context replaced the existing adopted-probe timeout")
		}
		return nil, errors.New("synthetic probe stopped after inspecting context")
	}}
	h.refreshAdoptedMigrationMetadata(id, path, "device", owner)
	if !called {
		t.Fatal("control did not reach the actual adopted metadata probe")
	}
	entry, _, _ := h.store.SnapshotForAuthorization(id)
	if entry.Duration != 17 {
		t.Fatal("failed synthetic probe changed existing metadata")
	}
}

func TestVODContextParentDeadlineAndValuesReachActualProbe(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	parentDeadline := time.Now().Add(30 * time.Second)
	parent, cancelParent := context.WithDeadline(context.WithValue(context.Background(), rootVODTraceKey{}, "synthetic-trace"), parentDeadline)
	defer cancelParent()
	combined, stop := vodWorkerContext(parent, owner)
	defer stop()
	called := false
	h := &PrequeueHandler{fullProber: fallbackFullProber{run: func(ctx context.Context, _ string) (*VideoFullResult, error) {
		called = true
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(parentDeadline) || ctx.Value(rootVODTraceKey{}) != "synthetic-trace" {
			t.Fatal("private owner dropped the caller's timeout or request value")
		}
		return nil, context.Canceled
	}}}
	h.probeAdoptedMigrationMetadata(combined, "/synthetic/parent-context.mkv")
	if !called {
		t.Fatal("actual probe context was not observed")
	}
}

func TestVODContextChoosesEarlierOriginalOwnerExpiryWithoutRenewal(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	original := owner.expires
	parent, cancelParent := context.WithDeadline(context.Background(), original.Add(time.Hour))
	defer cancelParent()
	combined, stop := vodWorkerContext(parent, owner)
	defer stop()
	deadline, ok := combined.Deadline()
	if !ok || !deadline.Equal(original) || !owner.expires.Equal(original) {
		t.Fatal("combining contexts extended the original owner boundary")
	}
}

func TestVODContextParentCancellationReachesBlockedProductionProbe(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	combined, stop := vodWorkerContext(parent, owner)
	defer stop()
	started := make(chan struct{})
	result := make(chan error, 1)
	h := &PrequeueHandler{fullProber: fallbackFullProber{run: func(ctx context.Context, _ string) (*VideoFullResult, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}}
	go func() {
		_, err := h.probeAdoptedMigrationMetadata(combined, "/synthetic/blocked-parent.mkv")
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("actual probe never started")
	}
	cancelParent()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("parent cancellation was replaced by another result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner context detached the blocked probe from caller cancellation")
	}
	if owner.ctx.Err() != nil {
		t.Fatal("one caller's cancellation revoked the shared owner context")
	}
}

func TestVODContextOwnerCancellationStopsActualAdoptedRefresh(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	h, id, path := rootVODAdoptHandler(t, owner)
	started, done := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)
	h.fullProber = fallbackFullProber{run: func(ctx context.Context, _ string) (*VideoFullResult, error) {
		close(started)
		<-ctx.Done()
		observed <- ctx.Err()
		return nil, ctx.Err()
	}}
	go func() {
		defer close(done)
		h.refreshAdoptedMigrationMetadata(id, path, "device", owner)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("actual adopted refresh did not start")
	}
	owner.release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retired owner left the actual adopted probe blocked")
	}
	if !errors.Is(<-observed, context.Canceled) {
		t.Fatal("actual adopted probe ignored owner cancellation")
	}
	entry, _, _ := h.store.SnapshotForAuthorization(id)
	if entry.Duration != 17 {
		t.Fatal("cancelled refresh published replacement metadata")
	}
}

func TestVODContextAlreadyRetiredOwnerAndStoppedChildStayRetired(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	owner.release()
	combined, stop := vodWorkerContext(context.Background(), owner)
	defer stop()
	if !errors.Is(combined.Err(), context.Canceled) {
		t.Fatal("already retired owner gained a fresh active worker context")
	}
	h, id, path := rootVODAdoptHandler(t, owner)
	var calls atomic.Int32
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
		calls.Add(1)
		return nil, errors.New("retired input must not be probed")
	}}
	h.refreshAdoptedMigrationMetadata(id, path, "device", owner)
	if calls.Load() != 0 {
		t.Fatal("retired owner started adopted metadata demand")
	}
}

func TestVODContextStoppingDerivedWorkerDoesNotCancelParentOrOwner(t *testing.T) {
	owner := rootVODAdoptOwner(t)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	combined, stop := vodWorkerContext(parent, owner)
	stop()
	if !errors.Is(combined.Err(), context.Canceled) || parent.Err() != nil || owner.ctx.Err() != nil {
		t.Fatal("child cleanup changed its parent's or owner's lifetime")
	}
}
