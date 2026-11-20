package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/services/profileunlock"
)

func familyConsumerRuntime(t *testing.T) (*vodTestState, *FamilyMediaRequest, *familyMediaRuntime, *familyHTTPTestOwner) {
	t.Helper()
	s, b, o := familyHTTPControlled(t)
	original := s.request("GET", "/?profileId=profile", "")
	q, ok := b.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("controlled original request denied")
	}
	t.Cleanup(q.Close)
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal("controlled actual runtime adoption failed", err)
	}
	return s, q, r, o
}

func TestFamilyConsumerRuntimeAdoptsOnceAndCopiesRemainOpaqueAndPowerless(t *testing.T) {
	s, q, r, owner := familyConsumerRuntime(t)
	if duplicate, err := q.adoptRuntime(s.request("GET", "/?profileId=profile", "")); duplicate != nil || err == nil {
		t.Fatal("runtime adopted twice")
	}
	if _, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) {
		t.Error("root request opened after ownership transfer")
		return nil, nil
	}); err == nil {
		t.Fatal("root opener bypassed transferred runtime")
	}
	if _, err := q.ResponseWriter(newFamilyControlledWriter()); err == nil {
		t.Fatal("root writer bypassed transferred runtime")
	}
	c, err := r.beginSubread(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	actualDone := make(chan struct{})
	close(actualDone)
	resource, err := r.openCleanup(actualDone, func(context.Context) (func(), error) { return func() {}, nil })
	if err != nil {
		t.Fatal(err)
	}
	rCopy, cCopy, resourceCopy := *r, *c, *resource
	for _, tc := range []struct {
		label  string
		values []any
	}{
		{"familyMediaRuntime(opaque)", []any{r, *r, rCopy, &rCopy}},
		{"familyMediaConsumer(opaque)", []any{c, *c, cCopy, &cCopy}},
		{"familyMediaCleanup(opaque)", []any{resource, *resource, resourceCopy, &resourceCopy}},
	} {
		for _, v := range tc.values {
			if fmt.Sprintf("%+v", v) != tc.label || fmt.Sprintf("%#v", v) != tc.label {
				t.Fatal("private runtime formatting exposed authority")
			}
			if raw, e := json.Marshal(v); e != nil || string(raw) != "null" {
				t.Fatal("private runtime serialized authority")
			}
		}
	}
	if rCopy.Check(context.Background()) == nil || cCopy.Check(context.Background()) == nil {
		t.Fatal("copied runtime borrowed original authority")
	}
	rCopy.Close()
	cCopy.Close()
	resourceCopy.Close()
	if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
		t.Fatal("copied cleanup retired original")
	}
	var imported familyMediaRuntime
	var importedConsumer familyMediaConsumer
	var importedCleanup familyMediaCleanup
	if json.Unmarshal([]byte(`{}`), &imported) == nil || json.Unmarshal([]byte(`{}`), &importedConsumer) == nil || json.Unmarshal([]byte(`{}`), &importedCleanup) == nil {
		t.Fatal("imported runtime authority")
	}
	q.Close()
	if owner.closed.Load() != 1 {
		t.Fatal("original request Close did not join adopted runtime exactly once")
	}
}

func TestFamilyConsumerSequentialFreshBodiesAndWritersDoNotRecaptureOrRenew(t *testing.T) {
	s, _, r, owner := familyConsumerRuntime(t)
	expires := r.ExpiresAt()
	for i := 0; i < 3; i++ {
		fresh := s.request("POST", "/?profileId=profile", `{"profileId":"profile","position":1}`)
		c, checked, err := r.beginConsumer(httptest.NewRecorder(), fresh, "profile")
		if err != nil {
			t.Fatal(err)
		}
		data, e := io.ReadAll(checked.Body)
		if e != nil || string(data) != `{"profileId":"profile","position":1}` {
			t.Fatal("fresh consumer consumed operation decoder")
		}
		body, e := c.OpenBody(func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("synthetic range")), nil
		})
		if e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(body)
		if e != nil || string(got) != "synthetic range" {
			t.Fatal("actual fresh range not read", e)
		}
		if _, e = c.OpenBody(func(context.Context) (io.ReadCloser, error) { t.Error("duplicate consumer open ran"); return nil, nil }); e == nil {
			t.Fatal("consumer opened twice")
		}
		target := newFamilyControlledWriter()
		close(target.writeGate)
		close(target.callbackGate)
		writer, e := c.ResponseWriter(target)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = c.ResponseWriter(target); e == nil {
			t.Fatal("consumer wrote through two response guards")
		}
		if _, ok := writer.(interface{ Unwrap() http.ResponseWriter }); ok {
			t.Fatal("consumer writer exposed unchecked target")
		}
		if _, ok := writer.(io.ReaderFrom); ok {
			t.Fatal("consumer writer exposed unchecked ReaderFrom")
		}
		target.mu.Lock()
		first := target.deadlines[0]
		target.mu.Unlock()
		if !first.Equal(expires) || !c.ExpiresAt().Equal(expires) {
			t.Fatal("fresh consumer renewed original deadline")
		}
		c.Close()
		if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
			t.Fatal("fresh range cleanup retired original runtime")
		}
	}
	if !owner.handed.Load() {
		t.Fatal("actual original handoff missing")
	}
}

func TestFamilyConsumerFreshSelectorDenialAndDisconnectCannotRetireOriginal(t *testing.T) {
	s, _, r, owner := familyConsumerRuntime(t)
	for _, fresh := range []*http.Request{
		s.request("GET", "/?profileId=other", ""), s.request("GET", "/?profileId=profile&profileId=profile", ""),
		mux.SetURLVars(s.request("GET", "/?profileId=profile", ""), map[string]string{"userID": "other"}),
		s.request("POST", "/", `{"profileId":"profile","PROFILEname":"other"}`),
	} {
		if c, _, err := r.beginConsumer(httptest.NewRecorder(), fresh, "profile"); c != nil || err == nil {
			t.Fatal("conflicting consumer acquired original authority")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := r.beginSubread(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source := newFamilyControlledRead()
	body, err := c.OpenBody(func(context.Context) (io.ReadCloser, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(source.closeGate) }) })
	readDone := make(chan struct{})
	caller := bytes.Repeat([]byte{0x42}, 64)
	go func() {
		defer close(readDone)
		if n, e := body.Read(caller); n != 0 || e == nil {
			t.Error("consumer cancellation published late bytes")
		}
	}()
	familyWait(t, source.entered, "consumer read did not block")
	cancel()
	familyWait(t, source.closeEntered, "caller cancellation did not interrupt consumer source")
	if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
		t.Fatal("consumer disconnect canceled original worker")
	}
	release.Do(func() { close(source.closeGate) })
	familyWait(t, readDone, "consumer source failed to retire")
	c.Close()
	if !bytes.Equal(caller, bytes.Repeat([]byte{0x42}, 64)) || !bytes.Equal(source.buffer, make([]byte, len(source.buffer))) {
		t.Fatal("late private staging escaped or was retained")
	}
	if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
		t.Fatal("consumer Close refunded original owner")
	}
}

func TestFamilyConsumerRuntimeCloseJoinsLateOpenAndRealSourceClose(t *testing.T) {
	_, q, r, owner := familyConsumerRuntime(t)
	c, err := r.beginSubread(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entered, gate, opened := make(chan struct{}), make(chan struct{}), make(chan struct{})
	source := newFamilyControlledRead()
	var releaseOpen, releaseClose sync.Once
	t.Cleanup(func() { releaseOpen.Do(func() { close(gate) }); releaseClose.Do(func() { close(source.closeGate) }) })
	go func() {
		defer close(opened)
		body, e := c.OpenBody(func(context.Context) (io.ReadCloser, error) { close(entered); <-gate; return source, nil })
		if body != nil || e == nil {
			t.Error("late source escaped retired runtime")
		}
	}()
	familyWait(t, entered, "actual source opener did not block")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, r.Context().Done(), "root request did not cancel adopted runtime")
	familyPending(t, closed, "original PG owner refunded before late opener")
	if owner.closed.Load() != 0 {
		t.Fatal("owner refunded before actual open returned")
	}
	releaseOpen.Do(func() { close(gate) })
	familyWait(t, source.closeEntered, "late opened source was not closed")
	familyPending(t, closed, "actual source Close was not joined")
	releaseClose.Do(func() { close(source.closeGate) })
	familyWait(t, opened, "late source opener did not return")
	familyWait(t, closed, "actual source cleanup did not release root owner")
	if owner.closed.Load() != 1 {
		t.Fatal("original owner refund was not exact")
	}
}

type familyConsumerBlockedOwner struct {
	*familyHTTPTestOwner
	block   atomic.Bool
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (o *familyConsumerBlockedOwner) Check(ctx context.Context) error {
	if o.block.Load() {
		o.once.Do(func() { close(o.entered) })
		<-o.gate
	}
	return o.familyHTTPTestOwner.Check(ctx)
}

func TestFamilyConsumerAdmissionPrecedesBlockedCurrentChecksAndIsSharedWithSubreads(t *testing.T) {
	s, b, base := familyHTTPControlled(t)
	o := &familyConsumerBlockedOwner{familyHTTPTestOwner: base, entered: make(chan struct{}), gate: make(chan struct{})}
	b.capture = func(*http.Request, string) (vodOriginalOwner, error) { return o, nil }
	original := s.request("GET", "/?profileId=profile", "")
	q, ok := b.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("original capture denied")
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(o.gate) }); q.Close() })
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	o.block.Store(true)
	var group sync.WaitGroup
	var heldMu sync.Mutex
	var held []*familyMediaConsumer
	for range familyMediaRuntimeConsumers {
		group.Add(1)
		go func() {
			defer group.Done()
			c, e := r.beginSubread(context.Background())
			if e != nil {
				t.Error(e)
				return
			}
			heldMu.Lock()
			held = append(held, c)
			heldMu.Unlock()
		}()
	}
	familyWait(t, o.entered, "current owner reader did not block")
	// Observe actual reservations, not a copied Boolean gate.
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		count := len(r.consumers)
		r.mu.Unlock()
		if count == familyMediaRuntimeConsumers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocked current reads did not hold every reservation")
		}
		time.Sleep(time.Millisecond)
	}
	busy := make(chan struct{})
	go func() {
		defer close(busy)
		c, _, e := r.beginConsumer(httptest.NewRecorder(), s.request("GET", "/?profileId=profile", ""), "profile")
		if c != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
			t.Error("fresh consumer bypassed shared subread capacity", e)
		}
	}()
	familyWait(t, busy, "capacity rejection entered blocked PG check")
	release.Do(func() { close(o.gate) })
	group.Wait()
	for _, c := range held {
		c.Close()
	}
	if c, e := r.beginSubread(context.Background()); e != nil {
		t.Fatal("retired consumers did not refund admission", e)
	} else {
		c.Close()
	}
}

func TestFamilyConsumerGlobalAdmissionCoversIndependentOriginalRuntimes(t *testing.T) {
	var runtimes []*familyMediaRuntime
	var held []*familyMediaConsumer
	t.Cleanup(func() {
		for _, c := range held {
			c.Close()
		}
		for _, r := range runtimes {
			r.Close()
		}
	})
	for range familyMediaGlobalConsumers / familyMediaRuntimeConsumers {
		_, _, r, _ := familyConsumerRuntime(t)
		runtimes = append(runtimes, r)
		for range familyMediaRuntimeConsumers {
			c, e := r.beginSubread(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			held = append(held, c)
		}
	}
	_, _, extra, _ := familyConsumerRuntime(t)
	runtimes = append(runtimes, extra)
	if c, e := extra.beginSubread(context.Background()); c != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		t.Fatal("independent runtimes escaped global admission", e)
	}
	held[0].Close()
	if c, e := extra.beginSubread(context.Background()); e != nil {
		t.Fatal("global capacity not reusable after actual consumer cleanup", e)
	} else {
		c.Close()
	}
}

func TestFamilyConsumerCleanupJoinsLateProcessOpenerAndActualDoneBeforeRefund(t *testing.T) {
	_, q, r, owner := familyConsumerRuntime(t)
	entered, gate, actualDone, stopped, opened := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var openOnce, doneOnce sync.Once
	t.Cleanup(func() { openOnce.Do(func() { close(gate) }); doneOnce.Do(func() { close(actualDone) }) })
	var stops atomic.Int32
	go func() {
		defer close(opened)
		job, e := r.openCleanup(actualDone, func(context.Context) (func(), error) {
			close(entered)
			<-gate
			return func() { stops.Add(1); close(stopped) }, nil
		})
		if job != nil || e == nil {
			t.Error("late process adopted retired runtime")
		}
	}()
	familyWait(t, entered, "process opener did not block")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, r.Context().Done(), "original runtime not canceled")
	familyPending(t, closed, "runtime refunded before late process open")
	openOnce.Do(func() { close(gate) })
	familyWait(t, stopped, "late actual process was not stopped")
	familyPending(t, closed, "runtime treated stop as actual process Wait")
	if owner.closed.Load() != 0 {
		t.Fatal("PG owner refunded before actual process/pump/output barrier")
	}
	doneOnce.Do(func() { close(actualDone) })
	familyWait(t, opened, "late process opener did not join actual completion")
	familyWait(t, closed, "actual resource completion did not retire runtime")
	if stops.Load() != 1 || owner.closed.Load() != 1 {
		t.Fatal("actual process cleanup/refund not exactly once")
	}
}

func TestFamilyConsumerCleanupAdmissionAndFailedOpenerRemainChargedUntilActualDone(t *testing.T) {
	_, _, r, _ := familyConsumerRuntime(t)
	var held []*familyMediaCleanup
	for range familyMediaRuntimeCleanups {
		done := make(chan struct{})
		close(done)
		job, e := r.openCleanup(done, func(context.Context) (func(), error) { return func() {}, nil })
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, job)
	}
	called := false
	if job, e := r.openCleanup(make(chan struct{}), func(context.Context) (func(), error) { called = true; return nil, nil }); job != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) || called {
		t.Fatal("cleanup capacity admitted an extra opener")
	}
	held[0].Close()
	entered, done, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var doneOnce sync.Once
	t.Cleanup(func() { doneOnce.Do(func() { close(done) }) })
	go func() {
		defer close(returned)
		job, e := r.openCleanup(done, func(context.Context) (func(), error) {
			close(entered)
			return nil, errors.New("synthetic process start failure")
		})
		if job != nil || e == nil {
			t.Error("failed resource opener published")
		}
	}()
	familyWait(t, entered, "failed resource opener did not run")
	familyPending(t, returned, "invalid stop allowed early refund without actual done")
	if job, e := r.openCleanup(make(chan struct{}), func(context.Context) (func(), error) { t.Error("failed opener reservation escaped"); return nil, nil }); job != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		t.Fatal("failed resource refunded before actual completion")
	}
	doneOnce.Do(func() { close(done) })
	familyWait(t, returned, "failed resource did not retire after actual completion")
	for _, job := range held {
		job.Close()
	}
}

func TestFamilyConsumerOriginalChangeCancelsEveryConsumerWithoutJoiningItsOwnCheck(t *testing.T) {
	_, q, r, owner := familyConsumerRuntime(t)
	var held []*familyMediaConsumer
	for range 2 {
		c, e := r.beginSubread(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, c)
	}
	owner.changed.Store(true)
	if held[0].Check(held[0].Context()) == nil {
		t.Fatal("current original change ignored")
	}
	for _, c := range held {
		familyWait(t, c.Context().Done(), "original revocation failed to cancel every consumer")
	}
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, closed, "check stack deadlocked while runtime cleanup joined")
	if owner.closed.Load() != 1 {
		t.Fatal("original current change did not retire exact owner")
	}
}

func TestFamilyConsumerOriginalCloseSignalsAllResourceStopsBeforeJoiningOne(t *testing.T) {
	_, _, r, owner := familyConsumerRuntime(t)
	firstDone, secondDone, firstStop, secondStop := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var one, two sync.Once
	t.Cleanup(func() { one.Do(func() { close(firstDone) }); two.Do(func() { close(secondDone) }) })
	for _, v := range []struct{ done, stop chan struct{} }{{firstDone, firstStop}, {secondDone, secondStop}} {
		stop := v.stop
		if _, e := r.openCleanup(v.done, func(context.Context) (func(), error) { return func() { close(stop) }, nil }); e != nil {
			t.Fatal(e)
		}
	}
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	familyWait(t, firstStop, "first owned process not signaled")
	familyWait(t, secondStop, "second process cancellation waited for first actual Wait")
	familyPending(t, closed, "runtime ignored actual worker completion")
	one.Do(func() { close(firstDone) })
	familyPending(t, closed, "first worker completion refunded second worker")
	two.Do(func() { close(secondDone) })
	familyWait(t, closed, "all real resource completions did not refund owner")
	if owner.closed.Load() != 1 {
		t.Fatal("original cleanup owner not closed once")
	}
}

func TestFamilyConsumerBlockedWriteAndDeadlineCallbackJoinWithoutCancelingOriginal(t *testing.T) {
	_, _, r, owner := familyConsumerRuntime(t)
	caller, cancel := context.WithCancel(context.Background())
	c, err := r.beginSubread(caller)
	if err != nil {
		t.Fatal(err)
	}
	target := newFamilyControlledWriter()
	var releaseWrite, releaseCallback sync.Once
	t.Cleanup(func() {
		releaseWrite.Do(func() { close(target.writeGate) })
		releaseCallback.Do(func() { close(target.callbackGate) })
	})
	w, err := c.ResponseWriter(target)
	if err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		if _, e := w.Write(bytes.Repeat([]byte{'x'}, 128<<10)); e == nil {
			t.Error("canceled actual consumer write succeeded")
		}
	}()
	familyWait(t, target.entered, "actual consumer write did not block")
	cancel()
	familyWait(t, target.interrupted, "consumer cancellation did not interrupt actual write")
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	familyPending(t, closed, "consumer refunded before real write completion")
	if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
		t.Fatal("consumer deadline cancellation retired original runtime")
	}
	releaseWrite.Do(func() { close(target.writeGate) })
	familyWait(t, writeDone, "actual consumer write not joined")
	familyPending(t, closed, "running consumer deadline callback was not joined")
	releaseCallback.Do(func() { close(target.callbackGate) })
	familyWait(t, closed, "actual consumer deadline callback not joined")
	if owner.closed.Load() != 0 || r.Check(r.Context()) != nil {
		t.Fatal("consumer writer retirement refunded original owner")
	}
}

func TestFamilyConsumerOriginalAbsoluteExpiryCannotBeExtendedByFreshConsumers(t *testing.T) {
	s, b, owner := familyHTTPControlled(t)
	owner.cancel()
	owner.expires = time.Now().Add(1500 * time.Millisecond)
	owner.ctx, owner.cancel = context.WithDeadline(context.Background(), owner.expires)
	original := s.request("GET", "/?profileId=profile", "")
	q, ok := b.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("short original denied")
	}
	t.Cleanup(q.Close)
	r, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := r.beginConsumer(httptest.NewRecorder(), s.request("GET", "/?profileId=profile", ""), "profile")
	if err != nil {
		t.Fatal(err)
	}
	if end, ok := c.Context().Deadline(); !ok || !end.Equal(r.ExpiresAt()) || end.After(owner.expires) {
		t.Fatal("consumer extended original absolute expiry")
	}
	familyWait(t, c.Context().Done(), "original absolute expiry did not cancel consumer")
	if c.Check(context.Background()) == nil || r.Check(context.Background()) == nil {
		t.Fatal("expired original allowed continued runtime work")
	}
	if next, err := r.beginSubread(context.Background()); next != nil || err == nil {
		t.Fatal("expired runtime issued a new subread")
	}
	q.Close()
	if owner.closed.Load() != 1 {
		t.Fatal("expired runtime did not retire exactly once")
	}
}

func TestFamilyConsumerRequestCloseJoinsInFlightOriginalAdoption(t *testing.T) {
	s, b, base := familyHTTPControlled(t)
	o := &familyConsumerBlockedOwner{familyHTTPTestOwner: base, entered: make(chan struct{}), gate: make(chan struct{})}
	b.capture = func(*http.Request, string) (vodOriginalOwner, error) { return o, nil }
	original := s.request("GET", "/?profileId=profile", "")
	q, ok := b.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("original denied")
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(o.gate) }); q.Close() })
	o.block.Store(true)
	adopted := make(chan struct{})
	go func() {
		defer close(adopted)
		runtime, e := q.adoptRuntime(original)
		if runtime != nil || e == nil {
			t.Error("closed original published a late runtime")
		}
	}()
	familyWait(t, o.entered, "original adoption did not block in current proof")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, q.Context().Done(), "request Close did not cancel pending adoption")
	familyPending(t, closed, "request refunded during original adoption")
	if base.closed.Load() != 0 {
		t.Fatal("original PG slot released before adoption stack joined")
	}
	release.Do(func() { close(o.gate) })
	familyWait(t, adopted, "late adoption did not retire")
	familyWait(t, closed, "request Close failed after original adoption joined")
	if base.closed.Load() != 1 {
		t.Fatal("original pending adoption refund not exact")
	}
}

func TestFamilyConsumerOriginalCloseInterruptsAndJoinsSelectorBodyBeforeRefund(t *testing.T) {
	s, q, r, owner := familyConsumerRuntime(t)
	body := newFamilyControlledRead()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(body.closeGate) }) })
	fresh := s.request("POST", "/?profileId=profile", "")
	fresh.Body = body
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		c, _, e := r.beginConsumer(httptest.NewRecorder(), fresh, "profile")
		if c != nil || e == nil {
			t.Error("late selector body published an admitted consumer")
		}
	}()
	familyWait(t, body.entered, "selector body reader did not block")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, body.closeEntered, "original cancellation did not interrupt actual selector body")
	familyPending(t, closed, "selector reader/callback was not joined before refund")
	if owner.closed.Load() != 0 {
		t.Fatal("original PG owner refunded during blocked selector Close")
	}
	release.Do(func() { close(body.closeGate) })
	familyWait(t, finished, "late selector read failed to retire")
	familyWait(t, closed, "selector setup did not join original cleanup")
	if owner.closed.Load() != 1 {
		t.Fatal("selector completion refund was not exact")
	}
}
