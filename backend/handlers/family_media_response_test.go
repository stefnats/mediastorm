package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type familyControlledRead struct {
	entered         chan struct{}
	closeEntered    chan struct{}
	closeGate       chan struct{}
	readGate        chan struct{}
	once, closeOnce sync.Once
	buffer          []byte
}

func newFamilyControlledRead() *familyControlledRead {
	return &familyControlledRead{entered: make(chan struct{}), closeEntered: make(chan struct{}), closeGate: make(chan struct{}), readGate: make(chan struct{})}
}
func (s *familyControlledRead) Read(p []byte) (int, error) {
	s.buffer = p
	s.once.Do(func() { close(s.entered) })
	<-s.readGate
	return copy(p, []byte("synthetic late bytes")), nil
}
func (s *familyControlledRead) Close() error {
	s.closeOnce.Do(func() { close(s.closeEntered); <-s.closeGate; close(s.readGate) })
	return nil
}
func familyWait(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}
func familyPending(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(message)
	default:
	}
}

func TestFamilyHTTPReadWatcherDiscardsLateBytesAndJoinsActualSourceBeforeRefund(t *testing.T) {
	q, owner := familyHTTPRequest(t)
	source := newFamilyControlledRead()
	body, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(source.closeGate) }) }
	t.Cleanup(release)
	caller := bytes.Repeat([]byte{0x7f}, 64)
	readDone := make(chan struct{})
	var n int
	var readErr error
	go func() { defer close(readDone); n, readErr = body.Read(caller) }()
	familyWait(t, source.entered, "actual source never blocked")
	owner.changed.Store(true) // Only the actual production watcher cancels the blocked IO.
	familyWait(t, source.closeEntered, "current owner change did not interrupt actual source")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyPending(t, closed, "HTTP owner closed before actual source Close joined")
	if owner.closed.Load() != 0 {
		t.Fatal("PG admission refunded while source Close was blocked")
	}
	release()
	familyWait(t, readDone, "source read did not join cancellation")
	familyWait(t, closed, "HTTP owner did not finish actual IO retirement")
	if n != 0 || readErr == nil || !bytes.Equal(caller, bytes.Repeat([]byte{0x7f}, 64)) {
		t.Fatal("late source bytes reached caller")
	}
	if !bytes.Equal(source.buffer, make([]byte, len(source.buffer))) {
		t.Fatal("late staging bytes were not cleared")
	}
	if owner.closed.Load() != 1 {
		t.Fatal("original owner was not refunded exactly once")
	}
}

type familyErrorRead struct {
	buffer []byte
	closed atomic.Int32
}

func (s *familyErrorRead) Read(p []byte) (int, error) {
	s.buffer = p
	return copy(p, "synthetic failing source"), errors.New("synthetic failure")
}
func (s *familyErrorRead) Close() error { s.closed.Add(1); return nil }
func TestFamilyHTTPBodyRejectsErroredChunksBoundsReadsAndHasOneOpener(t *testing.T) {
	q, _ := familyHTTPRequest(t)
	source := &familyErrorRead{}
	body, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if duplicate, e := q.OpenBody(func(context.Context) (io.ReadCloser, error) { calls++; return nil, nil }); e == nil || duplicate != nil || calls != 0 {
		t.Fatal("duplicate source opener ran")
	}
	caller := make([]byte, 128<<10)
	n, e := body.Read(caller)
	if n != 0 || e == nil || !bytes.Equal(caller, make([]byte, len(caller))) || len(source.buffer) != 32<<10 {
		t.Fatal("errored or oversized staging chunk published")
	}
	if !bytes.Equal(source.buffer, make([]byte, len(source.buffer))) {
		t.Fatal("errored staging bytes retained")
	}
	q.Close()
	if source.closed.Load() != 1 {
		t.Fatal("actual source closed more than once")
	}
}

func TestFamilyHTTPLateOpenerMustFinishActualCloseBeforeOwnerRefund(t *testing.T) {
	q, owner := familyHTTPRequest(t)
	entered, release, opened := make(chan struct{}), make(chan struct{}), make(chan struct{})
	source := &familyErrorRead{}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	go func() {
		defer close(opened)
		body, err := q.OpenBody(func(context.Context) (io.ReadCloser, error) { close(entered); <-release; return source, nil })
		if body != nil || err == nil {
			t.Error("late canceled source published")
		}
	}()
	familyWait(t, entered, "opener not entered")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, q.Context().Done(), "original request did not cancel")
	familyPending(t, closed, "owner refunded before blocked opener completed")
	if owner.closed.Load() != 0 {
		t.Fatal("original owner closed before opener returned")
	}
	once.Do(func() { close(release) })
	familyWait(t, opened, "late opener did not retire")
	familyWait(t, closed, "owner did not finish after source close")
	if source.closed.Load() != 1 || owner.closed.Load() != 1 {
		t.Fatal("late source or original owner cleanup was not exact")
	}
}

type familyControlledWriter struct {
	mu                                            sync.Mutex
	header                                        http.Header
	deadlines                                     []time.Time
	entered, interrupted, writeGate, callbackGate chan struct{}
	enterOnce, interruptOnce                      sync.Once
	maxWrite                                      int
}

func newFamilyControlledWriter() *familyControlledWriter {
	return &familyControlledWriter{header: make(http.Header), entered: make(chan struct{}), interrupted: make(chan struct{}), writeGate: make(chan struct{}), callbackGate: make(chan struct{})}
}
func (w *familyControlledWriter) Header() http.Header { return w.header }
func (w *familyControlledWriter) WriteHeader(int)     {}
func (w *familyControlledWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.maxWrite = max(w.maxWrite, len(p))
	w.mu.Unlock()
	w.enterOnce.Do(func() { close(w.entered) })
	<-w.interrupted
	<-w.writeGate
	return 0, io.ErrClosedPipe
}
func (w *familyControlledWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	if !deadline.After(time.Now().Add(time.Second)) {
		w.interruptOnce.Do(func() { close(w.interrupted) })
		<-w.callbackGate
	}
	return nil
}
func TestFamilyHTTPWriterOriginalDeadlineCancellationAndActualWriteCallbackJoin(t *testing.T) {
	q, owner := familyHTTPRequest(t)
	target := newFamilyControlledWriter()
	var releaseWrite, releaseCallback sync.Once
	t.Cleanup(func() {
		releaseWrite.Do(func() { close(target.writeGate) })
		releaseCallback.Do(func() { close(target.callbackGate) })
	})
	writer, err := q.ResponseWriter(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := writer.(interface{ Unwrap() http.ResponseWriter }); ok {
		t.Fatal("writer exposed a publication bypass")
	}
	if _, ok := writer.(io.ReaderFrom); ok {
		t.Fatal("writer exposed unchecked ReaderFrom")
	}
	target.mu.Lock()
	initial := target.deadlines[0]
	target.mu.Unlock()
	if !initial.Equal(q.ExpiresAt()) {
		t.Fatal("original deadline not installed before watcher")
	}
	written := make(chan struct{})
	go func() {
		defer close(written)
		n, e := writer.Write(bytes.Repeat([]byte{'x'}, 128<<10))
		if n != 0 || e == nil {
			t.Error("blocked denied write became success")
		}
	}()
	familyWait(t, target.entered, "actual writer did not block")
	owner.changed.Store(true)
	familyWait(t, target.interrupted, "current owner change did not shorten actual write deadline")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyPending(t, closed, "owner closed before actual writer returned")
	if owner.closed.Load() != 0 {
		t.Fatal("owner admission refunded during blocked write")
	}
	releaseWrite.Do(func() { close(target.writeGate) })
	familyWait(t, written, "actual write did not retire")
	familyPending(t, closed, "running deadline callback was not joined")
	if owner.closed.Load() != 0 {
		t.Fatal("owner admission refunded before callback returned")
	}
	releaseCallback.Do(func() { close(target.callbackGate) })
	familyWait(t, closed, "writer callback/source owner failed to join")
	target.mu.Lock()
	largest := target.maxWrite
	target.mu.Unlock()
	if largest > 32<<10 || owner.closed.Load() != 1 {
		t.Fatal("write bound or original close violated")
	}
}
func TestFamilyHTTPWriterUnavailableDeadlineAndLostOwnerNeverPublish(t *testing.T) {
	q, _ := familyHTTPRequest(t)
	if writer, err := q.ResponseWriter(httptest.NewRecorder()); err == nil || writer != nil {
		t.Fatal("unsupported write deadline became uninterruptible response")
	}
	q.Close()
	q2, owner := familyHTTPRequest(t)
	target := newFamilyControlledWriter()
	close(target.writeGate)
	close(target.callbackGate)
	writer, err := q2.ResponseWriter(target)
	if err != nil {
		t.Fatal(err)
	}
	owner.changed.Store(true)
	if n, e := writer.Write([]byte("must not publish")); n != 0 || e == nil {
		t.Fatal("lost owner published body")
	}
	if err := writer.(*familyMediaWriter).SetWriteDeadline(time.Now().Add(24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	last := target.deadlines[len(target.deadlines)-1]
	target.mu.Unlock()
	if last.After(time.Now().Add(time.Second)) {
		t.Fatal("late deadline setter renewed canceled response")
	}
}
