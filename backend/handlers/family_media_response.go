package handlers

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"novastream/services/profileunlock"
)

// OpenBody admits one opener only. Close waits even for a late opener that
// ignores cancellation, so the PG slot cannot be refunded before its FD closes.
func (q *FamilyMediaRequest) OpenBody(open func(context.Context) (io.ReadCloser, error)) (io.ReadCloser, error) {
	if !q.valid() || open == nil || q.Check(q.Context()) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.mu.Lock()
	if q.closed || q.openReady != nil {
		q.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	ready := make(chan struct{})
	q.openReady = ready
	q.mu.Unlock()
	defer close(ready)
	if q.Check(q.ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	source, err := open(q.ctx)
	if err != nil || source == nil || q.Check(q.ctx) != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	body := &familyMediaBody{request: q, source: source, done: make(chan struct{})}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		_ = source.Close()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.body = body
	// The callback only interrupts the body. Close owns callback completion.
	body.stop = familyMediaAfterFunc(q.ctx, body.interrupt)
	q.mu.Unlock()
	return body, nil
}

type familyMediaBody struct {
	request *FamilyMediaRequest
	source  io.ReadCloser
	mu      sync.Mutex
	readMu  sync.Mutex
	reads   sync.WaitGroup
	closed  bool
	once    sync.Once
	done    chan struct{}
	stop    func()
}

func (b *familyMediaBody) Read(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	b.reads.Add(1)
	b.mu.Unlock()
	defer b.reads.Done()
	if b.request.Check(b.request.ctx) != nil {
		b.request.cancel()
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	if len(p) == 0 {
		return 0, nil
	}
	buffer := make([]byte, min(len(p), 32<<10))
	defer clear(buffer)
	n, err := b.source.Read(buffer)
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if n < 0 || n > len(buffer) || err != nil && err != io.EOF || closed || b.request.Check(b.request.ctx) != nil {
		b.request.cancel()
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	copy(p, buffer[:n])
	return n, err
}
func (b *familyMediaBody) interrupt() {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		_ = b.source.Close()
		b.reads.Wait()
		close(b.done)
	})
}
func (b *familyMediaBody) Close() error {
	b.interrupt()
	if b.stop != nil {
		b.stop()
	}
	return nil
}

// ResponseWriter owns one response, preserving the original absolute deadline.
// It intentionally has no Unwrap or ReaderFrom bypass around current checks.
func (q *FamilyMediaRequest) ResponseWriter(w http.ResponseWriter) (http.ResponseWriter, error) {
	if !q.valid() || w == nil || q.Check(q.Context()) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.mu.Lock()
	if q.closed || q.writerReady != nil {
		q.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	ready := make(chan struct{})
	q.writerReady = ready
	q.mu.Unlock()
	defer close(ready)
	controller := http.NewResponseController(w)
	deadline := q.ExpiresAt()
	if expiry, ok := q.ctx.Deadline(); ok && expiry.Before(deadline) {
		deadline = expiry
	}
	if deadline.IsZero() || controller.SetWriteDeadline(deadline) != nil || q.Check(q.ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	guard := &familyMediaWriter{request: q, target: w, controller: controller, deadline: deadline}
	guard.stop = familyMediaAfterFunc(q.ctx, func() { _ = guard.SetWriteDeadline(time.Now()) })
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		guard.close()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.writer = guard
	q.mu.Unlock()
	return guard, nil
}

type familyMediaWriter struct {
	mu         sync.Mutex
	request    *FamilyMediaRequest
	target     http.ResponseWriter
	controller *http.ResponseController
	deadline   time.Time
	closed     bool
	writes     sync.WaitGroup
	once       sync.Once
	stop       func()
	deadlineMu sync.Mutex
}

func (w *familyMediaWriter) Header() http.Header { return w.target.Header() }
func (w *familyMediaWriter) begin() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return profileunlock.ErrFamilyMediaChanged
	}
	w.writes.Add(1)
	w.mu.Unlock()
	if err := w.request.Check(w.request.ctx); err != nil {
		w.writes.Done()
		w.request.cancel()
		return err
	}
	return nil
}
func (w *familyMediaWriter) WriteHeader(status int) {
	if w.begin() != nil {
		panic(http.ErrAbortHandler)
	}
	defer w.writes.Done()
	w.target.WriteHeader(status)
	if w.request.Check(w.request.ctx) != nil {
		w.request.cancel()
		panic(http.ErrAbortHandler)
	}
}
func (w *familyMediaWriter) Write(p []byte) (int, error) {
	if err := w.begin(); err != nil {
		return 0, err
	}
	defer w.writes.Done()
	written := 0
	for written < len(p) || written == 0 && len(p) == 0 {
		if w.request.Check(w.request.ctx) != nil {
			w.request.cancel()
			return written, profileunlock.ErrFamilyMediaChanged
		}
		part := p[written:min(len(p), written+(32<<10))]
		n, err := w.target.Write(part)
		if n < 0 || n > len(part) {
			w.request.cancel()
			return written, profileunlock.ErrFamilyMediaChanged
		}
		written += n
		if w.request.Check(w.request.ctx) != nil {
			w.request.cancel()
			return written, profileunlock.ErrFamilyMediaChanged
		}
		if err != nil {
			return written, err
		}
		if n != len(part) {
			return written, io.ErrShortWrite
		}
		if len(p) == 0 {
			break
		}
	}
	return written, nil
}
func (w *familyMediaWriter) FlushError() error {
	if err := w.begin(); err != nil {
		return err
	}
	defer w.writes.Done()
	if err := w.controller.Flush(); err != nil {
		return err
	}
	if w.request.Check(w.request.ctx) != nil {
		w.request.cancel()
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func (w *familyMediaWriter) Flush() {
	if w.FlushError() != nil {
		panic(http.ErrAbortHandler)
	}
}
func (w *familyMediaWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	if deadline.IsZero() || deadline.After(w.deadline) {
		deadline = w.deadline
	}
	if w.request.ctx.Err() != nil {
		deadline = time.Now()
	}
	return w.controller.SetWriteDeadline(deadline)
}
func (w *familyMediaWriter) close() {
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		w.writes.Wait()
		if w.stop != nil {
			w.stop()
		}
	})
}
