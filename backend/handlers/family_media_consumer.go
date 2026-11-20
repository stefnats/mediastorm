package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"novastream/services/profileunlock"
)

const (
	familyMediaGlobalConsumers  = 256
	familyMediaRuntimeConsumers = 8
	familyMediaRuntimeCleanups  = 8
)

var familyMediaConsumerBudget struct {
	sync.Mutex
	active int
}

// These private objects cannot be recovered from a session ID, DTO or copy.
type familyMediaRuntime struct{ *familyMediaRuntimeState }
type familyMediaRuntimeState struct {
	mu        sync.Mutex
	self      *familyMediaRuntime
	request   *FamilyMediaRequest
	consumers map[*familyMediaConsumer]struct{}
	cleanups  map[*familyMediaCleanup]struct{}
	retired   bool
	once      sync.Once
	done      chan struct{}
}

func (familyMediaRuntime) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("familyMediaRuntime(opaque)"))
}
func (familyMediaRuntime) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*familyMediaRuntime) UnmarshalJSON([]byte) error  { return profileunlock.ErrFamilyMediaChanged }
func (r *familyMediaRuntime) valid() bool {
	return r != nil && r.familyMediaRuntimeState != nil && r.self == r
}

// Adoption precedes actual runtime IO. The original request remains its sole
// authority; neither a later consumer nor a worker may capture a replacement.
func (q *FamilyMediaRequest) adoptRuntime(original *http.Request) (*familyMediaRuntime, error) {
	if !q.valid() || original == nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.mu.Lock()
	if q.closed || q.runtimeReady != nil || q.openReady != nil || q.writerReady != nil {
		q.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	ready := make(chan struct{})
	q.runtimeReady = ready
	q.mu.Unlock()
	defer close(ready)
	if q.Handoff(original) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	r := &familyMediaRuntime{&familyMediaRuntimeState{request: q, consumers: make(map[*familyMediaConsumer]struct{}), cleanups: make(map[*familyMediaCleanup]struct{}), done: make(chan struct{})}}
	r.self = r
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.ctx.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	q.runtime = r
	return r, nil
}

func (r *familyMediaRuntime) Context() context.Context {
	if !r.valid() {
		return familyClosedContext
	}
	return r.request.Context()
}
func (r *familyMediaRuntime) ExpiresAt() time.Time {
	if !r.valid() {
		return time.Time{}
	}
	return r.request.ExpiresAt()
}
func (r *familyMediaRuntime) Check(ctx context.Context) error {
	if !r.valid() || ctx == nil || ctx.Err() != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	r.mu.Lock()
	retired := r.retired
	r.mu.Unlock()
	if retired {
		return profileunlock.ErrFamilyMediaChanged
	}
	err := r.request.Check(ctx)
	// A departed consumer has no authority to retire its original worker.
	if err != nil && ctx.Err() == nil {
		r.request.cancel()
	}
	return err
}

type familyMediaConsumer struct{ *familyMediaConsumerState }
type familyMediaConsumerState struct {
	mu          sync.Mutex
	self        *familyMediaConsumer
	runtime     *familyMediaRuntime
	ctx         context.Context
	cancel      context.CancelFunc
	setupDone   chan struct{}
	stopCaller  func()
	once        sync.Once
	closed      bool
	openReady   chan struct{}
	body        *familyMediaBody
	writerReady chan struct{}
	writer      *familyMediaWriter
}

// The setup selector reader belongs to this admitted consumer too. Capture the
// original body so cancellation cannot close a later restored decoder cursor.
type familyMediaSelectorBody struct {
	body io.ReadCloser
	once sync.Once
}

func (b *familyMediaSelectorBody) Read(p []byte) (int, error) { return b.body.Read(p) }
func (b *familyMediaSelectorBody) Close() error {
	var err error
	b.once.Do(func() { err = b.body.Close() })
	return err
}

func (familyMediaConsumer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("familyMediaConsumer(opaque)"))
}
func (familyMediaConsumer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*familyMediaConsumer) UnmarshalJSON([]byte) error  { return profileunlock.ErrFamilyMediaChanged }
func (c *familyMediaConsumer) valid() bool {
	return c != nil && c.familyMediaConsumerState != nil && c.self == c
}

// No PG check, selector allocation or opener precedes this shared reservation.
func (r *familyMediaRuntime) reserveConsumer(caller context.Context) (*familyMediaConsumer, error) {
	if !r.valid() || caller == nil || caller.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	familyMediaConsumerBudget.Lock()
	defer familyMediaConsumerBudget.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired || r.Context().Err() != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	if familyMediaConsumerBudget.active >= familyMediaGlobalConsumers || len(r.consumers) >= familyMediaRuntimeConsumers {
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	ctx, cancel := context.WithDeadline(r.Context(), r.ExpiresAt())
	c := &familyMediaConsumer{&familyMediaConsumerState{runtime: r, ctx: ctx, cancel: cancel, setupDone: make(chan struct{})}}
	c.self = c
	r.consumers[c] = struct{}{}
	familyMediaConsumerBudget.active++
	return c, nil
}

// A fresh browser request is checked before its operation-specific decoder.
// The returned request retains its original body cursor after selector checks.
func (r *familyMediaRuntime) beginConsumer(w http.ResponseWriter, fresh *http.Request, target string) (*familyMediaConsumer, *http.Request, error) {
	if fresh == nil || w == nil {
		return nil, nil, profileunlock.ErrFamilyMediaChanged
	}
	c, err := r.reserveConsumer(fresh.Context())
	if err != nil {
		return nil, nil, err
	}
	c.stopCaller = familyMediaAfterFunc(fresh.Context(), c.cancel)
	var stopSelectors func()
	if fresh.Body != nil && fresh.Body != http.NoBody {
		body := &familyMediaSelectorBody{body: fresh.Body}
		fresh.Body = body
		stopSelectors = familyMediaAfterFunc(c.ctx, func() { _ = body.Close() })
	}
	checked, selected, err := familyMediaSelectors(w, fresh, target)
	if stopSelectors != nil {
		stopSelectors()
	}
	if err == nil && selected == r.request.target && target == r.request.target {
		err = r.request.CheckRequest(checked, target)
	} else {
		err = profileunlock.ErrFamilyMediaChanged
	}
	if err == nil {
		err = c.Check(c.ctx)
	}
	close(c.setupDone)
	if err != nil {
		c.Close()
		return nil, nil, err
	}
	return c, checked.WithContext(c.ctx), nil
}

// Subreads are actual server runtime work, never a forged fresh HTTP request.
// Their exact source/companion permission is supplied by the later local port.
func (r *familyMediaRuntime) beginSubread(caller context.Context) (*familyMediaConsumer, error) {
	c, err := r.reserveConsumer(caller)
	if err != nil {
		return nil, err
	}
	c.stopCaller = familyMediaAfterFunc(caller, c.cancel)
	err = c.Check(c.ctx)
	close(c.setupDone)
	if err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func (c *familyMediaConsumer) Context() context.Context {
	if !c.valid() {
		return familyClosedContext
	}
	return c.ctx
}
func (c *familyMediaConsumer) ExpiresAt() time.Time {
	if !c.valid() {
		return time.Time{}
	}
	return c.runtime.ExpiresAt()
}
func (c *familyMediaConsumer) cancelIO() {
	if c.valid() {
		c.cancel()
	}
}
func (c *familyMediaConsumer) Check(ctx context.Context) error {
	if !c.valid() || ctx == nil || ctx.Err() != nil || c.ctx.Err() != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return profileunlock.ErrFamilyMediaChanged
	}
	return c.runtime.Check(ctx)
}
func (c *familyMediaConsumer) OpenBody(open func(context.Context) (io.ReadCloser, error)) (io.ReadCloser, error) {
	if !c.valid() || open == nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	c.mu.Lock()
	if c.closed || c.openReady != nil {
		c.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	ready := make(chan struct{})
	c.openReady = ready
	c.mu.Unlock()
	defer close(ready)
	if c.Check(c.ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	source, err := open(c.ctx)
	if err != nil || source == nil || c.Check(c.ctx) != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	body := &familyMediaBody{request: c, source: source, done: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = source.Close()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	c.body = body
	body.stop = familyMediaAfterFunc(c.ctx, body.interrupt)
	c.mu.Unlock()
	return body, nil
}
func (c *familyMediaConsumer) ResponseWriter(w http.ResponseWriter) (http.ResponseWriter, error) {
	if !c.valid() || w == nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	c.mu.Lock()
	if c.closed || c.writerReady != nil {
		c.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	ready := make(chan struct{})
	c.writerReady = ready
	c.mu.Unlock()
	defer close(ready)
	if c.Check(c.ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	controller := http.NewResponseController(w)
	deadline := c.ExpiresAt()
	if end, ok := c.ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if deadline.IsZero() || controller.SetWriteDeadline(deadline) != nil || c.Check(c.ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	guard := &familyMediaWriter{request: c, target: w, controller: controller, deadline: deadline}
	guard.stop = familyMediaAfterFunc(c.ctx, func() { _ = guard.SetWriteDeadline(time.Now()) })
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		guard.close()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	c.writer = guard
	c.mu.Unlock()
	return guard, nil
}
func (c *familyMediaConsumer) Close() {
	if !c.valid() {
		return
	}
	c.once.Do(func() {
		c.cancel()
		<-c.setupDone
		c.mu.Lock()
		c.closed = true
		opening, writing := c.openReady, c.writerReady
		c.mu.Unlock()
		if opening != nil {
			<-opening
		}
		if writing != nil {
			<-writing
		}
		c.mu.Lock()
		body, writer := c.body, c.writer
		c.mu.Unlock()
		if body != nil {
			_ = body.Close()
		}
		if writer != nil {
			writer.close()
		}
		if c.stopCaller != nil {
			c.stopCaller()
		}
		familyMediaConsumerBudget.Lock()
		c.runtime.mu.Lock()
		delete(c.runtime.consumers, c)
		familyMediaConsumerBudget.active--
		c.runtime.mu.Unlock()
		familyMediaConsumerBudget.Unlock()
	})
}

type familyMediaCleanup struct{ *familyMediaCleanupState }
type familyMediaCleanupState struct {
	self       *familyMediaCleanup
	runtime    *familyMediaRuntime
	ready      chan struct{}
	actualDone <-chan struct{}
	stop       func()
	opened     bool
	once       sync.Once
}

func (familyMediaCleanup) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("familyMediaCleanup(opaque)"))
}
func (familyMediaCleanup) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*familyMediaCleanup) UnmarshalJSON([]byte) error  { return profileunlock.ErrFamilyMediaChanged }
func (c *familyMediaCleanup) valid() bool {
	return c != nil && c.familyMediaCleanupState != nil && c.self == c
}

// actualDone is closed by the real resource producer only after process Wait,
// pumps/FDs/output have retired. A late opener remains charged until then.
// Its stop function only interrupts that resource; it must not call this
// cleanup owner or runtime Close, which join the stop/actualDone stack.
func (r *familyMediaRuntime) openCleanup(actualDone <-chan struct{}, open func(context.Context) (func(), error)) (*familyMediaCleanup, error) {
	if !r.valid() || actualDone == nil || open == nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	r.mu.Lock()
	if r.retired || r.Context().Err() != nil {
		r.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	if len(r.cleanups) >= familyMediaRuntimeCleanups {
		r.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	c := &familyMediaCleanup{&familyMediaCleanupState{runtime: r, ready: make(chan struct{}), actualDone: actualDone}}
	c.self = c
	r.cleanups[c] = struct{}{}
	r.mu.Unlock()
	err := r.Check(r.Context())
	if err == nil {
		c.opened = true
		c.stop, err = open(r.Context())
	}
	if err == nil && c.stop == nil {
		err = profileunlock.ErrFamilyMediaChanged
	}
	if err == nil {
		err = r.Check(r.Context())
	}
	close(c.ready)
	if err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func (c *familyMediaCleanup) Close() {
	if !c.valid() {
		return
	}
	c.once.Do(func() {
		<-c.ready
		if c.opened {
			if c.stop != nil {
				c.stop()
			}
			<-c.actualDone
		}
		c.runtime.mu.Lock()
		delete(c.runtime.cleanups, c)
		c.runtime.mu.Unlock()
	})
}
func (r *familyMediaRuntime) Close() {
	if !r.valid() {
		return
	}
	r.once.Do(func() {
		r.mu.Lock()
		r.retired = true
		consumers := make([]*familyMediaConsumer, 0, len(r.consumers))
		for c := range r.consumers {
			consumers = append(consumers, c)
		}
		cleanups := make([]*familyMediaCleanup, 0, len(r.cleanups))
		for c := range r.cleanups {
			cleanups = append(cleanups, c)
		}
		r.mu.Unlock()
		r.request.cancel()
		// Signal every owner before joining one blocked cleanup. These tasks
		// remain bounded by the reservations held until their actual completion.
		var joined sync.WaitGroup
		for _, c := range consumers {
			joined.Add(1)
			go func(c *familyMediaConsumer) { defer joined.Done(); c.Close() }(c)
		}
		for _, c := range cleanups {
			joined.Add(1)
			go func(c *familyMediaCleanup) { defer joined.Done(); c.Close() }(c)
		}
		joined.Wait()
		r.request.closeRequestIO()
		close(r.done)
	})
}
