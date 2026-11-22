package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"novastream/internal/nzbfilesystem"
)

// OriginalNZBNamespace is a concrete physical runtime binding, never media permission.
type OriginalNZBNamespace struct{ *originalNZBNamespaceState }
type originalNZBNamespaceState struct {
	self        *OriginalNZBNamespace
	owner       *NzbSystem
	fs          *nzbfilesystem.NzbFilesystem
	work        *nzbStreamWork
	lower       *nzbfilesystem.OriginalMetadataNamespace
	ready, done chan struct{}
	setupErr    error
	closeErr    error
}
type OriginalNZBMetadataInfo struct{ Size int64 }
type OriginalNZBMetadata struct{ *originalNZBMetadataState }
type originalNZBMetadataState struct {
	mu        sync.Mutex
	closed    bool
	pending   sync.WaitGroup
	self      *OriginalNZBMetadata
	namespace *OriginalNZBNamespace
	work      *nzbStreamWork
	lower     *nzbfilesystem.OriginalMetadataSource
	done      chan struct{}
	err       error
	attempt   *OriginalNZBAttempt
}
type OriginalNZBStream struct{ *originalNZBStreamState }
type originalNZBStreamState struct {
	self     *OriginalNZBStream
	metadata *OriginalNZBMetadata
	lower    *nzbfilesystem.OriginalMetadataStream
}

func (ns *NzbSystem) CaptureOriginalNamespace(ctx context.Context) (*OriginalNZBNamespace, error) {
	if ns == nil || ns.self != ns || ctx == nil || ctx.Err() != nil {
		return nil, ErrNzbSystemUnavailable
	}
	ns.runtimeMu.Lock()
	if ns.closed || !ns.prepared || ns.life.Err() != nil || ns.nzbFs == nil {
		ns.runtimeMu.Unlock()
		return nil, ErrNzbSystemUnavailable
	}
	if previous := ns.originalNamespace; previous != nil {
		ns.runtimeMu.Unlock()
		select {
		case <-previous.ready:
		default:
			return nil, ErrNzbSystemUnavailable
		}
		if !previous.valid() || previous.work.ctx.Err() != nil || previous.lower == nil || previous.setupErr != nil {
			return nil, ErrNzbSystemUnavailable
		}
		return previous, nil
	}
	if len(ns.sources) >= 32 {
		ns.runtimeMu.Unlock()
		return nil, ErrNzbSystemUnavailable
	}
	// A namespace occupies the same actual source registry, including its setup.
	life, cancel := context.WithCancel(ns.life)
	work := &nzbStreamWork{owner: ns, ctx: life, cancel: cancel}
	work.stopRuntime = nzbRuntimeSignal(ns.life, cancel)
	ns.sources[work] = struct{}{}
	ns.sourceWork.Add(1)
	n := &OriginalNZBNamespace{originalNZBNamespaceState: &originalNZBNamespaceState{owner: ns, fs: ns.nzbFs, work: work, ready: make(chan struct{}), done: make(chan struct{})}}
	n.self = n
	ns.originalNamespace = n
	ns.runtimeMu.Unlock()
	go n.retire()
	setup, finish := context.WithTimeout(ctx, 10*time.Second)
	stopCaller := nzbRuntimeSignal(setup, work.cancel)
	actual, err := n.fs.CaptureOriginalMetadataNamespace(work.ctx)
	if err == nil && ns.originalNamespaceObserved != nil {
		ns.originalNamespaceObserved()
	}
	stopCaller()
	finish()
	if ctx.Err() != nil || work.ctx.Err() != nil {
		err = ErrNzbSystemUnavailable
	}
	n.lower = actual
	n.setupErr = err
	close(n.ready)
	if err != nil {
		work.cancel()
		<-n.done
		return nil, err
	}
	return n, nil
}
func (n *OriginalNZBNamespace) valid() bool {
	return n != nil && n.originalNZBNamespaceState != nil && n.self == n && n.owner != nil && n.owner.self == n.owner && n.work != nil && n.fs != nil
}
func (n *OriginalNZBNamespace) runtimeCurrent(active bool) error {
	if !n.valid() {
		return ErrNzbSystemUnavailable
	}
	select {
	case <-n.ready:
	default:
		return ErrNzbSystemUnavailable
	}
	ns := n.owner
	ns.runtimeMu.Lock()
	defer ns.runtimeMu.Unlock()
	if ns.closed || !ns.prepared || ns.originalNamespace != n || ns.nzbFs != n.fs || ns.life.Err() != nil || n.work.ctx.Err() != nil || n.lower == nil || n.setupErr != nil || active && !ns.active {
		return ErrNzbSystemUnavailable
	}
	return nil
}
func (n *OriginalNZBNamespace) MatchesOriginal(ctx context.Context, expected *NzbSystem, root string) error {
	if !n.valid() || n.owner != expected || ctx == nil || ctx.Err() != nil {
		return ErrNzbSystemUnavailable
	}
	if err := n.runtimeCurrent(false); err != nil {
		return err
	}
	return n.lower.MatchesOriginal(ctx, n.fs, root)
}
func (n *OriginalNZBNamespace) signal() {
	if n.valid() {
		n.work.cancel()
	}
}
func (n *OriginalNZBNamespace) retire() {
	<-n.work.ctx.Done()
	<-n.ready
	if n.lower != nil {
		n.closeErr = n.lower.CloseAndJoin()
	}
	// No wait on the whole sourceWork group: it contains this exact reservation.
	n.work.finish()
	close(n.done)
}
func (n *OriginalNZBNamespace) closeAndJoin() error {
	if !n.valid() {
		return ErrNzbSystemUnavailable
	}
	n.signal()
	<-n.done
	return n.closeErr
}
func (n *OriginalNZBNamespace) CaptureMetadata(ctx context.Context, name string) (*OriginalNZBMetadata, error) {
	if err := n.runtimeCurrent(true); err != nil {
		return nil, err
	}
	work, err := n.owner.beginStreamWork(ctx)
	if err != nil {
		return nil, err
	}
	lower, err := n.lower.Capture(work.ctx, name)
	if err != nil {
		work.finish()
		return nil, err
	}
	if n.owner.originalMetadataObserved != nil {
		n.owner.originalMetadataObserved()
	}
	m := &OriginalNZBMetadata{originalNZBMetadataState: &originalNZBMetadataState{namespace: n, work: work, lower: lower, done: make(chan struct{})}}
	m.self = m
	go m.retire()
	if err = m.Check(ctx); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}
func (m *OriginalNZBMetadata) valid() bool {
	return m != nil && m.originalNZBMetadataState != nil && m.self == m && m.namespace != nil && m.namespace.valid() && m.lower != nil && m.work != nil
}
func (m *OriginalNZBMetadata) retire() {
	select {
	case <-m.work.ctx.Done():
	case <-m.lower.Context().Done():
	}
	m.mu.Lock()
	m.closed = true
	a := m.attempt
	m.mu.Unlock()
	m.lower.SignalClose()
	if a != nil {
		a.SignalClose()
	}
	m.pending.Wait()
	var attemptErr error
	if a != nil {
		attemptErr = a.CloseAndJoin()
	}
	m.err = m.lower.CloseAndJoin()
	if attemptErr != nil {
		m.err = errors.Join(m.err, attemptErr)
	}
	m.work.finish()
	close(m.done)
}
func (m *OriginalNZBMetadata) Check(ctx context.Context) error {
	if !m.valid() || ctx == nil || ctx.Err() != nil || m.work.ctx.Err() != nil {
		return ErrNzbSystemUnavailable
	}
	if err := m.namespace.runtimeCurrent(true); err != nil {
		m.lower.SignalClose()
		return err
	}
	return m.lower.Check(ctx)
}
func originalNZBCancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (m *OriginalNZBMetadata) Context() context.Context {
	if !m.valid() {
		return originalNZBCancelledContext()
	}
	return m.lower.Context()
}
func (m *OriginalNZBMetadata) ExpiresAt() time.Time {
	if !m.valid() {
		return time.Time{}
	}
	return m.lower.ExpiresAt()
}
func (m *OriginalNZBMetadata) Info(ctx context.Context) (OriginalNZBMetadataInfo, error) {
	if err := m.Check(ctx); err != nil {
		return OriginalNZBMetadataInfo{}, err
	}
	info, err := m.lower.Info(ctx)
	return OriginalNZBMetadataInfo{Size: info.Size}, err
}
func (m *OriginalNZBMetadata) Open(ctx context.Context, start, length int64) (*OriginalNZBStream, error) {
	if err := m.Check(ctx); err != nil {
		return nil, err
	}
	lower, err := m.lower.Open(ctx, start, length)
	if err != nil {
		return nil, err
	}
	r := &OriginalNZBStream{originalNZBStreamState: &originalNZBStreamState{metadata: m, lower: lower}}
	r.self = r
	if err = m.Check(ctx); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}
func (m *OriginalNZBMetadata) Close() error {
	if !m.valid() {
		return ErrNzbSystemUnavailable
	}
	m.lower.SignalClose()
	<-m.done
	return m.err
}
func (r *OriginalNZBStream) valid() bool {
	return r != nil && r.originalNZBStreamState != nil && r.self == r && r.metadata != nil && r.metadata.valid() && r.lower != nil
}
func (r *OriginalNZBStream) Context() context.Context {
	if !r.valid() {
		return originalNZBCancelledContext()
	}
	return r.lower.Context()
}
func (r *OriginalNZBStream) Check(ctx context.Context) error {
	if !r.valid() {
		return ErrNzbSystemUnavailable
	}
	if err := r.metadata.Check(ctx); err != nil {
		return err
	}
	return r.lower.Check(ctx)
}
func (r *OriginalNZBStream) Read(p []byte) (n int, err error) {
	var pendingOwner *OriginalNZBMetadata
	defer func() {
		if err != nil && err != io.EOF {
			clear(p)
			n = 0
		}
		if pendingOwner != nil {
			pendingOwner.pending.Done()
		}
	}()
	if !r.valid() {
		return 0, ErrNzbSystemUnavailable
	}
	m := r.metadata
	m.mu.Lock()
	if m.closed || m.work.ctx.Err() != nil {
		m.mu.Unlock()
		return 0, ErrNzbSystemUnavailable
	}
	m.pending.Add(1)
	m.mu.Unlock()
	pendingOwner = m
	if err := r.Check(r.Context()); err != nil {
		return 0, err
	}
	n, err = r.lower.Read(p)
	if after := r.Check(r.Context()); after != nil {
		return 0, after
	}
	return n, err
}
func (r *OriginalNZBStream) Close() error {
	if !r.valid() {
		return ErrNzbSystemUnavailable
	}
	return r.metadata.Close()
}
func (r *OriginalNZBStream) CleanupDone() <-chan struct{} {
	if !r.valid() {
		return nil
	}
	return r.metadata.done
}

func (OriginalNZBNamespace) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalNZBNamespace(opaque)"))
}
func (OriginalNZBNamespace) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalNZBNamespace) UnmarshalJSON([]byte) error  { return ErrNzbSystemUnavailable }

func (OriginalNZBMetadata) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalNZBMetadata(opaque)"))
}
func (OriginalNZBMetadata) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalNZBMetadata) UnmarshalJSON([]byte) error  { return ErrNzbSystemUnavailable }

func (OriginalNZBStream) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalNZBStream(opaque)"))
}
func (OriginalNZBStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalNZBStream) UnmarshalJSON([]byte) error  { return ErrNzbSystemUnavailable }

// OriginalNZBAttempt keeps upper post-read checks and buffer cleanup owned until
// the identical lower request has physically ended. It never releases metadata.
type OriginalNZBAttempt struct{ *originalNZBAttemptState }
type originalNZBAttemptState struct {
	self        *OriginalNZBAttempt
	afterRead   func()
	metadata    *OriginalNZBMetadata
	lower       *nzbfilesystem.OriginalMetadataAttempt
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	pending     sync.WaitGroup
	ready, done chan struct{}
	stopCaller  func()
	err         error
}

func (m *OriginalNZBMetadata) OpenAttempt(ctx context.Context, start, length int64) (*OriginalNZBAttempt, error) {
	if !m.valid() || ctx == nil || ctx.Err() != nil || start < 0 || length <= 0 {
		return nil, ErrNzbSystemUnavailable
	}
	life, cancel := context.WithCancel(m.work.ctx)
	if deadline, ok := ctx.Deadline(); ok {
		cancel()
		life, cancel = context.WithDeadline(m.work.ctx, deadline)
	}
	a := &OriginalNZBAttempt{originalNZBAttemptState: &originalNZBAttemptState{metadata: m, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	a.self = a
	m.mu.Lock()
	if m.closed || m.work.ctx.Err() != nil || m.attempt != nil {
		m.mu.Unlock()
		cancel()
		return nil, ErrNzbSystemUnavailable
	}
	m.attempt = a
	m.pending.Add(1)
	m.mu.Unlock()
	defer m.pending.Done()
	go a.retire()
	published := false
	defer func() {
		close(a.ready)
		if !published {
			a.SignalClose()
			<-a.done
		}
	}()
	a.stopCaller = nzbRuntimeSignal(ctx, a.SignalClose)
	if err := m.Check(a.ctx); err != nil {
		return nil, err
	}
	lower, err := m.lower.OpenAttempt(a.ctx, start, length)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.lower = lower
	denied := a.closed || a.ctx.Err() != nil
	a.mu.Unlock()
	if denied {
		return nil, ErrNzbSystemUnavailable
	}
	if err = m.Check(a.ctx); err != nil {
		return nil, err
	}
	if err = lower.Check(a.ctx); err != nil {
		return nil, err
	}
	published = true
	return a, nil
}
func (a *OriginalNZBAttempt) valid() bool {
	return a != nil && a.originalNZBAttemptState != nil && a.self == a && a.metadata != nil && a.metadata.valid() && a.ctx != nil
}
func (a *OriginalNZBAttempt) Context() context.Context {
	if !a.valid() {
		return originalNZBCancelledContext()
	}
	return a.ctx
}
func (a *OriginalNZBAttempt) Check(ctx context.Context) error {
	if !a.valid() || ctx == nil || ctx.Err() != nil || a.ctx.Err() != nil {
		return ErrNzbSystemUnavailable
	}
	select {
	case <-a.ready:
	default:
		return ErrNzbSystemUnavailable
	}
	a.mu.Lock()
	denied := a.closed || a.lower == nil
	a.mu.Unlock()
	if denied {
		return ErrNzbSystemUnavailable
	}
	if err := a.metadata.Check(ctx); err != nil {
		return err
	}
	return a.lower.Check(ctx)
}
func (a *OriginalNZBAttempt) SignalClose() {
	if !a.valid() {
		return
	}
	a.mu.Lock()
	a.closed = true
	a.cancel()
	lower := a.lower
	a.mu.Unlock()
	if lower != nil {
		lower.SignalClose()
	}
}
func (a *OriginalNZBAttempt) retire() {
	<-a.ready
	if a.lower != nil {
		select {
		case <-a.ctx.Done():
		case <-a.lower.Context().Done():
		}
	} else {
		<-a.ctx.Done()
	}
	a.SignalClose()
	a.pending.Wait()
	if a.lower != nil {
		a.err = a.lower.CloseAndJoin()
	}
	if a.stopCaller != nil {
		a.stopCaller()
	}
	m := a.metadata
	m.mu.Lock()
	if m.attempt == a {
		m.attempt = nil
	}
	close(a.done)
	m.mu.Unlock()
}
func (a *OriginalNZBAttempt) CloseAndJoin() error {
	if !a.valid() {
		return ErrNzbSystemUnavailable
	}
	a.SignalClose()
	<-a.done
	return a.err
}
func (a *OriginalNZBAttempt) Close() error { return a.CloseAndJoin() }
func (a *OriginalNZBAttempt) CleanupDone() <-chan struct{} {
	if !a.valid() {
		return nil
	}
	return a.done
}
func (a *OriginalNZBAttempt) Read(p []byte) (n int, err error) {
	var pending bool
	defer func() {
		if err != nil && err != io.EOF {
			clear(p)
			n = 0
		}
		if pending {
			a.metadata.pending.Done()
			a.pending.Done()
		}
	}()
	if !a.valid() {
		return 0, ErrNzbSystemUnavailable
	}
	m := a.metadata
	m.mu.Lock()
	if m.closed || m.work.ctx.Err() != nil {
		m.mu.Unlock()
		return 0, ErrNzbSystemUnavailable
	}
	m.pending.Add(1)
	m.mu.Unlock()
	a.mu.Lock()
	if a.closed || a.ctx.Err() != nil || a.lower == nil {
		a.mu.Unlock()
		m.pending.Done()
		return 0, ErrNzbSystemUnavailable
	}
	a.pending.Add(1)
	a.mu.Unlock()
	pending = true
	if err := a.Check(a.ctx); err != nil {
		return 0, err
	}
	n, err = a.lower.Read(p)
	// A per-instance observer can hold only the return of genuine completed IO.
	// Upper pending work still owns the caller buffer through its postcheck.
	if a.afterRead != nil {
		a.afterRead()
	}
	if after := a.Check(a.ctx); after != nil {
		return 0, after
	}
	return n, err
}
func (OriginalNZBAttempt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalNZBAttempt(opaque)"))
}
func (OriginalNZBAttempt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalNZBAttempt) UnmarshalJSON([]byte) error  { return ErrNzbSystemUnavailable }
