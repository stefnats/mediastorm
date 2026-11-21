package integration

import (
	"context"
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
	m.mu.Unlock()
	m.lower.SignalClose()
	m.pending.Wait()
	m.err = m.lower.CloseAndJoin()
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
