package datastore

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/integration"
	"novastream/internal/settingscommit"
)

// FamilyNZBSources is one original purpose composition. It cannot authenticate
// a request or convert namespace/path DATA into a full media owner.
type FamilyNZBSources struct{ *familyNZBSourcesState }
type familyNZBSourcesState struct {
	self         *FamilyNZBSources
	store        *DataStore
	ns           *integration.NzbSystem
	manager      *config.Manager
	security     *config.ManagedSecurity
	coordinator  *settingscommit.Coordinator
	root         *config.FamilyNZBRoot
	namespace    *integration.OriginalNZBNamespace
	metadataRoot string
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	closed       bool
	work         sync.WaitGroup
	ready, done  chan struct{}
	setupErr     error
	once         sync.Once
}

func (FamilyNZBSources) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyNZBSources(opaque)"))
}
func (FamilyNZBSources) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBSources) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (r *FamilyNZBSources) valid() bool {
	return r != nil && r.familyNZBSourcesState != nil && r.self == r && r.store != nil && r.ctx != nil
}
func (r *FamilyNZBSources) MatchesOriginal(store *DataStore, ns *integration.NzbSystem) bool {
	if !r.valid() || r.store != store || r.ns != ns {
		return false
	}
	select {
	case <-r.ready:
	default:
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.closed && r.ctx.Err() == nil && r.setupErr == nil
}
func (r *FamilyNZBSources) ReadyForStore(store *DataStore) bool {
	return r.valid() && r.MatchesOriginal(store, r.ns)
}

func NewFamilyNZBSources(original context.Context, ds *DataStore, ns *integration.NzbSystem, manager *config.Manager, security *config.ManagedSecurity, c *settingscommit.Coordinator, root *config.FamilyNZBRoot) (out *FamilyNZBSources, err error) {
	if original == nil || original.Err() != nil || ds == nil || ns == nil || root == nil || !ds.SettingsCommitReady(c) || !security.Ready(manager, c) {
		return nil, ErrFamilyMediaUnavailable
	}
	familyMediaAdmission.Lock()
	if ds.familyMediaRetired || ds.familyNZBClosed {
		familyMediaAdmission.Unlock()
		return nil, ErrFamilyMediaUnavailable
	}
	if old := ds.familyNZBSources; old != nil {
		familyMediaAdmission.Unlock()
		if old.manager != manager || old.security != security || old.coordinator != c || old.root != root || !old.MatchesOriginal(ds, ns) {
			return nil, ErrFamilyMediaChanged
		}
		return old, nil
	}
	life, cancel := context.WithCancel(original)
	r := &FamilyNZBSources{&familyNZBSourcesState{store: ds, ns: ns, manager: manager, security: security, coordinator: c, root: root, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	r.self = r
	ds.familyNZBSources = r
	familyMediaAdmission.Unlock()
	defer func() {
		r.setupErr = err
		close(r.ready)
		if out == nil {
			r.Close()
		}
	}()
	setup, finish := context.WithTimeout(life, 10*time.Second)
	stopSetup := familyMediaAfterFunc(setup, cancel)
	defer func() { stopSetup(); finish() }()
	// Native holds cover only this bounded policy check, never provider work.
	policy, err := security.CaptureFamilyNZBPolicy(life, root)
	if err != nil {
		return nil, err
	}
	r.namespace, err = ns.CaptureOriginalNamespace(setup)
	if err != nil {
		return nil, err
	}
	values, err := r.current(setup, policy)
	if err != nil {
		return nil, err
	}
	r.metadataRoot = values.MetadataRoot
	if life.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	return r, nil
}

// A source passes its own immutable original observation. Registration keeps
// the original namespace/root, never a policy shared across future owners.
func (r *FamilyNZBSources) current(ctx context.Context, policy *config.FamilyNZBPolicyObservation) (values config.FamilyNZBPolicyValues, err error) {
	if !r.valid() || ctx == nil || ctx.Err() != nil || r.ctx.Err() != nil || policy == nil || r.namespace == nil || !r.store.SettingsCommitReady(r.coordinator) || !r.security.Ready(r.manager, r.coordinator) {
		return values, ErrFamilyMediaChanged
	}
	hold, err := r.coordinator.TryProgressFileHold(ctx)
	if err != nil {
		return values, err
	}
	defer func() {
		if closeErr := hold.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	fence, err := r.security.BindFamilyNZBPolicy(ctx, policy, hold)
	if err != nil {
		return values, err
	}
	values, err = fence.Values()
	if err != nil {
		return values, err
	}
	if r.metadataRoot != "" && values.MetadataRoot != r.metadataRoot {
		return values, ErrFamilyMediaChanged
	}
	if err = r.namespace.MatchesOriginal(ctx, r.ns, values.MetadataRoot); err != nil {
		return values, err
	}
	if r.ctx.Err() != nil || ctx.Err() != nil {
		return values, ErrFamilyMediaChanged
	}
	return values, nil
}
func (r *FamilyNZBSources) Close() {
	if !r.valid() {
		return
	}
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		<-r.ready
		r.work.Wait()
		close(r.done)
	})
	<-r.done
}
func (ds *DataStore) retireFamilyNZB() {
	familyMediaAdmission.Lock()
	ds.familyNZBClosed = true
	original := ds.familyNZBSources
	familyMediaAdmission.Unlock()
	if original != nil {
		original.Close()
	}
}

type FamilyNZBSourceInfo struct{ Size int64 }
type FamilyNZBSource struct{ *familyNZBSourceState }
type familyNZBSourceState struct {
	self                    *FamilyNZBSource
	owner                   *FamilyMediaOwner
	issuer                  *FamilyNZBSources
	policy                  *config.FamilyNZBPolicyObservation
	values                  config.FamilyNZBPolicyValues
	lower                   *integration.OriginalNZBMetadata
	ctx                     context.Context
	cancel                  context.CancelFunc
	stopOwner               func()
	mu                      sync.Mutex
	closed, opened, charged bool
	operations              sync.WaitGroup
	ready, done             chan struct{}
	err                     error
}
type FamilyNZBStream struct{ *familyNZBStreamState }
type familyNZBStreamState struct {
	self   *FamilyNZBStream
	source *FamilyNZBSource
	lower  *integration.OriginalNZBStream
}

func (FamilyNZBSource) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("FamilyNZBSource(opaque)")) }
func (FamilyNZBSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyNZBStream) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("FamilyNZBStream(opaque)")) }
func (FamilyNZBStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBStream) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (s *FamilyNZBSource) valid() bool {
	return s != nil && s.familyNZBSourceState != nil && s.self == s && s.owner.valid() && s.ctx != nil
}
func (s *FamilyNZBSource) Context() context.Context {
	if !s.valid() {
		return familyMediaClosedContext()
	}
	return s.ctx
}
func (s *FamilyNZBSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	deadline, _ := s.ctx.Deadline()
	return deadline
}
func (s *FamilyNZBSource) ReadyForStore(ds *DataStore) bool {
	return s.valid() && s.owner.store == ds && s.issuer != nil && s.issuer.ReadyForStore(ds)
}
func (s *FamilyNZBSource) enter() bool {
	if !s.valid() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil {
		return false
	}
	s.operations.Add(1)
	return true
}
func (o *FamilyMediaOwner) CaptureNZBSource(ctx context.Context, original *FamilyNZBSources, name string) (out *FamilyNZBSource, err error) {
	if !o.valid() || ctx == nil || ctx.Err() != nil || name == "" || len(name) > 4096 {
		return nil, ErrFamilyMediaChanged
	}
	o.mu.Lock()
	if !o.captured || o.closed || o.ctx.Err() != nil || o.source != nil {
		o.mu.Unlock()
		return nil, ErrFamilyMediaBusy
	}
	// The original existing cross-provider slot is permanent, including failure.
	parent := o.ctx
	if original.valid() {
		parent = original.ctx
	}
	expiry := o.snapshot.expires
	if maximum := time.Now().Add(24 * time.Hour); maximum.Before(expiry) {
		expiry = maximum
	}
	life, cancel := context.WithDeadline(parent, expiry)
	s := &FamilyNZBSource{&familyNZBSourceState{owner: o, issuer: original, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	s.self = s
	o.source = s
	o.mu.Unlock()
	s.stopOwner = familyMediaAfterFunc(o.ctx, cancel)
	go s.retire()
	defer func() {
		close(s.ready)
		if out == nil {
			s.Close()
		}
	}()
	if !original.ReadyForStore(o.store) {
		return nil, ErrFamilyMediaChanged
	}
	original.mu.Lock()
	if original.closed || original.ctx.Err() != nil {
		original.mu.Unlock()
		return nil, ErrFamilyMediaChanged
	}
	original.work.Add(1)
	s.charged = true
	original.mu.Unlock()
	// Temporary setup can cancel actual work, but its intentional final cancel
	// never shortens the successful original source lifetime.
	work, finish := context.WithTimeout(ctx, 30*time.Second)
	stopSource := familyMediaAfterFunc(s.ctx, finish)
	stopSetup := familyMediaAfterFunc(work, cancel)
	defer func() { stopSetup(); stopSource(); finish() }()
	if err = o.Check(work); err != nil {
		return nil, err
	}
	s.policy, err = original.security.CaptureFamilyNZBPolicy(s.ctx, original.root)
	if err != nil {
		return nil, err
	}
	s.values, err = original.current(work, s.policy)
	if err != nil {
		return nil, err
	}
	if err = s.checkOwner(work); err != nil {
		return nil, err
	}
	s.lower, err = original.namespace.CaptureMetadata(s.ctx, name)
	if err != nil {
		return nil, err
	}
	if err = s.Check(work); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *FamilyNZBSource) checkOwner(ctx context.Context) error {
	if s.owner.Check(ctx) != nil {
		return ErrFamilyMediaChanged
	}
	o := s.owner
	if o.Mode() != FamilyMediaInactive && o.Mode() != FamilyMediaActiveAdult {
		return ErrFamilyMediaChanged
	}
	if o.target != "" && o.snapshot.profile.AccountID != o.original.AccountID {
		return ErrFamilyMediaChanged
	}
	if o.target != "" && s.values.EnforceProfilePins && o.snapshot.outcome != ProfilePinNoPin && o.snapshot.outcome != ProfilePinUnlocked {
		return ErrFamilyMediaPINRequired
	}
	return nil
}
func (s *FamilyNZBSource) Check(ctx context.Context) (err error) {
	if ctx == nil || ctx.Err() != nil || !s.enter() {
		return ErrFamilyMediaChanged
	}
	defer s.operations.Done()
	work, cancel := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer cancel()
	stop := familyMediaAfterFunc(s.ctx, cancel)
	defer stop()
	defer func() {
		if err != nil && ctx.Err() == nil {
			s.cancel()
		}
	}()
	if err = s.checkOwner(work); err != nil {
		return err
	}
	values, err := s.issuer.current(work, s.policy)
	if err != nil || values != s.values {
		return ErrFamilyMediaChanged
	}
	if s.lower == nil || s.lower.Check(work) != nil || s.checkOwner(work) != nil || work.Err() != nil || s.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	return nil
}
func (s *FamilyNZBSource) Info(ctx context.Context) (FamilyNZBSourceInfo, error) {
	if ctx == nil || ctx.Err() != nil || !s.enter() {
		return FamilyNZBSourceInfo{}, ErrFamilyMediaChanged
	}
	defer s.operations.Done()
	if s.Check(ctx) != nil {
		return FamilyNZBSourceInfo{}, ErrFamilyMediaChanged
	}
	info, err := s.lower.Info(ctx)
	if err != nil || s.Check(ctx) != nil {
		return FamilyNZBSourceInfo{}, ErrFamilyMediaChanged
	}
	return FamilyNZBSourceInfo{Size: info.Size}, nil
}
func (s *FamilyNZBSource) Open(ctx context.Context, start, length int64) (out *FamilyNZBStream, err error) {
	if ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrFamilyMediaChanged
	}
	defer s.operations.Done()
	s.mu.Lock()
	if s.opened {
		s.mu.Unlock()
		return nil, ErrFamilyMediaChanged
	}
	s.opened = true
	s.mu.Unlock()
	defer func() {
		if out == nil {
			s.cancel()
		}
	}()
	if s.Check(ctx) != nil {
		return nil, ErrFamilyMediaChanged
	}
	lower, err := s.lower.Open(ctx, start, length)
	if err != nil {
		s.cancel()
		return nil, err
	}
	r := &FamilyNZBStream{&familyNZBStreamState{source: s, lower: lower}}
	r.self = r
	if s.Check(ctx) != nil {
		s.cancel()
		return nil, ErrFamilyMediaChanged
	}
	return r, nil
}
func (s *FamilyNZBSource) retire() {
	<-s.ready
	if s.lower == nil {
		<-s.ctx.Done()
	} else {
		select {
		case <-s.ctx.Done():
		case <-s.lower.Context().Done():
		}
	}
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	// Source context has signalled the original N1/F2 tree before any join.
	s.operations.Wait()
	if s.lower != nil {
		s.err = s.lower.Close()
	}
	if s.stopOwner != nil {
		s.stopOwner()
	}
	close(s.done)
	if s.charged {
		s.issuer.work.Done()
	}
}
func (s *FamilyNZBSource) Close() {
	if !s.valid() {
		return
	}
	s.cancel()
	<-s.done
}
func (r *FamilyNZBStream) valid() bool {
	return r != nil && r.familyNZBStreamState != nil && r.self == r && r.source.valid() && r.lower != nil
}
func (r *FamilyNZBStream) Context() context.Context {
	if !r.valid() {
		return familyMediaClosedContext()
	}
	return r.lower.Context()
}
func (r *FamilyNZBStream) Check(ctx context.Context) error {
	if !r.valid() {
		return ErrFamilyMediaChanged
	}
	return r.source.Check(ctx)
}
func (r *FamilyNZBStream) Read(p []byte) (n int, err error) {
	var pending *FamilyNZBSource
	defer func() {
		if err != nil && err != io.EOF {
			clear(p)
			n = 0
		}
		if pending != nil {
			pending.operations.Done()
		}
	}()
	if !r.valid() || !r.source.enter() {
		return 0, ErrFamilyMediaChanged
	}
	pending = r.source
	if err = r.Check(r.Context()); err != nil {
		return 0, err
	}
	n, err = r.lower.Read(p)
	if after := r.Check(r.Context()); after != nil {
		return 0, after
	}
	return n, err
}
func (r *FamilyNZBStream) Close() error {
	if !r.valid() {
		return ErrFamilyMediaChanged
	}
	r.source.Close()
	return r.source.err
}
func (r *FamilyNZBStream) CleanupDone() <-chan struct{} {
	if !r.valid() {
		return nil
	}
	return r.source.done
}
