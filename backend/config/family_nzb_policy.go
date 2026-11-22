package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"novastream/internal/settingscommit"
)

var errFamilyNZBRootChanged = errors.New("original NZB root changed")

// FamilyNZBRoot owns the actual startup directory, not a media permission.
type FamilyNZBRoot struct{ *familyNZBRootState }
type familyNZBRootState struct {
	self                                 *FamilyNZBRoot
	manager                              *Manager
	managerPath, cwdName, nativeAbsolute string
	cwd                                  *os.File
	cwdInfo                              os.FileInfo
	ctx                                  context.Context
	cancel                               context.CancelFunc
	mu                                   sync.Mutex
	closed, preparing, prepared, failed  bool
	cacheDirectory, metadataRoot         string
	operations                           sync.WaitGroup
	once                                 sync.Once
	done                                 chan struct{}
	err                                  error
}

func (FamilyNZBRoot) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("FamilyNZBRoot(opaque)")) }
func (FamilyNZBRoot) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBRoot) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (r *FamilyNZBRoot) valid() bool {
	return r != nil && r.familyNZBRootState != nil && r.self == r && r.manager != nil && r.ctx != nil
}
func (r *FamilyNZBRoot) enter() bool {
	if !r.valid() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return false
	}
	r.operations.Add(1)
	return true
}
func (r *FamilyNZBRoot) current(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || !r.enter() {
		return ErrSecurityCoordinator
	}
	defer r.operations.Done()
	if r.manager.path != r.managerPath {
		return ErrSecurityCoordinator
	}
	err := checkFamilyNZBDirectory(ctx, r)
	return r.observeDirectoryResult(err)
}
func (r *FamilyNZBRoot) observeDirectoryResult(err error) error {
	if errors.Is(err, errFamilyNZBRootChanged) {
		r.fail()
	}
	return err
}
func (r *FamilyNZBRoot) fail() { r.mu.Lock(); r.failed = true; r.cancel(); r.mu.Unlock() }
func (r *FamilyNZBRoot) observeCache(cache string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prepared && cache != r.cacheDirectory {
		r.failed = true
		r.cancel()
		return errFamilyNZBRootChanged
	}
	return nil
}
func (r *FamilyNZBRoot) Close() error {
	if !r.valid() {
		return ErrSecurityCoordinator
	}
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.operations.Wait()
		if r.cwd != nil {
			r.err = r.cwd.Close()
		}
		close(r.done)
	})
	<-r.done
	return r.err
}

type FamilyNZBPolicyValues struct {
	MetadataRoot       string
	EnforceProfilePins bool
}
type familyNZBPolicyRead struct {
	cacheDirectory, databaseURL string
	enforce                     bool
	version                     bridgeLoginPolicyVersion
	digest                      [32]byte
}
type FamilyNZBPolicyObservation struct {
	self    *FamilyNZBPolicyObservation
	owner   *ManagedSecurity
	root    *FamilyNZBRoot
	ctx     context.Context
	attempt *securityAttempt
	read    familyNZBPolicyRead
}
type FamilyNZBPolicyFence struct {
	self     *FamilyNZBPolicyFence
	original *FamilyNZBPolicyObservation
	ctx      context.Context
	hold     *settingscommit.ProgressFileHold
	values   FamilyNZBPolicyValues
}

func (FamilyNZBPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyNZBPolicyObservation(opaque)"))
}
func (FamilyNZBPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBPolicyObservation) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (FamilyNZBPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyNZBPolicyFence(opaque)"))
}
func (FamilyNZBPolicyFence) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBPolicyFence) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }

func (s *ManagedSecurity) familyNZBCurrent(ctx context.Context, r *FamilyNZBRoot) bool {
	return ctx != nil && ctx.Err() == nil && r.valid() && r.manager == s.manager && r.current(ctx) == nil && s.bridgePolicyCurrent(ctx)
}
func (s *ManagedSecurity) familyNZBDomain(read familyNZBPolicyRead) bool {
	url := read.databaseURL
	if s.override != "" {
		url = s.override
	}
	return url != "" && s.coordinator.MatchesSourceURL(url)
}

// PrepareFamilyNZBRoot is called under the original Startup mutation owner.
// Only its descriptive absolute result is passed to actual NS preparation.
func (s *ManagedSecurity) PrepareFamilyNZBRoot(ctx context.Context, r *FamilyNZBRoot) (string, error) {
	if !r.valid() || ctx == nil || ctx.Err() != nil || !s.enter() {
		return "", ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !r.enter() {
		return "", ErrSecurityCoordinator
	}
	defer r.operations.Done()
	r.mu.Lock()
	if r.prepared && !r.failed && !r.closed {
		value := r.metadataRoot
		cache := r.cacheDirectory
		r.mu.Unlock()
		if !s.familyNZBCurrent(ctx, r) {
			return "", ErrSecurityCoordinator
		}
		read, err := readOriginalFamilyNZBPolicy(ctx, r)
		if err != nil {
			return "", ErrSecurityCoordinator
		}
		if !s.familyNZBDomain(read) {
			r.fail()
			return "", ErrSecurityCoordinator
		}
		if read.cacheDirectory != cache || !s.familyNZBCurrent(ctx, r) {
			return "", ErrSecurityCoordinator
		}
		return value, nil
	}
	if r.preparing || r.failed || r.closed {
		r.mu.Unlock()
		return "", ErrSecurityCoordinator
	}
	r.preparing = true
	r.mu.Unlock()
	read, err := readOriginalFamilyNZBPolicy(ctx, r)
	if err == nil && (!s.familyNZBCurrent(ctx, r) || !s.familyNZBDomain(read)) {
		err = ErrSecurityCoordinator
	}
	absolute := ""
	if err == nil {
		absolute, err = familyNZBMetadataRoot(r.cwdName, read.cacheDirectory)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preparing = false
	if err != nil || r.closed || r.ctx.Err() != nil || ctx.Err() != nil {
		r.failed = true
		return "", ErrSecurityCoordinator
	}
	r.cacheDirectory = read.cacheDirectory
	r.metadataRoot = absolute
	r.prepared = true
	return absolute, nil
}

func (s *ManagedSecurity) CaptureFamilyNZBPolicy(ctx context.Context, r *FamilyNZBRoot) (*FamilyNZBPolicyObservation, error) {
	if ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !s.familyNZBCurrent(ctx, r) {
		return nil, ErrSecurityCoordinator
	}
	r.mu.Lock()
	ready := r.prepared && !r.failed && !r.closed
	rootCache := r.cacheDirectory
	r.mu.Unlock()
	if !ready {
		return nil, ErrSecurityCoordinator
	}
	attempt := settingsAttemptNow()
	read, err := readOriginalFamilyNZBPolicy(ctx, r)
	if err != nil {
		return nil, ErrSecurityCoordinator
	}
	if !s.familyNZBDomain(read) {
		r.fail()
		return nil, ErrSecurityCoordinator
	}
	if read.cacheDirectory != rootCache || !settingsAttemptCurrent(attempt) || !s.familyNZBCurrent(ctx, r) {
		return nil, ErrSecurityCoordinator
	}
	o := &FamilyNZBPolicyObservation{owner: s, root: r, ctx: ctx, attempt: attempt, read: read}
	o.self = o
	return o, nil
}
func (s *ManagedSecurity) BindFamilyNZBPolicy(ctx context.Context, o *FamilyNZBPolicyObservation, h *settingscommit.ProgressFileHold) (*FamilyNZBPolicyFence, error) {
	if o == nil || o.self != o || o.owner != s || ctx == nil || ctx.Err() != nil || o.ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !settingsAttemptCurrent(o.attempt) || !s.familyNZBCurrent(ctx, o.root) || h.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	read, err := readOriginalFamilyNZBPolicy(ctx, o.root)
	if err != nil {
		return nil, ErrSecurityCoordinator
	}
	if !s.familyNZBDomain(read) {
		o.root.fail()
		return nil, ErrSecurityCoordinator
	}
	if read != o.read || !settingsAttemptCurrent(o.attempt) || o.ctx.Err() != nil || !s.familyNZBCurrent(ctx, o.root) || h.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	o.root.mu.Lock()
	values := FamilyNZBPolicyValues{o.root.metadataRoot, read.enforce}
	o.root.mu.Unlock()
	f := &FamilyNZBPolicyFence{original: o, ctx: ctx, hold: h, values: values}
	f.self = f
	return f, nil
}
func (f *FamilyNZBPolicyFence) Values() (FamilyNZBPolicyValues, error) {
	if f == nil || f.self != f || f.original == nil || f.ctx.Err() != nil || f.original.ctx.Err() != nil || !f.original.owner.enter() {
		return FamilyNZBPolicyValues{}, ErrSecurityCoordinator
	}
	defer f.original.owner.operations.Done()
	o := f.original
	if !settingsAttemptCurrent(o.attempt) || !o.owner.familyNZBCurrent(f.ctx, o.root) || f.hold.Current(o.owner.coordinator) != nil {
		return FamilyNZBPolicyValues{}, ErrSecurityCoordinator
	}
	return f.values, nil
}
