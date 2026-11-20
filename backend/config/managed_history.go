package config

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrManagedConfigurationUnavailable = errors.New("managed configuration unavailable")
	ErrManagedConfigurationBusy        = errors.New("managed configuration busy")
)

const (
	managedLeaseLimit      = 256
	managedPathLeaseLimit  = 8
	managedDecodeLimit     = 8
	managedSnapshotLimit   = 4 << 20
	managedMaximumLifetime = 2 * time.Hour
)

// An epoch is never reused while an original lease retains it. The nonzero
// field also prevents the address ambiguity of zero-sized allocations.
type managedConfigEpoch struct{ live byte }

var managedConfigHistory = struct {
	sync.RWMutex
	epoch *managedConfigEpoch
}{epoch: &managedConfigEpoch{live: 1}}

var managedConfigAdmission = struct {
	sync.Mutex
	paths    map[string]int
	owners   map[*managedJellyfinState]struct{}
	leases   int
	decoding int
}{paths: make(map[string]int), owners: make(map[*managedJellyfinState]struct{})}

// Managed writes conservatively invalidate leases for every settings path.
// Writers only signal cancellation; they never wait for consumers or need a
// lease slot. The fence also serializes sibling Managers' existing .tmp writes.
func beginManagedConfigMutation() func() {
	managedConfigHistory.Lock()
	managedConfigHistory.epoch = &managedConfigEpoch{live: 1}
	managedConfigAdmission.Lock()
	for owner := range managedConfigAdmission.owners {
		owner.cancel()
	}
	managedConfigAdmission.Unlock()
	return managedConfigHistory.Unlock
}

// ReplaceManagedSettingsFile preserves the caller's staged file and destination
// semantics while placing the actual backup settings replacement under the same
// history fence as Manager.Save. Failed attempts also retire original leases.
func ReplaceManagedSettingsFile(staged, destination string) error {
	beginSettingsAttempt()
	reservation, err := BeginManagedReplacement(NewManager(destination))
	if err != nil {
		return err
	}
	defer reservation.Close()
	return reservation.Replace(staged, destination)
}

type managedSnapshotFile interface {
	check(context.Context) error
	close() error
}

type managedJellyfinCredentials struct {
	integrationID  string
	serverURL      string
	token          string
	userID         string
	ownerAccountID string
}

// ManagedJellyfinLease retains one original credential snapshot. It is only a
// managed-write lifetime, never account/PIN/ACL or upstream membership authority.
// Manual/external writes are unsupported: stat equality is not history proof.
// Credentials are available only to the fixed private scoped HTTP boundary.
type ManagedJellyfinLease struct{ state *managedJellyfinState }

type managedJellyfinState struct {
	mu          sync.Mutex
	self        *ManagedJellyfinLease
	ctx         context.Context
	cancel      context.CancelFunc
	deadline    time.Time
	epoch       *managedConfigEpoch
	attempt     *securityAttempt
	path        string
	file        managedSnapshotFile
	credentials managedJellyfinCredentials
	closed      bool
	bound       bool
	transport   *ManagedJellyfinTransport
	closeDone   chan struct{}
	closeErr    error
}

func (ManagedJellyfinLease) Format(w fmt.State, _ rune) {
	_, _ = w.Write([]byte("<opaque managed configuration>"))
}

func (ManagedJellyfinLease) MarshalJSON() ([]byte, error) {
	return []byte(`{"state":"opaque-managed-configuration"}`), nil
}

func (*ManagedJellyfinLease) UnmarshalJSON([]byte) error {
	return ErrManagedConfigurationUnavailable
}

func (lease *ManagedJellyfinLease) original() *managedJellyfinState {
	if lease == nil || lease.state == nil || lease.state.self != lease {
		return nil
	}
	return lease.state
}

// Context is cancellation only. A copied or imported capability has no context.
func (lease *ManagedJellyfinLease) Context() context.Context {
	if state := lease.original(); state != nil {
		return state.ctx
	}
	return nil
}

func (lease *ManagedJellyfinLease) Check(ctx context.Context) error {
	state := lease.original()
	if state == nil || ctx == nil || ctx.Err() != nil || !state.mu.TryLock() {
		return ErrManagedConfigurationUnavailable
	}
	defer state.mu.Unlock()
	if state.closed || state.ctx.Err() != nil || !state.deadline.After(time.Now()) || !managedConfigHistory.TryRLock() {
		return ErrManagedConfigurationUnavailable
	}
	defer managedConfigHistory.RUnlock()
	if !settingsAttemptCurrent(state.attempt) || state.epoch != managedConfigHistory.epoch || state.file == nil || state.file.check(ctx) != nil || state.ctx.Err() != nil || !state.deadline.After(time.Now()) || ctx.Err() != nil {
		return ErrManagedConfigurationUnavailable
	}
	return nil
}

// Close joins the owned scoped transport before releasing snapshot descriptors.
// Cancellation alone never refunds a slot.
func (lease *ManagedJellyfinLease) Close() error {
	state := lease.original()
	if state == nil {
		return ErrManagedConfigurationUnavailable
	}
	state.mu.Lock()
	if state.closed {
		done := state.closeDone
		state.mu.Unlock()
		<-done
		return state.closeErr
	}
	state.closed = true
	state.cancel()
	transport, file := state.transport, state.file
	state.transport, state.file = nil, nil
	state.mu.Unlock()
	// No HTTP completion may wait under the lease/history/admission locks.
	// Transport and body cleanup never call back into this parent Close.
	var err error
	if transport != nil {
		err = transport.Close()
	}
	if file != nil {
		if fileErr := file.close(); fileErr != nil {
			err = fileErr
		}
	}
	state.mu.Lock()
	state.credentials = managedJellyfinCredentials{}
	managedConfigAdmission.Lock()
	delete(managedConfigAdmission.owners, state)
	managedConfigAdmission.leases--
	managedConfigAdmission.paths[state.path]--
	if managedConfigAdmission.paths[state.path] == 0 {
		delete(managedConfigAdmission.paths, state.path)
	}
	managedConfigAdmission.Unlock()
	if err != nil {
		state.closeErr = ErrManagedConfigurationUnavailable
	}
	close(state.closeDone)
	state.mu.Unlock()
	return state.closeErr
}

func captureManagedJellyfin(ctx context.Context, manager *Manager, integrationID string, read func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error)) (*ManagedJellyfinLease, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || manager.path == "" || len(manager.path) > 4096 || len(integrationID) == 0 || len(integrationID) > 128 || strings.TrimSpace(integrationID) != integrationID {
		return nil, ErrManagedConfigurationUnavailable
	}
	rawParts := strings.Split(manager.path, string(filepath.Separator))
	if last := rawParts[len(rawParts)-1]; last == "" || last == "." {
		return nil, ErrManagedConfigurationUnavailable
	}
	// Do not clean away traversal that could have a different kernel meaning
	// through a symlink. Legacy writers retain their original path semantics.
	for _, part := range rawParts {
		if part == ".." {
			return nil, ErrManagedConfigurationUnavailable
		}
	}
	deadline, ok := ctx.Deadline()
	now := time.Now()
	if !ok || !deadline.After(now) {
		return nil, ErrManagedConfigurationUnavailable
	}
	if maximum := now.Add(managedMaximumLifetime); deadline.After(maximum) {
		deadline = maximum
	}
	path, err := filepath.Abs(manager.path)
	if err != nil || len(path) > 4096 {
		return nil, ErrManagedConfigurationUnavailable
	}
	path = filepath.Clean(path)
	attempt := settingsAttemptNow()
	if !managedConfigHistory.TryRLock() {
		return nil, ErrManagedConfigurationBusy
	}
	defer managedConfigHistory.RUnlock()
	managedConfigAdmission.Lock()
	if managedConfigAdmission.leases >= managedLeaseLimit || managedConfigAdmission.paths[path] >= managedPathLeaseLimit || managedConfigAdmission.decoding >= managedDecodeLimit {
		managedConfigAdmission.Unlock()
		return nil, ErrManagedConfigurationBusy
	}
	managedConfigAdmission.leases++
	managedConfigAdmission.paths[path]++
	managedConfigAdmission.decoding++
	managedConfigAdmission.Unlock()
	reserved := true
	defer func() {
		managedConfigAdmission.Lock()
		managedConfigAdmission.decoding--
		if reserved {
			managedConfigAdmission.leases--
			managedConfigAdmission.paths[path]--
			if managedConfigAdmission.paths[path] == 0 {
				delete(managedConfigAdmission.paths, path)
			}
		}
		managedConfigAdmission.Unlock()
	}()
	file, credentials, err := read(ctx, path, integrationID)
	if err != nil || file == nil {
		if file != nil {
			_ = file.close()
		}
		return nil, ErrManagedConfigurationUnavailable
	}
	if ctx.Err() != nil || !deadline.After(time.Now()) || !settingsAttemptCurrent(attempt) || file.check(ctx) != nil {
		_ = file.close()
		return nil, ErrManagedConfigurationUnavailable
	}
	lifetime, cancel := context.WithDeadline(ctx, deadline)
	if lifetime.Err() != nil || !deadline.After(time.Now()) {
		cancel()
		_ = file.close()
		return nil, ErrManagedConfigurationUnavailable
	}
	lease := &ManagedJellyfinLease{}
	state := &managedJellyfinState{self: lease, ctx: lifetime, cancel: cancel, deadline: deadline, epoch: managedConfigHistory.epoch, attempt: attempt, path: path, file: file, credentials: credentials, closeDone: make(chan struct{})}
	lease.state = state
	managedConfigAdmission.Lock()
	if !settingsAttemptCurrent(attempt) {
		managedConfigAdmission.Unlock()
		cancel()
		_ = file.close()
		return nil, ErrManagedConfigurationUnavailable
	}
	managedConfigAdmission.owners[state] = struct{}{}
	managedConfigAdmission.Unlock()
	reserved = false
	return lease, nil
}
