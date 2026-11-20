package remotemedia

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
)

const (
	familyJellyfinSetupBudget = 25 * time.Second
	familyJellyfinPoll        = 2 * time.Second
	familyJellyfinFreshness   = 5 * time.Second
)

type familyJellyfinWorkerKey struct {
	manager     *config.Manager
	integration string
}

var familyJellyfinWorkers = struct {
	sync.Mutex
	active int
	keys   map[familyJellyfinWorkerKey]int
}{keys: make(map[familyJellyfinWorkerKey]int)}

// This concrete adapter accepts only a genuine original PG source. Its HTTP
// metadata is not a substitute for that source's current actor/PIN/library ACL.
type FamilyJellyfinSources struct{ state *familyJellyfinSourcesState }
type familyJellyfinSourcesState struct {
	self    *FamilyJellyfinSources
	store   *datastore.DataStore
	manager *config.Manager
	remote  *Service
}

func (FamilyJellyfinSources) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinSources(opaque)"))
}
func (FamilyJellyfinSources) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyJellyfinSources) UnmarshalJSON([]byte) error {
	return profileunlock.ErrFamilyMediaUnavailable
}

func NewFamilyJellyfinSources(store *datastore.DataStore, manager *config.Manager, remote *Service) (*FamilyJellyfinSources, error) {
	if store == nil || store.Pool() == nil || manager == nil || remote == nil || remote.cfg != manager || !store.FamilyRemoteRepositoryReady(remote.repo) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	sources := &FamilyJellyfinSources{}
	sources.state = &familyJellyfinSourcesState{self: sources, store: store, manager: manager, remote: remote}
	return sources, nil
}
func (sources *FamilyJellyfinSources) ReadyFor(store *datastore.DataStore, manager *config.Manager) bool {
	if sources == nil || sources.state == nil || sources.state.self != sources {
		return false
	}
	s := sources.state
	return store != nil && store == s.store && manager != nil && manager == s.manager && s.remote != nil && s.remote.cfg == manager && store.FamilyRemoteRepositoryReady(s.remote.repo)
}

type FamilyJellyfinStreamInfo struct {
	FileName, Container  string
	Length, RunTimeTicks int64
	ReportedCodecs       []string
}

func (FamilyJellyfinStreamInfo) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinStreamInfo(metadata)"))
}
func (FamilyJellyfinStreamInfo) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

type FamilyJellyfinStream struct{ state *familyJellyfinStreamState }
type familyJellyfinStreamState struct {
	mu         sync.Mutex
	self       *FamilyJellyfinStream
	sources    *familyJellyfinSourcesState
	source     *profileunlock.FamilyJellyfinSource
	info       FamilyJellyfinStreamInfo
	ctx        context.Context
	cancel     context.CancelFunc
	exposed    context.Context
	endExposed context.CancelFunc
	deadline   time.Time
	lease      *config.ManagedJellyfinLease
	transport  *config.ManagedJellyfinTransport
	download   *config.ManagedJellyfinDownload
	joinConfig func() bool
	checks     chan struct{}
	readGate   sync.Mutex
	ops        sync.WaitGroup
	setupDone  chan struct{}
	done       chan struct{}
	watchDone  <-chan struct{}
	started    bool
	keyBound   bool
	key        familyJellyfinWorkerKey
	ready      bool
	closed     bool
	closeErr   error
	receipt    familyJellyfinReceipt
	observedAt time.Time
	remaining  int64
}

func (FamilyJellyfinStream) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinStream(opaque)"))
}
func (FamilyJellyfinStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyJellyfinStream) UnmarshalJSON([]byte) error  { return profileunlock.ErrFamilyMediaChanged }
func (stream *FamilyJellyfinStream) original() *familyJellyfinStreamState {
	if stream == nil || stream.state == nil || stream.state.self != stream {
		return nil
	}
	return stream.state
}
func (stream *FamilyJellyfinStream) Context() context.Context {
	if state := stream.original(); state != nil {
		return state.exposed
	}
	return nil
}
func (stream *FamilyJellyfinStream) ExpiresAt() time.Time {
	if state := stream.original(); state != nil {
		return state.deadline
	}
	return time.Time{}
}
func (stream *FamilyJellyfinStream) Info() FamilyJellyfinStreamInfo {
	if state := stream.original(); state != nil {
		info := state.info
		info.ReportedCodecs = append([]string(nil), info.ReportedCodecs...)
		return info
	}
	return FamilyJellyfinStreamInfo{}
}

func familyJellyfinLive(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	deadline, bounded := ctx.Deadline()
	return !bounded || deadline.After(time.Now())
}

// A callback signals cancellation only. Its external join accounts true-stop
// immediately, and never calls Close on a parent or the callback's active work.
func familyJellyfinAfterFunc(ctx context.Context, callback func()) func() bool {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	var once sync.Once
	prevented := false
	return func() bool {
		once.Do(func() {
			prevented = stop()
			if prevented {
				close(done)
			}
		})
		<-done
		return prevented
	}
}

func (sources *FamilyJellyfinSources) Open(caller context.Context, source *profileunlock.FamilyJellyfinSource) (stream *FamilyJellyfinStream, err error) {
	if sources == nil || sources.state == nil || !sources.ReadyFor(sources.state.store, sources.state.manager) || source == nil || !source.ReadyForStore(sources.state.store) || !familyJellyfinLive(caller) || !familyJellyfinLive(source.Context()) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	life, cancel := context.WithCancel(source.Context())
	state := &familyJellyfinStreamState{sources: sources.state, source: source, ctx: life, cancel: cancel, checks: make(chan struct{}, 1), setupDone: make(chan struct{}), done: make(chan struct{})}
	deadline := time.Now().Add(familyJellyfinSetupBudget)
	if expiry := source.ExpiresAt(); expiry.Before(deadline) {
		deadline = expiry
	}
	if expiry, ok := caller.Deadline(); ok && expiry.Before(deadline) {
		deadline = expiry
	}
	setup, cancelSetup := context.WithDeadline(life, deadline)
	joinCaller := familyJellyfinAfterFunc(caller, cancelSetup)
	// Installed before BeginOpen's preliminary original PG queries.
	joinSource := familyJellyfinAfterFunc(source.Context(), cancelSetup)
	stopSetup := familyJellyfinAfterFunc(setup, cancel)
	var finishOpen func()
	defer func() {
		if err != nil {
			state.cancel()
		}
		stopSetup()
		cancelSetup()
		joinCaller()
		joinSource()
		// Open has no active-op counter. End setup before waiting actualDone.
		close(state.setupDone)
		if err != nil && state.started {
			<-state.done
		}
		if !state.started {
			state.cancel()
		}
		if finishOpen != nil {
			finishOpen() // A failed acquisition cannot refund PG before cleanup.
		}
	}()
	finishOpen, err = source.BeginOpen(setup)
	if err != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	familyJellyfinWorkers.Lock()
	if familyJellyfinWorkers.active >= 8 {
		familyJellyfinWorkers.Unlock()
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	familyJellyfinWorkers.active++
	familyJellyfinWorkers.Unlock()
	state.started = true
	go state.cleanup()
	if source.BindRetirement(state.cancel, state.done) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	info, err := source.Info(setup)
	if err != nil || !familyJellyfinLive(setup) || !familyJellyfinLive(source.Context()) {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	state.key = familyJellyfinWorkerKey{sources.state.manager, info.IntegrationID}
	familyJellyfinWorkers.Lock()
	if familyJellyfinWorkers.keys[state.key] >= 2 {
		familyJellyfinWorkers.Unlock()
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	familyJellyfinWorkers.keys[state.key]++
	state.keyBound = true
	familyJellyfinWorkers.Unlock()
	state.lease, err = sources.state.manager.CaptureManagedJellyfin(life, info.IntegrationID)
	if err != nil || !familyJellyfinLive(setup) || !familyJellyfinLive(caller) {
		return nil, state.failure(caller)
	}
	state.joinConfig = familyJellyfinAfterFunc(state.lease.Context(), state.cancel)
	state.deadline = source.ExpiresAt()
	if expiry, ok := state.lease.Context().Deadline(); !ok {
		return nil, profileunlock.ErrFamilyMediaChanged
	} else if expiry.Before(state.deadline) {
		state.deadline = expiry
	}
	state.exposed, state.endExposed = context.WithDeadline(life, state.deadline)
	state.transport, err = state.lease.BindJellyfinScope(config.ManagedJellyfinScope{PrimaryGUID: info.PrimaryGUID, BackingGUID: info.BackingGUID, LibraryGUID: info.ExternalLibraryGUID})
	if err != nil {
		return nil, state.failure(caller)
	}
	if err = state.observe(setup, info); err != nil {
		return nil, state.failure(caller)
	}
	state.download, err = state.transport.OpenDownload(setup)
	if err != nil || state.download == nil || state.download.Info().Length != info.Size {
		return nil, state.failure(caller)
	}
	if err = state.observe(setup, info); err != nil || state.currentPG(setup, true) != nil {
		return nil, state.failure(caller)
	}
	// Use the genuine private HTTP3 current-check gate, not a racing direct
	// lease.Check. Its zero-length Read never consumes application body bytes.
	if n, checkErr := state.download.Read(nil); n != 0 || checkErr != nil {
		return nil, state.failure(caller)
	}
	prevented := stopSetup()
	if !prevented || !familyJellyfinLive(caller) || !familyJellyfinLive(setup) || state.currentPG(setup, true) != nil || !familyJellyfinLive(state.download.Context()) {
		return nil, state.failure(caller)
	}
	state.mu.Lock()
	if state.closed || !familyJellyfinLive(life) || !familyJellyfinLive(caller) {
		state.mu.Unlock()
		return nil, state.failure(caller)
	}
	state.info = FamilyJellyfinStreamInfo{FileName: "media-download.bin", Container: state.receipt.Container, Length: state.receipt.Size, RunTimeTicks: state.receipt.Ticks}
	var descriptor familyJellyfinDescriptor
	_ = json.Unmarshal([]byte(state.receipt.Descriptor), &descriptor)
	for _, codec := range descriptor.Streams {
		if codec.Codec != "" {
			state.info.ReportedCodecs = append(state.info.ReportedCodecs, codec.Type+":"+codec.Codec)
		}
	}
	state.remaining = state.receipt.Size
	state.ready = true
	watchDone := make(chan struct{})
	state.watchDone = watchDone
	result := &FamilyJellyfinStream{state: state}
	state.self = result
	state.mu.Unlock()
	go state.watch(info, watchDone)
	return result, nil
}

func (state *familyJellyfinStreamState) failure(caller context.Context) error {
	if caller != nil && !familyJellyfinLive(caller) {
		if err := caller.Err(); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	if !familyJellyfinLive(state.ctx) {
		return profileunlock.ErrFamilyMediaChanged
	}
	state.source.Invalidate()
	state.cancel() // Signal only; the external cleanup worker owns the joins.
	return profileunlock.ErrFamilyMediaChanged
}

func (state *familyJellyfinStreamState) fresh() bool {
	state.mu.Lock()
	observed, closed := state.observedAt, state.closed
	state.mu.Unlock()
	age := time.Since(observed)
	return !closed && !observed.IsZero() && age >= 0 && age < familyJellyfinFreshness
}

// No direct lease.Check can race HTTP3's private serialization. Actual managed
// checks occur in ObserveScope/Download.Read, including the genuine nil Read.
func (state *familyJellyfinStreamState) currentPG(ctx context.Context, observation bool) error {
	if !familyJellyfinLive(ctx) || !familyJellyfinLive(state.ctx) || !familyJellyfinLive(state.source.Context()) || state.lease != nil && !familyJellyfinLive(state.lease.Context()) || observation && !state.fresh() {
		return profileunlock.ErrFamilyMediaChanged
	}
	work, cancel := context.WithCancel(ctx)
	join := familyJellyfinAfterFunc(state.ctx, cancel)
	defer cancel()
	defer join()
	select {
	case state.checks <- struct{}{}:
		defer func() { <-state.checks }()
	case <-work.Done():
		return profileunlock.ErrFamilyMediaChanged
	case <-state.ctx.Done():
		return profileunlock.ErrFamilyMediaChanged
	}
	if state.source.Check(work) != nil || !familyJellyfinLive(work) || !familyJellyfinLive(ctx) || !familyJellyfinLive(state.ctx) || !familyJellyfinLive(state.source.Context()) || state.lease != nil && !familyJellyfinLive(state.lease.Context()) || observation && !state.fresh() {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}

func (state *familyJellyfinStreamState) observe(ctx context.Context, info profileunlock.FamilyJellyfinSourceInfo) error {
	state.mu.Lock()
	published := state.ready
	state.mu.Unlock()
	if state.currentPG(ctx, published) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	observation, err := state.transport.ObserveScope(ctx)
	defer clear(observation.PrimaryJSON)
	defer clear(observation.BackingJSON)
	defer clear(observation.AncestorsJSON)
	if err != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	receipt, err := parseFamilyJellyfin(info, observation)
	age := time.Since(observation.StartedAt)
	if err != nil || observation.StartedAt.IsZero() || age < 0 || age >= familyJellyfinFreshness || state.currentPG(ctx, published) != nil || !familyJellyfinLive(ctx) {
		return profileunlock.ErrFamilyMediaChanged
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	age = time.Since(observation.StartedAt)
	if state.closed || age < 0 || age >= familyJellyfinFreshness || !familyJellyfinLive(ctx) || !familyJellyfinLive(state.ctx) || !state.observedAt.IsZero() && receipt != state.receipt {
		return profileunlock.ErrFamilyMediaChanged
	}
	state.receipt, state.observedAt = receipt, observation.StartedAt
	return nil
}

func (state *familyJellyfinStreamState) watch(info profileunlock.FamilyJellyfinSourceInfo, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(familyJellyfinPoll)
	defer ticker.Stop()
	for {
		select {
		case <-state.ctx.Done():
			return
		case <-ticker.C:
			if state.observe(state.ctx, info) != nil {
				_ = state.failure(state.ctx)
				return
			}
		}
	}
}

func (state *familyJellyfinStreamState) admit() (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || !state.ready || !familyJellyfinLive(state.ctx) {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	state.ops.Add(1)
	return state.ops.Done, nil
}

func (stream *FamilyJellyfinStream) Check(ctx context.Context) error {
	state := stream.original()
	if state == nil || !familyJellyfinLive(ctx) {
		return profileunlock.ErrFamilyMediaChanged
	}
	if !state.readGate.TryLock() {
		return profileunlock.ErrFamilyMediaBusy // Caller-only contention, not proof loss.
	}
	defer state.readGate.Unlock()
	done, err := state.admit()
	if err != nil {
		return err
	}
	defer done()
	if state.currentPG(ctx, true) != nil {
		return state.failure(ctx)
	}
	if n, err := state.download.Read(nil); n != 0 || err != nil || state.currentPG(ctx, true) != nil || !familyJellyfinLive(ctx) {
		return state.failure(ctx)
	}
	return nil
}

func (stream *FamilyJellyfinStream) Read(target []byte) (int, error) {
	state := stream.original()
	if state == nil {
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	if !state.readGate.TryLock() {
		return 0, profileunlock.ErrFamilyMediaBusy
	}
	defer state.readGate.Unlock()
	done, err := state.admit()
	if err != nil {
		return 0, err
	}
	defer done()
	if state.currentPG(state.ctx, true) != nil {
		return 0, state.failure(state.ctx)
	}
	if len(target) == 0 {
		if n, err := state.download.Read(nil); n != 0 || err != nil || state.currentPG(state.ctx, true) != nil {
			return 0, state.failure(state.ctx)
		}
		return 0, nil
	}
	if state.remaining == 0 {
		// Final n>0 bytes must stay current for the caller's Check/Write. Only
		// this subsequent genuine zero-byte EOF ends the held stream lifetime.
		state.cancel()
		return 0, io.EOF
	}
	buffer := make([]byte, min(int64(len(target)), min(int64(32<<10), state.remaining)))
	defer clear(buffer)
	n, readErr := state.download.Read(buffer)
	if n < 0 || n > len(buffer) || readErr != nil && readErr != io.EOF || state.currentPG(state.ctx, true) != nil || n == 0 && readErr == nil || readErr == io.EOF && int64(n) < state.remaining {
		return 0, state.failure(state.ctx)
	}
	state.remaining -= int64(n)
	copy(target, buffer[:n])
	return n, readErr
}

func (stream *FamilyJellyfinStream) Close() error {
	state := stream.original()
	if state == nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	state.cancel()
	<-state.done
	return state.closeErr
}

func (state *familyJellyfinStreamState) cleanup() {
	<-state.ctx.Done()
	<-state.setupDone // Open is not counted in the active Check/Read WaitGroup.
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	if state.lease != nil && state.lease.Close() != nil {
		state.closeErr = profileunlock.ErrFamilyMediaChanged
	}
	state.ops.Wait()
	if state.watchDone != nil {
		<-state.watchDone
	}
	if state.joinConfig != nil {
		state.joinConfig()
	}
	if state.endExposed != nil {
		state.endExposed()
	}
	familyJellyfinWorkers.Lock()
	familyJellyfinWorkers.active--
	if state.keyBound {
		familyJellyfinWorkers.keys[state.key]--
		if familyJellyfinWorkers.keys[state.key] == 0 {
			delete(familyJellyfinWorkers.keys, state.key)
		}
	}
	familyJellyfinWorkers.Unlock()
	// No reverse source.Close/owner.Close edge: their original external owner
	// waits this completion before its own PG admission can be reclaimed.
	close(state.done)
}
