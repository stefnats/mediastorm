package settingsbootstrap

import (
	"context"
	"fmt"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

// This port transfers resource cleanup ownership only. It cannot authenticate,
// approve policy or issue a session, PIN, recording or media permission.
type recordingProgressCleanup interface{ Close() }

const (
	recordingProgressPreparing = iota
	recordingProgressCaptured
	recordingProgressConstructing
	recordingProgressAttached
	recordingProgressCleaning
	recordingProgressClosed
)

// RecordingProgressBuild is an original startup-owned, one-shot construction
// reservation. Runtime dependencies are live internal locking/configuration
// capabilities, not transport data, and must not outlive the bounded build.
type RecordingProgressBuild struct{ state *recordingProgressBuildState }
type recordingProgressBuildState struct {
	self        *RecordingProgressBuild
	startup     *Startup
	store       *datastore.DataStore
	service     *recordings.Service
	pins        *profileunlock.PGProfilePins
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	cleanupDone chan struct{}
	phase       int
}

func (RecordingProgressBuild) Format(state fmt.State, _ rune) {
	fmt.Fprint(state, "RecordingProgressBuild(opaque)")
}
func (RecordingProgressBuild) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*RecordingProgressBuild) UnmarshalJSON([]byte) error  { return config.ErrSecurityCoordinator }
func (b *RecordingProgressBuild) valid() bool {
	return b != nil && b.state != nil && b.state.self == b && b.state.startup != nil && b.state.startup.self == b.state.startup
}
func (b *RecordingProgressBuild) currentLocked(phase int) bool {
	state := b.state
	s := state.startup
	return state.phase == phase && s.recordingProgressBuilding == state && s.recordingProgressReady == state.done && !s.recordingProgressClosing && s.ctx.Err() == nil && state.ctx.Err() == nil
}
func (b *RecordingProgressBuild) ready() bool {
	state := b.state
	s := state.startup
	return s.ctx.Err() == nil && state.ctx.Err() == nil && s.source != nil && state.store != nil && state.service != nil && state.pins != nil &&
		state.store.SettingsCommitReady(s.source.Coordinator()) && state.service.DerivedTimelineReady(state.store) && state.pins.FamilyMediaReady(state.store) && s.security.Ready(s.manager, s.source.Coordinator())
}

// BeginRecordingProgressBuild never starts an unbegun Finish. A captured idle
// reservation can be retired by Startup.Close; actual construction is joined.
func (s *Startup) BeginRecordingProgressBuild(store *datastore.DataStore, service *recordings.Service, pins *profileunlock.PGProfilePins) (*RecordingProgressBuild, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	if !s.finishing || s.recordingProgressClosing || s.recordingProgressBuilding != nil || s.recordingProgress != nil || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, config.ErrSecurityCoordinator
	}
	ctx, cancel := context.WithCancel(s.ctx)
	b := &RecordingProgressBuild{}
	state := &recordingProgressBuildState{self: b, startup: s, store: store, service: service, pins: pins, ctx: ctx, cancel: cancel, done: make(chan struct{}), phase: recordingProgressPreparing}
	b.state = state
	s.recordingProgressBuilding = state
	s.recordingProgressReady = state.done
	s.mu.Unlock()
	// Joining the already-started original Once also publishes its actual
	// result. Readiness checks run outside the startup mutex.
	if err := s.Finish(); err != nil || !b.ready() {
		b.Close()
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	if !b.currentLocked(recordingProgressPreparing) {
		s.mu.Unlock()
		b.Close()
		return nil, config.ErrSecurityCoordinator
	}
	state.phase = recordingProgressCaptured
	s.mu.Unlock()
	return b, nil
}

// Runtime claims actual construction once. Only the concrete Main composition
// uses these original dependencies, immediately passing them to the existing
// handler constructor; callers must defer Close and never retain the values.
func (b *RecordingProgressBuild) Runtime() (*config.Manager, *config.ManagedSecurity, *settingscommit.Coordinator, error) {
	if !b.valid() {
		return nil, nil, nil, config.ErrSecurityCoordinator
	}
	s := b.state.startup
	s.mu.Lock()
	if !b.currentLocked(recordingProgressCaptured) {
		s.mu.Unlock()
		return nil, nil, nil, config.ErrSecurityCoordinator
	}
	b.state.phase = recordingProgressConstructing
	s.mu.Unlock()
	if !b.ready() {
		b.Close()
		return nil, nil, nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	current := b.currentLocked(recordingProgressConstructing)
	s.mu.Unlock()
	if !current {
		b.Close()
		return nil, nil, nil, config.ErrSecurityCoordinator
	}
	return s.manager, s.security, s.source.Coordinator(), nil
}

// Attach is exclusively cleanup ownership. A denied or late attach closes the
// actual constructed child before the builder barrier can be refunded.
func (b *RecordingProgressBuild) Attach(child recordingProgressCleanup) error {
	if !b.valid() {
		if child != nil {
			child.Close()
		}
		return config.ErrSecurityCoordinator
	}
	s := b.state.startup
	ready := child != nil && b.ready()
	s.mu.Lock()
	if ready && b.currentLocked(recordingProgressConstructing) {
		s.recordingProgress = child
		b.state.phase = recordingProgressAttached
		s.recordingProgressBuilding = nil
		b.state.cancel()
		close(b.state.done)
		s.mu.Unlock()
		return nil
	}
	ownsCleanup := child != nil && s.recordingProgressBuilding == b.state && b.state.phase != recordingProgressClosed && b.state.phase != recordingProgressAttached && b.state.phase != recordingProgressCleaning
	if ownsCleanup {
		b.state.phase = recordingProgressCleaning
		b.state.cleanupDone = make(chan struct{})
	}
	s.mu.Unlock()
	if child != nil {
		child.Close()
	}
	if ownsCleanup {
		s.mu.Lock()
		b.state.phase = recordingProgressClosed
		b.state.cancel()
		if s.recordingProgressBuilding == b.state {
			s.recordingProgressBuilding = nil
		}
		close(b.state.cleanupDone)
		close(b.state.done)
		s.mu.Unlock()
	} else {
		b.Close()
	}
	return config.ErrSecurityCoordinator
}
func (b *RecordingProgressBuild) Close() {
	if !b.valid() {
		return
	}
	s := b.state.startup
	s.mu.Lock()
	if b.state.phase == recordingProgressCleaning {
		done := b.state.cleanupDone
		s.mu.Unlock()
		<-done
		return
	}
	if b.state.phase != recordingProgressClosed && b.state.phase != recordingProgressAttached {
		b.state.phase = recordingProgressClosed
		b.state.cancel()
		if s.recordingProgressBuilding == b.state {
			s.recordingProgressBuilding = nil
		}
		close(b.state.done)
	}
	s.mu.Unlock()
}

func (s *Startup) closeRecordingProgress() {
	s.mu.Lock()
	s.recordingProgressClosing = true
	state := s.recordingProgressBuilding
	done := s.recordingProgressReady
	// No constructor has borrowed the dependencies yet. Retire this idle
	// reservation so direct Close cannot deadlock on an unused build token.
	if state != nil && state.phase == recordingProgressCaptured {
		state.phase = recordingProgressClosed
		state.cancel()
		s.recordingProgressBuilding = nil
		close(state.done)
	}
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	child := s.recordingProgress
	s.mu.Unlock()
	if child != nil {
		child.Close()
	}
}
