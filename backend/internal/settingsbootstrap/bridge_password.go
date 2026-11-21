package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/jellyfinbridge"
	"novastream/services/profileunlock"
)

// NewBridgePasswordLogin constructs only the password route's concrete managed
// dependencies. It neither starts Finish nor lends generic runtime capabilities.
func (s *Startup) NewBridgePasswordLogin(store *datastore.DataStore, pins *profileunlock.PGProfilePins) (*jellyfinbridge.ManagedPasswordLogin, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	finishing := s.finishing
	s.mu.Unlock()
	if !finishing {
		return nil, config.ErrSecurityCoordinator
	}
	// Join only the already-begun original Finish, outside the startup mutex.
	if err := s.Finish(); err != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.recordingProgressClosing {
		return nil, config.ErrSecurityCoordinator
	}
	// This constructor has no I/O, worker or independently owned resource.
	// Startup.Close cancels first and takes this mutex before closing the live
	// dependencies; a returned adapter rechecks those dependencies per request.
	return jellyfinbridge.NewManagedPasswordLogin(s.ctx, store, s.manager, s.security, s.source.Coordinator(), pins)
}
