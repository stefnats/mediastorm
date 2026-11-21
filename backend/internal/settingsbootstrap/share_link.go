package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/datastore"
)

// NewShareLinkCreator binds only the already-finished original managed source.
// It never starts Finish or exposes raw coordinator/transaction capabilities.
func (s *Startup) NewShareLinkCreator(store *datastore.DataStore) (*datastore.ShareLinkCreator, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	finishing := s.finishing
	s.mu.Unlock()
	if !finishing {
		return nil, config.ErrSecurityCoordinator
	}
	if s.Finish() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.recordingProgressClosing {
		return nil, config.ErrSecurityCoordinator
	}
	return datastore.NewShareLinkCreator(s.ctx, store, s.manager, s.security, s.source.Coordinator())
}
