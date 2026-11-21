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

// NewShareLinkRedeemer returns the single actual managed source runtime. It is
// owned by Startup.Close, which joins original source leases before teardown.
func (s *Startup) NewShareLinkRedeemer(store *datastore.DataStore) (*datastore.ShareLinkRedeemer, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	finishing := s.finishing
	s.mu.Unlock()
	if !finishing || s.Finish() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.recordingProgressClosing {
		return nil, config.ErrSecurityCoordinator
	}
	if s.sharePlayback != nil {
		if !s.sharePlayback.MatchesStore(store) {
			return nil, config.ErrSecurityCoordinator
		}
		return s.sharePlayback, nil
	}
	current, err := datastore.NewShareLinkRedeemer(s.ctx, store, s.manager, s.security, s.source.Coordinator())
	if err != nil {
		return nil, err
	}
	s.sharePlayback = current
	return current, nil
}
func (s *Startup) closeSharePlayback() {
	s.mu.Lock()
	owned := s.sharePlayback
	s.mu.Unlock()
	if owned != nil {
		owned.Close()
	}
}
