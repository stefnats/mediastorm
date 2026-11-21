package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/datastore"
)

// NewWatchRoomAuthority selects the single already-finished original managed
// room producer. It cannot start Finish or expose a raw SQL/policy capability.
func (s *Startup) NewWatchRoomAuthority(store *datastore.DataStore) (*datastore.WatchRoomAuthority, error) {
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
	if s.ctx.Err() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	if s.watchRoom != nil {
		if !s.watchRoom.MatchesStore(store) {
			return nil, config.ErrSecurityCoordinator
		}
		return s.watchRoom, nil
	}
	authority, e := datastore.NewWatchRoomAuthority(s.ctx, store, s.manager, s.security, s.source.Coordinator())
	if e != nil {
		return nil, e
	}
	if s.ctx.Err() != nil {
		authority.Close()
		return nil, config.ErrSecurityCoordinator
	}
	s.watchRoom = authority
	return authority, nil
}
func (s *Startup) closeWatchRoom() {
	s.mu.Lock()
	owned := s.watchRoom
	s.mu.Unlock()
	if owned != nil {
		owned.Close()
	}
}

// StartWatchRoomMaintenance starts DATA housekeeping on the same actual
// already-owned authority. closeWatchRoom joins its physical work before the
// security/source/store teardown; it never fabricates a user/source principal.
func (s *Startup) StartWatchRoomMaintenance(store *datastore.DataStore) error {
	authority, err := s.NewWatchRoomAuthority(store)
	if err != nil {
		return err
	}
	return authority.StartMaintenance()
}
