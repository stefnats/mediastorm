package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/integration"
)

// PrepareFamilyNZBMetadataRoot precedes actual NS preparation. The absolute
// path is DATA; only the later same-store/namespace/full-owner purpose opens it.
func (s *Startup) PrepareFamilyNZBMetadataRoot() (string, error) {
	if s == nil || s.self != s || s.security == nil || s.familyNZBRoot == nil || s.familyNZBRootErr != nil || !s.enter() {
		return "", config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	s.mu.Lock()
	if s.familyNZBClosing || s.familyNZBPreparing || s.ctx.Err() != nil {
		s.mu.Unlock()
		return "", config.ErrSecurityCoordinator
	}
	s.familyNZBPreparing = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.familyNZBPreparing = false; s.mu.Unlock() }()
	// No attempt to acquire another native hold while this original Startup
	// still owns its actual mutation reservation.
	if s.guard == nil || s.guard.Current(s.source.Coordinator()) != nil {
		return "", config.ErrSecurityCoordinator
	}
	root, err := s.security.PrepareFamilyNZBRoot(s.ctx, s.familyNZBRoot)
	if err != nil || s.guard.Current(s.source.Coordinator()) != nil {
		return "", config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.familyNZBClosing || s.ctx.Err() != nil {
		return "", config.ErrSecurityCoordinator
	}
	s.familyNZBRootPrepared = true
	return root, nil
}

func (s *Startup) NewFamilyNZBSources(store *datastore.DataStore, originalNS *integration.NzbSystem) (out *datastore.FamilyNZBSources, err error) {
	if s == nil || s.self != s || store == nil || originalNS == nil || s.source == nil || s.ctx.Err() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	if !s.finishing || s.familyNZBClosing || !s.familyNZBRootPrepared || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, config.ErrSecurityCoordinator
	}
	if s.familyNZBReady != nil {
		done := s.familyNZBReady
		matches := s.familyNZBStore == store && s.familyNZBOriginal == originalNS
		s.mu.Unlock()
		select {
		case <-done:
		default:
			return nil, config.ErrSecurityCoordinator
		}
		s.mu.Lock()
		result, buildErr := s.familyNZBSources, s.familyNZBBuildErr
		s.mu.Unlock()
		if !matches || buildErr != nil || result == nil || !result.MatchesOriginal(store, originalNS) {
			return nil, config.ErrSecurityCoordinator
		}
		return result, nil
	}
	// A private build reservation never enters operations: Finish waits those
	// original operations, while this constructor joins an already begun Finish.
	s.familyNZBStore = store
	s.familyNZBOriginal = originalNS
	s.familyNZBBuilding = true
	s.familyNZBReady = make(chan struct{})
	done := s.familyNZBReady
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		late := s.familyNZBClosing || s.ctx.Err() != nil
		s.mu.Unlock()
		if late && out != nil {
			out.Close()
			out = nil
			err = config.ErrSecurityCoordinator
		}
		s.mu.Lock()
		s.familyNZBSources = out
		s.familyNZBBuildErr = err
		s.familyNZBBuilding = false
		close(done)
		s.mu.Unlock()
	}()
	// sync.Once joins real SQL/native release. Early finishing is not success.
	if s.Finish() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	complete := s.finishCompleted && !s.familyNZBClosing && s.ctx.Err() == nil
	s.mu.Unlock()
	if !complete || !store.SettingsCommitReady(s.source.Coordinator()) || !s.security.Ready(s.manager, s.source.Coordinator()) {
		return nil, config.ErrSecurityCoordinator
	}
	return datastore.NewFamilyNZBSources(s.ctx, store, originalNS, s.manager, s.security, s.source.Coordinator(), s.familyNZBRoot)
}
func (s *Startup) closeFamilyNZB() {
	s.mu.Lock()
	s.familyNZBClosing = true
	done := s.familyNZBReady
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	source := s.familyNZBSources
	root := s.familyNZBRoot
	s.mu.Unlock()
	if source != nil {
		source.Close()
	}
	if root != nil {
		_ = root.Close()
	}
}
