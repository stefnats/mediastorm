package settingsbootstrap

import (
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/importer"
)

// NewSABLibraryPublicationIssuer uses only the original already-finished
// startup and importer. The caller must Close the issuer and join its original
// operations before shutting down the importer, native settings or SQL stores.
func (s *Startup) NewSABLibraryPublicationIssuer(store *datastore.DataStore, source *importer.Service) (*datastore.SABLibraryPublicationIssuer, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	finishing := s.finishing
	s.mu.Unlock()
	if !finishing {
		return nil, config.ErrSecurityCoordinator
	}
	if e := s.Finish(); e != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.recordingProgressClosing {
		return nil, config.ErrSecurityCoordinator
	}
	return datastore.NewSABLibraryPublicationIssuer(s.ctx, store, source, s.manager, s.security, s.source.Coordinator())
}
