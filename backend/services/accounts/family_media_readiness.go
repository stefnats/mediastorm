package accounts

import "novastream/internal/datastore"

// FamilyMediaReady proves that this concrete service authenticates through the
// expected original PostgreSQL store and its genuine account repository.
// Legacy/JSON services and injected authentication readers remain usable by
// their existing callers, but cannot satisfy the uninstalled family factory.
func (s *Service) FamilyMediaReady(expected *datastore.DataStore) bool {
	if s == nil || expected == nil || expected.Pool() == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	actual, ok := s.accountReader.(datastore.AccountRepository)
	return s.store == expected && ok && expected.FamilyAccountRepositoryReady(actual)
}
