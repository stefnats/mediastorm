package datastore

// FamilyAccountRepositoryReady correlates the actual private PostgreSQL
// repository pool. A factory's new wrapper is valid; a fake or transaction
// reader cannot declare itself ready by implementing an external Boolean port.
// This answers wiring identity only and performs no query or authentication.
func (ds *DataStore) FamilyAccountRepositoryReady(repo AccountRepository) bool {
	if ds == nil || ds.pool == nil {
		return false
	}
	actual, ok := repo.(*pgAccountRepo)
	return ok && actual != nil && actual.pool == ds.pool
}
