package parentauth

import (
	"novastream/internal/datastore"
	"novastream/services/accounts"
)

// FamilyMediaReady requires the actual account service and its private PG
// correlation. An arbitrary Accounts implementation's readiness callback is
// deliberately insufficient. Generic legacy constructors/auth stay unchanged.
func (g *Guard) FamilyMediaReady(expected *datastore.DataStore) bool {
	if g == nil {
		return false
	}
	actual, ok := g.accounts.(*accounts.Service)
	return ok && actual != nil && actual.FamilyMediaReady(expected)
}
