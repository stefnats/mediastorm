package parentauth

import (
	"context"
	"net/http/httptest"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
)

type familyReadinessClaimedAccounts struct {
	*freshAccounts
	claims int
}

func (a *familyReadinessClaimedAccounts) FamilyMediaReady(*datastore.DataStore) bool {
	a.claims++
	return true
}

func TestFamilyParentReadinessRejectsClaimedBooleanWithoutChangingFreshGuard(t *testing.T) {
	calls := 0
	fake := &familyReadinessClaimedAccounts{freshAccounts: &freshAccounts{verify: func(ctx context.Context, username, password string) (models.Account, error) {
		calls++
		if ctx.Err() != nil || username != "parent" || password != "synthetic parent password" {
			t.Error("legacy fresh input changed")
		}
		return models.Account{ID: "account-a"}, nil
	}}}
	guard := New(fake)
	if guard.FamilyMediaReady(&datastore.DataStore{}) || fake.claims != 0 || calls != 0 {
		t.Fatal("arbitrary readiness Boolean became family wiring")
	}
	for _, master := range []bool{false, true} {
		if !guard.RequireFresh(httptest.NewRecorder(), freshRequest(context.Background(), master, "", "synthetic parent password")) {
			t.Fatal("generic existing fresh guard changed")
		}
	}
	if calls != 2 || fake.claims != 0 {
		t.Fatal("readiness substituted for fresh password verification")
	}
}

func TestFamilyParentReadinessRejectsUnavailableAndJSONBackends(t *testing.T) {
	var unavailable *Guard
	var absent *accounts.Service
	for _, guard := range []*Guard{unavailable, New(nil), New(absent), New(&accounts.Service{})} {
		if guard.FamilyMediaReady(&datastore.DataStore{}) {
			t.Fatal("missing concrete account backend became family wiring")
		}
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic readiness parent password")
	legacy, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal("create own legacy JSON fixture")
	}
	if New(legacy).FamilyMediaReady(&datastore.DataStore{}) {
		t.Fatal("JSON account service became PG family wiring")
	}
}
