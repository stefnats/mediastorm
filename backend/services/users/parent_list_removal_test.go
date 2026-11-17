package users

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
)

func removalUsersSetup(t *testing.T) (*usersPGFixture, *Service, string, models.Session) {
	t.Helper()
	f, service, target, session := modeUsersSetup(t)
	if _, err := service.SetKidsAllowedLists(target, []string{"keep", "SYNTHETIC_LIST", "SYNTHETIC_LIST", "keep"}); err != nil {
		t.Fatal(err)
	}
	return f, service, target, session
}
func removalUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session) (*datastore.ParentListRemovalIntent, context.Context) {
	t.Helper()
	r := httptest.NewRequest(http.MethodDelete, "http://synthetic.invalid/api/users/"+target+"/kids/lists?url=SYNTHETIC_LIST", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", session.ClientID)
	checked, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	intent, err := ds.PrepareParentListRemoval(checked, target, "SYNTHETIC_LIST", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	if err = ds.BindParentListRemovalRetry(checked, intent, target, "SYNTHETIC_LIST", nil); err != nil {
		t.Fatal(err)
	}
	checked, err = ds.BoundParentListRemovalRequest(checked, intent)
	if err != nil {
		t.Fatal(err)
	}
	checked.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentListRemovalPassword(checked, intent); err != nil {
		t.Fatal(err)
	}
	return intent, checked.Context()
}
func TestParentListRemovalUsersTargetedAckPreservesOtherServiceWrites(t *testing.T) {
	f, a, target, session := removalUsersSetup(t)
	other, err := a.CreateForAccount("master", "Other local row")
	if err != nil {
		t.Fatal(err)
	}
	b := f.service(t, f.ds)
	intent, ctx := removalUsersIntent(t, f.ds, target, session)
	if _, err = b.Rename(other.ID, "External confirmed name"); err != nil {
		t.Fatal(err)
	}
	if _, err = a.CommitParentListRemoval(ctx, target, intent); err != nil {
		t.Fatal(err)
	}
	if dbUser(t, f, other.ID).Name != "External confirmed name" {
		t.Fatal("removal commit overwrote other service row")
	}
	stale, _ := a.Get(other.ID)
	if stale.Name == "External confirmed name" {
		t.Fatal("removal ack blindly resynchronized another row")
	}
	targetNow, _ := a.Get(target)
	if !reflect.DeepEqual(targetNow.KidsAllowedLists, []string{"keep", "keep"}) {
		t.Fatal("known removal commit not acknowledged")
	}
	if _, err = a.Rename(target, "Rename after removal ack"); err != nil {
		t.Fatal("removal ack retained stale target revision", err)
	}
}
func TestParentListRemovalUsersChangedTargetAndDirtyProjectionDeny(t *testing.T) {
	for _, mode := range []string{"second service target", "second service target ABA", "dirty local projection"} {
		t.Run(mode, func(t *testing.T) {
			f, a, target, session := removalUsersSetup(t)
			b := f.service(t, f.ds)
			intent, ctx := removalUsersIntent(t, f.ds, target, session)
			switch mode {
			case "second service target":
				if _, err := b.Rename(target, "Replacement target row"); err != nil {
					t.Fatal(err)
				}
			case "second service target ABA":
				if _, err := b.SetKidsAllowedLists(target, []string{"different"}); err != nil {
					t.Fatal(err)
				}
				if _, err := b.SetKidsAllowedLists(target, []string{"keep", "SYNTHETIC_LIST", "SYNTHETIC_LIST", "keep"}); err != nil {
					t.Fatal(err)
				}
			case "dirty local projection":
				a.mu.Lock()
				changed := a.users[target]
				changed.Name = "Unconfirmed local name"
				a.users[target] = changed
				a.mu.Unlock()
			}
			if _, err := a.CommitParentListRemoval(ctx, target, intent); err == nil {
				t.Fatal("stale/dirty target adopted by removal commit")
			}
			if len(dbUser(t, f, target).KidsAllowedLists) == 2 {
				t.Fatal("denied removal commit wrote target")
			}
		})
	}
}
func TestParentListRemovalUsersActualLostAckQuarantinesWithoutRollback(t *testing.T) {
	f, _, target, session := removalUsersSetup(t)
	// Initialize lineage before arming the proxy; migrations and first auth are
	// not the COMMIT whose actual successful acknowledgement is discarded.
	removalUsersIntent(t, f.ds, target, session)
	uri, proxy := usersLostCommitProxy(t, f)
	ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	a := f.service(t, ds)
	intent, ctx := removalUsersIntent(t, ds, target, session)
	before, _ := a.Get(target)
	proxy.drop.Store(true)
	if _, err = a.CommitParentListRemoval(ctx, target, intent); !errors.Is(err, datastore.ErrParentListRemovalUnknown) || !proxy.dropped.Load() {
		t.Fatal("unknown removal result reported success", err)
	}
	if !reflect.DeepEqual(dbUser(t, f, target).KidsAllowedLists, []string{"keep", "keep"}) {
		t.Fatal("lost ACK did not follow actual committed removal")
	}
	after, _ := a.Get(target)
	if !reflect.DeepEqual(after.KidsAllowedLists, before.KidsAllowedLists) {
		t.Fatal("unacknowledged removal projected as success")
	}
	version := usersVersion(t, f, target)
	if _, err = a.Rename(target, "Automatic retry must not happen"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("unknown removal did not quarantine later writes", err)
	}
	if usersVersion(t, f, target) != version {
		t.Fatal("quarantine changed actual committed row")
	}
}
