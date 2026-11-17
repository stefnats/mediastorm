package users

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"novastream/internal/datastore"
	"novastream/models"
)

const modeUserPassword = "synthetic mode parent password"

func modeUsersSetup(t *testing.T) (*usersPGFixture, *Service, string, models.Session) {
	t.Helper()
	f := usersPG(t)
	hash, err := bcrypt.GenerateFromPassword([]byte(modeUserPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='master'`, string(hash))
	service := f.service(t, f.ds)
	target, err := service.CreateForAccount("master", "Synthetic mode target")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "SYNTHETIC_PARENT_MODE_USER_SESSION", AccountID: "master", IsMaster: true, ClientID: "synthetic-mode-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err = f.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	return f, service, target.ID, session
}
func modeUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session) (*datastore.ParentModeIntent, context.Context) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid/api/users/"+target+"/kids/mode", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", session.ClientID)
	checked, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	intent, err := ds.PrepareParentMode(checked, target, "rating", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	if err = ds.BindParentModeRetry(checked, intent, target, "rating", nil); err != nil {
		t.Fatal(err)
	}
	checked, err = ds.BoundParentModeRequest(checked, intent)
	if err != nil {
		t.Fatal(err)
	}
	checked.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentModePassword(checked, intent); err != nil {
		t.Fatal(err)
	}
	return intent, checked.Context()
}
func TestParentModeUsersTargetedAckPreservesOtherServiceWrites(t *testing.T) {
	f, a, target, session := modeUsersSetup(t)
	other, err := a.CreateForAccount("master", "Other local row")
	if err != nil {
		t.Fatal(err)
	}
	b := f.service(t, f.ds)
	intent, ctx := modeUsersIntent(t, f.ds, target, session)
	if _, err = b.Rename(other.ID, "External confirmed name"); err != nil {
		t.Fatal(err)
	}
	if _, err = a.CommitParentMode(ctx, target, intent); err != nil {
		t.Fatal(err)
	}
	if dbUser(t, f, other.ID).Name != "External confirmed name" {
		t.Fatal("mode commit overwrote other service row")
	}
	stale, _ := a.Get(other.ID)
	if stale.Name == "External confirmed name" {
		t.Fatal("mode ack blindly resynchronized another row")
	}
	targetNow, _ := a.Get(target)
	if targetNow.KidsMode != "rating" {
		t.Fatal("known mode commit not acknowledged")
	}
	if _, err = a.Rename(target, "Rename after mode ack"); err != nil {
		t.Fatal("mode ack retained stale target revision", err)
	}
}
func TestParentModeUsersChangedTargetAndDirtyProjectionDeny(t *testing.T) {
	for _, mode := range []string{"second service target", "second service target ABA", "dirty local projection"} {
		t.Run(mode, func(t *testing.T) {
			f, a, target, session := modeUsersSetup(t)
			b := f.service(t, f.ds)
			intent, ctx := modeUsersIntent(t, f.ds, target, session)
			switch mode {
			case "second service target":
				if _, err := b.Rename(target, "Replacement target row"); err != nil {
					t.Fatal(err)
				}
			case "second service target ABA":
				if _, err := b.SetKidsMode(target, "content_list"); err != nil {
					t.Fatal(err)
				}
				if _, err := b.SetKidsMode(target, ""); err != nil {
					t.Fatal(err)
				}
			case "dirty local projection":
				a.mu.Lock()
				changed := a.users[target]
				changed.Name = "Unconfirmed local name"
				a.users[target] = changed
				a.mu.Unlock()
			}
			if _, err := a.CommitParentMode(ctx, target, intent); err == nil {
				t.Fatal("stale/dirty target adopted by mode commit")
			}
			if dbUser(t, f, target).KidsMode == "rating" {
				t.Fatal("denied mode commit wrote target")
			}
		})
	}
}
func TestParentModeUsersActualLostAckQuarantinesWithoutRollback(t *testing.T) {
	f, _, target, session := modeUsersSetup(t)
	// Initialize lineage before arming the proxy; migrations and first auth are
	// not the COMMIT whose actual successful acknowledgement is discarded.
	modeUsersIntent(t, f.ds, target, session)
	uri, proxy := usersLostCommitProxy(t, f)
	ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	a := f.service(t, ds)
	intent, ctx := modeUsersIntent(t, ds, target, session)
	before, _ := a.Get(target)
	proxy.drop.Store(true)
	if _, err = a.CommitParentMode(ctx, target, intent); !errors.Is(err, datastore.ErrParentModeUnknown) || !proxy.dropped.Load() {
		t.Fatal("unknown mode result reported success", err)
	}
	if dbUser(t, f, target).KidsMode != "rating" {
		t.Fatal("lost ACK did not follow actual committed mode")
	}
	after, _ := a.Get(target)
	if after.KidsMode != before.KidsMode {
		t.Fatal("unacknowledged mode projected as success")
	}
	version := usersVersion(t, f, target)
	if _, err = a.Rename(target, "Automatic retry must not happen"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("unknown mode did not quarantine later writes", err)
	}
	if usersVersion(t, f, target) != version {
		t.Fatal("quarantine changed actual committed row")
	}
}
