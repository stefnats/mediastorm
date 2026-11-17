package users

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/user_settings"
)

func kidsUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session, management, enabled bool) (*datastore.ParentKidsFlagIntent, context.Context, context.CancelFunc) {
	t.Helper()
	body := []byte(`{"isKidsProfile":false}`)
	if enabled {
		body = []byte(`{"isKidsProfile":true}`)
	}
	parent, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid/api/users/"+target+"/kids-profile", bytes.NewReader(body)).WithContext(parent)
	r.Header.Set("X-Client-ID", session.ClientID)
	checked, release, e := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	var i *datastore.ParentKidsFlagIntent
	if management {
		i, e = ds.PrepareParentKidsFlagManagement(checked, target, body, nil)
	} else {
		i, e = ds.PrepareParentKidsFlagAPI(checked, target, body, nil)
	}
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	if management {
		e = ds.BindParentKidsFlagManagementRetry(checked, i, target, body, nil)
	} else {
		e = ds.BindParentKidsFlagAPIRetry(checked, i, target, body, nil)
	}
	if e != nil {
		t.Fatal(e)
	}
	checked, e = ds.BoundParentKidsFlagRequest(checked, i)
	if e != nil {
		t.Fatal(e)
	}
	checked.Header.Set("X-Account-Password", modeUserPassword)
	if e = ds.VerifyParentKidsFlagPassword(checked, i); e != nil {
		t.Fatal(e)
	}
	return i, checked.Context(), cancel
}
func TestParentKidsFlagUsersTargetedConfirmedLease(t *testing.T) {
	f, a, target, session := modeUsersSetup(t)
	other, e := a.CreateForAccount("master", "Other")
	if e != nil {
		t.Fatal(e)
	}
	i, ctx, _ := kidsUsersIntent(t, f.ds, target, session, false, true)
	remote := f.service(t, f.ds)
	if _, e = remote.Rename(other.ID, "External confirmed"); e != nil {
		t.Fatal(e)
	}
	a.mu.Lock()
	dirty := a.users[other.ID]
	dirty.Color = "pending color"
	a.users[other.ID] = dirty
	otherRevision := a.persistence.revisions[other.ID]
	a.mu.Unlock()
	l, e := a.AcquireParentKidsFlagLease(ctx, target, i)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := l.Commit()
	if e != nil {
		l.Close()
		t.Fatal(e)
	}
	saved, e := l.Stage(receipt)
	if e != nil {
		l.Close()
		t.Fatal(e)
	}
	if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, l.ReceiptLease(), nil); e != nil {
		l.Close()
		t.Fatal(e)
	}
	l.Complete()
	l.Close()
	if !saved.IsKidsProfile || saved.KidsMode != "rating" || saved.AllowLiveTakeover || saved.AllowRecordingInterruption {
		t.Fatal("known full flag result missing")
	}
	a.mu.RLock()
	current := a.users[other.ID]
	revision := a.persistence.revisions[other.ID]
	a.mu.RUnlock()
	if !reflect.DeepEqual(current, dirty) || revision != otherRevision || dbUser(t, f, other.ID).Name != "External confirmed" {
		t.Fatal("target ACK replaced other row or pending edit")
	}
	actual, _ := a.Get(target)
	if !reflect.DeepEqual(saved, actual) {
		t.Fatal("known target not installed")
	}
}
func TestParentKidsFlagUsersOriginalLocalGatesAndCopy(t *testing.T) {
	for _, scenario := range []string{"dirty", "context", "copy intent", "uncertain"} {
		t.Run(scenario, func(t *testing.T) {
			f, a, target, session := modeUsersSetup(t)
			i, ctx, _ := kidsUsersIntent(t, f.ds, target, session, false, true)
			switch scenario {
			case "dirty":
				a.mu.Lock()
				u := a.users[target]
				u.Name = "pending"
				a.users[target] = u
				a.mu.Unlock()
			case "context":
				ctx = context.WithValue(ctx, struct{}{}, true)
			case "copy intent":
				copy := *i
				i = &copy
			case "uncertain":
				a.mu.Lock()
				a.markStorageUnknownLocked([]string{target})
				a.mu.Unlock()
			}
			before := usersVersion(t, f, target)
			io := f.ds.Pool().Stat().AcquireCount()
			if l, e := a.AcquireParentKidsFlagLease(ctx, target, i); e == nil || l != nil {
				if l != nil {
					l.Close()
				}
				t.Fatal("invalid local projection accepted")
			}
			if f.ds.Pool().Stat().AcquireCount() != io || usersVersion(t, f, target) != before {
				t.Fatal("invalid local gate reached SQL")
			}
		})
	}
}
func TestParentKidsFlagUsersKnownCompoundAfterLateCancel(t *testing.T) {
	f, a, target, session := modeUsersSetup(t)
	settings, e := user_settings.NewServiceWithStore(f.ds)
	if e != nil {
		t.Fatal(e)
	}
	i, ctx, cancel := kidsUsersIntent(t, f.ds, target, session, true, true)
	u, e := a.AcquireParentKidsFlagLease(ctx, target, i)
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	s, e := settings.AcquireParentKidsFlagDefaultsLease(ctx, target, i, u.ReceiptLease())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	receipt, e := u.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = u.Stage(receipt); e != nil {
		t.Fatal(e)
	}
	if e = s.Stage(receipt); e != nil {
		t.Fatal(e)
	}
	cancel()
	i.Close()
	if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u.ReceiptLease(), s.ReceiptLease()); e != nil {
		t.Fatal("known commit canceled historical ACK", e)
	}
	// Close is also responsible for completing a taken pair on normal unwind.
	s.Close()
	u.Close()
	actual, _ := a.Get(target)
	value, e := settings.Get(target)
	if !actual.IsKidsProfile || e != nil || value == nil || value.Display.SimpleMode == nil || !*value.Display.SimpleMode {
		t.Fatal("late cancel left half a cache")
	}
	if l, e := a.AcquireParentKidsFlagLease(ctx, target, i); e == nil || l != nil {
		t.Fatal("closed operation replayed")
	}
}
func TestParentKidsFlagUsersActualLostCompoundAckQuarantinesBoth(t *testing.T) {
	f, _, target, session := modeUsersSetup(t)
	uri, proxy := usersLostCommitProxy(t, f)
	ds, e := datastore.New(t.Context(), uri, datastore.WithMaxConns(3), datastore.WithMinConns(1))
	if e != nil {
		t.Fatal(e)
	}
	defer ds.Close()
	a := f.service(t, ds)
	settings, e := user_settings.NewServiceWithStore(ds)
	if e != nil {
		t.Fatal(e)
	}
	before, _ := a.Get(target)
	i, ctx, _ := kidsUsersIntent(t, ds, target, session, true, true)
	u, e := a.AcquireParentKidsFlagLease(ctx, target, i)
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	s, e := settings.AcquireParentKidsFlagDefaultsLease(ctx, target, i, u.ReceiptLease())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	proxy.drop.Store(true)
	receipt, e := u.Commit()
	if receipt != nil || !errors.Is(e, datastore.ErrParentKidsFlagUnknown) || !proxy.dropped.Load() {
		t.Fatal("actual lost COMMIT ACK not unknown", e)
	}
	u.Quarantine()
	s.Quarantine()
	s.Close()
	u.Close()
	actual := dbUser(t, f, target)
	var mode bool
	if e = f.ds.Pool().QueryRow(t.Context(), `SELECT (settings#>>'{display,simpleMode}')::boolean FROM user_settings WHERE user_id=$1`, target).Scan(&mode); e != nil || !mode || !actual.IsKidsProfile {
		t.Fatal("durable transaction was not compound", e)
	}
	after, _ := a.Get(target)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("unknown User result adopted")
	}
	value, e := settings.Get(target)
	if !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) || value != nil {
		t.Fatal("participating Settings not quarantined", e)
	}
	if _, e = a.Rename(target, "must stay blocked"); !errors.Is(e, ErrPersistenceUncertain) {
		t.Fatal("Users global stop removed", e)
	}
	if _, e = u.Commit(); e == nil {
		t.Fatal("unknown operation replayed")
	}
}
