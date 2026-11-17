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
)

func writesUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session, replace bool, body []byte) (*datastore.ParentListWriteIntent, context.Context) {
	t.Helper()
	method := http.MethodPost
	if replace {
		method = http.MethodPut
	}
	r := httptest.NewRequest(method, "http://synthetic.invalid/api/users/"+target+"/kids/lists", bytes.NewReader(body)).WithContext(t.Context())
	r.Header.Set("X-Client-ID", session.ClientID)
	checked, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	var intent *datastore.ParentListWriteIntent
	if replace {
		intent, err = ds.PrepareParentListReplace(checked, target, body, nil)
	} else {
		intent, err = ds.PrepareParentListAdd(checked, target, body, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	if replace {
		err = ds.BindParentListReplaceRetry(checked, intent, target, body, nil)
	} else {
		err = ds.BindParentListAddRetry(checked, intent, target, body, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	checked, err = ds.BoundParentListWriteRequest(checked, intent)
	if err != nil {
		t.Fatal(err)
	}
	checked.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentListWritePassword(checked, intent); err != nil {
		t.Fatal(err)
	}
	return intent, checked.Context()
}
func TestParentListWritesUsersKnownTargetedAckAndNoopRevision(t *testing.T) {
	for _, tc := range []struct {
		name, body    string
		replace, noop bool
	}{
		{"add", `{"url":"new"}`, false, false},
		{"duplicate add", `{"url":" SYNTHETIC_LIST "}`, false, true},
		{"replace", `{"lists":["replacement","replacement"]}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a, target, session := removalUsersSetup(t)
			other, err := a.CreateForAccount("master", "Other original row")
			if err != nil {
				t.Fatal(err)
			}
			b := f.service(t, f.ds)
			intent, ctx := writesUsersIntent(t, f.ds, target, session, tc.replace, []byte(tc.body))
			before := a.persistence.revisions[target]
			if _, err = b.Rename(other.ID, "External confirmed row"); err != nil {
				t.Fatal(err)
			}
			var saved models.User
			if tc.replace {
				saved, err = a.CommitParentListReplace(ctx, target, intent)
			} else {
				saved, err = a.CommitParentListAdd(ctx, target, intent)
			}
			if err != nil || saved.ID != target {
				t.Fatal("known original list commit not acknowledged", err)
			}
			if (a.persistence.revisions[target] == before) != tc.noop {
				t.Fatal("noop/write revision distinction lost")
			}
			if dbUser(t, f, other.ID).Name != "External confirmed row" {
				t.Fatal("targeted ack overwrote another service's row")
			}
			stale, _ := a.Get(other.ID)
			if stale.Name == "External confirmed row" {
				t.Fatal("targeted ack blindly resynchronized other rows")
			}
			if _, err = a.Rename(target, "Next legitimate target write"); err != nil {
				t.Fatal("target revision acknowledgement unusable", err)
			}
		})
	}
}
func TestParentListWritesUsersDirtyStaleAndForeignContextDeny(t *testing.T) {
	for _, scenario := range []string{"dirty", "external ABA", "replacement context"} {
		t.Run(scenario, func(t *testing.T) {
			f, a, target, session := removalUsersSetup(t)
			intent, ctx := writesUsersIntent(t, f.ds, target, session, false, []byte(`{"url":"new"}`))
			switch scenario {
			case "dirty":
				a.mu.Lock()
				changed := a.users[target]
				changed.Name = "Unconfirmed local change"
				a.users[target] = changed
				a.mu.Unlock()
			case "external ABA":
				b := f.service(t, f.ds)
				old, _ := b.Get(target)
				if _, err := b.SetKidsAllowedLists(target, []string{"different"}); err != nil {
					t.Fatal(err)
				}
				if _, err := b.SetKidsAllowedLists(target, old.KidsAllowedLists); err != nil {
					t.Fatal(err)
				}
			case "replacement context":
				type key struct{}
				ctx = context.WithValue(ctx, key{}, true)
			}
			before := usersVersion(t, f, target)
			ioBefore := f.ds.Pool().Stat().AcquireCount()
			if _, err := a.CommitParentListAdd(ctx, target, intent); err == nil {
				t.Fatal("stale/dirty/replacement context adopted")
			}
			if scenario == "replacement context" && f.ds.Pool().Stat().AcquireCount() != ioBefore {
				t.Fatal("replacement context reached SQL")
			}
			if usersVersion(t, f, target) != before {
				t.Fatal("denied operation changed target")
			}
		})
	}
}
func TestParentListWritesUsersActualUnknownCommitQuarantines(t *testing.T) {
	for _, tc := range []struct {
		name, body    string
		replace, noop bool
	}{
		{"add", `{"url":"new"}`, false, false},
		{"duplicate add", `{"url":"SYNTHETIC_LIST"}`, false, true},
		{"replace", `{"lists":["replacement"]}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, target, session := removalUsersSetup(t)
			writesUsersIntent(t, f.ds, target, session, tc.replace, []byte(tc.body))
			uri, proxy := usersLostCommitProxy(t, f)
			ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
			if err != nil {
				t.Fatal(err)
			}
			defer ds.Close()
			a := f.service(t, ds)
			intent, ctx := writesUsersIntent(t, ds, target, session, tc.replace, []byte(tc.body))
			before, _ := a.Get(target)
			revision := usersVersion(t, f, target)
			proxy.drop.Store(true)
			if tc.replace {
				_, err = a.CommitParentListReplace(ctx, target, intent)
			} else {
				_, err = a.CommitParentListAdd(ctx, target, intent)
			}
			if !errors.Is(err, datastore.ErrParentListWriteUnknown) || !proxy.dropped.Load() {
				t.Fatal("unknown commit reported success", err)
			}
			after, _ := a.Get(target)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unknown result published as fresh canonical user")
			}
			actual := dbUser(t, f, target)
			if tc.noop {
				if usersVersion(t, f, target) != revision || !reflect.DeepEqual(actual.KidsAllowedLists, before.KidsAllowedLists) {
					t.Fatal("duplicate transaction changed target")
				}
			} else if usersVersion(t, f, target) == revision {
				t.Fatal("lost ACK did not follow the actual committed write")
			}
			committed := usersVersion(t, f, target)
			if _, err = a.Rename(target, "No automatic retry"); !errors.Is(err, ErrPersistenceUncertain) {
				t.Fatal("unknown commit did not quarantine", err)
			}
			if usersVersion(t, f, target) != committed {
				t.Fatal("quarantine wrote to target")
			}
		})
	}
}
