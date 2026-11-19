package users

import (
	"context"
	"net/http"
	"net/http/httptest"
	"novastream/internal/datastore"
	"novastream/models"
	"reflect"
	"testing"
)

func regionalUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session) (*datastore.ParentRatingIntent, context.Context) {
	t.Helper()
	body := []byte(`{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}`)
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid/api/users/"+target+"/kids/regional-policy", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", session.ClientID)
	r, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	intent, err := ds.PrepareParentRegionalPolicy(r, target, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	if err = ds.BindParentRegionalPolicyRetry(r, intent, target, body, nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentRatingRequest(r, intent)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentRatingPassword(r, intent); err != nil {
		t.Fatal(err)
	}
	return intent, r.Context()
}
func TestRegionalPolicyKnownTargetAckPreservesUnrelatedDirtyProjection(t *testing.T) {
	f, service, target, session := modeUsersSetup(t)
	other, err := service.CreateForAccount("master", "Other synthetic profile")
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	dirty := service.users[other.ID]
	dirty.Name = "Pending unrelated name"
	service.users[other.ID] = dirty
	beforeRevision := service.persistence.revisions[other.ID]
	service.mu.Unlock()
	intent, ctx := regionalUsersIntent(t, f.ds, target, session)
	policy, err := service.CommitParentRegionalPolicy(ctx, target, intent)
	if err != nil || policy.MaxMovieAge != 6 || policy.MaxEpisodeAge != 12 {
		t.Fatal("known regional target acknowledgement failed", err)
	}
	service.mu.RLock()
	preserved := reflect.DeepEqual(service.users[other.ID], dirty) && service.persistence.revisions[other.ID] == beforeRevision
	service.mu.RUnlock()
	if !preserved {
		t.Fatal("regional acknowledgement replaced unrelated pending work")
	}
	// Next ordinary target write uses the acknowledged physical revision, while
	// generic setters still contain no regional cap assignment.
	service.mu.Lock()
	service.users[other.ID] = service.persistence.confirmed[other.ID]
	service.mu.Unlock()
	if _, err = service.Rename(target, "Known next target write"); err != nil {
		t.Fatal("regional receipt kept old target revision", err)
	}
	current, err := f.ds.CurrentRegionalProfile(t.Context(), target)
	if err != nil || current.Policy == nil || *current.Policy != policy {
		t.Fatal("ordinary acknowledged write changed regional caps")
	}
}
func TestRegionalPolicyDirtyTargetAndReplacementContextCannotCommit(t *testing.T) {
	for _, mode := range []string{"dirty", "context", "ABA"} {
		t.Run(mode, func(t *testing.T) {
			f, service, target, session := modeUsersSetup(t)
			intent, ctx := regionalUsersIntent(t, f.ds, target, session)
			switch mode {
			case "dirty":
				service.mu.Lock()
				u := service.users[target]
				u.Name = "Unconfirmed"
				service.users[target] = u
				service.mu.Unlock()
			case "context":
				ctx = context.WithValue(ctx, struct{}{}, true)
			case "ABA":
				f.exec(t, `UPDATE users SET name=name WHERE id=$1`, target)
			}
			if _, err := service.CommitParentRegionalPolicy(ctx, target, intent); err == nil {
				t.Fatal("stale local target/context committed regional caps")
			}
			current, err := f.ds.CurrentRegionalProfile(t.Context(), target)
			if err != nil || current.Policy != nil {
				t.Fatal("denied regional mutation changed stored caps")
			}
		})
	}
}
