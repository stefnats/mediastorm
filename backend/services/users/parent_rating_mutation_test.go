package users

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
)

func ratingUsersIntent(t *testing.T, ds *datastore.DataStore, target string, session models.Session, kind string) (*datastore.ParentRatingIntent, context.Context) {
	t.Helper()
	body := []byte(`{"rating":" new-value "}`)
	suffix := "/kids/rating"
	if kind == "movie" {
		suffix += "/movie"
	}
	if kind == "tv" {
		suffix += "/tv"
	}
	if kind == "pair" {
		suffix = "/kids/ratings"
		body = []byte(`{"movieRating":" movie-x ","tvRating":" tv-y "}`)
	}
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid/api/users/"+target+suffix, bytes.NewReader(body)).WithContext(t.Context())
	r.Header.Set("X-Client-ID", session.ClientID)
	r, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	var i *datastore.ParentRatingIntent
	switch kind {
	case "generic":
		i, err = ds.PrepareParentRatingGeneric(r, target, body, nil)
	case "movie":
		i, err = ds.PrepareParentRatingMovie(r, target, body, nil)
	case "tv":
		i, err = ds.PrepareParentRatingTV(r, target, body, nil)
	case "pair":
		i, err = ds.PrepareParentRatingPair(r, target, body, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	switch kind {
	case "generic":
		err = ds.BindParentRatingGenericRetry(r, i, target, body, nil)
	case "movie":
		err = ds.BindParentRatingMovieRetry(r, i, target, body, nil)
	case "tv":
		err = ds.BindParentRatingTVRetry(r, i, target, body, nil)
	case "pair":
		err = ds.BindParentRatingPairRetry(r, i, target, body, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentRatingRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentRatingPassword(r, i); err != nil {
		t.Fatal(err)
	}
	return i, r.Context()
}
func commitRatingUsers(s *Service, ctx context.Context, target string, i *datastore.ParentRatingIntent, kind string) (models.User, error) {
	switch kind {
	case "generic":
		return s.CommitParentRatingGeneric(ctx, target, i)
	case "movie":
		return s.CommitParentRatingMovie(ctx, target, i)
	case "tv":
		return s.CommitParentRatingTV(ctx, target, i)
	default:
		return s.CommitParentRatingPair(ctx, target, i)
	}
}
func TestParentRatingUsersTargetedAckAllFourKinds(t *testing.T) {
	for _, kind := range []string{"generic", "movie", "tv", "pair"} {
		t.Run(kind, func(t *testing.T) {
			f, a, target, session := modeUsersSetup(t)
			if _, err := a.SetKidsMaxRating(target, "legacy"); err != nil {
				t.Fatal(err)
			}
			other, err := a.CreateForAccount("master", "Other row")
			if err != nil {
				t.Fatal(err)
			}
			b := f.service(t, f.ds)
			i, ctx := ratingUsersIntent(t, f.ds, target, session, kind)
			if _, err = b.Rename(other.ID, "External acknowledged row"); err != nil {
				t.Fatal(err)
			}
			saved, err := commitRatingUsers(a, ctx, target, i, kind)
			if err != nil || saved.ID != target {
				t.Fatal(err)
			}
			if kind == "pair" && (saved.KidsMaxMovieRating != "movie-x" || saved.KidsMaxTVRating != "tv-y" || saved.KidsMaxRating != "legacy") {
				t.Fatal("pair fields or generic fallback changed")
			}
			if dbUser(t, f, other.ID).Name != "External acknowledged row" {
				t.Fatal("rating ack overwrote another service row")
			}
			stale, _ := a.Get(other.ID)
			if stale.Name == "External acknowledged row" {
				t.Fatal("rating ack blindly resynchronized other rows")
			}
			if _, err = a.Rename(target, "Next acknowledged write"); err != nil {
				t.Fatal("rating ack kept stale revision", err)
			}
		})
	}
}
func TestParentRatingUsersDirtyABAAndReplacementContextDeny(t *testing.T) {
	for _, scenario := range []string{"dirty", "ABA", "context"} {
		t.Run(scenario, func(t *testing.T) {
			f, a, target, session := modeUsersSetup(t)
			i, ctx := ratingUsersIntent(t, f.ds, target, session, "pair")
			switch scenario {
			case "dirty":
				a.mu.Lock()
				changed := a.users[target]
				changed.Name = "Unconfirmed edit"
				a.users[target] = changed
				a.mu.Unlock()
			case "ABA":
				b := f.service(t, f.ds)
				if _, err := b.SetKidsMaxMovieRating(target, "different"); err != nil {
					t.Fatal(err)
				}
				if _, err := b.SetKidsMaxMovieRating(target, ""); err != nil {
					t.Fatal(err)
				}
			case "context":
				type key struct{}
				ctx = context.WithValue(ctx, key{}, true)
			}
			before := usersVersion(t, f, target)
			ioBefore := f.ds.Pool().Stat().AcquireCount()
			if _, err := a.CommitParentRatingPair(ctx, target, i); err == nil {
				t.Fatal("stale/dirty/foreign context committed")
			}
			if scenario == "context" && f.ds.Pool().Stat().AcquireCount() != ioBefore {
				t.Fatal("foreign context reached SQL")
			}
			if usersVersion(t, f, target) != before {
				t.Fatal("denied pair altered profile")
			}
		})
	}
}
func TestParentRatingUsersActualLostPairAckQuarantines(t *testing.T) {
	f, _, target, session := modeUsersSetup(t)
	ratingUsersIntent(t, f.ds, target, session, "pair")
	uri, proxy := usersLostCommitProxy(t, f)
	ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	a := f.service(t, ds)
	i, ctx := ratingUsersIntent(t, ds, target, session, "pair")
	before, _ := a.Get(target)
	proxy.drop.Store(true)
	if _, err = a.CommitParentRatingPair(ctx, target, i); !errors.Is(err, datastore.ErrParentRatingUnknown) || !proxy.dropped.Load() {
		t.Fatal("unknown pair commit reported success", err)
	}
	actual := dbUser(t, f, target)
	if actual.KidsMaxMovieRating != "movie-x" || actual.KidsMaxTVRating != "tv-y" {
		t.Fatal("actual unknown pair was partial")
	}
	after, _ := a.Get(target)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("unknown pair projected as known success")
	}
	version := usersVersion(t, f, target)
	if _, err = a.Rename(target, "No automatic retry"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("unknown pair did not quarantine", err)
	}
	if usersVersion(t, f, target) != version {
		t.Fatal("quarantine wrote to profile")
	}
}
func TestParentRatingJSONPairPersistsTogetherAndKeepsGeneric(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateForAccount("synthetic-owner", "JSON pair")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetKidsMaxRating(target.ID, "legacy"); err != nil {
		t.Fatal(err)
	}
	saved, err := s.SetKidsMaxRatings(target.ID, " movie-lower ", " tv-lower ")
	if err != nil || saved.KidsMaxMovieRating != "movie-lower" || saved.KidsMaxTVRating != "tv-lower" || saved.KidsMaxRating != "legacy" {
		t.Fatal("JSON pair normalization or fallback changed", err)
	}
	reopened, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	actual, ok := reopened.Get(target.ID)
	if !ok || actual.KidsMaxMovieRating != "movie-lower" || actual.KidsMaxTVRating != "tv-lower" {
		t.Fatal("JSON pair not persisted together")
	}
	cleared, err := s.SetKidsMaxRatings(target.ID, "", "")
	if err != nil || cleared.KidsMaxMovieRating != "" || cleared.KidsMaxTVRating != "" || cleared.KidsMaxRating != "legacy" {
		t.Fatal("explicit pair clear changed fallback", err)
	}
}
func TestParentRatingJSONPairFailureRollsBackPublishedProjection(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateForAccount("synthetic-owner", "JSON pair failure")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetKidsMaxRatings(target.ID, "old-movie", "old-tv"); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Get(target.ID)
	originalPath := s.path
	originalBytes, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	// The real temp encode/sync runs, but replacing an existing directory fails.
	blocked := filepath.Join(dir, "blocked-destination")
	if err = os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	s.path = blocked
	if _, err = s.SetKidsMaxRatings(target.ID, "new-movie", "new-tv"); err == nil {
		t.Fatal("failed JSON save reported success")
	}
	after, _ := s.Get(target.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed JSON pair published one or both staged fields")
	}
	current, err := os.ReadFile(originalPath)
	if err != nil || !bytes.Equal(originalBytes, current) {
		t.Fatal("failed alternate destination changed confirmed JSON", err)
	}
}
