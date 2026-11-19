package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/metadata"
)

// Registered session/PIN middleware surrounds a counted synthetic cache port.
// Actual provider observations and physical files are separate metadata tests.
type personCreditsAccessMetadata struct {
	*metadata.Service
	reads     atomic.Int32
	languages atomic.Int32
	normal    atomic.Int32
	snapshot  *metadata.CachedTMDBPersonCreditsSnapshot
	err       error
	hook      func(int32)
}

func (s *personCreditsAccessMetadata) CachedTMDBPersonCreditsMetadataLanguage() string {
	return "en-US"
}
func (s *personCreditsAccessMetadata) GetCachedTMDBPersonCreditsSnapshot(id int64, at time.Time) (*metadata.CachedTMDBPersonCreditsSnapshot, error) {
	count := s.reads.Add(1)
	if id != 77 || at.IsZero() {
		return nil, errors.New("unexpected synthetic person ID")
	}
	if s.hook != nil {
		s.hook(count)
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.snapshot == nil {
		return nil, nil
	}
	copy := *s.snapshot
	copy.Candidates = append([]metadata.CachedTMDBPersonCreditsCandidate{}, s.snapshot.Candidates...)
	return &copy, nil
}
func (s *personCreditsAccessMetadata) GetCachedOriginalLanguageEvidence(string, string, time.Time) *models.OriginalLanguageEvidence {
	s.languages.Add(1)
	return nil
}
func (s *personCreditsAccessMetadata) GetTMDBList(context.Context, metadata.TMDBListOptions) ([]models.TrendingItem, int, error) {
	s.normal.Add(1)
	return nil, 0, errors.New("ordinary person-credit generation must not run")
}

func newPersonCreditsAccessFixture(t *testing.T) (*collectionAccessFixture, *personCreditsAccessMetadata) {
	t.Helper()
	guard := &popularTitlesAccessDenyTransport{}
	before := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = before
		if guard.calls.Load() != 0 {
			t.Errorf("unexpected provider HTTP: %d", guard.calls.Load())
		}
	})
	f := newCollectionAccessFixture(t)
	now := time.Now().UTC()
	cache := &personCreditsAccessMetadata{snapshot: &metadata.CachedTMDBPersonCreditsSnapshot{Version: 1, Provider: "tmdb", SourceType: "person-credits", PersonID: 77, MetadataLanguage: "en-US", ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), Candidates: []metadata.CachedTMDBPersonCreditsCandidate{{ID: "tmdb:movie:41", MediaType: "movie", TMDBID: 41, Name: "SYNTHETIC_SUPPRESSED_CAST_MEMBER"}, {ID: "tmdb:tv:42", MediaType: "series", TMDBID: 42, Name: "Visible saved cast title"}, {ID: "tmdb:movie:42", MediaType: "movie", TMDBID: 42, Name: "Same numeric ID, separate movie"}}}}
	f.meta.Service = cache
	f.list.SetMetadataService(cache)
	return f, cache
}
func personCreditsAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=tmdb-person-credits-cached&personId=77&includeUnknownOriginalLanguage=true&limit=1"
}
func personCreditsAccessState(t *testing.T, f *collectionAccessFixture, c *personCreditsAccessMetadata) []byte {
	t.Helper()
	pins := map[string][32]byte{}
	for _, id := range []string{f.open, f.locked, f.foreign, f.kids} {
		u, ok := f.users.Get(id)
		if !ok {
			t.Fatal("fixture profile missing")
		}
		pins[id] = sha256.Sum256([]byte(u.PinHash))
	}
	raw, err := json.Marshal([]any{collectionAccessStoredState(t, f), pins, c.snapshot})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestTMDBPersonCreditsCachedActualRegisteredAccountPINAndScope(t *testing.T) {
	f, c := newPersonCreditsAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open)} }},
		{"invalid-session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked-session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: token}
		}},
		{"expired-account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"foreign-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"missing-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign-selector-before-language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"locked-profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"known-child", 403, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.kids), token: f.login(f.account1)}
		}},
		{"master-child", 403, func() pinRequest {
			session, err := f.sessions.Create(f.account1, true, "synthetic", "loopback")
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.kids), token: session.Token}
		}},
		{"invalid-language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"unsupported-watched", 400, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&hideWatched=true", token: f.login(f.account1)}
		}},
		{"duplicate-source", 400, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&source=tmdb-list", token: f.login(f.account1)}
		}},
		{"duplicate-person", 400, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&personId=77", token: f.login(f.account1)}
		}},
		{"noncanonical-person", 400, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(personCreditsAccessTarget(f.open), "personId=77", "personId=077", 1), token: f.login(f.account1)}
		}},
		{"unsupported-provider-mode", 400, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&sourceType=private-list", token: f.login(f.account1)}
		}},
		{"own-open", 200, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"matching-profile-and-client", 200, func() pinRequest {
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.open) + "&userId=" + url.QueryEscape(f.open) + "&clientId=synthetic-client", token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
	}
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin} {
		scope := scope
		status := 403
		if scope == models.SessionScopeJellyfin {
			status = 401
		}
		cases = append(cases, struct {
			name   string
			status int
			make   func() pinRequest
		}{"scope-" + scope, status, func() pinRequest {
			session, err := f.sessions.CreateScoped(f.account1, false, "synthetic", "loopback", time.Hour, scope)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: session.Token}
		}})
	}
	cases = append(cases, struct {
		name   string
		status int
		make   func() pinRequest
	}{"resource-share", 403, func() pinRequest {
		session, err := f.sessions.CreateScopedWithResource(f.account1, false, "synthetic", "loopback", time.Hour, models.SessionScopeStream, "https://synthetic.invalid/resource")
		if err != nil {
			t.Fatal(err)
		}
		return pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: session.Token}
	}})
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			request := tc.make()
			before := personCreditsAccessState(t, f, c)
			c.reads.Store(0)
			c.languages.Store(0)
			w := f.do(request)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
			if !reflect.DeepEqual(before, personCreditsAccessState(t, f, c)) || c.normal.Load() != 0 {
				t.Fatal("cache browse changed saved state or invoked ordinary person-credit generation")
			}
			if tc.status != 200 {
				if c.reads.Load() != 0 || c.languages.Load() != 0 {
					t.Fatal("denial reached snapshot or language reader")
				}
				if strings.Contains(w.Body.String(), "Visible saved cast title") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denial disclosed cached rows")
				}
				return
			}
			var page struct {
				Source        string         `json:"source"`
				Items         []models.Title `json:"items"`
				Total         int            `json:"total"`
				Supported     bool           `json:"originalLanguageFilterSupported"`
				PersonCredits struct {
					PersonID   int64  `json:"personId"`
					SourceType string `json:"sourceType"`
					Scope      string `json:"scope"`
				} `json:"personCredits"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if page.Source != "tmdb-person-credits-cached" || !page.Supported || page.Total != 2 || len(page.Items) != 1 || page.Items[0].ID != "tmdb:tv:42" || page.PersonCredits.PersonID != 77 || page.PersonCredits.SourceType != "person-credits" || page.PersonCredits.Scope != "returned-cast-titles" || c.reads.Load() < 2 {
				t.Fatal("actual registered browse lost finite snapshot or suppression")
			}
			if w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal("profile response became cacheable")
			}
			req := httptest.NewRequest("GET", request.target, nil)
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateResponse(req, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestTMDBPersonCreditsCachedActualPINRevocationDuringSnapshotStopsPublication(t *testing.T) {
	f, c := newPersonCreditsAccessFixture(t)
	token := f.login(f.account1)
	unlock := f.verify(token, f.locked, "1234").UnlockToken
	c.hook = func(count int32) {
		if count == 1 {
			f.unlocks.RevokeProfile(f.locked)
		}
	}
	w := f.do(pinRequest{method: "GET", target: personCreditsAccessTarget(f.locked), token: token, unlock: unlock})
	if w.Code != 403 || strings.Contains(w.Body.String(), "Visible saved cast title") || c.normal.Load() != 0 {
		t.Fatalf("late PIN revocation status=%d", w.Code)
	}
}
func TestTMDBPersonCreditsCachedMissingAndChangedObservationNeverFallsBack(t *testing.T) {
	for _, mode := range []string{"unavailable", "changed"} {
		t.Run(mode, func(t *testing.T) {
			f, c := newPersonCreditsAccessFixture(t)
			if mode == "unavailable" {
				c.err = metadata.ErrTMDBPersonCreditsSnapshotUnavailable
			} else {
				c.hook = func(count int32) {
					if count == 2 {
						c.snapshot.Candidates[1].Name = "Replacement cast title"
					}
				}
			}
			w := f.do(pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: f.login(f.account1)})
			if w.Code != 409 || strings.Contains(w.Body.String(), "Visible saved cast title") || c.normal.Load() != 0 {
				t.Fatalf("snapshot failure=%s status=%d", mode, w.Code)
			}
		})
	}
}

func TestTMDBPersonCreditsCachedRegisteredMixedIdentityPagination(t *testing.T) {
	f, c := newPersonCreditsAccessFixture(t)
	token := f.login(f.account1)
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := personCreditsAccessState(t, f, c)
	for _, tc := range []struct {
		name, query string
		ids         []string
	}{
		{"series-first", "&offset=0", []string{"tmdb:tv:42"}},
		{"movie-next", "&offset=1", []string{"tmdb:movie:42"}},
		{"finite-random-pool", "", []string{"tmdb:tv:42", "tmdb:movie:42"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := personCreditsAccessTarget(f.open) + tc.query
			if tc.name == "finite-random-pool" {
				target = strings.Replace(target, "limit=1", "limit=0", 1)
			}
			w := f.do(pinRequest{method: "GET", target: target, token: token})
			if w.Code != 200 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var page struct {
				Items []models.Title `json:"items"`
				Total int            `json:"total"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(page.Items))
			for _, item := range page.Items {
				ids = append(ids, item.ID)
			}
			if page.Total != 2 || !reflect.DeepEqual(ids, tc.ids) {
				t.Fatalf("total=%d ids=%v", page.Total, ids)
			}
			req := httptest.NewRequest("GET", target, nil)
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateResponse(req, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			if c.normal.Load() != 0 || !reflect.DeepEqual(before, personCreditsAccessState(t, f, c)) {
				t.Fatal("paging invoked generation or changed saved state")
			}
		})
	}
}

func TestTMDBPersonCreditsCachedRegisteredRejectsWholeInvalidObservation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*metadata.CachedTMDBPersonCreditsSnapshot)
	}{
		{"other-provider-mode", func(s *metadata.CachedTMDBPersonCreditsSnapshot) { s.SourceType = "director-credits" }},
		{"other-person", func(s *metadata.CachedTMDBPersonCreditsSnapshot) { s.PersonID = 78 }},
		{"hidden-row-duplicate", func(s *metadata.CachedTMDBPersonCreditsSnapshot) {
			s.Candidates = append(s.Candidates, s.Candidates[0])
		}},
		{"off-page-kind-conflict", func(s *metadata.CachedTMDBPersonCreditsSnapshot) { s.Candidates[2].MediaType = "series" }},
		{"expired", func(s *metadata.CachedTMDBPersonCreditsSnapshot) { s.ExpiresAt = time.Now().Add(-time.Minute) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, c := newPersonCreditsAccessFixture(t)
			tc.change(c.snapshot)
			before := personCreditsAccessState(t, f, c)
			w := f.do(pinRequest{method: "GET", target: personCreditsAccessTarget(f.open), token: f.login(f.account1)})
			if w.Code != 409 || strings.Contains(w.Body.String(), "Visible saved cast title") || strings.Contains(w.Body.String(), "Same numeric ID") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if c.normal.Load() != 0 || !reflect.DeepEqual(before, personCreditsAccessState(t, f, c)) {
				t.Fatal("invalid observation invoked generation or changed state")
			}
		})
	}
}
