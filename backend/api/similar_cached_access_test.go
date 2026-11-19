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
type similarAccessMetadata struct {
	*metadata.Service
	reads     atomic.Int32
	languages atomic.Int32
	normal    atomic.Int32
	snapshot  *metadata.CachedSimilarSnapshot
	err       error
	hook      func(int32)
}

func (s *similarAccessMetadata) CachedSimilarMetadataLanguage() string { return "en-US" }
func (s *similarAccessMetadata) GetCachedSimilarSnapshot(kind string, id int64, at time.Time) (*metadata.CachedSimilarSnapshot, error) {
	count := s.reads.Add(1)
	if kind != "movie" || id != 77 || at.IsZero() {
		return nil, errors.New("unexpected synthetic Similar seed")
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
	copy.Candidates = append([]metadata.CachedSimilarCandidate{}, s.snapshot.Candidates...)
	return &copy, nil
}
func (s *similarAccessMetadata) GetCachedOriginalLanguageEvidence(string, string, time.Time) *models.OriginalLanguageEvidence {
	s.languages.Add(1)
	return nil
}
func (s *similarAccessMetadata) Similar(context.Context, string, int64) ([]models.Title, error) {
	s.normal.Add(1)
	return nil, errors.New("ordinary Similar must not run")
}

func newSimilarAccessFixture(t *testing.T) (*collectionAccessFixture, *similarAccessMetadata) {
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
	cache := &similarAccessMetadata{snapshot: &metadata.CachedSimilarSnapshot{Version: 1, Provider: "tmdb", MediaType: "movie", TMDBID: 77, MetadataLanguage: "en-US", ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), Candidates: []metadata.CachedSimilarCandidate{{ID: "tmdb:movie:41", TMDBID: 41, Name: "SYNTHETIC_SUPPRESSED_SIMILAR_MEMBER"}, {ID: "tmdb:movie:42", TMDBID: 42, Name: "Visible saved recommendation"}}}}
	f.meta.Service = cache
	f.list.SetMetadataService(cache)
	return f, cache
}
func similarAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=similar-cached&tmdbId=77&mediaType=movie&includeUnknownOriginalLanguage=true&limit=1"
}
func similarAccessState(t *testing.T, f *collectionAccessFixture, c *similarAccessMetadata) []byte {
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
func TestSimilarCachedActualRegisteredAccountPINAndScope(t *testing.T) {
	f, c := newSimilarAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: similarAccessTarget(f.open)} }},
		{"invalid-session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked-session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: token}
		}},
		{"expired-account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"foreign-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"missing-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign-selector-before-language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"locked-profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: similarAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"known-child", 403, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.kids), token: f.login(f.account1)}
		}},
		{"master-child", 403, func() pinRequest {
			session, err := f.sessions.Create(f.account1, true, "synthetic", "loopback")
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: similarAccessTarget(f.kids), token: session.Token}
		}},
		{"invalid-language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"unsupported-watched", 400, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&hideWatched=true", token: f.login(f.account1)}
		}},
		{"duplicate-source", 400, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&source=similar", token: f.login(f.account1)}
		}},
		{"duplicate-seed", 400, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&tmdbId=77", token: f.login(f.account1)}
		}},
		{"noncanonical-seed", 400, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(similarAccessTarget(f.open), "tmdbId=77", "tmdbId=077", 1), token: f.login(f.account1)}
		}},
		{"invalid-kind", 400, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(similarAccessTarget(f.open), "mediaType=movie", "mediaType=episode", 1), token: f.login(f.account1)}
		}},
		{"own-open", 200, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"matching-profile-and-client", 200, func() pinRequest {
			return pinRequest{method: "GET", target: similarAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.open) + "&userId=" + url.QueryEscape(f.open) + "&clientId=synthetic-client", token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: similarAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
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
			return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: session.Token}
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
		return pinRequest{method: "GET", target: similarAccessTarget(f.open), token: session.Token}
	}})
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			request := tc.make()
			before := similarAccessState(t, f, c)
			c.reads.Store(0)
			c.languages.Store(0)
			w := f.do(request)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
			if !reflect.DeepEqual(before, similarAccessState(t, f, c)) || c.normal.Load() != 0 {
				t.Fatal("cache browse changed saved state or invoked ordinary Similar")
			}
			if tc.status != 200 {
				if c.reads.Load() != 0 || c.languages.Load() != 0 {
					t.Fatal("denial reached snapshot or language reader")
				}
				if strings.Contains(w.Body.String(), "Visible saved recommendation") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denial disclosed cached rows")
				}
				return
			}
			var page struct {
				Source    string         `json:"source"`
				Items     []models.Title `json:"items"`
				Total     int            `json:"total"`
				Supported bool           `json:"originalLanguageFilterSupported"`
				Similar   struct {
					TMDBID int64  `json:"tmdbId"`
					Kind   string `json:"mediaType"`
					Scope  string `json:"scope"`
				} `json:"similar"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if page.Source != "similar-cached" || !page.Supported || page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "tmdb:movie:42" || page.Similar.TMDBID != 77 || page.Similar.Kind != "movie" || page.Similar.Scope != "returned-results" || c.reads.Load() < 2 {
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
func TestSimilarCachedActualPINRevocationDuringSnapshotStopsPublication(t *testing.T) {
	f, c := newSimilarAccessFixture(t)
	token := f.login(f.account1)
	unlock := f.verify(token, f.locked, "1234").UnlockToken
	c.hook = func(count int32) {
		if count == 1 {
			f.unlocks.RevokeProfile(f.locked)
		}
	}
	w := f.do(pinRequest{method: "GET", target: similarAccessTarget(f.locked), token: token, unlock: unlock})
	if w.Code != 403 || strings.Contains(w.Body.String(), "Visible saved recommendation") || c.normal.Load() != 0 {
		t.Fatalf("late PIN revocation status=%d", w.Code)
	}
}
func TestSimilarCachedMissingAndChangedObservationNeverFallsBack(t *testing.T) {
	for _, mode := range []string{"unavailable", "changed"} {
		t.Run(mode, func(t *testing.T) {
			f, c := newSimilarAccessFixture(t)
			if mode == "unavailable" {
				c.err = metadata.ErrSimilarSnapshotUnavailable
			} else {
				c.hook = func(count int32) {
					if count == 2 {
						c.snapshot.Candidates[1].Name = "Replacement recommendation"
					}
				}
			}
			w := f.do(pinRequest{method: "GET", target: similarAccessTarget(f.open), token: f.login(f.account1)})
			if w.Code != 409 || strings.Contains(w.Body.String(), "Visible saved recommendation") || c.normal.Load() != 0 {
				t.Fatalf("snapshot failure=%s status=%d", mode, w.Code)
			}
		})
	}
}
