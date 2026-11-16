package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/hiddenitems"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
)

const lovedAccessClient = "synthetic-loved-client"

// Actual API middleware and PIN verification surround counted ratings/cache
// ports. Repository completeness, provider observation and native execution are
// separate proofs; session creation here is not a password-login test.
type lovedAccessRatings struct {
	mu        sync.Mutex
	records   map[string][]models.TitleRating
	delivered map[string]models.Title
	reads     atomic.Int32
	observes  atomic.Int32
	puts      atomic.Int32
	profile   atomic.Value
	client    atomic.Value
}

func (s *lovedAccessRatings) List(_ context.Context, profile string) ([]models.TitleRating, error) {
	s.reads.Add(1)
	s.profile.Store(profile)
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]models.TitleRating(nil), s.records[profile]...), nil
}
func (s *lovedAccessRatings) Observe(profile, client string, titles []models.Title) {
	s.observes.Add(1)
	s.profile.Store(profile)
	s.client.Store(client)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, title := range titles {
		s.delivered[profile+"/"+client+"/"+title.MediaType+"/"+title.ID] = title
	}
}
func (s *lovedAccessRatings) Resolve(profile, client, kind, id string) (models.Title, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	title, ok := s.delivered[profile+"/"+client+"/"+kind+"/"+id]
	return title, ok
}
func (s *lovedAccessRatings) Put(context.Context, string, models.Title, string) error {
	s.puts.Add(1)
	return errors.New("unexpected preference write during stored browse")
}

type lovedAccessFixture struct {
	*pinTestEnv
	expired string
	ratings *lovedAccessRatings
	facts   *hiddenCatalogueAccessMetadata
}

func newLovedAccessFixture(t *testing.T) *lovedAccessFixture {
	t.Helper()
	f := &lovedAccessFixture{pinTestEnv: &pinTestEnv{t: t}}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-loved-master-password")
	accountStore, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, dst := range []*string{&f.account1, &f.account2, &f.expired} {
		var expiry *time.Time
		if i == 2 {
			past := time.Now().Add(-time.Hour)
			expiry = &past
		}
		account, err := accountStore.CreateWithExpiry([]string{"loved-owner", "loved-neighbour", "loved-expired"}[i], "synthetic-loved-password", expiry)
		if err != nil {
			t.Fatal(err)
		}
		*dst = account.ID
	}
	f.sessions, err = sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.users, err = users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		dst                *string
		account, name, pin string
	}{
		{&f.open, f.account1, "Loved open", ""}, {&f.locked, f.account1, "Loved locked", "1234"}, {&f.foreign, f.account2, "Loved neighbour", "9999"},
	} {
		profile, err := f.users.CreateForAccount(item.account, item.name)
		if err != nil {
			t.Fatal(err)
		}
		*item.dst = profile.ID
		if item.pin != "" {
			if _, err = f.users.SetPin(profile.ID, item.pin); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.ratings = &lovedAccessRatings{records: map[string][]models.TitleRating{}, delivered: map[string]models.Title{}}
	f.ratings.profile.Store("")
	f.ratings.client.Store("")
	for _, profile := range []string{f.open, f.locked, f.foreign} {
		f.ratings.records[profile] = []models.TitleRating{{Key: "PRIVATE_RATING_KEY", Aliases: []string{"PRIVATE_ALIAS"}, Value: "love", UpdatedAt: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC), Title: models.Title{ID: "tmdb:movie:41", Name: "SYNTHETIC_PRIVATE_LOVE", MediaType: "movie", TMDBID: 41, Year: 2020}}}
	}
	hidden, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.facts = &hiddenCatalogueAccessMetadata{}
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	meta.TitleRatings = f.ratings
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	list.SetMetadataService(f.facts)
	list.SetMetadataHandler(meta)
	list.SetHiddenItemsService(hidden)
	f.unlocks = profileunlock.NewService(nil)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	f.router = mux.NewRouter()
	Register(f.router, nil, meta,
		nil, nil, nil, nil, nil, nil, nil,
		handlers.NewUsersHandler(f.users, accountStore), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountStore, f.sessions, f.users, nil, nil, nil, "", nil,
		WithProfileUnlocks(f.unlocks, policy))
	return f
}

func lovedAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=loved-stored&includeUnknownOriginalLanguage=true&limit=1"
}

func (f *lovedAccessFixture) request(req pinRequest, client string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(req.method, req.target, nil)
	if req.token != "" {
		r.Header.Set("Authorization", "Bearer "+req.token)
	}
	if req.unlock != "" {
		r.Header.Set(ProfileUnlockHeader, req.unlock)
	}
	if client != "" {
		r.Header.Set("X-Client-ID", client)
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func TestLovedStoredLanguageActualAccountPINAndClient(t *testing.T) {
	f := newLovedAccessFixture(t)
	before, err := json.Marshal(f.ratings.records)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		status int
		client string
		make   func() pinRequest
	}{
		{"unauthenticated", 401, lovedAccessClient, func() pinRequest { return pinRequest{method: "GET", target: lovedAccessTarget(f.open)} }},
		{"invalid-session", 401, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked-session", 401, lovedAccessClient, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: token}
		}},
		{"expired-account", 401, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"scoped-stream", 403, lovedAccessClient, func() pinRequest {
			s, err := f.sessions.CreateScoped(f.account1, false, "synthetic-loved", "127.0.0.1", time.Hour, models.SessionScopeStream)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: s.Token}
		}},
		{"scoped-watch-party", 403, lovedAccessClient, func() pinRequest {
			s, err := f.sessions.CreateScopedWithResource(f.account1, false, "synthetic-loved", "127.0.0.1", time.Hour, models.SessionScopeWatchParty, "synthetic-room")
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: s.Token}
		}},
		{"foreign-profile", 404, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"unknown-profile", 404, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign-selector-before-language", 404, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"locked-profile", 403, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, lovedAccessClient, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: lovedAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"missing-client", 400, "", func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"oversized-client", 400, strings.Repeat("x", 129), func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"invalid-language", 400, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"unsupported-watched", 400, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open) + "&watchStatus=watched", token: f.login(f.account1)}
		}},
		{"own-open", 200, lovedAccessClient, func() pinRequest {
			return pinRequest{method: "GET", target: lovedAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, lovedAccessClient, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: lovedAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			req := c.make()
			f.ratings.reads.Store(0)
			f.ratings.observes.Store(0)
			f.ratings.puts.Store(0)
			f.facts.reads.Store(0)
			f.ratings.profile.Store("")
			f.ratings.client.Store("")
			w := f.request(req, c.client)
			if w.Code != c.status {
				t.Fatalf("status=%d want=%d", w.Code, c.status)
			}
			if c.status != http.StatusOK {
				if f.ratings.reads.Load()+f.ratings.observes.Load()+f.ratings.puts.Load()+f.facts.reads.Load() != 0 {
					t.Fatal("denial reached ratings, cache or delivery side effects")
				}
				if strings.Contains(w.Body.String(), "SYNTHETIC_PRIVATE_LOVE") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denial disclosed private browse data")
				}
				return
			}
			var body struct {
				Source    string `json:"source"`
				Total     int    `json:"total"`
				Supported bool   `json:"originalLanguageFilterSupported"`
				Items     []struct {
					Title     models.Title `json:"title"`
					UpdatedAt time.Time    `json:"updatedAt"`
				} `json:"items"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			u, _ := url.Parse(req.target)
			profile := strings.Split(u.Path, "/")[3]
			if body.Source != "loved-stored" || !body.Supported || body.Total != 1 || len(body.Items) != 1 {
				t.Fatal("accepted stored browse lost capability or counts")
			}
			if f.ratings.reads.Load() != 1 || f.facts.reads.Load() != 1 || f.ratings.observes.Load() != 1 || f.ratings.puts.Load() != 0 || f.ratings.profile.Load() != profile || f.ratings.client.Load() != c.client {
				t.Fatal("accepted browse did not retain the original profile/client and bounded read/delivery")
			}
			want := f.ratings.records[profile][0]
			if body.Items[0].Title.ID != want.Title.ID || body.Items[0].Title.Name != want.Title.Name || !body.Items[0].UpdatedAt.Equal(want.UpdatedAt) {
				t.Fatal("stored identity or rating observation time changed")
			}
			if strings.Contains(w.Body.String(), "PRIVATE_RATING_KEY") || strings.Contains(w.Body.String(), "PRIVATE_ALIAS") {
				t.Fatal("private rating fields escaped the DTO")
			}
			after, _ := json.Marshal(f.ratings.records)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("browse changed the complete original rating store")
			}
		})
	}
}
