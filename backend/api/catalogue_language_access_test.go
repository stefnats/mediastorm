package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
	"novastream/services/watchlist"
)

type catalogueAccessWatchlist struct {
	*watchlist.Service
	reads atomic.Int32
}

func (s *catalogueAccessWatchlist) List(profile string) ([]models.WatchlistItem, error) {
	s.reads.Add(1)
	return s.Service.List(profile)
}

type catalogueAccessFixture struct {
	*pinTestEnv
	stored         *catalogueAccessWatchlist
	entered        atomic.Int32
	expiredAccount string
}

// Reuse the PIN fixture's real login/verify/request methods with the actual
// account, session, ownership and PIN middleware around the real list handler.
func newCatalogueAccessFixture(t *testing.T) *catalogueAccessFixture {
	t.Helper()
	env := &pinTestEnv{t: t}
	f := &catalogueAccessFixture{pinTestEnv: env}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-catalogue-master-password")
	accountSvc, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, dst := range []*string{&env.account1, &env.account2, &f.expiredAccount} {
		var expires *time.Time
		if i == 2 {
			at := time.Now().Add(-time.Hour)
			expires = &at
		}
		account, e := accountSvc.CreateWithExpiry([]string{"catalogue-household", "catalogue-neighbour", "catalogue-expired"}[i], "synthetic-catalogue-password", expires)
		if e != nil {
			t.Fatal(e)
		}
		*dst = account.ID
	}
	env.sessions, err = sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	env.users, err = users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		dst                *string
		account, name, pin string
	}{
		{&env.locked, env.account1, "Catalog parent", "1234"},
		{&env.open, env.account1, "Catalog open", ""},
		{&env.foreign, env.account2, "Catalog neighbour", "9999"},
	} {
		profile, e := env.users.CreateForAccount(p.account, p.name)
		if e != nil {
			t.Fatal(e)
		}
		*p.dst = profile.ID
		if p.pin != "" {
			if _, e = env.users.SetPin(profile.ID, p.pin); e != nil {
				t.Fatal(e)
			}
		}
	}
	env.unlocks = profileunlock.NewService(nil)
	store, err := watchlist.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.stored = &catalogueAccessWatchlist{Service: store}
	for _, id := range []string{env.open, env.locked, env.foreign} {
		if _, err = store.AddOrUpdate(id, models.WatchlistUpsert{ID: "tmdb:movie:41", MediaType: "movie", Name: "SYNTHETIC-PRIVATE-CATALOGUE-ENTRY"}); err != nil {
			t.Fatal(err)
		}
	}
	list := handlers.NewDisplayListHandler(f.stored, nil, env.users)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	userHandler := handlers.NewUsersHandler(env.users, accountSvc)
	userHandler.SetProfileUnlocks(env.unlocks, func() time.Duration { return policy().ProfileUnlockTTL() })
	router := mux.NewRouter()
	protected := router.PathPrefix("/api").Subrouter()
	protected.Use(AccountAuthMiddleware(env.sessions, accountSvc))
	protected.Use(profileaccess.Middleware(env.users.BelongsToAccount))
	protected.Use(ProfilePinMiddleware(env.users, env.unlocks, policy))
	profiles := protected.PathPrefix("/users").Subrouter()
	profiles.Use(ProfileOwnershipMiddleware(env.users))
	profiles.HandleFunc("/{userID}/pin/verify", userHandler.VerifyPin).Methods(http.MethodPost)
	profiles.HandleFunc("/{userID}/display-list", func(w http.ResponseWriter, r *http.Request) { f.entered.Add(1); list.Get(w, r) }).Methods(http.MethodGet)
	env.router = router
	return f
}
func catalogueAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=watchlist&includeUnknownOriginalLanguage=true&limit=1&offset=0"
}
func TestCatalogueLanguageListActualAccountAndPINGuards(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		prepare func(*catalogueAccessFixture) pinRequest
	}{
		{"unauthenticated", 401, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open)}
		}},
		{"invalid session", 401, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked session", 401, func(f *catalogueAccessFixture) pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				f.t.Fatal(err)
			}
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open), token: token}
		}},
		{"expired account", 401, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open), token: f.login(f.expiredAccount)}
		}},
		{"scoped stream", 403, func(f *catalogueAccessFixture) pinRequest {
			s, err := f.sessions.CreateScoped(f.account1, false, "synthetic-catalogue", "127.0.0.1", time.Hour, models.SessionScopeStream)
			if err != nil {
				f.t.Fatal(err)
			}
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open), token: s.Token}
		}},
		{"foreign locked profile", 404, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"unknown profile", 404, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign selector before invalid language", 404, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"locked profile", 403, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"unlock from another session", 403, func(f *catalogueAccessFixture) pinRequest {
			first := f.login(f.account1)
			unlock := f.verify(first, f.locked, "1234").UnlockToken
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"foreign unlock", 403, func(f *catalogueAccessFixture) pinRequest {
			other := f.login(f.account2)
			unlock := f.verify(other, f.foreign, "9999").UnlockToken
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"own open profile", 200, func(f *catalogueAccessFixture) pinRequest {
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own unlocked profile", 200, func(f *catalogueAccessFixture) pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
		{"query unlock", 200, func(f *catalogueAccessFixture) pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: http.MethodGet, target: catalogueAccessTarget(f.locked) + "&profileUnlock=" + url.QueryEscape(unlock), token: token}
		}},
	}
	fixture := newCatalogueAccessFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture
			f.pinTestEnv.t = t
			f.entered.Store(0)
			f.stored.reads.Store(0)
			request := tc.prepare(f)
			rec := f.do(request)
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d", rec.Code, tc.status)
			}
			if tc.status != http.StatusOK {
				if f.entered.Load() != 0 || f.stored.reads.Load() != 0 {
					t.Fatal("denied request reached list handler or saved catalogue")
				}
				if strings.Contains(rec.Body.String(), "SYNTHETIC-PRIVATE-CATALOGUE") || strings.Contains(rec.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denied request exposed private list or capability")
				}
				return
			}
			if f.entered.Load() != 1 || f.stored.reads.Load() != 1 {
				t.Fatal("accepted request did not use exact own stored catalogue")
			}
			var payload struct {
				Supported bool                   `json:"originalLanguageFilterSupported"`
				Total     int                    `json:"total"`
				Items     []models.WatchlistItem `json:"items"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if !payload.Supported || payload.Total != 1 || len(payload.Items) != 1 || payload.Items[0].ID != "tmdb:movie:41" {
				t.Fatal("unknown-only filter lost capability or authorized stored item")
			}
			// There is deliberately no metadata provider here. A concrete language
			// cannot be inferred from the saved item's title, profile or external IDs.
			{
				q := request
				parsed, err := url.Parse(q.target)
				if err != nil {
					t.Fatal(err)
				}
				values := parsed.Query()
				values.Set("originalLanguages", "fra")
				values.Set("includeUnknownOriginalLanguage", "false")
				parsed.RawQuery = values.Encode()
				q.target = parsed.String()
				check := f.do(q)
				if check.Code != http.StatusOK {
					t.Fatalf("concrete language status=%d", check.Code)
				}
				if err := json.Unmarshal(check.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Total != 0 || len(payload.Items) != 0 {
					t.Fatal("saved scalar data invented original-language evidence")
				}
			}
		})
	}
}
