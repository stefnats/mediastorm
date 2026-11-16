package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/hiddenitems"
	"novastream/services/metadata"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
)

type hiddenCatalogueAccessStore struct {
	*hiddenitems.Service
	reads   atomic.Int32
	profile atomic.Value
}

func (s *hiddenCatalogueAccessStore) List(profile string) ([]models.HiddenItem, error) {
	s.reads.Add(1)
	s.profile.Store(profile)
	return s.Service.List(profile)
}

// The embedded interface methods are deliberately unavailable: this path must
// only consume the optional cache lookup, never enrich or demand provider data.
type hiddenCatalogueAccessMetadata struct {
	*metadata.Service
	reads atomic.Int32
}

func (s *hiddenCatalogueAccessMetadata) GetCachedOriginalLanguageEvidence(string, string, time.Time) *models.OriginalLanguageEvidence {
	s.reads.Add(1)
	return nil
}

type hiddenCatalogueAccessFixture struct {
	*pinTestEnv
	stored         *hiddenCatalogueAccessStore
	facts          *hiddenCatalogueAccessMetadata
	expiredAccount string
	items          map[string]models.HiddenItem
}

func newHiddenCatalogueAccessFixture(t *testing.T) *hiddenCatalogueAccessFixture {
	t.Helper()
	f := &hiddenCatalogueAccessFixture{pinTestEnv: &pinTestEnv{t: t}, items: map[string]models.HiddenItem{}}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-hidden-master-password")
	accountStore, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, destination := range []*string{&f.account1, &f.account2, &f.expiredAccount} {
		var expiry *time.Time
		if i == 2 {
			at := time.Now().Add(-time.Hour)
			expiry = &at
		}
		account, err := accountStore.CreateWithExpiry([]string{"hidden-owner", "hidden-neighbour", "hidden-expired"}[i], "synthetic-hidden-password", expiry)
		if err != nil {
			t.Fatal(err)
		}
		*destination = account.ID
	}
	f.sessions, err = sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.users, err = users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []struct {
		destination        *string
		account, name, pin string
	}{
		{&f.open, f.account1, "Hidden open", ""},
		{&f.locked, f.account1, "Hidden locked", "1234"},
		{&f.foreign, f.account2, "Hidden neighbour", "9999"},
	} {
		user, err := f.users.CreateForAccount(profile.account, profile.name)
		if err != nil {
			t.Fatal(err)
		}
		*profile.destination = user.ID
		if profile.pin != "" {
			if _, err = f.users.SetPin(user.ID, profile.pin); err != nil {
				t.Fatal(err)
			}
		}
	}
	store, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.stored = &hiddenCatalogueAccessStore{Service: store}
	f.stored.profile.Store("")
	for _, profile := range []string{f.open, f.locked, f.foreign} {
		item, err := store.Hide(profile, models.HiddenItemUpsert{ID: "tmdb:movie:41", MediaType: "movie", Name: "SYNTHETIC_PRIVATE_HIDDEN_ENTRY", Year: 2020, PosterURL: "https://synthetic.invalid/stored-poster"})
		if err != nil {
			t.Fatal(err)
		}
		// Keep an independent baseline, including any normalized ExternalIDs map.
		raw, err := json.Marshal(item)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot models.HiddenItem
		if err = json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		f.items[profile] = snapshot
	}
	f.unlocks = profileunlock.NewService(nil)
	f.facts = &hiddenCatalogueAccessMetadata{}
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	list.SetHiddenItemsService(f.stored)
	list.SetMetadataService(f.facts)
	userHandler := handlers.NewUsersHandler(f.users, accountStore)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	router := mux.NewRouter()
	Register(router,
		nil, &handlers.MetadataHandler{},
		nil, nil, nil, nil, nil, nil, nil,
		userHandler, nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountStore, f.sessions, f.users, nil, nil, nil, "", nil,
		WithProfileUnlocks(f.unlocks, policy),
	)
	f.router = router
	return f
}

func hiddenCatalogueAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=hidden&includeUnknownOriginalLanguage=true&limit=1&offset=0"
}

func TestHiddenCatalogueLanguageActualAccountAndPINGuards(t *testing.T) {
	f := newHiddenCatalogueAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open)} }},
		{"invalid session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open), token: token}
		}},
		{"expired account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open), token: f.login(f.expiredAccount)}
		}},
		{"scoped stream", 403, func() pinRequest {
			s, err := f.sessions.CreateScoped(f.account1, false, "synthetic-hidden", "127.0.0.1", time.Hour, models.SessionScopeStream)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open), token: s.Token}
		}},
		{"foreign locked profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"unknown profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign selector before invalid language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"locked profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"unlock from another session", 403, func() pinRequest {
			first := f.login(f.account1)
			unlock := f.verify(first, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"foreign unlock", 403, func() pinRequest {
			other := f.login(f.account2)
			unlock := f.verify(other, f.foreign, "9999").UnlockToken
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"own open profile", 200, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own unlocked profile", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
		{"query unlock", 200, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.locked) + "&profileUnlock=" + url.QueryEscape(unlock), token: token}
		}},
		{"invalid registered language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"unsupported watched query", 400, func() pinRequest {
			return pinRequest{method: "GET", target: hiddenCatalogueAccessTarget(f.open) + "&watchStatus=complete", token: f.login(f.account1)}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			request := tc.make()
			f.stored.reads.Store(0)
			f.facts.reads.Store(0)
			f.stored.profile.Store("")
			reply := f.do(request)
			if reply.Code != tc.status {
				t.Fatalf("status=%d want=%d", reply.Code, tc.status)
			}
			if tc.status != http.StatusOK {
				if f.stored.reads.Load() != 0 || f.facts.reads.Load() != 0 {
					t.Fatal("denied or invalid request reached stored hidden data or cached facts")
				}
				if strings.Contains(reply.Body.String(), "SYNTHETIC_PRIVATE_HIDDEN") || strings.Contains(reply.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denied request leaked hidden data/capability")
				}
				return
			}
			var body struct {
				Source    string              `json:"source"`
				Items     []models.HiddenItem `json:"items"`
				Total     int                 `json:"total"`
				Supported bool                `json:"originalLanguageFilterSupported"`
			}
			if err := json.Unmarshal(reply.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			path, _ := url.Parse(request.target)
			profile := strings.Split(path.Path, "/")[3]
			want := f.items[profile]
			if f.stored.reads.Load() != 1 || f.stored.profile.Load() != profile || f.facts.reads.Load() != 1 {
				t.Fatal("accepted page did not read exactly its original profile and cached fact")
			}
			if body.Source != "hidden" || !body.Supported || body.Total != 1 || len(body.Items) != 1 {
				t.Fatal("accepted hidden management page has invalid capability or count")
			}
			got := body.Items[0]
			if got.Key() != want.Key() || got.Name != want.Name || got.PosterURL != want.PosterURL || !got.HiddenAt.Equal(want.HiddenAt) {
				t.Fatal("management response lost original stored HiddenItem fields")
			}
			for originalProfile, originalItem := range f.items {
				still, err := f.stored.Service.List(originalProfile)
				if err != nil || len(still) != 1 || !reflect.DeepEqual(still[0], originalItem) {
					t.Fatal("language query mutated an original complete hidden profile store")
				}
			}
		})
	}
}
