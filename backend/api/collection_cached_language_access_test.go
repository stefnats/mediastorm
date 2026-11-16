package api

import (
	"encoding/json"
	"errors"
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

// Actual API registration, account/session middleware and PIN verification
// surround counted snapshot/language ports. Producer completeness, physical
// cache files, PostgreSQL, password login and native transport are separate proofs.
type collectionAccessMetadata struct {
	*metadata.Service
	snapshots atomic.Int32
	languages atomic.Int32
	snapshot  *metadata.CachedCollectionSnapshot
	err       error
}

func (s *collectionAccessMetadata) GetCachedCollectionSnapshot(id int64, at time.Time) (*metadata.CachedCollectionSnapshot, error) {
	s.snapshots.Add(1)
	if id != 77 || at.IsZero() {
		return nil, errors.New("unexpected collection request")
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.snapshot == nil {
		return nil, nil
	}
	copy := *s.snapshot
	copy.Movies = append([]metadata.CachedCollectionMovie(nil), copy.Movies...)
	return &copy, nil
}

func (s *collectionAccessMetadata) GetCachedOriginalLanguageEvidence(string, string, time.Time) *models.OriginalLanguageEvidence {
	s.languages.Add(1)
	return nil
}

// Shadow the newly promoted optional getter so this fake represents a legacy
// metadata provider that has no snapshot capability, rather than a nil getter.
type collectionMetadataWithoutSnapshot struct {
	*metadata.Service
	GetCachedCollectionSnapshot bool
}

type collectionAccessFixture struct {
	*pinTestEnv
	expired, kids string
	facts         *collectionAccessMetadata
	hidden        *hiddenitems.Service
	meta          *handlers.MetadataHandler
	list          *handlers.DisplayListHandler
}

func newCollectionAccessFixture(t *testing.T) *collectionAccessFixture {
	t.Helper()
	f := &collectionAccessFixture{pinTestEnv: &pinTestEnv{t: t}}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-collection-master-password")
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
		account, err := accountStore.CreateWithExpiry([]string{"collection-owner", "collection-neighbour", "collection-expired"}[i], "synthetic-collection-password", expiry)
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
	for _, row := range []struct {
		dst                *string
		account, name, pin string
	}{
		{&f.open, f.account1, "Collection open", ""}, {&f.locked, f.account1, "Collection locked", "1234"},
		{&f.foreign, f.account2, "Collection neighbour", "9999"}, {&f.kids, f.account1, "Collection child", ""},
	} {
		profile, err := f.users.CreateForAccount(row.account, row.name)
		if err != nil {
			t.Fatal(err)
		}
		*row.dst = profile.ID
		if row.pin != "" {
			if _, err := f.users.SetPin(profile.ID, row.pin); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := f.users.SetKidsProfile(f.kids, true); err != nil {
		t.Fatal(err)
	}
	f.hidden, err = hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{f.open, f.locked, f.foreign, f.kids} {
		if _, err := f.hidden.Hide(profile, models.HiddenItemUpsert{ID: "tmdb:movie:41", MediaType: "movie", Name: "SYNTHETIC_SUPPRESSED_COLLECTION_MEMBER"}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	f.facts = &collectionAccessMetadata{snapshot: &metadata.CachedCollectionSnapshot{
		Version: 1, Provider: "tmdb", CollectionID: 77, Name: "Synthetic collection", MetadataLanguage: "en-US", ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour),
		Movies: []metadata.CachedCollectionMovie{{ID: 41, Name: "SYNTHETIC_SUPPRESSED_COLLECTION_MEMBER", ReleaseDate: "2020-01-01"}, {ID: 42, Name: "Visible cache-only film", ReleaseDate: "2020-01-02"}},
	}}
	f.meta = handlers.NewMetadataHandler(f.facts, nil)
	f.meta.UsersService = f.users
	f.list = handlers.NewDisplayListHandler(nil, nil, f.users)
	f.list.SetMetadataService(f.facts)
	f.list.SetMetadataHandler(f.meta)
	f.list.SetHiddenItemsService(f.hidden)
	f.unlocks = profileunlock.NewService(nil)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	f.router = mux.NewRouter()
	Register(f.router, nil, f.meta,
		nil, nil, nil, nil, nil, nil, nil,
		handlers.NewUsersHandler(f.users, accountStore), nil, nil, f.list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountStore, f.sessions, f.users, nil, nil, nil, "", nil, WithProfileUnlocks(f.unlocks, policy))
	return f
}

func collectionAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=collection-cached&collectionId=77&includeUnknownOriginalLanguage=true&limit=1"
}

func collectionAccessStoredState(t *testing.T, f *collectionAccessFixture) []byte {
	t.Helper()
	var state []any
	for _, id := range []string{f.open, f.locked, f.foreign, f.kids} {
		profile, found := f.users.Get(id)
		if !found {
			t.Fatal("fixture profile missing")
		}
		hidden, err := f.hidden.List(id)
		if err != nil {
			t.Fatal(err)
		}
		state = append(state, profile, hidden)
	}
	state = append(state, f.facts.snapshot)
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCollectionCachedAccessActualAccountPINAndScope(t *testing.T) {
	f := newCollectionAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: collectionAccessTarget(f.open)} }},
		{"invalid-session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked-session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: token}
		}},
		{"expired-account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"foreign-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"missing-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign-selector-before-language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: collectionAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"locked-profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"known-child", 403, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.kids), token: f.login(f.account1)}
		}},
		{"invalid-language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"unsupported-watched", 400, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open) + "&watchStatus=watched", token: f.login(f.account1)}
		}},
		{"conflicting-collection", 400, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open) + "&id=78", token: f.login(f.account1)}
		}},
		{"noncanonical-pagination", 400, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open) + "&offset=01", token: f.login(f.account1)}
		}},
		{"own-open", 200, func() pinRequest {
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: collectionAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
	}
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin} {
		deniedStatus := 403
		// Session validation rejects bridge tokens before middleware scope checks.
		if scope == models.SessionScopeJellyfin {
			deniedStatus = 401
		}
		cases = append(cases, struct {
			name   string
			status int
			make   func() pinRequest
		}{"scope-" + scope, deniedStatus, func() pinRequest {
			session, err := f.sessions.CreateScoped(f.account1, false, "synthetic-collection", "loopback", time.Hour, scope)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: session.Token}
		}})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			req := c.make()
			before := collectionAccessStoredState(t, f)
			f.facts.snapshots.Store(0)
			f.facts.languages.Store(0)
			w := f.do(req)
			if w.Code != c.status {
				t.Fatalf("status=%d want=%d", w.Code, c.status)
			}
			if !reflect.DeepEqual(before, collectionAccessStoredState(t, f)) {
				t.Fatal("browse changed profile, hidden data or original observation")
			}
			if c.status != 200 {
				if f.facts.snapshots.Load()+f.facts.languages.Load() != 0 {
					t.Fatal("denial reached snapshot or language lookup")
				}
				if strings.Contains(w.Body.String(), "Visible cache-only film") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denial disclosed collection data")
				}
				return
			}
			var page struct {
				Source     string         `json:"source"`
				Total      int            `json:"total"`
				Supported  bool           `json:"originalLanguageFilterSupported"`
				Items      []models.Title `json:"items"`
				Collection struct {
					ID         int64     `json:"id"`
					ObservedAt time.Time `json:"observedAt"`
					ExpiresAt  time.Time `json:"expiresAt"`
				} `json:"collection"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if page.Source != "collection-cached" || !page.Supported || page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "tmdb:movie:42" || page.Collection.ID != 77 || !page.Collection.ObservedAt.Equal(f.facts.snapshot.ObservedAt) || !page.Collection.ExpiresAt.Equal(f.facts.snapshot.ExpiresAt) {
				t.Fatal("complete filtered browse lost identity, capability or original observation")
			}
			if f.facts.snapshots.Load() != 1 || f.facts.languages.Load() != 1 || strings.Contains(w.Body.String(), "SYNTHETIC_SUPPRESSED") {
				t.Fatal("hidden suppression or bounded cache-only lookup changed")
			}
			if w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal("private response became cacheable")
			}
		})
	}
}

func TestCollectionCachedAccessUnavailableIsNotEmptySuccess(t *testing.T) {
	for _, name := range []string{"missing-users", "missing-hidden", "missing-cache-port", "snapshot-missing", "snapshot-error", "snapshot-expired"} {
		t.Run(name, func(t *testing.T) {
			f := newCollectionAccessFixture(t)
			want := 503
			switch name {
			case "missing-users":
				f.meta.UsersService = nil
			case "missing-hidden":
				f.list.SetHiddenItemsService(nil)
			case "missing-cache-port":
				f.meta.Service = &collectionMetadataWithoutSnapshot{}
			case "snapshot-missing":
				f.facts.snapshot = nil
				want = 409
			case "snapshot-error":
				f.facts.err = errors.New("synthetic-private-cache-path")
				want = 409
			case "snapshot-expired":
				f.facts.snapshot.ExpiresAt = time.Now().Add(-time.Hour)
				want = 409
			}
			before := collectionAccessStoredState(t, f)
			w := f.do(pinRequest{method: "GET", target: collectionAccessTarget(f.open), token: f.login(f.account1)})
			if w.Code != want || strings.Contains(w.Body.String(), `"items"`) || strings.Contains(w.Body.String(), "synthetic-private-cache-path") {
				t.Fatalf("unavailable status=%d want=%d", w.Code, want)
			}
			if f.facts.languages.Load() != 0 || !reflect.DeepEqual(before, collectionAccessStoredState(t, f)) {
				t.Fatal("unavailable path demanded language data or changed saved state")
			}
			if name == "missing-users" || name == "missing-hidden" || name == "missing-cache-port" {
				if f.facts.snapshots.Load() != 0 {
					t.Fatal("missing required port reached snapshot lookup")
				}
			} else if f.facts.snapshots.Load() != 1 {
				t.Fatal("expected exactly one cache lookup")
			}
		})
	}
}
