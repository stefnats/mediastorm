package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/libraryaccess"
	"novastream/services/localmedia"
	"novastream/services/metadata"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This boundary fixture counts ListGroups calls. Real repository grouping and
// matched-title consistency are covered separately in the localmedia package.
type localLibraryAccessStore struct {
	*localmedia.Service
	reads   atomic.Int32
	library atomic.Value
	group   models.LocalMediaItemGroup
}

func (s *localLibraryAccessStore) ListGroups(ctx context.Context, id string, q models.LocalMediaItemListQuery) (*models.LocalMediaGroupListResult, error) {
	s.reads.Add(1)
	s.library.Store(id)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !q.IncludeCards || q.OriginalLanguageMatches == nil {
		return nil, errors.New("missing original language cards predicate")
	}
	if !q.OriginalLanguageMatches("tmdb:movie:41", "movie") {
		return nil, errors.New("unknown-only predicate rejected missing facts")
	}
	return &models.LocalMediaGroupListResult{Groups: []models.LocalMediaItemGroup{s.group}, Total: 1, Limit: q.Limit, Offset: q.Offset, OriginalLanguageFilterSupported: true}, nil
}

type localLibraryAccessMetadata struct {
	*metadata.Service
	reads atomic.Int32
}

func (s *localLibraryAccessMetadata) GetCachedOriginalLanguageEvidence(string, string, time.Time) *models.OriginalLanguageEvidence {
	s.reads.Add(1)
	return nil
}

type localLibraryAccessPolicy struct {
	datastore.LibraryAccessRepository
	profiles []string
}

func (p *localLibraryAccessPolicy) Get(ctx context.Context, id string) (*models.LibraryAccessPolicy, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if id != "library-owned" {
		return nil, nil
	}
	return &models.LibraryAccessPolicy{LibraryID: id, AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: append([]string{}, p.profiles...)}, nil
}

type localLibraryAccessFixture struct {
	*pinTestEnv
	stored         *localLibraryAccessStore
	facts          *localLibraryAccessMetadata
	expiredAccount string
}

func newLocalLibraryAccessFixture(t *testing.T) *localLibraryAccessFixture {
	t.Helper()
	f := &localLibraryAccessFixture{pinTestEnv: &pinTestEnv{t: t}}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-library-master-password")
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
		account, err := accountStore.CreateWithExpiry([]string{"library-owner", "library-neighbour", "library-expired"}[i], "synthetic-library-password", expiry)
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
		{&f.open, f.account1, "Library open", ""},
		{&f.locked, f.account1, "Library locked", "1234"},
		{&f.foreign, f.account2, "Library neighbour", "9999"},
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

	f.unlocks = profileunlock.NewService(nil)
	f.stored = &localLibraryAccessStore{group: models.LocalMediaItemGroup{ID: "synthetic-group", GroupType: "movie", LibraryType: models.LocalMediaLibraryTypeMovie, Title: "SYNTHETIC_PRIVATE_LIBRARY_CARD", Year: 2020, ItemCount: 2}}
	f.stored.library.Store("")
	f.facts = &localLibraryAccessMetadata{}
	local := handlers.NewLocalMediaHandler(f.stored, f.users, false)
	local.SetMetadataLanguageProviders(f.facts, nil, nil)
	local.SetLibraryAccessService(libraryaccess.New(&localLibraryAccessPolicy{profiles: []string{f.open, f.locked}}, nil, nil))
	userHandler := handlers.NewUsersHandler(f.users, accountStore)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	router := mux.NewRouter()
	Register(router, nil, &handlers.MetadataHandler{}, nil, nil, nil, nil, nil, nil, nil,
		userHandler, nil, nil, nil, &handlers.HistoryHandler{}, nil, nil, nil, nil, local,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		accountStore, f.sessions, f.users, nil, nil, nil, "", nil, WithProfileUnlocks(f.unlocks, policy))
	f.router = router
	return f
}
func localLibraryAccessTarget(profile string) string {
	return "/api/library/libraries/library-owned/groups?include=cards&profileId=" + url.QueryEscape(profile) + "&includeUnknownOriginalLanguage=true&limit=1&offset=0"
}
func TestLocalLibraryLanguageActualAccountPINAndACLGuards(t *testing.T) {
	f := newLocalLibraryAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open)} }},
		{"invalid session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open), token: token}
		}},
		{"expired account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open), token: f.login(f.expiredAccount)}
		}},
		{"scoped stream", 403, func() pinRequest {
			s, err := f.sessions.CreateScoped(f.account1, false, "synthetic-hidden", "127.0.0.1", time.Hour, models.SessionScopeStream)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open), token: s.Token}
		}},
		{"foreign locked profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"unknown profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign selector before invalid language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"locked profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"unlock from another session", 403, func() pinRequest {
			first := f.login(f.account1)
			unlock := f.verify(first, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"foreign unlock", 403, func() pinRequest {
			other := f.login(f.account2)
			unlock := f.verify(other, f.foreign, "9999").UnlockToken
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"own open profile", 200, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own unlocked profile", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
		{"query unlock", 200, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.locked) + "&profileUnlock=" + url.QueryEscape(unlock), token: token}
		}},
		{"invalid registered language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.open) + "&originalLanguages=not-a-language", token: f.login(f.account1)}
		}},
		{"filtered detail request", 400, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(localLibraryAccessTarget(f.open), "include=cards", "include=all", 1), token: f.login(f.account1)}
		}},
		{"ungranted library", 404, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(localLibraryAccessTarget(f.open), "library-owned", "library-denied", 1), token: f.login(f.account1)}
		}},
		{"own unlocked but ungranted profile", 404, func() pinRequest {
			token := f.login(f.account2)
			return pinRequest{method: "GET", target: localLibraryAccessTarget(f.foreign), token: token, unlock: f.verify(token, f.foreign, "9999").UnlockToken}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			r := tc.make()
			f.stored.reads.Store(0)
			f.facts.reads.Store(0)
			f.stored.library.Store("")
			expected := f.stored.group
			reply := f.do(r)
			if reply.Code != tc.status {
				t.Fatalf("status=%d want=%d", reply.Code, tc.status)
			}
			if tc.status != http.StatusOK {
				if f.stored.reads.Load() != 0 || f.facts.reads.Load() != 0 {
					t.Fatal("denied or invalid request reached ListGroups or cached language facts")
				}
				if strings.Contains(reply.Body.String(), "SYNTHETIC_PRIVATE_LIBRARY") || strings.Contains(reply.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denied request leaked library data or capability")
				}
				return
			}
			var body models.LocalMediaGroupListResult
			if err := json.Unmarshal(reply.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if f.stored.reads.Load() != 1 || f.stored.library.Load() != "library-owned" || f.facts.reads.Load() != 1 {
				t.Fatal("accepted request did not consume its single authorized list and actual private cache predicate")
			}
			if !body.OriginalLanguageFilterSupported || body.Total != 1 || len(body.Groups) != 1 || !reflect.DeepEqual(body.Groups[0], expected) {
				t.Fatal("local cards response lost original fields or support marker")
			}
			if !reflect.DeepEqual(f.stored.group, expected) {
				t.Fatal("language query mutated the original service fixture")
			}
		})
	}
}
