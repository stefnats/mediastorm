package api

import (
	"errors"
	"net/http"
	"net/url"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/customlists"
	historypkg "novastream/services/history"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type watchingAccessDenyTransport struct{ calls atomic.Int32 }

func (d *watchingAccessDenyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("synthetic API fixture denies provider HTTP")
}

type watchingAccessHistory struct {
	*historypkg.Service // Every normal history/progress/recording call is poison.
	reads               atomic.Int32
	hook                func(string)
}

func (h *watchingAccessHistory) GetCachedContinueWatchingPool(user string, _ time.Time) (*historypkg.CachedContinueWatchingPool, error) {
	h.reads.Add(1)
	if h.hook != nil {
		h.hook(user)
	}
	now := time.Now().UTC()
	return &historypkg.CachedContinueWatchingPool{ID: strings.Repeat("a", 32), ObservedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute), Complete: true, Items: []historypkg.CachedContinueWatchingItem{{SeriesID: "tmdb:movie:42", SeriesTitle: "Observed own film", MediaType: "movie", UpdatedAt: now}}}, nil
}
func (h *watchingAccessHistory) CachedContinueWatchingPoolCurrent(string, string, time.Time) bool {
	return true
}
func watchingAccessFixture(t *testing.T) (*collectionAccessFixture, *watchingAccessHistory) {
	t.Helper()
	guard := &watchingAccessDenyTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = old
		if guard.calls.Load() != 0 {
			t.Fatalf("unexpected provider demand: %d", guard.calls.Load())
		}
	})
	// Guard precedes ALL constructors, including the reused API/account/PIN fixture.
	f := newCollectionAccessFixture(t)
	history := &watchingAccessHistory{}
	lists, err := customlists.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.list.CustomListsService = lists
	f.list.SetHistoryHandler(handlers.NewHistoryHandler(history, f.users, false))
	return f, history
}
func watchingAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=continue-watching-cached&includeUnknownOriginalLanguage=true&limit=1"
}
func TestCachedWatchingActualRegisteredAccountPINAndScope(t *testing.T) {
	f, history := watchingAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: watchingAccessTarget(f.open)} }},
		{"invalid-session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"expired-account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"foreign-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"foreign-query-selector", 404, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign), token: f.login(f.account1)}
		}},
		{"locked-profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: watchingAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"known-child", 403, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.kids), token: f.login(f.account1)}
		}},
		{"master-still-child", 403, func() pinRequest {
			session, err := f.sessions.Create(f.account1, true, "synthetic", "loopback")
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: watchingAccessTarget(f.kids), token: session.Token}
		}},
		{"own-open", 200, func() pinRequest {
			return pinRequest{method: "GET", target: watchingAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: watchingAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
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
			return pinRequest{method: "GET", target: watchingAccessTarget(f.open), token: session.Token}
		}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			req := tc.make()
			history.reads.Store(0)
			w := f.do(req)
			if w.Code != tc.status {
				t.Fatalf("status%d want%d", w.Code, tc.status)
			}
			if tc.status != 200 && history.reads.Load() != 0 {
				t.Fatal("denial reached observation getter")
			}
			if tc.status == 200 && (history.reads.Load() != 1 || !strings.Contains(w.Body.String(), "Observed own film")) {
				t.Fatal("valid owned request did not consume cache")
			}
		})
	}
}
func TestCachedWatchingActualPINRevocationDuringReadStopsPublication(t *testing.T) {
	f, history := watchingAccessFixture(t)
	token := f.login(f.account1)
	unlock := f.verify(token, f.locked, "1234").UnlockToken
	history.hook = func(profile string) { f.unlocks.RevokeProfile(profile) }
	w := f.do(pinRequest{method: "GET", target: watchingAccessTarget(f.locked), token: token, unlock: unlock})
	if w.Code != 403 || history.reads.Load() != 1 || strings.Contains(w.Body.String(), "Observed own film") {
		t.Fatalf("late PIN revocation status%d", w.Code)
	}
}
