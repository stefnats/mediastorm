package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/handlers"
	"novastream/models"
	historypkg "novastream/services/history"
)

type popularTitlesAccessDenyTransport struct{ calls atomic.Int32 }

func (d *popularTitlesAccessDenyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("synthetic popular-titles API fixture denies provider HTTP")
}

// The counted seam delegates to a real service. Its private snapshot owner and
// revisions are verified by the promoted RecentWatchesSnapshotCurrent method.
type popularTitlesAccessHistory struct {
	*historypkg.Service
	reads atomic.Int32
	hook  func(string)
	path  string
}

func (h *popularTitlesAccessHistory) SnapshotPopularWatches(ctx context.Context, viewer string, users map[string]models.User, days int, at time.Time) (*historypkg.RecentWatchesSnapshot, error) {
	h.reads.Add(1)
	snapshot, err := h.Service.SnapshotPopularWatches(ctx, viewer, users, days, at)
	if err == nil && h.hook != nil {
		h.hook(viewer)
	}
	return snapshot, err
}

func popularTitlesAccessFixture(t *testing.T) (*collectionAccessFixture, *popularTitlesAccessHistory) {
	t.Helper()
	guard := &popularTitlesAccessDenyTransport{}
	previous := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = previous
		if guard.calls.Load() != 0 {
			t.Errorf("unexpected provider HTTP demand: %d", guard.calls.Load())
		}
	})
	// Install the guard before every account, metadata and history constructor.
	f := newCollectionAccessFixture(t)
	dir := t.TempDir()
	service, err := historypkg.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	history := &popularTitlesAccessHistory{Service: service, path: filepath.Join(dir, "watched_items.json")}
	if _, err := f.users.SetActivityPrivacy(f.foreign, models.ActivityPrivacyShared); err != nil {
		t.Fatal(err)
	}
	watched := true
	if _, err := service.UpdateWatchHistory(f.foreign, models.WatchHistoryUpdate{
		MediaType: "movie", ItemID: "tmdb:movie:42", Name: "Observed shared film",
		Watched: &watched, WatchedAt: time.Now().UTC().Add(-time.Hour), WatchedSeconds: 120,
	}); err != nil {
		t.Fatal(err)
	}
	f.list.SetHistoryHandler(handlers.NewHistoryHandler(history, f.users, false))
	return f, history
}

func popularTitlesAccessTarget(profile string) string {
	return "/api/users/" + url.PathEscape(profile) + "/display-list?source=popular-on-server-cached&minimumViews=1&includeUnknownOriginalLanguage=true&limit=1"
}

func popularTitlesAccessStoredState(t *testing.T, f *collectionAccessFixture, history *popularTitlesAccessHistory) []byte {
	t.Helper()
	watched, err := os.ReadFile(history.path)
	if err != nil {
		t.Fatal(err)
	}
	fingerprints := make(map[string][32]byte)
	for _, id := range []string{f.open, f.locked, f.foreign, f.kids} {
		profile, found := f.users.Get(id)
		if !found {
			t.Fatal("fixture profile missing")
		}
		fingerprints[id] = sha256.Sum256([]byte(profile.PinHash))
	}
	state, err := json.Marshal([]any{collectionAccessStoredState(t, f), watched, fingerprints})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestPopularTitlesCachedActualRegisteredAccountPINAndScope(t *testing.T) {
	f, history := popularTitlesAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open)}
		}},
		{"invalid-session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked-session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: token}
		}},
		{"expired-account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: f.login(f.expired)}
		}},
		{"foreign-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"missing-profile", 404, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget("synthetic-missing"), token: f.login(f.account1)}
		}},
		{"foreign-query-before-language", 404, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.foreign) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"locked-profile", 403, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"different-session-unlock", 403, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"known-child", 403, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.kids), token: f.login(f.account1)}
		}},
		{"master-still-child", 403, func() pinRequest {
			session, err := f.sessions.Create(f.account1, true, "synthetic", "loopback")
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.kids), token: session.Token}
		}},
		{"invalid-language", 400, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&originalLanguages=invalid", token: f.login(f.account1)}
		}},
		{"unsupported-watched-selector", 400, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&watchStatus=watched", token: f.login(f.account1)}
		}},
		{"invalid-view-threshold", 400, func() pinRequest {
			return pinRequest{method: "GET", target: strings.Replace(popularTitlesAccessTarget(f.open), "minimumViews=1", "minimumViews=0", 1), token: f.login(f.account1)}
		}},
		{"invalid-popular-window", 400, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&activityWindowDays=6", token: f.login(f.account1)}
		}},
		{"conflicting-threshold-alias", 400, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&minimumProfiles=2", token: f.login(f.account1)}
		}},
		{"matching-threshold-alias", 200, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&minimumProfiles=1", token: f.login(f.account1)}
		}},
		{"matching-profile-client-selectors", 200, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&profileId=" + url.QueryEscape(f.open) + "&userId=" + url.QueryEscape(f.open) + "&clientId=synthetic-client", token: f.login(f.account1)}
		}},
		{"maximum-popular-window", 200, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open) + "&activityWindowDays=365", token: f.login(f.account1)}
		}},
		{"own-open", 200, func() pinRequest {
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own-unlocked", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
	}
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin} {
		scope := scope
		status := 403
		if scope == models.SessionScopeJellyfin {
			status = 401 // Session validation rejects bridge tokens first.
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
			return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: session.Token}
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
		return pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: session.Token}
	}})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			req := tc.make()
			before := popularTitlesAccessStoredState(t, f, history)
			history.reads.Store(0)
			f.facts.languages.Store(0)
			w := f.do(req)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d: %s", w.Code, tc.status, w.Body.String())
			}
			if !reflect.DeepEqual(before, popularTitlesAccessStoredState(t, f, history)) {
				t.Fatal("browse changed profiles, hidden records, history or cached observation")
			}
			if tc.status != 200 {
				if history.reads.Load() != 0 || f.facts.languages.Load() != 0 {
					t.Fatal("denial reached history snapshot or language reader")
				}
				if strings.Contains(w.Body.String(), "Observed shared film") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
					t.Fatal("denial disclosed shared history")
				}
				return
			}
			var page struct {
				Source    string                `json:"source"`
				Total     int                   `json:"total"`
				Supported bool                  `json:"originalLanguageFilterSupported"`
				Items     []models.TrendingItem `json:"items"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if page.Source != "popular-on-server-cached" || !page.Supported || page.Total != 1 || len(page.Items) != 1 || page.Items[0].Title.ID != "tmdb:movie:42" || page.Items[0].Title.Name != "Observed shared film" || page.Items[0].Rank != 1 || page.Items[0].Title.CardSubtitle != "1 view" || history.reads.Load() != 1 {
				t.Fatal("owned adult browse lost the real shared-history snapshot projection")
			}
			if w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal("private history response became cacheable")
			}
		})
	}
}

func TestPopularTitlesCachedActualPINRevocationDuringReadStopsPublication(t *testing.T) {
	f, history := popularTitlesAccessFixture(t)
	token := f.login(f.account1)
	unlock := f.verify(token, f.locked, "1234").UnlockToken
	history.hook = func(profile string) { f.unlocks.RevokeProfile(profile) }
	w := f.do(pinRequest{method: "GET", target: popularTitlesAccessTarget(f.locked), token: token, unlock: unlock})
	if w.Code != 403 || history.reads.Load() != 1 || strings.Contains(w.Body.String(), "Observed shared film") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
		t.Fatalf("late PIN revocation status=%d, history reads=%d", w.Code, history.reads.Load())
	}
}

func TestPopularTitlesCachedActualContributorOptOutDuringReadStopsPublication(t *testing.T) {
	f, history := popularTitlesAccessFixture(t)
	history.hook = func(string) {
		if _, err := f.users.SetActivityPrivacy(f.foreign, models.ActivityPrivacyNotShared); err != nil {
			t.Fatal(err)
		}
	}
	w := f.do(pinRequest{method: "GET", target: popularTitlesAccessTarget(f.open), token: f.login(f.account1)})
	if w.Code != 409 || history.reads.Load() != 1 || strings.Contains(w.Body.String(), "Observed shared film") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
		t.Fatalf("late contributor opt-out status=%d history reads=%d", w.Code, history.reads.Load())
	}
}
