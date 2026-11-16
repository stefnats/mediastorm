package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	historypkg "novastream/services/history"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type watchingDenyTransport struct{ calls atomic.Int32 }

func (d *watchingDenyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("synthetic fixture denies all external HTTP")
}
func watchingTransportGuard(t *testing.T) *watchingDenyTransport {
	t.Helper()
	d := &watchingDenyTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = d
	t.Cleanup(func() {
		http.DefaultTransport = old
		if d.calls.Load() != 0 {
			t.Errorf("unexpected provider HTTP: %d", d.calls.Load())
		}
	})
	return d
}

type watchingCachedPort struct {
	historyService // Any normal builder/history/progress method is poison.
	pool           *historypkg.CachedContinueWatchingPool
	reads          int
	current        bool
	currentHook    func()
	err            error
}

func (p *watchingCachedPort) GetCachedContinueWatchingPool(user string, _ time.Time) (*historypkg.CachedContinueWatchingPool, error) {
	p.reads++
	if user != "profile" {
		return nil, errors.New("wrong profile")
	}
	if p.err != nil {
		return nil, p.err
	}
	raw, _ := json.Marshal(p.pool)
	var result historypkg.CachedContinueWatchingPool
	_ = json.Unmarshal(raw, &result)
	return &result, nil
}
func (p *watchingCachedPort) CachedContinueWatchingPoolCurrent(user, id string, _ time.Time) bool {
	if p.currentHook != nil {
		p.currentHook()
	}
	return p.current && user == "profile" && id == p.pool.ID
}

type watchingLists struct {
	customListsService
	lists []models.CustomList
	items []models.WatchlistItem
	err   error
	calls int
	hook  func(int)
}

func (l *watchingLists) ListLists(string) ([]models.CustomList, error) {
	l.calls++
	if l.hook != nil {
		l.hook(l.calls)
	}
	return l.lists, l.err
}
func (l *watchingLists) ListItems(string, string) ([]models.WatchlistItem, error) {
	return l.items, l.err
}

type watchingFixture struct {
	base  *lovedStoredFixture
	port  *watchingCachedPort
	lists *watchingLists
}

func newWatchingFixture(t *testing.T, n int) *watchingFixture {
	t.Helper()
	watchingTransportGuard(t) // Before every actual service/handler constructor.
	base := newLovedStoredFixture(t)
	at := time.Now().UTC().Add(-time.Second)
	pool := &historypkg.CachedContinueWatchingPool{ID: strings.Repeat("a", 32), ObservedAt: at, ExpiresAt: at.Add(10 * time.Minute), Complete: true, Items: []historypkg.CachedContinueWatchingItem{}}
	for i := 1; i <= n; i++ {
		pool.Items = append(pool.Items, historypkg.CachedContinueWatchingItem{SeriesID: fmt.Sprintf("tmdb:movie:%d", i), SeriesTitle: fmt.Sprintf("Observed %04d", i), MediaType: "movie", Year: 2000, UpdatedAt: at, ExternalIDs: map[string]string{"tmdb": fmt.Sprint(i)}})
	}
	port := &watchingCachedPort{pool: pool, current: true}
	lists := &watchingLists{}
	base.display.SetHistoryHandler(NewHistoryHandler(port, nil, false))
	base.display.CustomListsService = lists
	return &watchingFixture{base, port, lists}
}
func watchingRequest(query string) *http.Request {
	r := lovedStoredRequest("")
	r.URL.RawQuery = "source=continue-watching-cached&" + query
	return r
}
func watchingHTTP(f *watchingFixture, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.base.display.Get(w, watchingRequest(query))
	return w
}
func watchingPage(t *testing.T, w *httptest.ResponseRecorder) (int, []historypkg.CachedContinueWatchingItem) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var response struct {
		Source    string
		Total     int
		Items     []historypkg.CachedContinueWatchingItem
		Supported bool `json:"originalLanguageFilterSupported"`
	}
	if json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Source != "continue-watching-cached" || !response.Supported {
		t.Fatal("invalid wrapper")
	}
	return response.Total, response.Items
}
func (f *watchingFixture) fact(id, kind, lang string) {
	f.base.cache.facts[kind+":"+id] = &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: strings.Replace(id, "tmdb:tv:", "tmdb:series:", 1), ObservedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour)}
}
func TestCachedWatchingLanguageBeforePagingPastLegacyDiscoveryCap(t *testing.T) {
	f := newWatchingFixture(t, 701)
	f.fact("tmdb:movie:650", "movie", "fra")
	f.fact("tmdb:movie:651", "movie", "nob")
	total, rows := watchingPage(t, watchingHTTP(f, "originalLanguages=fra,nob&sortBy=name&sortDirection=asc&limit=1"))
	if total != 2 || len(rows) != 1 || rows[0].SeriesID != "tmdb:movie:650" {
		t.Fatal("late language filter lost before paging")
	}
	_, rows = watchingPage(t, watchingHTTP(f, "originalLanguages=fra,nob&sortBy=name&sortDirection=asc&limit=1&offset=1"))
	if len(rows) != 1 || rows[0].SeriesID != "tmdb:movie:651" {
		t.Fatal("continuation lost")
	}
	total, rows = watchingPage(t, watchingHTTP(f, "includeUnknownOriginalLanguage=true&limit=0"))
	if total != 699 || len(rows) != 699 {
		t.Fatal("unknown or full pool capped")
	}
	total, _ = watchingPage(t, watchingHTTP(f, "originalLanguages=fra&includeUnknownOriginalLanguage=true&limit=0"))
	if total != 700 {
		t.Fatal("unknown OR failed")
	}
}
func TestCachedWatchingCurrentHiddenAndSetAsidePrecedeTotal(t *testing.T) {
	f := newWatchingFixture(t, 3)
	_, err := f.base.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Hidden"})
	if err != nil {
		t.Fatal(err)
	}
	f.lists.lists = []models.CustomList{{ID: "aside", Name: " Set Aside "}}
	f.lists.items = []models.WatchlistItem{{ID: "other", MediaType: "movie", ExternalIDs: map[string]string{"tmdb": "2"}}}
	total, rows := watchingPage(t, watchingHTTP(f, "limit=1"))
	if total != 1 || rows[0].SeriesID != "tmdb:movie:3" {
		t.Fatal("suppression after paging")
	}
}
func TestCachedWatchingUsesShowLanguageWithoutChangingEpisodeTargets(t *testing.T) {
	f := newWatchingFixture(t, 0)
	ep := &historypkg.CachedContinueWatchingEpisode{SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:9", ItemID: "tmdb:tv:7:s01e02"}
	f.port.pool.Items = []historypkg.CachedContinueWatchingItem{{SeriesID: "tmdb:tv:7", SeriesTitle: "Observed show", MediaType: "series", UpdatedAt: f.port.pool.ObservedAt, NextEpisode: ep}}
	f.fact("tmdb:tv:7", "series", "fra")
	f.fact("tvdb:episode:9", "episode", "deu")
	total, rows := watchingPage(t, watchingHTTP(f, "originalLanguages=fra"))
	if total != 1 || !reflect.DeepEqual(rows[0].NextEpisode, ep) {
		t.Fatal("show language or episode identity changed")
	}
	total, _ = watchingPage(t, watchingHTTP(f, "originalLanguages=deu"))
	if total != 0 {
		t.Fatal("episode language used as show language")
	}
}
func TestCachedWatchingMissingExpiredAndRevisionLostNeverFallback(t *testing.T) {
	for _, name := range []string{"missing", "expired", "revision"} {
		t.Run(name, func(t *testing.T) {
			f := newWatchingFixture(t, 1)
			switch name {
			case "missing":
				f.port.err = errors.New("missing")
			case "expired":
				f.port.pool.ExpiresAt = time.Now().Add(-time.Second)
			case "revision":
				f.port.current = false
			}
			w := watchingHTTP(f, "")
			if w.Code != 409 || !strings.Contains(w.Body.String(), "continue_watching_snapshot_unavailable") {
				t.Fatalf("unexpected %d", w.Code)
			}
		})
	}
}
func TestCachedWatchingKnownAdultOwnerScopeAndPINBeforeCache(t *testing.T) {
	for _, name := range []string{"child", "foreign", "missing-profile", "stream", "pin"} {
		t.Run(name, func(t *testing.T) {
			f := newWatchingFixture(t, 1)
			r := watchingRequest("")
			want := 403
			switch name {
			case "child":
				f.base.meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "account", IsKidsProfile: true}}
			case "foreign":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "other"))
				want = 404
			case "missing-profile":
				f.base.meta.UsersService = fallbackProfiles{}
				want = 404
			case "stream":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
			case "pin":
				r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, _ *http.Request, _ string) bool { w.WriteHeader(403); return false })
			}
			w := httptest.NewRecorder()
			profileaccess.Middleware(func(id, account string) bool { return id == "profile" && account == "account" })(http.HandlerFunc(f.base.display.Get)).ServeHTTP(w, r)
			if w.Code != want || f.port.reads != 0 {
				t.Fatalf("guard %s status%d cache%d", name, w.Code, f.port.reads)
			}
		})
	}
}
func TestCachedWatchingSuppressionReadFailureOrLateReplacementFailsClosed(t *testing.T) {
	for _, name := range []string{"read-error", "late-aside", "late-owner", "late-pool"} {
		t.Run(name, func(t *testing.T) {
			f := newWatchingFixture(t, 1)
			want := 409
			switch name {
			case "read-error":
				f.lists.err = errors.New("synthetic suppression failure")
				want = 503
			case "late-aside":
				f.lists.hook = func(n int) {
					if n == 2 {
						f.lists.lists = []models.CustomList{{ID: "aside", Name: "Set Aside"}}
						f.lists.items = []models.WatchlistItem{{ID: "tmdb:movie:1", MediaType: "movie"}}
					}
				}
			case "late-owner":
				f.lists.hook = func(n int) {
					if n == 2 {
						f.base.meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "other"}}
					}
				}
			case "late-pool":
				f.lists.hook = func(n int) {
					if n == 1 {
						f.port.current = false
					}
				}
			}
			w := watchingHTTP(f, "")
			if w.Code != want {
				t.Fatalf("status%d", w.Code)
			}
		})
	}
}
func TestCachedWatchingUnsupportedQueriesRejectBeforeReader(t *testing.T) {
	for _, q := range []string{"sortBy=added", "sortBy=duration", "sortBy=rating", "watchStatus=watched", "genres=Drama", "includeFacets=true", "includeUnwatchedCounts=true", "runtimeMin=10", "originalLanguages=zzz", "sortBy=name&sortBy=rating", "limit=abc", "limit=-1", "offset=01", "limit=1&limit=2", "includeFacets=false&includeFacets=true", "runtimeMin=10", "unknownRequirement=true"} {
		t.Run(q, func(t *testing.T) {
			f := newWatchingFixture(t, 1)
			w := watchingHTTP(f, q)
			if w.Code != 400 || f.port.reads != 0 {
				t.Fatalf("query status%d reads%d", w.Code, f.port.reads)
			}
		})
	}
}

type watchingPublicationRecorder struct {
	*httptest.ResponseRecorder
	writes, headers int
}

func (w *watchingPublicationRecorder) Write(b []byte) (int, error) {
	w.writes++
	return w.ResponseRecorder.Write(b)
}
func (w *watchingPublicationRecorder) WriteHeader(status int) {
	w.headers++
	w.ResponseRecorder.WriteHeader(status)
}
func TestCachedWatchingLateCallbackCancellationCannotPublish(t *testing.T) {
	for _, stage := range []string{"suppression", "pool-current"} {
		t.Run(stage, func(t *testing.T) {
			f := newWatchingFixture(t, 1)
			r := watchingRequest("")
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			if stage == "suppression" {
				f.lists.hook = func(n int) {
					if n == 2 {
						cancel()
					}
				}
			} else {
				f.port.currentHook = cancel
			}
			w := &watchingPublicationRecorder{ResponseRecorder: httptest.NewRecorder()}
			f.base.display.Get(w, r)
			if w.writes != 0 || w.headers != 0 || w.Body.Len() != 0 {
				t.Fatal("cancelled cached request published a response")
			}
		})
	}
}
