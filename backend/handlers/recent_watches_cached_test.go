package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
	historypkg "novastream/services/history"
)

type recentCachedProfiles struct {
	mu          sync.Mutex
	items       map[string]models.User
	gets, lists int
}

func (p *recentCachedProfiles) Get(id string) (models.User, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gets++
	u, ok := p.items[id]
	return u, ok
}
func (p *recentCachedProfiles) ListAll() []models.User {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lists++
	out := make([]models.User, 0, len(p.items))
	for _, u := range p.items {
		out = append(out, u)
	}
	return out
}
func (p *recentCachedProfiles) change(id string, change func(*models.User)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	u := p.items[id]
	change(&u)
	p.items[id] = u
}

type recentCachedHistory struct {
	*historypkg.Service
	reads, watchReads int
	afterSnapshot     func()
	beforeCurrent     func()
	err               error
}

func (s *recentCachedHistory) SnapshotRecentWatches(ctx context.Context, viewer string, users map[string]models.User, window int, at time.Time) (*historypkg.RecentWatchesSnapshot, error) {
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	v, err := s.Service.SnapshotRecentWatches(ctx, viewer, users, window, at)
	if s.afterSnapshot != nil {
		s.afterSnapshot()
	}
	return v, err
}
func (s *recentCachedHistory) RecentWatchesSnapshotCurrent(v *historypkg.RecentWatchesSnapshot) bool {
	if s.beforeCurrent != nil {
		s.beforeCurrent()
	}
	return s.Service.RecentWatchesSnapshotCurrent(v)
}
func (s *recentCachedHistory) GetWatchHistoryItem(viewer, kind, id string) (*models.WatchHistoryItem, error) {
	s.watchReads++
	return s.Service.GetWatchHistoryItem(viewer, kind, id)
}

// Normal list/enrichment paths deliberately panic rather than falling back.
func (*recentCachedHistory) ListWatchHistory(string) ([]models.WatchHistoryItem, error) {
	panic("normal history list must not run")
}
func (*recentCachedHistory) ListContinueWatching(string) ([]models.SeriesWatchState, error) {
	panic("normal watching must not run")
}

type recentCachedFixture struct {
	*lovedStoredFixture
	profiles *recentCachedProfiles
	history  *recentCachedHistory
	dir      string
}

func newRecentCachedFixture(t *testing.T, rows map[string][]models.WatchHistoryItem) *recentCachedFixture {
	t.Helper()
	watchingTransportGuard(t) // Installed before constructors, including real cache readers.
	f := newLovedStoredFixture(t)
	profiles := &recentCachedProfiles{items: map[string]models.User{
		"profile":   {ID: "profile", AccountID: "account", Name: "Viewer", ActivityPrivacy: models.ActivityPrivacyNotShared},
		"alice":     {ID: "alice", AccountID: "other-account", Name: "Alice", ActivityPrivacy: models.ActivityPrivacyShared, CreatedAt: time.Now().Add(-time.Hour)},
		"anonymous": {ID: "anonymous", AccountID: "private-owner", Name: "PRIVATE_ANONYMOUS_NAME", ActivityPrivacy: models.ActivityPrivacySharedAnonymous},
		"private":   {ID: "private", AccountID: "private-owner", Name: "PRIVATE_NONSHARED_NAME", ActivityPrivacy: models.ActivityPrivacyNotShared},
	}}
	dir := t.TempDir()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "watched_items.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	svc, err := historypkg.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	port := &recentCachedHistory{Service: svc}
	f.meta.UsersService = profiles
	f.display.HistoryHandler = NewHistoryHandler(port, nil, false)
	return &recentCachedFixture{f, profiles, port, dir}
}
func recentCachedMovie(id, name string, at time.Time) models.WatchHistoryItem {
	return models.WatchHistoryItem{ID: "movie:" + id, MediaType: "movie", ItemID: id, Name: name, Watched: true, WatchedAt: at, UpdatedAt: at, WatchedSeconds: 30, Source: models.WatchSourcePlayed}
}
func (f *recentCachedFixture) fact(id, kind, language string) {
	f.cache.facts[kind+":"+id] = &models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: id, ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
}
func recentCachedRequest(query string) *http.Request {
	r := httptest.NewRequest("GET", "/api/users/profile/display-list?source=recently-watched-cached&"+query, nil)
	r.Header.Set("X-Client-ID", "synthetic-client")
	r = mux.SetURLVars(r, map[string]string{"userID": "profile"})
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
}
func recentCachedHTTP(f *recentCachedFixture, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.display.Get(w, recentCachedRequest(query))
	return w
}

type recentCachedReply struct {
	Source                          string                `json:"source"`
	Items                           []models.TrendingItem `json:"items"`
	Total                           int                   `json:"total"`
	AlphabetBuckets                 []string              `json:"alphabetBuckets"`
	OriginalLanguageFilterSupported bool                  `json:"originalLanguageFilterSupported"`
}

func decodeRecentCached(t *testing.T, w *httptest.ResponseRecorder) recentCachedReply {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("missing private response policy")
	}
	var v recentCachedReply
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Source != "recently-watched-cached" || !v.OriginalLanguageFilterSupported || v.Items == nil {
		t.Fatalf("incomplete cached contract: %+v", v)
	}
	return v
}
func recentCachedIDs(v recentCachedReply) []string {
	out := []string{}
	for _, item := range v.Items {
		out = append(out, item.Title.ID)
	}
	return out
}
func assertRecentCachedStatus(t *testing.T, f *recentCachedFixture, query string, want int) {
	t.Helper()
	w := recentCachedHTTP(f, query)
	if w.Code != want {
		t.Fatalf("status=%d want=%d body=%s", w.Code, want, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("missing private response policy on failure")
	}
	if strings.Contains(w.Body.String(), `"items"`) {
		t.Fatal("failure published cards")
	}
}

func TestRecentWatchesCachedFiltersCompleteHistoryBeforeContributorCapAndPage(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	rows := []models.WatchHistoryItem{}
	for i := 1; i <= 701; i++ {
		rows = append(rows, recentCachedMovie(fmt.Sprintf("tmdb:movie:%d", i), fmt.Sprintf("Title %03d", i), now.Add(-time.Duration(i)*time.Minute)))
	}
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": rows})
	for i := 1; i <= 701; i++ {
		lang := "eng"
		if i == 650 || i == 653 {
			lang = "fra"
		}
		f.fact(fmt.Sprintf("tmdb:movie:%d", i), "movie", lang)
	}
	for _, tc := range []struct {
		query string
		total int
		ids   []string
		rank  int
	}{
		{"originalLanguages=fra&maxItemsPerProfile=2&limit=1", 2, []string{"tmdb:movie:650"}, 1},
		{"originalLanguages=fra&maxItemsPerProfile=2&limit=1&offset=1", 2, []string{"tmdb:movie:653"}, 2},
		{"originalLanguages=fra&maxItemsPerProfile=2&offset=2", 2, []string{}, 0},
		{"originalLanguages=fra&maxItemsPerProfile=1", 1, []string{"tmdb:movie:650"}, 1},
		{"originalLanguages=fra&maxItemsPerProfile=2&sortBy=name&sortDirection=desc", 2, []string{"tmdb:movie:653", "tmdb:movie:650"}, 1},
		{"originalLanguages=fra&maxItemsPerProfile=2&sortDirection=asc", 2, []string{"tmdb:movie:653", "tmdb:movie:650"}, 1},
		{"originalLanguages=fra&titleFilter=653&maxItemsPerProfile=1", 1, []string{"tmdb:movie:653"}, 1},
		{"originalLanguages=fra&limit=2000000&offset=2000000", 2, []string{}, 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			v := decodeRecentCached(t, recentCachedHTTP(f, tc.query))
			if v.Total != tc.total || !reflect.DeepEqual(recentCachedIDs(v), tc.ids) {
				t.Fatalf("wrong deep page: %+v", v)
			}
			if tc.rank > 0 && v.Items[0].Rank != tc.rank {
				t.Fatal("page rank was reset")
			}
		})
	}
	if f.repo.calls.Load() != 0 || f.repo.writes.Load() != 0 || f.ratings.observes.Load() != 0 {
		t.Fatal("cached browse touched ratings/enrichment")
	}
}

func TestRecentWatchesCachedNoGlobalDiscoveryTruncation(t *testing.T) {
	rows := map[string][]models.WatchHistoryItem{}
	now := time.Now().Add(-time.Minute)
	for i := 1; i <= 601; i++ {
		rows[fmt.Sprintf("shared-%03d", i)] = []models.WatchHistoryItem{recentCachedMovie(fmt.Sprintf("tmdb:movie:%d", i), fmt.Sprintf("Film %03d", i), now)}
	}
	f := newRecentCachedFixture(t, rows)
	for user := range rows {
		f.profiles.items[user] = models.User{ID: user, Name: "Shared", ActivityPrivacy: models.ActivityPrivacyShared}
	}
	v := decodeRecentCached(t, recentCachedHTTP(f, "sortBy=name&maxItemsPerProfile=1&limit=10&offset=550"))
	if v.Total != 601 || len(v.Items) != 10 || v.Items[0].Title.ID != "tmdb:movie:551" || v.Items[0].Rank != 551 {
		t.Fatalf("complete finite shelf was truncated: %+v", v)
	}
}

func TestRecentWatchesCachedAlphabetBeforeCapWithIndependentFacetPool(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {
		recentCachedMovie("tmdb:movie:1", "Alpha", now), recentCachedMovie("tmdb:movie:2", "Bravo", now.Add(-time.Minute)), recentCachedMovie("tmdb:movie:3", "Beta", now.Add(-2*time.Minute)),
	}})
	for _, cap := range []int{1, 2} {
		v := decodeRecentCached(t, recentCachedHTTP(f, fmt.Sprintf("alphabet=B&maxItemsPerProfile=%d&sortBy=name", cap)))
		ids := []string{"tmdb:movie:2"}
		if cap == 2 {
			ids = []string{"tmdb:movie:3", "tmdb:movie:2"}
		}
		if v.Total != cap || !reflect.DeepEqual(recentCachedIDs(v), ids) || !reflect.DeepEqual(v.AlphabetBuckets, []string{"A", "B"}) {
			t.Fatalf("alphabet selection/facets lost eligible rows: %+v", v)
		}
	}
}

func TestRecentWatchesCachedHiddenAndViewerWatchedBeforeCap(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{
		"alice":   {recentCachedMovie("tmdb:movie:1", "Hidden", now), recentCachedMovie("tmdb:movie:2", "Watched", now.Add(-time.Minute)), recentCachedMovie("tmdb:movie:3", "Promoted", now.Add(-2*time.Minute))},
		"profile": {recentCachedMovie("tmdb:movie:2", "Viewer history", now)},
	})
	if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "stored-hidden", MediaType: "movie", ExternalIDs: map[string]string{"tmdb": "1"}}); err != nil {
		t.Fatal(err)
	}
	v := decodeRecentCached(t, recentCachedHTTP(f, "hideWatched=true&maxItemsPerProfile=1"))
	if v.Total != 1 || v.Items[0].Title.ID != "tmdb:movie:3" || f.history.watchReads != 2 {
		t.Fatalf("visibility was applied after cap: %+v", v)
	}
}

func TestRecentWatchesCachedSharedAnonymousPrivateAndPrimaryProjection(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	a := recentCachedMovie("tmdb:movie:1", "Shared", now)
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {a}, "anonymous": {recentCachedMovie("tmdb:movie:2", "Anonymous film", now)}, "private": {recentCachedMovie("tmdb:movie:3", "PRIVATE_FILM", now)}, "profile": {recentCachedMovie("tmdb:movie:4", "PRIVATE_VIEWER_FILM", now)}})
	w := recentCachedHTTP(f, "")
	v := decodeRecentCached(t, w)
	if v.Total != 2 || !reflect.DeepEqual(recentCachedIDs(v), []string{"tmdb:movie:1", "tmdb:movie:2"}) {
		t.Fatalf("wrong shared pool: %+v", v)
	}
	for _, item := range v.Items {
		if item.Title.ID == "tmdb:movie:1" && (item.Title.TMDBID != 1 || item.Title.IMDBID != "" || item.Title.TVDBID != 0 || item.Title.CardSubtitle != "Alice watched") {
			t.Fatalf("unbound saved side IDs leaked: %+v", item)
		}
		if item.Title.ID == "tmdb:movie:2" && item.Title.CardSubtitle != "Fellow user watched" {
			t.Fatal("anonymous attribution changed")
		}
	}
	for _, secret := range []string{"PRIVATE_", "private-owner", `"group"`, `"userId"`, `"watch"`, `"revisions"`, `"PinHash"`, "tt9999999"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("private snapshot field escaped: %s", secret)
		}
	}
}

func TestRecentWatchesCachedStrictSelectorsBeforeAnyReaders(t *testing.T) {
	for _, query := range []string{"source=recently-watched", "source=recently-watched-cached", "originalLanguages=not-a-language", "originalLanguages=fra&originalLanguages=eng", "includeUnknownOriginalLanguage=1", "limit=-1", "limit=01", "offset=1.0", "offset=999999999999999999999999999", "activityWindowDays=0", "activityWindowDays=91", "maxItemsPerProfile=0", "maxItemsPerProfile=21", "hideWatched=1", "includeFacets=true", "includeUnwatchedCounts=true", "hideUnreleased=true", "watchStatus=watched", "genres=Drama", "ratingSource=imdb", "sortBy=rating", "sortDirection=sideways", "filterMediaType=episode", "alphabet=AB", "homeView=movies&filterMediaType=series", "homeView=unknown", "profileId=foreign", "userId=foreign", "unknown=value", "limit=1&limit=2", "titleFilter=%zz"} {
		t.Run(query, func(t *testing.T) {
			f := newRecentCachedFixture(t, nil)
			assertRecentCachedStatus(t, f, query, 400)
			if f.profiles.gets != 0 || f.profiles.lists != 0 || f.history.reads != 0 || f.cache.calls.Load() != 0 {
				t.Fatal("invalid selector consulted readers")
			}
		})
	}
}

func TestRecentWatchesCachedKeepsMatchingClientProfileTransport(t *testing.T) {
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:2", "Legacy", time.Now().Add(-time.Minute))}})
	v := decodeRecentCached(t, recentCachedHTTP(f, "profileId=profile&userId=profile&clientId=fixture&token=synthetic&profileUnlock=synthetic&homeView=movies"))
	if v.Total != 1 || v.Items[0].Title.ID != "tmdb:2" || v.Items[0].Title.TMDBID != 2 {
		t.Fatal("existing transport or legacy primary alias was lost")
	}
}

func TestRecentWatchesCachedRejectsChangedContributorFacts(t *testing.T) {
	for _, change := range []string{"privacy", "name", "owner", "birth", "new-opt-in"} {
		t.Run(change, func(t *testing.T) {
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:movie:1", "Film", time.Now().Add(-time.Minute))}})
			f.history.afterSnapshot = func() {
				id := "alice"
				if change == "new-opt-in" {
					id = "private"
				}
				f.profiles.change(id, func(u *models.User) {
					switch change {
					case "privacy":
						u.ActivityPrivacy = models.ActivityPrivacyNotShared
					case "name":
						u.Name = "Changed"
					case "owner":
						u.AccountID = "Changed"
					case "birth":
						u.CreatedAt = time.Now()
					case "new-opt-in":
						u.ActivityPrivacy = models.ActivityPrivacyShared
					}
				})
			}
			assertRecentCachedStatus(t, f, "", 409)
		})
	}
}

func TestRecentWatchesCachedRejectsChangedViewerAndHiddenState(t *testing.T) {
	for _, change := range []string{"viewer-owner", "viewer-pin", "viewer-kids", "hidden"} {
		t.Run(change, func(t *testing.T) {
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:movie:1", "Film", time.Now().Add(-time.Minute))}})
			f.history.afterSnapshot = func() {
				if change == "hidden" {
					return
				}
				f.profiles.change("profile", func(u *models.User) {
					switch change {
					case "viewer-owner":
						u.AccountID = "changed"
					case "viewer-pin":
						u.PinHash = "synthetic-changed-stamp"
					case "viewer-kids":
						u.IsKidsProfile = true
					}
				})
			}
			if change == "hidden" {
				f.cache.reader = recentCachedLanguageReader(func(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
					if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie"}); err != nil {
						t.Fatal(err)
					}
					return nil
				})
			}
			query := ""
			if change == "hidden" {
				query = "includeUnknownOriginalLanguage=true"
			}
			assertRecentCachedStatus(t, f, query, 409)
		})
	}
}

type recentCachedLanguageReader func(string, string, time.Time) *models.OriginalLanguageEvidence

func (f recentCachedLanguageReader) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	return f(id, kind, at)
}

func TestRecentWatchesCachedRejectsChangedCacheMembershipEvenOutsidePage(t *testing.T) {
	for _, change := range []string{"accepted-expires", "excluded-becomes-match"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:movie:1", "First", now), recentCachedMovie("tmdb:movie:2", "Second", now.Add(-time.Minute))}})
			calls := map[string]int{}
			f.cache.reader = recentCachedLanguageReader(func(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
				calls[id]++
				lang := "fra"
				if id == "tmdb:movie:2" {
					if change == "accepted-expires" && calls[id] > 1 {
						return nil
					}
					if change == "excluded-becomes-match" && calls[id] == 1 {
						lang = "eng"
					}
				}
				return &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: at.Add(-time.Hour), ExpiresAt: at.Add(time.Hour)}
			})
			assertRecentCachedStatus(t, f, "originalLanguages=fra&maxItemsPerProfile=1&limit=1", 409)
		})
	}
}

func TestRecentWatchesCachedRejectsSameServiceHistoryChanges(t *testing.T) {
	for _, viewer := range []bool{false, true} {
		t.Run(fmt.Sprintf("viewer=%t", viewer), func(t *testing.T) {
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:movie:1", "Film", time.Now().Add(-time.Minute))}})
			f.history.beforeCurrent = func() {
				user := "alice"
				if viewer {
					user = "profile"
				}
				watched := true
				if _, err := f.history.Service.UpdateWatchHistory(user, models.WatchHistoryUpdate{MediaType: "movie", ItemID: "tmdb:movie:999", Name: "New history", Watched: &watched, WatchedSeconds: 30, Source: models.WatchSourcePlayed}); err != nil {
					t.Fatal(err)
				}
			}
			assertRecentCachedStatus(t, f, "hideWatched=true", 409)
		})
	}
}

func TestRecentWatchesCachedUnavailableReadersAndKids(t *testing.T) {
	for _, condition := range []string{"no-history", "legacy-history-only", "no-metadata", "no-hidden", "snapshot-error", "kids", "foreign-owner", "missing-viewer"} {
		t.Run(condition, func(t *testing.T) {
			f := newRecentCachedFixture(t, nil)
			want := 503
			switch condition {
			case "no-history":
				f.display.HistoryHandler = nil
			case "legacy-history-only":
				f.display.HistoryHandler.Service = &lovedStoredHistoryPoison{}
			case "no-metadata":
				f.meta.Service = nil
			case "no-hidden":
				f.display.HiddenItemsService = nil
			case "snapshot-error":
				f.history.err = errors.New("synthetic")
				want = 409
			case "kids":
				f.profiles.change("profile", func(u *models.User) { u.IsKidsProfile = true })
				want = 403
			case "foreign-owner":
				f.profiles.change("profile", func(u *models.User) { u.AccountID = "foreign" })
				want = 404
			case "missing-viewer":
				delete(f.profiles.items, "profile")
				want = 404
			}
			assertRecentCachedStatus(t, f, "", want)
		})
	}
}

func TestRecentWatchesCachedCancellationStopsWithoutPublishing(t *testing.T) {
	f := newRecentCachedFixture(t, nil)
	r := recentCachedRequest("")
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	w := httptest.NewRecorder()
	f.display.Get(w, r.WithContext(ctx))
	if w.Body.Len() != 0 || f.history.reads != 0 {
		t.Fatal("cancelled request read or published history")
	}
	r = recentCachedRequest("")
	ctx, cancel = context.WithCancel(r.Context())
	f.history.afterSnapshot = cancel
	w = httptest.NewRecorder()
	f.display.Get(w, r.WithContext(ctx))
	if w.Body.Len() != 0 {
		t.Fatal("cancelled snapshot published cards")
	}
}

func TestRecentWatchesCachedPureSettingsFallbackAndExplicitCap(t *testing.T) {
	for _, tc := range []struct {
		name, body, query string
		cap               int
		absent, directory bool
	}{
		{"configured", `{"homeShelves":{"recentlyWatchedCapPerProfile":2}}`, "", 2, false, false},
		{"absent", "", "", 3, true, false},
		{"malformed", "{ invalid", "", 3, false, false},
		{"invalid-type", `{"homeShelves":{"recentlyWatchedCapPerProfile":"2"}}`, "", 3, false, false},
		{"out-of-range", `{"homeShelves":{"recentlyWatchedCapPerProfile":99}}`, "", 3, false, false},
		{"trailing", `{"homeShelves":{"recentlyWatchedCapPerProfile":1}} {}`, "", 3, false, false},
		{"unreadable", "", "", 3, false, true},
		{"override", `{"homeShelves":{"recentlyWatchedCapPerProfile":2}}`, "maxItemsPerProfile=1", 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			rows := []models.WatchHistoryItem{}
			for i := 1; i <= 5; i++ {
				rows = append(rows, recentCachedMovie(fmt.Sprintf("tmdb:movie:%d", i), "Film", now.Add(-time.Duration(i)*time.Minute)))
			}
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": rows})
			path := f.meta.CfgManager.ConfigPath()
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if tc.directory {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if !tc.absent {
				if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			v := decodeRecentCached(t, recentCachedHTTP(f, tc.query))
			if v.Total != tc.cap {
				t.Fatalf("cap=%d want=%d", v.Total, tc.cap)
			}
			if tc.absent {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("read endpoint created settings")
				}
			} else if !tc.directory {
				raw, err := os.ReadFile(path)
				if err != nil || string(raw) != tc.body {
					t.Fatal("read endpoint rewrote settings")
				}
			}
		})
	}
}

func TestRecentWatchesCachedPrimaryIdentityAndEpisodeConsistency(t *testing.T) {
	for _, tc := range []struct {
		name    string
		watch   models.RecentWatch
		valid   bool
		id      string
		numeric int64
	}{
		{"movie", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:movie:42"}, true, "tmdb:movie:42", 42},
		{"legacy-movie", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:42"}, true, "tmdb:42", 42},
		{"stored-series", models.RecentWatch{MediaType: "series", ItemID: "tmdb:series:42", Name: "Stored series"}, true, "tmdb:series:42", 42},
		{"legacy-tvdb", models.RecentWatch{MediaType: "episode", ItemID: "tvdb:42:s01e01", SeriesID: "tvdb:42", SeasonNumber: 1, EpisodeNumber: 1}, true, "tvdb:42", 42},
		{"legacy-tv", models.RecentWatch{MediaType: "episode", ItemID: "tmdb:tv:82728:s01e01", SeriesID: "tmdb:tv:82728", SeasonNumber: 1, EpisodeNumber: 1}, true, "tmdb:tv:82728", 82728},
		{"series-alias", models.RecentWatch{MediaType: "episode", ItemID: "tmdb:tv:42:s01e01", SeriesID: "tmdb:series:42", SeasonNumber: 1, EpisodeNumber: 1}, true, "tmdb:series:42", 42},
		{"imdb", models.RecentWatch{MediaType: "movie", ItemID: "imdb:tt0000042"}, true, "imdb:tt0000042", 0},
		{"wrong-series", models.RecentWatch{MediaType: "episode", ItemID: "tmdb:series:99:s01e01", SeriesID: "tmdb:series:42", SeasonNumber: 1, EpisodeNumber: 1}, false, "", 0},
		{"wrong-season", models.RecentWatch{MediaType: "episode", ItemID: "tmdb:series:42:s02e01", SeriesID: "tmdb:series:42", SeasonNumber: 1, EpisodeNumber: 1}, false, "", 0},
		{"wrong-episode", models.RecentWatch{MediaType: "episode", ItemID: "tmdb:series:42:s01e02", SeriesID: "tmdb:series:42", SeasonNumber: 1, EpisodeNumber: 1}, false, "", 0},
		{"negative-season", models.RecentWatch{MediaType: "episode", ItemID: "stored-episode", SeriesID: "tmdb:series:42", SeasonNumber: -1}, false, "", 0},
		{"wrong-kind", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:series:42"}, false, "", 0},
		{"movie-with-episode", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:movie:42:s01e01"}, false, "", 0},
		{"raw-path", models.RecentWatch{MediaType: "movie", ItemID: "/private/film.mkv"}, false, "", 0},
		{"credential-url", models.RecentWatch{MediaType: "movie", ItemID: "https://synthetic:synthetic@example.invalid/file"}, false, "", 0},
		{"opaque", models.RecentWatch{MediaType: "movie", ItemID: "unbound-saved-id", ExternalIDs: map[string]string{"tmdb": "42"}}, false, "", 0},
		{"malformed-imdb", models.RecentWatch{MediaType: "movie", ItemID: "imdb:not-an-id"}, false, "", 0},
		{"noncanonical-number", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:movie:0042"}, false, "", 0},
		{"zero", models.RecentWatch{MediaType: "movie", ItemID: "tmdb:movie:0"}, false, "", 0},
		{"whitespace", models.RecentWatch{MediaType: "movie", ItemID: " tmdb:movie:42"}, false, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Imported side IDs must not become authority in the projection. Put
			// this poison on the RecentWatch itself: persisted history performs
			// its existing identity migration before a snapshot is constructed.
			tc.watch.ExternalIDs = map[string]string{"tmdb": "999", "tvdb": "998", "imdb": "tt9999999"}
			title, ok := cachedRecentTitle(tc.watch)
			if ok != tc.valid || ok && (title.ID != tc.id || title.TMDBID+title.TVDBID != tc.numeric || title.IMDBID == "tt9999999") {
				t.Fatalf("unexpected projection: %+v valid=%t", title, ok)
			}
		})
	}
}

func TestRecentWatchesCachedHiddenPrimaryAliasesBeforeContributorCap(t *testing.T) {
	for _, tc := range []struct {
		name, kind, cardID, hiddenID string
	}{
		{"movie-canonical-marker", "movie", "tmdb:42", "tmdb:movie:42"},
		{"movie-legacy-marker", "movie", "tmdb:movie:42", "tmdb:42"},
		{"series-tv-marker", "series", "tmdb:series:42", "tmdb:tv:42"},
		{"series-canonical-marker", "series", "tmdb:tv:42", "tmdb:series:42"},
		{"series-short-marker", "series", "tmdb:series:42", "tmdb:42"},
		{"series-short-card", "series", "tmdb:42", "tmdb:series:42"},
		{"tvdb-canonical-marker", "series", "tvdb:42", "tvdb:series:42"},
		{"tvdb-short-marker", "series", "tvdb:series:42", "tvdb:42"},
		{"imdb-prefixed-marker", "movie", "tt0000042", "imdb:tt0000042"},
		{"imdb-bare-marker", "movie", "imdb:tt0000042", "tt0000042"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			newest := recentCachedMovie(tc.cardID, "Hidden newest", now)
			olderID := "tmdb:" + tc.kind + ":43"
			older := recentCachedMovie(olderID, "Older eligible", now.Add(-time.Minute))
			newest.MediaType, older.MediaType = tc.kind, tc.kind
			newest.ID, older.ID = tc.kind+":"+newest.ItemID, tc.kind+":"+older.ItemID
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {newest, older}})
			if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: tc.hiddenID, MediaType: tc.kind}); err != nil {
				t.Fatal(err)
			}
			before, err := f.hidden.List("profile")
			if err != nil {
				t.Fatal(err)
			}
			v := decodeRecentCached(t, recentCachedHTTP(f, "maxItemsPerProfile=1&filterMediaType="+tc.kind))
			if v.Total != 1 || !reflect.DeepEqual(recentCachedIDs(v), []string{olderID}) {
				t.Fatalf("hidden primary alias consumed the contributor cap: %+v", v)
			}
			after, err := f.hidden.List("profile")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("browse mutated hidden markers")
			}
		})
	}
}

func TestRecentWatchesCachedHiddenPrimaryTokensKeepProviderAndKindBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, hiddenID, hiddenKind, cardID, cardKind string }{
		{"different-provider", "tvdb:movie:42", "movie", "tmdb:movie:42", "movie"},
		{"different-kind", "tmdb:series:42", "series", "tmdb:movie:42", "movie"},
		{"bare-number", "42", "movie", "tmdb:movie:42", "movie"},
		{"unrelated-primary", "tmdb:movie:999", "movie", "tmdb:42", "movie"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newest := recentCachedMovie(tc.cardID, "Newest eligible", time.Now().Add(-time.Minute))
			newest.MediaType = tc.cardKind
			newest.ID = tc.cardKind + ":" + newest.ItemID
			f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {newest}})
			if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: tc.hiddenID, MediaType: tc.hiddenKind}); err != nil {
				t.Fatal(err)
			}
			v := decodeRecentCached(t, recentCachedHTTP(f, "maxItemsPerProfile=1"))
			if v.Total != 1 || !reflect.DeepEqual(recentCachedIDs(v), []string{tc.cardID}) {
				t.Fatalf("primary alias matching crossed a provider, kind or unrelated-ID boundary: %+v", v)
			}
		})
	}
}
