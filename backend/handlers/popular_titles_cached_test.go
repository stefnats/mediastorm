package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"novastream/models"
	historypkg "novastream/services/history"
)

type popularCachedHistory struct {
	*recentCachedHistory
	mutateSnapshot func(*historypkg.RecentWatchesSnapshot)
	window         int
}

func (s *popularCachedHistory) SnapshotPopularWatches(ctx context.Context, viewer string, users map[string]models.User, window int, at time.Time) (*historypkg.RecentWatchesSnapshot, error) {
	s.reads++
	s.window = window
	if s.err != nil {
		return nil, s.err
	}
	snapshot, err := s.Service.SnapshotPopularWatches(ctx, viewer, users, window, at)
	if snapshot != nil && s.mutateSnapshot != nil {
		s.mutateSnapshot(snapshot)
	}
	if s.afterSnapshot != nil {
		s.afterSnapshot()
	}
	return snapshot, err
}

type popularCachedFixture struct {
	*recentCachedFixture
	port *popularCachedHistory
}

func newPopularCachedFixture(t *testing.T, rows map[string][]models.WatchHistoryItem) *popularCachedFixture {
	t.Helper()
	f := newRecentCachedFixture(t, rows)
	port := &popularCachedHistory{recentCachedHistory: f.history}
	f.display.HistoryHandler.Service = port
	return &popularCachedFixture{f, port}
}
func popularCachedRequest(query string) *http.Request {
	r := recentCachedRequest("")
	r.URL.RawQuery = "source=popular-on-server-cached&" + query
	return r
}
func popularCachedHTTP(f *popularCachedFixture, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.display.Get(w, popularCachedRequest(query))
	return w
}
func decodePopularCached(t *testing.T, w *httptest.ResponseRecorder) recentCachedReply {
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
	if v.Source != "popular-on-server-cached" || !v.OriginalLanguageFilterSupported || v.Items == nil {
		t.Fatalf("incomplete cached popularity response: %+v", v)
	}
	return v
}
func assertPopularCachedStatus(t *testing.T, f *popularCachedFixture, query string, want int) {
	t.Helper()
	w := popularCachedHTTP(f, query)
	if w.Code != want {
		t.Fatalf("status=%d want=%d body=%s", w.Code, want, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" || strings.Contains(w.Body.String(), `"items"`) {
		t.Fatal("failure published private cards or omitted no-store")
	}
}
func popularCachedSeries(id, name string, at time.Time) models.WatchHistoryItem {
	v := recentCachedMovie(id, name, at)
	v.MediaType = "series"
	v.ID = "series:" + id
	return v
}
func popularCachedEpisode(series string, episode int, name string, at time.Time) models.WatchHistoryItem {
	id := fmt.Sprintf("%s:s01e%02d", series, episode)
	return models.WatchHistoryItem{ID: "episode:" + id, ItemID: id, MediaType: "episode", Name: fmt.Sprintf("Episode %d", episode), SeriesID: series, SeriesName: name, SeasonNumber: 1, EpisodeNumber: episode, Watched: true, WatchedAt: at, UpdatedAt: at, WatchedSeconds: 30, Source: models.WatchSourcePlayed}
}

func TestPopularTitlesCachedCompleteGroupsBeforeLanguageTotalAndPaging(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	rows := map[string][]models.WatchHistoryItem{"alice": {}, "anonymous": {}}
	for i := 1; i <= 653; i++ {
		for user := range rows {
			rows[user] = append(rows[user], recentCachedMovie(fmt.Sprintf("tmdb:movie:%d", i), fmt.Sprintf("Film %03d", i), now.Add(-time.Duration(i)*time.Minute)))
		}
	}
	f := newPopularCachedFixture(t, rows)
	for i := 1; i <= 653; i++ {
		lang := "eng"
		if i == 650 || i == 653 {
			lang = "fra"
		}
		if i == 651 {
			lang = "jpn"
		}
		if i == 652 {
			lang = "haw"
		}
		f.fact(fmt.Sprintf("tmdb:movie:%d", i), "movie", lang)
	}
	for _, tc := range []struct {
		q     string
		total int
		ids   []string
	}{
		{"originalLanguages=fra&limit=1", 2, []string{"tmdb:movie:650"}},
		{"originalLanguages=fra&limit=1&offset=1", 2, []string{"tmdb:movie:653"}},
		{"originalLanguages=fra&offset=2", 2, []string{}},
		{"originalLanguages=fra&sortBy=name&sortDirection=desc", 2, []string{"tmdb:movie:653", "tmdb:movie:650"}},
		{"originalLanguages=jpn,haw&sortBy=name", 2, []string{"tmdb:movie:651", "tmdb:movie:652"}},
		{"originalLanguages=fra&titleFilter=653", 1, []string{"tmdb:movie:653"}},
		{"originalLanguages=fra&limit=2000000&offset=2000000", 2, []string{}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			v := decodePopularCached(t, popularCachedHTTP(f, tc.q))
			if v.Total != tc.total || !reflect.DeepEqual(recentCachedIDs(v), tc.ids) {
				t.Fatalf("wrong complete popularity page: %+v", v)
			}
			for _, item := range v.Items {
				if item.Rank != 2 || item.Title.CardSubtitle != "2 views" {
					t.Fatal("rank must remain aggregate view count")
				}
			}
		})
	}
	all := decodePopularCached(t, popularCachedHTTP(f, "sortBy=name&limit=0"))
	if all.Total != 653 || len(all.Items) != 653 {
		t.Fatal("popular groups inherited discovery cutoff")
	}
	late := decodePopularCached(t, popularCachedHTTP(f, "sortBy=name&offset=600&limit=1"))
	if late.Total != 653 || late.Items[0].Title.ID != "tmdb:movie:601" || late.Items[0].Rank != 2 {
		t.Fatal("late unfiltered page lost aggregate count")
	}
	if f.repo.calls.Load() != 0 || f.repo.writes.Load() != 0 || f.ratings.observes.Load() != 0 {
		t.Fatal("cache browse invoked ratings or enrichment")
	}
}

func TestPopularTitlesCachedCountsCompletedItemsAndKeepsStoredAliasRepresentative(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {
		popularCachedSeries("tmdb:42", "Latest stored name", now), popularCachedEpisode("tmdb:tv:42", 1, "Earlier series", now.Add(-time.Minute)), popularCachedEpisode("tmdb:series:42", 2, "Earlier series", now.Add(-2*time.Minute)),
		recentCachedMovie("tmdb:movie:43", "One completed film", now),
	}})
	f.fact("tmdb:series:42", "series", "fra")
	v := decodePopularCached(t, popularCachedHTTP(f, "originalLanguages=fra"))
	if v.Total != 1 || v.Items[0].Title.ID != "tmdb:42" || v.Items[0].Title.Name != "Latest stored name" || v.Items[0].Rank != 3 || v.Items[0].Title.CardSubtitle != "3 views" {
		t.Fatalf("episode rows did not aggregate under bound primary: %+v", v)
	}
	for _, q := range []string{"minimumProfiles=3", "minimumViews=3&minimumProfiles=3"} {
		v = decodePopularCached(t, popularCachedHTTP(f, q))
		if v.Total != 1 || v.Items[0].Rank != 3 {
			t.Fatal("legacy threshold alias changed count semantics")
		}
	}
	watched := true
	for i := 0; i < 3; i++ {
		if _, err := f.history.Service.UpdateWatchHistory("alice", models.WatchHistoryUpdate{MediaType: "movie", ItemID: "tmdb:movie:43", Name: "One completed film", Watched: &watched, WatchedSeconds: 30, Source: models.WatchSourcePlayed}); err != nil {
			t.Fatal(err)
		}
	}
	v = decodePopularCached(t, popularCachedHTTP(f, "minimumViews=1&filterMediaType=movie"))
	if v.Total != 1 || v.Items[0].Rank != 1 || v.Items[0].Title.CardSubtitle != "1 view" {
		t.Fatal("repeated writes invented a playback journal")
	}
}

func TestPopularTitlesCachedPrimaryGroupingNeverUsesUnboundSidesOrNames(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	rows := []models.WatchHistoryItem{recentCachedMovie("tmdb:movie:42", "Same name", now), recentCachedMovie("tvdb:movie:42", "Same name", now), popularCachedSeries("tmdb:series:42", "Same name", now), popularCachedSeries("tvdb:series:42", "Same name", now), recentCachedMovie("imdb:tt0000042", "Same name", now)}
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": rows, "anonymous": rows, "private": rows})
	// Poison the owned post-load projection: history migration is outside this
	// handler and would canonicalize persisted conflicting aliases beforehand.
	f.port.mutateSnapshot = func(s *historypkg.RecentWatchesSnapshot) {
		for i := range s.Items {
			s.Items[i].Watch.ExternalIDs = map[string]string{"tmdb": "999", "tvdb": "999", "imdb": "tt9999999"}
		}
	}
	v := decodePopularCached(t, popularCachedHTTP(f, ""))
	if v.Total != 5 {
		t.Fatalf("cross-provider or name-based merge: %+v", v)
	}
	ids := recentCachedIDs(v)
	sort.Strings(ids)
	want := []string{"imdb:tt0000042", "tmdb:movie:42", "tmdb:series:42", "tvdb:movie:42", "tvdb:series:42"}
	sort.Strings(want)
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("unbound sides changed primary IDs: %v", ids)
	}
	for _, item := range v.Items {
		if item.Rank != 2 || item.Title.CardSubtitle != "2 views" || item.Title.TMDBID == 999 || item.Title.TVDBID == 999 || item.Title.IMDBID == "tt9999999" {
			t.Fatal("private rows or unbound IDs affected aggregation")
		}
	}
}

func TestPopularTitlesCachedKnownPrimaryAliasesCoalesceCounts(t *testing.T) {
	for _, tc := range []struct{ name, kind, a, b string }{
		{"tmdb-movie", "movie", "tmdb:42", "tmdb:movie:42"}, {"tmdb-series", "series", "tmdb:series:42", "tmdb:tv:42"}, {"tvdb-series", "series", "tvdb:42", "tvdb:series:42"}, {"imdb-movie", "movie", "tt0000042", "imdb:tt0000042"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			a, b := recentCachedMovie(tc.a, "New representative", now), recentCachedMovie(tc.b, "Earlier representative", now.Add(-time.Minute))
			a.MediaType, b.MediaType = tc.kind, tc.kind
			a.ID, b.ID = tc.kind+":"+a.ItemID, tc.kind+":"+b.ItemID
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {a}, "anonymous": {b}})
			v := decodePopularCached(t, popularCachedHTTP(f, ""))
			if v.Total != 1 || v.Items[0].Title.ID != tc.a || v.Items[0].Rank != 2 {
				t.Fatalf("safe primary aliases did not coalesce: %+v", v)
			}
		})
	}
}

func TestPopularTitlesCachedHiddenAliasesAndWatchedApplyBeforePage(t *testing.T) {
	for _, tc := range []struct{ name, kind, card, hidden string }{
		{"movie-canonical-marker", "movie", "tmdb:42", "tmdb:movie:42"}, {"movie-short-marker", "movie", "tmdb:movie:42", "tmdb:42"},
		{"series-tv-marker", "series", "tmdb:series:42", "tmdb:tv:42"}, {"series-canonical-marker", "series", "tmdb:tv:42", "tmdb:series:42"},
		{"series-short-marker", "series", "tmdb:series:42", "tmdb:42"}, {"series-short-card", "series", "tmdb:42", "tmdb:series:42"},
		{"tvdb-canonical-marker", "series", "tvdb:42", "tvdb:series:42"}, {"tvdb-short-marker", "series", "tvdb:series:42", "tvdb:42"},
		{"imdb-prefixed-marker", "movie", "tt0000042", "imdb:tt0000042"}, {"imdb-bare-marker", "movie", "imdb:tt0000042", "tt0000042"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			newest := recentCachedMovie(tc.card, "Hidden newest", now)
			olderID := "tmdb:" + tc.kind + ":43"
			older := recentCachedMovie(olderID, "Visible older", now.Add(-time.Minute))
			newest.MediaType, older.MediaType = tc.kind, tc.kind
			newest.ID, older.ID = tc.kind+":"+newest.ItemID, tc.kind+":"+older.ItemID
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {newest, older}, "anonymous": {newest, older}})
			if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: tc.hidden, MediaType: tc.kind}); err != nil {
				t.Fatal(err)
			}
			v := decodePopularCached(t, popularCachedHTTP(f, "limit=1"))
			if v.Total != 1 || !reflect.DeepEqual(recentCachedIDs(v), []string{olderID}) {
				t.Fatalf("hidden alias reached page/total: %+v", v)
			}
		})
	}
	now := time.Now().Add(-time.Minute)
	a, b := recentCachedMovie("tmdb:movie:1", "Already seen", now), recentCachedMovie("tmdb:movie:2", "Not seen", now.Add(-time.Minute))
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {a, b}, "anonymous": {a, b}, "profile": {a}})
	v := decodePopularCached(t, popularCachedHTTP(f, "hideWatched=true&limit=1"))
	if v.Total != 1 || v.Items[0].Title.ID != "tmdb:movie:2" {
		t.Fatal("viewer watched filter applied after page")
	}
}

func TestPopularTitlesCachedAlphabetSortAndIndependentFacets(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	a, b, c := recentCachedMovie("tmdb:movie:1", "Alpha", now), recentCachedMovie("tmdb:movie:2", "Bravo", now.Add(-time.Minute)), recentCachedMovie("tmdb:movie:3", "Beta", now.Add(-2*time.Minute))
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {a, b, c}, "anonymous": {a, b}})
	v := decodePopularCached(t, popularCachedHTTP(f, "minimumViews=1&alphabet=B&sortBy=name"))
	if v.Total != 2 || !reflect.DeepEqual(recentCachedIDs(v), []string{"tmdb:movie:3", "tmdb:movie:2"}) || !reflect.DeepEqual(v.AlphabetBuckets, []string{"A", "B"}) || v.Items[0].Rank != 1 || v.Items[1].Rank != 2 {
		t.Fatalf("alphabet lost eligible pool or view ranks: %+v", v)
	}
	v = decodePopularCached(t, popularCachedHTTP(f, "minimumViews=1&sortDirection=asc"))
	if !reflect.DeepEqual(recentCachedIDs(v), []string{"tmdb:movie:3", "tmdb:movie:1", "tmdb:movie:2"}) {
		t.Fatal("default ascending changed deterministic tie order")
	}
}

func TestPopularTitlesCachedStrictSelectorsBeforeReaders(t *testing.T) {
	for _, q := range []string{"source=popular-on-server-cached", "minimumViews=0", "minimumViews=101", "minimumViews=01", "minimumProfiles=0", "minimumViews=2&minimumProfiles=3", "minimumViews=2&minimumViews=2", "activityWindowDays=6", "activityWindowDays=366", "maxItemsPerProfile=3", "originalLanguages=fr", "originalLanguages=fra&originalLanguages=eng", "includeUnknownOriginalLanguage=1", "offset=-1", "limit=01", "offset=9999999999999999999999", "watchStatus=watched", "genres=Drama", "ratingSource=imdb", "sortBy=year", "sortBy=added", "sortBy=duration", "sortBy=rating", "sortDirection=sideways", "filterMediaType=episode", "alphabet=AB", "homeView=custom", "homeView=shows&filterMediaType=movie", "hideWatched=1", "includeFacets=true", "includeUnwatchedCounts=true", "hideUnreleased=true", "profileId=foreign", "userId=foreign", "unknown=value", "titleFilter=%zz"} {
		t.Run(q, func(t *testing.T) {
			f := newPopularCachedFixture(t, nil)
			assertPopularCachedStatus(t, f, q, 400)
			if f.profiles.gets != 0 || f.profiles.lists != 0 || f.port.reads != 0 || f.cache.calls.Load() != 0 {
				t.Fatal("invalid query consulted readers")
			}
		})
	}
}

func TestPopularTitlesCachedChangedPrivacyViewerHiddenAndHistoryDenyPublication(t *testing.T) {
	for _, change := range []string{"opt-out", "anonymous", "owner", "name", "birth", "new-opt-in", "viewer-owner", "viewer-pin", "viewer-kids", "hidden", "viewer-history", "source-history"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			row := recentCachedMovie("tmdb:movie:1", "Film", now)
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {row}, "anonymous": {row}})
			f.port.afterSnapshot = func() {
				id := "alice"
				if strings.HasPrefix(change, "viewer-") {
					id = "profile"
				}
				if change == "new-opt-in" {
					id = "private"
				}
				f.profiles.change(id, func(u *models.User) {
					switch change {
					case "opt-out":
						u.ActivityPrivacy = models.ActivityPrivacyNotShared
					case "anonymous":
						u.ActivityPrivacy = models.ActivityPrivacySharedAnonymous
					case "owner", "viewer-owner":
						u.AccountID = "changed"
					case "name":
						u.Name = "changed"
					case "birth":
						u.CreatedAt = time.Now()
					case "new-opt-in":
						u.ActivityPrivacy = models.ActivityPrivacyShared
					case "viewer-pin":
						u.PinHash = "synthetic-changed-pin"
					case "viewer-kids":
						u.IsKidsProfile = true
					}
				})
			}
			q := ""
			if change == "hidden" {
				q = "includeUnknownOriginalLanguage=true"
				f.cache.reader = recentCachedLanguageReader(func(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
					if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie"}); err != nil {
						t.Fatal(err)
					}
					return nil
				})
			}
			if change == "viewer-history" || change == "source-history" {
				f.port.beforeCurrent = func() {
					id := "alice"
					if change == "viewer-history" {
						id = "profile"
					}
					watched := true
					if _, err := f.history.Service.UpdateWatchHistory(id, models.WatchHistoryUpdate{MediaType: "movie", ItemID: "tmdb:movie:999", Name: "Changed history", Watched: &watched, WatchedSeconds: 30, Source: models.WatchSourcePlayed}); err != nil {
						t.Fatal(err)
					}
				}
			}
			assertPopularCachedStatus(t, f, q, 409)
		})
	}
}

func TestPopularTitlesCachedLanguageRecheckUsesCanonicalAliasAndExcludedGroups(t *testing.T) {
	for _, change := range []string{"expiry", "excluded-becomes-match"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now().Add(-time.Minute)
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:1", "First", now), recentCachedMovie("tmdb:2", "Second", now.Add(-time.Minute))}})
			calls := map[string]int{}
			f.cache.reader = recentCachedLanguageReader(func(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
				if !strings.HasPrefix(id, "tmdb:movie:") {
					t.Fatal("language identity was not primary-canonical")
				}
				calls[id]++
				lang := "fra"
				if id == "tmdb:movie:2" {
					if change == "expiry" && calls[id] > 1 {
						return nil
					}
					if change == "excluded-becomes-match" && calls[id] == 1 {
						lang = "eng"
					}
				}
				return &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: at.Add(-time.Hour), ExpiresAt: at.Add(time.Hour)}
			})
			assertPopularCachedStatus(t, f, "minimumViews=1&originalLanguages=fra&limit=1", 409)
		})
	}
}

func TestPopularTitlesCachedWindowSettingsStayReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name, body, query string
		window            int
		absent, directory bool
	}{
		{"configured", `{"homeShelves":{"popularOnServerWindowDays":120},"preserve":"exact bytes"}`, "", 120, false, false},
		{"missing", "", "", 90, true, false}, {"malformed", "{ invalid", "", 90, false, false}, {"wrong-type", `{"homeShelves":{"popularOnServerWindowDays":"120"}}`, "", 90, false, false}, {"too-small", `{"homeShelves":{"popularOnServerWindowDays":6}}`, "", 90, false, false}, {"trailing", `{"homeShelves":{"popularOnServerWindowDays":120}} {}`, "", 90, false, false}, {"unreadable", "", "", 90, false, true}, {"explicit", `{"homeShelves":{"popularOnServerWindowDays":120}}`, "activityWindowDays=30", 30, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := recentCachedMovie("tmdb:movie:1", "Stored", time.Now().Add(-100*24*time.Hour))
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {row}, "anonymous": {row}})
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
			v := decodePopularCached(t, popularCachedHTTP(f, tc.query))
			want := 0
			if tc.window >= 100 {
				want = 1
			}
			if f.port.window != tc.window || v.Total != want {
				t.Fatalf("window=%d total=%d", f.port.window, v.Total)
			}
			if tc.absent {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("browse created settings")
				}
			} else if !tc.directory {
				raw, err := os.ReadFile(path)
				if err != nil || string(raw) != tc.body {
					t.Fatal("browse rewrote settings")
				}
			}
		})
	}
}

func TestPopularTitlesCachedUnavailableAndCancellation(t *testing.T) {
	for _, condition := range []string{"missing-history", "legacy-only", "missing-cache", "missing-hidden", "snapshot-error", "kids", "foreign-owner"} {
		t.Run(condition, func(t *testing.T) {
			f := newPopularCachedFixture(t, nil)
			want := 503
			switch condition {
			case "missing-history":
				f.display.HistoryHandler = nil
			case "legacy-only":
				f.display.HistoryHandler.Service = &lovedStoredHistoryPoison{}
			case "missing-cache":
				f.meta.Service = nil
			case "missing-hidden":
				f.display.HiddenItemsService = nil
			case "snapshot-error":
				f.port.err = errors.New("synthetic")
				want = 409
			case "kids":
				f.profiles.change("profile", func(u *models.User) { u.IsKidsProfile = true })
				want = 403
			case "foreign-owner":
				f.profiles.change("profile", func(u *models.User) { u.AccountID = "foreign" })
				want = 404
			}
			assertPopularCachedStatus(t, f, "", want)
		})
	}
	f := newPopularCachedFixture(t, nil)
	r := popularCachedRequest("")
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	w := httptest.NewRecorder()
	f.display.Get(w, r.WithContext(ctx))
	if w.Body.Len() != 0 || f.port.reads != 0 {
		t.Fatal("cancelled request read or published")
	}
	r = popularCachedRequest("")
	ctx, cancel = context.WithCancel(r.Context())
	f.port.afterSnapshot = cancel
	w = httptest.NewRecorder()
	f.display.Get(w, r.WithContext(ctx))
	if w.Body.Len() != 0 {
		t.Fatal("cancelled snapshot published")
	}
}

// The ordinary history interface deliberately has no Popular snapshot method.
// This adapter must continue working for Recent without gaining that optional API.
type popularRecentOnlyHistory struct {
	historyService
	recent *recentCachedHistory
}

func (s *popularRecentOnlyHistory) SnapshotRecentWatches(ctx context.Context, viewer string, users map[string]models.User, window int, at time.Time) (*historypkg.RecentWatchesSnapshot, error) {
	return s.recent.SnapshotRecentWatches(ctx, viewer, users, window, at)
}
func (s *popularRecentOnlyHistory) RecentWatchesSnapshotCurrent(snapshot *historypkg.RecentWatchesSnapshot) bool {
	return s.recent.RecentWatchesSnapshotCurrent(snapshot)
}
func (s *popularRecentOnlyHistory) GetWatchHistoryItem(viewer, kind, id string) (*models.WatchHistoryItem, error) {
	return s.recent.GetWatchHistoryItem(viewer, kind, id)
}

func TestPopularTitlesCachedExtractionPreservesRecentOnlyOptionalReader(t *testing.T) {
	f := newRecentCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {recentCachedMovie("tmdb:movie:42", "Recent only", time.Now().Add(-time.Minute))}})
	f.display.HistoryHandler.Service = &popularRecentOnlyHistory{recent: f.history}
	v := decodeRecentCached(t, recentCachedHTTP(f, ""))
	if v.Total != 1 || v.Items[0].Title.ID != "tmdb:movie:42" || f.history.reads != 1 {
		t.Fatal("Recent acquired a Popular snapshot requirement")
	}
	w := httptest.NewRecorder()
	f.display.Get(w, popularCachedRequest("minimumViews=1"))
	if w.Code != 503 || f.history.reads != 1 || strings.Contains(w.Body.String(), `"items"`) {
		t.Fatal("unsupported Popular source fell back to Recent")
	}
}

func TestPopularTitlesCachedEffectiveWindowRecheckAndExplicitOverride(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(fmt.Sprintf("override=%t", override), func(t *testing.T) {
			row := recentCachedMovie("tmdb:movie:42", "Popular", time.Now().Add(-time.Minute))
			f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {row}, "anonymous": {row}})
			path := f.meta.CfgManager.ConfigPath()
			if err := os.WriteFile(path, []byte(`{"homeShelves":{"popularOnServerWindowDays":90}}`), 0600); err != nil {
				t.Fatal(err)
			}
			f.port.afterSnapshot = func() {
				if err := os.WriteFile(path, []byte(`{"homeShelves":{"popularOnServerWindowDays":120}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if override {
				v := decodePopularCached(t, popularCachedHTTP(f, "activityWindowDays=90"))
				if v.Total != 1 || f.port.window != 90 {
					t.Fatal("explicit query inherited changed settings")
				}
			} else {
				assertPopularCachedStatus(t, f, "", 409)
			}
		})
	}
}

func TestPopularTitlesCachedRepresentativeNameAndRecencyAreIndependent(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	a := []models.WatchHistoryItem{popularCachedSeries("tmdb:42", "", now), popularCachedEpisode("tmdb:tv:42", 1, "Usable stored name", now.Add(-time.Minute)), recentCachedMovie("tmdb:movie:44", "Later representative", now.Add(-30*time.Second)), recentCachedMovie("tmdb:movie:99", "", now)}
	b := []models.WatchHistoryItem{popularCachedSeries("tmdb:series:42", "Earlier name", now.Add(-2*time.Minute)), recentCachedMovie("tmdb:movie:44", "Later representative", now.Add(-30*time.Second)), recentCachedMovie("tmdb:movie:99", "  ", now)}
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": a, "anonymous": b, "charlie": {recentCachedMovie("tmdb:movie:44", "Later representative", now.Add(-30*time.Second))}})
	f.profiles.items["charlie"] = models.User{ID: "charlie", Name: "Charlie", ActivityPrivacy: models.ActivityPrivacyShared}
	v := decodePopularCached(t, popularCachedHTTP(f, ""))
	if v.Total != 2 || v.Items[0].Title.Name != "Usable stored name" || v.Items[0].Rank != 3 || v.Items[1].Title.ID != "tmdb:movie:44" || v.Items[1].Rank != 3 {
		t.Fatalf("nameless rows were lost from counts/recency or fabricated into a card: %+v", v)
	}
}

func TestPopularTitlesCachedUnknownCannotRescueUnboundOrContradictoryPrimary(t *testing.T) {
	row := recentCachedMovie("tmdb:movie:42", "Only valid primary", time.Now().Add(-time.Minute))
	f := newPopularCachedFixture(t, map[string][]models.WatchHistoryItem{"alice": {row}, "anonymous": {row}})
	f.port.mutateSnapshot = func(snapshot *historypkg.RecentWatchesSnapshot) {
		for _, id := range []string{"/private/film.mkv", "https://synthetic:synthetic@example.invalid/film", "unbound-opaque", "imdb:not-an-id", "tmdb:movie:42:s01e01", "tmdb:movie:0", "tmdb:series:42"} {
			watch := snapshot.Items[0].Watch
			watch.ItemID = id
			watch.ExternalIDs = map[string]string{"tmdb": "42"}
			snapshot.Items = append(snapshot.Items, historypkg.RecentWatchSnapshotItem{Group: 1, Watch: watch})
		}
		watch := snapshot.Items[0].Watch
		watch.MediaType = "episode"
		watch.ItemID = "tmdb:series:99:s01e01"
		watch.SeriesID = "tmdb:series:42"
		watch.SeriesName = "Contradictory series"
		watch.SeasonNumber, watch.EpisodeNumber = 1, 1
		snapshot.Items = append(snapshot.Items, historypkg.RecentWatchSnapshotItem{Group: 1, Watch: watch})
	}
	for _, q := range []string{"", "includeUnknownOriginalLanguage=true"} {
		v := decodePopularCached(t, popularCachedHTTP(f, q))
		if v.Total != 1 || v.Items[0].Title.ID != "tmdb:movie:42" || v.Items[0].Rank != 2 {
			t.Fatalf("unbound primary was rescued by unknown/side IDs: %+v", v)
		}
	}
}
