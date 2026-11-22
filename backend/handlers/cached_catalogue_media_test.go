package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"novastream/models"
	historypkg "novastream/services/history"
	"novastream/services/playback"
)

type cachedMediaFixture struct {
	h        *DisplayListHandler
	request  func(string) *http.Request
	snapshot func() any
	expire   func()
}

func newCachedMediaFixture(t *testing.T, source string, count int) cachedMediaFixture {
	t.Helper()
	switch source {
	case "collection-cached":
		f := newCollectionCachedFixture(t, count)
		return cachedMediaFixture{f.base.display, collectionCachedRequest, func() any { return f.port.value }, func() { f.port.value.ExpiresAt = time.Now().Add(-time.Second) }}
	case "similar-cached":
		f := newSimilarCachedFixture(t, count)
		return cachedMediaFixture{f.display, similarCachedRequest, func() any { return f.port.snapshot }, func() { f.port.snapshot.ExpiresAt = time.Now().Add(-time.Second) }}
	case "tmdb-public-list-cached":
		f := newTMDBPublicListCachedFixture(t, count)
		return cachedMediaFixture{f.display, tmdbPublicListCachedRequest, func() any { return f.port.snapshot }, func() { f.port.snapshot.ExpiresAt = time.Now().Add(-time.Second) }}
	case "tmdb-person-credits-cached":
		f := newTMDBPersonCreditsCachedFixture(t, count)
		return cachedMediaFixture{f.display, tmdbPersonCreditsCachedRequest, func() any { return f.port.snapshot }, func() { f.port.snapshot.ExpiresAt = time.Now().Add(-time.Second) }}
	case "continue-watching-cached":
		f := newWatchingFixture(t, count)
		return cachedMediaFixture{f.base.display, watchingRequest, func() any { return f.port.pool }, func() { f.port.pool.ExpiresAt = time.Now().Add(-time.Second) }}
	case "recently-watched-cached", "popular-on-server-cached":
		rows := map[string][]models.WatchHistoryItem{"alice": {}, "anonymous": {}}
		at := time.Now().Add(-time.Minute)
		for i := 1; i <= count; i++ {
			for user := range rows {
				rows[user] = append(rows[user], recentCachedMovie(fmt.Sprintf("tmdb:movie:%d", i), fmt.Sprintf("Observed %04d", i), at.Add(-time.Duration(i)*time.Minute)))
			}
		}
		if source == "recently-watched-cached" {
			f := newRecentCachedFixture(t, rows)
			return cachedMediaFixture{h: f.display, request: func(q string) *http.Request { return recentCachedRequest("maxItemsPerProfile=20&" + q) }}
		}
		f := newPopularCachedFixture(t, rows)
		return cachedMediaFixture{h: f.display, request: popularCachedRequest}
	case "loved-stored", "hidden":
		f := newLovedStoredFixture(t)
		for i := 1; i <= count; i++ {
			id := fmt.Sprintf("tmdb:movie:%d", i)
			name := fmt.Sprintf("Observed %04d", i)
			if source == "loved-stored" {
				f.seed(id, "movie", name, "", time.Now().Add(-time.Duration(i)*time.Minute))
			} else {
				if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: id, MediaType: "movie", Name: name}); err != nil {
					t.Fatal(err)
				}
			}
		}
		return cachedMediaFixture{h: f.display, request: func(q string) *http.Request {
			r := lovedStoredRequest(q)
			v := r.URL.Query()
			v.Set("source", source)
			r.URL.RawQuery = v.Encode()
			return r
		}}
	}
	t.Fatalf("unsupported fixture source %s", source)
	return cachedMediaFixture{}
}

func cachedMediaLibraries(h *DisplayListHandler, count int, mixed bool) *savedMediaLocalFixture {
	local := &savedMediaLocalFixture{libraries: []models.LocalMediaLibrary{{ID: "movies", Type: models.LocalMediaLibraryTypeMovie}, {ID: "shows", Type: models.LocalMediaLibraryTypeShow}}, rows: map[string][]models.LocalMediaItem{}}
	for i := 1; i <= count; i++ {
		kind, library, title := "movie", "movies", fmt.Sprintf("tmdb:movie:%d", i)
		if mixed && i%2 == 0 {
			kind, library, title = "series", "shows", fmt.Sprintf("tmdb:tv:%d", i)
		}
		lang := "eng"
		if i == 650 || i == 653 || count < 650 && i == count {
			lang = "fra"
		}
		row := savedMediaRow(fmt.Sprintf("file-%d", i), title, savedMediaProbe(lang, "aac", "hevc"))
		row.LibraryID = library
		row.MatchedMediaType = kind
		if kind == "series" {
			row.LibraryType = models.LocalMediaLibraryTypeShow
			row.SeasonNumber = 1
			row.EpisodeNumber = 1
		}
		local.rows[library] = append(local.rows[library], row)
	}
	h.SetStoredMediaLibraries(local, nil, savedMediaAccessFixture{})
	return local
}

type cachedMediaReply struct {
	Source    string            `json:"source"`
	Items     []json.RawMessage `json:"items"`
	Total     int               `json:"total"`
	Supported bool              `json:"storedMediaFilterSupported"`
	Basis     string            `json:"storedMediaFilterBasis"`
}

func cachedMediaGet(t *testing.T, f cachedMediaFixture, query string) (*httptest.ResponseRecorder, cachedMediaReply) {
	t.Helper()
	w := httptest.NewRecorder()
	f.h.Get(w, f.request(query))
	var response cachedMediaReply
	if w.Code == 200 {
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
	}
	return w, response
}
func cachedMediaID(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var item struct {
		ID       string          `json:"id"`
		SeriesID string          `json:"seriesId"`
		Title    json.RawMessage `json:"title"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatal(err)
	}
	if item.ID != "" {
		return item.ID
	}
	if item.SeriesID != "" {
		return item.SeriesID
	}
	var title models.Title
	if err := json.Unmarshal(item.Title, &title); err != nil {
		t.Fatal(err)
	}
	return title.ID
}

func TestCachedCatalogueMediaFiltersBeforeTotalsAndPages(t *testing.T) {
	for _, source := range []string{"collection-cached", "similar-cached", "tmdb-public-list-cached", "tmdb-person-credits-cached", "continue-watching-cached", "recently-watched-cached", "popular-on-server-cached", "loved-stored", "hidden"} {
		t.Run(source, func(t *testing.T) {
			f := newCachedMediaFixture(t, source, 654)
			local := cachedMediaLibraries(f.h, 654, source == "tmdb-public-list-cached" || source == "tmdb-person-credits-cached")
			var before []byte
			if f.snapshot != nil {
				before, _ = json.Marshal(f.snapshot())
			}
			query := "audioLanguage=fra&audioCodec=aac&videoCodec=hevc&subtitleLanguage=spa&minHeight=2160&hdr=hdr10&subtitles=present&mediaUnknown=exclude&sortBy=name&offset=1&limit=1"
			w, page := cachedMediaGet(t, f, query)
			// Recent shared activity intentionally retains one row per contributor.
			wantTotal := 2
			wantID := "tmdb:movie:653"
			if source == "recently-watched-cached" {
				wantTotal = 4
				wantID = "tmdb:movie:650"
			}
			if w.Code != 200 || page.Source != source || page.Total != wantTotal || len(page.Items) != 1 || cachedMediaID(t, page.Items[0]) != wantID || !page.Supported || page.Basis != "stored_library_versions" {
				t.Fatalf("late filtered page: status%d %s", w.Code, w.Body.String())
			}
			if local.reads != 2 {
				t.Fatalf("inventory reread per candidate: %d", local.reads)
			}
			if f.snapshot != nil {
				after, _ := json.Marshal(f.snapshot())
				if !reflect.DeepEqual(before, after) {
					t.Fatal("filter renewed or modified original snapshot")
				}
			}
			w, tail := cachedMediaGet(t, f, query+"&titleFilter=does-not-exist")
			if w.Code != 200 || tail.Total != 0 || len(tail.Items) != 0 {
				t.Fatalf("empty filtered tail: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCachedCatalogueMediaUnknownAndExpiredSnapshots(t *testing.T) {
	for _, source := range []string{"collection-cached", "similar-cached", "tmdb-public-list-cached", "tmdb-person-credits-cached", "continue-watching-cached", "recently-watched-cached", "popular-on-server-cached", "loved-stored", "hidden"} {
		t.Run(source, func(t *testing.T) {
			f := newCachedMediaFixture(t, source, 3)
			local := cachedMediaLibraries(f.h, 3, false)
			// Remove all observed media facts. Unknown is selectable, never guessed.
			local.rows = map[string][]models.LocalMediaItem{}
			for _, unknown := range []string{"include", "only", "exclude"} {
				w, page := cachedMediaGet(t, f, "audioLanguage=fra&mediaUnknown="+unknown)
				want := 3
				if source == "recently-watched-cached" {
					want = 6
				}
				if unknown == "exclude" {
					want = 0
				}
				if w.Code != 200 || page.Total != want {
					t.Fatalf("%s status%d total%d want%d: %s", unknown, w.Code, page.Total, want, w.Body.String())
				}
			}
			local.reads = 0
			w, plain := cachedMediaGet(t, f, "")
			if w.Code != 200 || !plain.Supported || plain.Basis != "" || local.reads != 0 {
				t.Fatalf("unfiltered capability warmed media: %d %s reads%d", w.Code, w.Body.String(), local.reads)
			}
			if f.expire != nil {
				f.expire()
				w, _ := cachedMediaGet(t, f, "audioLanguage=fra&mediaUnknown=include")
				if w.Code != 409 {
					t.Fatalf("expired snapshot became filtered success: %d %s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func TestCachedCatalogueMediaSelectorsAreValidatedBeforeStoredReads(t *testing.T) {
	for _, source := range []string{"collection-cached", "similar-cached", "tmdb-public-list-cached", "tmdb-person-credits-cached", "continue-watching-cached", "recently-watched-cached", "popular-on-server-cached"} {
		t.Run(source, func(t *testing.T) {
			f := newCachedMediaFixture(t, source, 3)
			local := cachedMediaLibraries(f.h, 3, false)
			for _, query := range []string{"audioLanguage=invalid-language", "minHeight=wrong", "mediaUnknown=guess", "videoCodec=hevc&videoCodec=h264"} {
				w, _ := cachedMediaGet(t, f, query)
				if w.Code != 400 || local.reads != 0 {
					t.Fatalf("invalid selector reached inventory: %d %s reads%d", w.Code, w.Body.String(), local.reads)
				}
			}
			local.fail = true
			if w, _ := cachedMediaGet(t, f, "audioLanguage=fra&mediaUnknown=include"); w.Code != 503 {
				t.Fatalf("failed media read became unknown success: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCachedContinueWatchingFiltersExactOfficialNextEpisode(t *testing.T) {
	f := newWatchingFixture(t, 0)
	f.port.pool.Items = []historypkg.CachedContinueWatchingItem{{SeriesID: "tmdb:tv:20", SeriesTitle: "Only next episode matters", MediaType: "series", NextEpisode: &historypkg.CachedContinueWatchingEpisode{SeasonNumber: 1, EpisodeNumber: 2, Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:tv:20", Ordering: "official"}}}}
	local := cachedMediaLibraries(f.base.display, 0, false)
	first := savedMediaRow("episode-one", "tmdb:tv:20", savedMediaProbe("fra", "aac", "hevc"))
	first.LibraryID = "shows"
	first.LibraryType = models.LocalMediaLibraryTypeShow
	first.SeasonNumber = 1
	first.EpisodeNumber = 1
	next := first
	next.ID = "episode-two"
	next.EpisodeNumber = 2
	next.Probe = savedMediaProbe("eng", "aac", "hevc")
	local.rows["shows"] = []models.LocalMediaItem{first, next}
	fixture := cachedMediaFixture{h: f.base.display, request: watchingRequest}
	w, page := cachedMediaGet(t, fixture, "audioLanguage=fra&mediaUnknown=exclude")
	if w.Code != 200 || page.Total != 0 {
		t.Fatalf("previous episode supplied next facts: %d %s", w.Code, w.Body.String())
	}
	local.rows["shows"][1].Probe = savedMediaProbe("fra", "aac", "hevc")
	w, page = cachedMediaGet(t, fixture, "audioLanguage=fra&mediaUnknown=exclude")
	if w.Code != 200 || page.Total != 1 {
		t.Fatalf("exact next episode not matched: %d %s", w.Code, w.Body.String())
	}
	f.port.pool.Items[0].NextEpisode.Numbering.Ordering = "dvd"
	w, page = cachedMediaGet(t, fixture, "audioLanguage=fra&mediaUnknown=only")
	if w.Code != 200 || page.Total != 1 {
		t.Fatalf("alternative numbering must remain unknown: %d %s", w.Code, w.Body.String())
	}
}

func TestPermanentPrequeueStoredMediaFilters(t *testing.T) {
	h, _ := catalogueLanguageFixture(t)
	h.PrequeueStore = catalogueLanguagePrequeue{entries: []*playback.PrequeueEntry{
		{TitleID: "tmdb:movie:1", TitleName: "Earlier incompatible", MediaType: "movie", Persistent: true, UserID: "profile-a"},
		{TitleID: "tmdb:movie:3", TitleName: "Matching saved", MediaType: "movie", Persistent: true, UserID: "profile-a"},
		{TitleID: "tmdb:movie:3", TitleName: "Not persistent", MediaType: "movie", Persistent: false, UserID: "profile-a"},
	}}
	cachedMediaLibraries(h, 3, false)
	w := catalogueLanguageGet(t, h, "source=permanent-prequeue&audioLanguage=fra&mediaUnknown=exclude&limit=1")
	var response cachedMediaReply
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	if response.Total != 1 || len(response.Items) != 1 || cachedMediaID(t, response.Items[0]) != "tmdb:movie:3" || !response.Supported || response.Basis != "stored_library_versions" {
		t.Fatalf("saved preparation filter: %s", w.Body.String())
	}
}
