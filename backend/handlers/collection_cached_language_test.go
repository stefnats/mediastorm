package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/metadata"
)

type collectionCachedPort struct {
	metadataService // Every non-cache service method is intentionally poison.
	value           *metadata.CachedCollectionSnapshot
	actual          cachedCollectionReader
	languages       cachedOriginalLanguageReader
	facts           map[string]*models.OriginalLanguageEvidence
	snapshots       atomic.Int32
	factsRead       atomic.Int32
	err             error
}

func (p *collectionCachedPort) GetCachedCollectionSnapshot(id int64, at time.Time) (*metadata.CachedCollectionSnapshot, error) {
	p.snapshots.Add(1)
	if p.actual != nil {
		return p.actual.GetCachedCollectionSnapshot(id, at)
	}
	return p.value, p.err
}
func (p *collectionCachedPort) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	p.factsRead.Add(1)
	if p.languages != nil {
		return p.languages.GetCachedOriginalLanguageEvidence(id, kind, at)
	}
	fact := p.facts[kind+":"+id]
	return models.CurrentOriginalLanguageEvidence(models.Title{ID: id, MediaType: kind, OriginalLanguageEvidence: fact}, at)
}

type collectionCachedFixture struct {
	base *lovedStoredFixture
	port *collectionCachedPort
}

func newCollectionCachedFixture(t *testing.T, n int) *collectionCachedFixture {
	t.Helper()
	base := newLovedStoredFixture(t)
	at := time.Now().UTC().Add(-time.Hour)
	snapshot := &metadata.CachedCollectionSnapshot{Version: 1, Provider: "tmdb", CollectionID: 77, Name: "Observed collection", MetadataLanguage: "en-US", ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour), Movies: make([]metadata.CachedCollectionMovie, 0, n)}
	for i := 1; i <= n; i++ {
		snapshot.Movies = append(snapshot.Movies, metadata.CachedCollectionMovie{ID: int64(i), Name: fmt.Sprintf("Observed %04d", i), ReleaseDate: "2000-01-01", PosterPath: "/observed.jpg"})
	}
	port := &collectionCachedPort{value: snapshot, facts: map[string]*models.OriginalLanguageEvidence{}}
	base.meta.Service = port
	return &collectionCachedFixture{base, port}
}
func collectionCachedRequest(q string) *http.Request {
	r := lovedStoredRequest("")
	r.URL.RawQuery = "source=collection-cached&collectionId=77&" + q
	return r
}
func collectionCachedHTTP(f *collectionCachedFixture, q string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.base.display.Get(w, collectionCachedRequest(q))
	return w
}
func decodeCachedCollection(t *testing.T, w *httptest.ResponseRecorder) cachedCollectionBrowseResponse {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status%d: %s", w.Code, w.Body.String())
	}
	var wire struct {
		Source     string                            `json:"source"`
		Items      []models.Title                    `json:"items"`
		Total      int                               `json:"total"`
		Alphabet   []string                          `json:"alphabetBuckets"`
		Supported  bool                              `json:"originalLanguageFilterSupported"`
		Collection cachedCollectionBrowseObservation `json:"collection"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Source != "collection-cached" || !wire.Supported || wire.Items == nil || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("cached browse wire contract")
	}
	return cachedCollectionBrowseResponse{DisplayListResponse: DisplayListResponse{Source: wire.Source, Items: wire.Items, Total: wire.Total, AlphabetBuckets: wire.Alphabet, OriginalLanguageFilterSupported: wire.Supported}, Collection: wire.Collection}
}
func (f *collectionCachedFixture) language(id int, language string) {
	at := time.Now().UTC().Add(-time.Hour)
	key := fmt.Sprintf("tmdb:movie:%d", id)
	f.port.facts["movie:"+key] = &models.OriginalLanguageEvidence{Source: "tmdb", SourceTitleID: key, Language: language, ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}
}

func TestCollectionCachedCompleteSelectionBeforePagesAndRandom(t *testing.T) {
	f := newCollectionCachedFixture(t, 702)
	for i, language := range []string{"fra", "haw", "nob", "nno", "nor", "zho"} {
		f.language(651+i, language)
	}
	f.language(702, "fra")
	if _, err := f.base.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:702", MediaType: "movie", Name: "Observed 0702"}); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(f.port.value)
	first := decodeCachedCollection(t, collectionCachedHTTP(f, "originalLanguages=fra&limit=1"))
	if first.Total != 1 || first.Items.([]models.Title)[0].ID != "tmdb:movie:651" {
		t.Fatal("late language/Hidden must precede paging", first.Total)
	}
	if f.port.factsRead.Load() != 701 {
		t.Fatal("Hidden must precede language reads", f.port.factsRead.Load())
	}
	for _, language := range []string{"haw", "nob", "nno", "nor", "zho"} {
		t.Run(language, func(t *testing.T) {
			reply := decodeCachedCollection(t, collectionCachedHTTP(f, "originalLanguages="+language+"&limit=1"))
			if reply.Total != 1 {
				t.Fatal(reply.Total)
			}
		})
	}
	page := decodeCachedCollection(t, collectionCachedHTTP(f, "offset=650&limit=2"))
	if page.Total != 701 || len(page.Items.([]models.Title)) != 2 || page.Items.([]models.Title)[0].ID != "tmdb:movie:651" {
		t.Fatal("no discovery cap/slice-first", page.Total)
	}
	pool := decodeCachedCollection(t, collectionCachedHTTP(f, "limit=0"))
	if pool.Total != 701 || len(pool.Items.([]models.Title)) != 701 {
		t.Fatal("full Random pool")
	}
	after, _ := json.Marshal(f.port.value)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("shared snapshot mutated")
	}
	if f.base.ratings.observes.Load() != 0 || f.base.repo.calls.Load() != 0 {
		t.Fatal("cache browse entered rating/encodeMetadata")
	}
}

func TestCollectionCachedUnknownORAndExactLanguageAnchors(t *testing.T) {
	f := newCollectionCachedFixture(t, 7)
	f.language(1, "fra")
	f.language(2, "haw")
	f.language(3, "fra")
	f.language(4, "fra")
	f.language(5, "fra")
	f.port.facts["movie:tmdb:movie:3"].SourceTitleID = "tmdb:movie:999"
	f.port.facts["movie:tmdb:movie:4"].ExpiresAt = time.Now().Add(-time.Minute)
	f.port.facts["movie:tmdb:movie:5"].SourceTitleID = "tmdb:series:5"
	for _, tc := range []struct {
		query string
		total int
	}{
		{"originalLanguages=fra", 1}, {"originalLanguages=haw", 1}, {"includeUnknownOriginalLanguage=true", 5}, {"originalLanguages=fra&includeUnknownOriginalLanguage=true", 6}, {"", 7},
	} {
		t.Run(tc.query, func(t *testing.T) {
			reply := decodeCachedCollection(t, collectionCachedHTTP(f, tc.query))
			if reply.Total != tc.total {
				t.Fatalf("total%d want%d", reply.Total, tc.total)
			}
		})
	}
}

func TestCollectionCachedUnsupportedRequirementsBeforeCache(t *testing.T) {
	for _, query := range []string{"watchStatus=complete", "watchStatus=all&watchStatus=complete", "genres=Drama", "ratingSource=imdb", "sortBy=rating", "sortBy=duration", "sortBy=added", "includeUnwatchedCounts=true", "includeFacets=true", "hideWatched=true", "hideUnreleased=true", "runtimeMin=30", "yearFrom=2000", "collectionId=78", "id=78", "collectionId=077", "offset=-1", "limit=bad", "titleFilter=A&titleFilter=B", "originalLanguages=zzz", "unknown=1"} {
		t.Run(query, func(t *testing.T) {
			f := newCollectionCachedFixture(t, 1)
			w := collectionCachedHTTP(f, query)
			if w.Code != 400 || f.port.snapshots.Load() != 0 || f.port.factsRead.Load() != 0 || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
				t.Fatalf("unsupported requirements must fail before reads: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCollectionCachedUnavailableAdultAndPurePresentation(t *testing.T) {
	for _, mode := range []string{"missing profile reader", "missing profile", "child", "missing Hidden", "missing snapshot", "wrong anchor", "expired", "reader error"} {
		t.Run(mode, func(t *testing.T) {
			f := newCollectionCachedFixture(t, 1)
			want := 503
			switch mode {
			case "missing profile reader":
				f.base.meta.UsersService = nil
			case "missing profile":
				f.base.meta.UsersService = fallbackProfiles{}
				want = 404
			case "child":
				f.base.meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "account", IsKidsProfile: true}}
				want = 403
			case "missing Hidden":
				f.base.display.HiddenItemsService = nil
			case "missing snapshot":
				f.port.value = nil
				want = 409
			case "wrong anchor":
				f.port.value.CollectionID = 78
				want = 409
			case "expired":
				f.port.value.ExpiresAt = time.Now().Add(-time.Minute)
				want = 409
			case "reader error":
				f.port.err = metadata.ErrCollectionSnapshotUnavailable
				want = 409
			}
			w := collectionCachedHTTP(f, "")
			if w.Code != want || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
				t.Fatalf("mode%s status%d want%d", mode, w.Code, want)
			}
			if (mode == "missing profile reader" || mode == "missing profile" || mode == "child" || mode == "missing Hidden") && f.port.snapshots.Load() != 0 {
				t.Fatal("denied before snapshot")
			}
		})
	}
	f := newCollectionCachedFixture(t, 3)
	f.port.value.Movies[0].Name = "Alpha"
	f.port.value.Movies[1].Name = "Beta"
	f.port.value.Movies[2].Name = "Zeta"
	f.port.value.Movies[2].ReleaseDate = ""
	reply := decodeCachedCollection(t, collectionCachedHTTP(f, "alphabet=A&sortBy=name&limit=0"))
	if reply.Total != 1 || strings.Join(reply.AlphabetBuckets, ",") != "A,B,Z" {
		t.Fatal("alphabet facet removes its own constraint", reply.AlphabetBuckets)
	}
	descending := decodeCachedCollection(t, collectionCachedHTTP(f, "sortBy=year&sortDirection=desc"))
	if descending.Items.([]models.Title)[2].Name != "Zeta" {
		t.Fatal("undated must remain last")
	}
	filtered := decodeCachedCollection(t, collectionCachedHTTP(f, "filterMediaType=series"))
	if filtered.Total != 0 {
		t.Fatal("known movie-only pool")
	}
}

func TestCollectionCachedConcurrentViewsPreserveOriginalObservation(t *testing.T) {
	f := newCollectionCachedFixture(t, 702)
	before, _ := json.Marshal(f.port.value)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := collectionCachedHTTP(f, "limit=1&offset=650")
			if w.Code != 200 {
				t.Error(w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	after, _ := json.Marshal(f.port.value)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("view changed snapshot or timestamps")
	}
}
