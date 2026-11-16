package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/hiddenitems"
	"novastream/services/titleratings"
)

type lovedStoredRepo struct {
	mu     sync.RWMutex
	items  map[string][]models.TitleRating
	calls  atomic.Int32
	writes atomic.Int32
	err    error
}

func (r *lovedStoredRepo) List(_ context.Context, p string) ([]models.TitleRating, error) {
	r.calls.Add(1)
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.items[p], r.err
}
func (r *lovedStoredRepo) Put(_ context.Context, p string, v models.TitleRating) error {
	r.writes.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []models.TitleRating{}
	for _, old := range r.items[p] {
		if titleratings.Match([]models.TitleRating{old}, v.Title) == "none" {
			out = append(out, old)
		}
	}
	if v.Value != "none" {
		out = append(out, v)
	}
	r.items[p] = out
	return nil
}
func (r *lovedStoredRepo) snapshot() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, _ := json.Marshal(r.items)
	return b
}

type lovedStoredRatings struct {
	*titleratings.Service
	observes atomic.Int32
	observed atomic.Int32
}

func (s *lovedStoredRatings) Observe(p, c string, t []models.Title) {
	s.observes.Add(1)
	s.observed.Add(int32(len(t)))
	s.Service.Observe(p, c, t)
}

type lovedStoredMetadata struct {
	metadataService // Every non-cache call is poison, including cached artwork.
	reader          cachedOriginalLanguageReader
	facts           map[string]*models.OriginalLanguageEvidence
	calls           atomic.Int32
	mu              sync.Mutex
	identities      []string
}

func (m *lovedStoredMetadata) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	m.calls.Add(1)
	m.mu.Lock()
	m.identities = append(m.identities, kind+":"+id)
	m.mu.Unlock()
	if m.reader != nil {
		return m.reader.GetCachedOriginalLanguageEvidence(id, kind, at)
	}
	f := m.facts[kind+":"+id]
	if f == nil {
		return nil
	}
	return models.CurrentOriginalLanguageEvidence(models.Title{ID: id, MediaType: kind, OriginalLanguageEvidence: f}, at)
}

type lovedStoredHistoryPoison struct{ historyService }
type lovedStoredAvailabilityPoison struct{ sourceAvailabilityService }

type lovedStoredFixture struct {
	display *DisplayListHandler
	meta    *MetadataHandler
	cache   *lovedStoredMetadata
	repo    *lovedStoredRepo
	ratings *lovedStoredRatings
	hidden  *hiddenitems.Service
}

func newLovedStoredFixture(t *testing.T) *lovedStoredFixture {
	t.Helper()
	repo := &lovedStoredRepo{items: map[string][]models.TitleRating{}}
	ratings := &lovedStoredRatings{Service: titleratings.New(repo)}
	cache := &lovedStoredMetadata{facts: map[string]*models.OriginalLanguageEvidence{}}
	meta := NewMetadataHandler(cache, testConfigManager(t))
	meta.TitleRatings = ratings
	meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "account"}}
	meta.HistoryService = &lovedStoredHistoryPoison{}
	meta.SourceAvailability = &lovedStoredAvailabilityPoison{}
	hidden, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	display := NewDisplayListHandler(nil, nil, nil)
	display.SetMetadataHandler(meta)
	display.SetHiddenItemsService(hidden)
	display.SetHistoryService(&lovedStoredHistoryPoison{})
	return &lovedStoredFixture{display, meta, cache, repo, ratings, hidden}
}
func (f *lovedStoredFixture) seed(id, kind, name, language string, at time.Time) {
	title := models.Title{ID: id, MediaType: kind, Name: name, Year: 2000, Certification: "PG"}
	f.repo.items["profile"] = append(f.repo.items["profile"], models.TitleRating{Key: "PRIVATE_KEY", Aliases: titleratings.Aliases(title), Title: title, Value: "love", UpdatedAt: at})
	if language != "" {
		f.cache.facts[kind+":"+id] = &models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: id, ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
	}
}
func lovedStoredRequest(query string) *http.Request {
	r := httptest.NewRequest("GET", "/api/users/profile/display-list?source=loved-stored&"+query, nil)
	r.Header.Set("X-Client-ID", "fixture-client")
	r = mux.SetURLVars(r, map[string]string{"userID": "profile"})
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
}
func lovedStoredHTTP(t *testing.T, f *lovedStoredFixture, q string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	f.display.Get(w, lovedStoredRequest(q))
	return w
}

type lovedStoredReply struct {
	Source    string            `json:"source"`
	Items     []lovedStoredItem `json:"items"`
	Total     int               `json:"total"`
	Alphabet  []string          `json:"alphabetBuckets"`
	Supported bool              `json:"originalLanguageFilterSupported"`
}

func decodeLovedStored(t *testing.T, w *httptest.ResponseRecorder) lovedStoredReply {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("missing private response policy")
	}
	var v lovedStoredReply
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Source != "loved-stored" || !v.Supported || v.Items == nil {
		t.Fatalf("incomplete stored contract: %+v", v)
	}
	return v
}
func TestLovedStoredCompleteIndexBeforeLanguageHiddenAndPaging(t *testing.T) {
	f := newLovedStoredFixture(t)
	saved := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	for i := 1; i <= 653; i++ {
		lang := "eng"
		if i >= 650 {
			lang = "fra"
		}
		f.seed(fmt.Sprintf("tmdb:movie:%d", i), "movie", fmt.Sprintf("Saved %03d", i), lang, saved.Add(time.Duration(i)*time.Second))
	}
	// A provider alias suppresses one late match without becoming language proof.
	if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "different-saved-id", MediaType: "movie", Name: "Hidden", ExternalIDs: map[string]string{"tmdb": "651"}}); err != nil {
		t.Fatal(err)
	}
	f.repo.items["profile"][650].Title.TMDBID = 651
	f.repo.items["foreign"] = []models.TitleRating{{Title: models.Title{ID: "tmdb:movie:999", MediaType: "movie", Name: "Foreign"}, Value: "love"}}
	before := string(f.repo.snapshot())
	for _, tc := range []struct {
		q     string
		total int
		names []string
		ranks []int
	}{
		{"originalLanguages=fra&sortBy=name&limit=1", 3, []string{"Saved 650"}, []int{1}},
		{"originalLanguages=fra&sortBy=name&limit=1&offset=1", 3, []string{"Saved 652"}, []int{2}},
		{"originalLanguages=fra&sortBy=name&limit=1&offset=2", 3, []string{"Saved 653"}, []int{3}},
		{"originalLanguages=fra&sortBy=name&offset=3", 3, []string{}, []int{}},
		{"originalLanguages=fra&sortBy=name&limit=0", 3, []string{"Saved 650", "Saved 652", "Saved 653"}, []int{1, 2, 3}},
		{"originalLanguages=fra&homeView=movies&limit=0&sortBy=name", 3, []string{"Saved 650", "Saved 652", "Saved 653"}, []int{1, 2, 3}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			v := decodeLovedStored(t, lovedStoredHTTP(t, f, tc.q))
			names := []string{}
			ranks := []int{}
			for _, i := range v.Items {
				names = append(names, i.Title.Name)
				ranks = append(ranks, i.Rank)
			}
			if v.Total != tc.total || !reflect.DeepEqual(names, tc.names) || !reflect.DeepEqual(ranks, tc.ranks) {
				t.Fatalf("wrong complete result: %+v", v)
			}
		})
	}
	all := decodeLovedStored(t, lovedStoredHTTP(t, f, "limit=0&sortBy=name"))
	if all.Total != 652 || len(all.Items) != 652 {
		t.Fatal("saved pool inherited discovery cap")
	}
	if got := decodeLovedStored(t, lovedStoredHTTP(t, f, "sortBy=name&offset=650&limit=1")); got.Total != 652 || got.Items[0].Title.ID != "tmdb:movie:652" {
		t.Fatal("late unfiltered page unavailable")
	}
	if string(f.repo.snapshot()) != before || f.repo.writes.Load() != 0 {
		t.Fatal("browse changed saved preferences")
	}
}
func TestLovedStoredUnknownORExactAnchorsAndStoredScalarPoison(t *testing.T) {
	f := newLovedStoredFixture(t)
	at := time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)
	f.seed("tmdb:movie:1", "movie", "French", "fra", at)
	f.seed("tmdb:movie:2", "movie", "Missing", "", at)
	f.seed("tmdb:movie:3", "movie", "Expired", "fra", at)
	f.cache.facts["movie:tmdb:movie:3"].ExpiresAt = time.Now().Add(-time.Second)
	f.seed("tmdb:movie:4", "movie", "Wrong anchor", "fra", at)
	f.cache.facts["movie:tmdb:movie:4"].SourceTitleID = "tmdb:movie:999"
	f.seed("tmdb:movie:1", "series", "Wrong kind", "fra", at)
	f.seed("manual-id", "movie", "Scalar spoof", "", at)
	f.repo.items["profile"][5].Title = models.Title{ID: "manual-id", Name: "Scalar spoof", MediaType: "movie", Year: 1999, TMDBID: 1, Language: "fra", OriginalLanguageEvidence: f.cache.facts["movie:tmdb:movie:1"], Overview: "PRIVATE_OVERVIEW", Poster: &models.Image{URL: "PRIVATE_ARTWORK"}, SourceAvailability: &models.SourceAvailability{}}
	for _, tc := range []struct {
		q     string
		total int
	}{{"originalLanguages=fra", 1}, {"includeUnknownOriginalLanguage=true", 5}, {"originalLanguages=fra&includeUnknownOriginalLanguage=true", 6}, {"originalLanguages=haw", 0}, {"includeUnknownOriginalLanguage=false", 6}} {
		t.Run(tc.q, func(t *testing.T) {
			w := lovedStoredHTTP(t, f, tc.q)
			v := decodeLovedStored(t, w)
			if v.Total != tc.total {
				t.Fatalf("total=%d want%d", v.Total, tc.total)
			}
			for _, p := range []string{"PRIVATE_", "originalLanguageEvidence", "sourceAvailability", "poster"} {
				if strings.Contains(w.Body.String(), p) {
					t.Fatalf("untrusted field leaked: %s", p)
				}
			}
		})
	}
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()
	for _, id := range f.cache.identities {
		if id == "movie:tmdb:movie:999" {
			t.Fatal("accepted imported side identity")
		}
	}
}
func TestLovedStoredRejectsUnsupportedQueryBeforeAnyRead(t *testing.T) {
	for _, q := range []string{"watchStatus=complete", "watchStatus=all&watchStatus=complete", "genres=&genres=Drama", "ratingSource=&ratingSource=imdb", "includeFacets=false&includeFacets=true", "includeUnwatchedCounts=unknown", "watchStatus=partial", "watchStatus=none", "hideWatched=true", "hideUnreleased=true", "genres=Drama", "sortBy=duration", "sortBy=rating", "ratingSource=imdb", "includeUnwatchedCounts=true", "includeFacets=true", "filterMediaType=episode", "sortDirection=sideways", "sortBy=bogus", "originalLanguages=fr", "originalLanguages=zzz", "originalLanguages=eng,", "originalLanguages=" + strings.Repeat("eng,", 34) + "fra", "includeUnknownOriginalLanguage=maybe", "includeUnknownOriginalLanguage=true&includeUnknownOriginalLanguage=false"} {
		t.Run(q, func(t *testing.T) {
			f := newLovedStoredFixture(t)
			f.repo.err = errors.New("PRIVATE_DIAGNOSTIC")
			w := lovedStoredHTTP(t, f, q)
			if w.Code != 400 || f.repo.calls.Load() != 0 || f.cache.calls.Load() != 0 || f.ratings.observes.Load() != 0 || strings.Contains(w.Body.String(), "PRIVATE_") {
				t.Fatalf("invalid request did work: %d list%d fact%d observe%d", w.Code, f.repo.calls.Load(), f.cache.calls.Load(), f.ratings.observes.Load())
			}
		})
	}
}
func TestLovedStoredAllowedStoredSortAndAlphabetIgnoreCurrentConstraint(t *testing.T) {
	f := newLovedStoredFixture(t)
	a := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	f.seed("tmdb:movie:2", "movie", "Bravo", "eng", a.Add(time.Minute))
	f.seed("tmdb:movie:1", "movie", "Alpha", "eng", a)
	f.seed("tmdb:series:3", "series", "Cedar", "fra", a)
	f.repo.items["profile"][0].Title.Year = 2002
	f.repo.items["profile"][1].Title.Year = 1999
	for _, tc := range []struct {
		q, id    string
		alphabet []string
	}{{"alphabet=A&sortBy=name", "tmdb:movie:1", []string{"A", "B", "C"}}, {"filterMediaType=movie&alphabet=A", "tmdb:movie:1", []string{"A", "B"}}, {"sortBy=added&sortDirection=asc&limit=1", "tmdb:movie:1", []string{"A", "B", "C"}}, {"sortBy=year&limit=1", "tmdb:movie:1", []string{"A", "B", "C"}}, {"limit=1", "tmdb:movie:2", []string{"A", "B", "C"}}} {
		t.Run(tc.q, func(t *testing.T) {
			v := decodeLovedStored(t, lovedStoredHTTP(t, f, tc.q))
			if v.Items[0].Title.ID != tc.id || !reflect.DeepEqual(v.Alphabet, tc.alphabet) {
				t.Fatalf("wrong ordering/facets: %+v", v)
			}
		})
	}
}
func TestLovedStoredReaderFailureDoesNotClaimEmptyKnownResult(t *testing.T) {
	for _, mode := range []string{"metadata", "users", "ratings", "hidden", "list"} {
		t.Run(mode, func(t *testing.T) {
			f := newLovedStoredFixture(t)
			switch mode {
			case "metadata":
				f.display.MetadataHandler = nil
			case "users":
				f.meta.UsersService = nil
			case "ratings":
				f.meta.TitleRatings = nil
			case "hidden":
				f.display.HiddenItemsService = nil
			case "list":
				f.repo.err = errors.New("PRIVATE_DIAGNOSTIC")
			}
			w := lovedStoredHTTP(t, f, "")
			if w.Code != 503 || f.cache.calls.Load() != 0 || f.ratings.observes.Load() != 0 || mode != "list" && f.repo.calls.Load() != 0 || strings.Contains(w.Body.String(), "PRIVATE_") || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
				t.Fatalf("failed reader returned success: %d", w.Code)
			}
		})
	}
	f := newLovedStoredFixture(t)
	f.seed("tmdb:movie:1", "movie", "Unknown", "", time.Now())
	f.meta.Service = nil
	if got := decodeLovedStored(t, lovedStoredHTTP(t, f, "includeUnknownOriginalLanguage=true")); got.Total != 1 {
		t.Fatal("missing optional cache incorrectly hid unknown")
	}
}
func TestLovedStoredSelectedPageOnlyRegistersOwnedEditableTitles(t *testing.T) {
	f := newLovedStoredFixture(t)
	at := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 3; i++ {
		f.seed(fmt.Sprintf("tmdb:movie:%d", i), "movie", fmt.Sprint(i), "", at)
	}
	v := decodeLovedStored(t, lovedStoredHTTP(t, f, "limit=1&offset=1&sortBy=name&userId=foreign"))
	if len(v.Items) != 1 || v.Items[0].Title.ID != "tmdb:movie:2" || f.ratings.observed.Load() != 1 {
		t.Fatal("registered outside returned page")
	}
	for _, tc := range []struct {
		p, c, id string
		ok       bool
	}{{"profile", "fixture-client", "tmdb:movie:2", true}, {"profile", "fixture-client", "tmdb:movie:1", false}, {"foreign", "fixture-client", "tmdb:movie:2", false}, {"profile", "other-client", "tmdb:movie:2", false}} {
		_, ok := f.ratings.Resolve(tc.p, tc.c, "movie", tc.id)
		if ok != tc.ok {
			t.Fatal("receipt crossed original page/profile/client")
		}
	}
	w := httptest.NewRecorder()
	f.meta.TitleRating(w, ratingRequest("PUT", "tmdb:movie:2", `{"rating":"like"}`))
	if w.Code != 200 || f.repo.writes.Load() != 1 {
		t.Fatalf("existing edit path rejected actual receipt: %d", w.Code)
	}
	if got := decodeLovedStored(t, lovedStoredHTTP(t, f, "limit=0")); got.Total != 2 {
		t.Fatal("updated preference remained in Love selection")
	}
}
func TestLovedStoredRandomPoolDoesNotExpandReceiptCapacity(t *testing.T) {
	f := newLovedStoredFixture(t)
	for i := 1; i <= 2101; i++ {
		f.seed(fmt.Sprintf("tmdb:movie:%d", i), "movie", fmt.Sprintf("Saved %04d", i), "", time.Unix(0, 0))
	}
	v := decodeLovedStored(t, lovedStoredHTTP(t, f, "limit=0&sortBy=name"))
	if len(v.Items) != 2101 || v.Total != 2101 {
		t.Fatal("Random pool truncated to receipt/discovery limit")
	}
	if _, ok := f.ratings.Resolve("profile", "fixture-client", "movie", v.Items[0].Title.ID); ok {
		t.Fatal("existing bounded receipt eviction unexpectedly widened")
	}
	if _, ok := f.ratings.Resolve("profile", "fixture-client", "movie", v.Items[len(v.Items)-1].Title.ID); !ok {
		t.Fatal("latest actual receipt unavailable")
	}
	v = decodeLovedStored(t, lovedStoredHTTP(t, f, "limit=1&sortBy=name"))
	if _, ok := f.ratings.Resolve("profile", "fixture-client", "movie", v.Items[0].Title.ID); !ok {
		t.Fatal("reload did not restore existing edit receipt")
	}
}
func TestLovedStoredConcurrentPrivateViewsLeaveSavedStateUnchanged(t *testing.T) {
	f := newLovedStoredFixture(t)
	for i := 1; i <= 80; i++ {
		f.seed(fmt.Sprintf("tmdb:movie:%d", i), "movie", fmt.Sprintf("Saved %03d", i), "fra", time.Unix(int64(i), 0))
	}
	before := string(f.repo.snapshot())
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := httptest.NewRecorder()
			f.display.Get(w, lovedStoredRequest(fmt.Sprintf("originalLanguages=fra&sortBy=name&offset=%d&limit=1", i)))
			var v lovedStoredReply
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &v) != nil || v.Total != 80 || len(v.Items) != 1 {
				errs <- fmt.Errorf("bad view %d/%d", w.Code, v.Total)
				return
			}
			v.Items[0].Title.Name = "MUTATED_COPY"
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if string(f.repo.snapshot()) != before || f.repo.writes.Load() != 0 {
		t.Fatal("concurrent view mutated stored title/aliases")
	}
}
func TestLovedStoredExistingScopeAndKidsChecksBeforeReads(t *testing.T) {
	for _, mode := range []string{"client", "foreign", "missing", "stream", "locked", "curated", "rating_unknown", "rating_allowed"} {
		t.Run(mode, func(t *testing.T) {
			f := newLovedStoredFixture(t)
			f.seed("tmdb:movie:1", "movie", "Saved", "fra", time.Now())
			r := lovedStoredRequest("originalLanguages=fra")
			switch mode {
			case "client":
				r.Header.Del("X-Client-ID")
			case "foreign":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "other"))
			case "missing":
				f.meta.UsersService = fallbackProfiles{}
			case "stream":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
			case "locked":
				r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, _ *http.Request, _ string) bool {
					http.Error(w, "locked", 403)
					return false
				})
			case "curated":
				f.meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "account", IsKidsProfile: true, KidsMode: "curated"}}
			case "rating_unknown", "rating_allowed":
				f.meta.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: "account", IsKidsProfile: true, KidsMode: "rating", KidsMaxMovieRating: "PG"}}
				if mode == "rating_unknown" {
					f.repo.items["profile"][0].Title.Certification = ""
				}
			}
			w := httptest.NewRecorder()
			profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(http.HandlerFunc(f.display.Get)).ServeHTTP(w, r)
			if mode == "rating_unknown" || mode == "rating_allowed" {
				v := decodeLovedStored(t, w)
				want := 0
				if mode == "rating_allowed" {
					want = 1
				}
				if v.Total != want {
					t.Fatal("existing kids rating selection changed")
				}
				return
			}
			if w.Code < 400 || f.repo.calls.Load() != 0 || f.cache.calls.Load() != 0 || f.ratings.observes.Load() != 0 {
				t.Fatalf("identity denial did work: %d", w.Code)
			}
		})
	}
}
func TestLovedStoredPreservesDuplicateAdapterRowsAndOriginalTimes(t *testing.T) {
	f := newLovedStoredFixture(t)
	a := time.Date(2026, 11, 16, 0, 0, 0, 0, time.FixedZone("saved", 3600))
	f.seed("tmdb:movie:1", "movie", "First", "", a)
	f.seed("tmdb:movie:1", "movie", "Second", "", a.Add(time.Second))
	v := decodeLovedStored(t, lovedStoredHTTP(t, f, "sortBy=name"))
	if v.Total != 2 || v.Items[0].Title.Name != "First" || v.Items[1].Title.Name != "Second" || !v.Items[0].UpdatedAt.Equal(a) || !v.Items[1].UpdatedAt.Equal(a.Add(time.Second)) {
		t.Fatal("duplicate ordinals/timestamps were replaced")
	}
}
func TestLovedStoredCanonicalLanguageBound(t *testing.T) {
	f := newLovedStoredFixture(t)
	for i, lang := range []string{"nob", "nno", "nor", "haw", "zul", "jpn"} {
		f.seed(fmt.Sprintf("tmdb:movie:%d", i+1), "movie", lang, lang, time.Now())
	}
	for _, lang := range []string{"nob", "nno", "nor", "haw", "zul", "jpn"} {
		v := decodeLovedStored(t, lovedStoredHTTP(t, f, "originalLanguages="+url.QueryEscape(lang)))
		if v.Total != 1 || v.Items[0].Title.Name != lang {
			t.Fatal("canonical codes merged or rejected")
		}
	}
}
