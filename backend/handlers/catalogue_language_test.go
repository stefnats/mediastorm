package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httptest"
	"net/url"
	"novastream/models"
	"novastream/services/customlists"
	"novastream/services/hiddenitems"
	"novastream/services/playback"
	"novastream/services/watchlist"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type catalogueLanguageMetadata struct {
	fakeMetadataService
	facts map[string]*models.OriginalLanguageEvidence
	calls atomic.Int32
}

func (s *catalogueLanguageMetadata) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	s.calls.Add(1)
	f := s.facts[id]
	if f == nil {
		return nil
	}
	// This fake stands in for the cache-only getter; real validation is exercised
	// separately against actual metadata caches and actual provider observations.
	title := models.Title{ID: id, MediaType: kind, OriginalLanguageEvidence: f}
	return models.CurrentOriginalLanguageEvidence(title, at)
}
func catalogueLanguageFixture(t *testing.T) (*DisplayListHandler, *catalogueLanguageMetadata) {
	t.Helper()
	wl, err := watchlist.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	custom, err := customlists.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := NewDisplayListHandler(wl, custom, nil)
	meta := &catalogueLanguageMetadata{facts: map[string]*models.OriginalLanguageEvidence{}}
	h.MetadataService = meta
	return h, meta
}
func catalogueLanguageGet(t *testing.T, h *DisplayListHandler, query string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/users/profile-a/display-list?"+query, nil)
	r = mux.SetURLVars(r, map[string]string{"userID": "profile-a"})
	w := httptest.NewRecorder()
	h.Get(w, r)
	return w
}
func catalogueLanguageSeed(t *testing.T, h *DisplayListHandler, m *catalogueLanguageMetadata, id, name, lang string) {
	t.Helper()
	_, err := h.WatchlistService.AddOrUpdate("profile-a", models.WatchlistUpsert{ID: id, Name: name, MediaType: "movie", Genres: []string{"Drama"}, Year: 2020})
	if err != nil {
		t.Fatal(err)
	}
	if lang != "" {
		m.facts[id] = &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
	}
}
func catalogueLanguageDecode(t *testing.T, w *httptest.ResponseRecorder) (DisplayListResponse, []models.WatchlistItem) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var response DisplayListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Items []models.WatchlistItem `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	return response, raw.Items
}
func TestCatalogueLanguageFiltersWholeSavedIndexBeforePage(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	for i := 1; i <= 65; i++ {
		lang := "en"
		if i == 64 {
			lang = "fr"
		}
		if i == 65 {
			lang = "ja"
		}
		catalogueLanguageSeed(t, h, m, fmt.Sprintf("tmdb:movie:%d", i), fmt.Sprintf("Title %03d", i), lang)
	}
	for _, tc := range []struct {
		q     string
		total int
		names []string
	}{
		{"originalLanguages=fra,jpn&sortBy=name&limit=1", 2, []string{"Title 064"}},
		{"originalLanguages=fra,jpn&sortBy=name&offset=1&limit=1", 2, []string{"Title 065"}},
		{"originalLanguages=fra,jpn&sortBy=name&offset=2&limit=1", 2, []string{}},
		{"originalLanguages=fra&titleFilter=065", 0, []string{}},
		{"originalLanguages=jpn&genres=Comedy", 0, []string{}},
		{"originalLanguages=fra,jpn&homeView=movies&sortBy=name&offset=1&limit=1", 2, []string{"Title 065"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, tc.q))
			names := []string{}
			for _, item := range items {
				names = append(names, item.Name)
			}
			if response.Total != tc.total || !response.OriginalLanguageFilterSupported || !reflect.DeepEqual(names, tc.names) {
				t.Fatalf("total=%d supported=%v names=%v", response.Total, response.OriginalLanguageFilterSupported, names)
			}
		})
	}
	// Clear and random-pick preparation use the same existing GET; limit=0
	// requests the entire matching dataset rather than a new discovery search.
	response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "originalLanguages=fra,jpn&limit=0"))
	if response.Total != 2 || len(items) != 2 {
		t.Fatal("full filtered random pool changed")
	}
	m.calls.Store(0)
	response, items = catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "limit=0"))
	if response.Total != 65 || len(items) != 65 || m.calls.Load() != 0 {
		t.Fatal("unfiltered request performed evidence reads")
	}
}
func TestCatalogueLanguageUnknownIsExplicitORAndRareCodesStayDistinct(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	for i, lang := range []string{"de", "fr", "ja", "nob", "nno", "nor", "haw", "", "is", "en"} {
		catalogueLanguageSeed(t, h, m, fmt.Sprintf("tmdb:movie:%d", i+1), fmt.Sprintf("Title %02d", i+1), lang)
	}
	m.facts["tmdb:movie:9"].ExpiresAt = time.Now().Add(-time.Second)
	m.facts["tmdb:movie:10"].SourceTitleID = "tmdb:movie:999"
	for _, tc := range []struct {
		query string
		total int
	}{
		{"originalLanguages=nob", 1}, {"originalLanguages=nno", 1}, {"originalLanguages=nor", 1}, {"originalLanguages=haw,jpn", 2},
		{"includeUnknownOriginalLanguage=true", 3}, {"originalLanguages=fra&includeUnknownOriginalLanguage=true", 4},
		{"originalLanguages=deu&includeUnknownOriginalLanguage=false", 1}, {"includeUnknownOriginalLanguage=false", 10},
	} {
		t.Run(tc.query, func(t *testing.T) {
			response, _ := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, tc.query))
			if response.Total != tc.total {
				t.Fatalf("total=%d want=%d", response.Total, tc.total)
			}
		})
	}
	// A missing optional getter means unknown, never fallback to manual side IDs.
	h.MetadataService = &fakeMetadataService{}
	response, _ := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "includeUnknownOriginalLanguage=true"))
	if response.Total != 10 {
		t.Fatal("optional legacy metadata service did not preserve unknown")
	}
}
func TestCatalogueLanguageStrictQueryAndUnsupportedSources(t *testing.T) {
	h, _ := catalogueLanguageFixture(t)
	invalid := []string{"originalLanguages=%", "homeView=movies&originalLanguages=%", "originalLanguages=de", "originalLanguages=ger", "originalLanguages=DEU", "originalLanguages=deu,", "originalLanguages=zzz", "originalLanguages=und", "originalLanguages=mul", "originalLanguages=zxx", "originalLanguages=de-DE", "originalLanguages=%20deu", "includeUnknownOriginalLanguage=1", "includeUnknownOriginalLanguage=true&includeUnknownOriginalLanguage=false"}
	codes := []string{"eng", "fra", "deu", "jpn", "spa", "ita", "por", "rus", "zho", "kor", "ara", "hin", "nob", "nno", "nor", "haw", "isl", "fin", "swe", "dan", "nld", "pol", "ces", "slk", "ukr", "tur", "ell", "heb", "tha", "vie", "ind", "msa", "tam"}
	invalid = append(invalid, "originalLanguages="+strings.Join(codes, ","))
	for _, query := range invalid {
		t.Run(query, func(t *testing.T) {
			w := catalogueLanguageGet(t, h, query)
			if w.Code != 400 {
				t.Fatalf("invalid query status=%d %s", w.Code, w.Body.String())
			}
		})
	}
	for _, source := range []string{"trending", "loved", "continue-watching", "top-ten", "mdblist", "collection", "unknown"} {
		t.Run(source, func(t *testing.T) {
			w := catalogueLanguageGet(t, h, "source="+source+"&originalLanguages=fra")
			if w.Code != 400 {
				t.Fatalf("unsupported status=%d", w.Code)
			}
		})
	}
	if w := catalogueLanguageGet(t, h, "originalLanguages="+url.QueryEscape(strings.Join(codes[:32], ","))); w.Code != 200 {
		t.Fatalf("32 canonical codes rejected: %s", w.Body.String())
	}
}

type catalogueLanguagePrequeue struct{ entries []*playback.PrequeueEntry }

func (s catalogueLanguagePrequeue) ListAll() []*playback.PrequeueEntry { return s.entries }
func TestCatalogueLanguageStoredAliasesAndProfileIsolation(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	catalogueLanguageSeed(t, h, m, "tmdb:movie:1", "Saved", "fr")
	list, err := h.CustomListsService.CreateList("profile-a", "Synthetic")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.CustomListsService.AddItem("profile-a", list.ID, models.WatchlistUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Saved", Year: 2020})
	if err != nil {
		t.Fatal(err)
	}
	h.PrequeueStore = catalogueLanguagePrequeue{[]*playback.PrequeueEntry{{TitleID: "tmdb:movie:1", MediaType: "movie", TitleName: "Saved", Persistent: true, UserID: "profile-a"}, {TitleID: "tmdb:movie:1", MediaType: "movie", TitleName: "Foreign", Persistent: true, UserID: "profile-b"}, {TitleID: "tmdb:movie:1", MediaType: "movie", Persistent: false, UserID: "profile-a"}}}
	for _, source := range []string{"watchlist", "custom-list", "custom_user_list", "custom-user-list", "permanent-prequeue", "mdblist", "mdblist-url", "mdblist-shelf"} {
		t.Run(source, func(t *testing.T) {
			query := url.Values{"source": {source}, "listId": {list.ID}, "originalLanguages": {"fra"}}
			if strings.HasPrefix(source, "mdblist") {
				query.Set("url", customListShelfURLPrefix+list.ID)
				query.Set("deferArtwork", "true")
			}
			w := catalogueLanguageGet(t, h, query.Encode())
			if w.Code != http.StatusOK {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			var response DisplayListResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if !response.OriginalLanguageFilterSupported || response.Total != 1 {
				t.Fatalf("response %+v", response)
			}
		})
	}
}

type catalogueLargeCustom struct {
	customListsService
	items []models.WatchlistItem
}

func (s catalogueLargeCustom) ListItems(user, list string) ([]models.WatchlistItem, error) {
	if user != "profile-a" || list != "large-list" {
		return []models.WatchlistItem{}, nil
	}
	return append([]models.WatchlistItem(nil), s.items...), nil
}
func TestCatalogueLanguageStoredSentinelHomeViewBeyondDiscoveryWindow(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	rows := make([]models.WatchlistItem, 0, 601)
	at := time.Now()
	for i := 1; i <= 601; i++ {
		kind := "movie"
		if i == 601 {
			kind = "series"
		}
		id := fmt.Sprintf("tmdb:%s:%d", kind, i)
		rows = append(rows, models.WatchlistItem{ID: id, MediaType: kind, Name: fmt.Sprintf("Title %03d", i), Year: 2020})
		m.facts[id] = &models.OriginalLanguageEvidence{Language: "fr", Source: "tmdb", SourceTitleID: id, ObservedAt: at.Add(-time.Hour), ExpiresAt: at.Add(time.Hour)}
	}
	h.CustomListsService = catalogueLargeCustom{items: rows}
	query := "source=mdblist-shelf&url=" + url.QueryEscape(customListShelfURLPrefix+"large-list") + "&homeView=movies&filterMediaType=series&originalLanguages=fra&sortBy=name&limit=1&offset=599&deferArtwork=true"
	w := catalogueLanguageGet(t, h, query)
	if w.Code != 200 {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	var response struct {
		Total     int                   `json:"total"`
		Items     []models.TrendingItem `json:"items"`
		Supported bool                  `json:"originalLanguageFilterSupported"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total != 600 || len(response.Items) != 1 || response.Items[0].Title.ID != "tmdb:movie:600" || !response.Supported {
		t.Fatalf("complete stored index lost: %+v", response)
	}
	r := httptest.NewRequest("GET", "/api/users/profile-b/display-list?"+query+"&userId=profile-a", nil)
	r = mux.SetURLVars(r, map[string]string{"userID": "profile-b"})
	w = httptest.NewRecorder()
	h.Get(w, r)
	var other DisplayListResponse
	_ = json.Unmarshal(w.Body.Bytes(), &other)
	if w.Code != 200 || other.Total != 0 {
		t.Fatalf("query profile changed saved owner: %d %s", w.Code, w.Body.String())
	}
}

func TestCatalogueLanguageCurrentHiddenPolicyPrecedesFactsAndPaging(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	catalogueLanguageSeed(t, h, m, "tmdb:movie:1", "Hidden French", "fr")
	catalogueLanguageSeed(t, h, m, "tmdb:movie:2", "Visible French", "fr")
	hidden, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.HiddenItemsService = hidden
	if _, err = hidden.Hide("profile-a", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie"}); err != nil {
		t.Fatal(err)
	}
	m.calls.Store(0)
	response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "originalLanguages=fra&limit=1"))
	if response.Total != 1 || len(items) != 1 || items[0].ID != "tmdb:movie:2" || m.calls.Load() != 1 {
		t.Fatalf("hidden row reached evidence/page: total=%d items=%+v reads=%d", response.Total, items, m.calls.Load())
	}
	if _, err = hidden.Unhide("profile-a", "movie", "tmdb:movie:1"); err != nil {
		t.Fatal(err)
	}
	if _, err = hidden.Hide("profile-b", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie"}); err != nil {
		t.Fatal(err)
	}
	response, _ = catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "originalLanguages=fra&limit=1"))
	if response.Total != 2 {
		t.Fatal("another profile's hidden state changed this index")
	}
}
