package handlers

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/hiddenitems"
)

type hiddenManagementStore struct {
	hiddenItemsService
	items       []models.HiddenItem
	reads       atomic.Int32
	selfFilters atomic.Int32
}

func (s *hiddenManagementStore) List(string) ([]models.HiddenItem, error) {
	s.reads.Add(1)
	return append([]models.HiddenItem(nil), s.items...), nil
}
func (s *hiddenManagementStore) FilterHiddenWatchlistItems(string, []models.WatchlistItem) []models.WatchlistItem {
	s.selfFilters.Add(1)
	panic("hidden management must not suppress its own rows")
}

type hiddenManagementMetadata struct {
	metadataService
	reader cachedOriginalLanguageReader
	reads  atomic.Int32
}

func (s *hiddenManagementMetadata) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	s.reads.Add(1)
	return s.reader.GetCachedOriginalLanguageEvidence(id, kind, at)
}

type hiddenHistoryPoison struct{ historyService }
type hiddenMetadataHistoryPoison struct{ historyServiceInterface }
type hiddenPrequeuePoison struct{ persistentPrequeueStore }
type hiddenSourcePoison struct{ sourceAvailabilityService }
type hiddenTastePoison struct{ tasteOnboardingService }
type hiddenRatingPoison struct{ titleRatingService }

func hiddenManagementGet(t *testing.T, h *DisplayListHandler, query string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/users/profile-a/display-list?source=hidden&"+query, nil)
	r = mux.SetURLVars(r, map[string]string{"userID": "profile-a"})
	w := httptest.NewRecorder()
	h.Get(w, r)
	return w
}
func hiddenManagementDecode(t *testing.T, w *httptest.ResponseRecorder) (DisplayListResponse, []models.HiddenItem) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("hidden status%d %s", w.Code, w.Body.String())
	}
	var response DisplayListResponse
	var payload struct {
		Items []models.HiddenItem `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if response.Source != "hidden" || !response.OriginalLanguageFilterSupported || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("missing private management capability")
	}
	return response, payload.Items
}
func hiddenManagementFixture(t *testing.T) (*DisplayListHandler, *hiddenManagementStore, *catalogueLanguageMetadata, *hiddenManagementMetadata) {
	t.Helper()
	facts := &catalogueLanguageMetadata{facts: map[string]*models.OriginalLanguageEvidence{}}
	meta := &hiddenManagementMetadata{reader: facts}
	stored := &hiddenManagementStore{}
	h := NewDisplayListHandler(nil, nil, nil)
	h.SetHiddenItemsService(stored)
	h.MetadataService = meta
	h.HistoryService = &hiddenHistoryPoison{}
	h.PrequeueStore = &hiddenPrequeuePoison{}
	h.MetadataHandler = &MetadataHandler{Service: meta, HistoryService: &hiddenMetadataHistoryPoison{}, SourceAvailability: &hiddenSourcePoison{}, Taste: &hiddenTastePoison{}, TitleRatings: &hiddenRatingPoison{}}
	return h, stored, facts, meta
}
func TestHiddenCatalogueLanguageWholeStoredIndexBeforePageAndRandomPool(t *testing.T) {
	h, stored, facts, meta := hiddenManagementFixture(t)
	at := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 650; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
		language := "eng"
		if i == 648 {
			language = "fra"
		}
		if i == 649 {
			language = "haw"
		}
		stored.items = append(stored.items, models.HiddenItem{ID: id, MediaType: "movie", Name: fmt.Sprintf("Title %04d", i), Year: 2000 + i%20, HiddenAt: at.Add(-time.Duration(i) * time.Minute), ExternalIDs: map[string]string{"tmdb": fmt.Sprint(i + 1)}})
		facts.facts[id] = &models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: id, ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}
	}
	stored.items = append(stored.items, models.HiddenItem{ID: "manual-language-scalar", MediaType: "movie", Name: "Manual French", HiddenAt: at.Add(-651 * time.Minute), ExternalIDs: map[string]string{"tmdb": "649", "originalLanguage": "fra"}})
	original, _ := json.Marshal(stored.items)
	for _, c := range []struct {
		q     string
		total int
		ids   []string
	}{
		{"originalLanguages=fra,haw&sortBy=name&limit=1", 2, []string{"tmdb:movie:649"}},
		{"originalLanguages=fra,haw&sortBy=name&limit=1&offset=1", 2, []string{"tmdb:movie:650"}},
		{"originalLanguages=fra,haw&limit=1&offset=2", 2, []string{}},
		{"includeUnknownOriginalLanguage=true&limit=50", 1, []string{"manual-language-scalar"}},
		{"originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name&limit=0", 2, []string{"manual-language-scalar", "tmdb:movie:649"}},
		{"originalLanguages=fra,haw&homeView=movies&sortBy=name&offset=1&limit=1", 2, []string{"tmdb:movie:650"}},
		{"originalLanguages=fra,haw&homeView=shows&limit=0", 0, []string{}},
		{"originalLanguages=fra,haw&titleFilter=0649&limit=0", 1, []string{"tmdb:movie:650"}},
		{"originalLanguages=fra,haw&alphabet=T&sortBy=added&sortDirection=asc&limit=0", 2, []string{"tmdb:movie:650", "tmdb:movie:649"}},
	} {
		t.Run(c.q, func(t *testing.T) {
			response, items := hiddenManagementDecode(t, hiddenManagementGet(t, h, c.q))
			ids := []string{}
			for _, item := range items {
				ids = append(ids, item.ID)
			}
			if response.Total != c.total || !reflect.DeepEqual(ids, c.ids) {
				t.Fatalf("hidden total%d ids%v", response.Total, ids)
			}
		})
	}
	response, items := hiddenManagementDecode(t, hiddenManagementGet(t, h, "limit=50&offset=600"))
	if response.Total != 651 || len(items) != 50 || items[48].ID != "tmdb:movie:649" || !items[48].HiddenAt.Equal(stored.items[648].HiddenAt) {
		t.Fatal("stored window capped or hiddenAt lost")
	}
	meta.reads.Store(0)
	response, items = hiddenManagementDecode(t, hiddenManagementGet(t, h, "limit=0&homeView=movies"))
	if response.Total != 651 || len(items) != 651 || meta.reads.Load() != 0 || stored.selfFilters.Load() != 0 {
		t.Fatal("unfiltered query warmed evidence or suppressed rows")
	}
	after, _ := json.Marshal(stored.items)
	if !reflect.DeepEqual(original, after) {
		t.Fatal("read/sort changed global hidden records")
	}
}
func TestHiddenCatalogueLanguageRejectsUnstoredRequirementsBeforeReads(t *testing.T) {
	h, stored, _, meta := hiddenManagementFixture(t)
	for _, query := range []string{"watchStatus=watched", "watchStatus=none", "genres=Drama", "sortBy=rating", "sortBy=duration", "includeUnwatchedCounts=true", "includeFacets=true", "ratingSource=imdb", "sortBy=unsupported", "filterMediaType=episode", "originalLanguages=de", "originalLanguages=zzz", "originalLanguages=fra,,eng", "includeUnknownOriginalLanguage=maybe"} {
		t.Run(query, func(t *testing.T) {
			w := hiddenManagementGet(t, h, query)
			if w.Code != 400 || stored.reads.Load() != 0 || meta.reads.Load() != 0 {
				t.Fatalf("unsupported hidden requirement demanded work: status%d reads%d cache%d", w.Code, stored.reads.Load(), meta.reads.Load())
			}
		})
	}
}
func TestHiddenCatalogueLanguagePreservesDuplicateAdapterRowsAndOriginalFields(t *testing.T) {
	h, stored, _, _ := hiddenManagementFixture(t)
	at := time.Now().UTC()
	stored.items = []models.HiddenItem{{ID: "same", MediaType: "movie", Name: "Zulu", HiddenAt: at, PosterURL: "saved-z"}, {ID: "same", MediaType: "movie", Name: "Alpha", HiddenAt: at.Add(-time.Hour), PosterURL: "saved-a"}}
	_, items := hiddenManagementDecode(t, hiddenManagementGet(t, h, "sortBy=name&includeUnknownOriginalLanguage=true"))
	if len(items) != 2 || items[0].Name != "Alpha" || items[0].ID != "same" || items[0].PosterURL != "saved-a" || !items[0].HiddenAt.Equal(stored.items[1].HiddenAt) || items[1].Name != "Zulu" {
		t.Fatal("private query ordinals escaped or original rows changed")
	}
}
func TestHiddenCatalogueLanguageLegacyArrayAndUnhideRemainAuthoritative(t *testing.T) {
	service, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Hide("profile-a", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Saved French"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Hide("profile-a", models.HiddenItemUpsert{ID: "tmdb:movie:2", MediaType: "movie", Name: "Saved unknown"})
	if err != nil {
		t.Fatal(err)
	}
	h, _, facts, _ := hiddenManagementFixture(t)
	h.SetHiddenItemsService(service)
	at := time.Now().Add(-time.Hour)
	facts.facts[first.ID] = &models.OriginalLanguageEvidence{Language: "fra", Source: "tmdb", SourceTitleID: first.ID, ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}
	_, items := hiddenManagementDecode(t, hiddenManagementGet(t, h, "originalLanguages=fra&limit=1"))
	if len(items) != 1 || items[0].ID != first.ID || !service.IsHidden("profile-a", "movie", first.ID, nil) || !service.IsHidden("profile-a", "movie", second.ID, nil) {
		t.Fatal("management filter changed suppression membership")
	}
	legacy := NewHiddenItemsHandler(service, nil)
	request := httptest.NewRequest("GET", "/api/users/profile-a/hidden-items", nil)
	request = mux.SetURLVars(request, map[string]string{"userID": "profile-a"})
	reply := httptest.NewRecorder()
	legacy.List(reply, request)
	var raw []models.HiddenItem
	if reply.Code != 200 || json.Unmarshal(reply.Body.Bytes(), &raw) != nil || len(raw) != 2 {
		t.Fatal("legacy complete array changed")
	}
	request = httptest.NewRequest("DELETE", "/api/users/profile-a/hidden-items/movie/"+url.PathEscape(first.ID), nil)
	request = mux.SetURLVars(request, map[string]string{"userID": "profile-a", "mediaType": "movie", "id": first.ID})
	reply = httptest.NewRecorder()
	legacy.Unhide(reply, request)
	if reply.Code != 204 || service.IsHidden("profile-a", "movie", first.ID, nil) || !service.IsHidden("profile-a", "movie", second.ID, nil) {
		t.Fatal("explicit unhide semantics changed")
	}
}
