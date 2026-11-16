package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
)

// The real repository/grouping pipeline is tested in services/localmedia. This
// port records the actual HTTP query and applies its private cache predicate to
// stable catalogue references, without claiming file or provider verification.
type localLibraryLanguageGroups struct {
	localMediaService
	groups     []models.LocalMediaItemGroup
	references map[string]string
	reads      int
	query      models.LocalMediaItemListQuery
}

func (s *localLibraryLanguageGroups) ListGroups(_ context.Context, _ string, q models.LocalMediaItemListQuery) (*models.LocalMediaGroupListResult, error) {
	s.reads++
	s.query = q
	selected := make([]models.LocalMediaItemGroup, 0, len(s.groups))
	for _, g := range s.groups {
		kind := "movie"
		if g.LibraryType == models.LocalMediaLibraryTypeShow {
			kind = "series"
		}
		if q.OriginalLanguageMatches == nil || q.OriginalLanguageMatches(s.references[g.ID], kind) {
			selected = append(selected, g)
		}
	}
	total := len(selected)
	limit := q.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return &models.LocalMediaGroupListResult{Groups: selected[offset:end], Total: total, Limit: limit, Offset: offset, OriginalLanguageFilterSupported: q.IncludeCards}, nil
}
func localLibraryLanguageRequest(h *LocalMediaHandler, q string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", "/api/library/libraries/local/groups?"+q, nil)
	req = mux.SetURLVars(req, map[string]string{"libraryID": "local"})
	w := httptest.NewRecorder()
	h.ListGroups(w, req)
	return w
}
func localLibraryLanguageDecode(t *testing.T, w *httptest.ResponseRecorder) models.LocalMediaGroupListResult {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status%d: %s", w.Code, w.Body.String())
	}
	var out models.LocalMediaGroupListResult
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func localLibraryLanguageFixture(t *testing.T) (*LocalMediaHandler, *localLibraryLanguageGroups, *catalogueLanguageMetadata) {
	t.Helper()
	stored := &localLibraryLanguageGroups{references: map[string]string{"one": "tmdb:movie:1", "two": "tmdb:movie:2", "three": "", "four": "tmdb:movie:4"}, groups: []models.LocalMediaItemGroup{}}
	for _, id := range []string{"one", "two", "three", "four"} {
		stored.groups = append(stored.groups, models.LocalMediaItemGroup{ID: id, GroupType: "title", Title: id, LibraryType: models.LocalMediaLibraryTypeMovie, ItemCount: 2, Poster: &models.Image{Type: "poster", Language: "en"}, TMDBID: 1})
	}
	meta := &catalogueLanguageMetadata{facts: map[string]*models.OriginalLanguageEvidence{}}
	for id, lang := range map[string]string{"tmdb:movie:1": "eng", "tmdb:movie:2": "fra", "tmdb:movie:4": "haw"} {
		meta.facts[id] = &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
	}
	meta.applyArtworkLang = "it"
	h := NewLocalMediaHandler(stored, fakeLocalMediaUsersProvider{}, false)
	h.SetMetadataLanguageProviders(meta, metadataLanguageManager(t, []string{"eng", "ita"}, "eng"), &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"profile-a": {Metadata: models.MetadataSettings{PrimaryLanguage: "ita"}}}})
	return h, stored, meta
}
func TestLocalLibraryLanguageHandlerCachePredicateAndNoArtwork(t *testing.T) {
	h, stored, meta := localLibraryLanguageFixture(t)
	old := http.DefaultTransport
	poison := &catalogueWireTransport{}
	http.DefaultTransport = poison
	t.Cleanup(func() { http.DefaultTransport = old })
	for _, tc := range []struct {
		name, q string
		total   int
		ids     []string
	}{
		{"selected", "originalLanguages=fra,haw&limit=1", 2, []string{"two"}},
		{"continuation", "originalLanguages=fra,haw&limit=1&offset=1", 2, []string{"four"}},
		{"unknown", "includeUnknownOriginalLanguage=true", 1, []string{"three"}},
		{"or unknown", "originalLanguages=fra&includeUnknownOriginalLanguage=true", 2, []string{"two", "three"}},
		{"rare", "originalLanguages=haw", 1, []string{"four"}},
		{"empty", "originalLanguages=isl", 0, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&profileId=profile-a&"+tc.q))
			if !out.OriginalLanguageFilterSupported || out.Total != tc.total {
				t.Fatalf("result%+v", out)
			}
			ids := []string{}
			for _, g := range out.Groups {
				ids = append(ids, g.ID)
				if g.ItemCount != 2 || g.Poster.Language != "en" {
					t.Fatalf("stored card changed %+v", g)
				}
			}
			if !reflect.DeepEqual(ids, tc.ids) {
				t.Fatal(ids)
			}
		})
	}
	if stored.reads != 6 || meta.calls.Load() == 0 || meta.applyArtworkCalls != 0 || poison.calls.Load() != 0 {
		t.Fatalf("reads%d facts%d artwork%d HTTP%d", stored.reads, meta.calls.Load(), meta.applyArtworkCalls, poison.calls.Load())
	}
	if stored.query.OriginalLanguageMatches == nil {
		t.Fatal("no pre-page matcher forwarded")
	}
}
func TestLocalLibraryLanguageInvalidBeforeGroupsAndGetter(t *testing.T) {
	for _, q := range []string{"originalLanguages=fra", "include=items&includeUnknownOriginalLanguage=true", "include=cards&originalLanguages=fr", "include=cards&originalLanguages=zzz", "include=cards&originalLanguages=FRA", "include=cards&originalLanguages=fra,", "include=cards&includeUnknownOriginalLanguage=1", "include=cards&includeUnknownOriginalLanguage=true&includeUnknownOriginalLanguage=false", "include=cards&originalLanguages=%GG", "include=cards&originalLanguages=" + strings.Repeat("fra,", 33)} {
		t.Run(q, func(t *testing.T) {
			h, stored, meta := localLibraryLanguageFixture(t)
			w := localLibraryLanguageRequest(h, q)
			if w.Code != 400 || stored.reads != 0 || meta.calls.Load() != 0 || meta.applyArtworkCalls != 0 || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
				t.Fatalf("status%d reads%d facts%d: %s", w.Code, stored.reads, meta.calls.Load(), w.Body.String())
			}
		})
	}
}
func TestLocalLibraryLanguageLegacyArtworkStillRuns(t *testing.T) {
	h, stored, meta := localLibraryLanguageFixture(t)
	out := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&profileId=profile-a"))
	if out.Total != 4 || !out.OriginalLanguageFilterSupported || meta.applyArtworkCalls != 4 || meta.calls.Load() != 0 || stored.query.OriginalLanguageMatches != nil {
		t.Fatalf("legacy changed %+v artwork%d facts%d", out, meta.applyArtworkCalls, meta.calls.Load())
	}
	if out.Groups[0].TextPoster == nil || out.Groups[0].TextPoster.Language != "it" {
		t.Fatal("legacy localization removed")
	}
}
func TestLocalLibraryLanguageNoGetterAndObservationCache(t *testing.T) {
	h, _, meta := localLibraryLanguageFixture(t)
	h.metadata = &fakeMetadataService{}
	out := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&originalLanguages=fra"))
	if out.Total != 0 {
		t.Fatal("missing getter invented language")
	}
	out = localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&includeUnknownOriginalLanguage=true"))
	if out.Total != 4 {
		t.Fatal("missing getter lost unknown")
	}
	h.metadata = meta
	matcher := h.localLibraryLanguageMatcher(httptest.NewRequest("GET", "/", nil), displayListOriginalLanguageQuery{Languages: map[string]bool{"fra": true}})
	for n := 0; n < 10; n++ {
		if !matcher("tmdb:movie:2", "movie") {
			t.Fatal("fact lost")
		}
	}
	if meta.calls.Load() != 1 {
		t.Fatal("same reference reread", meta.calls.Load())
	}
}
