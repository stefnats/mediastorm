package handlers

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/metadata"
	"novastream/services/playback"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type catalogueWireSaved struct {
	watchlistService
	items []models.WatchlistItem
}

func (s catalogueWireSaved) List(string) ([]models.WatchlistItem, error) {
	return append([]models.WatchlistItem(nil), s.items...), nil
}

type catalogueWireCustom struct {
	customListsService
	items []models.WatchlistItem
}

func (s catalogueWireCustom) ListItems(user, list string) ([]models.WatchlistItem, error) {
	return append([]models.WatchlistItem(nil), s.items...), nil
}

type catalogueWireTransport struct{ calls atomic.Int32 }

func (s *catalogueWireTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.calls.Add(1)
	return nil, fmt.Errorf("unexpected HTTP demand")
}

// Synthetic cache files model already observed normal-provider sidecars. The
// real observation/identity/timestamp producer is tested in metadata's tests;
// this fixture exercises the real cache reader through real serialized GETs.
func catalogueWireHandler(t *testing.T) (*DisplayListHandler, *catalogueWireTransport) {
	t.Helper()
	dir := t.TempDir()
	meta := metadata.NewService("", "", "eng", dir, 24, false, metadata.MDBListConfig{})
	observed := time.Now().Add(-time.Hour).UTC()
	added := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	items := []models.WatchlistItem{}
	for i, lang := range []string{"en", "fr", "ja", "", "haw", "de"} {
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
		items = append(items, models.WatchlistItem{ID: id, Name: fmt.Sprintf("Synthetic %02d", i+1), MediaType: "movie", Year: 2020, AddedAt: added, Genres: []string{"Drama"}})
		if lang == "" {
			continue
		}
		fact := models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: observed, ExpiresAt: observed.Add(24 * time.Hour)}
		key := sha1.Sum([]byte(fmt.Sprintf("tmdb:movie:original_language_evidence:v1:%d", i+1)))
		path := filepath.Join(dir, "metadata", hex.EncodeToString(key[:])+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(fact)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	h := NewDisplayListHandler(catalogueWireSaved{items: items}, catalogueWireCustom{items: items}, nil)
	h.MetadataService = meta
	entries := []*playback.PrequeueEntry{}
	for _, item := range items {
		entries = append(entries, &playback.PrequeueEntry{TitleID: item.ID, TitleName: item.Name, MediaType: item.MediaType, Year: item.Year, Persistent: true, UserID: "profile-a", CreatedAt: added})
	}
	h.PrequeueStore = catalogueLanguagePrequeue{entries}
	transport := &catalogueWireTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old })
	return h, transport
}

func TestCatalogueLanguageActualHandlerWireAndOpenAPI(t *testing.T) {
	h, transport := catalogueWireHandler(t)
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	for _, tc := range []struct {
		name, query  string
		total, count int
	}{
		{"watchlist-unfiltered.json", "limit=2&sortBy=name", 6, 2},
		{"watchlist-selected.json", "originalLanguages=fra,jpn&limit=1&sortBy=name", 2, 1},
		{"watchlist-continuation.json", "originalLanguages=fra,jpn&limit=1&offset=1&sortBy=name", 2, 1},
		{"watchlist-unknown.json", "includeUnknownOriginalLanguage=true", 1, 1},
		{"watchlist-code-or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name", 2, 2},
		{"watchlist-rare.json", "originalLanguages=haw", 1, 1},
		{"watchlist-empty.json", "originalLanguages=isl", 0, 0},
		{"custom-list-selected.json", "source=custom_user_list&listId=list-a&originalLanguages=fra,jpn&sortBy=name", 2, 2},
		{"permanent-prequeue-selected.json", "source=permanent-prequeue&originalLanguages=fra,jpn&sortBy=name", 2, 2},
		{"watchlist-random-pool.json", "originalLanguages=fra,jpn&limit=0&sortBy=name", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := "/api/users/profile-a/display-list?" + tc.query
			r := httptest.NewRequest("GET", target, nil)
			r = mux.SetURLVars(r, map[string]string{"userID": "profile-a"})
			if err := validator.ValidateRequest(r, nil); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.Get(w, r)
			if w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatalf("schema: %v\n%s", err, w.Body.String())
			}
			var payload struct {
				Total     int               `json:"total"`
				Items     []json.RawMessage `json:"items"`
				Supported bool              `json:"originalLanguageFilterSupported"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Total != tc.total || len(payload.Items) != tc.count || !payload.Supported {
				t.Fatalf("payload %+v", payload)
			}
			fixtures[tc.name] = append([]byte(nil), w.Body.Bytes()...)
		})
	}
	if transport.calls.Load() != 0 {
		t.Fatalf("filter/page caused HTTP demand: %d", transport.calls.Load())
	}
	if export := os.Getenv("CATALOGUE_LANGUAGE_CONTRACT_DIR"); export != "" {
		if err := os.MkdirAll(export, 0700); err != nil {
			t.Fatal(err)
		}
		hashes := map[string]string{}
		for name, data := range fixtures {
			if err := os.WriteFile(filepath.Join(export, name), data, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			hashes[name] = hex.EncodeToString(sum[:])
		}
		manifest := map[string]any{"version": 1, "producer": "TestCatalogueLanguageActualHandlerWireAndOpenAPI", "route": "GET /api/users/{userID}/display-list", "scope": "Actual serialized handler with real metadata cache reader; synthetic saved data and observed sidecars; no authentication-traffic claim", "responses": len(fixtures)}
		for name, v := range map[string]any{"manifest.json": manifest, "sha256.json": hashes} {
			data, _ := json.MarshalIndent(v, "", "  ")
			if err := os.WriteFile(filepath.Join(export, name), append(data, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCatalogueLanguageSavedSideIDsCannotBindFact(t *testing.T) {
	h, transport := catalogueWireHandler(t)
	// Actual language sidecar exists for movie1. A manual saved record's side ID
	// and imported scalar must not attach that fact to unrelated catalogue IDs.
	h.WatchlistService = catalogueWireSaved{items: []models.WatchlistItem{{ID: "manual:foreign", MediaType: "movie", Name: "Manual", ExternalIDs: map[string]string{"tmdb": "1"}}, {ID: "tvdb:movie:99", MediaType: "movie", Name: "Wrong anchor", ExternalIDs: map[string]string{"tmdb": "1"}}}}
	// Avoid unrelated existing release enrichment demand: retain the exact real
	// cache reader while legacy optional enrichment methods use existing fakes.
	real := h.MetadataService.(cachedOriginalLanguageReader)
	h.MetadataService = &catalogueWireReaderOnly{cachedOriginalLanguageReader: real}
	response, _ := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "originalLanguages=eng"))
	if response.Total != 0 {
		t.Fatal("manual side IDs attached actual fact")
	}
	response, _ = catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "includeUnknownOriginalLanguage=true"))
	if response.Total != 2 || transport.calls.Load() != 0 {
		t.Fatal("unbound facts did not stay unknown")
	}
}

type catalogueWireReaderOnly struct {
	fakeMetadataService
	cachedOriginalLanguageReader
}

func TestCatalogueLanguageFilterCacheReadsConcurrentAndImmutable(t *testing.T) {
	h, _ := catalogueWireHandler(t)
	// Concurrent independent request-local fact maps read one immutable cache;
	// caller-side saved rows are copied and cannot mutate the provider sidecar.
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			w := catalogueLanguageGet(t, h, "originalLanguages=fra,jpn&sortBy=name")
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"total":2`) {
				t.Errorf("response %d %s", w.Code, w.Body.String())
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
