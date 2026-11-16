package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	historypkg "novastream/services/history"
	"novastream/services/metadata"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type watchingWireNormalMetadata struct {
	historypkg.MetadataService
	movies, series atomic.Int32
}

func (m *watchingWireNormalMetadata) MovieInfo(_ context.Context, q models.MovieDetailsQuery) (*models.Title, error) {
	m.movies.Add(1)
	return &models.Title{ID: q.TitleID, Name: q.Name, Year: q.Year, TMDBID: q.TMDBID, MediaType: "movie", Poster: &models.Image{URL: "https://private.invalid/poster?token=synthetic"}}, nil
}
func (m *watchingWireNormalMetadata) SeriesDetailsLite(_ context.Context, q models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	m.series.Add(1)
	return &models.SeriesDetails{Title: models.Title{ID: q.TitleID, Name: q.Name, MediaType: "series", TMDBID: 800}, Seasons: []models.SeriesSeason{{Number: 1, Episodes: []models.SeriesEpisode{{SeasonNumber: 1, EpisodeNumber: 1, ID: "tvdb:episode:8001", Name: "Earlier episode", AiredDate: "2000-01-01"}, {SeasonNumber: 1, EpisodeNumber: 2, ID: "tvdb:episode:8002", Name: "Next episode", AiredDate: "2000-01-02"}}}}}, nil
}

func TestCachedWatchingActualNormalProducerHTTPAndWire(t *testing.T) {
	// The default deny transport is installed BEFORE every service constructor.
	// The only allowed TCP client below has a literal-loopback, exact-address dial guard.
	denied := watchingTransportGuard(t)
	dir := t.TempDir()
	now := time.Now().UTC()
	progress := []models.PlaybackProgress{}
	for i := 1; i <= 701; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", i)
		progress = append(progress, models.PlaybackProgress{ID: id, ItemID: id, MediaType: "movie", MovieName: fmt.Sprintf("Observed %04d", i), Year: 2000, ExternalIDs: map[string]string{"tmdb": strconv.Itoa(i)}, Position: 120, Duration: 1200, PercentWatched: 10, UpdatedAt: now})
	}
	progress = append(progress, models.PlaybackProgress{ID: "tmdb:tv:800:s01e02", ItemID: "tmdb:tv:800:s01e02", SeriesID: "tmdb:tv:800", SeriesName: "Observed show", MediaType: "episode", SeasonNumber: 1, EpisodeNumber: 2, ExternalIDs: map[string]string{"tmdb": "800"}, Position: 270, Duration: 2700, PercentWatched: 10, UpdatedAt: now})
	raw, err := json.Marshal(map[string][]models.PlaybackProgress{"profile": progress})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "playback_progress.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	service, err := historypkg.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	normalMeta := &watchingWireNormalMetadata{}
	service.SetMetadataService(normalMeta)
	// Actual validated cache reader, empty provider keys. No synthetic TMDB redirect.
	languageDir := t.TempDir()
	cache := metadata.NewService("", "", "eng", languageDir, 24, false, metadata.MDBListConfig{})
	for _, f := range []struct {
		id         int
		kind, lang string
	}{{650, "movie", "fra"}, {651, "movie", "nob"}, {652, "movie", "haw"}, {800, "series", "fra"}} {
		fact := models.OriginalLanguageEvidence{Language: f.lang, Source: "tmdb", SourceTitleID: fmt.Sprintf("tmdb:%s:%d", f.kind, f.id), ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
		collectionCacheWireFile(t, languageDir, fmt.Sprintf("tmdb:%s:original_language_evidence:v1:%d", f.kind, f.id), fact)
	}
	fixture := newLovedStoredFixture(t)
	fixture.meta.Service = cache
	lists := &watchingLists{lists: []models.CustomList{{ID: "aside", Name: "Set Aside"}}, items: []models.WatchlistItem{{ID: "tmdb:movie:2", MediaType: "movie"}}}
	fixture.display.CustomListsService = lists
	if _, err = fixture.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Hidden"}); err != nil {
		t.Fatal(err)
	}
	normal := NewHistoryHandler(service, nil, false)
	fixture.display.SetHistoryHandler(normal)
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/history/continue", normal.ListContinueWatching).Methods("GET")
	router.HandleFunc("/api/users/{userID}/display-list", fixture.display.Get).Methods("GET")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(router).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	transport := collectionLoopbackTransport(server.Listener.Addr().String())
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	get := func(path string) (*http.Response, []byte) {
		t.Helper()
		r, _ := http.NewRequest("GET", server.URL+path, nil)
		response, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil {
			t.Fatal(e)
		}
		return response, body
	}
	response, body := get("/api/users/profile/display-list?source=continue-watching-cached")
	if response.StatusCode != 409 || normalMeta.movies.Load() != 0 || normalMeta.series.Load() != 0 {
		t.Fatal("missing pool warmed normal producer")
	}

	missingRequest := httptest.NewRequest("GET", "/api/users/profile/display-list?source=continue-watching-cached", nil)
	if err := validator.ValidateRequest(missingRequest, nil); err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateResponse(missingRequest, response.StatusCode, response.Header, body); err != nil {
		t.Fatal(err)
	}
	fixtures["missing.json"] = body
	response, body = get("/api/users/profile/history/continue")
	if response.StatusCode != 200 {
		t.Fatal("normal producer failed", response.StatusCode)
	}
	var normalRows []models.SeriesWatchState
	if json.Unmarshal(body, &normalRows) != nil || len(normalRows) != 702 {
		t.Fatalf("normal complete feed has %d rows", len(normalRows))
	}
	var normalEpisode *models.EpisodeReference
	for _, row := range normalRows {
		if row.SeriesID == "tmdb:tv:800" {
			normalEpisode = row.NextEpisode
		}
	}
	if normalEpisode == nil || normalEpisode.ItemID != "tmdb:tv:800:s01e02" {
		t.Fatal("normal canonical episode unavailable")
	}
	callsMovie, callsSeries := normalMeta.movies.Load(), normalMeta.series.Load()
	for _, tc := range []struct {
		name, q      string
		total, count int
	}{
		{"unfiltered.json", "sortBy=name&sortDirection=asc&limit=2", 700, 2},
		{"late-selected.json", "originalLanguages=fra,nob&sortBy=name&sortDirection=asc&limit=1", 3, 1},
		{"continuation.json", "originalLanguages=fra,nob&sortBy=name&sortDirection=asc&limit=1&offset=1", 3, 1},
		{"unknown-only.json", "includeUnknownOriginalLanguage=true&limit=0", 696, 696},
		{"or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true&limit=0", 698, 698},
		{"rare.json", "originalLanguages=haw&limit=0", 1, 1},
		{"empty.json", "originalLanguages=isl", 0, 0},
		{"random-pool.json", "originalLanguages=fra,nob&sortBy=name&sortDirection=asc&limit=0", 3, 3},
		{"show-episode.json", "filterMediaType=series&originalLanguages=fra", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/api/users/profile/display-list?source=continue-watching-cached&" + tc.q
			response, body := get(path)
			if response.StatusCode != 200 {
				t.Fatalf("status%d: %s", response.StatusCode, body)
			}
			req := httptest.NewRequest("GET", path, nil)
			if e := validator.ValidateRequest(req, nil); e != nil {
				t.Fatal(e)
			}
			if e := validator.ValidateResponse(req, response.StatusCode, response.Header, body); e != nil {
				t.Fatal(e)
			}
			var page struct {
				Source    string
				Items     []historypkg.CachedContinueWatchingItem
				Total     int
				Supported bool `json:"originalLanguageFilterSupported"`
				Pool      struct {
					ID                    string
					ObservedAt, ExpiresAt time.Time
					Complete              bool
				}
			}
			if json.Unmarshal(body, &page) != nil || page.Source != "continue-watching-cached" || page.Total != tc.total || len(page.Items) != tc.count || !page.Supported || !page.Pool.Complete {
				t.Fatalf("invalid page total=%d count=%d", page.Total, len(page.Items))
			}
			if strings.Contains(string(body), "private.invalid") || strings.Contains(string(body), "posterUrl") || strings.Contains(string(body), "watchedEpisodes") {
				t.Fatal("non-whitelisted field escaped")
			}
			if tc.name == "late-selected.json" && page.Items[0].SeriesID != "tmdb:movie:650" {
				t.Fatal("late language match lost")
			}
			if tc.name == "continuation.json" && page.Items[0].SeriesID != "tmdb:movie:651" {
				t.Fatal("continuation changed")
			}
			if tc.name == "show-episode.json" && (page.Items[0].NextEpisode.ItemID != normalEpisode.ItemID || page.Items[0].NextEpisode.EpisodeID != normalEpisode.EpisodeID) {
				t.Fatal("normal episode target changed")
			}
			fixtures[tc.name] = append([]byte(nil), body...)
		})
	}
	if normalMeta.movies.Load() != callsMovie || normalMeta.series.Load() != callsSeries || denied.calls.Load() != 0 {
		t.Fatal("cached browse started cold work")
	}
	// Export only on the dedicated immutable wire run, never on suite/race repeats.
	if output := os.Getenv("CW_CONTRACT_DIR"); output != "" {
		if err = os.MkdirAll(output, 0700); err != nil {
			t.Fatal(err)
		}
		manifest := map[string]any{"producer": "TestCachedWatchingActualNormalProducerHTTPAndWire", "seedAt": now, "evaluatedAt": time.Now().UTC(), "normalRows": len(normalRows), "coldMovieCallsDuringBrowse": 0, "coldSeriesCallsDuringBrowse": 0, "defaultTransportCalls": 0, "scope": "Actual local HTTP handlers, normal history service producer and real original-language cache getter; synthetic persisted progress/metadata port/sidecars, injected authenticated account context; no login/SQL/provider/Android execution."}
		hashes := map[string]string{}
		for name, body := range fixtures {
			path := filepath.Join(output, name)
			if _, e := os.Stat(path); !os.IsNotExist(e) {
				t.Fatal("refusing fixture overwrite", name)
			}
			if e := os.WriteFile(path, body, 0600); e != nil {
				t.Fatal(e)
			}
			hashes[name] = fmt.Sprintf("%x", sha256.Sum256(body))
		}
		manifest["sha256"] = hashes
		encoded, _ := json.MarshalIndent(manifest, "", "  ")
		encoded = append(encoded, '\n')
		if err = os.WriteFile(filepath.Join(output, "manifest.json"), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
