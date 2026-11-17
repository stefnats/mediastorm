package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	historypkg "novastream/services/history"
	"novastream/services/metadata"
)

// Unchanged native production builder URLs against the normal history producer,
// validated cached-language reader and actual loopback handlers. Authentication
// is injected over a synthetic owner, not login, SQL or full Android execution.
func TestNativeCachedWatchingRequests(t *testing.T) {
	exportDir := os.Getenv("NATIVE_CACHED_WATCHING_CONTRACT_DIR")
	if exportDir == "" {
		t.Skip("native cached watching request exports not configured")
	}
	queryFile := filepath.Join(exportDir, "native-cached-watching-queries.json")
	queryRaw, err := os.ReadFile(queryFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryRaw) > 32768 || fmt.Sprintf("%x", sha256.Sum256(queryRaw)) != "e54b5c79877b8dbb3928e1ce5d3b85e593367516fa037aedfa8494c837473c94" {
		t.Fatal("native query bytes changed")
	}
	var queries map[string]string
	if err := json.Unmarshal(queryRaw, &queries); err != nil || len(queries) != 7 {
		t.Fatal("seven native queries required")
	}
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
	raw, err := json.Marshal(map[string][]models.PlaybackProgress{"profile-a": progress})
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
	fixture.meta.UsersService = fallbackProfiles{models.User{ID: "profile-a", AccountID: "account"}}
	if _, err = fixture.hidden.Hide("profile-a", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Hidden"}); err != nil {
		t.Fatal(err)
	}
	normal := NewHistoryHandler(service, nil, false)
	fixture.display.SetHistoryHandler(normal)
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/history/continue", normal.ListContinueWatching).Methods("GET")
	router.HandleFunc("/api/users/{userID}/display-list", fixture.display.Get).Methods("GET")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile-a" && a == "account" })(router).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	transport := collectionLoopbackTransport(server.Listener.Addr().String())
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	get := func(path string) (*http.Request, *http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024+1))
		_ = response.Body.Close()
		if err != nil || len(body) > 4*1024*1024 {
			t.Fatal("invalid bounded response")
		}
		return req, response, body
	}
	validate := func(req *http.Request, resp *http.Response, body []byte) {
		t.Helper()
		if err := validator.ValidateRequest(req, nil); err != nil {
			t.Fatal(err)
		}
		if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, body); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range queries {
		u, err := url.ParseRequestURI(value)
		if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Path != "/api/users/profile-a/display-list" || u.Query().Get("source") != "continue-watching-cached" {
			t.Fatal("unexpected native URL")
		}
	}
	req, resp, body := get(queries["unfiltered"])
	if resp.StatusCode != 409 || normalMeta.movies.Load() != 0 || normalMeta.series.Load() != 0 {
		t.Fatal("missing pool caused warmup")
	}
	validate(req, resp, body)
	_, resp, body = get("/api/users/profile-a/history/continue")
	var normalRows []models.SeriesWatchState
	if resp.StatusCode != 200 || json.Unmarshal(body, &normalRows) != nil || len(normalRows) != 702 {
		t.Fatal("normal complete producer failed")
	}
	var show models.SeriesWatchState
	for _, row := range normalRows {
		if row.SeriesID == "tmdb:tv:800" {
			show = row
		}
	}
	if show.NextEpisode == nil || show.NextEpisode.ItemID != "tmdb:tv:800:s01e02" {
		t.Fatal("canonical producer episode missing")
	}
	observation, err := service.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	movies, series := normalMeta.movies.Load(), normalMeta.series.Load()
	cases := []struct {
		name         string
		total, count int
		ids          []string
	}{
		{"unfiltered", 700, 2, nil},
		{"continuation", 3, 1, []string{"tmdb:movie:651"}},
		{"french", 2, 2, []string{"tmdb:movie:650", "tmdb:tv:800"}},
		{"unknown", 696, 696, nil},
		{"or-unknown", 699, 699, nil},
		{"random-pool", 3, 3, []string{"tmdb:movie:650", "tmdb:movie:651", "tmdb:tv:800"}},
		{"series", 1, 1, []string{"tmdb:tv:800"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, resp, body := get(queries[c.name])
			if resp.StatusCode != 200 {
				t.Fatalf("native HTTP status=%d", resp.StatusCode)
			}
			validate(req, resp, body)
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
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			if page.Source != "continue-watching-cached" || !page.Supported || page.Total != c.total || len(page.Items) != c.count || !page.Pool.Complete || page.Pool.ID != observation.ID || !page.Pool.ObservedAt.Equal(observation.ObservedAt) || !page.Pool.ExpiresAt.Equal(observation.ExpiresAt) {
				t.Fatalf("wrong pool/page total=%d count=%d", page.Total, len(page.Items))
			}
			ids := make([]string, 0, len(page.Items))
			for _, row := range page.Items {
				ids = append(ids, row.SeriesID)
				if row.SeriesID == "tmdb:movie:1" || row.SeriesID == "tmdb:movie:2" {
					t.Fatal("suppressed row escaped")
				}
				if row.SeriesID == "tmdb:tv:800" && (row.NextEpisode == nil || row.NextEpisode.ItemID != show.NextEpisode.ItemID || row.NextEpisode.EpisodeID != show.NextEpisode.EpisodeID || !reflect.DeepEqual(row.Resume, show.Resume) || !row.UpdatedAt.Equal(show.UpdatedAt)) {
					t.Fatal("canonical episode or resume changed")
				}
			}
			if c.ids != nil && !reflect.DeepEqual(ids, c.ids) {
				t.Fatalf("wrong selected IDs %v", ids)
			}
			if resp.Header.Get("Cache-Control") != "private, no-store" || strings.Contains(string(body), "private.invalid") || strings.Contains(string(body), "posterUrl") {
				t.Fatal("non-whitelisted fields")
			}
			if normalMeta.movies.Load() != movies || normalMeta.series.Load() != series || denied.calls.Load() != 0 || fixture.repo.calls.Load() != 0 || fixture.repo.writes.Load() != 0 || fixture.ratings.observes.Load() != 0 {
				t.Fatal("cached request caused excluded work")
			}
		})
	}
	after, err := service.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	afterBytes, err := json.Marshal(after)
	if err != nil || !bytes.Equal(frozen, afterBytes) {
		t.Fatal("native query changed pool")
	}
	latest, err := os.ReadFile(queryFile)
	if err != nil || !bytes.Equal(queryRaw, latest) {
		t.Fatal("native queries changed during run")
	}
}
