package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/localmedia"
	"novastream/services/remotemedia"
)

func TestLocalLibraryLanguagePostgresActualFullIndexAndRemoteRejection(t *testing.T) {
	raw := os.Getenv("LOCAL_LIBRARY_LANGUAGE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("synthetic PostgreSQL socket fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic database")
	}
	defer admin.Close()
	schema := fmt.Sprintf("synthetic_local_library_language_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal("create own test schema")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own schema")
		}
	}()
	target := *u
	q := target.Query()
	q.Set("search_path", schema)
	target.RawQuery = q.Encode()
	store, err := datastore.New(ctx, target.String(), datastore.WithMaxConns(2))
	if err != nil {
		t.Fatal("migrate own schema")
	}
	defer store.Close()
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	if err := store.LocalMedia().CreateLibrary(ctx, &models.LocalMediaLibrary{ID: "local", Name: "Synthetic local", Type: models.LocalMediaLibraryTypeMovie, RootPath: "/must-not-stat", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= 652; n++ {
		for v := 1; v <= 2; v++ {
			id := fmt.Sprintf("item-%03d-%d", n, v)
			item := &models.LocalMediaItem{ID: id, LibraryID: "local", RelativePath: id + ".mkv", FilePath: "/must-not-stat/" + id, FileName: id, LibraryType: models.LocalMediaLibraryTypeMovie, MatchedTitleID: fmt.Sprintf("tmdb:movie:%d", n), MatchedMediaType: "movie", MatchedName: fmt.Sprintf("Saved %03d", n), MatchStatus: models.LocalMediaMatchStatusMatched, SizeBytes: 1234, CreatedAt: now, UpdatedAt: now}
			if err := store.LocalMedia().UpsertItem(ctx, item); err != nil {
				t.Fatal(err)
			}
		}
	}
	reader, hashes := localLibraryLanguageCache(t)
	_ = hashes
	service, err := localmedia.NewService(store, reader, "must-not-execute-ffprobe")
	if err != nil {
		t.Fatal(err)
	}
	cfg := metadataLanguageManager(t, []string{"eng", "ita"}, "eng")
	h := NewLocalMediaHandler(service, nil, false)
	h.SetMetadataLanguageProviders(reader, cfg, &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"profile-a": {Metadata: models.MetadataSettings{PrimaryLanguage: "ita"}}}})
	remote, err := remotemedia.NewService(store, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.SetRemoteMediaService(remote)
	if err := store.RemoteMedia().CreateLibrary(ctx, &models.RemoteMediaLibrary{ID: "remote", Name: "Synthetic remote", Type: models.LocalMediaLibraryTypeMovie, Provider: "plex", AccountID: "synthetic", ExternalLibraryID: "remote", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	old := http.DefaultTransport
	transport := &catalogueWireTransport{}
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old })
	before, err := store.LocalMedia().ListAllItemsByLibrary(ctx, "local")
	if err != nil {
		t.Fatal(err)
	}
	original := map[string]models.LocalMediaItem{}
	for _, item := range before {
		original[item.ID] = item
	}
	t.Run("full stored index later page", func(t *testing.T) {
		out := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&profileId=profile-a&includeUnknownOriginalLanguage=true&sort=name&dir=asc&limit=1&offset=643"))
		// Six current known catalogue facts are excluded, leaving646 groups. This
		// page reaches the end of the FULL saved index, not a retained first500.
		if out.Total != 646 || out.Groups[0].ID != "movie:tmdb:movie:650" || out.Groups[0].ItemCount != 2 || out.Groups[0].TotalSizeBytes != 2468 {
			t.Fatalf("full index %+v", out)
		}
	})
	t.Run("actual language selection", func(t *testing.T) {
		out := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "include=cards&originalLanguages=fra,haw&limit=1&offset=1&sort=name&dir=asc"))
		if out.Total != 2 || out.Groups[0].ID != "movie:tmdb:movie:3" || out.Groups[0].ItemCount != 2 {
			t.Fatal(out)
		}
	})
	t.Run("remote normal no marker", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/library/libraries/remote/groups?include=cards", nil)
		req = mux.SetURLVars(req, map[string]string{"libraryID": "remote"})
		w := httptest.NewRecorder()
		h.ListGroups(w, req)
		if w.Code != 200 || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
			t.Fatalf("remote legacy %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("stored media capability local and remote actual readers", func(t *testing.T) {
		local := localLibraryLanguageDecode(t, localLibraryLanguageRequest(h, "audioLanguage=fr&mediaUnknown=only&limit=1&offset=650"))
		if !local.StoredMediaFilterSupported || local.StoredMediaFilterBasis != "stored_library_versions" || local.Total != 652 || len(local.Groups) != 1 || local.Offset != 650 {
			t.Fatalf("actual local stored unknown facts/page contract: %+v", local)
		}
		for _, provider := range []string{models.MediaSourcePlex, models.MediaSourceJellyfin} {
			libraryID := "marker-" + provider
			if err := store.RemoteMedia().CreateLibrary(ctx, &models.RemoteMediaLibrary{ID: libraryID, Name: "Synthetic marker", Type: models.LocalMediaLibraryTypeMovie, Provider: provider, AccountID: "synthetic", ExternalLibraryID: libraryID, CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			for n := 0; n < 2; n++ {
				id := fmt.Sprintf("%s-%d", libraryID, n)
				if err := store.RemoteMedia().UpsertItem(ctx, &models.RemoteMediaItem{ID: id, LibraryID: libraryID, ExternalItemID: id, ExternalMediaID: id, GroupKey: id, LibraryType: models.LocalMediaLibraryTypeMovie, Title: id, FileName: id + ".mkv", StreamPath: provider + "media:" + id, CreatedAt: now, UpdatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			r := httptest.NewRequest("GET", "/api/library/libraries/"+libraryID+"/groups?include=cards&audioLanguage=fr&mediaUnknown=only&limit=1&offset=1", nil)
			r = mux.SetURLVars(r, map[string]string{"libraryID": libraryID})
			w := httptest.NewRecorder()
			h.ListGroups(w, r)
			remote := localLibraryLanguageDecode(t, w)
			if !remote.StoredMediaFilterSupported || remote.StoredMediaFilterBasis != "stored_library_versions" || remote.OriginalLanguageFilterSupported || remote.Total != 2 || len(remote.Groups) != 1 || remote.Offset != 1 {
				t.Fatalf("actual %s stored unknown facts/page contract: %+v", provider, remote)
			}
		}
	})
	// Poison the REAL remote item query while retaining the actual library row.
	// A filtered remote request must reject before touching this missing table.
	if _, err := store.Pool().Exec(ctx, "ALTER TABLE remote_media_items RENAME TO own_poison_remote_items"); err != nil {
		t.Fatal(err)
	}
	t.Run("remote active before actual list", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/library/libraries/remote/groups?include=cards&originalLanguages=fra", nil)
		req = mux.SetURLVars(req, map[string]string{"libraryID": "remote"})
		w := httptest.NewRecorder()
		factsBefore := reader.reads.Load()
		h.ListGroups(w, req)
		if w.Code != 400 || reader.reads.Load() != factsBefore {
			t.Fatalf("remote filter %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("poison control real list fails", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/library/libraries/remote/groups?include=cards", nil)
		req = mux.SetURLVars(req, map[string]string{"libraryID": "remote"})
		w := httptest.NewRecorder()
		h.ListGroups(w, req)
		if w.Code != 500 {
			t.Fatalf("poison not reached %d", w.Code)
		}
	})
	t.Run("concurrent independent views", func(t *testing.T) {
		var wg sync.WaitGroup
		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				query := "include=cards&originalLanguages=fra&limit=1"
				expected := 1
				if n%2 == 1 {
					query = "include=cards&includeUnknownOriginalLanguage=true&limit=1&offset=643&sort=name&dir=asc"
					expected = 646
				}
				response := localLibraryLanguageRequest(h, query)
				var page models.LocalMediaGroupListResult
				if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &page) != nil || page.Total != expected || len(page.Groups) != 1 {
					t.Errorf("concurrent view%d status%d total%d", n, response.Code, page.Total)
				}
			}(n)
		}
		wg.Wait()
	})
	after, err := store.LocalMedia().ListAllItemsByLibrary(ctx, "local")
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]models.LocalMediaItem{}
	for _, item := range after {
		actual[item.ID] = item
	}
	if !reflect.DeepEqual(original, actual) {
		a, _ := json.Marshal(original)
		b, _ := json.Marshal(actual)
		t.Fatalf("saved index changed lengths%d/%d", len(a), len(b))
	}
	if transport.calls.Load() != 0 {
		t.Fatal("extra HTTP")
	}
}
