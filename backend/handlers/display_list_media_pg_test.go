package handlers

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/remotemedia"
)

func TestSavedMediaFiltersPostgresLibrarySnapshots(t *testing.T) {
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	target, err := url.Parse(raw)
	if err != nil || target.Scheme != "postgresql" || target.Host != "" || target.User == nil || !strings.HasPrefix(target.User.Username(), "synthetic_") || !strings.HasPrefix(target.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket PostgreSQL required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("synthetic_saved_media_%d", time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	query := target.Query()
	query.Set("search_path", schema)
	target.RawQuery = query.Encode()
	store, err := datastore.New(t.Context(), target.String(), datastore.WithMaxConns(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	h, m := catalogueLanguageFixture(t)
	for _, n := range []int{1, 2, 3} {
		catalogueLanguageSeed(t, h, m, fmt.Sprintf("tmdb:movie:%d", n), fmt.Sprintf("Movie %d", n), "")
	}
	now := time.Now().UTC()
	local := &models.LocalMediaLibrary{ID: "local", Name: "Stored local", Type: models.LocalMediaLibraryTypeMovie, RootPath: t.TempDir(), CreatedAt: now, UpdatedAt: now}
	if err := store.LocalMedia().CreateLibrary(t.Context(), local); err != nil {
		t.Fatal(err)
	}
	row := savedMediaRow("one", "tmdb:movie:1", savedMediaProbe("fra", "aac", "hevc"))
	row.FileName = "one.mkv"
	row.RelativePath = row.FileName
	row.CreatedAt = now
	row.UpdatedAt = now
	if err := store.LocalMedia().UpsertItem(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
	remote := &models.RemoteMediaLibrary{ID: "remote", Name: "Stored Jellyfin", Provider: models.MediaSourceJellyfin, Type: models.LocalMediaLibraryTypeMovie, AccountID: "synthetic-account", ExternalLibraryID: "synthetic-movies", CreatedAt: now, UpdatedAt: now}
	if err := store.RemoteMedia().CreateLibrary(t.Context(), remote); err != nil {
		t.Fatal(err)
	}
	version := &models.RemoteMediaItem{ID: "two", LibraryID: remote.ID, ExternalItemID: "movie-two", ExternalMediaID: "part-two", GroupKey: "movie-two", LibraryType: remote.Type, Title: "Movie 2", ExternalIDs: &models.LocalMediaExternalIDs{TMDB: "2"}, VideoCodec: "hevc", Width: 3840, Height: 2160, StreamPath: "jellyfinmedia:two", CreatedAt: now, UpdatedAt: now}
	if err := store.RemoteMedia().UpsertItem(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	// The real grant table references real profiles, unlike the in-memory
	// catalogue fixture used to supply the visible list itself.
	if _, err := store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash) VALUES('synthetic-library-account','Synthetic library viewer','synthetic unused hash'); INSERT INTO users(id,account_id,name) VALUES('another-profile','synthetic-library-account','Synthetic other viewer')`); err != nil {
		t.Fatal(err)
	}
	access := libraryaccess.New(store.LibraryAccess(), store.LocalMedia(), store.RemoteMedia())
	for _, id := range []string{local.ID, remote.ID} {
		if err := access.Set(t.Context(), models.LibraryAccessPolicy{LibraryID: id, AccessMode: models.LibraryAccessModeAll}); err != nil {
			t.Fatal(err)
		}
	}
	service, err := remotemedia.NewService(store, config.NewManager(t.TempDir()+"/unused.json"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Nil provider clients make any accidental provider/probe path fail; this
	// endpoint must consume only the actual persisted catalogue rows.
	h.SetStoredMediaLibraries(store.LocalMedia(), service, access)
	response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "videoCodec=hevc&minHeight=2160&mediaUnknown=exclude&sortBy=name&limit=1&offset=1"))
	if response.Total != 2 || len(items) != 1 || items[0].ID != "tmdb:movie:2" {
		t.Fatalf("persisted local/remote page: %+v %+v", response, items)
	}
	if err := access.Set(t.Context(), models.LibraryAccessPolicy{LibraryID: remote.ID, AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"another-profile"}}); err != nil {
		t.Fatal(err)
	}
	response, items = catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "videoCodec=hevc&mediaUnknown=exclude&sortBy=name"))
	if response.Total != 1 || len(items) != 1 || items[0].ID != "tmdb:movie:1" {
		t.Fatalf("current existing library policy not used: %+v %+v", response, items)
	}
}
