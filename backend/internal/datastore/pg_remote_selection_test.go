package datastore

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
	"novastream/models"
)

func TestRemoteAutomaticSelectionPostgresJSONRoundtrip(t *testing.T) {
	raw := os.Getenv("AUTOMATIC_LIBRARY_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("disposable PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("a synthetic Unix-socket fixture database is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect to fixture")
	}
	defer admin.Close()
	dbName := fmt.Sprintf("synthetic_automatic_library_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
	}()
	target := *u
	target.Path = "/" + dbName
	pool, err := pgxpool.New(ctx, target.String())
	if err != nil {
		t.Fatal("connect to disposable fixture")
	}
	defer pool.Close()
	if err := runMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	repo := &pgRemoteMediaRepo{pool: pool}
	now := time.Now().UTC()
	library := &models.RemoteMediaLibrary{ID: "library", Name: "Synthetic", Type: models.LocalMediaLibraryTypeShow, Provider: models.MediaSourcePlex, AccountID: "synthetic-account", ExternalLibraryID: "1", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateLibrary(ctx, library); err != nil {
		t.Fatal(err)
	}
	item := &models.RemoteMediaItem{ID: "version", LibraryID: library.ID, ExternalItemID: "episode", ExternalMediaID: "part", GroupKey: "show", LibraryType: library.Type, StreamPath: "plexmedia:version", SinglePart: true, SeriesExternalIDs: &models.LocalMediaExternalIDs{TMDB: "123"}, EpisodeExternalIDs: &models.LocalMediaExternalIDs{TVDB: "456"}, AudioLanguages: []string{"fra"}, ProviderData: map[string]string{"partKey": "/library/parts/part/file"}, CreatedAt: now, UpdatedAt: now}
	if err := repo.UpsertItem(ctx, item); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetItem(ctx, item.ID)
	if err != nil || got == nil || !got.SinglePart || got.SeriesExternalIDs == nil || got.SeriesExternalIDs.TMDB != "123" || got.EpisodeExternalIDs == nil || got.EpisodeExternalIDs.TVDB != "456" || len(got.AudioLanguages) != 1 || got.AudioLanguages[0] != "fra" {
		t.Fatalf("typed facts did not survive existing JSON column: %+v %v", got, err)
	}
	// A later ordinary sync with missing parent/part evidence must erase the
	// previous proof instead of resurrecting the old JSON envelope.
	models.ClearRemoteSelectionProof(got)
	if err := repo.UpsertItem(ctx, got); err != nil {
		t.Fatal(err)
	}
	got, err = repo.GetItem(ctx, item.ID)
	if err != nil || got == nil || got.SinglePart || got.SeriesExternalIDs != nil || got.EpisodeExternalIDs != nil || got.ProviderData["partKey"] != "/library/parts/part/file" {
		t.Fatal("unknown sync retained old proof or broke manual playback")
	}
}
