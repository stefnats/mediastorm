package localmedia

import (
	"context"
	"path/filepath"
	"testing"

	"novastream/models"
)

func TestAuthorizationSnapshotDoesNotInspectOrOpenLocalFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "does-not-exist.mkv")
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", RootPath: root}, items: map[string]*models.LocalMediaItem{"item": {ID: "item", LibraryID: "library", FilePath: path, ExternalIDs: &models.LocalMediaExternalIDs{TMDB: "41"}, Metadata: &models.Title{ID: "tmdb:movie:41", Poster: &models.Image{URL: "original"}}, LastSeenScanID: "scan"}}}
	s := &Service{repo: repo}
	item, err := s.SnapshotItemForAuthorization(context.Background(), "item")
	if err != nil || item.FilePath != path || item.LastSeenScanID != "scan" {
		t.Fatalf("pure snapshot inspected missing file: %+v %v", item, err)
	}
	item.ExternalIDs.TMDB = "99"
	item.Metadata.Poster.URL = "changed"
	item.FilePath = "outside"
	again, err := s.SnapshotItemForAuthorization(context.Background(), "item")
	if err != nil || again.ExternalIDs.TMDB != "41" || again.Metadata.Poster.URL != "original" || again.FilePath != path {
		t.Fatal("snapshot changed repository metadata")
	}
	if _, err = s.GetItem(context.Background(), "item"); err == nil {
		t.Fatal("normal GetItem control did not inspect missing file")
	}
}
func TestAuthorizationSnapshotRejectsMissingAndOutOfRootWithoutIO(t *testing.T) {
	root := t.TempDir()
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", RootPath: root}, items: map[string]*models.LocalMediaItem{"item": {ID: "item", LibraryID: "library", FilePath: filepath.Join(root, "..", "outside.mkv")}}}
	s := &Service{repo: repo}
	if _, err := s.SnapshotItemForAuthorization(context.Background(), "item"); err == nil {
		t.Fatal("outside path accepted")
	}
	repo.items["item"].FilePath = filepath.Join(root, "missing.mkv")
	repo.items["item"].IsMissing = true
	if _, err := s.SnapshotItemForAuthorization(context.Background(), "item"); err == nil {
		t.Fatal("known missing item accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SnapshotItemForAuthorization(ctx, "item"); err != context.Canceled {
		t.Fatal("cancelled read accepted")
	}
}
