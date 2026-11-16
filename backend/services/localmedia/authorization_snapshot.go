package localmedia

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"novastream/models"
)

// SnapshotItemForAuthorization reads catalogue state only. Unlike GetItem and
// GetFallbackItem it performs no Stat/Open, metadata lookup or probe. A snapshot
// is not a fresh file/catalogue correspondence proof by itself.
func (s *Service) SnapshotItemForAuthorization(ctx context.Context, id string) (*models.LocalMediaItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, ErrItemNotFound
	}
	item, err := s.repo.GetItem(ctx, id)
	if err != nil {
		return nil, err
	}
	if item == nil || item.IsMissing {
		return nil, ErrItemNotFound
	}
	library, err := s.repo.GetLibrary(ctx, item.LibraryID)
	if err != nil {
		return nil, err
	}
	if library == nil {
		return nil, ErrLibraryNotFound
	}
	path := filepath.Clean(strings.TrimSpace(item.FilePath))
	if path == "." || path == "" || !pathWithinLibraryRoots(*library, path) {
		return nil, ErrItemNotFound
	}
	data, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	var copy models.LocalMediaItem
	if err = json.Unmarshal(data, &copy); err != nil {
		return nil, err
	}
	copy.FilePath = path
	copy.LastSeenScanID = item.LastSeenScanID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &copy, nil
}
