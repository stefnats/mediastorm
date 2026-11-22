package remotemedia

import (
	"context"
	"novastream/models"
)

// StoredFilterVersions projects saved provider parts without opening streams,
// refreshing metadata, or applying a page limit. The caller checks library access.
func (s *Service) StoredFilterVersions(ctx context.Context, libraryID string) ([]models.LocalMediaItem, error) {
	library, err := s.repo.GetLibrary(ctx, libraryID)
	if err != nil {
		return nil, err
	}
	if library == nil {
		return nil, ErrNotFound
	}
	rows, err := s.repo.ListItems(ctx, libraryID, false)
	if err != nil {
		return nil, err
	}
	items := make([]models.LocalMediaItem, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if row.IsMissing || row.LibraryID != library.ID {
			continue
		}
		item := toLocalItem(library, row)
		// A series title is associated with its saved parent identity, never with
		// an individual episode's provider IDs. Missing parent IDs stay unknown.
		if library.Type == models.LocalMediaLibraryTypeShow {
			item.ExternalIDs = row.SeriesExternalIDs
		}
		items = append(items, item)
	}
	return items, nil
}
