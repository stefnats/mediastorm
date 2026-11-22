package localmedia

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"novastream/internal/importer"
	"novastream/models"
	"novastream/services/arrclient"
)

func (s *Service) ValidateARRLibrary(ctx context.Context, id, kind string) error {
	library, e := s.GetLibrary(ctx, id)
	if e != nil {
		return e
	}
	if library == nil {
		return ErrLibraryNotFound
	}
	if kind == "sonarr" && library.Type != models.LocalMediaLibraryTypeShow || kind == "radarr" && library.Type != models.LocalMediaLibraryTypeMovie {
		return fmt.Errorf("choose a matching local movie or series library")
	}
	return nil
}

// ImportARRFiles runs the normal library scanner and links the exact files from
// the app's import history. Playback keeps using the existing local resolver.
func (s *Service) ImportARRFiles(ctx context.Context, libraryID, remoteRoot string, files []arrclient.ImportedFile) (importer.ARRLibraryResult, error) {
	result := importer.ARRLibraryResult{ItemIDs: []string{}}
	work, finish, e := s.beginLocalWork(ctx)
	if e != nil {
		return result, e
	}
	defer finish()
	ctx = work
	library, e := s.repo.GetLibrary(ctx, libraryID)
	if e != nil {
		return result, e
	}
	if library == nil {
		return result, ErrLibraryNotFound
	}
	roots := libraryRoots(*library)
	if len(roots) == 0 {
		return result, ErrLibraryPathNeeded
	}
	wanted := map[string]arrclient.ImportedFile{}
	for _, file := range files {
		name := file.Path
		if remoteRoot != "" {
			relative, e := filepath.Rel(filepath.Clean(remoteRoot), file.Path)
			if e != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
				return result, fmt.Errorf("imported file does not match configured library path")
			}
			name = filepath.Join(roots[0], relative)
		}
		name = filepath.Clean(name)
		if !filepath.IsAbs(name) || !pathWithinLibraryRoots(*library, name) {
			return result, fmt.Errorf("imported file is outside configured local library")
		}
		if info, e := os.Lstat(name); e != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			result.Missing = true
			continue
		}
		if previous, exists := wanted[name]; exists && file.MediaType == "series" {
			left, leftOK := arrImportedEpisodes(previous)
			right, rightOK := arrImportedEpisodes(file)
			file.Episodes = append(left, right...)
			file.EpisodeMappingIncomplete = !leftOK || !rightOK || previous.TMDBID != file.TMDBID || previous.TVDBID != file.TVDBID || previous.IMDBID != file.IMDBID
			file.AmbiguousEpisodes = previous.AmbiguousEpisodes || file.AmbiguousEpisodes || len(file.Episodes) > 1
		}
		wanted[name] = file
	}
	if len(wanted) == 0 {
		result.Missing = true
		return result, nil
	}
	if _, e = s.RunScan(ctx, libraryID); e != nil {
		return result, e
	}
	items, e := s.repo.ListAllItemsByLibrary(ctx, libraryID)
	if e != nil {
		return result, e
	}
	found := 0
	for _, item := range items {
		file, ok := wanted[filepath.Clean(item.FilePath)]
		if !ok || item.IsMissing {
			continue
		}
		found++
		episodes, mapped := arrImportedEpisodes(file)
		combined := mapped && len(episodes) > 1
		unresolved := file.MediaType == "series" && (!mapped || file.AmbiguousEpisodes && !combined)
		manual := item.MatchStatus == models.LocalMediaMatchStatusManual
		if unresolved {
			result.NeedsMatch = true
			result.Reason = "multiple_episodes"
		}
		if manual {
			if arrItemIdentityConflict(item, file) || file.MediaType == "series" && !arrEpisodeMembershipMatches(item, episodes, mapped) {
				result.NeedsMatch = true
				if result.Reason == "" {
					result.Reason = "manual_match_conflict"
				}
			}
		} else {
			if metadata := arrImportedTitle(file); metadata != nil {
				if item.Metadata != nil && !arrItemIdentityConflict(item, file) {
					// Keep artwork/details for the same identity, while correcting the
					// nonmanual catalogue key to the app's definitive imported identity.
					enriched := *item.Metadata
					enriched.ID = metadata.ID
					enriched.MediaType = metadata.MediaType
					if metadata.TMDBID > 0 {
						enriched.TMDBID = metadata.TMDBID
					}
					if metadata.TVDBID > 0 {
						enriched.TVDBID = metadata.TVDBID
					}
					if metadata.IMDBID != "" {
						enriched.IMDBID = metadata.IMDBID
					}
					if metadata.Name != "" {
						enriched.Name = metadata.Name
					}
					if metadata.Year > 0 {
						enriched.Year = metadata.Year
					}
					metadata = &enriched
				}
				item.MatchedTitleID, item.MatchedMediaType, item.MatchedName, item.MatchedYear = metadata.ID, metadata.MediaType, metadata.Name, metadata.Year
				item.Metadata = metadata
				item.MatchStatus = models.LocalMediaMatchStatusMatched
				item.Confidence = 1
			}
			item.Episodes = nil
			if unresolved {
				item.SeasonNumber, item.EpisodeNumber = 0, 0
				item.MatchStatus = models.LocalMediaMatchStatusUnmatched
				item.Confidence = 0
			} else if combined {
				// Membership is complete; timing is not. Keep one physical item
				// without inventing a primary episode or intra-file seek offset.
				item.Episodes = episodes
				item.SeasonNumber, item.EpisodeNumber = 0, 0
			} else if file.MediaType == "series" && len(episodes) == 1 {
				item.SeasonNumber, item.EpisodeNumber = episodes[0].SeasonNumber, episodes[0].EpisodeNumber
			}
		}
		probe, probeErr := s.ProbeItemForPlayback(ctx, &item)
		if probeErr != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			if errors.Is(probeErr, ErrItemNotFound) {
				result.Missing = true
			} else {
				result.InvalidMedia = true
			}
		} else {
			item.Probe = probe
		}
		if e = s.repo.UpsertItem(ctx, &item); e != nil {
			return result, e
		}
		result.ItemIDs = append(result.ItemIDs, item.ID)
		if item.MatchedTitleID == "" || item.MatchStatus == models.LocalMediaMatchStatusUnmatched || item.MatchStatus == models.LocalMediaMatchStatusLowConfidence {
			result.NeedsMatch = true
		}
	}
	if found != len(wanted) {
		result.Missing = true
	}
	if e = s.refreshLibrarySummary(ctx, libraryID); e != nil {
		return result, e
	}
	return result, nil
}
func arrImportedTitle(file arrclient.ImportedFile) *models.Title {
	title := &models.Title{Name: file.Title, Year: file.Year, MediaType: file.MediaType, TMDBID: file.TMDBID, TVDBID: file.TVDBID, IMDBID: file.IMDBID}
	switch {
	case file.MediaType == "series" && file.TVDBID > 0:
		title.ID = fmt.Sprintf("tvdb:series:%d", file.TVDBID)
	case file.MediaType == "series" && file.TMDBID > 0:
		title.ID = fmt.Sprintf("tmdb:tv:%d", file.TMDBID)
	case file.MediaType == "movie" && file.TMDBID > 0:
		title.ID = fmt.Sprintf("tmdb:movie:%d", file.TMDBID)
	default:
		return nil
	}
	return title
}

func arrItemIdentityConflict(item models.LocalMediaItem, file arrclient.ImportedFile) bool {
	expected := arrImportedTitle(file)
	if expected == nil {
		return false
	}
	if item.MatchedMediaType != "" && item.MatchedMediaType != file.MediaType {
		return true
	}
	if item.MatchedTitleID != "" && item.MatchedTitleID != expected.ID {
		alternate := ""
		if file.TMDBID > 0 {
			if file.MediaType == "series" {
				alternate = fmt.Sprintf("tmdb:tv:%d", file.TMDBID)
			} else {
				alternate = fmt.Sprintf("tmdb:movie:%d", file.TMDBID)
			}
		}
		if item.MatchedTitleID != alternate && item.MatchedTitleID != "imdb:"+file.IMDBID {
			return true
		}
	}
	if item.Metadata == nil {
		return false
	}
	if file.TMDBID > 0 && item.Metadata.TMDBID > 0 && file.TMDBID != item.Metadata.TMDBID {
		return true
	}
	if file.TVDBID > 0 && item.Metadata.TVDBID > 0 && file.TVDBID != item.Metadata.TVDBID {
		return true
	}
	if file.IMDBID != "" && item.Metadata.IMDBID != "" && file.IMDBID != item.Metadata.IMDBID {
		return true
	}
	if file.TMDBID > 0 && item.Metadata.TMDBID == file.TMDBID || file.TVDBID > 0 && item.Metadata.TVDBID == file.TVDBID || file.IMDBID != "" && item.Metadata.IMDBID == file.IMDBID {
		return false
	}
	return item.MatchedTitleID != "" && item.MatchedTitleID != expected.ID
}

func arrImportedEpisodes(file arrclient.ImportedFile) ([]models.LocalMediaEpisode, bool) {
	if file.MediaType != "series" || file.EpisodeMappingIncomplete {
		return nil, false
	}
	if len(file.Episodes) > 0 {
		return models.NormalizeLocalMediaEpisodes(file.Episodes)
	}
	if file.AmbiguousEpisodes {
		return nil, false
	}
	return models.NormalizeLocalMediaEpisodes([]models.LocalMediaEpisode{{SeasonNumber: file.SeasonNumber, EpisodeNumber: file.EpisodeNumber}})
}

func arrEpisodeMembershipMatches(item models.LocalMediaItem, episodes []models.LocalMediaEpisode, mapped bool) bool {
	if !mapped {
		return false
	}
	existing := item.Episodes
	if len(existing) == 0 {
		existing = []models.LocalMediaEpisode{{SeasonNumber: item.SeasonNumber, EpisodeNumber: item.EpisodeNumber}}
	}
	normalized, ok := models.NormalizeLocalMediaEpisodes(existing)
	if !ok || len(normalized) != len(episodes) {
		return false
	}
	for i := range episodes {
		if normalized[i] != episodes[i] {
			return false
		}
	}
	return true
}
