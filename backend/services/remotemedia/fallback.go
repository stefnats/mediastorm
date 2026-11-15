package remotemedia

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"novastream/models"
)

// FindPlaybackCandidates reads only the last completed library scan. Manual
// browsing remains available when identity or complete-part proof is absent.
func (s *Service) FindPlaybackCandidates(ctx context.Context, query models.LocalMediaMatchQuery, episode *models.EpisodeReference, allowLibrary func(string) bool) ([]models.RemoteMediaItem, error) {
	if allowLibrary == nil || remoteLookupLibraryType(query.MediaType) == "" {
		return nil, nil
	}
	libraries, err := s.repo.ListLibraries(ctx)
	if err != nil {
		return nil, err
	}
	var found []models.RemoteMediaItem
	for _, library := range libraries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if library.Type != remoteLookupLibraryType(query.MediaType) || (library.Provider != models.MediaSourcePlex && library.Provider != models.MediaSourceJellyfin) || !allowLibrary(library.ID) {
			continue
		}
		items, err := s.repo.ListItems(ctx, library.ID, false)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if item.LibraryID != library.ID || item.LibraryType != library.Type || !completeRemotePart(item, library.Provider) {
				continue
			}
			ids := item.ExternalIDs
			if library.Type == models.LocalMediaLibraryTypeShow {
				ids = item.SeriesExternalIDs
				if episode == nil || !models.OfficialEpisodeCoordinates(query.TitleID, episode, seriesIDs(ids)...) || item.SeasonNumber != episode.SeasonNumber || item.EpisodeNumber != episode.EpisodeNumber || !episodeIdentityMatches(item.EpisodeExternalIDs, episode) {
					continue
				}
			}
			if exactIdentityMatches(ids, query) {
				found = append(found, item)
			}
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].ID < found[j].ID })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return found, nil
}

func (s *Service) GetFallbackItem(ctx context.Context, id string) (*models.RemoteMediaItem, error) {
	item, err := s.repo.GetItem(ctx, id)
	if err != nil || item == nil {
		return nil, ErrNotFound
	}
	library, err := s.repo.GetLibrary(ctx, item.LibraryID)
	if err != nil || library == nil || item.LibraryType != library.Type || !completeRemotePart(*item, library.Provider) {
		return nil, ErrNotFound
	}
	return item, nil
}

func completeRemotePart(item models.RemoteMediaItem, provider string) bool {
	if item.IsMissing || !item.SinglePart || item.ID == "" || item.ExternalItemID == "" || item.ExternalMediaID == "" || item.StreamPath != provider+"media:"+item.ID {
		return false
	}
	switch provider {
	case models.MediaSourcePlex:
		// Automatic preparation must never enter the legacy part repair path,
		// whose manual compatibility fallback can select a different version.
		return strings.TrimSpace(item.ProviderData["partKey"]) != ""
	case models.MediaSourceJellyfin:
		return item.ProviderData["itemId"] == item.ExternalItemID && item.ProviderData["mediaSourceId"] == item.ExternalMediaID
	}
	return false
}

func seriesIDs(ids *models.LocalMediaExternalIDs) []string {
	if ids == nil {
		return nil
	}
	var result []string
	if ids.TMDB != "" {
		result = append(result, "tmdb:series:"+ids.TMDB)
	}
	if ids.TVDB != "" {
		result = append(result, "tvdb:series:"+ids.TVDB)
	}
	if ids.IMDB != "" {
		result = append(result, ids.IMDB)
	}
	return result
}

// Every overlapping provider ID must agree. Names and local group keys never
// establish an automatic match, even when a localized title happens to agree.
func exactIdentityMatches(ids *models.LocalMediaExternalIDs, query models.LocalMediaMatchQuery) bool {
	if ids == nil {
		return false
	}
	requested := map[string]string{"imdb": query.IMDBID, "tmdb": query.TMDBID, "tvdb": query.TVDBID}
	id := strings.ToLower(strings.TrimSpace(query.TitleID))
	parts := strings.Split(id, ":")
	provider, value := "", ""
	if strings.HasPrefix(id, "tt") && !strings.Contains(id, ":") {
		provider, value = "imdb", id
	} else if len(parts) == 3 && (parts[0] == "tmdb" || parts[0] == "tvdb") {
		kind := "movie"
		if remoteLookupLibraryType(query.MediaType) == models.LocalMediaLibraryTypeShow {
			kind = "series"
		}
		if parts[1] != kind && !(kind == "series" && parts[1] == "tv") {
			return false
		}
		provider, value = parts[0], parts[2]
	} else if len(parts) == 2 && parts[0] == "tvdb" && remoteLookupLibraryType(query.MediaType) == models.LocalMediaLibraryTypeShow {
		provider, value = "tvdb", parts[1]
	}
	if provider != "" {
		if prior := strings.TrimSpace(requested[provider]); prior != "" && !strings.EqualFold(prior, value) {
			return false
		}
		requested[provider] = value
	}
	matched := false
	for provider, actual := range map[string]string{"imdb": ids.IMDB, "tmdb": ids.TMDB, "tvdb": ids.TVDB} {
		want := strings.TrimSpace(requested[provider])
		actual = strings.TrimSpace(actual)
		if want == "" || actual == "" {
			continue
		}
		if !strings.EqualFold(actual, want) {
			return false
		}
		matched = true
	}
	return matched
}

func episodeIdentityMatches(ids *models.LocalMediaExternalIDs, episode *models.EpisodeReference) bool {
	if ids == nil || episode == nil {
		return false
	}
	matched := false
	check := func(actual, expected string) bool {
		actual, expected = strings.TrimSpace(actual), strings.TrimSpace(expected)
		if actual == "" || expected == "" || expected == "0" {
			return true
		}
		if actual != expected {
			return false
		}
		matched = true
		return true
	}
	if !check(ids.TVDB, episode.TvdbID) {
		return false
	}
	parts := strings.Split(strings.ToLower(strings.TrimSpace(episode.EpisodeID)), ":")
	if len(parts) == 3 && parts[1] == "episode" {
		if n, err := strconv.ParseInt(parts[2], 10, 64); err == nil && n > 0 {
			switch parts[0] {
			case "tmdb":
				if !check(ids.TMDB, parts[2]) {
					return false
				}
			case "tvdb":
				if !check(ids.TVDB, parts[2]) {
					return false
				}
			}
		}
	}
	return matched
}
