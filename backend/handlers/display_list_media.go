package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"novastream/internal/auth"
	"novastream/internal/mediaidentity"
	"novastream/models"
	"novastream/services/localmedia"
)

type storedListLocalMedia interface {
	ListLibraries(context.Context) ([]models.LocalMediaLibrary, error)
	ListAllItemsByLibrary(context.Context, string) ([]models.LocalMediaItem, error)
}
type storedListRemoteMedia interface {
	ListLibraries(context.Context) ([]models.RemoteMediaLibrary, error)
	StoredFilterVersions(context.Context, string) ([]models.LocalMediaItem, error)
}
type storedListLibraryAccess interface {
	CanAccess(context.Context, string, string, string, bool) (bool, error)
}

// SetStoredMediaLibraries attaches the same catalogue repositories and library
// access checks used by library browsing. No provider or stream opener is used.
func (h *DisplayListHandler) SetStoredMediaLibraries(local storedListLocalMedia, remote storedListRemoteMedia, access storedListLibraryAccess) {
	h.storedMediaLocal, h.storedMediaRemote, h.storedMediaAccess = local, remote, access
}
func (h *DisplayListHandler) storedMediaReady() bool {
	return h.storedMediaAccess != nil && (h.storedMediaLocal != nil || h.storedMediaRemote != nil)
}
func storedListMediaBasis(active bool) string {
	if active {
		return "stored_library_versions"
	}
	return ""
}
func savedListMediaSupported(r *http.Request) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source"))) {
	case "", "watchlist", "custom-list", "custom_user_list", "custom-user-list":
		return true
	case "mdblist", "mdblist-url", "mdblist-shelf":
		return strings.HasPrefix(strings.TrimSpace(r.URL.Query().Get("url")), customListShelfURLPrefix)
	default:
		return false
	}
}
func storedLibraryKind(kind models.LocalMediaLibraryType) string {
	switch kind {
	case models.LocalMediaLibraryTypeMovie:
		return "movie"
	case models.LocalMediaLibraryTypeShow:
		return "series"
	}
	return ""
}
func storedLibraryIDs(ids *models.LocalMediaExternalIDs) map[string]string {
	if ids == nil {
		return nil
	}
	return map[string]string{"tmdb": ids.TMDB, "tvdb": ids.TVDB, "imdb": ids.IMDB}
}
func storedLibraryIdentity(item models.LocalMediaItem, kind string) mediaidentity.Identity {
	id := strings.TrimSpace(item.MatchedTitleID)
	ids := storedLibraryIDs(item.ExternalIDs)
	if item.MatchStatus == models.LocalMediaMatchStatusManual && id != "" {
		ids = nil
	}
	if id == "" && item.Metadata != nil {
		id = item.Metadata.ID
	}
	return mediaidentity.Resolve(mediaidentity.Input{MediaType: kind, ID: id, ExternalIDs: ids})
}

// The existential test is per saved file. For a series this means at least one
// stored episode/version, not that every episode has the requested tracks.
func (h *DisplayListHandler) filterSavedListMedia(r *http.Request, profile string, items []models.WatchlistItem, filter models.LocalMediaFilters) ([]models.WatchlistItem, error) {
	if !h.storedMediaReady() {
		return nil, errors.New("stored catalogue unavailable")
	}
	index := make(map[string][]*models.LocalMediaProbe)
	add := func(kind string, rows []models.LocalMediaItem) {
		if kind == "" {
			return
		}
		for _, row := range rows {
			if row.IsMissing || row.MatchStatus == models.LocalMediaMatchStatusUnmatched || row.MatchStatus == models.LocalMediaMatchStatusLowConfidence {
				continue
			}
			identity := storedLibraryIdentity(row, kind)
			if identity.ID == "" {
				continue
			}
			for _, key := range identity.IndexKeys() {
				index[key] = append(index[key], row.Probe)
			}
		}
	}
	allowed := func(library string) (bool, error) {
		return h.storedMediaAccess.CanAccess(r.Context(), library, auth.GetAccountID(r), profile, auth.IsMaster(r))
	}
	if h.storedMediaLocal != nil {
		libraries, err := h.storedMediaLocal.ListLibraries(r.Context())
		if err != nil {
			return nil, err
		}
		for _, library := range libraries {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			ok, err := allowed(library.ID)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			rows, err := h.storedMediaLocal.ListAllItemsByLibrary(r.Context(), library.ID)
			if err != nil {
				return nil, err
			}
			add(storedLibraryKind(library.Type), rows)
		}
	}
	if h.storedMediaRemote != nil {
		libraries, err := h.storedMediaRemote.ListLibraries(r.Context())
		if err != nil {
			return nil, err
		}
		for _, library := range libraries {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			ok, err := allowed(library.ID)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			rows, err := h.storedMediaRemote.StoredFilterVersions(r.Context(), library.ID)
			if err != nil {
				return nil, err
			}
			add(storedLibraryKind(library.Type), rows)
		}
	}
	selected := make([]models.WatchlistItem, 0, len(items))
	for _, item := range items {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		identity := mediaidentity.Resolve(mediaidentity.Input{MediaType: item.MediaType, ID: item.ID, ExternalIDs: item.ExternalIDs})
		found, match := false, false
		for _, key := range identity.IndexKeys() {
			for _, probe := range index[key] {
				found = true
				match = match || localmedia.MatchesMediaFilters(probe, filter)
			}
		}
		if !found {
			match = localmedia.MatchesMediaFilters(nil, filter)
		}
		if match {
			selected = append(selected, item)
		}
	}
	return selected, nil
}

func (h *DisplayListHandler) enrichStoredMediaWatchState(profile string, items []models.WatchlistItem) {
	if h.HistoryService == nil {
		return
	}
	history, err := h.HistoryService.ListWatchHistory(profile)
	if err != nil {
		return
	}
	series, _ := h.HistoryService.ListSeriesStates(profile)
	progress, _ := h.HistoryService.ListPlaybackProgress(profile)
	enrichWatchlistItems(items, buildWatchStateIndex(history, series, progress), nil, false)
}
