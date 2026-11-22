package handlers

import (
	"context"
	"errors"
	"net/http"
	historypkg "novastream/services/history"
	"strconv"
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
func storedCatalogueMediaSupported(r *http.Request) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source"))) {
	case "", "watchlist", "custom-list", "custom_user_list", "custom-user-list",
		"permanent-prequeue", "hidden", "loved-stored", "continue-watching-cached",
		"collection-cached", "similar-cached", "recently-watched-cached",
		"popular-on-server-cached", "tmdb-public-list-cached", "tmdb-person-credits-cached":
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
func (h *DisplayListHandler) storedMediaMatcher(r *http.Request, profile string, filter models.LocalMediaFilters) (*storedTitleMediaMatcher, error) {
	if !filter.Active() {
		return &storedTitleMediaMatcher{}, nil
	}
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
			if kind == "series" && row.SeasonNumber >= 0 && row.EpisodeNumber > 0 {
				episode := mediaidentity.Resolve(mediaidentity.Input{MediaType: "episode", SeriesID: identity.ID, SeasonNumber: row.SeasonNumber, EpisodeNumber: row.EpisodeNumber, ExternalIDs: identity.ExternalIDs})
				for _, key := range episode.IndexKeys() {
					index[key] = append(index[key], row.Probe)
				}
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
	return &storedTitleMediaMatcher{filter: filter, index: index}, nil
}

// A request reads the saved library inventory once; all catalogue candidates
// then use the same facts before source-specific sorting, caps and pagination.
type storedTitleMediaMatcher struct {
	filter models.LocalMediaFilters
	index  map[string][]*models.LocalMediaProbe
}

func (m *storedTitleMediaMatcher) matches(title models.Title) bool {
	ids := map[string]string{"imdb": title.IMDBID}
	if title.TMDBID > 0 {
		ids["tmdb"] = strconv.FormatInt(title.TMDBID, 10)
	}
	if title.TVDBID > 0 {
		ids["tvdb"] = strconv.FormatInt(title.TVDBID, 10)
	}
	return m.matchesIdentity(mediaidentity.Resolve(mediaidentity.Input{MediaType: title.MediaType, ID: title.ID, ExternalIDs: ids}))
}
func (m *storedTitleMediaMatcher) matchesIdentity(identity mediaidentity.Identity) bool {
	if !m.filter.Active() {
		return true
	}
	found := false
	for _, key := range identity.IndexKeys() {
		for _, probe := range m.index[key] {
			found = true
			if localmedia.MatchesMediaFilters(probe, m.filter) {
				return true
			}
		}
	}
	return !found && localmedia.MatchesMediaFilters(nil, m.filter)
}

func (h *DisplayListHandler) requestStoredMediaMatcher(r *http.Request, profile string) (*storedTitleMediaMatcher, error) {
	filter, err := parseLocalMediaFilters(r)
	if err != nil {
		return nil, err
	}
	return h.storedMediaMatcher(r, profile, filter)
}

func (h *DisplayListHandler) filterSavedListMedia(r *http.Request, profile string, items []models.WatchlistItem, filter models.LocalMediaFilters) ([]models.WatchlistItem, error) {
	matcher, err := h.storedMediaMatcher(r, profile, filter)
	if err != nil {
		return nil, err
	}
	selected := make([]models.WatchlistItem, 0, len(items))
	for _, item := range items {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		if matcher.matchesIdentity(mediaidentity.Resolve(mediaidentity.Input{MediaType: item.MediaType, ID: item.ID, ExternalIDs: item.ExternalIDs})) {
			selected = append(selected, item)
		}
	}
	return selected, nil
}

func storedMediaQueryKey(key string) bool {
	switch key {
	case "audioLanguage", "subtitleLanguage", "audioCodec", "videoCodec", "minHeight", "hdr", "subtitles", "mediaUnknown":
		return true
	}
	return false
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

func (m *storedTitleMediaMatcher) matchesWatching(row historypkg.CachedContinueWatchingItem) bool {
	if !m.filter.Active() {
		return true
	}
	if row.MediaType == "movie" {
		return m.matchesIdentity(mediaidentity.Resolve(mediaidentity.Input{MediaType: "movie", ID: row.SeriesID, ExternalIDs: row.ExternalIDs}))
	}
	next := row.NextEpisode
	if next == nil || !models.OfficialEpisodeCoordinates(row.SeriesID, &models.EpisodeReference{SeasonNumber: next.SeasonNumber, EpisodeNumber: next.EpisodeNumber, Numbering: next.Numbering}) {
		return localmedia.MatchesMediaFilters(nil, m.filter)
	}
	return m.matchesIdentity(mediaidentity.Resolve(mediaidentity.Input{MediaType: "episode", SeriesID: row.SeriesID, SeasonNumber: next.SeasonNumber, EpisodeNumber: next.EpisodeNumber, ExternalIDs: row.ExternalIDs}))
}
