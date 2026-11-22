package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"novastream/internal/apierror"
	"novastream/models"
	"novastream/services/kids"
	"novastream/services/kidsvod"
)

type singleGermanTitle interface {
	EnrichGermanTitleCertification(context.Context, *models.Title)
}

func enrichGermanDetail(ctx context.Context, service any, title *models.Title) {
	if reader, ok := service.(singleGermanTitle); ok {
		reader.EnrichGermanTitleCertification(ctx, title)
	}
}

type cachedGermanTitles interface{ EnrichCachedTitleCertifications([]models.Title) }

func (h *MetadataHandler) germanProfile(id string) bool {
	if h.UsersService == nil {
		return false
	}
	user, ok := h.UsersService.Get(id)
	return ok && user.IsKidsProfile && user.KidsMode == kids.GermanMetadataMode
}
func (h *MetadataHandler) enrichKidsTitles(ctx context.Context, id string, service metadataService, titles []models.Title) {
	if !h.germanProfile(id) {
		service.EnrichTitleCertifications(ctx, titles)
		return
	}
	if cached, ok := service.(cachedGermanTitles); ok {
		cached.EnrichCachedTitleCertifications(titles)
	}
}
func (h *MetadataHandler) enrichKidsTrending(ctx context.Context, id string, service metadataService, items []models.TrendingItem) {
	if !h.germanProfile(id) {
		service.EnrichTrendingCertifications(ctx, items)
		return
	}
	titles := make([]models.Title, len(items))
	for i := range items {
		titles[i] = items[i].Title
	}
	if cached, ok := service.(cachedGermanTitles); ok {
		cached.EnrichCachedTitleCertifications(titles)
	}
	for i := range items {
		items[i].Title = titles[i]
	}
}
func (h *MetadataHandler) enrichKidsSearch(ctx context.Context, id string, service metadataService, items []models.SearchResult) {
	if !h.germanProfile(id) {
		service.EnrichSearchCertifications(ctx, items)
		return
	}
	titles := make([]models.Title, len(items))
	for i := range items {
		titles[i] = items[i].Title
	}
	if cached, ok := service.(cachedGermanTitles); ok {
		cached.EnrichCachedTitleCertifications(titles)
	}
	for i := range items {
		items[i].Title = titles[i]
	}
}

type germanAgeMetadata interface {
	SeriesDetailsProvider
	MovieDetailsProvider
}
type germanAgeProfiles interface {
	Get(string) (models.User, bool)
}

// Only this explicit profile mode uses title-level German TMDB metadata.
// It does not implement or claim the separate verified-edition classification port.
func checkGermanProfileTitle(ctx context.Context, users any, metadata germanAgeMetadata, profileID, titleID, kind string) error {
	profiles, ok := users.(germanAgeProfiles)
	if !ok {
		return nil
	}
	profile, exists := profiles.Get(profileID)
	if !exists || !profile.IsKidsProfile || profile.KidsMode != kids.GermanMetadataMode {
		return nil
	}
	denied := errors.New("German TMDB age rating is missing, conflicting, expired or above this profile limit (series rating, not individual-episode proof)")
	if ctx == nil || ctx.Err() != nil || metadata == nil {
		return denied
	}
	if kind == "tv" || kind == "show" {
		kind = "series"
	}
	parts := strings.Split(titleID, ":")
	// Existing canonical IMDb aliases do not encode the media kind. They are
	// usable only when the ordinary caller already supplies that kind.
	if (kind == "movie" || kind == "series") && len(parts) == 2 && parts[0] == "imdb" {
		parts = []string{"imdb", kind, parts[1]}
	} else if (kind == "movie" || kind == "series") && len(parts) == 1 && strings.HasPrefix(parts[0], "tt") {
		parts = []string{"imdb", kind, parts[0]}
	}
	if len(parts) != 3 {
		return denied
	}
	if parts[1] != "movie" && parts[1] != "series" && parts[1] != "tv" {
		return denied
	}
	if parts[1] == "movie" {
		if kind != "" && kind != "movie" {
			return denied
		}
		kind = "movie"
	}
	if parts[1] == "series" || parts[1] == "tv" {
		if kind != "" && kind != "series" {
			return denied
		}
		kind = "series"
	}
	if kind != "movie" && kind != "series" {
		return denied
	}
	tmdb, tvdb := int64(0), int64(0)
	imdb := ""
	switch parts[0] {
	case "tmdb":
		tmdb, _ = strconv.ParseInt(parts[2], 10, 64)
		if tmdb <= 0 {
			return denied
		}
	case "tvdb":
		tvdb, _ = strconv.ParseInt(parts[2], 10, 64)
		if tvdb <= 0 {
			return denied
		}
	case "imdb":
		imdb = parts[2]
		if !strings.HasPrefix(imdb, "tt") {
			return denied
		}
	default:
		return denied
	}
	read, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var title *models.Title
	if kind == "movie" {
		title, _ = metadata.MovieInfo(read, models.MovieDetailsQuery{TitleID: titleID, TMDBID: tmdb, TVDBID: tvdb, IMDBID: imdb})
	} else {
		details, _ := metadata.SeriesDetails(read, models.SeriesDetailsQuery{TitleID: titleID, TMDBID: tmdb, TVDBID: tvdb, IMDBID: imdb})
		if details != nil {
			title = &details.Title
		}
	}
	if title == nil || read.Err() != nil || (tmdb > 0 && title.TMDBID != tmdb) {
		return denied
	}
	returnedKind := strings.ToLower(strings.TrimSpace(title.MediaType))
	if returnedKind == "tv" {
		returnedKind = "series"
	}
	if returnedKind != kind {
		return denied
	}
	// Metadata fallbacks may return another identifier. Require the requested
	// provider's actual field or its consistent canonical title ID, not a name
	// match or merely the presence of some other title's German age evidence.
	matched := tmdb > 0 && title.TMDBID == tmdb
	if tvdb > 0 && title.TVDBID != 0 {
		if title.TVDBID != tvdb {
			return denied
		}
		matched = true
	}
	if imdb != "" && title.IMDBID != "" {
		if title.IMDBID != imdb {
			return denied
		}
		matched = true
	}
	returnedID := strings.Split(title.ID, ":")
	if len(returnedID) == 2 && returnedID[0] == "imdb" {
		returnedID = []string{"imdb", kind, returnedID[1]}
	} else if len(returnedID) == 1 && strings.HasPrefix(returnedID[0], "tt") {
		returnedID = []string{"imdb", kind, returnedID[0]}
	}
	if len(returnedID) == 3 && (returnedID[0] == "tmdb" || returnedID[0] == "tvdb" || returnedID[0] == "imdb") {
		idKind := returnedID[1]
		if idKind == "tv" {
			idKind = "series"
		}
		if idKind != kind {
			return denied
		}
		if returnedID[0] == parts[0] {
			same := returnedID[2] == parts[2]
			if tmdb > 0 || tvdb > 0 {
				id, err := strconv.ParseInt(returnedID[2], 10, 64)
				same = err == nil && id > 0 && (id == tmdb || id == tvdb)
			}
			if !same {
				return denied
			}
			matched = true
		}
	}
	if !matched {
		return denied
	}
	enrichGermanDetail(read, metadata, title)
	current, ok := profiles.Get(profileID)
	if !ok || current.KidsMode != profile.KidsMode || current.IsKidsProfile != profile.IsKidsProfile || current.KidsMaxMovieRating != profile.KidsMaxMovieRating || current.KidsMaxTVRating != profile.KidsMaxTVRating {
		return denied
	}
	movie, series, _ := kids.RatingLimits(current)
	limit := movie
	if kind == "series" {
		limit = series
	}
	if !kids.IsTitleRatingAllowed(*title, limit) {
		return denied
	}
	return nil
}
func writeGermanAgeError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}
	apierror.Write(w, http.StatusForbidden, kidsvod.CodeClassificationUnknown, err.Error())
	return false
}
func (h *PrequeueHandler) checkGermanAge(w http.ResponseWriter, r *http.Request, profile, title, kind string) bool {
	return writeGermanAgeError(w, checkGermanProfileTitle(r.Context(), h.users, h.germanMetadata, profile, title, kind))
}
func (h *PlaybackHandler) SetGermanAgeMetadata(service germanAgeMetadata) { h.germanMetadata = service }
func (h *PrequeueHandler) SetGermanAgeMetadata(service germanAgeMetadata) { h.germanMetadata = service }

// JSON persistence uses the same explicit-mode values as the PG mutation path.
func (h *UsersHandler) validGermanRatings(w http.ResponseWriter, id string, values ...string) bool {
	profile, ok := h.Service.Get(id)
	if !ok || profile.KidsMode != kids.GermanMetadataMode {
		return true
	}
	for _, value := range values {
		if !kids.ValidGermanAge(value) {
			http.Error(w, "German TMDB age limits must be 0, 6, 12, 16 or 18", http.StatusBadRequest)
			return false
		}
	}
	return true
}
