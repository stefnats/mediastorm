package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/playback"
	"novastream/services/trackselect"
	"novastream/utils/filter"
)

type prequeueRemoteMedia interface {
	FindPlaybackCandidates(context.Context, models.LocalMediaMatchQuery, *models.EpisodeReference, func(string) bool) ([]models.RemoteMediaItem, error)
	GetFallbackItem(context.Context, string) (*models.RemoteMediaItem, error)
}

func (h *PrequeueHandler) SetRemoteFallback(service prequeueRemoteMedia) { h.remoteFallback = service }

func automaticLibraryScope(scope string, enabled bool) string {
	if !enabled {
		return scope
	}
	sum := sha256.Sum256([]byte(scope + "|automatic-library-v1"))
	return "scope_" + hex.EncodeToString(sum[:])[:16]
}

func prequeueEpisodeMatchesForMode(requested, prepared *models.EpisodeReference, automatic bool) bool {
	if !prequeueEpisodeMatches(requested, prepared) {
		return false
	}
	if !automatic || requested == nil {
		return true
	}
	if prepared == nil {
		return false
	}
	if id := strings.TrimSpace(requested.EpisodeID); id != "" && id != strings.TrimSpace(prepared.EpisodeID) {
		return false
	}
	if id := strings.TrimSpace(requested.TvdbID); id != "" && id != "0" && id != strings.TrimSpace(prepared.TvdbID) {
		return false
	}
	return true
}

func prequeueSuppliedIdentityConsistent(req playback.PrequeueRequest) bool {
	id := strings.ToLower(strings.TrimSpace(req.TitleID))
	parts := strings.Split(id, ":")
	provider, expected := "", ""
	if len(parts) == 3 && (parts[0] == "tmdb" || parts[0] == "tvdb") {
		provider, expected = parts[0], parts[2]
	} else if len(parts) == 2 && (parts[0] == "imdb" || parts[0] == "tvdb") {
		provider, expected = parts[0], parts[1]
	} else if strings.HasPrefix(id, "tt") && !strings.Contains(id, ":") {
		provider, expected = "imdb", id
	}
	actual := strings.TrimSpace(map[string]string{"tmdb": req.TmdbID, "tvdb": req.TvdbID, "imdb": req.ImdbID}[provider])
	return actual == "" || strings.EqualFold(actual, expected)
}

func libraryStreamIdentity(path string) (string, string, bool) {
	for _, provider := range []string{"local", "plex", "jellyfin"} {
		if tail, ok := strings.CutPrefix(path, provider+"media:"); ok {
			id, _, _ := strings.Cut(tail, "/")
			return provider, id, id != ""
		}
	}
	return "", "", false
}

func verifiedLibraryEpisode(titleID string, details *models.SeriesDetails, episode *models.EpisodeReference) bool {
	if details == nil || episode == nil || details.Numbering == nil || episode.EpisodeID == "" {
		return false
	}
	aliases := []string{details.Title.ID}
	if details.Title.TMDBID > 0 {
		aliases = append(aliases, "tmdb:series:"+positiveIDString(details.Title.TMDBID))
	}
	if details.Title.TVDBID > 0 {
		aliases = append(aliases, "tvdb:series:"+positiveIDString(details.Title.TVDBID))
	}
	if details.Title.IMDBID != "" {
		aliases = append(aliases, details.Title.IMDBID)
	}
	matchedTitle := false
	for _, alias := range aliases {
		if models.CanonicalSeriesIdentity(alias) == models.CanonicalSeriesIdentity(titleID) {
			matchedTitle = true
		}
	}
	if !matchedTitle || !models.SameEpisodeNumbering(details.Numbering, episode.Numbering) || !models.OfficialEpisodeCoordinates(titleID, episode, aliases...) {
		return false
	}
	count := 0
	for _, season := range details.Seasons {
		for _, current := range season.Episodes {
			if season.Number != episode.SeasonNumber || current.SeasonNumber != episode.SeasonNumber || current.EpisodeNumber != episode.EpisodeNumber {
				continue
			}
			count++
			if current.ID != episode.EpisodeID || (current.Numbering != nil && !models.SameEpisodeNumbering(current.Numbering, details.Numbering)) {
				return false
			}
		}
	}
	return count == 1
}

// Unknown list/rating authority is deliberately unavailable for automatic
// remote substitution. The explicit library browsing path retains its policy.
func (h *PrequeueHandler) remoteItemAllowed(userID string) bool {
	profiles, ok := h.users.(interface {
		Get(string) (models.User, bool)
	})
	if !ok {
		return false
	}
	profile, ok := profiles.Get(userID)
	return ok && !profile.IsKidsProfile
}

func remoteLibraryIdentity(item models.RemoteMediaItem) string {
	data, _ := json.Marshal([]any{item.ID, item.LibraryID, item.LibraryType, item.ExternalItemID, item.ExternalMediaID, item.GroupKey, item.ExternalIDs, item.SeriesExternalIDs, item.EpisodeExternalIDs, item.SinglePart, item.SeasonNumber, item.EpisodeNumber, item.FileName, item.StreamPath, item.ProviderData, item.SizeBytes, item.IsMissing})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *PrequeueHandler) findRemoteFallbackCandidates(ctx context.Context, scopes []prequeueLocalScope, userID string, query models.LocalMediaMatchQuery, episode *models.EpisodeReference) []models.RemoteMediaItem {
	if h.remoteFallback == nil || h.localAccess == nil || len(scopes) != 1 || !scopes[0].automaticLibrarySelection || scopes[0].accountID == "" || !h.remoteItemAllowed(userID) {
		return nil
	}
	scope := scopes[0]
	if query.MediaType != "movie" && !scope.libraryEpisodeVerified {
		return nil
	}
	if scope.tmdbID != "" {
		query.TMDBID = scope.tmdbID
	}
	if scope.tvdbID != "" {
		query.TVDBID = scope.tvdbID
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	items, err := h.remoteFallback.FindPlaybackCandidates(checkCtx, query, episode, func(libraryID string) bool {
		allowed, err := h.localAccess.CanAccess(checkCtx, libraryID, scope.accountID, userID, scope.master)
		return err == nil && allowed
	})
	if err != nil || checkCtx.Err() != nil {
		return nil
	}
	return items
}

func (h *PrequeueHandler) resolveRemoteItem(ctx context.Context, item models.RemoteMediaItem, opts prequeueResolutionOptions) (*models.PlaybackResolution, error) {
	if h.remoteFallback == nil || h.localAccess == nil || h.fullProber == nil || opts.localScope == nil || !opts.localScope.automaticLibrarySelection || !h.remoteItemAllowed(opts.userID) {
		return nil, errors.New("library playback validation unavailable")
	}
	validate := func() (*models.RemoteMediaItem, error) {
		current, err := h.remoteFallback.GetFallbackItem(ctx, item.ID)
		if err != nil || current == nil || remoteLibraryIdentity(*current) != remoteLibraryIdentity(item) {
			return nil, errors.New("library media identity changed or is unavailable")
		}
		allowed, err := h.localAccess.CanAccess(ctx, current.LibraryID, opts.localScope.accountID, opts.userID, opts.localScope.master)
		if err != nil || !allowed || !h.remoteItemAllowed(opts.userID) {
			return nil, errors.New("library media unavailable")
		}
		return current, nil
	}
	current, err := validate()
	if err != nil {
		return nil, err
	}
	if err := validateLibrarySize(current.SizeBytes, opts); err != nil {
		return nil, err
	}
	if err := validateLibraryTerms(current.FileName, opts.localFilter); err != nil {
		return nil, err
	}
	probe, err := h.probeLibraryItem(ctx, current.StreamPath)
	if err != nil {
		return nil, err
	}
	if err := validateLocalFallbackQuality(probe, opts.localFilter); err != nil {
		return nil, err
	}
	if _, err := validate(); err != nil {
		return nil, err
	}
	return &models.PlaybackResolution{WebDAVPath: current.StreamPath, FileSize: current.SizeBytes, HealthStatus: "healthy", Probe: probe}, nil
}

func validateLibrarySize(size int64, opts prequeueResolutionOptions) error {
	if size <= 0 {
		return errors.New("library media size is unverified")
	}
	if !readyEntryFitsAdaptiveCaps(&playback.PrequeueEntry{FileSize: size, MediaType: opts.mediaType}, opts.adaptiveCaps) {
		return errors.New("library file exceeds the current playback size limit")
	}
	limit := opts.localFilter.MaxSizeMovieGB
	if opts.mediaType != "movie" {
		limit = opts.localFilter.MaxSizeEpisodeGB
	}
	if limit != nil && *limit > 0 && float64(size)/(1024*1024*1024) > *limit {
		return errors.New("library file exceeds the current playback size limit")
	}
	return nil
}

func validateLibraryTerms(name string, settings models.FilterSettings) error {
	if terms := filter.CompileTerms(settings.RequiredTerms); len(terms) > 0 && !filter.MatchesAnyTerm(name, terms) {
		return errors.New("library file does not meet required release terms")
	}
	if filter.MatchesAnyTerm(name, filter.CompileTerms(settings.FilterOutTerms)) {
		return errors.New("library file matches excluded release terms")
	}
	return nil
}

// A probe is allowed only while preparing playback, never by catalog/status
// requests. Bound both the wait and outstanding workers for uncooperative I/O.
func (h *PrequeueHandler) probeLibraryItem(ctx context.Context, path string) (*VideoFullResult, error) {
	select {
	case localFallbackProbes <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	type result struct {
		probe *VideoFullResult
		err   error
	}
	done := make(chan result, 1)
	go func() {
		defer func() { <-localFallbackProbes }()
		probe, err := h.fullProber.ProbeVideoFull(context.WithValue(ctx, localSourceProbeContextKey{}, true), path)
		done <- result{probe, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-done:
		if result.err != nil {
			return nil, errors.New("existing library media could not be validated")
		}
		return result.probe, nil
	}
}

// The normal selector may intentionally fall back when no preferred language
// matches. Automatic substitution must prove the exact requested language.
func strictLibraryAudio(streams []models.AudioStreamInfo, prefs trackselect.Preferences) *models.SelectedTrack {
	prefs = prefs.Normalized()
	eligible := make([]models.AudioStreamInfo, 0, len(streams))
	for _, stream := range streams {
		if trackselect.IsCommentaryTrack(stream.Title) {
			continue
		}
		allowed := len(prefs.AllowedLanguages) == 0
		for _, language := range prefs.AllowedLanguages {
			if trackselect.MatchesLanguage(stream.Language, stream.Title, language) {
				allowed = true
			}
		}
		if allowed {
			eligible = append(eligible, stream)
		}
	}
	index := -1
	if prefs.AudioLanguage != "" {
		index = trackselect.FindAudioTrack(eligible, prefs.AudioLanguage)
	} else if len(prefs.AllowedLanguages) > 0 {
		index = trackselect.FindAllowedAudioTrack(eligible, prefs.AllowedLanguages, "")
	} else if len(eligible) > 0 {
		index = eligible[0].Index
	}
	return trackselect.DescribeAudioTrack(streams, index)
}

func selectLibraryTracks(streams []models.AudioStreamInfo, subtitles []models.SubtitleStreamInfo, prefs trackselect.Preferences) *models.TrackSelection {
	audio := strictLibraryAudio(streams, prefs)
	if audio == nil {
		return nil
	}
	normalized := prefs.Normalized()
	if normalized.AudioLanguage == "" && len(normalized.AllowedLanguages) == 0 {
		// Eligibility proves usable audio; unconstrained viewers retain the
		// existing player/container default, including later default tracks.
		return trackselect.Select(streams, subtitles, prefs)
	}
	// Subtitle foreign-audio policy must describe the rendition that will
	// actually play, including an unconstrained container-default selection.
	ordered := make([]models.AudioStreamInfo, 0, len(streams))
	for _, stream := range streams {
		if stream.Index == audio.StreamIndex {
			ordered = append(ordered, stream)
		}
	}
	for _, stream := range streams {
		if stream.Index != audio.StreamIndex {
			ordered = append(ordered, stream)
		}
	}
	selectionPrefs := prefs
	if audio.LanguageCode != "" {
		selectionPrefs.AudioLanguage = audio.LanguageCode
	} else {
		selectionPrefs.AudioLanguage = audio.Language
	}
	selection := trackselect.Select(ordered, subtitles, selectionPrefs)
	selection.Audio, selection.AudioLanguage = audio, normalized.AudioLanguage
	return selection
}

func (h *PrequeueHandler) remoteEntryAllowed(r *http.Request, entry *playback.PrequeueEntry, itemID string) bool {
	if h.remoteFallback == nil || h.localAccess == nil || !entry.AutomaticLibrarySelection || !h.remoteItemAllowed(entry.UserID) {
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	item, err := h.remoteFallback.GetFallbackItem(ctx, itemID)
	if err != nil || item == nil || item.StreamPath != entry.StreamPath || entry.LibraryIdentity == "" || remoteLibraryIdentity(*item) != entry.LibraryIdentity {
		return false
	}
	allowed, err := h.localAccess.CanAccess(ctx, item.LibraryID, auth.GetAccountID(r), entry.UserID, auth.IsMaster(r))
	return err == nil && allowed
}
