package metadata

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

func originalLanguageCacheKey(kind string, id int64) string {
	return cacheKey("tmdb", kind, "original_language_evidence", "v1", fmt.Sprint(id))
}

// recordOriginalLanguage is called only after a normal TMDB response supplied
// original_language and its actual response ID. It performs no provider I/O.
func (c *tmdbClient) recordOriginalLanguage(title *models.Title, responseID int64, raw string, at time.Time) {
	if title == nil || responseID <= 0 || title.TMDBID != responseID {
		return
	}
	kind := title.MediaType
	if kind == "tv" {
		kind = "series"
	}
	if kind != "movie" && kind != "series" {
		return
	}
	value := strings.ToLower(strings.TrimSpace(raw))
	if len(value) < 2 || len(value) > 3 || models.NormalizeEvidenceLanguage(value) == "" {
		return
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			return
		}
	}
	ttl := models.OriginalLanguageEvidenceMaxTTL
	if c.cache != nil && c.cache.ttl > 0 && c.cache.ttl < ttl {
		ttl = c.cache.ttl
	}
	fact := &models.OriginalLanguageEvidence{Language: value, Source: "tmdb", SourceTitleID: fmt.Sprintf("tmdb:%s:%d", kind, responseID), ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ttl)}
	title.OriginalLanguageEvidence = fact
	if models.CurrentOriginalLanguageEvidence(*title, at) == nil {
		title.OriginalLanguageEvidence = nil
		return
	}
	if c.cache != nil {
		_ = c.cache.set(originalLanguageCacheKey(kind, responseID), fact)
	}
}

// A legacy scalar language or cache file mtime cannot establish provenance.
// Cached copies retain the provider observation's original timestamps.
func (s *Service) hydrateCachedOriginalLanguageEvidence(title *models.Title) {
	if s == nil || s.cache == nil || title == nil || title.TMDBID <= 0 {
		return
	}
	kind := title.MediaType
	if kind == "tv" {
		kind = "series"
	}
	if kind != "movie" && kind != "series" {
		return
	}
	var value models.OriginalLanguageEvidence
	if ok, _ := s.cache.get(originalLanguageCacheKey(kind, title.TMDBID), &value); !ok {
		return
	}
	copy := *title
	copy.OriginalLanguageEvidence = &value
	if fact := models.CurrentOriginalLanguageEvidence(copy, time.Now()); fact != nil {
		current := models.CurrentOriginalLanguageEvidence(*title, time.Now())
		if current == nil || fact.ObservedAt.After(current.ObservedAt) {
			title.OriginalLanguageEvidence = fact
		}
	}
}

// GetCachedOriginalLanguageEvidence reads an already observed provider fact for
// an exact catalogue identity. Saved caller-supplied side IDs are never inputs.
// Missing mappings and legacy scalar languages remain unknown; no provider
// request or observation timestamp refresh is performed.
func (s *Service) GetCachedOriginalLanguageEvidence(titleID, mediaType string, at time.Time) *models.OriginalLanguageEvidence {
	if s == nil || s.cache == nil || at.IsZero() {
		return nil
	}
	kind := mediaType
	if kind == "tv" {
		kind = "series"
	}
	if kind != "movie" && kind != "series" {
		return nil
	}
	var id int64
	parts := strings.Split(titleID, ":")
	if len(parts) == 3 && parts[0] == "tmdb" && (parts[1] == kind || (kind == "series" && parts[1] == "tv")) {
		id, _ = strconv.ParseInt(parts[2], 10, 64)
		if strconv.FormatInt(id, 10) != parts[2] {
			return nil
		}
	} else if kind == "movie" && len(parts) == 3 && parts[0] == "tvdb" && parts[1] == "movie" {
		raw := parts[2]
		tvdbID, _ := strconv.ParseInt(raw, 10, 64)
		if tvdbID <= 0 || strconv.FormatInt(tvdbID, 10) != raw {
			return nil
		}
		id = s.cachedOriginalLanguageMovieTMDBID(tvdbID)
	} else if kind == "series" && ((len(parts) == 3 && parts[0] == "tvdb" && parts[1] == "series") || (len(parts) == 2 && parts[0] == "tvdb")) {
		raw := parts[len(parts)-1]
		tvdbID, _ := strconv.ParseInt(raw, 10, 64)
		if tvdbID <= 0 || strconv.FormatInt(tvdbID, 10) != raw {
			return nil
		}
		// This key is written only by the normal TMDB find-by-TVDB response.
		_, _ = s.cache.get(cacheKey("tmdb", "resolve", "series", "tvdb", raw), &id)
	} else {
		imdbID := titleID
		if len(parts) == 2 && parts[0] == "imdb" {
			imdbID = parts[1]
		}
		if !strings.HasPrefix(imdbID, "tt") || len(imdbID) < 3 || len(imdbID) > 16 {
			return nil
		}
		for _, c := range imdbID[2:] {
			if c < '0' || c > '9' {
				return nil
			}
		}
		if s.idCache == nil {
			return nil
		}
		keyKind := "movie"
		if kind == "series" {
			keyKind = "tv"
		}
		_, _ = s.idCache.get(cacheKey("id", "imdb-to-tmdb", keyKind, imdbID), &id)
	}
	if id <= 0 {
		return nil
	}
	var fact models.OriginalLanguageEvidence
	if ok, _ := s.cache.get(originalLanguageCacheKey(kind, id), &fact); !ok {
		return nil
	}
	title := models.Title{ID: fmt.Sprintf("tmdb:%s:%d", kind, id), TMDBID: id, MediaType: kind, OriginalLanguageEvidence: &fact}
	return models.CurrentOriginalLanguageEvidence(title, at)
}

// Unlike assembled detail titles, this raw provider response has never had
// request-side external IDs backfilled into its identity fields.
func (s *Service) cachedOriginalLanguageMovieTMDBID(tvdbID int64) int64 {
	var raw tvdbMovieExtendedData
	if ok, _ := s.cache.get(movieExtendedCacheID(tvdbID), &raw); !ok || raw.ID != tvdbID {
		return 0
	}
	var found int64
	for _, remote := range raw.RemoteIDs {
		switch strings.ToLower(strings.TrimSpace(remote.SourceName)) {
		case "themoviedb.com", "themoviedb", "tmdb":
			id, err := strconv.ParseInt(remote.ID, 10, 64)
			if err != nil || id <= 0 || strconv.FormatInt(id, 10) != remote.ID || (found != 0 && found != id) {
				return 0
			}
			found = id
		}
	}
	return found
}
