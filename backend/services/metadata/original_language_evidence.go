package metadata

import (
	"fmt"
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
