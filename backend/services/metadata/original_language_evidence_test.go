package metadata

import (
	"net/http"
	"novastream/models"
	"sync/atomic"
	"testing"
	"time"
)

func TestOriginalLanguageEvidenceUsesActualTMDBResponseWithoutExtraDemand(t *testing.T) {
	for _, kind := range []string{"movie", "series"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := shelfTestService(t)
			var calls atomic.Int32
			s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(200, `{"id":41,"title":"Synthetic title","name":"Synthetic title","original_language":"is"}`, nil), nil
			})}
			var title *models.Title
			var err error
			if kind == "movie" {
				title, err = s.tmdb.movieDetails(t.Context(), 41)
			} else {
				title, err = s.tmdb.seriesDetails(t.Context(), 41)
			}
			if err != nil {
				t.Fatal(err)
			}
			fact := title.OriginalLanguageEvidence
			if fact == nil || fact.Language != "is" || fact.SourceTitleID != "tmdb:"+kind+":41" || calls.Load() != 1 {
				t.Fatalf("actual payload fact: %+v calls=%d", fact, calls.Load())
			}
			copy := models.Title{ID: "opaque-title", TMDBID: 41, MediaType: kind, Language: "de"}
			s.hydrateCachedOriginalLanguageEvidence(&copy)
			if copy.OriginalLanguageEvidence == nil || !copy.OriginalLanguageEvidence.ObservedAt.Equal(fact.ObservedAt) || !copy.OriginalLanguageEvidence.ExpiresAt.Equal(fact.ExpiresAt) || calls.Load() != 1 {
				t.Fatal("cache read lost original timestamp or caused demand")
			}
			copy.OriginalLanguageEvidence.Language = "fr"
			s.hydrateCachedOriginalLanguageEvidence(title)
			if title.OriginalLanguageEvidence.Language != "is" {
				t.Fatal("hydration shared mutable fact")
			}
		})
	}
}

func TestOriginalLanguageEvidenceLegacyScalarAndExpiredSidecarStayUnknown(t *testing.T) {
	s, _ := shelfTestService(t)
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected provider request"); return nil, nil })}
	title := models.Title{ID: "tmdb:movie:41", TMDBID: 41, MediaType: "movie", Language: "de"}
	s.hydrateCachedOriginalLanguageEvidence(&title)
	if title.OriginalLanguageEvidence != nil {
		t.Fatal("legacy locale promoted")
	}
	at := time.Now().Add(-25 * time.Hour)
	fact := models.OriginalLanguageEvidence{Language: "fr", Source: "tmdb", SourceTitleID: title.ID, ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}
	if err := s.cache.set(originalLanguageCacheKey("movie", 41), fact); err != nil {
		t.Fatal(err)
	}
	s.hydrateCachedOriginalLanguageEvidence(&title)
	if title.OriginalLanguageEvidence != nil {
		t.Fatal("sidecar mtime renewed expired proof")
	}
	s.tmdb.recordOriginalLanguage(&title, 99, "fr", time.Now())
	if title.OriginalLanguageEvidence != nil {
		t.Fatal("mismatched response anchored")
	}
}
