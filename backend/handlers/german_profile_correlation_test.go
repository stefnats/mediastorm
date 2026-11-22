package handlers

import (
	"context"
	"testing"

	"novastream/models"
)

type germanCorrelationMetadata struct {
	*germanMetadataFixture
	enrichments int
}

func (m *germanCorrelationMetadata) EnrichGermanTitleCertification(context.Context, *models.Title) {
	m.enrichments++
}

func TestGermanPlaybackMatchesRequestedMediaKindAndProviderAlias(t *testing.T) {
	for _, tc := range []struct {
		name, titleID, kind string
		change              func(*models.Title)
		allowed             bool
	}{
		{"matching-tmdb-movie", "tmdb:movie:123", "movie", nil, true},
		{"matching-tmdb-series-tv-alias", "tmdb:tv:123", "series", func(v *models.Title) { *v = germanFacts("series", "6"); v.ID = "tmdb:tv:123" }, true},
		{"same-number-wrong-returned-kind", "tmdb:movie:123", "movie", func(v *models.Title) { *v = germanFacts("series", "6") }, false},
		{"wrong-canonical-id-kind", "tvdb:movie:456", "movie", func(v *models.Title) { v.TVDBID = 456; v.ID = "tvdb:series:456" }, false},
		{"matching-tvdb-field", "tvdb:movie:456", "movie", func(v *models.Title) { v.TVDBID = 456 }, true},
		{"matching-tvdb-series-field", "tvdb:series:456", "series", func(v *models.Title) { *v = germanFacts("series", "6"); v.TVDBID = 456 }, true},
		{"matching-tvdb-canonical-id", "tvdb:movie:456", "movie", func(v *models.Title) { v.ID = "tvdb:movie:456" }, true},
		{"wrong-tvdb-field", "tvdb:movie:456", "movie", func(v *models.Title) { v.TVDBID = 789 }, false},
		{"missing-tvdb-correlation", "tvdb:movie:456", "movie", nil, false},
		{"conflicting-tvdb-field-and-id", "tvdb:movie:456", "movie", func(v *models.Title) { v.TVDBID = 789; v.ID = "tvdb:movie:456" }, false},
		{"conflicting-tvdb-id-and-field", "tvdb:movie:456", "movie", func(v *models.Title) { v.TVDBID = 456; v.ID = "tvdb:movie:789" }, false},
		{"matching-imdb-field", "imdb:movie:tt1234567", "movie", func(v *models.Title) { v.IMDBID = "tt1234567" }, true},
		{"matching-imdb-series-alias", "imdb:tt1234567", "series", func(v *models.Title) { *v = germanFacts("series", "6"); v.IMDBID = "tt1234567" }, true},
		{"matching-bare-imdb-movie-alias", "tt1234567", "movie", func(v *models.Title) { v.IMDBID = "tt1234567" }, true},
		{"matching-returned-imdb-canonical-id", "imdb:movie:tt1234567", "movie", func(v *models.Title) { v.ID = "imdb:tt1234567" }, true},
		{"wrong-imdb-field", "imdb:movie:tt1234567", "movie", func(v *models.Title) { v.IMDBID = "tt7654321" }, false},
		{"missing-imdb-correlation", "imdb:movie:tt1234567", "movie", nil, false},
		{"conflicting-imdb-id-and-field", "imdb:movie:tt1234567", "movie", func(v *models.Title) { v.IMDBID = "tt1234567"; v.ID = "tt7654321" }, false},
		{"imdb-without-known-kind", "imdb:tt1234567", "", func(v *models.Title) { v.IMDBID = "tt1234567" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := germanFacts("movie", "6")
			if tc.change != nil {
				tc.change(&value)
			}
			metadata := &germanCorrelationMetadata{germanMetadataFixture: &germanMetadataFixture{value: value}}
			err := checkGermanProfileTitle(t.Context(), germanProfileFixture(), metadata, "child", tc.titleID, tc.kind)
			if (err == nil) != tc.allowed {
				t.Fatalf("requested title correlation allowed=%v error=%v", tc.allowed, err)
			}
			if tc.allowed {
				if metadata.reads != 1 || metadata.enrichments != 1 {
					t.Fatal("matching alias did not use the existing single-title path", metadata.reads, metadata.enrichments)
				}
			} else if metadata.enrichments != 0 {
				t.Fatal("mismatched title was enriched before correlation", metadata.enrichments)
			}
		})
	}
}
