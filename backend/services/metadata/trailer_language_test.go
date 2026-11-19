package metadata

import (
	"reflect"
	"testing"

	"novastream/models"
)

func TestTrailerLanguagePreferenceUsesRegisteredTags(t *testing.T) {
	for _, code := range []string{"fr", "fra", "fre", "fr-CA", " FR_ca "} {
		t.Run(code, func(t *testing.T) {
			s := &Service{tmdb: &tmdbClient{language: code}, client: &tvdbClient{language: "eng"}}
			if got := s.preferredTrailerLanguage(); got != "fra" {
				t.Fatalf("language=%q", got)
			}
		})
	}
	for _, code := range []string{"", "und", "mul", "zxx", "invalid-language-value"} {
		t.Run("invalid-"+code, func(t *testing.T) {
			if got := canonicalTrailerLanguage(code); got != "" {
				t.Fatalf("language=%q", got)
			}
		})
	}
	if (*Service)(nil).preferredTrailerLanguage() != "" || (&Service{}).preferredTrailerLanguage() != "" {
		t.Fatal("missing preference invented a language")
	}
	if got := (&Service{client: &tvdbClient{language: "spa"}}).preferredTrailerLanguage(); got != "spa" {
		t.Fatal(got)
	}
}

func TestTrailerLanguagePrimaryPrefersSuitableDeclaredLanguage(t *testing.T) {
	pool := []models.Trailer{
		{URL: "https://synthetic.invalid/en", Type: "Trailer", Name: "Official Final Series Trailer", Language: "en", Official: true, Resolution: 2160, Site: "youtube", Source: "tmdb"},
		{URL: "https://synthetic.invalid/fr", Type: "Trailer", Name: "Bande-annonce", Language: "fr-CA", Resolution: 720, Source: "tmdb"},
		{URL: "https://synthetic.invalid/es", Type: "Teaser", Name: "Avance", Language: "spa", Source: "tmdb"},
		{URL: "https://synthetic.invalid/haw", Name: "Kihei", Language: "haw", Source: "tvdb"},
	}
	before := append([]models.Trailer{}, pool...)
	for _, tc := range []struct{ language, url string }{{"fra", pool[1].URL}, {"es-MX", pool[2].URL}, {"haw", pool[3].URL}, {"jpn", pool[0].URL}, {"", pool[0].URL}} {
		t.Run(tc.language, func(t *testing.T) {
			if got := selectPrimaryTrailer(pool, tc.language); got == nil || got.URL != tc.url {
				t.Fatalf("primary=%+v", got)
			}
		})
	}
	if selectPrimaryTrailer(pool).URL != pool[0].URL || !reflect.DeepEqual(before, pool) {
		t.Fatal("legacy selection or input order changed")
	}
	if selectPrimaryTrailer(nil, "fra") != nil {
		t.Fatal("empty pool gained a trailer")
	}
}

func TestTrailerLanguageDoesNotPromoteClipsOrPromotionalLabels(t *testing.T) {
	for _, candidate := range []models.Trailer{
		{Type: "Clip", Language: "fr", Name: "Clip"}, {Type: "Featurette", Language: "fr", Name: "Video"},
		{Type: "Trailer", Language: "fr", Name: "Behind the scenes"}, {Type: "Trailer", Language: "fr", Name: "Making of"},
		{Type: "", Language: "fr", Name: "Unknown", Source: "tmdb"}, {Type: "Trailer", Language: "und", Name: "Trailer"},
	} {
		t.Run(candidate.Name+candidate.Type, func(t *testing.T) {
			candidate.URL = "https://synthetic.invalid/other"
			pool := []models.Trailer{{Type: "Trailer", Language: "en", URL: "https://synthetic.invalid/en", Official: true}, candidate}
			if got := selectPrimaryTrailer(pool, "fra"); got == nil || got.URL != pool[0].URL {
				t.Fatalf("primary=%+v", got)
			}
		})
	}
}

func TestTrailerLanguagePreservesSeasonAndLegacyQualityFallback(t *testing.T) {
	pool := []models.Trailer{
		{URL: "https://synthetic.invalid/show-fr", Type: "Trailer", Language: "fr", SeasonNumber: 0},
		{URL: "https://synthetic.invalid/s2-en", Type: "Trailer", Language: "en", SeasonNumber: 2, Official: true},
		{URL: "https://synthetic.invalid/s3-fr", Type: "Trailer", Language: "fr", SeasonNumber: 3},
	}
	if got := selectPrimaryTrailerForSeason(pool, 2, "fra"); got == nil || got.URL != pool[1].URL {
		t.Fatal("wrong season selected")
	}
	if got := selectPrimaryTrailerForSeason(pool, 1, "fra"); got == nil || got.URL != pool[0].URL {
		t.Fatal("existing season-one show fallback lost")
	}
	pool = append(pool, models.Trailer{URL: "https://synthetic.invalid/s2-fr", Type: "Teaser", Language: "fra", SeasonNumber: 2})
	if got := selectPrimaryTrailerForSeason(pool, 2, "fr-FR"); got == nil || got.URL != pool[3].URL {
		t.Fatal("matching season language ignored")
	}
	if selectPrimaryTrailerForSeason(pool, 0, "fra") != nil || selectPrimaryTrailerForSeason(nil, 2, "fra") != nil {
		t.Fatal("invalid season gained a primary")
	}
	if got := selectPrimaryTrailerForSeason(pool, 2); got == nil || got.URL != pool[1].URL {
		t.Fatal("legacy quality fallback changed")
	}
}
