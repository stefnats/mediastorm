package metadata

import (
	"strings"

	"novastream/models"
)

// Prefer provider-declared trailer language. This is not spoken-audio proof;
// missing or unusable language labels retain the existing quality fallback.
func (s *Service) preferredTrailerLanguage() string {
	if s == nil {
		return ""
	}
	if s.tmdb != nil {
		return canonicalTrailerLanguage(s.tmdb.language)
	}
	if s.client != nil {
		return canonicalTrailerLanguage(s.client.language)
	}
	return ""
}

func canonicalTrailerLanguage(value string) string {
	return models.NormalizeEvidenceLanguage(strings.ReplaceAll(strings.TrimSpace(value), "_", "-"))
}

func matchesPreferredTrailer(t *models.Trailer, preferred string) bool {
	if t == nil || preferred == "" || canonicalTrailerLanguage(t.Language) != preferred {
		return false
	}
	// A localized featurette or clip must not replace an actual trailer merely
	// because its language matches. TVDB trailer records can omit the type.
	switch strings.ToLower(strings.TrimSpace(t.Type)) {
	case "":
		if !strings.EqualFold(t.Source, "tvdb") {
			return false
		}
	case "trailer", "teaser":
	default:
		return false
	}
	name := strings.ToLower(t.Name)
	for _, keyword := range []string{"best reviewed", "pre-order", "recap", "behind the scenes", "making of", "featurette"} {
		if strings.Contains(name, keyword) {
			return false
		}
	}
	return true
}

func preferredTrailerAvailable(trailers []models.Trailer, seasonNumber int, preferred []string) (string, bool) {
	if len(preferred) == 0 {
		return "", false
	}
	language := canonicalTrailerLanguage(preferred[0])
	if language == "" {
		return "", false
	}
	for i := range trailers {
		if seasonNumber > 0 && trailers[i].SeasonNumber != seasonNumber && !(seasonNumber == 1 && trailers[i].SeasonNumber == 0) {
			continue
		}
		if matchesPreferredTrailer(&trailers[i], language) {
			return language, true
		}
	}
	return language, false
}
