package kids

import (
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

const GermanMetadataMode = "rating_de"
const germanLimitPrefix = "tmdb-de:"

// RatingLimits keeps the explicit provider-metadata choice separate from old US fields.
func RatingLimits(user models.User) (movie, series string, enabled bool) {
	if !user.IsKidsProfile {
		return "", "", false
	}
	if user.KidsMode == GermanMetadataMode {
		// Even an absent/invalid separate value remains a restriction, never US fallback.
		return germanLimitPrefix + user.KidsMaxMovieRating, germanLimitPrefix + user.KidsMaxTVRating, true
	}
	if user.KidsMode != "rating" {
		return "", "", false
	}
	movie, series = user.KidsMaxMovieRating, user.KidsMaxTVRating
	if movie == "" && series == "" {
		movie, series = user.KidsMaxRating, user.KidsMaxRating
	}
	// Legacy fields intentionally retain arbitrary future strings. They may
	// not impersonate this private opt-in marker when the mode is still US.
	if strings.HasPrefix(movie, germanLimitPrefix) {
		movie = "unsupported-us-rating"
	}
	if strings.HasPrefix(series, germanLimitPrefix) {
		series = "unsupported-us-rating"
	}
	return movie, series, movie != "" || series != ""
}

func IsRatingMode(mode string) bool { return mode == "rating" || mode == GermanMetadataMode }
func ValidGermanAge(value string) bool {
	switch value {
	case "0", "6", "12", "16", "18":
		return true
	}
	return false
}

// GermanMetadataAge uses a TMDB title/series claim, never official edition/episode proof.
func GermanMetadataAge(title models.Title, now time.Time) (int, bool) {
	kind := strings.ToLower(strings.TrimSpace(title.MediaType))
	if kind == "tv" {
		kind = "series"
	}
	if title.Adult || (kind != "movie" && kind != "series") || title.TMDBID <= 0 {
		return 0, false
	}
	expected := "tmdb:" + kind + ":" + strconv.FormatInt(title.TMDBID, 10)
	if strings.HasPrefix(title.ID, "tmdb:") {
		id := strings.Replace(title.ID, "tmdb:tv:", "tmdb:series:", 1)
		if id != expected {
			return 0, false
		}
	}
	scope := "series"
	if kind == "movie" {
		scope = "movie_release"
	}
	found, age := false, 0
	for _, fact := range title.CertificationEvidence {
		if fact.Country != "DE" || fact.Source != "tmdb" || fact.SourceType != "metadata" || fact.SourceTitleID != expected || fact.Scope != scope {
			continue
		}
		if fact.RetrievedAt.IsZero() || fact.RetrievedAt.After(now) || fact.ExpiresAt == nil || !fact.ExpiresAt.After(now) || !fact.ExpiresAt.After(fact.RetrievedAt) {
			continue
		}
		if !ValidGermanAge(fact.Value) {
			return 0, false
		}
		next, _ := strconv.Atoi(fact.Value)
		if found && age != next {
			return 0, false
		}
		found, age = true, next
	}
	return age, found
}

func IsTitleRatingAllowed(title models.Title, limit string) bool {
	if !strings.HasPrefix(limit, germanLimitPrefix) {
		return IsRatingAllowed(title.Certification, limit, title.MediaType)
	}
	value := strings.TrimPrefix(limit, germanLimitPrefix)
	if !ValidGermanAge(value) {
		return false
	}
	maximum, _ := strconv.Atoi(value)
	age, known := GermanMetadataAge(title, time.Now().UTC())
	return known && age <= maximum
}
