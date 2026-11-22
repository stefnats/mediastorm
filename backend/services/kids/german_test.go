package kids

import (
	"testing"
	"time"

	"novastream/models"
)

func germanTitle(at time.Time, kind string, age string) models.Title {
	scope := "series"
	if kind == "movie" {
		scope = "movie_release"
	}
	expiry := at.Add(time.Hour)
	return models.Title{ID: "tmdb:" + kind + ":123", TMDBID: 123, MediaType: kind, Certification: "PG", CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: age, Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:" + kind + ":123", Scope: scope, RetrievedAt: at.Add(-time.Minute), ExpiresAt: &expiry}}}
}
func TestGermanTitleMetadataRequiresExplicitModeAndSeparateLimits(t *testing.T) {
	at := time.Now().UTC()
	title := germanTitle(at, "movie", "6")
	for _, tc := range []struct {
		mode, movie, series, generic string
		want                         bool
	}{
		{"rating_de", "6", "12", "", true}, {"rating_de", "0", "12", "", false}, {"rating_de", "12", "6", "", true},
		{"rating_de", "", "6", "G", false}, {"rating_de", "G", "TV-Y", "G", false}, {"rating", "6", "12", "", false}, {"rating", "tmdb-de:6", "tmdb-de:12", "", false}, {"rating", "G", "TV-Y", "", false},
	} {
		t.Run(tc.mode+"/"+tc.movie+"/"+tc.series, func(t *testing.T) {
			movie, _, enabled := RatingLimits(models.User{IsKidsProfile: true, KidsMode: tc.mode, KidsMaxMovieRating: tc.movie, KidsMaxTVRating: tc.series, KidsMaxRating: tc.generic})
			if !enabled || IsTitleRatingAllowed(title, movie) != tc.want {
				t.Fatal("profile mode or independent cap changed", movie, enabled)
			}
		})
	}
	movie, series, enabled := RatingLimits(models.User{IsKidsProfile: true, KidsMode: "rating", KidsMaxRating: "PG"})
	if !enabled || movie != "PG" || series != "PG" {
		t.Fatal("legacy US fallback changed")
	}
	if _, _, enabled := RatingLimits(models.User{IsKidsProfile: false, KidsMode: "rating_de"}); enabled {
		t.Fatal("adult acquired child rule")
	}
}
func TestGermanMetadataRejectsUnknownStaleConflictingAndWrongIdentity(t *testing.T) {
	at := time.Now().UTC()
	base := germanTitle(at, "movie", "6")
	cases := map[string]func(*models.Title){
		"missing":             func(v *models.Title) { v.CertificationEvidence = nil },
		"US scalar alone":     func(v *models.Title) { v.CertificationEvidence = nil; v.Certification = "FSK 6" },
		"foreign country":     func(v *models.Title) { v.CertificationEvidence[0].Country = "FR" },
		"foreign source":      func(v *models.Title) { v.CertificationEvidence[0].Source = "other" },
		"foreign source type": func(v *models.Title) { v.CertificationEvidence[0].SourceType = "assertion" },
		"foreign title":       func(v *models.Title) { v.CertificationEvidence[0].SourceTitleID = "tmdb:movie:124" },
		"series collision": func(v *models.Title) {
			v.CertificationEvidence[0].SourceTitleID = "tmdb:series:123"
			v.CertificationEvidence[0].Scope = "series"
		},
		"mismatched ID":      func(v *models.Title) { v.ID = "tmdb:movie:124" },
		"missing identity":   func(v *models.Title) { v.TMDBID = 0 },
		"unknown kind":       func(v *models.Title) { v.MediaType = "episode" },
		"missing expiry":     func(v *models.Title) { v.CertificationEvidence[0].ExpiresAt = nil },
		"expired":            func(v *models.Title) { v.CertificationEvidence[0].ExpiresAt = &at },
		"future observation": func(v *models.Title) { v.CertificationEvidence[0].RetrievedAt = at.Add(time.Minute) },
		"zero observation":   func(v *models.Title) { v.CertificationEvidence[0].RetrievedAt = time.Time{} },
		"unknown age":        func(v *models.Title) { v.CertificationEvidence[0].Value = "7" },
		"US age":             func(v *models.Title) { v.CertificationEvidence[0].Value = "PG" },
		"adult":              func(v *models.Title) { v.Adult = true },
		"conflict": func(v *models.Title) {
			other := v.CertificationEvidence[0]
			other.Value = "12"
			v.CertificationEvidence = append(v.CertificationEvidence, other)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			value := base
			value.CertificationEvidence = models.CopyCertificationEvidence(base.CertificationEvidence)
			change(&value)
			if _, known := GermanMetadataAge(value, at); known {
				t.Fatal("unknown/conflicting metadata became a German rating")
			}
		})
	}
}
func TestGermanMetadataKeepsSeriesScopeAndCurrentReleaseAgreement(t *testing.T) {
	at := time.Now().UTC()
	title := germanTitle(at, "series", "6")
	age, known := GermanMetadataAge(title, at)
	if !known || age != 6 {
		t.Fatal("series title metadata lost")
	}
	title.CertificationEvidence[0].Scope = "episode"
	if _, known := GermanMetadataAge(title, at); known {
		t.Fatal("episode proof invented from wrong scope")
	}
	title = germanTitle(at, "movie", "6")
	duplicate := title.CertificationEvidence[0]
	duplicate.Note = "Another edition"
	title.CertificationEvidence = append(title.CertificationEvidence, duplicate)
	if _, known := GermanMetadataAge(title, at); !known {
		t.Fatal("matching current releases became conflict")
	}
	expired := duplicate
	expired.Value = "12"
	expired.ExpiresAt = &at
	title.CertificationEvidence = append(title.CertificationEvidence, expired)
	if age, known := GermanMetadataAge(title, at); !known || age != 6 {
		t.Fatal("expired observation replaced current metadata")
	}
}
func TestGermanFiltersApplySameTitleFactsToSearchListsAndLibrary(t *testing.T) {
	at := time.Now().UTC()
	good := germanTitle(at, "movie", "6")
	bad := germanTitle(at, "movie", "12")
	movie, series, _ := RatingLimits(models.User{IsKidsProfile: true, KidsMode: "rating_de", KidsMaxMovieRating: "6", KidsMaxTVRating: "12"})
	if len(FilterTitlesByRatings([]models.Title{good, bad}, movie, series)) != 1 {
		t.Fatal("title filter disagrees")
	}
	if len(FilterTrendingByRatings([]models.TrendingItem{{Title: good}, {Title: bad}}, movie, series)) != 1 {
		t.Fatal("list filter disagrees")
	}
	if len(FilterSearchByRatings([]models.SearchResult{{Title: good}, {Title: bad}}, movie, series)) != 1 {
		t.Fatal("search filter disagrees")
	}
	group := models.LocalMediaItemGroup{LibraryType: models.LocalMediaLibraryTypeMovie, TMDBID: 123, CertificationEvidence: good.CertificationEvidence}
	if !IsLibraryGroupAllowed(group, movie, series) {
		t.Fatal("stored local/remote facts ignored")
	}
	group.CertificationEvidence = nil
	group.Certification = "PG"
	if IsLibraryGroupAllowed(group, movie, series) {
		t.Fatal("US library scalar became DE metadata")
	}
}
