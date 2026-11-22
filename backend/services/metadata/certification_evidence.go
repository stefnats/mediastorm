package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

type tmdbContentRatingsResponse struct {
	Results []tmdbContentRating `json:"results"`
}
type tmdbContentRating struct {
	Country string `json:"iso_3166_1"`
	Rating  string `json:"rating"`
}

func regionalCertification(country, value, media string, sourceID int64, at time.Time, expires *time.Time) (models.CertificationEvidence, bool) {
	country = strings.ToUpper(strings.TrimSpace(country))
	value = strings.TrimSpace(value)
	if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' || country[1] < 'A' || country[1] > 'Z' || value == "" || len(value) > 64 || sourceID <= 0 || at.IsZero() {
		return models.CertificationEvidence{}, false
	}
	scope := "series"
	if media == "movie" {
		scope = "movie_release"
	}
	return models.CertificationEvidence{Country: country, Value: value, Source: "tmdb", SourceType: "metadata", SourceTitleID: fmt.Sprintf("tmdb:%s:%d", media, sourceID), Scope: scope, RetrievedAt: at.UTC(), ExpiresAt: expires}, true
}

func parseTMDBSeriesCertificationEvidence(payload tmdbContentRatingsResponse, sourceID int64, at time.Time, expires *time.Time) []models.CertificationEvidence {
	var evidence []models.CertificationEvidence
	for _, row := range payload.Results {
		if item, ok := regionalCertification(row.Country, row.Rating, "series", sourceID, at, expires); ok {
			evidence = append(evidence, item)
		}
		if len(evidence) >= models.MaxCertificationEvidence {
			break
		}
	}
	return models.MergeCertificationEvidence(evidence)
}

func parseTMDBMovieCertificationEvidence(payload tmdbReleaseDatesResponse, sourceID int64, at time.Time, expires *time.Time) []models.CertificationEvidence {
	var evidence []models.CertificationEvidence
	for _, country := range payload.Results {
		for _, row := range country.ReleaseDates {
			item, ok := regionalCertification(country.ISO31661, row.Certification, "movie", sourceID, at, expires)
			if !ok || len(row.ReleaseDate) > 40 || len(row.ISO6391) > 16 || len(row.Note) > 512 {
				continue
			}
			if row.Type >= 1 && row.Type <= 6 {
				item.ReleaseType = row.Type
			}
			item.ReleaseDate = strings.TrimSpace(row.ReleaseDate)
			item.Language = strings.TrimSpace(row.ISO6391)
			item.Note = strings.TrimSpace(row.Note)
			evidence = append(evidence, item)
			if len(evidence) >= models.MaxCertificationEvidence {
				return models.MergeCertificationEvidence(evidence)
			}
		}
	}
	return models.MergeCertificationEvidence(evidence)
}

// The base cache TTL is conservative relative to its positive per-key jitter.
// With no positive configured TTL, absence of expiry remains explicit unknown.
func (c *tmdbClient) certificationExpiry(at time.Time) *time.Time {
	if c.cache == nil || c.cache.ttl <= 0 {
		return nil
	}
	expires := at.Add(c.cache.ttl).UTC()
	return &expires
}

func copyTitleCertificationEvidence(title *models.Title) *models.Title {
	if title == nil {
		return nil
	}
	copy := *title
	copy.CertificationEvidence = models.CopyCertificationEvidence(title.CertificationEvidence)
	copy.OriginalLanguageEvidence = models.CopyOriginalLanguageEvidence(title.OriginalLanguageEvidence)
	return &copy
}

func tvCertificationEvidenceCacheKey(id int64) string {
	return cacheKey("tmdb", "tv", "certification_evidence", "v1", strconv.FormatInt(id, 10))
}

// Existing scalar cache entries remain readable without creating a request to
// fill absent evidence. An old cache string is never promoted into provenance.
func (s *Service) hydrateCachedTVCertificationEvidence(title *models.Title, id int64) bool {
	if s.cache == nil || !allowsSeriesCertificationEvidence(title) || id <= 0 {
		return false
	}
	var evidence []models.CertificationEvidence
	if ok, _ := s.cache.get(tvCertificationEvidenceCacheKey(id), &evidence); !ok || len(evidence) == 0 {
		return false
	}
	before := len(title.CertificationEvidence)
	title.CertificationEvidence = models.MergeCertificationEvidence(evidence, title.CertificationEvidence)
	return len(title.CertificationEvidence) > before
}

func (s *Service) hydrateCachedCertificationEvidence(title *models.Title) {
	s.hydrateCachedOriginalLanguageEvidence(title)
	if s == nil || s.cache == nil || title == nil || title.TMDBID <= 0 {
		return
	}
	switch strings.ToLower(strings.TrimSpace(title.MediaType)) {
	case "movie":
		var cached cachedReleasesWithCert
		if ok, _ := s.cache.get(cacheKey("tmdb", "movie", "releases", "v2", strconv.FormatInt(title.TMDBID, 10)), &cached); ok {
			title.CertificationEvidence = models.MergeCertificationEvidence(cached.CertificationEvidence, title.CertificationEvidence)
		}
	case "series", "tv":
		s.hydrateCachedTVCertificationEvidence(title, title.TMDBID)
	}
}

func allowsSeriesCertificationEvidence(title *models.Title) bool {
	if title == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(title.MediaType)) {
	case "", "series", "tv":
		// Empty projections are internal scratch titles in existing series
		// assembly; explicit movie/person/unknown namespaces must not inherit TV.
		return true
	default:
		return false
	}
}

// EnrichCachedTitleCertifications reuses only retained normal metadata responses.
// A DE catalogue filter must not create one provider request per visible card.
func (s *Service) EnrichCachedTitleCertifications(titles []models.Title) {
	for i := range titles {
		s.hydrateCachedCertificationEvidence(&titles[i])
	}
}

// EnrichGermanTitleCertification is for one explicitly opened DE-profile title.
// Normal catalogue filtering uses only EnrichCachedTitleCertifications instead.
func (s *Service) EnrichGermanTitleCertification(ctx context.Context, title *models.Title) {
	if s == nil || s.tmdb == nil || title == nil || title.TMDBID <= 0 || ctx == nil || ctx.Err() != nil {
		return
	}
	s.hydrateCachedCertificationEvidence(title)
	now := time.Now().UTC()
	for _, fact := range title.CertificationEvidence {
		if fact.Country == "DE" && fact.Source == "tmdb" && !fact.RetrievedAt.IsZero() && !fact.RetrievedAt.After(now) && fact.ExpiresAt != nil && fact.ExpiresAt.After(now) {
			return
		}
	}
	read, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var observed *models.Title
	switch strings.ToLower(title.MediaType) {
	case "movie":
		observed, _ = s.tmdb.movieDetails(read, title.TMDBID)
	case "series", "tv":
		observed, _ = s.tmdb.seriesDetails(read, title.TMDBID)
	default:
		return
	}
	if observed != nil && read.Err() == nil {
		title.CertificationEvidence = models.MergeCertificationEvidence(observed.CertificationEvidence, title.CertificationEvidence)
	}
}
