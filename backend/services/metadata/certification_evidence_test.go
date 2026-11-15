package metadata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

const certificationMovieFixture = `{"id":123,"title":"Synthetic movie","release_dates":{"results":[{"iso_3166_1":"US","release_dates":[{"type":4,"certification":"PG","release_date":"2020-01-01T00:00:00Z"},{"type":3,"certification":"R"}]},{"iso_3166_1":"DE","release_dates":[{"type":3,"certification":"6","release_date":"2020-01-01T00:00:00Z"},{"type":4,"certification":"12","release_date":"2020-02-01T00:00:00Z","note":"Different edition"}]},{"iso_3166_1":"FR","release_dates":[{"type":4,"certification":"Tous publics"}]}]}}`

func TestCertificationEvidenceMoviePayloadPreservesCountryEditionAndLegacySelection(t *testing.T) {
	s, _ := shelfTestService(t)
	var calls atomic.Int32
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.URL.Path != "/3/movie/123" {
			t.Errorf("additional endpoint: %s", r.URL.Path)
		}
		return jsonResponse(200, certificationMovieFixture, nil), nil
	})}
	title, err := s.tmdb.movieDetails(t.Context(), 123)
	if err != nil {
		t.Fatal(err)
	}
	// The low-level movie title historically has no scalar; release enrichment
	// sets the first US value using the existing selection rules.
	if title.Certification != "" || len(title.CertificationEvidence) != 5 {
		t.Fatalf("movie scalar/evidence: %+v", title)
	}
	if !s.enrichMovieReleases(t.Context(), title, 123) || title.Certification != "PG" || calls.Load() != 1 {
		t.Fatal("evidence added requests or changed US selection")
	}
	for _, item := range title.CertificationEvidence {
		if item.Source != "tmdb" || item.SourceType != "metadata" || item.SourceTitleID != "tmdb:movie:123" || item.Scope != "movie_release" || item.RetrievedAt.IsZero() || item.ExpiresAt == nil {
			t.Fatalf("bad provenance: %+v", item)
		}
	}
	de := []models.CertificationEvidence{}
	for _, item := range title.CertificationEvidence {
		if item.Country == "DE" {
			de = append(de, item)
		}
	}
	if len(de) != 2 || de[0].Value != "6" || de[1].Value != "12" || de[0].ReleaseType == de[1].ReleaseType {
		t.Fatal("different DE provider rows collapsed or mapped")
	}
	before := models.CopyCertificationEvidence(title.CertificationEvidence)
	var cached cachedReleasesWithCert
	if ok, _ := s.cache.get(cacheKey("tmdb", "movie", "releases", "v2", "123"), &cached); !ok {
		t.Fatal("release cache missing")
	}
	restored := models.Title{MediaType: "movie", TMDBID: 123, Certification: "PG"}
	s.hydrateCachedCertificationEvidence(&restored)
	if len(restored.CertificationEvidence) != 5 || !restored.CertificationEvidence[0].RetrievedAt.Equal(before[0].RetrievedAt) || calls.Load() != 1 {
		t.Fatal("cached hydration renewed freshness or contacted provider")
	}
}

func TestCertificationEvidenceTVScopeUsesResponseIDAndExistingScalarCache(t *testing.T) {
	s, _ := shelfTestService(t)
	var calls atomic.Int32
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(200, `{"id":41,"name":"Synthetic series","content_ratings":{"results":[{"iso_3166_1":"DE","rating":"12"},{"iso_3166_1":"US","rating":""},{"iso_3166_1":"US","rating":"TV-14"},{"iso_3166_1":"ES","rating":"16"}]}}`, nil), nil
	})}
	title, err := s.tmdb.seriesDetails(t.Context(), 999)
	if err != nil {
		t.Fatal(err)
	}
	if title.Certification != "" || len(title.CertificationEvidence) != 3 {
		t.Fatalf("legacy first-US selection changed: %+v", title)
	}
	for _, item := range title.CertificationEvidence {
		if item.Scope != "series" || item.SourceTitleID != "tmdb:series:41" || item.ReleaseType != 0 {
			t.Fatal("caller alias or episode claim used as provenance")
		}
	}
	if err := s.cache.set(cacheKey("tmdb", "tv", "content_rating", "v1", "41"), "TV-PG"); err != nil {
		t.Fatal(err)
	}
	copy := models.Title{MediaType: "series"}
	if !s.enrichTVContentRating(t.Context(), &copy, 41) || copy.Certification != "TV-PG" || len(copy.CertificationEvidence) != 3 || calls.Load() != 1 {
		t.Fatal("sidecar replaced scalar or triggered another request")
	}
	first := copy.CertificationEvidence[0].RetrievedAt
	s.hydrateCachedCertificationEvidence(&models.Title{MediaType: "series", TMDBID: 41})
	if !copy.CertificationEvidence[0].RetrievedAt.Equal(first) {
		t.Fatal("copy renewed retrieval time")
	}
}

func TestCertificationEvidenceLegacyCachesDoNotTriggerRequestsOrInventProvenance(t *testing.T) {
	s, _ := shelfTestService(t)
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("legacy scalar cache caused an evidence request")
		return nil, fmt.Errorf("unexpected request")
	})}
	_ = s.cache.set(cacheKey("tmdb", "tv", "content_rating", "v1", "41"), "TV-PG")
	_ = s.cache.set(cacheKey("tmdb", "movie", "releases", "v2", "123"), cachedReleasesWithCert{Certification: "PG", Releases: []models.Release{{Type: "digital"}}})
	for _, title := range []models.Title{{MediaType: "series", TMDBID: 41}, {MediaType: "movie", TMDBID: 123}} {
		if title.MediaType == "movie" {
			s.enrichMovieReleases(t.Context(), &title, 123)
		} else {
			s.enrichTVContentRating(t.Context(), &title, 41)
		}
		if title.Certification == "" || title.CertificationEvidence != nil {
			t.Fatal("old scalar became typed evidence")
		}
	}
	titles := []models.Title{{MediaType: "movie", TMDBID: 123, Certification: "PG"}, {MediaType: "series", TMDBID: 41, Certification: "TV-PG"}}
	s.EnrichTitleCertifications(t.Context(), titles)
	if titles[0].CertificationEvidence != nil || titles[1].CertificationEvidence != nil {
		t.Fatal("scalar-only enrichment invented evidence")
	}
}

func TestCertificationEvidenceValidationBoundsUnknownExpiryAndMissingProviderID(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	payload := tmdbContentRatingsResponse{Results: []tmdbContentRating{{Country: " de ", Rating: " 6 "}, {Country: "invalid", Rating: "6"}, {Country: "DE", Rating: ""}, {Country: "FR", Rating: strings.Repeat("x", 65)}}}
	got := parseTMDBSeriesCertificationEvidence(payload, 41, at, nil)
	if len(got) != 1 || got[0].Country != "DE" || got[0].Value != "6" || got[0].ExpiresAt != nil || !got[0].RetrievedAt.Equal(at) {
		t.Fatalf("unexpected evidence: %+v", got)
	}
	if len(parseTMDBSeriesCertificationEvidence(payload, 0, at, nil)) != 0 {
		t.Fatal("missing response ID was replaced by caller identity")
	}
	for i := 0; i < 300; i++ {
		payload.Results = append(payload.Results, tmdbContentRating{Country: "DE", Rating: fmt.Sprint(i)})
	}
	if len(parseTMDBSeriesCertificationEvidence(payload, 41, at, nil)) > models.MaxCertificationEvidence {
		t.Fatal("unbounded provider evidence")
	}
	client := &tmdbClient{}
	if client.certificationExpiry(at) != nil {
		t.Fatal("cacheless payload invented expiry")
	}
}

func TestCertificationEvidenceConcurrentDetailsReturnsIndependentSlices(t *testing.T) {
	s, _ := shelfTestService(t)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		once.Do(func() { close(started) })
		<-release
		return jsonResponse(200, certificationMovieFixture, nil), nil
	})}
	results := make(chan *models.Title, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			title, err := s.tmdb.movieDetails(t.Context(), 123)
			if err != nil {
				t.Error(err)
				return
			}
			results <- title
		}()
	}
	<-started
	close(release)
	wg.Wait()
	close(results)
	all := []*models.Title{}
	for title := range results {
		all = append(all, title)
	}
	if len(all) != 8 {
		t.Fatal("missing concurrent result")
	}
	for i, title := range all {
		title.CertificationEvidence[0].Value = fmt.Sprint(i)
		if i > 0 && all[i-1].CertificationEvidence[0].Value != fmt.Sprint(i-1) {
			t.Fatal("singleflight returned shared evidence slice")
		}
	}
}

func TestCertificationEvidenceCachedReleaseAndFieldProjectionPreserveRetrieval(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	evidence := parseTMDBSeriesCertificationEvidence(tmdbContentRatingsResponse{Results: []tmdbContentRating{{Country: "DE", Rating: "6"}}}, 41, at, nil)
	full := models.Title{Certification: "TV-PG", CertificationEvidence: evidence}
	data, _ := json.Marshal(full)
	var restored models.Title
	_ = json.Unmarshal(data, &restored)
	projected := extractTitleFields(&restored, []string{"certification"})
	if len(projected.CertificationEvidence) != 1 || !projected.CertificationEvidence[0].RetrievedAt.Equal(at) {
		t.Fatal("certification projection dropped or renewed evidence")
	}
}

func TestCertificationEvidenceRawMovieRowsSurviveNegativeReleaseWindowCache(t *testing.T) {
	s, _ := shelfTestService(t)
	var calls atomic.Int32
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(200, `{"id":123,"title":"Synthetic","release_dates":{"results":[{"iso_3166_1":"DE","release_dates":[{"type":99,"certification":"6"}]}]}}`, nil), nil
	})}
	first := models.Title{MediaType: "movie", TMDBID: 123}
	if s.enrichMovieReleases(t.Context(), &first, 123) || len(first.CertificationEvidence) != 1 || first.Certification != "" || first.CertificationEvidence[0].ReleaseType != 0 {
		t.Fatal("raw-only provider row was dropped or became a release window")
	}
	second := models.Title{MediaType: "movie", TMDBID: 123}
	if s.enrichMovieReleases(t.Context(), &second, 123) || len(second.CertificationEvidence) != 1 || !second.CertificationEvidence[0].RetrievedAt.Equal(first.CertificationEvidence[0].RetrievedAt) || calls.Load() != 1 {
		t.Fatal("negative window cache dropped provenance or retried")
	}
}

func TestCertificationEvidenceSearchNeverCrossesCollidingProviderNamespaces(t *testing.T) {
	s, _ := shelfTestService(t)
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("cached namespace test contacted provider")
		return nil, fmt.Errorf("unexpected request")
	})}
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	tv := parseTMDBSeriesCertificationEvidence(tmdbContentRatingsResponse{Results: []tmdbContentRating{{Country: "DE", Rating: "6"}}}, 41, at, nil)
	movie := models.CopyCertificationEvidence(tv)
	movie[0].SourceTitleID = "tmdb:movie:41"
	movie[0].Scope = "movie_release"
	_ = s.cache.set(tvCertificationEvidenceCacheKey(41), tv)
	_ = s.cache.set(cacheKey("tmdb", "tv", "content_rating", "v1", "41"), "TV-PG")
	_ = s.cache.set(cacheKey("tmdb", "movie", "releases", "v2", "41"), cachedReleasesWithCert{Certification: "PG", CertificationEvidence: movie, Releases: []models.Release{{Type: "digital"}}})
	results := []models.SearchResult{{Title: models.Title{MediaType: "person", TMDBID: 41}}, {Title: models.Title{MediaType: "unknown", TMDBID: 41}}, {Title: models.Title{MediaType: "series", TMDBID: 41}}, {Title: models.Title{MediaType: "Movie", TMDBID: 41}}}
	s.EnrichSearchCertifications(t.Context(), results)
	for _, i := range []int{0, 1} {
		if results[i].Title.Certification != "TV-PG" || results[i].Title.CertificationEvidence != nil {
			t.Fatal("typed TV provenance crossed namespaces or legacy scalar behavior changed")
		}
	}
	if results[2].Title.CertificationEvidence[0].SourceTitleID != "tmdb:series:41" || results[3].Title.CertificationEvidence[0].SourceTitleID != "tmdb:movie:41" {
		t.Fatal("supported namespace selected the wrong cached provenance")
	}
}
