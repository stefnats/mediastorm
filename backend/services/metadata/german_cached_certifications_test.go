package metadata

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"

	"novastream/models"
	"novastream/services/kids"
)

func TestGermanCatalogueUsesRetainedNormalDetailsWithoutPerCardRequests(t *testing.T) {
	service, _ := shelfTestService(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/3/movie/123" {
			t.Errorf("unexpected synthetic metadata request: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":123,"title":"Stored German movie","release_dates":{"results":[{"iso_3166_1":"US","release_dates":[{"type":3,"certification":"PG"}]},{"iso_3166_1":"DE","release_dates":[{"type":3,"certification":"6"}]}]}}`))
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	defer client.CloseIdleConnections()
	transport := client.Transport
	service.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(original *http.Request) (*http.Response, error) {
		copied := original.Clone(original.Context())
		address := *original.URL
		address.Scheme = target.Scheme
		address.Host = target.Host
		copied.URL = &address
		return transport.RoundTrip(copied)
	})}
	// This is the existing ordinary details request, not a new browse warmup.
	title, err := service.tmdb.movieDetails(t.Context(), 123)
	if err != nil || title == nil || requests.Load() != 1 {
		t.Fatal("normal synthetic details did not retain regional metadata", err)
	}
	rows := make([]models.Title, 100)
	for i := range rows {
		id := int64(1000 + i)
		rows[i] = models.Title{ID: "tmdb:movie:" + strconv.FormatInt(id, 10), TMDBID: id, MediaType: "movie", Certification: "PG"}
	}
	rows[73] = models.Title{ID: "tmdb:movie:123", TMDBID: 123, MediaType: "movie", Certification: "PG"}
	service.EnrichCachedTitleCertifications(rows)
	if requests.Load() != 1 {
		t.Fatal("DE catalogue initiated additional provider requests")
	}
	movie, series, _ := kids.RatingLimits(models.User{IsKidsProfile: true, KidsMode: "rating_de", KidsMaxMovieRating: "6", KidsMaxTVRating: "6"})
	allowed := kids.FilterTitlesByRatings(rows, movie, series)
	if len(allowed) != 1 || allowed[0].TMDBID != 123 {
		t.Fatal("missing German data was not blocked or stored facts were lost")
	}
	if allowed[0].Certification != "PG" {
		t.Fatal("legacy US scalar was erased or rewritten")
	}
	if !allowed[0].CertificationEvidence[0].RetrievedAt.Equal(title.CertificationEvidence[0].RetrievedAt) {
		t.Fatal("cache read renewed evidence")
	}
	service.EnrichCachedTitleCertifications(rows)
	if requests.Load() != 1 {
		t.Fatal("repeat filtered browse fetched metadata")
	}
}

func TestGermanExplicitSeriesEnrichmentRetainsUSScalarAndNormalDetailsCache(t *testing.T) {
	service, _ := shelfTestService(t)
	var requests atomic.Int32
	service.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		if r.URL.Path != "/3/tv/41" {
			t.Errorf("unexpected metadata path: %s", r.URL.Path)
		}
		return jsonResponse(200, `{"id":41,"name":"Synthetic series","content_ratings":{"results":[{"iso_3166_1":"US","rating":"TV-PG"},{"iso_3166_1":"DE","rating":"6"}]}}`, nil), nil
	})}
	title := models.Title{ID: "tmdb:series:41", TMDBID: 41, MediaType: "series", Certification: "TV-MA"}
	service.EnrichGermanTitleCertification(t.Context(), &title)
	if title.Certification != "TV-MA" || requests.Load() != 1 {
		t.Fatal("explicit German lookup erased US scalar or did not use normal details")
	}
	_, series, _ := kids.RatingLimits(models.User{IsKidsProfile: true, KidsMode: "rating_de", KidsMaxMovieRating: "6", KidsMaxTVRating: "6"})
	if !kids.IsTitleRatingAllowed(title, series) {
		t.Fatal("normal title response's retained German series facts were ignored")
	}
	original := models.CopyCertificationEvidence(title.CertificationEvidence)
	copy := title
	copy.CertificationEvidence = nil
	service.EnrichGermanTitleCertification(t.Context(), &copy)
	if requests.Load() != 1 || len(copy.CertificationEvidence) != len(original) || !copy.CertificationEvidence[0].RetrievedAt.Equal(original[0].RetrievedAt) {
		t.Fatal("normal cached lookup fetched or renewed metadata")
	}
}
