package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

// This fixture exposes stored normal-detail facts only; US enrichment calls are counted.
type germanStoredMetadata struct {
	*fakeMetadataService
	facts               []models.Title
	reads               atomic.Int32
	providerEnrichments atomic.Int32
}

func (s *germanStoredMetadata) EnrichCachedTitleCertifications(titles []models.Title) {
	s.reads.Add(1)
	for i := range titles {
		for _, fact := range s.facts {
			if titles[i].TMDBID == fact.TMDBID && titles[i].MediaType == fact.MediaType {
				titles[i].CertificationEvidence = models.MergeCertificationEvidence(fact.CertificationEvidence, titles[i].CertificationEvidence)
			}
		}
	}
}
func (s *germanStoredMetadata) EnrichTitleCertifications(context.Context, []models.Title) {
	s.providerEnrichments.Add(1)
}
func (s *germanStoredMetadata) EnrichTrendingCertifications(context.Context, []models.TrendingItem) {
	s.providerEnrichments.Add(1)
}
func (s *germanStoredMetadata) EnrichSearchCertifications(context.Context, []models.SearchResult) {
	s.providerEnrichments.Add(1)
}
func germanStoredFixture() (*germanStoredMetadata, *germanProfiles) {
	profile := germanProfileFixture()
	profile.value.ID = "profile"
	profile.value.AccountID = "account"
	fact := germanFacts("movie", "6")
	fact.Name = "Stored German movie"
	return &germanStoredMetadata{fakeMetadataService: &fakeMetadataService{}, facts: []models.Title{fact}}, profile
}
func TestGermanTasteSetupUsesNumericSeparateLimits(t *testing.T) {
	_, profiles := germanStoredFixture()
	h := &MetadataHandler{UsersService: profiles}
	if got := h.tasteMediaTypes("profile"); len(got) != 2 || got[0] != "movie" || got[1] != "series" || !h.tasteAvailable("profile") {
		t.Fatal("valid DE setup unavailable", got)
	}
	profiles.value.KidsMaxMovieRating = "7"
	if got := h.tasteMediaTypes("profile"); len(got) != 1 || got[0] != "series" {
		t.Fatal("invalid movie age enabled setup", got)
	}
	profiles.value.KidsMaxTVRating = ""
	profiles.value.KidsMaxRating = "PG"
	if h.tasteAvailable("profile") {
		t.Fatal("generic US value enabled DE setup")
	}
	profiles.value.KidsMode = "rating"
	profiles.value.KidsMaxMovieRating = "PG"
	profiles.value.KidsMaxTVRating = "TV-14"
	if len(h.tasteMediaTypes("profile")) != 2 {
		t.Fatal("legacy setup changed")
	}
}
func TestGermanStartupAndSimilarCandidatesHydrateStoredNormalDetails(t *testing.T) {
	service, profiles := germanStoredFixture()
	card := service.facts[0]
	card.CertificationEvidence = nil
	startup := &StartupHandler{usersProvider: profiles, metadata: service}
	original := []models.TrendingItem{{Title: card}}
	filtered := startup.applyFilters(original, "profile", unreleasedVisibilityPolicy{IncludeMovies: true, IncludeShows: true}, false)
	if len(filtered) != 1 || len(filtered[0].Title.CertificationEvidence) == 0 || len(original[0].Title.CertificationEvidence) != 0 {
		t.Fatal("startup lost cached facts or mutated stored card")
	}
	service.similarByKey = map[string][]models.Title{"movie:1": {card}}
	h := NewMetadataHandler(service, testConfigManager(t))
	h.UsersService = profiles
	h.HistoryService = &fakeMetadataHistoryService{}
	h.WatchlistService = &recommendationWatchlistFake{items: map[string][]models.WatchlistItem{"profile": {{ID: "tmdb:movie:1", MediaType: "movie", Name: "Saved seed", AddedAt: time.Now()}}}}
	additional := h.additionalRecommendationSeeds("profile")
	response := h.buildPersonalizedRecommendations(t.Context(), "profile", nil, nil, 14, 10, h.personalizedKidsRatingFilter("profile"), additional, true)
	if len(response.Items) != 1 || response.Items[0].Title.TMDBID != 123 {
		t.Fatal("similar candidate ignored stored DE detail facts", response)
	}
	if service.providerEnrichments.Load() != 0 {
		t.Fatal("cache decision requested certification enrichment")
	}
	if len(service.similarByKey["movie:1"][0].CertificationEvidence) != 0 {
		t.Fatal("shared similar card mutated")
	}
}
func TestGermanPersonalizedOutputRechecksCachedExpiryAndConflict(t *testing.T) {
	for _, scenario := range []string{"expired", "current-conflict", "current-sidecar"} {
		t.Run(scenario, func(t *testing.T) {
			service, profiles := germanStoredFixture()
			title := service.facts[0]
			want := 0
			switch scenario {
			case "expired":
				past := time.Now().Add(-time.Second)
				title.CertificationEvidence[0].ExpiresAt = &past
				service.facts = nil
			case "current-conflict":
				service.facts = []models.Title{germanFacts("movie", "12")}
			case "current-sidecar":
				title.CertificationEvidence = nil
				want = 1
			}
			h := NewMetadataHandler(service, testConfigManager(t))
			h.UsersService = profiles
			h.HistoryService = &fakeMetadataHistoryService{}
			response := PersonalizedRecommendationsResponse{Items: []models.TrendingItem{{Title: title}}, Movies: []models.TrendingItem{{Title: title}}, Series: []models.TrendingItem{{Title: title}}, Total: 1}
			w := httptest.NewRecorder()
			h.writePersonalizedRecommendations(w, httptest.NewRequest(http.MethodGet, "/recommendations?userId=profile&deferArtwork=true", nil), "profile", response, nil, nil)
			var actual PersonalizedRecommendationsResponse
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &actual) != nil || len(actual.Items) != want || len(actual.Movies) != want || len(actual.Series) != want || actual.Total != want {
				t.Fatal("cached response bypassed current DE facts", w.Code, w.Body.String())
			}
			if service.providerEnrichments.Load() != 0 {
				t.Fatal("output initiated provider certification requests")
			}
			if len(response.Items) != 1 || response.Total != 1 {
				t.Fatal("shared response mutated")
			}
		})
	}
}
func TestGermanDeliveredBareTitleCanBeRatedUsingCurrentStoredFacts(t *testing.T) {
	service, profiles := germanStoredFixture()
	h, ratings, repo, _ := ratingFixture(t)
	h.Service = service
	h.UsersService = profiles
	title := service.facts[0]
	ratings.Observe("profile", "fixture-client", []models.Title{title})
	bare, ok := ratings.Resolve("profile", "fixture-client", "movie", title.ID)
	if !ok || len(bare.CertificationEvidence) != 0 {
		t.Fatal("fixture did not exercise actual bare projection")
	}
	if status, value := ratingHTTP(t, h, http.MethodPut, title.ID, `{"rating":"love"}`); status != 200 || value != "love" {
		t.Fatal("allowed delivered title could not be rated", status, value)
	}
	if status, value := ratingHTTP(t, h, http.MethodGet, title.ID, ""); status != 200 || value != "love" {
		t.Fatal("saved rating unavailable", status, value)
	}
	if len(repo.items["profile"]) != 1 || len(repo.items["profile"][0].Title.CertificationEvidence) != 0 {
		t.Fatal("durable preferences acquired metadata facts")
	}
	seeds, err := h.ratingRecommendationSeeds(t.Context(), "profile")
	if err != nil || len(seeds) != 1 {
		t.Fatal("bare loved title lost current permitted seed", err, seeds)
	}
	service.facts = nil
	if status, _ := ratingHTTP(t, h, http.MethodGet, title.ID, ""); status != 404 || repo.writes != 1 {
		t.Fatal("missing current facts reused stored age decision", status)
	}
	if service.providerEnrichments.Load() != 0 {
		t.Fatal("rating fetched provider certification")
	}
}
