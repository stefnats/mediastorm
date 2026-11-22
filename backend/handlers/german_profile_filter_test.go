package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/kids"
)

type germanProfiles struct{ value models.User }

func (p *germanProfiles) Get(id string) (models.User, bool)    { return p.value, id == p.value.ID }
func (p *germanProfiles) BelongsToAccount(string, string) bool { return true }

type germanMetadataFixture struct {
	value models.Title
	reads int
}

func (m *germanMetadataFixture) MovieInfo(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	m.reads++
	copy := m.value
	copy.CertificationEvidence = models.CopyCertificationEvidence(copy.CertificationEvidence)
	return &copy, nil
}
func (m *germanMetadataFixture) SeriesDetails(context.Context, models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	m.reads++
	return &models.SeriesDetails{Title: m.value}, nil
}
func germanFacts(kind, age string) models.Title {
	now := time.Now().UTC()
	expires := now.Add(time.Hour)
	scope := "series"
	if kind == "movie" {
		scope = "movie_release"
	}
	return models.Title{ID: "tmdb:" + kind + ":123", TMDBID: 123, MediaType: kind, Certification: "PG", CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: age, Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:" + kind + ":123", Scope: scope, RetrievedAt: now.Add(-time.Minute), ExpiresAt: &expires}}}
}

type germanPlaybackFixture struct{ resolves, queues, batches int }

func (s *germanPlaybackFixture) Resolve(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
	s.resolves++
	return &models.PlaybackResolution{WebDAVPath: "/synthetic/allowed.mkv"}, nil
}
func (s *germanPlaybackFixture) ResolveBatch(context.Context, models.NZBResult, []models.BatchEpisodeTarget) (*models.BatchResolveResponse, error) {
	s.batches++
	return &models.BatchResolveResponse{}, nil
}
func (s *germanPlaybackFixture) QueueStatus(context.Context, int64) (*models.PlaybackResolution, error) {
	s.queues++
	return &models.PlaybackResolution{WebDAVPath: "/synthetic/allowed.mkv"}, nil
}
func germanProfileFixture() *germanProfiles {
	return &germanProfiles{models.User{ID: "child", IsKidsProfile: true, KidsMode: kids.GermanMetadataMode, KidsMaxMovieRating: "6", KidsMaxTVRating: "12"}}
}
func TestGermanManualResolveAndQueuedPollUseActualProfileLimits(t *testing.T) {
	for _, kind := range []string{"resolve", "queue", "batch"} {
		t.Run(kind, func(t *testing.T) {
			profiles := germanProfileFixture()
			metadata := &germanMetadataFixture{value: germanFacts("movie", "12")}
			service := &germanPlaybackFixture{}
			h := NewPlaybackHandler(service)
			h.SetUsersService(profiles)
			h.SetGermanAgeMetadata(metadata)
			invoke := func() *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				switch kind {
				case "resolve":
					h.Resolve(w, httptest.NewRequest(http.MethodPost, "/api/playback/resolve", strings.NewReader(`{"profileId":"child","titleId":"tmdb:movie:123","result":{"title":"Claimed harmless title"}}`)))
				case "queue":
					r := httptest.NewRequest(http.MethodGet, "/api/playback/queue/7?profileId=child&titleId=tmdb:movie:123", nil)
					h.QueueStatus(w, mux.SetURLVars(r, map[string]string{"queueID": "7"}))
				case "batch":
					// DownloadApi.resolveBatch sends result.raw from downloadSources.
					// The actual indexer response attaches both selectors to that result.
					results := []models.NZBResult{{Title: "Synthetic season pack"}}
					annotateResultsProfile(results, "child", "tmdb:series:123")
					body, err := json.Marshal(map[string]any{"profileId": "child", "result": results[0], "episodes": []map[string]any{{"seasonNumber": 1, "episodeNumber": 1, "episodeCode": "S01E01"}}})
					if err != nil {
						t.Fatal(err)
					}
					h.ResolveBatch(w, httptest.NewRequest(http.MethodPost, "/api/playback/resolve-batch", strings.NewReader(string(body))))
				}
				return w
			}
			if kind == "batch" {
				metadata.value = germanFacts("series", "16")
			}
			denied := invoke()
			if denied.Code != 403 || service.resolves+service.queues+service.batches != 0 {
				t.Fatal("denied DE request performed source work", denied.Code)
			}
			var errorBody map[string]any
			if json.Unmarshal(denied.Body.Bytes(), &errorBody) != nil || errorBody["code"] != "kids_vod_classification_unknown" {
				t.Fatal("missing understandable age error")
			}
			if kind == "batch" {
				metadata.value = germanFacts("series", "6")
			} else {
				metadata.value = germanFacts("movie", "6")
			}
			allowed := invoke()
			if allowed.Code != 200 || service.resolves+service.queues+service.batches != 1 {
				t.Fatal("allowed DE source not returned", allowed.Code, allowed.Body.String())
			}
			metadata.value.CertificationEvidence = nil
			if again := invoke(); again.Code != 403 || service.resolves+service.queues+service.batches != 1 {
				t.Fatal("lost metadata reused an old positive age result")
			}
		})
	}
}
func TestGermanAgeChecksLeaveAdultsAndLegacyUSUnchanged(t *testing.T) {
	profiles := germanProfileFixture()
	profiles.value.KidsMode = "rating"
	metadata := &germanMetadataFixture{value: germanFacts("movie", "18")}
	if err := checkGermanProfileTitle(t.Context(), profiles, metadata, "child", "tmdb:movie:123", "movie"); err != nil || metadata.reads != 0 {
		t.Fatal("legacy mode performed German lookup")
	}
	profiles.value.KidsMode = kids.GermanMetadataMode
	profiles.value.IsKidsProfile = false
	if err := checkGermanProfileTitle(t.Context(), profiles, metadata, "child", "tmdb:movie:123", "movie"); err != nil || metadata.reads != 0 {
		t.Fatal("adult acquired a child rule")
	}
	profiles.value.IsKidsProfile = true
	for _, invalid := range []string{"tmdb:episode:123", "tmdb:person:123", "tmdb:unknown:123"} {
		if err := checkGermanProfileTitle(t.Context(), profiles, metadata, "child", invalid, "series"); err == nil || metadata.reads != 0 {
			t.Fatal("non-series identity became series metadata", invalid)
		}
	}
	if err := checkGermanProfileTitle(t.Context(), profiles, nil, "child", "tmdb:movie:123", "movie"); err == nil {
		t.Fatal("missing metadata service allowed German playback")
	}
}

// This wrapper supplies the normal metadata interface without allowing any provider method.
type germanCachedService struct {
	metadataService
	cached, network int
	fact            models.Title
}

func (s *germanCachedService) EnrichCachedTitleCertifications(titles []models.Title) {
	s.cached++
	for i := range titles {
		if titles[i].TMDBID == s.fact.TMDBID {
			titles[i].CertificationEvidence = models.CopyCertificationEvidence(s.fact.CertificationEvidence)
		}
	}
}
func (s *germanCachedService) EnrichTrendingCertifications(context.Context, []models.TrendingItem) {
	s.network++
}
func (s *germanCachedService) EnrichTitleCertifications(context.Context, []models.Title) { s.network++ }
func (s *germanCachedService) EnrichSearchCertifications(context.Context, []models.SearchResult) {
	s.network++
}
func TestGermanCatalogueEnrichmentIsCacheOnlyWithExistingUSScalars(t *testing.T) {
	profiles := germanProfileFixture()
	service := &germanCachedService{fact: germanFacts("movie", "6")}
	h := &MetadataHandler{UsersService: profiles}
	items := make([]models.TrendingItem, 100)
	for i := range items {
		items[i].Title = models.Title{ID: "tmdb:movie:999", TMDBID: 999, MediaType: "movie", Certification: "PG"}
	}
	items[50].Title = service.fact
	items[50].Title.CertificationEvidence = nil
	filtered := h.filterTrendingByKids(t.Context(), "child", service, items)
	if len(filtered) != 1 || filtered[0].Title.TMDBID != 123 || service.cached != 1 || service.network != 0 {
		t.Fatal("DE cards fetched providers or ignored retained evidence")
	}
	profiles.value.KidsMode = "rating"
	profiles.value.KidsMaxMovieRating = "PG"
	h.filterTrendingByKids(t.Context(), "child", service, items)
	if service.network != 1 {
		t.Fatal("legacy enrichment behavior changed")
	}
}

// Only the exercised ordinary detail methods are supplied by this local fixture.
type germanDetailsFixture struct {
	metadataService
	value models.Title
}

func (s *germanDetailsFixture) MovieDetails(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	copy := s.value
	copy.CertificationEvidence = models.CopyCertificationEvidence(copy.CertificationEvidence)
	return &copy, nil
}
func (s *germanDetailsFixture) SeriesDetails(context.Context, models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	return &models.SeriesDetails{Title: s.value}, nil
}
func (s *germanDetailsFixture) EnrichCachedTitleCertifications([]models.Title) {}
func TestGermanDetailsStartupRecommendationsAndLocalMatchAgree(t *testing.T) {
	profiles := germanProfileFixture()
	service := &germanDetailsFixture{}
	h := NewMetadataHandler(service, nil)
	h.UsersService = profiles
	startup := &StartupHandler{usersProvider: profiles}
	local := &PrequeueHandler{users: profiles}
	for _, kind := range []string{"movie", "series"} {
		t.Run(kind, func(t *testing.T) {
			for _, age := range []string{"6", "18", ""} {
				title := germanFacts(kind, age)
				service.value = title
				want := age == "6"
				w := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, "/details?userId=child&tmdbId=123", nil)
				if kind == "movie" {
					h.MovieDetails(w, request)
				} else {
					h.SeriesDetails(w, request)
				}
				if (w.Code == http.StatusOK) != want {
					t.Fatalf("%s details: %d %s", age, w.Code, w.Body.String())
				}
				items := []models.TrendingItem{{Title: title}}
				if (len(startup.applyFilters(items, "child", unreleasedVisibilityPolicy{IncludeMovies: true, IncludeShows: true}, false)) == 1) != want {
					t.Fatal("startup disagrees")
				}
				if isPersonalizedRatingAllowed(title, h.personalizedKidsRatingFilter("child")) != want {
					t.Fatal("recommendations disagree")
				}
				libraryType := models.LocalMediaLibraryTypeMovie
				if kind == "series" {
					libraryType = models.LocalMediaLibraryTypeShow
				}
				if local.localItemAllowed(models.LocalMediaItem{LibraryType: libraryType, Metadata: &title}, "child") != want {
					t.Fatal("local choice disagrees")
				}
			}
		})
	}
}

type germanJSONProfiles struct {
	usersService
	value models.User
}

func (s *germanJSONProfiles) Get(id string) (models.User, bool) { return s.value, id == s.value.ID }
func TestGermanJSONRatingValidationDoesNotChangeLegacyValues(t *testing.T) {
	profiles := &germanJSONProfiles{value: germanProfileFixture().value}
	h := &UsersHandler{Service: profiles}
	for _, value := range []string{"0", "6", "12", "16", "18"} {
		if !h.validGermanRatings(httptest.NewRecorder(), "child", value) {
			t.Fatal("canonical age denied", value)
		}
	}
	for _, value := range []string{"", "7", "G", "TV-Y", "FSK 6"} {
		w := httptest.NewRecorder()
		if h.validGermanRatings(w, "child", value) || w.Code != 400 {
			t.Fatal("invalid DE value accepted", value)
		}
	}
	profiles.value.KidsMode = "rating"
	if !h.validGermanRatings(httptest.NewRecorder(), "child", "", "future-us-value") {
		t.Fatal("legacy decoding changed")
	}
}

func TestGermanAgeDenialKeepsSavedReportAndDoesNotStartReplacement(t *testing.T) {
	for _, action := range []string{"report", "report-and-retry", "retry"} {
		t.Run(action, func(t *testing.T) {
			h, entry, reports := releaseReportFixture(t)
			profiles := germanProfileFixture()
			profiles.value.ID = entry.UserID
			h.users = profiles
			h.SetGermanAgeMetadata(&germanMetadataFixture{value: germanFacts("movie", "18")})
			body := releaseReportRequest{SourceVersion: sourceReportVersion(entry), Reason: "wrong_title", Retry: action == "report-and-retry"}
			w := httptest.NewRecorder()
			if action == "retry" {
				body.Reason = ""
				h.RetryRelease(w, reportRequest(t, http.MethodPost, entry.ID, body, false))
				if w.Code != 403 || len(reports.List()) != 0 {
					t.Fatal("denied retry started or saved report", w.Code, w.Body.String())
				}
			} else {
				h.ReportRelease(w, reportRequest(t, http.MethodPost, entry.ID, body, false))
				if w.Code != 200 || len(reports.List()) != 1 {
					t.Fatal("report was lost with separate age decision", w.Code, w.Body.String())
				}
				var response map[string]json.RawMessage
				if json.Unmarshal(w.Body.Bytes(), &response) != nil || response["report"] == nil || response["replacement"] != nil {
					t.Fatal("unexpected replacement/report response")
				}
				if action == "report-and-retry" && response["retryError"] == nil {
					t.Fatal("saved report's denied retry was not explained")
				}
			}
			current, ok := h.store.Get(entry.ID)
			if !ok || current.StreamPath != entry.StreamPath {
				t.Fatal("denied retry replaced existing selection")
			}
		})
	}
}
