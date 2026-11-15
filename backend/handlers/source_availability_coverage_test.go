package handlers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/config"
	"novastream/models"
	"novastream/services/indexer"
	"novastream/services/sourceavailability"
)

// The worker sees only a synthetic in-memory provider. Aggregation is exercised
// through real handlers after those existing episode observations are cached.
type coverageAPISearch struct{ calls atomic.Int64 }

func (s *coverageAPISearch) BeginAvailabilitySearch(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (s *coverageAPISearch) SearchWithScoringSplit(_ context.Context, options indexer.SearchOptions) (<-chan indexer.ScoredSplitSearchResult, <-chan indexer.ScoredSplitSearchResult) {
	s.calls.Add(1)
	result := indexer.ScoredSplitSearchResult{}
	if options.Query == "Synthetic Series S01E01" {
		result.RawCount = 1
		result.Scored = []models.ScoredNZBResult{{NZBResult: models.NZBResult{Title: "Synthetic.Series.S01E01.1080p", Indexer: "Synthetic", ServiceType: models.ServiceTypeUsenet, Attributes: map[string]string{"languages": "English, French, Spanish, German"}}, FilterStatus: "passed"}}
	}
	u, d := make(chan indexer.ScoredSplitSearchResult, 1), make(chan indexer.ScoredSplitSearchResult, 1)
	u <- result
	d <- indexer.ScoredSplitSearchResult{Disabled: true}
	close(u)
	close(d)
	return u, d
}

func TestSourceCoverageRealEpisodeAndTitleHTTPResponsesMatchOpenAPI(t *testing.T) {
	h, svc := titleEndpointFixture(t)
	cfg, err := h.CfgManager.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.SourceAvailability.IndexerNames = []string{"Synthetic"}
	cfg.SourceAvailability.MaxTitlesPerCycle = 100
	cfg.Indexers = []config.IndexerConfig{{Name: "Synthetic", Type: "newznab", Enabled: true}}
	if err := h.CfgManager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	search := &coverageAPISearch{}
	svc = sourceavailability.NewService(svc.Store, h.CfgManager, search, sourceavailability.NewScopeFactory(h.CfgManager, nil, nil, nil, func(id string) bool { return id == "profile" }))
	h.SourceAvailability = svc
	h.UsersService = fallbackProfiles{models.User{ID: "profile", AccountID: contractAccountID}}
	title := models.Title{ID: "tmdb:series:123", Name: "Synthetic Series", MediaType: "series", Year: 2020}
	numbering := &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}
	episodes := []models.SeriesEpisode{{ID: "ep-1", SeasonNumber: 1, EpisodeNumber: 1, Numbering: numbering, AiredDate: "2020-01-01"}, {ID: "ep-2", SeasonNumber: 1, EpisodeNumber: 2, Numbering: numbering, AiredDate: "2020-01-02"}}
	h.Service = &fakeMetadataService{seriesResp: &models.SeriesDetails{Title: title, Seasons: []models.SeriesSeason{{Number: 1, Episodes: episodes}}}}
	if _, err := svc.ReplaceEpisodeView("profile", "fixture-client", "seed", 1, title, episodes); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	svc.Start(ctx)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	version := int64(1)
ready:
	for {
		select {
		case <-deadline.C:
			t.Fatal("synthetic worker did not cache episode observations")
		case <-tick.C:
			version++
			response, err := svc.ReplaceEpisodeView("profile", "fixture-client", "seed", version, title, episodes)
			if err != nil {
				t.Fatal(err)
			}
			if response.EpisodeCoverage != nil && response.EpisodeCoverage.State == "partial" {
				break ready
			}
		}
	}
	cancel()
	if search.calls.Load() != 2 {
		t.Fatalf("unexpected synthetic searches: %d", search.calls.Load())
	}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	call := contractCall{method: "PUT", target: "/api/users/profile/source-availability/episodes/http-series", vars: map[string]string{"userID": "profile", "viewID": "http-series"}, headers: map[string]string{"X-Client-ID": "fixture-client"}, body: map[string]any{"version": 1, "titleId": title.ID, "numbering": numbering, "episodes": []map[string]any{{"episodeId": "ep-1", "seasonNumber": 1, "episodeNumber": 1}, {"episodeId": "ep-2", "seasonNumber": 1, "episodeNumber": 2}}}}
	episodeResponse := decodeContract[sourceavailability.EpisodeViewResponse](t, exercise(t, validator, call, h.EpisodeSourceHints, 200))
	if episodeResponse.EpisodeCoverage == nil || episodeResponse.EpisodeCoverage.State != "partial" || len(episodeResponse.Items) != 2 || episodeResponse.Items[0].Availability.State != "found" || episodeResponse.Items[1].Availability.State != "not_found" {
		t.Fatalf("wrong episode HTTP coverage: %+v", episodeResponse)
	}
	svc.AnnotateTitles("profile", "fixture-client", []models.Title{title})
	call.target = "/api/users/profile/source-availability/titles/http-titles"
	call.vars["viewID"] = "http-titles"
	call.body = map[string]any{"version": 1, "titles": []sourceavailability.TitleCoordinate{{ID: title.ID, MediaType: "series"}}}
	response := decodeContract[sourceavailability.TitleViewResponse](t, exercise(t, validator, call, h.TitleSourceHints, 200))
	if len(response.Items) != 1 || response.Items[0].Availability.State != "found" || response.Items[0].Availability.Complete || response.Items[0].Availability.EpisodeCoverage.State != "partial" || response.Items[0].Availability.EpisodeCoverage.CatalogueComplete {
		t.Fatalf("wrong title HTTP coverage: %+v", response)
	}
	if search.calls.Load() != 2 {
		t.Fatal("cached HTTP aggregation requested an additional search")
	}
}
