package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
	"novastream/services/debrid"
	"novastream/services/mediaevidence"
	"novastream/services/sourcefilters"
)

type sourceFilterSearchRecorder struct {
	mu      sync.Mutex
	options []debrid.SearchOptions
	results []models.NZBResult
}

func (s *sourceFilterSearchRecorder) Search(ctx context.Context, options debrid.SearchOptions) ([]models.NZBResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.options = append(s.options, options)
	out := append([]models.NZBResult(nil), s.results...)
	for i := range out {
		out[i].Attributes = make(map[string]string, len(s.results[i].Attributes))
		for key, value := range s.results[i].Attributes {
			out[i].Attributes[key] = value
		}
	}
	return out, nil
}

func TestSourceFiltersRetentionPrecedesPresentationCapsWithoutExtraFetches(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		perResolution, finalLimit, normalCount int
	}{
		{"per_resolution_and_final", 1, 1, 1},
		{"final_fifty", 0, 50, 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := time.Now().UTC().Add(-time.Minute)
			results := []models.NZBResult{}
			for i := 0; i < 64; i++ {
				language, token := "eng", "ENGLISH"
				if i == 63 {
					language, token = "fra", "FRENCH"
				}
				result := models.NZBResult{GUID: fmt.Sprintf("synthetic-%03d", i), Title: fmt.Sprintf("Movie.2024.1080p.WEB-DL.%s-GROUP%03d", token, i), Indexer: "Synthetic", ServiceType: models.ServiceTypeDebrid, Link: fmt.Sprintf("https://example.invalid/never-open/%d", i), SizeBytes: int64(1_000_000 - i*1000), Attributes: map[string]string{"resolution": "1080p", "languages": language}}
				mediaevidence.ObserveCandidate(&result, observed)
				results = append(results, result)
			}
			for _, rejected := range []struct{ id, title, resolution string }{
				{"camera", "Movie.2024.1080p.CAM-GROUP", "1080p"},
				{"too_large_frame", "Movie.2024.2160p.WEB-DL-GROUP", "2160p"},
			} {
				result := models.NZBResult{GUID: rejected.id, Title: rejected.title, Indexer: "Synthetic", ServiceType: models.ServiceTypeDebrid, Link: "https://example.invalid/never-open/" + rejected.id, SizeBytes: 100, Attributes: map[string]string{"resolution": rejected.resolution}}
				mediaevidence.ObserveCandidate(&result, observed)
				results = append(results, result)
			}
			settings := config.DefaultSettings()
			settings.Streaming.ServiceMode = config.StreamingServiceModeDebrid
			settings.Filtering.MaxResolution = "1080p"
			settings.Filtering.FilterOutTerms = []string{"CAM"}
			settings.Playback.MaxResultsPerResolution = tc.perResolution
			settings.Ranking.Criteria = []config.RankingCriterion{{ID: config.RankingSize, Name: "File Size", Enabled: true, Order: 0}}
			manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
			if err := manager.Save(settings); err != nil {
				t.Fatal(err)
			}
			provider := &sourceFilterSearchRecorder{results: results}
			service := NewService(manager, nil, provider)
			options := SearchOptions{Query: "Movie 2024", MediaType: "movie", Year: 2024, MaxResults: tc.finalLimit, IncludeFiltered: true}
			normal, err := service.SearchWithScoring(t.Context(), options)
			if err != nil || len(normal) != tc.normalCount {
				t.Fatalf("ordinary capped results=%d err=%v", len(normal), err)
			}
			for _, row := range normal {
				if row.GUID == "synthetic-063" {
					t.Fatal("late language fixture unexpectedly survived the ordinary cap")
				}
			}
			retainedOptions := options
			retainedOptions.RetainForSourceFilters = true
			retained, err := service.SearchWithScoring(t.Context(), retainedOptions)
			if err != nil || len(retained) != len(results) {
				t.Fatalf("retained results=%d want=%d err=%v", len(retained), len(results), err)
			}
			passed, rejected, matched := 0, 0, []string{}
			for _, row := range retained {
				if row.FilterStatus == "filtered" {
					rejected++
					if (row.GUID != "camera" && row.GUID != "too_large_frame") || row.FilterReason == "" {
						t.Fatal("retention lost or changed a policy rejection")
					}
					continue
				}
				if row.FilterStatus != "passed" || row.GUID == "camera" || row.GUID == "too_large_frame" {
					t.Fatal("retention promoted a rejected candidate")
				}
				passed++
				facts := mediaevidence.Candidate(row.NZBResult, time.Now())
				if sourcefilters.Evaluate(sourcefilters.Filters{Basis: "candidate", Unknown: "exclude", AudioLanguage: "fra"}, facts, sourcefilters.Target{MediaType: "movie"}, time.Now()) == "match" {
					matched = append(matched, row.GUID)
				}
				if facts == nil || !facts.Candidate.ObservedAt.Equal(observed) {
					t.Fatal("retention refreshed or lost the original claim timestamp")
				}
			}
			if passed != 64 || rejected != 2 || !reflect.DeepEqual(matched, []string{"synthetic-063"}) {
				t.Fatalf("passed=%d rejected=%d French matches=%v", passed, rejected, matched)
			}
			provider.mu.Lock()
			calls := append([]debrid.SearchOptions(nil), provider.options...)
			provider.mu.Unlock()
			if len(calls) != 1 || calls[0].MaxResults != 0 {
				t.Fatalf("warm retained search performed extra/provider-capped fetches: %d", len(calls))
			}
			// A first negotiated search also performs exactly the same ordinary
			// source fetch. Retention changes presentation, not provider options.
			coldProvider := &sourceFilterSearchRecorder{results: results}
			cold := NewService(manager, nil, coldProvider)
			coldResults, err := cold.SearchWithScoring(t.Context(), retainedOptions)
			if err != nil || len(coldResults) != len(results) {
				t.Fatal("first retained search lost candidates")
			}
			coldProvider.mu.Lock()
			coldCalls := append([]debrid.SearchOptions(nil), coldProvider.options...)
			coldProvider.mu.Unlock()
			if !reflect.DeepEqual(calls, coldCalls) {
				t.Fatal("retention changed normal upstream query/fetch options")
			}
		})
	}
}
