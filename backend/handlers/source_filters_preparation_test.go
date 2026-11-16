package handlers

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
	"novastream/services/mediaevidence"
	"novastream/services/playback"
	"novastream/services/sourcefilters"
)

func TestSourceFilterPreparationConfigurationIsCapturedBeforeDeferredProbe(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "reconfigured during probe"}[change], func(t *testing.T) {
			result := models.NZBResult{Title: "Synthetic.Movie.2024.1080p", GUID: "fixture-1", ServiceType: models.ServiceTypeDebrid, Attributes: map[string]string{"resolution": "1080p"}}
			h, catalog, _ := newFallbackWorker(t, &fallbackSearch{results: []models.NZBResult{result}})
			catalog.empty = true
			entered, release := make(chan struct{}), make(chan struct{})
			h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
				close(entered)
				<-release
				facts := evidenceFacts(time.Now())
				facts.Video.Codec = "h264"
				return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080, Duration: 100, MetadataEvidence: facts, MetadataEvidenceInputVersion: probeInputVersion(path)}, nil
			}}
			entry, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details", h.prequeueSettingsScopeKey("profile", "device", "tmdb:movie:123"))
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.runPrequeueWorker(entry.ID, entry.TitleID, entry.TitleName, "", "movie", 2024, "profile", "device", nil, nil, 0, true, prequeueWorkerInteractive)
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("probe did not start")
			}
			if change {
				cfg, err := h.configManager.Load()
				if err != nil {
					t.Fatal(err)
				}
				cfg.Playback.PreferredAudioLanguage = "fra"
				if err = h.configManager.Save(cfg); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not complete")
			}
			got, _ := h.store.Get(entry.ID)
			if got.Status != playback.PrequeueStatusReady {
				t.Fatalf("synthetic worker failed: %+v", got)
			}
			if change {
				if got.MediaEvidence != nil || got.MediaEvidenceConfigurationKey != "" {
					t.Fatal("old input was stamped with new provider configuration")
				}
			} else {
				if got.MediaEvidence == nil || got.MediaEvidenceConfigurationKey == "" || got.MediaEvidenceConfigurationKey != h.preparationConfigurationKey() {
					t.Fatal("unchanged provider configuration not bound")
				}
				data, _ := json.Marshal(got)
				if strings.Contains(string(data), got.MediaEvidenceConfigurationKey) {
					t.Fatal("private configuration digest leaked into persisted/public JSON")
				}
			}
		})
	}
}

func TestSourceFilterQueryClientRequiresExactPreparationMarker(t *testing.T) {
	for _, marker := range []string{"", "other-device", "query-device"} {
		t.Run("marker="+marker, func(t *testing.T) {
			h, search, _ := sourceFilterFixture(t, 1)
			prepared := NewPrequeueHandler(nil, nil, nil, nil, nil, false)
			prepared.configManager = testResolutionConfigManager(t, config.DefaultSettings())
			h.SourceFilterPrepared = prepared
			target := sourcefilters.Target{TitleID: "tmdb:movie:41", MediaType: "movie"}
			scope := sourcefilters.Scope{AccountID: "account", ProfileID: "profile", ClientID: "query-device"}
			entry, _ := prepared.store.CreateScoped(target.TitleID, "Fixture", "profile", "movie", 2024, nil, "details", prepared.PrequeueSettingsScopeKey("profile", "query-device", target.TitleID))
			prepared.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) {
				e.Status = playback.PrequeueStatusReady
				e.StreamPath = "/webdav/fixture.mkv"
				e.FileSize = 100
				e.SelectedResult = &search.results[0]
			})
			prepared.recordPreparationEvidence(entry.ID, marker, &VideoFullResult{MetadataEvidence: evidenceFacts(time.Now()), MetadataEvidenceInputVersion: probeInputVersion("/webdav/fixture.mkv")}, prepared.preparationConfigurationKey())
			r := httptest.NewRequest("GET", "/api/indexers/search?clientId=query-device", nil)
			facts := h.sourceFilterPreparedEvidence(r, scope, target)
			version := mediaevidence.CandidateVersion(search.results[0])
			if (facts[version] != nil) != (marker == "query-device") {
				t.Fatalf("query-device adopted marker %q: %+v", marker, facts)
			}
			if r.Header.Get("X-Client-ID") != "" {
				t.Fatal("proof normalization mutated incoming headers")
			}
		})
	}
}
