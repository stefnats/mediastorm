package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/client_settings"
	"novastream/services/playback"
	"novastream/services/trackselect"
	"novastream/services/user_settings"
)

func TestObservedMinimumResolution(t *testing.T) {
	for _, tc := range []struct {
		name, minimum string
		width, height int
		rejected      bool
	}{
		{"real720 fails4k", "2160p", 1280, 720, true},
		{"real720 permitted", "720p", 1280, 720, false},
		{"no minimum", "", 1280, 720, false},
		{"cropped4k", "2160p", 3840, 1600, false},
		{"unknown is not4k", "2160p", 0, 0, true},
		{"unsupported preference", "invalid", 3840, 2160, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &VideoFullResult{VideoCodec: "h264", VideoWidth: tc.width, VideoHeight: tc.height}
			if err := validateMinimumVideoResolution(probe, tc.minimum); (err != nil) != tc.rejected {
				t.Fatalf("minimum=%q: %v", tc.minimum, err)
			}
		})
	}
	if err := validateMinimumVideoResolution(nil, "2160p"); err == nil {
		t.Fatal("missing probe satisfied a strict request")
	}
	// MaxResolution remains an independent upper bound on local playback.
	if err := validateLocalFallbackQuality(&VideoFullResult{VideoCodec: "h264", VideoWidth: 3840, VideoHeight: 2160}, models.FilterSettings{MinResolution: models.StringPtr("720p"), MaxResolution: models.StringPtr("1080p")}); err == nil {
		t.Fatal("minimum disabled the existing maximum")
	}
}

func TestMinimumResolutionSettingsAndPreparedReuse(t *testing.T) {
	h, _, _ := newFallbackWorker(t, &fallbackSearch{})
	profiles, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	devices, err := client_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.userSettingsSvc, h.clientSettingsSvc = profiles, devices
	cfg, err := h.configManager.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Filtering.MinResolution = "2160p"
	if err := h.configManager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	resolver := h.TrackPreferenceResolver()
	key, _, filter := h.prequeueSettingsScopeDetails("profile", "device", "tmdb:movie:123", nil)
	if models.StringVal(filter.MinResolution, "") != "2160p" {
		t.Fatal("global minimum not inherited")
	}
	if err := profiles.Update("profile", models.UserSettings{Filtering: models.FilterSettings{MinResolution: models.StringPtr("720p")}}); err != nil {
		t.Fatal(err)
	}
	stored, err := profiles.Get("profile")
	if err != nil || stored == nil || models.StringVal(stored.Filtering.MinResolution, "") != "720p" {
		t.Fatalf("profile preference not stored: %v", err)
	}
	profileKey, _, filter := h.prequeueSettingsScopeDetails("profile", "device", "tmdb:movie:123", nil)
	if profileKey == key || models.StringVal(filter.MinResolution, "") != "720p" {
		t.Fatal("changed profile reused old strict preparation")
	}
	if got, err := requestedMinimumResolution(resolver, "profile", "device"); err != nil || got != "720p" {
		t.Fatalf("manual/profile minimum=%q: %v", got, err)
	}
	if err := devices.Update("device", "profile", models.ClientFilterSettings{MinResolution: models.StringPtr("")}); err != nil {
		t.Fatal(err)
	}
	deviceKey, _, filter := h.prequeueSettingsScopeDetails("profile", "device", "tmdb:movie:123", nil)
	if deviceKey == profileKey || filter.MinResolution == nil || *filter.MinResolution != "" {
		t.Fatal("explicit device clear was lost or reused old preparation")
	}
	if got, err := requestedMinimumResolution(resolver, "profile", "device"); err != nil || got != "" {
		t.Fatalf("manual/device minimum=%q: %v", got, err)
	}
	if got, err := requestedMinimumResolution(resolver, "another-profile", ""); err != nil || got != "2160p" {
		t.Fatalf("profile preference leaked: %q %v", got, err)
	}
	fields := SettingsSchema["filtering"].(map[string]interface{})["fields"].(map[string]interface{})
	if fields["minResolution"] == nil {
		t.Fatal("minimum preference is missing from the existing settings form")
	}
}

func TestMislabelledQualityUsesSuitableAlternative(t *testing.T) {
	for _, mode := range []string{"strict4k", "allow720", "unknown", "local below minimum"} {
		t.Run(mode, func(t *testing.T) {
			first := models.NZBResult{Title: "Synthetic.Movie.2160p.WEB-DL", GUID: "mislabelled", Link: "https://example.invalid/first", ServiceType: models.ServiceTypeDebrid, Indexer: "AIOStreams", Attributes: map[string]string{"resolution": "2160p"}}
			alternative := first
			alternative.Title = "Synthetic.Movie.2160p.WEB-DL.Alternative"
			alternative.GUID = "alternative"
			alternative.Link = "https://example.invalid/second"
			search := &fallbackSearch{results: []models.NZBResult{first, alternative}}
			h, catalog, _ := newFallbackWorker(t, search)
			catalog.empty = mode != "local below minimum"
			cfg, err := h.configManager.Load()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Filtering.MinResolution = "2160p"
			cfg.Streaming.ResolveFirstReadySource = false
			if mode == "allow720" {
				cfg.Filtering.MinResolution = "720p"
			}
			cfg.Playback.PreferLocalFiles = mode == "local below minimum"
			if err := h.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			h.playbackSvc = &stubPlaybackService{resolve: func(_ context.Context, candidate models.NZBResult) (*models.PlaybackResolution, error) {
				return &models.PlaybackResolution{WebDAVPath: "/webdav/" + candidate.GUID + ".mkv"}, nil
			}}
			h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
				p := &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720, Duration: 100}
				if strings.Contains(path, "alternative") {
					p.VideoWidth, p.VideoHeight = 3840, 2160
				}
				if mode == "unknown" && strings.Contains(path, "mislabelled") {
					p.VideoWidth, p.VideoHeight = 0, 0
				}
				return p, nil
			}}
			got := runFallbackWorker(t, h)
			want := "alternative"
			wantResolution := "2160p"
			if mode == "allow720" {
				want = "mislabelled"
				wantResolution = "720p"
			}
			if got.Status != playback.PrequeueStatusReady || got.SelectedResult == nil || got.SelectedResult.GUID != want {
				t.Fatalf("selection=%+v error=%s", got.SelectedResult, got.Error)
			}
			if got.ResultAttributes["resolution"] != wantResolution || got.ResultAttributes["advertisedResolution"] != "2160p" || got.ResultAttributes["resolutionSource"] != "file_probe" {
				t.Fatalf("incorrect display attributes: %#v", got.ResultAttributes)
			}
			if got.SelectedResult.Attributes["resolution"] != "2160p" || first.Attributes["resolution"] != "2160p" {
				t.Fatal("probe rewrote the original provider claim/search data")
			}
			// A second projection must not turn the corrected label into the provider claim.
			again := observedQualityAttributes(got.ResultAttributes, &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720})
			if again["advertisedResolution"] != "2160p" {
				t.Fatal("original advertised resolution was replaced on refresh")
			}
		})
	}
}

func TestManualResolveHonoursObservedMinimum(t *testing.T) {
	for _, mode := range []string{"reject720", "allow720", "unknown", "queued", "batch"} {
		t.Run(mode, func(t *testing.T) {
			prequeue, _, _ := newFallbackWorker(t, &fallbackSearch{})
			cfg, err := prequeue.configManager.Load()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Filtering.MinResolution = "2160p"
			if mode == "allow720" {
				cfg.Filtering.MinResolution = "720p"
			}
			if err := prequeue.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			p := &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720}
			if mode == "unknown" {
				p.VideoWidth, p.VideoHeight = 0, 0
			}
			h := NewPlaybackHandler(&mockPlaybackService{
				resolveFunc: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
					return &models.PlaybackResolution{WebDAVPath: "/synthetic.mkv"}, nil
				},
				queueStatusFunc: func(context.Context, int64) (*models.PlaybackResolution, error) {
					return &models.PlaybackResolution{QueueID: 1, WebDAVPath: "/synthetic.mkv"}, nil
				},
			})
			h.TrackPreferences = &trackselect.Resolver{Config: prequeue.configManager}
			calls := 0
			h.VideoProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { calls++; return p, nil }}
			body := []byte(`{"result":{"title":"Synthetic2160p","attributes":{"resolution":"2160p"}},"episodes":[{"seasonNumber":1,"episodeNumber":1}]}`)
			r := httptest.NewRequest(http.MethodPost, "/api/playback/resolve", bytes.NewReader(body))
			w := httptest.NewRecorder()
			switch mode {
			case "queued":
				r = httptest.NewRequest(http.MethodGet, "/api/playback/queue/1", nil)
				r = mux.SetURLVars(r, map[string]string{"queueID": "1"})
				h.QueueStatus(w, r)
			case "batch":
				h.ResolveBatch(w, r)
			default:
				h.Resolve(w, r)
			}
			if calls != 1 {
				t.Fatalf("real file selection was not probed once: %d", calls)
			}
			if mode == "allow720" {
				if w.Code != http.StatusOK {
					t.Fatalf("allowed file rejected: %d %s", w.Code, w.Body.String())
				}
				var got models.PlaybackResolution
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.QualityAttributes["resolution"] != "720p" || got.QualityAttributes["advertisedResolution"] != "2160p" {
					t.Fatalf("manual quality: %#v", got.QualityAttributes)
				}
			} else if mode == "batch" {
				var got models.BatchResolveResponse
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if w.Code != http.StatusOK || len(got.Results) != 1 || got.Results[0].Resolution != nil || !strings.Contains(got.Results[0].Error, "720p") {
					t.Fatalf("batch quality mismatch not exposed: %s", w.Body.String())
				}
			} else if w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("strict minimum bypassed: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestObservedQualityUsesActualFileProbe(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal("ffmpeg required for synthetic media regression")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal("ffprobe required for synthetic media regression")
	}
	file := filepath.Join(t.TempDir(), "Synthetic.claimed.2160p.mkv")
	cmd := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=1280x720:r=1", "-frames:v", "1", "-c:v", "libx264", "-threads", "1", file)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate synthetic720p: %v %s", err, output)
	}
	video := &VideoHandler{ffprobePath: ffprobe, streamer: directCastTestProvider{directURL: file}, metadataCache: make(map[string]*cachedMetadataEntry)}
	probe, err := video.ProbeVideoFull(t.Context(), file)
	if err != nil {
		t.Fatal(err)
	}
	if probe.VideoWidth != 1280 || probe.VideoHeight != 720 || probe.MetadataEvidence == nil {
		t.Fatalf("actual file probe dimensions=%dx%d", probe.VideoWidth, probe.VideoHeight)
	}
	if err := validateMinimumVideoResolution(probe, "2160p"); err == nil {
		t.Fatal("filename overrode actual720p video")
	}
	if err := validateMinimumVideoResolution(probe, "720p"); err != nil {
		t.Fatal(err)
	}
	attrs := observedQualityAttributes(map[string]string{"resolution": "2160p"}, probe)
	if attrs["resolution"] != "720p" || attrs["advertisedResolution"] != "2160p" {
		t.Fatalf("actual probe display=%#v", attrs)
	}
}

func TestMigrationCannotBypassMinimumOrKeepOldFileQuality(t *testing.T) {
	for _, mode := range []string{"strict4k", "allow720"} {
		t.Run(mode, func(t *testing.T) {
			h, _, _ := newFallbackWorker(t, &fallbackSearch{})
			cfg, err := h.configManager.Load()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Filtering.MinResolution = "2160p"
			if mode == "allow720" {
				cfg.Filtering.MinResolution = "720p"
			}
			if err := h.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			entry, _ := h.store.Create("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "manual")
			h.store.Update(entry.ID, func(e *playback.PrequeueEntry) {
				e.Status = playback.PrequeueStatusReady
				e.StreamPath = "/old.mkv"
				e.ResultAttributes = map[string]string{"resolution": "2160p", "videoWidth": "3840", "resolutionSource": "file_probe"}
			})
			probeCalls := 0
			h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
				probeCalls++
				return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720, Duration: 100, AudioStreams: []AudioStreamInfo{{Index: 4, Codec: "aac", Language: "eng"}}}, nil
			}}
			payload, err := json.Marshal(adoptMigrationRequest{StreamPath: "/new.mkv", Result: models.NZBResult{Title: "Synthetic2160p", GUID: "new", Attributes: map[string]string{"resolution": "2160p"}}})
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue/"+entry.ID+"/adopt-migration", bytes.NewReader(payload))
			r = mux.SetURLVars(r, map[string]string{"prequeueID": entry.ID})
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
			w := httptest.NewRecorder()
			h.AdoptMigration(w, r)
			after, _ := h.store.Get(entry.ID)
			if probeCalls != 1 {
				t.Fatalf("migration probed the actual file %d times, want exactly one", probeCalls)
			}
			if mode == "allow720" && (after.Duration != 100 || len(after.AudioTracks) != 1 || after.AudioTracks[0].Index != 4) {
				t.Fatalf("observed probe did not populate migration metadata: duration=%v audio=%+v", after.Duration, after.AudioTracks)
			}
			if mode == "strict4k" {
				if w.Code != http.StatusUnprocessableEntity || after.StreamPath != "/old.mkv" {
					t.Fatalf("strict migration replaced current playback: %d %s", w.Code, w.Body.String())
				}
			} else if w.Code != http.StatusOK || after.StreamPath != "/new.mkv" || after.ResultAttributes["resolution"] != "720p" || after.ResultAttributes["videoWidth"] != "1280" || after.ResultAttributes["advertisedResolution"] != "2160p" {
				t.Fatalf("new source retained old file facts: %d %#v %s", w.Code, after.ResultAttributes, w.Body.String())
			}
		})
	}
}

func TestManualTracksReuseOneProbeForQuality(t *testing.T) {
	for _, minimum := range []string{"720p", ""} {
		t.Run("minimum="+minimum, func(t *testing.T) {
			prequeue, _, _ := newFallbackWorker(t, &fallbackSearch{})
			cfg, err := prequeue.configManager.Load()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Filtering.MinResolution = minimum
			if err := prequeue.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			shared := &models.PlaybackResolution{WebDAVPath: "/synthetic720p.mkv"}
			h := NewPlaybackHandler(&mockPlaybackService{resolveFunc: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) { return shared, nil }})
			h.TrackPreferences = &trackselect.Resolver{Config: prequeue.configManager}
			calls := 0
			h.VideoProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
				calls++
				return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720, AudioStreams: []AudioStreamInfo{{Index: 1, Codec: "aac", Language: "eng"}}}, nil
			}}
			r := httptest.NewRequest(http.MethodPost, "/api/playback/resolve", strings.NewReader(`{"result":{"title":"Synthetic2160p","attributes":{"resolution":"2160p"}},"includeTrackSelection":true}`))
			w := httptest.NewRecorder()
			h.Resolve(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("resolve: %d %s", w.Code, w.Body.String())
			}
			var got models.PlaybackResolution
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || got.TrackSelection == nil || got.TrackSelection.Audio == nil || got.TrackSelection.Audio.StreamIndex != 1 || got.QualityAttributes["resolution"] != "720p" || got.QualityAttributes["advertisedResolution"] != "2160p" {
				t.Fatalf("calls=%d tracks=%+v quality=%#v", calls, got.TrackSelection, got.QualityAttributes)
			}
			if shared.Probe != nil || shared.TrackSelection != nil || shared.QualityAttributes != nil {
				t.Fatal("request probe/selection mutated the shared service result")
			}
		})
	}
}
