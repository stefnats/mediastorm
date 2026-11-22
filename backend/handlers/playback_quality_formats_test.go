package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/playback"
	"novastream/services/trackselect"
	"novastream/services/user_settings"
)

func qualityFormatProbe() *VideoFullResult {
	return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1280, VideoHeight: 720,
		AudioStreams:     []AudioStreamInfo{{Index: 4, Codec: "aac", Language: "eng"}, {Index: 8, Codec: "truehd", Language: "fra"}},
		MetadataEvidence: &models.MediaProbeFacts{Video: models.MediaVideoFacts{Codec: "h264", Width: 1280, Height: 720, HDR: "sdr"}, AudioStreamsComplete: true, AudioStreams: []models.MediaAudioFacts{{Index: 4, Codec: "aac", Language: "eng"}, {Index: 8, Codec: "truehd", Language: "fra"}}},
	}
}
func qualityFormatClaims() map[string]string {
	return map[string]string{"resolution": "2160p", "codec": "HEVC", "audio": "TrueHD,Atmos", "hdr": "DV,HDR10", "hasDV": "true", "source": "WEB-DL"}
}
func assertQualityFormat(t *testing.T, got map[string]string, audio string, index int) {
	t.Helper()
	if got["codec"] != "h264" || got["codecSource"] != "file_probe" || got["advertisedCodec"] != "HEVC" || got["hdr"] != "SDR" || got["hdrSource"] != "file_probe" || got["hasDV"] != "false" || got["advertisedHDR"] != "DV,HDR10" || got["advertisedHasDV"] != "true" {
		t.Fatal("video claim was not corrected separately", got)
	}
	if got["audio"] != audio || got["audioSource"] != "file_probe" || got["advertisedAudio"] != "TrueHD,Atmos" {
		t.Fatal("selected audio claim was not corrected separately", got)
	}
	if got["audioStreamIndex"] != strconv.Itoa(index) {
		t.Fatal("audio belongs to another container index", got)
	}
}

func TestObservedCodecHDRUsesSelectedTrackAndKeepsClaims(t *testing.T) {
	claims := qualityFormatClaims()
	original := qualityFormatClaims()
	probe := qualityFormatProbe()
	first := observedQualityAttributes(claims, probe, 4)
	assertQualityFormat(t, first, "aac", 4)
	if !reflect.DeepEqual(claims, original) || probe.AudioStreams[0].Codec != "aac" {
		t.Fatal("projection mutated release/probe")
	}
	second := observedQualityAttributes(first, probe, 8)
	assertQualityFormat(t, second, "truehd", 8)
	if first["audio"] != "aac" || first["audioStreamIndex"] != "4" {
		t.Fatal("second projection mutated first track")
	}
	// The selected file has no stream 99. Do not keep stream 8's codec as if it
	// belonged to this selection or invent a first-track fallback.
	missing := observedQualityAttributes(second, probe, 99)
	if missing["audioSource"] != "" || missing["audioStreamIndex"] != "" || missing["audio"] != original["audio"] || missing["advertisedAudio"] != original["audio"] {
		t.Fatal("old track codec survived an unknown selection", missing)
	}
	if got := observedQualityAttributes(claims, probe); got["audioSource"] != "" || got["audio"] != claims["audio"] {
		t.Fatal("unselected multi-track inventory pretended to be selected", got)
	}
}

func TestObservedDynamicRangeDoesNotGuessSDR(t *testing.T) {
	for _, tc := range []struct {
		name    string
		probe   *VideoFullResult
		hdr, dv string
	}{
		{"nil", nil, "DV,HDR10", "true"},
		{"legacy-false-is-unknown", &VideoFullResult{VideoCodec: "h264"}, "DV,HDR10", "true"},
		{"explicit-unknown", &VideoFullResult{MetadataEvidence: &models.MediaProbeFacts{Video: models.MediaVideoFacts{}}}, "DV,HDR10", "true"},
		{"positive-hdr10", &VideoFullResult{HasHDR10: true}, "HDR10", "false"},
		{"positive-dv", &VideoFullResult{HasDolbyVision: true}, "DV", "true"},
		{"observed-hlg", &VideoFullResult{MetadataEvidence: &models.MediaProbeFacts{Video: models.MediaVideoFacts{HDR: "hlg"}}}, "HLG", "false"},
		{"explicit-sdr", qualityFormatProbe(), "SDR", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := observedQualityAttributes(qualityFormatClaims(), tc.probe)
			if got["hdr"] != tc.hdr || got["hasDV"] != tc.dv {
				t.Fatal("wrong dynamic range", got)
			}
			known := tc.name != "nil" && tc.name != "legacy-false-is-unknown" && tc.name != "explicit-unknown"
			if (got["hdrSource"] == "file_probe") != known {
				t.Fatal("unknown HDR became a file observation", got)
			}
		})
	}
	// A later incomplete projection cannot recycle a previous explicit SDR fact.
	previous := observedQualityAttributes(qualityFormatClaims(), qualityFormatProbe(), 4)
	unknown := observedQualityAttributes(previous, &VideoFullResult{})
	if unknown["hdrSource"] != "" || unknown["hdr"] != "DV,HDR10" || unknown["hasDV"] != "true" {
		t.Fatal("earlier file fact became a fresh SDR claim", unknown)
	}
}

func TestObservedSoleAudioRequiresCompleteInventory(t *testing.T) {
	for _, tc := range []struct {
		name        string
		complete    bool
		raw, legacy []models.MediaAudioFacts
		selected    []int
		known       bool
	}{
		{"complete-sole", true, []models.MediaAudioFacts{{Index: 4, Codec: "aac"}}, nil, nil, true},
		{"incomplete-sole", false, []models.MediaAudioFacts{{Index: 4, Codec: "aac"}}, nil, nil, false},
		{"filtered-list-is-not-sole", true, []models.MediaAudioFacts{{Index: 4, Codec: "aac"}, {Index: 8, Codec: "truehd"}}, []models.MediaAudioFacts{{Index: 4, Codec: "aac"}}, nil, false},
		{"exact-selected-from-incomplete", false, []models.MediaAudioFacts{{Index: 4, Codec: "aac"}}, nil, []int{4}, true},
		{"unknown-codec", true, []models.MediaAudioFacts{{Index: 4, Codec: "unknown"}}, nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &VideoFullResult{MetadataEvidence: &models.MediaProbeFacts{AudioStreamsComplete: tc.complete, AudioStreams: tc.raw}}
			for _, row := range tc.legacy {
				probe.AudioStreams = append(probe.AudioStreams, AudioStreamInfo{Index: row.Index, Codec: row.Codec})
			}
			got := observedQualityAttributes(qualityFormatClaims(), probe, tc.selected...)
			if (got["audioSource"] == "file_probe") != tc.known {
				t.Fatal("audio inventory was mistaken for a selected stream", got)
			}
			if tc.known && (got["audio"] != "aac" || got["audioStreamIndex"] != "4") {
				t.Fatal("wrong selected codec", got)
			}
		})
	}
}

func TestManualObservedFormatsReuseExistingProbe(t *testing.T) {
	for _, mode := range []string{"resolve", "queue"} {
		t.Run(mode, func(t *testing.T) {
			prequeue, _, _ := newFallbackWorker(t, &fallbackSearch{})
			cfg, err := prequeue.configManager.Load()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Playback.PreferredAudioLanguage = "eng"
			if err = prequeue.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			original := &models.PlaybackResolution{WebDAVPath: "/synthetic-selected.mkv", Probe: qualityFormatProbe(), QualityAttributes: qualityFormatClaims()}
			h := NewPlaybackHandler(&mockPlaybackService{resolveFunc: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) { return original, nil }, queueStatusFunc: func(context.Context, int64) (*models.PlaybackResolution, error) { return original, nil }})
			h.TrackPreferences = &trackselect.Resolver{Config: prequeue.configManager}
			probes := 0
			h.VideoProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { probes++; return nil, nil }}
			body, _ := json.Marshal(map[string]any{"result": models.NZBResult{Title: "Synthetic.HEVC.DV.TrueHD", Attributes: qualityFormatClaims()}, "includeTrackSelection": true})
			r := httptest.NewRequest(http.MethodPost, "/api/playback/resolve", bytes.NewReader(body))
			w := httptest.NewRecorder()
			if mode == "queue" {
				r = httptest.NewRequest(http.MethodGet, "/api/playback/queue/1?includeTrackSelection=true", nil)
				r = mux.SetURLVars(r, map[string]string{"queueID": "1"})
				h.QueueStatus(w, r)
			} else {
				h.Resolve(w, r)
			}
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var got models.PlaybackResolution
			if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			assertQualityFormat(t, got.QualityAttributes, "aac", 4)
			if probes != 0 || got.TrackSelection.AudioStreamIndex() != 4 {
				t.Fatal("re-probed the file or changed the selected track", probes, got.TrackSelection)
			}
			if original.QualityAttributes["codec"] != "HEVC" || original.TrackSelection != nil {
				t.Fatal("shared service result mutated")
			}
		})
	}
}

type qualityUnexpectedMetadataProbe struct{ t *testing.T }

func (p qualityUnexpectedMetadataProbe) ProbeVideoMetadata(context.Context, string) (*VideoMetadataResult, error) {
	p.t.Fatal("cached full probe was not reused for track metadata")
	return nil, nil
}

func TestAutomaticObservedFormatsReuseSelectedFileProbe(t *testing.T) {
	candidate := models.NZBResult{GUID: "observed-format", Title: "Synthetic.HEVC.DV.TrueHD", Link: "https://example.invalid/selected", ServiceType: models.ServiceTypeDebrid, Attributes: qualityFormatClaims()}
	h, catalog, _ := newFallbackWorker(t, &fallbackSearch{results: []models.NZBResult{candidate}})
	// Match normal server wiring without permitting an extra metadata probe.
	h.metadataProber = qualityUnexpectedMetadataProbe{t}
	preferences, preferenceErr := user_settings.NewService(t.TempDir())
	if preferenceErr != nil {
		t.Fatal(preferenceErr)
	}
	h.userSettingsSvc = preferences
	catalog.empty = true
	cfg, err := h.configManager.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Filtering.MinResolution = "720p"
	cfg.Playback.PreferredAudioLanguage = "eng"
	cfg.Streaming.ResolveFirstReadySource = false
	if err = h.configManager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	h.playbackSvc = &stubPlaybackService{resolve: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
		return &models.PlaybackResolution{WebDAVPath: "/synthetic-selected.mkv"}, nil
	}}
	calls := 0
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { calls++; return qualityFormatProbe(), nil }}
	got := runFallbackWorker(t, h)
	if got.Status != playback.PrequeueStatusReady || calls != 1 {
		t.Fatal("preparation failed or re-probed", got.Error, calls)
	}
	assertQualityFormat(t, got.ResultAttributes, "aac", 4)
	if got.SelectedAudioTrack != 4 || got.SelectedResult.Attributes["codec"] != "HEVC" || candidate.Attributes["audio"] != "TrueHD,Atmos" {
		t.Fatal("wrong selected track or changed provider claims", got.SelectedAudioTrack, got.SelectedResult)
	}
}

func TestObservedFormatsComeFromActualFileProbe(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "Synthetic.claimed.HEVC.DV.TrueHD.mkv")
	command := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "1", "-c:v", "libx264", "-threads", "1", "-x264-params", "colorprim=bt709:transfer=bt709:colormatrix=bt709", "-color_trc", "bt709", "-color_primaries", "bt709", "-colorspace", "bt709", "-c:a", "aac", "-metadata:s:a:0", "language=eng", file)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatal("generate synthetic codec/HDR fixture", err, string(output))
	}
	video := &VideoHandler{ffprobePath: ffprobe, streamer: directCastTestProvider{directURL: file}, metadataCache: map[string]*cachedMetadataEntry{}}
	probe, err := video.ProbeVideoFull(t.Context(), file)
	if err != nil {
		t.Fatal(err)
	}
	if probe.VideoCodec != "h264" || probe.MetadataEvidence == nil || probe.MetadataEvidence.Video.HDR != "sdr" || len(probe.AudioStreams) != 1 || probe.AudioStreams[0].Codec != "aac" {
		t.Fatal("actual fixture probe differs", probe)
	}
	actual := observedQualityAttributes(qualityFormatClaims(), probe, probe.AudioStreams[0].Index)
	if actual["codec"] != "h264" || actual["audio"] != "aac" || actual["hdr"] != "SDR" || actual["hasDV"] != "false" || actual["advertisedCodec"] != "HEVC" || actual["advertisedAudio"] != "TrueHD,Atmos" || actual["advertisedHDR"] != "DV,HDR10" {
		t.Fatal("release title overrode actual file formats", actual)
	}
}

func TestObservedFormatsFollowHealthyReportedAudio(t *testing.T) {
	prequeue, entry, registry := releaseReportFixture(t)
	broken := 2
	if _, err := registry.Report(badstreams.ReportRequest{Result: *entry.SelectedResult, Context: badstreams.PlaybackContext{ProfileID: "profile", TitleID: entry.TitleID, AudioTrackIndex: &broken}, Reason: "audio_problem", ReporterID: "profile"}); err != nil {
		t.Fatal(err)
	}
	probe := qualityFormatProbe()
	probe.AudioStreams = []AudioStreamInfo{{Index: 1, Language: "eng", Codec: "flac"}, {Index: 2, Language: "deu", Codec: "truehd"}, {Index: 3, Language: "deu", Codec: "aac"}}
	probe.MetadataEvidence.AudioStreams = []models.MediaAudioFacts{{Index: 1, Language: "eng", Codec: "flac"}, {Index: 2, Language: "deu", Codec: "truehd"}, {Index: 3, Language: "deu", Codec: "aac"}}
	manual := &PlaybackHandler{BadStreams: registry, TrackPreferences: prequeue.trackPreferenceResolver()}
	calls := 0
	manual.VideoProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { calls++; return probe, nil }}
	original := &models.PlaybackResolution{WebDAVPath: entry.StreamPath}
	result, err := manual.filterResolvedReportedAudio(reportRequest(t, "POST", entry.ID, nil, false), *entry.SelectedResult, original, prequeue.reportContextForPrequeue(entry.ID, "profile", "fixture-device", nil))
	if err != nil || result == nil {
		t.Fatal("healthy track not available", err)
	}
	if calls != 1 || result.Probe != probe || result.TrackSelection.AudioStreamIndex() != 3 || original.Probe != nil {
		t.Fatal("actual existing report probe/track was lost or mutated", calls, result)
	}
	quality := observedQualityAttributes(qualityFormatClaims(), result.Probe, result.TrackSelection.AudioStreamIndex())
	if quality["audio"] != "aac" || quality["audioStreamIndex"] != "3" || quality["advertisedAudio"] != "TrueHD,Atmos" {
		t.Fatal("reported track codec still shown", quality)
	}
}

func TestMigrationObservedFormatsDoNotKeepOldFileFacts(t *testing.T) {
	h, _, _ := newFallbackWorker(t, &fallbackSearch{})
	cfg, err := h.configManager.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Filtering.MinResolution = "720p"
	cfg.Playback.PreferredAudioLanguage = "eng"
	if err = h.configManager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	entry, _ := h.store.Create("tmdb:movie:123", "Synthetic", "profile", "movie", 2024, nil, "manual")
	h.store.Update(entry.ID, func(e *playback.PrequeueEntry) {
		e.Status = playback.PrequeueStatusReady
		e.StreamPath = "/old-file.mkv"
		e.ResultAttributes = observedQualityAttributes(qualityFormatClaims(), qualityFormatProbe(), 4)
	})
	next := qualityFormatProbe()
	next.VideoCodec = "vp9"
	next.MetadataEvidence.Video.Codec = "vp9"
	next.MetadataEvidence.Video.HDR = "hlg"
	next.AudioStreams = []AudioStreamInfo{{Index: 4, Codec: "opus", Language: "eng"}}
	next.MetadataEvidence.AudioStreams = []models.MediaAudioFacts{{Index: 4, Codec: "opus", Language: "eng"}}
	calls := 0
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { calls++; return next, nil }}
	payload, _ := json.Marshal(adoptMigrationRequest{StreamPath: "/new-file.mkv", Result: models.NZBResult{Title: "Synthetic.HEVC.DV.TrueHD", GUID: "new-source", Attributes: qualityFormatClaims()}})
	request := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue/"+entry.ID+"/adopt-migration", bytes.NewReader(payload))
	request = mux.SetURLVars(request, map[string]string{"prequeueID": entry.ID})
	request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeyAccountID, "account"))
	response := httptest.NewRecorder()
	h.AdoptMigration(response, request)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	got, _ := h.store.Get(entry.ID)
	if calls != 1 || got.StreamPath != "/new-file.mkv" || got.SelectedAudioTrack != 4 || got.ResultAttributes["codec"] != "vp9" || got.ResultAttributes["audio"] != "opus" || got.ResultAttributes["hdr"] != "HLG" || got.ResultAttributes["hasDV"] != "false" {
		t.Fatal("old file attributes survived migration or probe repeated", calls, got.ResultAttributes)
	}
	if got.ResultAttributes["advertisedCodec"] != "HEVC" || got.ResultAttributes["advertisedHDR"] != "DV,HDR10" {
		t.Fatal("original new-release claims lost", got.ResultAttributes)
	}
}
