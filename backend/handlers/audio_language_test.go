package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"novastream/config"
	"novastream/services/audiolanguage"
	"novastream/services/playback"
)

type spokenLanguageFixtureProber struct {
	run func(context.Context, string, int, float64) ([]byte, error)
}

func (spokenLanguageFixtureProber) ProbeVideoFull(context.Context, string) (*VideoFullResult, error) {
	return &VideoFullResult{}, nil
}
func (s spokenLanguageFixtureProber) SamplePreparedAudio(ctx context.Context, path string, index int, start float64) ([]byte, error) {
	return s.run(ctx, path, index, start)
}

func languageFixtureReply(w http.ResponseWriter, _ *http.Request) {
	_ = json.NewEncoder(w).Encode(map[string]any{"task": "transcribe", "language": "es", "duration": 20, "language_probabilities": map[string]float64{"es": 0.97},
		"segments": []map[string]any{{"start": 1, "end": 15, "text": "Synthetic spoken dialogue contains enough distinct letters for a sample.", "avg_logprob": -0.1, "no_speech_prob": 0.01}}})
}

func TestAudioLanguageCheckBindsSourceTrackAndEffectivePreference(t *testing.T) {
	h, entry, registry := releaseReportFixture(t)
	server := httptest.NewServer(http.HandlerFunc(languageFixtureReply))
	defer server.Close()
	cfg := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Playback.AudioLanguageCheckURL = server.URL + "/inference"
	if err := cfg.Save(settings); err != nil {
		t.Fatal(err)
	}
	h.SetConfigManager(cfg)
	var offsets []float64
	h.fullProber = spokenLanguageFixtureProber{run: func(_ context.Context, path string, index int, start float64) ([]byte, error) {
		if path != entry.StreamPath || index != 2 {
			t.Fatal("sample did not use the bound selection")
		}
		offsets = append(offsets, start)
		pcm := make([]byte, audiolanguage.SampleBytes)
		for i := 0; i < len(pcm); i += 2 {
			binary.LittleEndian.PutUint16(pcm[i:], 2000)
		}
		return pcm, nil
	}}
	track := 2
	request := audioLanguageRequest{SourceVersion: sourceReportVersion(entry), AudioTrackIndex: &track, StartOffset: 590}
	w := httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, http.MethodPost, entry.ID, request, false))
	var result struct {
		audiolanguage.Result
		SourceVersion   string `json:"sourceVersion"`
		AudioTrackIndex int    `json:"audioTrackIndex"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Status != "likely_mismatch" || result.RequestedLanguage != "deu" || result.DetectedLanguage != "spa" || result.AudioTrackIndex != 2 || result.SourceVersion != request.SourceVersion {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if len(offsets) != 2 || offsets[0] != 545 || offsets[1] != 580 {
		t.Fatalf("end-clamped windows=%v", offsets)
	}
	if registry.MatchFor(*entry.SelectedResult, h.vodReportContext(entry, "fixture-device")) != nil {
		t.Fatal("analysis silently reported a release")
	}
	current, _ := h.store.Get(entry.ID)
	if current.StreamPath != entry.StreamPath || current.SelectedAudioTrack != 2 || current.Status != playback.PrequeueStatusReady {
		t.Fatal("analysis changed playback")
	}
}

func TestAudioLanguageCheckRejectsStaleSelectionAndDisablesByDefault(t *testing.T) {
	h, entry, _ := releaseReportFixture(t)
	var samples atomic.Int32
	h.fullProber = spokenLanguageFixtureProber{run: func(context.Context, string, int, float64) ([]byte, error) { samples.Add(1); return nil, nil }}
	track := 2
	request := audioLanguageRequest{SourceVersion: sourceReportVersion(entry), AudioTrackIndex: &track}
	w := httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, "POST", entry.ID, request, false))
	if w.Code != 503 || samples.Load() != 0 {
		t.Fatal("unconfigured analysis ran")
	}
	request.SourceVersion = "stale"
	w = httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, "POST", entry.ID, request, false))
	if w.Code != 409 || samples.Load() != 0 {
		t.Fatal("stale selection sampled")
	}
	request.SourceVersion = sourceReportVersion(entry)
	track = 99
	w = httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, "POST", entry.ID, request, false))
	if w.Code != 400 || samples.Load() != 0 {
		t.Fatal("unknown track sampled")
	}
	w = httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, "GET", entry.ID, nil, false))
	if w.Code != 405 {
		t.Fatal("GET started analysis")
	}
}

func TestAudioLanguageCheckDiscardsMidCheckSourceChange(t *testing.T) {
	h, entry, _ := releaseReportFixture(t)
	server := httptest.NewServer(http.HandlerFunc(languageFixtureReply))
	defer server.Close()
	cfg := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Playback.AudioLanguageCheckURL = server.URL
	if err := cfg.Save(settings); err != nil {
		t.Fatal(err)
	}
	h.SetConfigManager(cfg)
	h.fullProber = spokenLanguageFixtureProber{run: func(context.Context, string, int, float64) ([]byte, error) {
		h.store.Update(entry.ID, func(e *playback.PrequeueEntry) { e.StreamPath = "/webdav/new-selection.mp4" })
		return make([]byte, audiolanguage.SampleBytes), nil
	}}
	track := 2
	request := audioLanguageRequest{SourceVersion: sourceReportVersion(entry), AudioTrackIndex: &track}
	w := httptest.NewRecorder()
	h.CheckAudioLanguage(w, reportRequest(t, "POST", entry.ID, request, false))
	if w.Code != 409 || strings.Contains(w.Body.String(), "likely_") {
		t.Fatalf("stale result escaped: %d %s", w.Code, w.Body.String())
	}
}

func TestAudioLanguageEndpointRemainsServerConfiguration(t *testing.T) {
	cfg := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Playback.AudioLanguageCheckURL = "http://127.0.0.1:9876/inference"
	if err := cfg.Save(settings); err != nil {
		t.Fatal(err)
	}
	h := &SettingsHandler{Manager: cfg}
	for _, master := range []bool{false, true} {
		w := httptest.NewRecorder()
		h.GetSettings(w, reportRequest(t, http.MethodGet, "unused", nil, master))
		if w.Code != http.StatusOK || strings.Contains(w.Body.String(), settings.Playback.AudioLanguageCheckURL) != master {
			t.Fatalf("master=%v status=%d endpoint presence mismatch", master, w.Code)
		}
	}
	persisted, err := cfg.Load()
	if err != nil || persisted.Playback.AudioLanguageCheckURL != settings.Playback.AudioLanguageCheckURL {
		t.Fatal("client settings read changed server configuration")
	}
}

func TestAudioLanguageSamplerDecodesExactTrackThroughHTTP(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("FFmpeg required for actual selected-track decoding")
	}
	path := filepath.Join(t.TempDir(), "synthetic-two-track.mkv")
	command := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000", "-t", "21", "-map", "0:a", "-map", "1:a", "-c:a", "pcm_s16le", path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("synthetic audio generation: %v %s", err, output)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(filepath.Dir(path))))
	defer server.Close()
	h := &VideoHandler{ffmpegPath: ffmpeg}
	quiet, err := h.SamplePreparedAudio(t.Context(), server.URL+"/"+filepath.Base(path), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	audible, err := h.SamplePreparedAudio(t.Context(), server.URL+"/"+filepath.Base(path), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(quiet) != audiolanguage.SampleBytes || len(audible) != audiolanguage.SampleBytes {
		t.Fatalf("sample lengths quiet=%d audible=%d", len(quiet), len(audible))
	}
	var nonzeroQuiet, nonzeroAudible int
	for i := 0; i < len(quiet); i += 2 {
		if binary.LittleEndian.Uint16(quiet[i:]) != 0 {
			nonzeroQuiet++
		}
		if binary.LittleEndian.Uint16(audible[i:]) != 0 {
			nonzeroAudible++
		}
	}
	if nonzeroQuiet != 0 || nonzeroAudible < 10000 {
		t.Fatalf("track selection quiet=%d audible=%d", nonzeroQuiet, nonzeroAudible)
	}
}
