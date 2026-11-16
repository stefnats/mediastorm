package debrid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"novastream/config"
	"novastream/models"
	"novastream/services/mediaevidence"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFreshExplicitDebridProbePreservesInventoryAndOriginalTimes(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "ffprobe")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s' '{\"streams\":[{\"index\":0,\"codec_type\":\"video\",\"codec_name\":\"hevc\"},{\"index\":1,\"codec_type\":\"audio\",\"codec_name\":\"aac\",\"tags\":{\"language\":\"isl\"},\"disposition\":{\"default\":1}},{\"index\":3,\"codec_type\":\"subtitle\",\"codec_name\":\"unknown_codec\"}]}'\n"), 0755); err != nil {
		t.Fatal(err)
	}
	hs := NewHealthService(config.NewManager(filepath.Join(dir, "config.json")))
	hs.SetFFProbePath(binary)
	result, err := hs.probeAllTracks(context.Background(), "https://example.invalid/synthetic-no-io")
	if err != nil {
		t.Fatal(err)
	}
	if result.MetadataEvidence == nil || len(result.MetadataEvidence.SubtitleStreams) != 1 || !result.MetadataEvidence.SubtitleStreamsComplete || result.MetadataEvidence.AudioStreams[0].Language != "isl" || result.MetadataEvidence.AudioStreams[0].AudioDescription != nil || result.MetadataEvidence.Video.HDR != "" {
		t.Fatalf("raw metadata: %+v", result.MetadataEvidence)
	}
	candidate := models.NZBResult{GUID: "release", Attributes: map[string]string{"languages": "fr"}}
	binding := trackEvidenceBinding{candidate: candidate, selection: "selected-part"}
	hs.probeTracksAsync("hash", "https://example.invalid/synthetic-no-io", binding)
	cached := hs.trackCache["hash"]
	if cached.mediaEvidence == nil || cached.candidateVersion != mediaevidence.CandidateVersion(candidate) || cached.selection != "selected-part" {
		t.Fatal("probe association missing")
	}
	copy := cloneDebridHealthCheck(DebridHealthCheck{MediaEvidence: cached.mediaEvidence})
	copy.MediaEvidence.Probed.AudioStreams[0].Language = "fra"
	if cached.mediaEvidence.Probed.AudioStreams[0].Language != "isl" {
		t.Fatal("cache shared mutable facts")
	}
	old := cached.mediaEvidence.Probed.ObservedAt
	if !models.CurrentMediaEvidence(cached.mediaEvidence, time.Now()).Probed.ObservedAt.Equal(old) {
		t.Fatal("cache renewed timestamps")
	}
	if models.CurrentMediaEvidence(cached.mediaEvidence, old.Add(16*time.Minute)) != nil {
		t.Fatal("two-hour legacy track cache extended fifteen-minute evidence")
	}
}

func TestSharedFullProbeDifferentInputCannotProduceDebridFacts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" {
			t.Fatal("unexpected byte/provider demand")
		}
		w.Header().Set("Content-Type", "video/x-matroska")
		w.Header().Set("Content-Length", "10485760")
	}))
	defer server.Close()
	at := time.Now()
	probe := &models.VideoFullResult{AudioStreams: []models.AudioStreamInfo{{Index: 1, Codec: "aac", Language: "fra"}}, MetadataEvidence: mediaevidence.ProbeFacts([]mediaevidence.ProbeStream{{Index: 1, CodecType: "audio", CodecName: "aac", Tags: map[string]string{"language": "fra"}}}, at), MetadataEvidenceInputVersion: mediaevidence.InputVersion("different-file")}
	prober := &reusableFullProber{result: probe}
	hs := NewHealthService(config.NewManager(filepath.Join(t.TempDir(), "config.json")))
	hs.SetFFProbePath("must-not-run")
	hs.SetFullProber(prober)
	result, err := hs.CheckHealth(context.Background(), models.NZBResult{Title: "Synthetic", Link: server.URL + "/video.mkv", ServiceType: models.ServiceTypeDebrid, Attributes: map[string]string{"preresolved": "true", "stream_url": server.URL + "/video.mkv"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.MediaEvidence != nil || result.MediaProbe.MetadataEvidence != nil || prober.calls != 1 {
		t.Fatal("unrelated fresh probe was reassociated or extra probe requested")
	}
}
