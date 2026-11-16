package recordings

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"novastream/models"
)

func TestCaptureRealCodecAndAudioLayoutChangePreservesBothIndependentParts(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("local ffmpeg fixture binary required")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("local ffprobe fixture binary required")
	}
	fastCaptureRetries(t)
	dir := t.TempDir()
	inputs := []string{filepath.Join(dir, "h264-mono.ts"), filepath.Join(dir, "hevc-stereo.ts")}
	for i, path := range inputs {
		codec := "libx264"
		channels := "1"
		if i == 1 {
			codec = "libx265"
			channels = "2"
		}
		args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.6", "-c:v", codec, "-threads", "1"}
		if i == 1 {
			args = append(args, "-x265-params", "pools=none:frame-threads=1:log-level=error")
		} else {
			args = append(args, "-preset", "ultrafast")
		}
		args = append(args, "-c:a", "aac", "-ac", channels, "-f", "mpegts", path)
		if out, err := exec.Command(ffmpeg, args...).CombinedOutput(); err != nil {
			t.Fatalf("generate synthetic %s media: %v %s", codec, err, out)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := -1
		if r.URL.Path == "/one" {
			index = 0
		}
		if r.URL.Path == "/two" {
			index = 1
		}
		if index < 0 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		http.ServeFile(w, r, inputs[index])
	}))
	defer server.Close()
	job := newTestRecording(time.Now().Add(10 * time.Second))
	s := newTestService(newFakeRecordingRepo(job), ffmpeg, filepath.Join(dir, "recordings"))
	sources := captureTestSources()
	sources[0].URL = server.URL + "/one"
	sources[1].URL = server.URL + "/two"
	p := &capturePlannerFixture{sources: sources}
	admitted := 0
	var firstHash [32]byte
	p.prepare = func(_ context.Context, source RecordingSource) (RecordingInput, error) {
		if admitted >= 2 {
			return RecordingInput{}, errors.New("synthetic source window closed")
		}
		if admitted == 1 {
			path, _, err := s.ResolvePartFile(job.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			firstHash = sha256.Sum256(data)
		}
		admitted++
		return RecordingInput{Source: source}, nil
	}
	s.channels = p
	s.startRecording(job)
	got, _ := s.Get(job.ID)
	if got.Status != models.RecordingStatusFailed || got.Capture == nil || len(got.Capture.Parts) != 2 || !got.Capture.Incomplete {
		t.Fatalf("incompatible input was represented as continuous: %+v", got)
	}
	for i, want := range []struct {
		video    string
		channels int
	}{{"h264", 1}, {"hevc", 2}} {
		path, part, err := s.ResolvePartFile(job.ID, i+1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && sha256.Sum256(data) != firstHash {
			t.Fatal("earlier recorded bytes changed after a different codec/layout arrived")
		}
		out, err := exec.Command(ffprobe, "-v", "error", "-show_entries", "stream=codec_type,codec_name,channels", "-of", "json", path).Output()
		if err != nil {
			t.Fatal("preserved part was not independently probeable", err)
		}
		var metadata struct {
			Streams []struct {
				Type     string `json:"codec_type"`
				Codec    string `json:"codec_name"`
				Channels int    `json:"channels"`
			} `json:"streams"`
		}
		if err = json.Unmarshal(out, &metadata); err != nil {
			t.Fatal(err)
		}
		video, audio := false, false
		for _, stream := range metadata.Streams {
			if stream.Type == "video" && stream.Codec == want.video {
				video = true
			}
			if stream.Type == "audio" && stream.Channels == want.channels {
				audio = true
			}
		}
		if !video || !audio || part.Bytes != int64(len(data)) {
			t.Fatalf("part%d lost its actual codec/layout: %s", i+1, out)
		}
		t.Logf("Preserved part%d: %s video, %d audio channels, %d measured bytes", i+1, want.video, want.channels, part.Bytes)
	}
}
