package recordings

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Generation/probing are test-fixture controls only. The actual exporter is
// configured with a nonexistent media executable and performs no such work.
func TestRecordingPartsExportRealMixedCodecFilesRemainIndependent(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal("synthetic media fixture requires local ffmpeg")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal("synthetic media fixture requires local ffprobe")
	}
	f := newExportFixture(t)
	for i, codec := range []string{"libx264", "libx265"} {
		index := []int{1, 3}[i]
		path := filepath.Join(f.dir, mustExportPartName(index))
		args := []string{"-y", "-nostdin", "-hide_banner", "-loglevel", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.6", "-c:v", codec, "-threads", "1"}
		if i == 1 {
			args = append(args, "-x265-params", "pools=none:frame-threads=1:log-level=error")
		} else {
			args = append(args, "-preset", "ultrafast")
		}
		args = append(args, "-c:a", "aac", "-ac", []string{"1", "2"}[i], "-f", "mpegts", path)
		if out, err := exec.Command(ffmpeg, args...).CombinedOutput(); err != nil {
			t.Fatalf("synthetic generation: %v %s", err, out)
		}
		f.data[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	e := f.open(t, context.Background(), exportAllowed)
	var b bytes.Buffer
	if err := e.WriteZIP(&b); err != nil {
		t.Fatal(err)
	}
	files := readExportZIP(t, b.Bytes())
	for i, index := range []int{1, 3} {
		name := mustExportPartName(index)
		if !bytes.Equal(files[name], f.data[i]) {
			t.Fatal("archive changed actual codec bytes")
		}
		// Probe the extracted entry, not a metadata claim or source fixture path.
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, files[name], 0600); err != nil {
			t.Fatal(err)
		}
		raw, err := exec.Command(ffprobe, "-v", "error", "-show_entries", "stream=codec_name,codec_type,channels", "-of", "json", path).Output()
		if err != nil {
			t.Fatal(err)
		}
		var probe struct {
			Streams []struct {
				Codec    string `json:"codec_name"`
				Type     string `json:"codec_type"`
				Channels int    `json:"channels"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			t.Fatal(err)
		}
		video, audio := false, false
		for _, stream := range probe.Streams {
			video = video || stream.Type == "video" && stream.Codec == []string{"h264", "hevc"}[i]
			audio = audio || stream.Type == "audio" && stream.Channels == i+1
		}
		if !video || !audio {
			t.Fatalf("independent extracted media lost codec/layout: %s", raw)
		}
		t.Logf("ZIP Store original part %d: %d bytes, %s video, %d audio channels", index, len(files[name]), []string{"h264", "hevc"}[i], i+1)
	}
}
