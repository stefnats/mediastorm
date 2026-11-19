package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"novastream/config"
)

func TestRecordingDerivedCurrentToolsArePureAndTrackActualConfigChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	raw := []byte(`{"transmux":{"ffmpegPath":"/server/synthetic-ffmpeg","ffprobePath":"/server/synthetic-ffprobe"},"cache":{"directory":"must-not-materialize"}}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	read := recordingDerivedToolsReader(config.NewManager(path))
	tools, err := read(context.Background())
	if err != nil || tools.FFmpegPath != "/server/synthetic-ffmpeg" || tools.FFprobePath != "/server/synthetic-ffprobe" {
		t.Fatal("actual configured tools were not read")
	}
	after, _ := os.Stat(path)
	got, _ := os.ReadFile(path)
	entries, _ := os.ReadDir(dir)
	if !bytes.Equal(raw, got) || !before.ModTime().Equal(after.ModTime()) || len(entries) != 1 {
		t.Fatal("pure tool read materialized settings or cache files")
	}
	if err = os.WriteFile(path, []byte(`{"transmux":{"ffmpegPath":"/server/new-ffmpeg","ffprobePath":"/server/new-ffprobe"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := read(context.Background())
	if err != nil || next == tools || next.FFmpegPath != "/server/new-ffmpeg" {
		t.Fatal("tool reader returned a cached old configuration")
	}
	if err = os.WriteFile(path, []byte(`{"transmux":{"ffmpegPath":"/server/new-ffmpeg","ffprobePath":"/server/new-ffprobe"},"cache":{"directory":"changed-root"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := read(context.Background())
	if err != nil || changed.FFmpegPath != next.FFmpegPath || changed.FFprobePath != next.FFprobePath || changed.ConfigurationKey == next.ConfigurationKey || len(changed.ConfigurationKey) != 64 {
		t.Fatal("recording root change retained old derived configuration")
	}
}

func TestRecordingDerivedCurrentToolsRejectUnavailableNonregularAndOversizedConfig(t *testing.T) {
	for _, mode := range []string{"missing", "directory", "symlink", "fifo", "malformed", "too large", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "settings.json")
			ctx := context.Background()
			switch mode {
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := filepath.Join(root, "target.json")
				if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(path, []byte(`{"transmux":`), 0600); err != nil {
					t.Fatal(err)
				}
			case "too large":
				if err := os.WriteFile(path, bytes.Repeat([]byte(" "), (4<<20)+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := recordingDerivedToolsReader(config.NewManager(path))(ctx); err == nil {
				t.Fatal("unavailable config invented current tools")
			}
		})
	}
	if _, err := recordingDerivedToolsReader(nil)(context.Background()); err == nil {
		t.Fatal("missing manager invented current tools")
	}
}

func TestRecordingDerivedCurrentToolsMatchPureExistingExecutableDefaults(t *testing.T) {
	for _, raw := range []string{`{}`, `{"transmux":{}}`, `{"transmux":{"ffmpegPath":"","ffprobePath":""}}`} {
		path := filepath.Join(t.TempDir(), "settings.json")
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		value, err := recordingDerivedToolsReader(config.NewManager(path))(context.Background())
		if err != nil || value.FFmpegPath != "ffmpeg" || value.FFprobePath != "ffprobe" {
			t.Fatal("pure executable defaults differ from existing settings behavior")
		}
		got, _ := os.ReadFile(path)
		if string(got) != raw {
			t.Fatal("defaults were saved during a current read")
		}
	}
}
