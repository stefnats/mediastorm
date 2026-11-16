package recordings

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/streaming"
)

func TestRecordingPartSelectorsNeverFallBackToLegacyFile(t *testing.T) {
	for _, suffix := range []string{"parts", "parts/", "parts/0", "parts/01", "parts/-1", "parts/+1", "parts/257", "parts/999999999999999999999", "parts/1/", "parts/../1", "parts/%31", "parts/1?x=1", "Parts/1", "/parts/1", "parts%2F2", "p%61rts/2", "parts%252f2", "p%2561rts/2"} {
		t.Run(suffix, func(t *testing.T) {
			_, _, explicit, valid := parsePartStreamPath("recording:rec-1/" + suffix)
			if !explicit || valid {
				t.Fatal("invalid reserved selector could select a legacy file")
			}
		})
	}
	for _, path := range []string{"recording:rec-1", "recording:rec-1/My old file.ts", "recording:rec-1/My%20old%20file.ts", "recording:rec-1/p%61rts.ts", "recording:rec-1/100% hits.ts"} {
		id, _, explicit, valid := parsePartStreamPath(path)
		if !valid || explicit || id != "rec-1" {
			t.Fatal("legacy recording ID lookup changed")
		}
	}
	if id, index, explicit, valid := parsePartStreamPath("recording:rec-1/parts/256"); !valid || !explicit || id != "rec-1" || index != 256 {
		t.Fatal("bounded canonical selector refused")
	}
}

func providerPartsFixture(t *testing.T) (*StreamProvider, *fakeRecordingRepo, models.Recording, map[int]string) {
	t.Helper()
	root := t.TempDir()
	now := time.Now().UTC()
	closed := now.Add(-time.Second)
	recording := models.Recording{ID: "rec-1", UserID: "viewer", Status: models.RecordingStatusRunning, OutputPath: filepath.Join(root, "untrusted.ts"),
		Capture: &models.RecordingCapture{Version: 1, Phase: "capturing", Incomplete: true, Parts: []models.RecordingPart{
			{Index: 3, StartedAt: now},
			{Index: 1, StartedAt: now.Add(-3 * time.Second), EndedAt: &closed, EndReason: "capture_failed"},
			{Index: 2, StartedAt: now.Add(-2 * time.Second), EndedAt: &closed, EndReason: "source_ended"},
		}}}
	repo := newFakeRecordingRepo(recording)
	service := NewService(repo, "ffmpeg", root)
	paths := make(map[int]string)
	for index, content := range map[int]string{1: "", 2: "first captured section", 3: "second captured section"} {
		path, err := service.partPath(recording, index)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		paths[index] = path
	}
	return NewStreamProvider(service), repo, recording, paths
}

func TestRecordingPartsProviderResolvesOwnFilesAndClosedFirstPart(t *testing.T) {
	provider, _, _, paths := providerPartsFixture(t)
	for _, tc := range []struct {
		path  string
		index int
		grows bool
	}{{"recording:rec-1/old-name.ts", 2, false}, {"recording:rec-1/parts/2", 2, false}, {"recording:rec-1/parts/3", 3, true}} {
		file, err := provider.GetDirectURL(t.Context(), tc.path)
		if err != nil || file != paths[tc.index] {
			t.Fatalf("selected file %q error %v", file, err)
		}
		growing, err := provider.IsGrowing(t.Context(), tc.path)
		if err != nil || growing != tc.grows {
			t.Fatalf("growing=%v error %v; want %v", growing, err, tc.grows)
		}
	}
	for _, path := range []string{"recording:rec-1/parts/1", "recording:rec-1/parts/4", "recording:rec-1/parts/02"} {
		if _, err := provider.GetDirectURL(t.Context(), path); !errors.Is(err, streaming.ErrNotFound) {
			t.Fatalf("unavailable part did not fail closed: %v", err)
		}
	}
}

func TestRecordingPartProviderRangeAndHead(t *testing.T) {
	provider, _, _, _ := providerPartsFixture(t)
	response, err := provider.Stream(t.Context(), streaming.Request{Path: "recording:rec-1/parts/2", Method: http.MethodGet, RangeHeader: "bytes=2-6"})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Close()
	bytes, err := io.ReadAll(response.Body)
	if err != nil || string(bytes) != "rst c" || response.Status != http.StatusPartialContent || response.ContentLength != 5 {
		t.Fatalf("range bytes=%q status=%d error=%v", bytes, response.Status, err)
	}
	head, err := provider.Stream(t.Context(), streaming.Request{Path: "recording:rec-1/parts/3", Method: http.MethodHead})
	if err != nil {
		t.Fatal(err)
	}
	defer head.Close()
	if head.Body != nil || head.ContentLength != int64(len("second captured section")) || head.Headers.Get("Content-Length") != strconv.Itoa(len("second captured section")) {
		t.Fatal("HEAD did not describe the selected part")
	}
}

func TestRecordingPartProviderGetKeepsDeclaredSnapshotLength(t *testing.T) {
	provider, _, _, paths := providerPartsFixture(t)
	response, err := provider.Stream(t.Context(), streaming.Request{Path: "recording:rec-1/parts/3", Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Close()
	file, err := os.OpenFile(paths[3], os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString(" appended later")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("append active part: %v %v", writeErr, closeErr)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil || string(data) != "second captured section" || int64(len(data)) != response.ContentLength ||
		response.Headers.Get("Content-Length") != strconv.Itoa(len(data)) {
		t.Fatal("active part exceeded its declared snapshot length")
	}
}

func TestRecordingPartsProviderQuarantinesInvalidOrUnownedFiles(t *testing.T) {
	for _, kind := range []string{"restored", "unknown-version", "duplicate-index", "symlink", "traversal", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			provider, repo, recording, paths := providerPartsFixture(t)
			switch kind {
			case "restored":
				recording.Capture.Restored = true
			case "unknown-version":
				recording.Capture.Version = 2
			case "duplicate-index":
				recording.Capture.Parts = append(recording.Capture.Parts, recording.Capture.Parts[2])
			case "symlink":
				if err := os.Remove(paths[2]); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(paths[3], paths[2]); err != nil {
					t.Fatal(err)
				}
			case "traversal":
				recording.UserID = "../viewer"
			case "oversized":
				recording.Capture.Parts = make([]models.RecordingPart, models.MaxRecordingParts+1)
			}
			if err := repo.Update(context.Background(), &recording); err != nil {
				t.Fatal(err)
			}
			if _, err := provider.GetDirectURL(t.Context(), "recording:rec-1/parts/2"); !errors.Is(err, streaming.ErrNotFound) {
				t.Fatalf("unsafe part was not refused: %v", err)
			}
			if kind != "symlink" {
				if _, err := provider.GetDirectURL(t.Context(), "recording:rec-1/old.ts"); !errors.Is(err, streaming.ErrNotFound) {
					t.Fatalf("invalid capture downgraded to legacy: %v", err)
				}
			}
		})
	}
}
