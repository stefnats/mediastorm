package recordings

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/models"
)

type exportFixture struct {
	service *Service
	repo    *fakeRecordingRepo
	job     models.Recording
	dir     string
	data    [][]byte
}

func newExportFixture(t *testing.T) *exportFixture {
	t.Helper()
	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-time.Minute)
	job := models.Recording{ID: "export-job", UserID: "export-owner", CreatedAt: start, Status: models.RecordingStatusFailed, Title: "PRIVATE_TITLE", SourceURL: "PRIVATE_PROVIDER_URL", OutputPath: "PRIVATE_FS_PATH", Error: "PRIVATE_ERROR", Capture: &models.RecordingCapture{Version: 1, Phase: "interrupted", Incomplete: true, SourceChanges: 1, RunID: "PRIVATE_RUN", StationID: "PRIVATE_STATION", Parts: []models.RecordingPart{{Index: 3, StartedAt: start, EndedAt: &end, EndReason: "source_ended", SourceID: "PRIVATE_SOURCE", ChannelID: "PRIVATE_CHANNEL", StreamPath: "PRIVATE_STREAM"}, {Index: 1, StartedAt: start, EndedAt: &end, EndReason: "capture_failed"}}, Gaps: []models.RecordingGap{{StartedAt: start, EndedAt: &end, Reason: "capacity"}}}}
	repo := newFakeRecordingRepo(job)
	root := t.TempDir()
	s := NewService(repo, "must-not-invoke-ffmpeg", root)
	s.starting = make(map[string]bool)
	dir := filepath.Join(root, job.UserID, job.ID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := [][]byte{bytes.Repeat([]byte("original-first-media\n"), 10000), []byte("original-third-media")}
	for i, index := range []int{1, 3} {
		if err := os.WriteFile(filepath.Join(dir, mustExportPartName(index)), data[i], 0600); err != nil {
			t.Fatal(err)
		}
	}
	return &exportFixture{s, repo, job, dir, data}
}
func (f *exportFixture) open(t *testing.T, ctx context.Context, check func() error) *PartsExport {
	t.Helper()
	e, err := f.service.OpenPartsExport(ctx, f.job.ID, check)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func exportAllowed() error { return nil }
func readExportZIP(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, file := range z.File {
		if file.Method != zip.Store {
			t.Fatal("export recompressed media")
		}
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		files[file.Name] = body
	}
	return files
}
func TestRecordingPartsExportPreservesAllBytesAndPrivateManifest(t *testing.T) {
	for _, status := range []models.RecordingStatus{models.RecordingStatusCompleted, models.RecordingStatusFailed, models.RecordingStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			f := newExportFixture(t)
			f.job.Status = status
			_ = f.repo.Update(context.Background(), &f.job)
			e := f.open(t, context.Background(), exportAllowed)
			var b bytes.Buffer
			if err := e.WriteZIP(&b); err != nil {
				t.Fatal(err)
			}
			got := readExportZIP(t, b.Bytes())
			if len(got) != 3 || !bytes.Equal(got["part-000001.ts"], f.data[0]) || !bytes.Equal(got["part-000003.ts"], f.data[1]) {
				t.Fatal("original indices/bytes changed")
			}
			var manifest ExportManifest
			if err := json.Unmarshal(got["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			if !manifest.Incomplete || manifest.Status != status || len(manifest.Gaps) != 1 || manifest.Gaps[0].Reason != "capacity" || len(manifest.Parts) != 2 || manifest.Parts[0].Index != 1 {
				t.Fatalf("dishonest manifest: %+v", manifest)
			}
			for i, p := range manifest.Parts {
				hash := sha256.Sum256(f.data[i])
				if p.Bytes != int64(len(f.data[i])) || p.SHA256 != hex.EncodeToString(hash[:]) {
					t.Fatal("fixed sizes/hash missing")
				}
			}
			if bytes.Contains(b.Bytes(), []byte("PRIVATE_")) {
				t.Fatal("private recording metadata leaked")
			}
			if err := e.WriteZIP(io.Discard); err == nil {
				t.Fatal("export reused")
			}
			if strings.ContainsAny(e.Filename(), "/\\\r\n") {
				t.Fatal("unsafe download filename")
			}
		})
	}
}
func TestRecordingPartsExportRejectsUnprovenOrActiveLedger(t *testing.T) {
	cases := map[string]func(*exportFixture){
		"invalid timestamp": func(f *exportFixture) {
			f.job.Capture.Parts[0].StartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			e := f.job.Capture.Parts[0].StartedAt.Add(time.Second)
			f.job.Capture.Parts[0].EndedAt = &e
		},
		"pending":         func(f *exportFixture) { f.job.Status = models.RecordingStatusPending },
		"running":         func(f *exportFixture) { f.job.Status = models.RecordingStatusRunning },
		"restored":        func(f *exportFixture) { f.job.Capture.Restored = true },
		"version":         func(f *exportFixture) { f.job.Capture.Version = 2 },
		"no parts":        func(f *exportFixture) { f.job.Capture.Parts = nil },
		"unclosed":        func(f *exportFixture) { f.job.Capture.Parts[0].EndedAt = nil },
		"unknown end":     func(f *exportFixture) { f.job.Capture.Parts[0].EndReason = "PRIVATE_ARBITRARY" },
		"duplicate":       func(f *exportFixture) { f.job.Capture.Parts[0].Index = 1 },
		"invalid index":   func(f *exportFixture) { f.job.Capture.Parts[0].Index = 257 },
		"unknown owner":   func(f *exportFixture) { f.job.UserID = "" },
		"traversal":       func(f *exportFixture) { f.job.UserID = "../foreign" },
		"missing":         func(f *exportFixture) { _ = os.Remove(filepath.Join(f.dir, mustExportPartName(3))) },
		"writer active":   func(f *exportFixture) { f.service.active[f.job.ID] = func() {} },
		"writer starting": func(f *exportFixture) { f.service.starting[f.job.ID] = true },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newExportFixture(t)
			f.job.Capture = models.CopyRecordingCapture(f.job.Capture)
			change(f)
			_ = f.repo.Update(context.Background(), &f.job)
			e, err := f.service.OpenPartsExport(context.Background(), f.job.ID, exportAllowed)
			if e != nil {
				e.Close()
			}
			if err == nil {
				t.Fatal("unproven export allowed")
			}
		})
	}
}
func TestRecordingPartsExportRejectsSymlinksAndSpecialInputs(t *testing.T) {
	for _, which := range []string{"root", "profile", "job", "part", "fifo"} {
		t.Run(which, func(t *testing.T) {
			f := newExportFixture(t)
			path := f.service.outputDir
			switch which {
			case "profile":
				path = filepath.Dir(f.dir)
			case "job":
				path = f.dir
			case "part", "fifo":
				path = filepath.Join(f.dir, mustExportPartName(1))
			}
			if which == "fifo" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				moved := path + "-original"
				if err := os.Rename(path, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, path); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				e, err := f.service.OpenPartsExport(context.Background(), f.job.ID, exportAllowed)
				if e != nil {
					e.Close()
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("special input exported")
				}
			case <-time.After(time.Second):
				t.Fatal("special input blocked export")
			}
		})
	}
}
func TestRecordingPartsExportOpenHandlesNeverReopenReplacedPaths(t *testing.T) {
	f := newExportFixture(t)
	e := f.open(t, context.Background(), exportAllowed)
	for _, index := range []int{1, 3} {
		p := filepath.Join(f.dir, mustExportPartName(index))
		if err := os.Rename(p, p+"-owned"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("FOREIGN_REPLACEMENT"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var b bytes.Buffer
	if err := e.WriteZIP(&b); err != nil {
		t.Fatal(err)
	}
	got := readExportZIP(t, b.Bytes())
	if !bytes.Equal(got[mustExportPartName(1)], f.data[0]) || bytes.Contains(b.Bytes(), []byte("FOREIGN_REPLACEMENT")) {
		t.Fatal("export reopened replacement")
	}
}

type exportActionWriter struct {
	bytes.Buffer
	once   sync.Once
	action func()
}

func (w *exportActionWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.once.Do(w.action)
	return n, err
}
func TestRecordingPartsExportStopsOnRevocationLedgerAndFileChanges(t *testing.T) {
	for _, change := range []string{"authority", "owner", "generation", "grow", "truncate", "writer-error"} {
		t.Run(change, func(t *testing.T) {
			f := newExportFixture(t)
			var revoked atomic.Bool
			check := func() error {
				if revoked.Load() {
					return ErrRecordingExportAccess
				}
				return nil
			}
			e := f.open(t, context.Background(), check)
			w := &exportActionWriter{action: func() {
				switch change {
				case "authority":
					revoked.Store(true)
				case "owner", "generation":
					r := f.job
					r.Capture = models.CopyRecordingCapture(r.Capture)
					if change == "owner" {
						r.UserID = "foreign"
					} else {
						r.Capture.RunID = "replacement"
					}
					_ = f.repo.Update(context.Background(), &r)
				case "grow":
					p := filepath.Join(f.dir, mustExportPartName(1))
					fd, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
					if err != nil {
						t.Error(err)
						return
					}
					_, _ = fd.Write([]byte("extra"))
					fd.Close()
				case "truncate":
					_ = os.Truncate(filepath.Join(f.dir, mustExportPartName(1)), 1)
				}
			}}
			var out io.Writer = w
			if change == "writer-error" {
				out = exportFailWriter{}
			}
			if err := e.WriteZIP(out); err == nil {
				t.Fatal("failed transfer completed")
			}
			if _, err := zip.NewReader(bytes.NewReader(w.Bytes()), int64(w.Len())); err == nil {
				t.Fatal("failed transfer finalized ZIP")
			}
		})
	}
}

type exportFailWriter struct{}

func (exportFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("synthetic disconnected client")
}

type exportBlockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *exportBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func TestRecordingPartsExportCancellationKeepsSlotUntilTransferJoins(t *testing.T) {
	f := newExportFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one := f.open(t, ctx, exportAllowed)
	two := f.open(t, context.Background(), exportAllowed)
	w := &exportBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- one.WriteZIP(w) }()
	<-w.entered
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		_, err := one.files[0].file.Stat()
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel did not close owned input")
		}
		time.Sleep(time.Millisecond)
	}
	if e, err := f.service.OpenPartsExport(context.Background(), f.job.ID, exportAllowed); !errors.Is(err, ErrRecordingExportBusy) {
		if e != nil {
			e.Close()
		}
		t.Fatal("slot released while transfer blocked", err)
	}
	close(w.release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled transfer succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("transfer did not join")
	}
	one.Close()
	two.Close()
	next := f.open(t, context.Background(), exportAllowed)
	next.Close()
}
func TestRecordingPartsExportZeroLengthAndUnknownGapAreHonest(t *testing.T) {
	f := newExportFixture(t)
	if err := os.Truncate(filepath.Join(f.dir, mustExportPartName(3)), 0); err != nil {
		t.Fatal(err)
	}
	f.job.Capture.Gaps[0].Reason = "PRIVATE_GAP"
	_ = f.repo.Update(context.Background(), &f.job)
	e := f.open(t, context.Background(), exportAllowed)
	var b bytes.Buffer
	if err := e.WriteZIP(&b); err != nil {
		t.Fatal(err)
	}
	files := readExportZIP(t, b.Bytes())
	if len(files[mustExportPartName(3)]) != 0 || !bytes.Contains(files["manifest.json"], []byte(`"reason": "unknown"`)) || bytes.Contains(b.Bytes(), []byte("PRIVATE_")) {
		t.Fatal("zero/unknown data fabricated")
	}
}
func TestRecordingPartsExportDeniedBeforeDirectoryAccess(t *testing.T) {
	f := newExportFixture(t)
	f.service.outputDir = filepath.Join(t.TempDir(), "never-created")
	_, err := f.service.OpenPartsExport(context.Background(), f.job.ID, func() error { return ErrRecordingExportAccess })
	if !errors.Is(err, ErrRecordingExportAccess) {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.service.outputDir); !os.IsNotExist(err) {
		t.Fatal("denied export created output state")
	}
}
