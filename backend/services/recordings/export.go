package recordings

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"novastream/models"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

var (
	ErrRecordingExportUnavailable = errors.New("recording parts export is unavailable")
	ErrRecordingExportBusy        = errors.New("recording parts exports are busy")
	ErrRecordingExportAccess      = errors.New("recording export authorization changed")
	recordingExportSlots          = make(chan struct{}, 2)
)

type ExportPart struct {
	Index     int       `json:"index"`
	Entry     string    `json:"entry"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
	EndReason string    `json:"endReason"`
}
type ExportGap struct {
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	Reason    string     `json:"reason"`
}
type ExportManifest struct {
	Version       int                    `json:"version"`
	RecordingID   string                 `json:"recordingId"`
	Status        models.RecordingStatus `json:"status"`
	Incomplete    bool                   `json:"incomplete"`
	SourceChanges int                    `json:"sourceChanges"`
	Parts         []ExportPart           `json:"parts"`
	Gaps          []ExportGap            `json:"gaps"`
}
type exportFile struct {
	file *os.File
	info os.FileInfo
}

// PartsExport owns exact already opened files and fixed measured lengths. No
// path from the ledger/client is used for input, and no provider is contacted.
// Close retains the process slot until transfer cleanup has actually finished.
type PartsExport struct {
	service     *Service
	ctx         context.Context
	authorize   func() error
	original    models.Recording
	fingerprint [32]byte
	files       []exportFile
	manifest    ExportManifest
	closeFiles  sync.Once
	closeOnce   sync.Once
	stop        func() bool
	transferMu  sync.Mutex
	used        bool
	closed      bool
}

func terminalExportStatus(status models.RecordingStatus) bool {
	return status == models.RecordingStatusCompleted || status == models.RecordingStatusFailed || status == models.RecordingStatusCancelled
}
func closedExportReason(reason string) bool {
	switch reason {
	case "schedule", "source_ended", "capture_failed", "shutdown", "cancelled":
		return true
	}
	return false
}
func exportFingerprint(r models.Recording) ([32]byte, error) {
	raw, err := json.Marshal([]any{r.ID, r.UserID, r.CreatedAt, r.Status, r.Capture})
	if err != nil {
		return [32]byte{}, ErrRecordingExportUnavailable
	}
	return sha256.Sum256(raw), nil
}
func (s *Service) exportRecording(ctx context.Context, id string) (models.Recording, error) {
	if ctx.Err() != nil {
		return models.Recording{}, ctx.Err()
	}
	if s == nil || s.repo == nil {
		return models.Recording{}, ErrRecordingExportUnavailable
	}
	r, err := s.repo.Get(ctx, id)
	if err != nil || r == nil || r.ID != id || !safeRecordingComponent(r.ID) || !safeRecordingComponent(r.UserID) || !terminalExportStatus(r.Status) || !validCaptureParts(r.Capture) || len(r.Capture.Parts) == 0 || r.Capture.SourceChanges < 0 || r.Capture.SourceChanges > models.MaxRecordingParts {
		return models.Recording{}, ErrRecordingExportUnavailable
	}
	s.mu.Lock()
	active := s.active[id] != nil || s.starting[id]
	s.mu.Unlock()
	if active {
		return models.Recording{}, ErrRecordingExportUnavailable
	}
	copy := *r
	copy.Capture = models.CopyRecordingCapture(r.Capture)
	for _, p := range copy.Capture.Parts {
		if p.StartedAt.IsZero() || p.EndedAt == nil || p.EndedAt.Before(p.StartedAt) || !closedExportReason(p.EndReason) {
			return models.Recording{}, ErrRecordingExportUnavailable
		}
	}
	return copy, nil
}

// OpenPartsExport performs no stat/open until the supplied original HTTP
// authority has been checked. The optional interface does not widen old fakes.
func (s *Service) OpenPartsExport(ctx context.Context, id string, authorize func() error) (result *PartsExport, err error) {
	if authorize == nil {
		return nil, ErrRecordingExportAccess
	}
	if err = authorize(); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	select {
	case recordingExportSlots <- struct{}{}:
	default:
		return nil, ErrRecordingExportBusy
	}
	e := &PartsExport{service: s, ctx: ctx, authorize: authorize}
	defer func() {
		if result == nil {
			_ = e.Close()
		}
	}()
	r, err := s.exportRecording(ctx, id)
	if err != nil {
		return nil, err
	}
	e.original = r
	e.fingerprint, err = exportFingerprint(r)
	if err != nil {
		return nil, err
	}
	sort.Slice(r.Capture.Parts, func(i, j int) bool { return r.Capture.Parts[i].Index < r.Capture.Parts[j].Index })
	e.manifest = ExportManifest{Version: 1, RecordingID: r.ID, Status: r.Status, Incomplete: r.Capture.Incomplete, SourceChanges: r.Capture.SourceChanges, Parts: []ExportPart{}, Gaps: []ExportGap{}}
	// OpenRoot itself follows its initial name. SameFile anchors that first root
	// to the checked real directory, then each child root to its checked parent.
	rootPath, err := filepath.Abs(s.outputDir)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	if err = e.check(); err != nil {
		return nil, err
	}
	before, err := os.Lstat(rootPath)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrRecordingExportUnavailable
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	defer root.Close()
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(before, actual) {
		return nil, ErrRecordingExportUnavailable
	}
	if err = e.check(); err != nil {
		return nil, err
	}
	profile, err := openExportRoot(root, r.UserID)
	if err != nil {
		return nil, err
	}
	defer profile.Close()
	if err = e.check(); err != nil {
		return nil, err
	}
	job, err := openExportRoot(profile, r.ID)
	if err != nil {
		return nil, err
	}
	defer job.Close()
	var total int64
	for _, part := range r.Capture.Parts {
		if err = e.check(); err != nil {
			return nil, err
		}
		name := filepath.Base(mustExportPartName(part.Index))
		info, err := job.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			return nil, ErrRecordingExportUnavailable
		}
		file, err := job.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return nil, ErrRecordingExportUnavailable
		}
		e.files = append(e.files, exportFile{file: file, info: info})
		opened, err := file.Stat()
		if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() < 0 || opened.Size() > math.MaxInt64-total {
			return nil, ErrRecordingExportUnavailable
		}
		after, err := job.Lstat(name)
		if err != nil || !after.Mode().IsRegular() || !os.SameFile(opened, after) {
			return nil, ErrRecordingExportUnavailable
		}
		total += opened.Size()
		e.files[len(e.files)-1].info = opened
		e.manifest.Parts = append(e.manifest.Parts, ExportPart{Index: part.Index, Entry: name, Bytes: opened.Size(), StartedAt: part.StartedAt.UTC(), EndedAt: part.EndedAt.UTC(), EndReason: part.EndReason})
	}
	for _, gap := range r.Capture.Gaps {
		if gap.StartedAt.IsZero() || gap.EndedAt != nil && gap.EndedAt.Before(gap.StartedAt) {
			return nil, ErrRecordingExportUnavailable
		}
		reason := "unknown"
		if gap.Reason == "capacity" || gap.Reason == "source_retry" {
			reason = gap.Reason
		}
		var ended *time.Time
		if gap.EndedAt != nil {
			value := gap.EndedAt.UTC()
			ended = &value
		}
		e.manifest.Gaps = append(e.manifest.Gaps, ExportGap{StartedAt: gap.StartedAt.UTC(), EndedAt: ended, Reason: reason})
	}
	if err = e.check(); err != nil {
		return nil, err
	}
	// The immutable file slice is complete before cancellation can close it.
	e.stop = context.AfterFunc(ctx, e.closeInputs)
	return e, nil
}

func mustExportPartName(index int) string { return fmt.Sprintf("part-%06d.ts", index) }
func openExportRoot(parent *os.Root, name string) (*os.Root, error) {
	before, err := parent.Lstat(name)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrRecordingExportUnavailable
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	actual, err := child.Stat(".")
	after, e := parent.Lstat(name)
	if err != nil || e != nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, actual) || !os.SameFile(actual, after) {
		_ = child.Close()
		return nil, ErrRecordingExportUnavailable
	}
	return child, nil
}
func (e *PartsExport) Filename() string { return "recording-" + e.original.ID + "-parts.zip" }
func (e *PartsExport) check() error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if err := e.authorize(); err != nil {
		return err
	}
	current, err := e.service.exportRecording(e.ctx, e.original.ID)
	if err != nil {
		return ErrRecordingExportUnavailable
	}
	fingerprint, err := exportFingerprint(current)
	if err != nil || fingerprint != e.fingerprint {
		return ErrRecordingExportUnavailable
	}
	return e.ctx.Err()
}
func (e *PartsExport) closeInputs() {
	e.closeFiles.Do(func() {
		for _, input := range e.files {
			_ = input.file.Close()
		}
	})
}
func (e *PartsExport) Close() error {
	e.closeOnce.Do(func() {
		if e.stop != nil {
			e.stop()
		}
		e.closeInputs()
		e.transferMu.Lock()
		defer e.transferMu.Unlock()
		e.closed = true
		<-recordingExportSlots
	})
	return nil
}

type exportCheckedWriter struct {
	owner *PartsExport
	out   io.Writer
}

func (w exportCheckedWriter) Write(p []byte) (int, error) {
	if err := w.owner.check(); err != nil {
		return 0, err
	}
	return w.out.Write(p)
}

// WriteZIP never closes/finalizes a ZIP after any failure. A failed partial
// response has no successful central directory and is not a completed archive.
func (e *PartsExport) WriteZIP(out io.Writer) error {
	e.transferMu.Lock()
	defer e.transferMu.Unlock()
	if e.closed || e.used {
		return ErrRecordingExportUnavailable
	}
	e.used = true
	if err := e.check(); err != nil {
		return err
	}
	zw := zip.NewWriter(exportCheckedWriter{owner: e, out: out})
	// Two export slots bound these fixed read buffers to two MiB total.
	buffer := make([]byte, 1<<20)
	for i, input := range e.files {
		if err := e.check(); err != nil {
			return err
		}
		part := &e.manifest.Parts[i]
		header := &zip.FileHeader{Name: part.Entry, Method: zip.Store}
		header.SetModTime(part.EndedAt)
		entry, err := zw.CreateHeader(header)
		if err != nil {
			return err
		}
		digest := sha256.New()
		remaining := part.Bytes
		for remaining > 0 {
			if err = e.check(); err != nil {
				return err
			}
			wanted := min(int64(len(buffer)), remaining)
			n, readErr := input.file.Read(buffer[:wanted])
			if err = e.check(); err != nil {
				clear(buffer)
				return err
			}
			if n > 0 {
				if int64(n) > remaining {
					return ErrRecordingExportUnavailable
				}
				_, _ = digest.Write(buffer[:n])
				written, err := entry.Write(buffer[:n])
				if err != nil {
					return err
				}
				if written != n {
					return io.ErrShortWrite
				}
				remaining -= int64(n)
			}
			if readErr != nil && !(readErr == io.EOF && remaining == 0) {
				return ErrRecordingExportUnavailable
			}
			if n == 0 && readErr == nil {
				return io.ErrNoProgress
			}
		}
		info, err := input.file.Stat()
		if err != nil || info.Size() != part.Bytes || !info.ModTime().Equal(input.info.ModTime()) || !os.SameFile(input.info, info) {
			return ErrRecordingExportUnavailable
		}
		part.SHA256 = hex.EncodeToString(digest.Sum(nil))
	}
	if err := e.check(); err != nil {
		return err
	}
	manifest, err := json.MarshalIndent(e.manifest, "", "  ")
	if err != nil {
		return err
	}
	if len(manifest) > 1<<20 {
		return ErrRecordingExportUnavailable
	}
	entry, err := zw.CreateHeader(&zip.FileHeader{Name: "manifest.json", Method: zip.Store})
	if err != nil {
		return err
	}
	if _, err = entry.Write(append(manifest, '\n')); err != nil {
		return err
	}
	if err = e.check(); err != nil {
		return err
	}
	return zw.Close()
}
