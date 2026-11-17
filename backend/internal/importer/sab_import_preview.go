package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"novastream/config"
	"novastream/services/usenetengine"
)

var ErrSABImportPreviewUnavailable = errors.New("local download inventory unavailable")

const (
	sabPreviewMaxEntries       = 128
	sabPreviewMaxBytes   int64 = 1 << 40
)

// Server callbacks are supplied only by the original authenticated handler.
// No HTTP body, receipt JSON, output path or boolean can supply these readers.
type SABImportPreviewSettings func(context.Context) (config.SABnzbdSettings, error)
type SABImportPreviewFile struct {
	Ordinal       int    `json:"ordinal"`
	Kind          string `json:"kind"`
	Extension     string `json:"extension"`
	ObservedBytes int64  `json:"observedBytes"`
}
type SABImportPreview struct {
	QueueID       int64                  `json:"queueId"`
	State         string                 `json:"state"`
	ObservedAt    time.Time              `json:"observedAt"`
	EntryCount    int                    `json:"entryCount"`
	ObservedBytes int64                  `json:"observedBytes"`
	Files         []SABImportPreviewFile `json:"files"`
}
type sabPreviewMapping struct{ remote, local, source string }

func sabPreviewConfig(ctx context.Context, reader SABImportPreviewSettings) (sabPreviewMapping, error) {
	if reader == nil || ctx.Err() != nil {
		return sabPreviewMapping{}, ErrSABImportPreviewUnavailable
	}
	cfg, err := reader(ctx)
	if err != nil || ctx.Err() != nil || cfg.Enabled == nil || !*cfg.Enabled || cfg.FallbackHost == "" || cfg.FallbackAPIKey == "" {
		return sabPreviewMapping{}, ErrSABImportPreviewUnavailable
	}
	remote, local := cfg.ImportPreviewRemoteRoot, cfg.ImportPreviewLocalRoot
	// POSIX only, explicit absolute roots. A whole filesystem root is not a
	// completed-job namespace. No same-host or remote-path guessing is performed.
	valid := func(v string) bool {
		return strings.HasPrefix(v, "/") && v != "/" && path.Clean(v) == v && !strings.ContainsAny(v, "\\\x00") && !strings.Contains(v, "://")
	}
	if !valid(remote) || !valid(local) || !filepath.IsAbs(local) {
		return sabPreviewMapping{}, ErrSABImportPreviewUnavailable
	}
	raw, _ := json.Marshal([]string{strings.TrimSpace(cfg.FallbackHost), cfg.FallbackAPIKey})
	sum := sha256.Sum256(raw)
	return sabPreviewMapping{remote, local, hex.EncodeToString(sum[:])}, nil
}
func sabPreviewExtension(name string) (string, string) {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv":
		return "media_candidate", "mkv"
	case ".mp4":
		return "media_candidate", "mp4"
	case ".ts":
		return "media_candidate", "ts"
	case ".m2ts":
		return "media_candidate", "m2ts"
	case ".avi":
		return "media_candidate", "avi"
	case ".nfo":
		return "other_regular", "nfo"
	case ".srt":
		return "other_regular", "srt"
	default:
		return "other_regular", "other"
	}
}
func sabSameInfo(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// PreviewSABImport inventories one completed receipt's explicitly mapped local
// directory. It reads metadata only, not file payload. It does not reserve space,
// import/copy/delete data, inspect codecs, identify a title or mint a file grant.
func (s *Service) PreviewSABImport(ctx context.Context, id int64, settings SABImportPreviewSettings, authorize func(context.Context) error) (SABImportPreview, error) {
	return s.withSABImportInventory(ctx, id, settings, authorize, 5*time.Second, nil)
}

type sabInventoryFile struct {
	name string
	file *os.File
	info os.FileInfo
}
type sabHeldInventory struct {
	result     SABImportPreview
	receipt    *sabFallbackReceipt
	sourceRoot *os.File
	files      []sabInventoryFile
	check      func() error
	checkFile  func(int) error
}

// The consumer executes synchronously while all original descriptors and the
// shared operation slot remain owned. It never receives a public path grant.
func (s *Service) withSABImportInventory(ctx context.Context, id int64, settings SABImportPreviewSettings, authorize func(context.Context) error, budget time.Duration, consume func(context.Context, *sabHeldInventory) (SABImportPreview, error)) (SABImportPreview, error) {
	fail := func() (SABImportPreview, error) { return SABImportPreview{}, ErrSABImportPreviewUnavailable }
	if ctx == nil || ctx.Err() != nil || authorize == nil || budget <= 0 || budget > 5*time.Minute {
		return fail()
	}
	ctx, stop := context.WithTimeout(ctx, budget)
	defer stop()
	if authorize(ctx) != nil {
		return fail()
	}
	mapping, err := sabPreviewConfig(ctx, settings)
	if err != nil {
		return fail()
	}
	_, _, receipt, err := s.fallbackItem(ctx, id)
	if err != nil {
		return SABImportPreview{}, err
	}
	if receipt == nil || receipt.JobID == "" || receipt.Status.State != "download_completed" || receipt.Source != mapping.source {
		return fail()
	}
	expectedStatus := receipt.Status
	// The same exact receipt and settings must remain current at every next stage.
	// Metadata refresh claims are mutable housekeeping, not preview authority.
	checkBase := func(ctx context.Context) error {
		if ctx.Err() != nil || authorize(ctx) != nil {
			return ErrSABImportPreviewUnavailable
		}
		current, e := sabPreviewConfig(ctx, settings)
		if e != nil || current != mapping {
			return ErrSABImportPreviewUnavailable
		}
		_, _, r, e := s.fallbackItem(ctx, id)
		if e != nil || r == nil || r.Attempt != receipt.Attempt || r.Source != receipt.Source || r.JobID != receipt.JobID || !reflect.DeepEqual(r.Status, expectedStatus) {
			return ErrSABImportPreviewUnavailable
		}
		return ctx.Err()
	}
	observed, err := s.observeSABFallback(ctx, id, true, checkBase)
	if err != nil {
		return SABImportPreview{}, err
	}
	if observed.raw == nil || observed.raw.Status != usenetengine.StatusCompleted || observed.status.State != "download_completed" || observed.attempt != receipt.Attempt || observed.source != receipt.Source || observed.jobID != receipt.JobID {
		return fail()
	}
	expectedStatus = observed.status
	check := func() error {
		if checkBase(ctx) != nil {
			return ErrSABImportPreviewUnavailable
		}
		_, _, r, e := s.fallbackItem(ctx, id)
		if e != nil || r == nil || !reflect.DeepEqual(r.Status, observed.status) {
			return ErrSABImportPreviewUnavailable
		}
		return ctx.Err()
	}
	if check() != nil {
		return fail()
	}
	rawPath := observed.raw.OutputPath
	if !strings.HasPrefix(rawPath, mapping.remote+"/") || path.Clean(rawPath) != rawPath || strings.ContainsAny(rawPath, "\\\x00") || strings.Contains(rawPath, "://") {
		return fail()
	}
	relative := strings.TrimPrefix(rawPath, mapping.remote+"/")
	if relative == "" || strings.HasPrefix(relative, "/") {
		return fail()
	}
	for _, part := range strings.Split(relative, "/") {
		if part == "" || part == "." || part == ".." {
			return fail()
		}
	}
	// A separate shared slot covers the entire metadata inventory after the
	// existing status claim/HTTP slot has actually returned. Never release a slot
	// while an OS operation or an owned descriptor is still active.
	release := s.fallbackSlot()
	if release == nil {
		return SABImportPreview{}, ErrSABFallbackBusy
	}
	defer release()
	if check() != nil {
		return fail()
	}
	initial, err := os.Lstat(mapping.local)
	if err != nil || !initial.IsDir() || initial.Mode()&os.ModeSymlink != 0 {
		return fail()
	}
	if check() != nil {
		return fail()
	}
	root, err := os.OpenRoot(mapping.local)
	if err != nil {
		return fail()
	}
	defer root.Close()
	if check() != nil {
		return fail()
	}
	rootFD, err := root.Open(".")
	if err != nil {
		return fail()
	}
	defer rootFD.Close()
	if check() != nil {
		return fail()
	}
	rootInfo, err := rootFD.Stat()
	if err != nil || !os.SameFile(initial, rootInfo) || !rootInfo.IsDir() || check() != nil {
		return fail()
	}
	openDirectory := func() (*os.File, error) {
		current := rootFD
		owned := false
		for _, component := range strings.Split(relative, "/") {
			if check() != nil {
				if owned {
					current.Close()
				}
				return nil, ErrSABImportPreviewUnavailable
			}
			fd, e := syscall.Openat(int(current.Fd()), component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			if owned {
				current.Close()
			}
			if e != nil {
				return nil, ErrSABImportPreviewUnavailable
			}
			current = os.NewFile(uintptr(fd), "owned completed directory")
			owned = true
			if check() != nil {
				current.Close()
				return nil, ErrSABImportPreviewUnavailable
			}
			info, e := current.Stat()
			if e != nil || !info.IsDir() || check() != nil {
				current.Close()
				return nil, ErrSABImportPreviewUnavailable
			}
		}
		return current, nil
	}
	directory, err := openDirectory()
	if err != nil {
		return fail()
	}
	defer directory.Close()
	if check() != nil {
		return fail()
	}
	before, err := directory.Stat()
	if err != nil || check() != nil {
		return fail()
	}
	entries, err := directory.ReadDir(sabPreviewMaxEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return fail()
	}
	if len(entries) == 0 || len(entries) > sabPreviewMaxEntries || check() != nil {
		return fail()
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	heldFiles := make([]sabInventoryFile, 0, len(entries))
	defer func() {
		for _, f := range heldFiles {
			f.file.Close()
		}
	}()
	result := SABImportPreview{QueueID: id, State: "local_inventory", Files: make([]SABImportPreviewFile, 0, len(entries))}
	for _, entry := range entries {
		if entry.Name() == "" || strings.ContainsAny(entry.Name(), "/\\\x00") || check() != nil {
			return fail()
		}
		info, e := root.Lstat(path.Join(relative, entry.Name()))
		if e != nil || !info.Mode().IsRegular() || info.Size() < 0 {
			return fail()
		}
		if check() != nil {
			return fail()
		}
		fd, e := syscall.Openat(int(directory.Fd()), entry.Name(), syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e != nil {
			return fail()
		}
		f := os.NewFile(uintptr(fd), "owned completed regular file")
		heldFiles = append(heldFiles, sabInventoryFile{entry.Name(), f, info})
		if check() != nil {
			return fail()
		}
		actual, e := f.Stat()
		if e != nil || !actual.Mode().IsRegular() || !sabSameInfo(info, actual) || actual.Size() > sabPreviewMaxBytes-result.ObservedBytes || check() != nil {
			return fail()
		}
		result.ObservedBytes += actual.Size()
		kind, extension := sabPreviewExtension(entry.Name())
		result.Files = append(result.Files, SABImportPreviewFile{len(result.Files) + 1, kind, extension, actual.Size()})
	}
	verify := func() error {
		// Reopen only directories with anchored no-follow traversal, never payloads.
		// Confirm both the held and currently named identities before publishing.
		again, err := openDirectory()
		if err != nil {
			return ErrSABImportPreviewUnavailable
		}
		defer again.Close()
		if check() != nil {
			return ErrSABImportPreviewUnavailable
		}
		after, err := again.Stat()
		if err != nil || !sabSameInfo(before, after) || check() != nil {
			return ErrSABImportPreviewUnavailable
		}
		currentEntries, err := again.ReadDir(sabPreviewMaxEntries + 1)
		if err != nil && !errors.Is(err, io.EOF) {
			return ErrSABImportPreviewUnavailable
		}
		if len(currentEntries) != len(entries) {
			return ErrSABImportPreviewUnavailable
		}
		sort.Slice(currentEntries, func(i, j int) bool { return currentEntries[i].Name() < currentEntries[j].Name() })
		for i, held := range heldFiles {
			if currentEntries[i].Name() != held.name || check() != nil {
				return ErrSABImportPreviewUnavailable
			}
			actual, e := held.file.Stat()
			if e != nil || !sabSameInfo(held.info, actual) {
				return ErrSABImportPreviewUnavailable
			}
			if check() != nil {
				return ErrSABImportPreviewUnavailable
			}
			named, e := root.Lstat(path.Join(relative, held.name))
			if e != nil || !named.Mode().IsRegular() || !sabSameInfo(actual, named) {
				return ErrSABImportPreviewUnavailable
			}
		}
		if check() != nil {
			return ErrSABImportPreviewUnavailable
		}
		rootCurrent, err := os.Lstat(mapping.local)
		if err != nil || !rootCurrent.IsDir() || !os.SameFile(rootInfo, rootCurrent) || check() != nil {
			return ErrSABImportPreviewUnavailable
		}
		return nil
	}
	if verify() != nil {
		return fail()
	}
	result.EntryCount = len(result.Files)
	result.ObservedAt = time.Now().UTC()
	if consume != nil {
		checkFile := func(i int) error {
			if i < 0 || i >= len(heldFiles) || check() != nil {
				return ErrSABImportPreviewUnavailable
			}
			rootCurrent, e := os.Lstat(mapping.local)
			if e != nil || !rootCurrent.IsDir() || !os.SameFile(rootInfo, rootCurrent) {
				return ErrSABImportPreviewUnavailable
			}
			currentDirectory, e := openDirectory()
			if e != nil {
				return ErrSABImportPreviewUnavailable
			}
			currentInfo, e := currentDirectory.Stat()
			currentDirectory.Close()
			if e != nil || !os.SameFile(before, currentInfo) {
				return ErrSABImportPreviewUnavailable
			}
			held := heldFiles[i]
			actual, e := held.file.Stat()
			if e != nil || !sabSameInfo(held.info, actual) {
				return ErrSABImportPreviewUnavailable
			}
			named, e := root.Lstat(path.Join(relative, held.name))
			if e != nil || !named.Mode().IsRegular() || !sabSameInfo(actual, named) || check() != nil {
				return ErrSABImportPreviewUnavailable
			}
			return nil
		}
		return consume(ctx, &sabHeldInventory{result, receipt, rootFD, heldFiles, verify, checkFile})
	}
	return result, nil
}
