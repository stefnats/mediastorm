package importer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"novastream/internal/database"
)

var ErrSABImportStageUnavailable = errors.New("managed staging unavailable")

// This internal, uninstalled seam has no HTTP/configuration/worker consumer.
// Only a trusted server supplies the private target policy and the original
// current-master/session authorization callback. A preview DTO is not authority.
type SABImportStagePolicy struct {
	TargetRoot string
	MaxBytes   int64
}
type SABImportStageSettings func(context.Context) (SABImportStagePolicy, error)
type SABImportStageStatus struct {
	QueueID       int64
	State         string
	ObservedBytes int64
	Files         []database.SABStageFile
}

func stageHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}
func stageNamespace(info os.FileInfo) (string, error) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return "", ErrSABImportStageUnavailable
	}
	return stageHash(fmt.Sprintf("sab-stage-root-v1:%d:%d", s.Dev, s.Ino)), nil
}
func stageTarget(ctx context.Context, reader SABImportStageSettings) (SABImportStagePolicy, error) {
	if reader == nil || ctx.Err() != nil {
		return SABImportStagePolicy{}, ErrSABImportStageUnavailable
	}
	p, e := reader(ctx)
	if e != nil || ctx.Err() != nil || p.MaxBytes <= 0 || !strings.HasPrefix(p.TargetRoot, "/") || p.TargetRoot == "/" || path.Clean(p.TargetRoot) != p.TargetRoot || strings.ContainsAny(p.TargetRoot, "\\\x00") || strings.Contains(p.TargetRoot, "://") {
		return SABImportStagePolicy{}, ErrSABImportStageUnavailable
	}
	return p, nil
}
func stageOpenRoot(name string) (*os.File, os.FileInfo, error) {
	before, e := os.Lstat(name)
	if e != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, ErrSABImportStageUnavailable
	}
	f, e := os.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if e != nil {
		return nil, nil, ErrSABImportStageUnavailable
	}
	actual, e := f.Stat()
	if e != nil || !actual.IsDir() || !os.SameFile(before, actual) {
		f.Close()
		return nil, nil, ErrSABImportStageUnavailable
	}
	return f, actual, nil
}

// Parent identity checks reject nested source/target roots even through a
// configured path alias. They do not recursively enumerate directories.
func stageDisjoint(a *os.File, b os.FileInfo) bool {
	current := a
	owned := false
	defer func() {
		if owned {
			current.Close()
		}
	}()
	for depth := 0; depth < 256; depth++ {
		info, e := current.Stat()
		if e != nil || os.SameFile(info, b) {
			return false
		}
		fd, e := syscall.Openat(int(current.Fd()), "..", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e != nil {
			return false
		}
		parent := os.NewFile(uintptr(fd), "staging ancestor")
		pi, e := parent.Stat()
		if e != nil {
			parent.Close()
			return false
		}
		if os.SameFile(info, pi) {
			parent.Close()
			return true
		}
		if owned {
			current.Close()
		}
		current = parent
		owned = true
	}
	return false
}

// StageSABImport copies only exact held regular-file bytes into exclusive opaque
// names under a separate server-owned root. It never registers playable media,
// scans a library, deletes source files or releases a durable logical charge.
// Logical byte admission is not physical free-space reservation. There is no
// detached worker: caller cancellation and a five-minute maximum remain bound.
type sabStageRepository interface {
	Find(context.Context, int64, string, string, string) (*database.SABStageRecord, error)
	Claim(context.Context, database.SABStageClaim, int64) (database.SABStageRecord, error)
	Finish(context.Context, string, string, []database.SABStageFile) error
}

func (s *Service) StageSABImport(ctx context.Context, id int64, source SABImportPreviewSettings, target SABImportStageSettings, authorize func(context.Context) error) (SABImportStageStatus, error) {
	if s == nil || s.database == nil || s.database.Connection() == nil {
		return SABImportStageStatus{}, ErrSABImportStageUnavailable
	}
	return s.stageSABImportWithRepository(ctx, id, source, target, authorize, database.NewSABStageRepository(s.database.Connection()))
}

// The private repository parameter permits precise post-real-COMMIT error
// injection in tests; production always uses the existing actual SQLite DB.
func (s *Service) stageSABImportWithRepository(ctx context.Context, id int64, source SABImportPreviewSettings, target SABImportStageSettings, authorize func(context.Context) error, repo sabStageRepository) (SABImportStageStatus, error) {
	fail := func() (SABImportStageStatus, error) { return SABImportStageStatus{}, ErrSABImportStageUnavailable }
	if ctx == nil || ctx.Err() != nil || authorize == nil || repo == nil {
		return fail()
	}
	ctx, stop := context.WithTimeout(ctx, 5*time.Minute)
	defer stop()
	if authorize(ctx) != nil {
		return fail()
	}
	policy, e := stageTarget(ctx, target)
	if e != nil || authorize(ctx) != nil {
		return fail()
	}
	early, rootInfo, e := stageOpenRoot(policy.TargetRoot)
	if e != nil {
		return fail()
	}
	early.Close()
	namespace, e := stageNamespace(rootInfo)
	if e != nil || ctx.Err() != nil || authorize(ctx) != nil {
		return fail()
	}
	mapping, e := sabPreviewConfig(ctx, source)
	if e != nil {
		return fail()
	}
	_, _, receipt, e := s.fallbackItem(ctx, id)
	if e != nil || receipt == nil || receipt.Source != mapping.source || receipt.Status.State != "download_completed" {
		return fail()
	}
	prior, e := repo.Find(ctx, id, receipt.Attempt, receipt.Source, namespace)
	if e != nil {
		return fail()
	}
	if prior != nil {
		return SABImportStageStatus{}, database.ErrSABStageExists
	}
	var claimed *database.SABStageRecord
	var result SABImportStageStatus
	_, e = s.withSABImportInventory(ctx, id, source, authorize, 5*time.Minute, func(work context.Context, inventory *sabHeldInventory) (SABImportPreview, error) {
		abort := func() (SABImportPreview, error) { return SABImportPreview{}, ErrSABImportStageUnavailable }
		if inventory.receipt.Attempt != receipt.Attempt || inventory.receipt.Source != receipt.Source || inventory.receipt.JobID != receipt.JobID || inventory.check() != nil {
			return abort()
		}
		root, actual, e := stageOpenRoot(policy.TargetRoot)
		if e != nil {
			return abort()
		}
		defer root.Close()
		if !os.SameFile(rootInfo, actual) {
			return abort()
		}
		sourceInfo, e := inventory.sourceRoot.Stat()
		if e != nil || !stageDisjoint(root, sourceInfo) || !stageDisjoint(inventory.sourceRoot, actual) {
			return abort()
		}
		checkTarget := func() error {
			if work.Err() != nil || authorize(work) != nil {
				return ErrSABImportStageUnavailable
			}
			p, e := stageTarget(work, target)
			if e != nil || p != policy {
				return ErrSABImportStageUnavailable
			}
			named, e := os.Lstat(policy.TargetRoot)
			if e != nil || !named.IsDir() || named.Mode() != actual.Mode() || !os.SameFile(actual, named) {
				return ErrSABImportStageUnavailable
			}
			held, e := root.Stat()
			if e != nil || held.Mode() != actual.Mode() || !os.SameFile(held, actual) || work.Err() != nil || authorize(work) != nil {
				return ErrSABImportStageUnavailable
			}
			return nil
		}
		if checkTarget() != nil || inventory.check() != nil {
			return abort()
		}
		item, _, current, e := s.fallbackItem(work, id)
		if e != nil || item.Metadata == nil || current == nil || current.Attempt != receipt.Attempt || current.Source != receipt.Source || current.JobID != receipt.JobID || current.Status.State != "download_completed" {
			return abort()
		}
		var nonce [16]byte
		if _, e := rand.Read(nonce[:]); e != nil {
			return abort()
		}
		operation := hex.EncodeToString(nonce[:])
		claim, e := repo.Claim(work, database.SABStageClaim{OperationID: operation, QueueID: id, ReceiptAttempt: receipt.Attempt, SourceHash: receipt.Source, Namespace: namespace, PathHash: stageHash(policy.TargetRoot), ExpectedBytes: inventory.result.ObservedBytes, FileCount: len(inventory.files), ExpectedMetadata: *item.Metadata}, policy.MaxBytes)
		if claim.OperationID != "" {
			claimed = &claim
		}
		if e != nil {
			return SABImportPreview{}, e
		}
		if checkTarget() != nil || inventory.check() != nil {
			return abort()
		}
		if e := syscall.Mkdirat(int(root.Fd()), operation, 0700); e != nil {
			return abort()
		}
		fd, e := syscall.Openat(int(root.Fd()), operation, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e != nil {
			return abort()
		}
		directory := os.NewFile(uintptr(fd), "private staged attempt")
		defer directory.Close()
		directoryInfo, e := directory.Stat()
		if e != nil || !directoryInfo.IsDir() {
			return abort()
		}
		var heldTargets []*sabCopiedFile
		defer func() {
			for _, f := range heldTargets {
				f.file.Close()
			}
		}()
		checkDirectory := func() error {
			if checkTarget() != nil {
				return ErrSABImportStageUnavailable
			}
			held, e := directory.Stat()
			if e != nil || !held.IsDir() || !os.SameFile(directoryInfo, held) {
				return ErrSABImportStageUnavailable
			}
			fd, e := syscall.Openat(int(root.Fd()), operation, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			if e != nil {
				return ErrSABImportStageUnavailable
			}
			named := os.NewFile(uintptr(fd), "current staged attempt")
			ni, e := named.Stat()
			named.Close()
			if e != nil || !os.SameFile(directoryInfo, ni) {
				return ErrSABImportStageUnavailable
			}
			for i, copied := range heldTargets {
				actual, e := copied.file.Stat()
				if e != nil || !sabSameInfo(copied.info, actual) {
					return ErrSABImportStageUnavailable
				}
				fd, e := syscall.Openat(int(directory.Fd()), fmt.Sprintf("data-%04d.bin", i+1), syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
				if e != nil {
					return ErrSABImportStageUnavailable
				}
				named := os.NewFile(uintptr(fd), "current copied file")
				info, e := named.Stat()
				named.Close()
				if e != nil || !sabSameInfo(copied.info, info) {
					return ErrSABImportStageUnavailable
				}
			}
			return nil
		}
		files := make([]database.SABStageFile, 0, len(inventory.files))
		buffer := make([]byte, 1<<20)
		for i, held := range inventory.files {
			copied, e := stageCopyOne(work, directory, i, held, buffer, func() error {
				if inventory.checkFile(i) != nil || checkDirectory() != nil {
					return ErrSABImportStageUnavailable
				}
				return nil
			})
			if e != nil {
				return abort()
			}
			heldTargets = append(heldTargets, copied)
			files = append(files, copied.value)
		}
		if checkDirectory() != nil || inventory.check() != nil {
			return abort()
		}
		if e := directory.Sync(); e != nil {
			return abort()
		}
		if e := root.Sync(); e != nil {
			return abort()
		}
		if checkDirectory() != nil || inventory.check() != nil {
			return abort()
		}
		if e := repo.Finish(work, operation, "copied", files); e != nil {
			return SABImportPreview{}, e
		}
		if checkDirectory() != nil || inventory.check() != nil {
			return abort()
		}
		result = SABImportStageStatus{id, "staged_copy", inventory.result.ObservedBytes, files}
		return inventory.result, nil
	})
	if e != nil {
		// Failure housekeeping starts only after actual source/target cleanup returned.
		// It never releases bytes or permits a retry; ambiguous COMMIT is not retried.
		if claimed != nil && !errors.Is(e, database.ErrSABStageOutcomeUnknown) {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = repo.Finish(cleanup, claimed.OperationID, "failed_partial", nil)
			cancel()
		}
		if errors.Is(e, database.ErrSABStageOutcomeUnknown) {
			return SABImportStageStatus{QueueID: id, State: "outcome_unknown"}, e
		}
		return SABImportStageStatus{}, e
	}
	return result, nil
}

type sabCopiedFile struct {
	value database.SABStageFile
	file  *os.File
	info  os.FileInfo
}

func stageCopyOne(ctx context.Context, directory *os.File, index int, source sabInventoryFile, buffer []byte, check func() error) (*sabCopiedFile, error) {
	return stageCopyWithWriter(ctx, directory, index, source, buffer, check, func(f *os.File, p []byte) (int, error) { return f.Write(p) })
}

// The writer port is private and permits explicitly simulated write failures;
// the installed/default implementation always writes the owned exclusive FD.
func stageCopyWithWriter(ctx context.Context, directory *os.File, index int, source sabInventoryFile, buffer []byte, check func() error, write func(*os.File, []byte) (int, error)) (*sabCopiedFile, error) {
	fail := func() (*sabCopiedFile, error) { return nil, ErrSABImportStageUnavailable }
	if ctx.Err() != nil || check() != nil {
		return fail()
	}
	name := fmt.Sprintf("data-%04d.bin", index+1)
	fd, e := syscall.Openat(int(directory.Fd()), name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if e != nil {
		return fail()
	}
	target := os.NewFile(uintptr(fd), "owned staged file")
	closed := false
	defer func() {
		if !closed {
			target.Close()
		}
	}()
	original, e := target.Stat()
	if e != nil || !original.Mode().IsRegular() {
		return fail()
	}
	digest := sha256.New()
	remaining := source.info.Size()
	var written int64
	for remaining > 0 {
		if ctx.Err() != nil || check() != nil {
			return fail()
		}
		limit := int64(len(buffer))
		if remaining < limit {
			limit = remaining
		}
		n, e := source.file.Read(buffer[:limit])
		if e != nil || n <= 0 || int64(n) > remaining {
			return fail()
		}
		if ctx.Err() != nil || check() != nil {
			return fail()
		}
		offset := 0
		for offset < n {
			if ctx.Err() != nil || check() != nil {
				return fail()
			}
			count, e := write(target, buffer[offset:n])
			if e != nil || count <= 0 || count > n-offset {
				return fail()
			}
			digest.Write(buffer[offset : offset+count])
			written += int64(count)
			offset += count
			if ctx.Err() != nil || check() != nil {
				return fail()
			}
		}
		remaining -= int64(n)
	}
	if check() != nil || ctx.Err() != nil || written != source.info.Size() {
		return fail()
	}
	if e := target.Sync(); e != nil {
		return fail()
	}
	final, e := target.Stat()
	if e != nil || !final.Mode().IsRegular() || !os.SameFile(original, final) || final.Size() != written || check() != nil {
		return fail()
	}
	// Inspect the current name with a nofollow descriptor, without payload reads.
	namedFD, e := syscall.Openat(int(directory.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return fail()
	}
	named := os.NewFile(uintptr(namedFD), "current staged file")
	info, e := named.Stat()
	if e != nil || !sabSameInfo(final, info) || check() != nil {
		named.Close()
		return fail()
	}
	e = target.Close()
	closed = true
	if e != nil || check() != nil {
		named.Close()
		return fail()
	}
	return &sabCopiedFile{database.SABStageFile{Ordinal: index + 1, Bytes: written, SHA256: hex.EncodeToString(digest.Sum(nil))}, named, info}, nil
}
