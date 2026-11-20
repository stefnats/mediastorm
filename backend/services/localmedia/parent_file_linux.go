//go:build linux

package localmedia

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	"novastream/internal/datastore"
	"novastream/services/kidsvod"
)

type parentFileNode struct {
	Path          string
	Device, Inode uint64
}
type parentFileStat struct {
	Device, Inode uint64
	Size          int64
	Mode          uint32
	Mtime, Ctime  syscall.Timespec
}
type openedParentFile struct {
	mu                                      sync.Mutex
	store                                   *datastore.DataStore
	account, profile, item                  string
	snapshot                                datastore.LocalParentFileSnapshot
	configuredRoot, canonicalRoot, relative string
	root                                    *os.Root
	file                                    *os.File
	nodes                                   []parentFileNode
	stat                                    parentFileStat
	fingerprint                             string
	closed                                  bool
}

func parentStat(info os.FileInfo) (parentFileStat, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Nlink != 1 || st.Size <= 0 {
		return parentFileStat{}, false
	}
	return parentFileStat{Device: uint64(st.Dev), Inode: st.Ino, Size: st.Size, Mode: st.Mode, Mtime: st.Mtim, Ctime: st.Ctim}, true
}
func parentNodes(path string) ([]parentFileNode, error) {
	var reverse []parentFileNode
	pathBytes := 0
	for {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			return nil, kidsvod.ErrParentFileChanged
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil, kidsvod.ErrParentFileChanged
		}
		reverse = append(reverse, parentFileNode{path, uint64(st.Dev), st.Ino})
		pathBytes += len(path)
		if len(reverse) > 64 || pathBytes > 16<<10 {
			return nil, kidsvod.ErrParentFileChanged
		}
		next := filepath.Dir(path)
		if next == path {
			break
		}
		path = next
	}
	return reverse, nil
}
func reliableParentFile(file *os.File) bool {
	var fs unix.Statfs_t
	if unix.Fstatfs(int(file.Fd()), &fs) != nil {
		return false
	}
	// The initial vertical supports regular stable local files only. Network,
	// proc/sys, FUSE, devices and unknown filesystems are deliberately denied.
	switch uint64(fs.Type) {
	case 0xef53, 0x58465342, 0x9123683e, 0x01021994, 0x794c7630:
		return true
	}
	return false
}

// OpenParentFile opens no media bytes and runs no probe. The held actual FD and
// rooted directory survive the bounded parent prompt; range requests duplicate
// this FD rather than reopening a pathname. Normal in-place changes are checked
// by the reliable local stat version; no content-hash immutability is claimed.
func (s *Service) OpenParentFile(ctx context.Context, subject kidsvod.Subject, item string) (kidsvod.ParentFileSource, error) {
	if s == nil || s.store == nil {
		return nil, kidsvod.ErrParentFileChanged
	}
	snapshot, err := s.store.CurrentLocalParentFile(ctx, subject.AccountID(), subject.ProfileID(), item)
	if err != nil {
		return nil, err
	}
	opened, err := openParentFile(ctx, s.store, subject.AccountID(), subject.ProfileID(), snapshot)
	if err != nil {
		return nil, err
	}
	return opened, nil
}
func openParentFile(ctx context.Context, store *datastore.DataStore, account, profile string, snapshot datastore.LocalParentFileSnapshot) (*openedParentFile, error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(snapshot.FilePath) || filepath.Clean(snapshot.FilePath) != snapshot.FilePath {
		return nil, kidsvod.ErrParentFileChanged
	}
	for _, configured := range snapshot.Roots {
		if !filepath.IsAbs(configured) || filepath.Clean(configured) != configured {
			continue
		}
		canonical, err := filepath.EvalSymlinks(configured)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(configured, snapshot.FilePath)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
			continue
		}
		full := filepath.Join(canonical, rel)
		resolved, err := filepath.EvalSymlinks(full)
		if err != nil || resolved != full {
			continue
		}
		nodes, err := parentNodes(filepath.Dir(full))
		if err != nil {
			continue
		}
		root, err := os.OpenRoot(canonical)
		if err != nil {
			continue
		}
		file, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			_ = root.Close()
			continue
		}
		info, err := file.Stat()
		version, ok := parentFileStat{}, false
		if err == nil {
			version, ok = parentStat(info)
		}
		if !ok || !reliableParentFile(file) {
			_ = file.Close()
			_ = root.Close()
			continue
		}
		opened := &openedParentFile{store: store, account: account, profile: profile, item: snapshot.ItemID, snapshot: snapshot, configuredRoot: configured, canonicalRoot: canonical, relative: rel, root: root, file: file, nodes: nodes, stat: version}
		raw, _ := json.Marshal([]any{snapshot.Revision, configured, canonical, rel, nodes, version})
		sum := sha256.Sum256(raw)
		opened.fingerprint = hex.EncodeToString(sum[:])
		if err = opened.Check(ctx); err != nil {
			_ = opened.Close()
			continue
		}
		return opened, nil
	}
	return nil, kidsvod.ErrParentFileChanged
}
func (f *openedParentFile) Fingerprint() string { return f.fingerprint }
func (f *openedParentFile) ItemID() string      { return f.item }
func (f *openedParentFile) Size() int64         { return f.stat.Size }
func (f *openedParentFile) Name() string        { return filepath.Base(f.snapshot.FileName) }
func (f *openedParentFile) Check(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return kidsvod.ErrParentFileChanged
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return kidsvod.ErrParentFileChanged
	}
	if f.store == nil {
		return kidsvod.ErrParentFileChanged
	}
	current, err := f.store.CurrentLocalParentFile(ctx, f.account, f.profile, f.item)
	if err != nil || current.Revision != f.snapshot.Revision {
		return kidsvod.ErrParentFileChanged
	}
	canonical, err := filepath.EvalSymlinks(f.configuredRoot)
	if err != nil || canonical != f.canonicalRoot {
		return kidsvod.ErrParentFileChanged
	}
	currentRoot, err := os.Stat(canonical)
	heldRoot, rootErr := f.root.Stat(".")
	if err != nil || rootErr != nil || !currentRoot.IsDir() || !heldRoot.IsDir() || !os.SameFile(currentRoot, heldRoot) {
		return kidsvod.ErrParentFileChanged
	}
	for _, node := range f.nodes {
		info, err := os.Lstat(node.Path)
		if err != nil || !info.IsDir() {
			return kidsvod.ErrParentFileChanged
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || uint64(st.Dev) != node.Device || st.Ino != node.Inode {
			return kidsvod.ErrParentFileChanged
		}
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(f.canonicalRoot, f.relative))
	if err != nil || resolved != filepath.Join(f.canonicalRoot, f.relative) {
		return kidsvod.ErrParentFileChanged
	}
	info, err := f.file.Stat()
	if err != nil {
		return kidsvod.ErrParentFileChanged
	}
	actual, ok := parentStat(info)
	if !ok || actual != f.stat {
		return kidsvod.ErrParentFileChanged
	}
	named, err := f.root.Stat(f.relative)
	if err != nil {
		return kidsvod.ErrParentFileChanged
	}
	version, ok := parentStat(named)
	if !ok || version != f.stat || !os.SameFile(info, named) || !reliableParentFile(f.file) || ctx.Err() != nil {
		return kidsvod.ErrParentFileChanged
	}
	return nil
}
func (f *openedParentFile) OpenReader(ctx context.Context) (kidsvod.FileReadAtCloser, error) {
	if err := f.Check(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || ctx.Err() != nil {
		return nil, kidsvod.ErrParentFileChanged
	}
	fd, err := unix.FcntlInt(f.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "parent-approved-local-file"), nil
}

// The descriptor must be the actual held-file duplicate, never an expected
// filename/version label pasted onto a newly opened process input.
func (f *openedParentFile) CheckDescriptor(ctx context.Context, file *os.File) error {
	if file == nil || f.Check(ctx) != nil {
		return kidsvod.ErrParentFileChanged
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || ctx.Err() != nil {
		return kidsvod.ErrParentFileChanged
	}
	actual, err := file.Stat()
	original, e := f.file.Stat()
	if err != nil || e != nil || !os.SameFile(actual, original) {
		return kidsvod.ErrParentFileChanged
	}
	version, ok := parentStat(actual)
	if !ok || version != f.stat || ctx.Err() != nil {
		return kidsvod.ErrParentFileChanged
	}
	return nil
}
func (f *openedParentFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	err := f.file.Close()
	if rootErr := f.root.Close(); err == nil {
		err = rootErr
	}
	return err
}
