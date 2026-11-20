//go:build linux

package localmedia

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
)

type familyLocalFile struct {
	self                            *familyLocalFile
	source                          *profileunlock.FamilyLocalSource
	info                            profileunlock.FamilyLocalFileInfo
	configured, canonical, relative string
	root                            *os.Root
	file                            *os.File
	nodes                           []parentFileNode
	stat                            parentFileStat
	mu                              sync.Mutex
	closed                          bool
	readers                         map[*familyLocalReader]bool
	once                            sync.Once
	done                            chan struct{}
	stop                            func()
}

func (*familyLocalFile) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*familyLocalFile) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyLocalFile(opaque)"))
}
func (s *Service) FamilyLocalSourceReady(expected *datastore.DataStore) bool {
	return s != nil && expected != nil && s.store == expected && expected.FamilyLocalRepositoryReady(s.repo)
}
func (s *Service) OpenFamilyLocalSource(ctx context.Context, source *profileunlock.FamilyLocalSource) (opened profileunlock.FamilyLocalFile, err error) {
	if s == nil || !s.FamilyLocalSourceReady(s.store) || source == nil || !source.ReadyForStore(s.store) || ctx == nil || ctx.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	finish, err := source.BeginOpen(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	info, err := source.Info(ctx)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(info.FilePath) || filepath.Clean(info.FilePath) != info.FilePath {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	for _, configured := range info.Roots {
		if !filepath.IsAbs(configured) || filepath.Clean(configured) != configured {
			continue
		}
		canonical, e := filepath.EvalSymlinks(configured)
		if e != nil {
			continue
		}
		relative, e := filepath.Rel(configured, info.FilePath)
		if e != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			continue
		}
		full := filepath.Join(canonical, relative)
		resolved, e := filepath.EvalSymlinks(full)
		if e != nil || resolved != full {
			continue
		}
		nodes, e := parentNodes(filepath.Dir(full))
		if e != nil {
			continue
		}
		root, e := os.OpenRoot(canonical)
		if e != nil {
			continue
		}
		file, e := root.OpenFile(relative, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
		if e != nil {
			root.Close()
			continue
		}
		actual, e := file.Stat()
		version, ok := parentFileStat{}, false
		if e == nil {
			version, ok = parentStat(actual)
		}
		if !ok || version.Size != info.Size || !reliableParentFile(file) {
			file.Close()
			root.Close()
			continue
		}
		value := &familyLocalFile{source: source, info: profileunlock.FamilyLocalFileInfo{ItemID: info.ItemID, LibraryID: info.LibraryID, Name: info.FileName, Size: version.Size}, configured: configured, canonical: canonical, relative: relative, root: root, file: file, nodes: nodes, stat: version, readers: map[*familyLocalReader]bool{}, done: make(chan struct{})}
		value.self = value
		if value.Check(ctx) != nil {
			value.retire()
			continue
		}
		value.stop = familyLocalAfterFunc(source.Context(), value.retire)
		if source.BindRetirement(value.closeBound, value.done) != nil {
			value.closeBound()
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		if value.Check(ctx) != nil {
			value.closeBound()
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		return value, nil
	}
	return nil, profileunlock.ErrFamilyMediaChanged
}
func (f *familyLocalFile) Context() context.Context {
	if f == nil || f.self != f {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return f.source.Context()
}
func (f *familyLocalFile) Info() profileunlock.FamilyLocalFileInfo {
	if f == nil || f.self != f {
		return profileunlock.FamilyLocalFileInfo{}
	}
	return f.info
}
func (f *familyLocalFile) Check(ctx context.Context) (err error) {
	defer func() {
		if err != nil && f != nil && f.self == f && ctx != nil && ctx.Err() == nil {
			f.source.Invalidate()
		}
	}()
	if f == nil || f.self != f || ctx == nil || ctx.Err() != nil || f.source.Check(ctx) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return profileunlock.ErrFamilyMediaChanged
	}
	canonical, e := filepath.EvalSymlinks(f.configured)
	if e != nil || canonical != f.canonical {
		return profileunlock.ErrFamilyMediaChanged
	}
	namedRoot, e := os.Stat(canonical)
	heldRoot, rootErr := f.root.Stat(".")
	if e != nil || rootErr != nil || !os.SameFile(namedRoot, heldRoot) {
		return profileunlock.ErrFamilyMediaChanged
	}
	for _, node := range f.nodes {
		now, e := os.Lstat(node.Path)
		if e != nil || !now.IsDir() {
			return profileunlock.ErrFamilyMediaChanged
		}
		stat, ok := now.Sys().(*syscall.Stat_t)
		if !ok || uint64(stat.Dev) != node.Device || stat.Ino != node.Inode {
			return profileunlock.ErrFamilyMediaChanged
		}
	}
	full := filepath.Join(f.canonical, f.relative)
	resolved, e := filepath.EvalSymlinks(full)
	if e != nil || resolved != full {
		return profileunlock.ErrFamilyMediaChanged
	}
	held, e := f.file.Stat()
	named, namedErr := f.root.Stat(f.relative)
	if e != nil || namedErr != nil || !os.SameFile(held, named) || !reliableParentFile(f.file) {
		return profileunlock.ErrFamilyMediaChanged
	}
	current, ok := parentStat(held)
	other, otherOK := parentStat(named)
	if !ok || !otherOK || current != f.stat || other != f.stat || ctx.Err() != nil || f.source.Check(ctx) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func familyLocalAfterFunc(ctx context.Context, callback func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop() {
				close(done)
			}
		})
		<-done
	}
}
func (f *familyLocalFile) retire() {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		var readers []*familyLocalReader
		for reader := range f.readers {
			readers = append(readers, reader)
		}
		f.mu.Unlock()
		for _, reader := range readers {
			_ = reader.Close()
		}
		_ = f.file.Close()
		_ = f.root.Close()
		close(f.done)
	})
}
func (f *familyLocalFile) closeBound() {
	f.retire()
	if f.stop != nil {
		f.stop()
	}
}
func (f *familyLocalFile) Close() error {
	if f != nil && f.self == f {
		f.source.Close()
	}
	return nil
}

var familyLocalReaderAdmission = struct {
	sync.Mutex
	count int
}{}

type familyLocalReader struct {
	owner                 *familyLocalFile
	file                  *os.File
	ctx                   context.Context
	cancel                context.CancelFunc
	start, length, offset int64
	mu, readMu            sync.Mutex
	closed                bool
	reads                 sync.WaitGroup
	once                  sync.Once
	cleanupOnce           sync.Once
	interruptDone         chan struct{}
	done                  chan struct{}
	stop                  func()
}

func (f *familyLocalFile) OpenReader(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	if f == nil || f.self != f || start < 0 || length < 0 || start > f.info.Size || length > f.info.Size-start || f.Check(ctx) != nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	f.mu.Lock()
	if f.closed || len(f.readers) >= 8 {
		f.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	familyLocalReaderAdmission.Lock()
	if familyLocalReaderAdmission.count >= 256 {
		familyLocalReaderAdmission.Unlock()
		f.mu.Unlock()
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	familyLocalReaderAdmission.count++
	familyLocalReaderAdmission.Unlock()
	fd, err := unix.FcntlInt(f.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		familyLocalReaderAdmission.Lock()
		familyLocalReaderAdmission.count--
		familyLocalReaderAdmission.Unlock()
		f.mu.Unlock()
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	reader := &familyLocalReader{owner: f, file: os.NewFile(uintptr(fd), "family-local-original"), ctx: life, cancel: cancel, start: start, length: length, interruptDone: make(chan struct{}), done: make(chan struct{})}
	stopOwn := familyLocalAfterFunc(life, reader.interrupt)
	stopSource := familyLocalAfterFunc(f.source.Context(), cancel)
	// Both cancellation links are owned until actual duplicate FD/read cleanup.
	reader.stop = func() { stopOwn(); stopSource() }
	f.readers[reader] = true
	f.mu.Unlock()
	actual, err := reader.file.Stat()
	version, ok := parentFileStat{}, false
	if err == nil {
		version, ok = parentStat(actual)
	}
	if !ok || version != f.stat || f.Check(ctx) != nil {
		reader.Close()
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	return reader, nil
}
func (r *familyLocalReader) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	r.reads.Add(1)
	r.mu.Unlock()
	defer r.reads.Done()
	if r.offset >= r.length {
		return 0, io.EOF
	}
	if r.ctx.Err() != nil || r.owner.Check(r.ctx) != nil {
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	amount := int64(len(p))
	if amount > 32<<10 {
		amount = 32 << 10
	}
	if amount > r.length-r.offset {
		amount = r.length - r.offset
	}
	buffer := make([]byte, int(amount))
	defer clear(buffer)
	n, err := r.file.ReadAt(buffer, r.start+r.offset)
	actual, statErr := r.file.Stat()
	version, ok := parentFileStat{}, false
	if statErr == nil {
		version, ok = parentStat(actual)
	}
	if n < 0 || n > len(buffer) || err != nil && err != io.EOF || !ok || version != r.owner.stat || r.ctx.Err() != nil || r.owner.Check(r.ctx) != nil {
		return 0, profileunlock.ErrFamilyMediaChanged
	}
	copy(p, buffer[:n])
	r.offset += int64(n)
	return n, err
}
func (r *familyLocalReader) interrupt() {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		_ = r.file.Close()
		r.reads.Wait()
		close(r.interruptDone)
	})
}
func (r *familyLocalReader) Close() error {
	r.interrupt()
	if r.stop != nil {
		r.stop()
	}
	// Cancellation callbacks only interrupt. Keep this reader charged and
	// visible to original cleanup until their actual wrappers have returned.
	r.cleanupOnce.Do(func() {
		familyLocalReaderAdmission.Lock()
		familyLocalReaderAdmission.count--
		familyLocalReaderAdmission.Unlock()
		r.owner.mu.Lock()
		delete(r.owner.readers, r)
		close(r.done)
		r.owner.mu.Unlock()
	})
	return nil
}

var _ profileunlock.FamilyLocalFile = (*familyLocalFile)(nil)
