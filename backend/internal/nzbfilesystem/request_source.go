package nzbfilesystem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/spf13/afero"
	"google.golang.org/protobuf/proto"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzb/utils"
	"novastream/internal/usenet"
)

var (
	ErrSourceCleanupUnsupported = errors.New("actual NZB source cleanup unavailable")
	ErrRequestSourceRetired     = errors.New("original request source retired")
	ErrRequestSourceBudget      = errors.New("request source budget exceeded")
)

// RequestSourceBudgets are I/O DATA, never an account, source or PIN permission.
type RequestSourceBudgets struct {
	MetadataBytes                  int64
	Entries, Depth, PendingReaders int
}

func boundedRequestBudgets(b RequestSourceBudgets) (RequestSourceBudgets, error) {
	if b.MetadataBytes == 0 {
		b.MetadataBytes = 16 << 20
	}
	if b.Entries == 0 {
		b.Entries = 4096
	}
	if b.Depth == 0 {
		b.Depth = 32
	}
	if b.PendingReaders == 0 {
		b.PendingReaders = 64
	}
	if b.MetadataBytes < 1 || b.MetadataBytes > 16<<20 || b.Entries < 1 || b.Entries > 4096 || b.Depth < 1 || b.Depth > 32 || b.PendingReaders < 1 || b.PendingReaders > 64 {
		return b, ErrRequestSourceBudget
	}
	return b, nil
}

type sourceLifetime struct {
	self                    *sourceLifetime
	body                    io.ReadCloser
	ctx                     context.Context
	cancel                  context.CancelFunc
	mu                      sync.Mutex
	retired                 bool
	pending                 sync.WaitGroup
	children                map[io.ReadCloser]<-chan struct{}
	once                    sync.Once
	done                    chan struct{}
	err                     error
	stop                    func() bool
	callbackDone, setupDone chan struct{}
	limit                   int
	request                 *RequestSource
}

func newSourceLifetime(ctx context.Context, cancel context.CancelFunc, body io.ReadCloser, limit int) *sourceLifetime {
	s := &sourceLifetime{body: body, ctx: ctx, cancel: cancel, done: make(chan struct{}), callbackDone: make(chan struct{}), setupDone: make(chan struct{}), children: make(map[io.ReadCloser]<-chan struct{}), limit: limit}
	s.self = s
	s.stop = context.AfterFunc(ctx, func() { defer close(s.callbackDone); s.signal() })
	close(s.setupDone)
	return s
}
func (s *sourceLifetime) enter() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.ctx.Err() != nil {
		return ErrRequestSourceRetired
	}
	s.pending.Add(1)
	return nil
}
func (s *sourceLifetime) leave() { s.pending.Done() }
func (s *sourceLifetime) signal() {
	s.mu.Lock()
	s.retired = true
	s.cancel()
	children := make([]io.ReadCloser, 0, len(s.children))
	for r := range s.children {
		children = append(children, r)
	}
	s.mu.Unlock()
	for _, r := range children {
		_ = r.Close()
	}
}
func (s *sourceLifetime) register(r io.ReadCloser) error {
	if s.request != nil {
		if err := s.request.registerReader(r); err != nil {
			return err
		}
	}
	done, err := usenet.ReaderCleanupDone(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	completed := make([]io.ReadCloser, 0)
	for old, ack := range s.children {
		select {
		case <-ack:
			completed = append(completed, old)
			delete(s.children, old)
		default:
		}
	}
	denied := error(nil)
	if s.retired || s.ctx.Err() != nil {
		denied = ErrRequestSourceRetired
	} else if len(s.children) >= s.limit {
		denied = ErrRequestSourceBudget
	}
	if denied == nil {
		s.children[r] = done
	}
	s.mu.Unlock()
	// Closed concrete acknowledgements make these joins immediate. Preserve failures.
	for _, old := range completed {
		if e := usenet.JoinReaderCleanup(old); e != nil {
			s.recordError(e)
		}
	}
	if denied != nil {
		if e := usenet.JoinReaderCleanup(r); e != nil {
			s.recordError(e)
		}
		return denied
	}
	return nil
}
func (s *sourceLifetime) recordError(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}
func (s *sourceLifetime) join() error {
	s.once.Do(func() {
		s.signal()
		s.pending.Wait()
		s.mu.Lock()
		children := make([]io.ReadCloser, 0, len(s.children))
		for r := range s.children {
			children = append(children, r)
		}
		s.mu.Unlock()
		for _, r := range children {
			if err := usenet.JoinReaderCleanup(r); err != nil {
				s.recordError(err)
			}
		}
		<-s.setupDone
		if s.stop != nil && !s.stop() {
			<-s.callbackDone
		}
		close(s.done)
	})
	<-s.done
	return s.err
}
func sourceLifetimeOf(body io.ReadCloser) (*sourceLifetime, error) {
	var s *sourceLifetime
	switch value := body.(type) {
	case *MetadataVirtualFile:
		if value != nil {
			s = value.lifetime
		}
	case *MetadataVirtualDirectory:
		if value != nil {
			s = value.lifetime
		}
	case *requestFile:
		if value != nil && value.owner != nil && value.owner.self == value.owner {
			return sourceLifetimeOf(value.File)
		}
	}
	if s == nil || s.self != s || s.body != body {
		return nil, ErrSourceCleanupUnsupported
	}
	return s, nil
}

// RequireSourceCleanup describes protocol readiness; it is neither permission nor completion.
func RequireSourceCleanup(body io.ReadCloser) error {
	s, err := sourceLifetimeOf(body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.ctx.Err() != nil {
		return ErrSourceCleanupUnsupported
	}
	return nil
}

// SourceCleanupDone exposes only the original actor's physical completion.
// It neither authorizes I/O nor treats an arbitrary Closer as completion proof.
// A file owning its request must include the outer request's callback, pending
// operations and readers, rather than its earlier inner-file acknowledgement.
func SourceCleanupDone(body io.ReadCloser) (<-chan struct{}, error) {
	if file, ok := body.(*requestFile); ok {
		if file == nil || file.owner == nil || file.owner.self != file.owner || !file.ownsSource {
			return nil, ErrSourceCleanupUnsupported
		}
		if _, err := sourceLifetimeOf(file.File); err != nil {
			return nil, err
		}
		return file.owner.done, nil
	}
	life, err := sourceLifetimeOf(body)
	if err != nil || life.done == nil || life.setupDone == nil {
		return nil, ErrSourceCleanupUnsupported
	}
	return life.done, nil
}

// SignalSourceClose never waits for an active Read or a cleanup callback.
func SignalSourceClose(body io.ReadCloser) error {
	s, err := sourceLifetimeOf(body)
	if err != nil {
		return err
	}
	s.signal()
	return nil
}

// JoinSourceCleanup accepts only original concrete source actors, not an arbitrary CloseAndJoin interface.
func JoinSourceCleanup(body io.ReadCloser) error {
	s, err := sourceLifetimeOf(body)
	if err != nil {
		return err
	}
	s.signal()
	// The signal precedes the file's Read mutex. The outer cipher then closes before inner joins.
	if closeErr := body.Close(); closeErr != nil {
		s.recordError(closeErr)
	}
	return s.join()
}

func attachSourceLifetime(ctx context.Context, cancel context.CancelFunc, body afero.File) *sourceLifetime {
	limit := 64
	if request, _ := ctx.Value(requestSourceKey{}).(*RequestSource); request != nil && request.self == request {
		limit = request.budgets.PendingReaders
	}
	life := newSourceLifetime(ctx, cancel, body, limit)
	if request, _ := ctx.Value(requestSourceKey{}).(*RequestSource); request != nil && request.self == request {
		life.request = request
	}
	switch value := body.(type) {
	case *MetadataVirtualFile:
		value.lifetime = life
		value.ctx = ctx
	case *MetadataVirtualDirectory:
		value.lifetime = life
	}
	return life
}

// RequestSource owns real bounded filesystem work. It is not media authorization.
type requestSourceKey struct{}
type RequestSource struct {
	self                    *RequestSource
	fs                      *NzbFilesystem
	budgets                 RequestSourceBudgets
	ctx                     context.Context
	cancel                  context.CancelFunc
	mu                      sync.Mutex
	retired                 bool
	entries                 int
	pending                 sync.WaitGroup
	files                   map[*requestFile]struct{}
	readers                 map[io.ReadCloser]<-chan struct{}
	once                    sync.Once
	done                    chan struct{}
	err                     error
	stop                    func() bool
	callbackDone, setupDone chan struct{}
}

func (nfs *NzbFilesystem) NewRequestSource(ctx context.Context, budgets RequestSourceBudgets) (*RequestSource, error) {
	if nfs == nil || nfs.remoteFile == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrRequestSourceRetired
	}
	b, err := boundedRequestBudgets(budgets)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithDeadline(ctx, time.Now().Add(24*time.Hour))
	value := &RequestSource{fs: nfs, budgets: b, ctx: life, cancel: cancel, files: make(map[*requestFile]struct{}), readers: make(map[io.ReadCloser]<-chan struct{}), done: make(chan struct{}), callbackDone: make(chan struct{}), setupDone: make(chan struct{})}
	value.self = value
	value.ctx = context.WithValue(life, requestSourceKey{}, value)
	value.stop = context.AfterFunc(life, func() { defer close(value.callbackDone); _ = value.Close() })
	close(value.setupDone)
	return value, nil
}
func (s *RequestSource) begin(name string) error {
	if s == nil || s.self != s {
		return ErrRequestSourceRetired
	}
	if !utf8.ValidString(name) || len(name) > 4096 || strings.ContainsRune(name, 0) || strings.Contains(name, "\\") {
		return fs.ErrInvalid
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return fs.ErrPermission
		}
	}
	clean := strings.Trim(path.Clean("/"+name), "/")
	depth := 0
	if clean != "" {
		depth = len(strings.Split(clean, "/"))
	}
	if depth > s.budgets.Depth {
		return ErrRequestSourceBudget
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.ctx.Err() != nil {
		return ErrRequestSourceRetired
	}
	s.pending.Add(1)
	return nil
}
func (s *RequestSource) Close() error {
	if s == nil || s.self != s {
		return ErrSourceCleanupUnsupported
	}
	s.mu.Lock()
	s.retired = true
	s.cancel()
	files := make([]*requestFile, 0, len(s.files))
	for f := range s.files {
		files = append(files, f)
	}
	readers := make([]io.ReadCloser, 0, len(s.readers))
	for r := range s.readers {
		readers = append(readers, r)
	}
	s.mu.Unlock()
	for _, r := range readers {
		_ = r.Close()
	}
	for _, f := range files {
		_ = SignalSourceClose(f.File)
	}
	return nil
}
func (s *RequestSource) CloseAndJoin() error {
	if s == nil || s.self != s {
		return ErrSourceCleanupUnsupported
	}
	s.once.Do(func() {
		_ = s.Close()
		s.pending.Wait()
		s.mu.Lock()
		files := make([]*requestFile, 0, len(s.files))
		for f := range s.files {
			files = append(files, f)
		}
		s.mu.Unlock()
		for _, f := range files {
			if err := f.closeFile(); err != nil {
				s.mu.Lock()
				if s.err == nil {
					s.err = err
				}
				s.mu.Unlock()
			}
		}
		s.mu.Lock()
		readers := make([]io.ReadCloser, 0, len(s.readers))
		for r := range s.readers {
			readers = append(readers, r)
		}
		s.mu.Unlock()
		for _, r := range readers {
			if err := usenet.JoinReaderCleanup(r); err != nil {
				s.mu.Lock()
				if s.err == nil {
					s.err = err
				}
				s.mu.Unlock()
			}
		}

		<-s.setupDone
		if s.stop != nil && !s.stop() {
			<-s.callbackDone
		}
		close(s.done)
	})
	<-s.done
	return s.err
}
func (s *RequestSource) Open(name string) (afero.File, error) {
	return s.OpenFile(name, os.O_RDONLY, 0)
}
func (s *RequestSource) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if flag != os.O_RDONLY {
		return nil, os.ErrPermission
	}
	args, err := utils.NewPathWithArgsFromString(name)
	if err != nil {
		return nil, err
	}
	if args.IsCopy() {
		return nil, os.ErrPermission
	}
	if err := s.begin(args.Path); err != nil {
		return nil, err
	}
	defer s.pending.Done()
	ok, file, err := s.fs.remoteFile.OpenFile(s.ctx, args.Path, args)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	value := &requestFile{File: file, owner: s}
	s.mu.Lock()
	for old := range s.files {
		if old.closed {
			delete(s.files, old)
		}
	}
	if s.retired || s.ctx.Err() != nil || len(s.files) >= 64 {
		err := ErrRequestSourceBudget
		if s.retired || s.ctx.Err() != nil {
			err = ErrRequestSourceRetired
		}
		s.mu.Unlock()
		_ = JoinSourceCleanup(file)
		return nil, err
	}
	s.files[value] = struct{}{}
	s.mu.Unlock()
	return value, nil
}

type requestFile struct {
	afero.File
	owner      *RequestSource
	once       sync.Once
	err        error
	closed     bool
	ownsSource bool
}

func (f *requestFile) Read(p []byte) (int, error) {
	if err := f.owner.begin(f.Name()); err != nil {
		return 0, err
	}
	defer f.owner.pending.Done()
	n, err := f.File.Read(p)
	if f.owner.ctx.Err() != nil {
		return 0, ErrRequestSourceRetired
	}
	return n, err
}
func (f *requestFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.owner.begin(f.Name()); err != nil {
		return 0, err
	}
	defer f.owner.pending.Done()
	return f.File.Seek(offset, whence)
}
func (f *requestFile) Readdir(count int) ([]fs.FileInfo, error) {
	if err := f.owner.begin(f.Name()); err != nil {
		return nil, err
	}
	defer f.owner.pending.Done()
	return f.File.Readdir(count)
}
func (f *requestFile) closeFile() error {
	f.once.Do(func() {
		f.err = JoinSourceCleanup(f.File)
		f.owner.mu.Lock()
		f.closed = true
		if f.err != nil && f.owner.err == nil {
			f.owner.err = f.err
		}
		f.owner.mu.Unlock()
	})
	return f.err
}
func (f *requestFile) Close() error {
	err := f.closeFile()
	if f.ownsSource {
		if e := f.owner.CloseAndJoin(); err == nil {
			err = e
		}
	}
	return err
}
func (s *RequestSource) Stat(name string) (fs.FileInfo, error) {
	file, err := s.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.Stat()
}
func (s *RequestSource) RemoveAll(name string) error {
	if err := s.begin(name); err != nil {
		return err
	}
	defer s.pending.Done()
	root, err := os.OpenRoot(s.fs.remoteFile.metadataService.RootPath())
	if err != nil {
		return err
	}
	defer root.Close()
	original, err := s.requestMutationMetadata(root, name)
	if err != nil {
		return err
	}
	if err = s.ctx.Err(); err != nil {
		return err
	}
	// Remove exactly one regular metadata file. Never recurse into a directory.
	if err = root.Remove(original); err != nil {
		return err
	}
	return s.ctx.Err()
}
func (s *RequestSource) Rename(oldName, newName string) error {
	if err := s.begin(oldName); err != nil {
		return err
	}
	defer s.pending.Done()
	if err := s.begin(newName); err != nil {
		return err
	}
	defer s.pending.Done()
	root, err := os.OpenRoot(s.fs.remoteFile.metadataService.RootPath())
	if err != nil {
		return err
	}
	defer root.Close()
	original, err := s.requestMutationMetadata(root, oldName)
	if err != nil {
		return err
	}
	destination, err := requestMutationName(root, newName)
	if err != nil {
		return err
	}
	// A destination parent must already exist; this request path never creates trees.
	if current, e := root.Lstat(destination); e == nil && !current.Mode().IsRegular() {
		return os.ErrPermission
	} else if e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if err = s.ctx.Err(); err != nil {
		return err
	}
	// FileMetadata has no virtual filename/path field. source_nzb_path describes
	// original import provenance, so an exact byte-preserving move is intentional.
	if err = root.Rename(original, destination); err != nil {
		return err
	}
	return s.ctx.Err()
}
func requestMutationName(root *os.Root, name string) (string, error) {
	relative := strings.TrimPrefix(path.Clean("/"+name), "/")
	if relative == "" || relative == "." {
		return "", os.ErrPermission
	}
	// Metadata directories and unknown physical files are unsupported mutations.
	if _, err := root.Lstat(relative); err == nil {
		return "", os.ErrPermission
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return relative + ".meta", nil
}
func (s *RequestSource) requestMutationMetadata(root *os.Root, name string) (string, error) {
	actual, _, _, err := s.readMutationMetadata(root, name)
	return actual, err
}
func (s *RequestSource) readMutationMetadata(root *os.Root, name string) (string, fs.FileInfo, *metapb.FileMetadata, error) {
	actual, err := requestMutationName(root, name)
	if err != nil {
		return "", nil, nil, err
	}
	before, err := root.Lstat(actual)
	if err != nil {
		return "", nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > s.budgets.MetadataBytes {
		return "", nil, nil, ErrRequestSourceBudget
	}
	value, err := s.fs.remoteFile.metadataService.ReadRequestMetadata(s.ctx, name, s.budgets.MetadataBytes)
	if err != nil {
		return "", nil, nil, err
	}
	if value == nil {
		return "", nil, nil, os.ErrNotExist
	}
	if err = requestMetadataStillNamed(root, actual, before); err != nil {
		return "", nil, nil, err
	}
	if err = s.ctx.Err(); err != nil {
		return "", nil, nil, err
	}
	return actual, before, value, nil
}
func requestMetadataStillNamed(root *os.Root, name string, before fs.FileInfo) error {
	after, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
		return ErrRequestSourceRetired
	}
	return nil
}

// A request health child never falls through to the unbounded legacy reader.
func (s *RequestSource) updateFileStatus(name string, status metapb.FileStatus) error {
	if err := s.begin(name); err != nil {
		return err
	}
	defer s.pending.Done()
	root, err := os.OpenRoot(s.fs.remoteFile.metadataService.RootPath())
	if err != nil {
		return err
	}
	defer root.Close()
	actual, before, value, err := s.readMutationMetadata(root, name)
	if err != nil {
		return err
	}
	value.Status = status
	value.ModifiedAt = time.Now().Unix()
	raw, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	defer clear(raw)
	if int64(len(raw)) > s.budgets.MetadataBytes {
		return ErrRequestSourceBudget
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return err
	}
	staged := path.Join(path.Dir(actual), ".request-metadata-"+hex.EncodeToString(nonce[:])+".tmp")
	if err = s.ctx.Err(); err != nil {
		return err
	}
	file, err := root.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, before.Mode().Perm())
	if err != nil {
		return err
	}
	defer root.Remove(staged)
	defer file.Close()
	if err = s.ctx.Err(); err != nil {
		return err
	}
	if n, e := file.Write(raw); e != nil {
		return e
	} else if n != len(raw) {
		return io.ErrShortWrite
	}
	if err = s.ctx.Err(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = requestMetadataStillNamed(root, actual, before); err != nil {
		return err
	}
	if err = s.ctx.Err(); err != nil {
		return err
	}
	if err = root.Rename(staged, actual); err != nil {
		return err
	}
	return s.ctx.Err()
}

func (s *RequestSource) reserveEntries(count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.ctx.Err() != nil {
		return ErrRequestSourceRetired
	}
	if count > s.budgets.Entries-s.entries {
		return ErrRequestSourceBudget
	}
	s.entries += count
	return nil
}
func (f *requestFile) Stat() (fs.FileInfo, error) {
	if err := f.owner.begin(f.Name()); err != nil {
		return nil, err
	}
	defer f.owner.pending.Done()
	return f.File.Stat()
}
func (f *requestFile) Readdirnames(n int) ([]string, error) {
	infos, err := f.Readdir(n)
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}
	return names, err
}
func (f *requestFile) ReadAt(p []byte, offset int64) (int, error) { return 0, os.ErrPermission }

// Context-aware entry points preserve the original WebDAV context and keep mutations joined.
func (s *RequestSource) OpenFileWithContext(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.OpenFile(name, flag, perm)
}

func (s *RequestSource) Name() string                                      { return "RequestNzbFilesystem" }
func (s *RequestSource) Remove(name string) error                          { return s.RemoveAll(name) }
func (s *RequestSource) Create(name string) (afero.File, error)            { return nil, os.ErrPermission }
func (s *RequestSource) Mkdir(name string, perm os.FileMode) error         { return os.ErrPermission }
func (s *RequestSource) MkdirAll(name string, perm os.FileMode) error      { return os.ErrPermission }
func (s *RequestSource) Chmod(name string, mode os.FileMode) error         { return os.ErrPermission }
func (s *RequestSource) Chown(name string, uid, gid int) error             { return os.ErrPermission }
func (s *RequestSource) Chtimes(name string, atime, mtime time.Time) error { return os.ErrPermission }
func (s *RequestSource) StatWithContext(ctx context.Context, name string) (fs.FileInfo, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.Stat(name)
}
func (s *RequestSource) RemoveAllWithContext(ctx context.Context, name string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.RemoveAll(name)
}
func (s *RequestSource) RenameWithContext(ctx context.Context, oldName, newName string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.Rename(oldName, newName)
}

// A view-wide registry bounds active and retired-but-unjoined inner readers across files.
func (s *RequestSource) registerReader(reader io.ReadCloser) error {
	ack, err := usenet.ReaderCleanupDone(reader)
	if err != nil {
		return err
	}
	s.mu.Lock()
	completed := make([]io.ReadCloser, 0)
	for old, done := range s.readers {
		select {
		case <-done:
			completed = append(completed, old)
			delete(s.readers, old)
		default:
		}
	}
	var denied error
	if s.retired || s.ctx.Err() != nil {
		denied = ErrRequestSourceRetired
	} else if len(s.readers) >= s.budgets.PendingReaders {
		denied = ErrRequestSourceBudget
	}
	if denied == nil {
		s.readers[reader] = ack
	}
	s.mu.Unlock()
	for _, old := range completed {
		if e := usenet.JoinReaderCleanup(old); e != nil {
			s.mu.Lock()
			if s.err == nil {
				s.err = e
			}
			s.mu.Unlock()
		}
	}
	if denied != nil {
		if e := usenet.JoinReaderCleanup(reader); e != nil {
			s.mu.Lock()
			if s.err == nil {
				s.err = e
			}
			s.mu.Unlock()
		}
		return denied
	}
	return nil
}

// Request opens do not use legacy path-based existence probes through symlinks.
func (s *RequestSource) pathEntry(ctx context.Context, name string) (bool, *metapb.FileMetadata, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	relative := strings.TrimPrefix(path.Clean("/"+name), "/")
	if relative == "" {
		relative = "."
	}
	root, err := os.OpenRoot(s.fs.remoteFile.metadataService.RootPath())
	if err != nil {
		return false, nil, err
	}
	defer root.Close()
	info, err := root.Lstat(relative)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, nil, os.ErrPermission
		}
		if err = ctx.Err(); err != nil {
			return false, nil, err
		}
		return true, nil, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, nil, err
	}
	value, err := s.fs.remoteFile.metadataService.ReadRequestMetadata(ctx, name, s.budgets.MetadataBytes)
	return false, value, err
}
