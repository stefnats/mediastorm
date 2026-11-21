//go:build linux

package nzbfilesystem

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"novastream/internal/encryption"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzb/utils"
	"novastream/internal/pool"
)

var ErrOriginalMetadataUnavailable = errors.New("original metadata source unavailable")

const originalMetadataLimit = 16 << 20
const originalMetadataMemory = 64 << 20
const originalMetadataSegments = 65536

// These actors attest physical source ownership only. They never authorize media.
type OriginalMetadataInfo struct{ Size int64 }
type originalNode struct {
	file    *os.File
	name    string
	version unix.Stat_t
}
type OriginalMetadataNamespace struct {
	*originalMetadataNamespaceState
}
type originalMetadataNamespaceState struct {
	self        *OriginalMetadataNamespace
	fs          *NzbFilesystem
	root        string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          *sync.Mutex
	closed      bool
	pending     *sync.WaitGroup
	nodes       []originalNode
	sources     map[*OriginalMetadataSource]struct{}
	used        int64
	ready, done chan struct{}
	err         error
}
type originalDecoderInputs struct {
	manager                   pool.Manager
	rclone, encryptionHeaders encryption.Cipher
	workers, cacheMB          int
	password, salt            string
}
type OriginalMetadataSource struct{ *originalMetadataSourceState }
type originalMetadataSourceState struct {
	decoder        originalDecoderInputs
	afterRead      func()
	self           *OriginalMetadataSource
	owner          *OriginalMetadataNamespace
	name           string
	ctx            context.Context
	cancel         context.CancelFunc
	expiry         time.Time
	mu             *sync.Mutex
	closed, opened bool
	pending        *sync.WaitGroup
	nodes          []originalNode
	file           *os.File
	version        unix.Stat_t
	digest         [32]byte
	meta           *metapb.FileMetadata
	charge         int64
	stream         *OriginalMetadataStream
	ready, done    chan struct{}
	err            error
	stopNamespace  func()
}
type OriginalMetadataStream struct{ *originalMetadataStreamState }
type originalMetadataStreamState struct {
	readMu    *sync.Mutex
	remaining int64
	self      *OriginalMetadataStream
	source    *OriginalMetadataSource
	raw       *requestFile
	ctx       context.Context
	stop      func()
}

func originalContext(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrOriginalMetadataUnavailable
	}
	c, cancel := context.WithDeadline(ctx, time.Now().Add(limit))
	return c, cancel, nil
}

// The callback only signals. Its actual end is joined before intentional cancel.
func originalLink(ctx context.Context, signal func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); signal() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}
func originalDirectoryEqual(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Mode&unix.S_IFMT == unix.S_IFDIR
}
func originalFileEqual(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim && a.Mode&unix.S_IFMT == unix.S_IFREG
}
func originalStat(f *os.File) (unix.Stat_t, error) {
	var st unix.Stat_t
	if f == nil {
		return st, ErrOriginalMetadataUnavailable
	}
	e := unix.Fstat(int(f.Fd()), &st)
	return st, e
}
func originalOpenAt(parent *os.File, name string, directory bool) (originalNode, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, e := unix.Openat(int(parent.Fd()), name, flags, 0)
	if e != nil {
		return originalNode{}, e
	}
	f := os.NewFile(uintptr(fd), "original metadata descriptor")
	st, e := originalStat(f)
	if e != nil || directory && st.Mode&unix.S_IFMT != unix.S_IFDIR || !directory && st.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = f.Close()
		return originalNode{}, ErrOriginalMetadataUnavailable
	}
	return originalNode{file: f, name: name, version: st}, nil
}
func originalCloseNodes(nodes []originalNode) error {
	var result error
	for i := len(nodes) - 1; i >= 0; i-- {
		result = errors.Join(result, nodes[i].file.Close())
	}
	return result
}
func originalCheckNodes(ctx context.Context, nodes []originalNode, parent *os.File) error {
	for _, node := range nodes {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		held, e := originalStat(node.file)
		if e != nil || !originalDirectoryEqual(held, node.version) {
			return ErrOriginalMetadataUnavailable
		}
		var named unix.Stat_t
		if parent == nil {
			e = unix.Lstat("/", &named)
		} else {
			e = unix.Fstatat(int(parent.Fd()), node.name, &named, unix.AT_SYMLINK_NOFOLLOW)
		}
		if e != nil || !originalDirectoryEqual(named, node.version) {
			return ErrOriginalMetadataUnavailable
		}
		parent = node.file
	}
	return nil
}
func originalPath(name string) bool {
	if name == "" || len(name) > 4096 || !utf8.ValidString(name) || strings.ContainsAny(name, "\\\x00?%#:") || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || !strings.HasSuffix(name, ".meta") {
		return false
	}
	parts := strings.Split(name, "/")
	if len(parts) > 32 || parts[0] == "queue" || parts[0] == "webdav" {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func (fs *NzbFilesystem) CaptureOriginalMetadataNamespace(ctx context.Context) (*OriginalMetadataNamespace, error) {
	if fs == nil || fs.remoteFile == nil || fs.remoteFile.metadataService == nil || fs.remoteFile.configGetter == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrOriginalMetadataUnavailable
	}
	root := fs.remoteFile.metadataService.RootPath()
	if !filepath.IsAbs(root) || root != filepath.Clean(root) || len(root) > 4096 || !utf8.ValidString(root) || strings.ContainsAny(root, "\\\x00") {
		return nil, ErrOriginalMetadataUnavailable
	}
	parts := strings.Split(strings.TrimPrefix(root, "/"), "/")
	if root == "/" || len(parts) > 32 {
		return nil, ErrOriginalMetadataUnavailable
	}
	life, cancel := context.WithCancel(ctx)
	n := &OriginalMetadataNamespace{originalMetadataNamespaceState: &originalMetadataNamespaceState{mu: &sync.Mutex{}, pending: &sync.WaitGroup{}, fs: fs, root: root, ctx: life, cancel: cancel, sources: make(map[*OriginalMetadataSource]struct{}), ready: make(chan struct{}), done: make(chan struct{})}}
	n.self = n
	go n.retire()
	work, finish := context.WithTimeout(life, 10*time.Second)
	unlink := originalLink(work, n.SignalClose)
	var setupErr error
	defer func() {
		unlink()
		finish()
		close(n.ready)
		if setupErr != nil {
			n.SignalClose()
			_ = n.CloseAndJoin()
		}
	}()
	if work.Err() != nil {
		setupErr = ErrOriginalMetadataUnavailable
		return nil, setupErr
	}
	fd, e := unix.Open("/", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if e != nil {
		setupErr = e
		return nil, e
	}
	first := os.NewFile(uintptr(fd), "original namespace anchor")
	st, e := originalStat(first)
	if e != nil {
		_ = first.Close()
		setupErr = e
		return nil, e
	}
	n.nodes = append(n.nodes, originalNode{file: first, version: st})
	for _, part := range parts {
		if work.Err() != nil || part == "" || part == "." || part == ".." {
			setupErr = ErrOriginalMetadataUnavailable
			return nil, setupErr
		}
		node, e := originalOpenAt(n.nodes[len(n.nodes)-1].file, part, true)
		if e != nil {
			setupErr = e
			return nil, e
		}
		n.nodes = append(n.nodes, node)
	}
	if e = originalCheckNodes(work, n.nodes, nil); e != nil {
		setupErr = e
		return nil, e
	}
	return n, nil
}
func (n *OriginalMetadataNamespace) valid() bool {
	return n != nil && n.originalMetadataNamespaceState != nil && n.self == n && n.ctx != nil
}
func (n *OriginalMetadataNamespace) MatchesOriginal(ctx context.Context, fs *NzbFilesystem, root string) error {
	if !n.valid() || ctx == nil || ctx.Err() != nil || fs != n.fs || root != n.root {
		return ErrOriginalMetadataUnavailable
	}
	n.mu.Lock()
	if n.closed || n.ctx.Err() != nil {
		n.mu.Unlock()
		return ErrOriginalMetadataUnavailable
	}
	n.pending.Add(1)
	n.mu.Unlock()
	defer n.pending.Done()
	if e := originalCheckNodes(ctx, n.nodes, nil); e != nil {
		if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			n.SignalClose()
		}
		return e
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if n.ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	return nil
}
func (n *OriginalMetadataNamespace) SignalClose() {
	if !n.valid() {
		return
	}
	n.mu.Lock()
	n.closed = true
	n.cancel()
	sources := make([]*OriginalMetadataSource, 0, len(n.sources))
	for s := range n.sources {
		sources = append(sources, s)
	}
	n.mu.Unlock()
	for _, s := range sources {
		s.SignalClose()
	}
}
func (n *OriginalMetadataNamespace) retire() {
	<-n.ctx.Done()
	n.SignalClose()
	<-n.ready
	n.pending.Wait()
	n.mu.Lock()
	sources := make([]*OriginalMetadataSource, 0, len(n.sources))
	for s := range n.sources {
		sources = append(sources, s)
	}
	n.mu.Unlock()
	for _, s := range sources {
		n.err = errors.Join(n.err, s.CloseAndJoin())
	}
	n.err = errors.Join(n.err, originalCloseNodes(n.nodes))
	close(n.done)
}
func (n *OriginalMetadataNamespace) CloseAndJoin() error {
	if !n.valid() {
		return ErrOriginalMetadataUnavailable
	}
	n.SignalClose()
	<-n.done
	return n.err
}
func (n *OriginalMetadataNamespace) reserve(s *OriginalMetadataSource, bytes int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.ctx.Err() != nil || bytes < 0 || bytes > originalMetadataMemory-n.used {
		return ErrRequestSourceBudget
	}
	n.used += bytes
	s.charge += bytes
	return nil
}
func (n *OriginalMetadataNamespace) Capture(ctx context.Context, name string) (*OriginalMetadataSource, error) {
	if !n.valid() || !originalPath(name) {
		return nil, ErrOriginalMetadataUnavailable
	}
	life, cancel, e := originalContext(ctx, 24*time.Hour)
	if e != nil {
		return nil, e
	}
	if deadline, ok := n.ctx.Deadline(); ok {
		current, _ := life.Deadline()
		if deadline.Before(current) {
			cancel()
			life, cancel = context.WithDeadline(ctx, deadline)
		}
	}
	s := &OriginalMetadataSource{originalMetadataSourceState: &originalMetadataSourceState{mu: &sync.Mutex{}, pending: &sync.WaitGroup{}, owner: n, name: name, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	s.self = s
	s.expiry, _ = life.Deadline()
	// Registration and reservation precede all physical capture/allocation work.
	n.mu.Lock()
	if n.closed || n.ctx.Err() != nil || len(n.sources) >= 32 {
		n.mu.Unlock()
		cancel()
		return nil, ErrOriginalMetadataUnavailable
	}
	n.pending.Add(1)
	n.sources[s] = struct{}{}
	n.mu.Unlock()
	s.stopNamespace = originalLink(n.ctx, s.SignalClose)
	go s.retire()
	work, finish := context.WithTimeout(life, 30*time.Second)
	unlink := originalLink(work, s.SignalClose)
	var captureErr error
	defer func() {
		unlink()
		finish()
		close(s.ready)
		n.pending.Done()
		if captureErr != nil {
			s.SignalClose()
			_ = s.CloseAndJoin()
		}
	}()
	fail := func(e error) (*OriginalMetadataSource, error) { captureErr = e; return nil, e }
	if e := n.MatchesOriginal(work, n.fs, n.root); e != nil {
		return fail(e)
	}
	mrf := n.fs.remoteFile
	cfg := mrf.configGetter()
	if cfg == nil || len(cfg.RClone.Password) > 4096 || len(cfg.RClone.Salt) > 4096 {
		return fail(ErrOriginalMetadataUnavailable)
	}
	s.decoder = originalDecoderInputs{manager: mrf.poolManager, rclone: mrf.rcloneCipher, encryptionHeaders: mrf.aesCipher, workers: cfg.Streaming.MaxDownloadWorkers, cacheMB: cfg.Streaming.MaxCacheSizeMB, password: cfg.RClone.Password, salt: cfg.RClone.Salt}
	parent := n.nodes[len(n.nodes)-1].file
	parts := strings.Split(name, "/")
	for _, part := range parts[:len(parts)-1] {
		if work.Err() != nil {
			return fail(ErrOriginalMetadataUnavailable)
		}
		node, e := originalOpenAt(parent, part, true)
		if e != nil {
			return fail(e)
		}
		s.nodes = append(s.nodes, node)
		parent = node.file
	}
	node, e := originalOpenAt(parent, parts[len(parts)-1], false)
	if e != nil {
		return fail(e)
	}
	s.file = node.file
	s.version = node.version
	r := s.version.Size
	if r < 1 || r > originalMetadataLimit {
		return fail(ErrRequestSourceBudget)
	}
	if e = n.reserve(s, r+16384); e != nil {
		return fail(e)
	}
	raw := make([]byte, int(r))
	if e = originalReadExact(work, s.file, raw); e != nil {
		return fail(e)
	}
	if e = s.checkPhysical(work); e != nil {
		return fail(e)
	}
	s.digest = sha256.Sum256(raw)
	count, e := originalMetadataWire(work, raw)
	if e != nil {
		return fail(e)
	}
	if e = n.reserve(s, 2*r+256*int64(count)); e != nil {
		return fail(e)
	}
	meta := &metapb.FileMetadata{SegmentData: make([]*metapb.SegmentData, 0, count)}
	e = (proto.UnmarshalOptions{Merge: true, DiscardUnknown: true, RecursionLimit: 2, NoLazyDecoding: true}).Unmarshal(raw, meta)
	if e != nil || len(meta.SegmentData) != count || work.Err() != nil || meta.FileSize <= 0 || meta.Status == metapb.FileStatus_FILE_STATUS_CORRUPTED {
		return fail(ErrOriginalMetadataUnavailable)
	}
	// No raw DTO constructor escapes; this sole private tree belongs to this receipt.
	for _, segment := range meta.SegmentData {
		if segment == nil || segment.Id == "" || segment.SegmentSize <= 0 || segment.StartOffset < 0 || segment.EndOffset < segment.StartOffset {
			return fail(ErrOriginalMetadataUnavailable)
		}
	}
	s.meta = meta
	if e = s.checkPhysical(work); e != nil {
		return fail(e)
	}
	return s, nil
}
func originalReadExact(ctx context.Context, f *os.File, data []byte) error {
	for off := 0; off < len(data); {
		if ctx.Err() != nil {
			return ErrOriginalMetadataUnavailable
		}
		end := off + (64 << 10)
		if end > len(data) {
			end = len(data)
		}
		n, e := f.ReadAt(data[off:end], int64(off))
		off += n
		if e != nil || n == 0 {
			return ErrOriginalMetadataUnavailable
		}
	}
	var extra [1]byte
	n, e := f.ReadAt(extra[:], int64(len(data)))
	if n != 0 || e != io.EOF || ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	return nil
}

// Scan actual wire before allocating any decoded tree or segment pointer slice.
func originalMetadataWire(ctx context.Context, b []byte) (int, error) {
	seen := uint16(0)
	count := 0
	for len(b) > 0 {
		if ctx.Err() != nil {
			return 0, ErrOriginalMetadataUnavailable
		}
		num, typ, k := protowire.ConsumeTag(b)
		if k < 0 || num < 1 || num > 9 {
			return 0, ErrOriginalMetadataUnavailable
		}
		b = b[k:]
		if num != 9 {
			bit := uint16(1) << uint(num)
			if seen&bit != 0 {
				return 0, ErrOriginalMetadataUnavailable
			}
			seen |= bit
		}
		if num == 2 || num == 6 || num == 7 || num == 9 {
			if typ != protowire.BytesType {
				return 0, ErrOriginalMetadataUnavailable
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, ErrOriginalMetadataUnavailable
			}
			b = b[n:]
			if num != 9 {
				if len(v) > 4096 || !utf8.Valid(v) {
					return 0, ErrOriginalMetadataUnavailable
				}
				continue
			}
			count++
			if count > originalMetadataSegments {
				return 0, ErrRequestSourceBudget
			}
			seenSegment := uint8(0)
			for len(v) > 0 {
				if ctx.Err() != nil {
					return 0, ErrOriginalMetadataUnavailable
				}
				field, wt, n := protowire.ConsumeTag(v)
				if n < 0 || (field != 1 && field != 3 && field != 4 && field != 5) {
					return 0, ErrOriginalMetadataUnavailable
				}
				v = v[n:]
				bit := uint8(1) << uint(field)
				if seenSegment&bit != 0 {
					return 0, ErrOriginalMetadataUnavailable
				}
				seenSegment |= bit
				if field == 5 {
					if wt != protowire.BytesType {
						return 0, ErrOriginalMetadataUnavailable
					}
					id, n := protowire.ConsumeBytes(v)
					if n < 0 || len(id) == 0 || len(id) > 1024 || !utf8.Valid(id) {
						return 0, ErrOriginalMetadataUnavailable
					}
					for _, part := range id {
						if part == 0 || part == 10 || part == 13 {
							return 0, ErrOriginalMetadataUnavailable
						}
					}
					v = v[n:]
				} else {
					if wt != protowire.VarintType {
						return 0, ErrOriginalMetadataUnavailable
					}
					_, n := protowire.ConsumeVarint(v)
					if n < 0 {
						return 0, ErrOriginalMetadataUnavailable
					}
					v = v[n:]
				}
			}
		} else {
			if typ != protowire.VarintType {
				return 0, ErrOriginalMetadataUnavailable
			}
			value, n := protowire.ConsumeVarint(b)
			if n < 0 || num == 3 && value > 3 || num == 8 && value > 2 {
				return 0, ErrOriginalMetadataUnavailable
			}
			b = b[n:]
		}
	}
	if count == 0 {
		return 0, ErrOriginalMetadataUnavailable
	}
	return count, nil
}
func (s *OriginalMetadataSource) valid() bool {
	return s != nil && s.originalMetadataSourceState != nil && s.self == s && s.owner != nil && s.owner.valid() && s.ctx != nil
}
func (s *OriginalMetadataSource) enter() error {
	if !s.valid() {
		return ErrOriginalMetadataUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	s.pending.Add(1)
	return nil
}
func (s *OriginalMetadataSource) checkPhysical(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.ctx.Err() != nil || s.owner.ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	if e := originalCheckNodes(ctx, s.owner.nodes, nil); e != nil {
		return e
	}
	parent := s.owner.nodes[len(s.owner.nodes)-1].file
	if e := originalCheckNodes(ctx, s.nodes, parent); e != nil {
		return e
	}
	if len(s.nodes) > 0 {
		parent = s.nodes[len(s.nodes)-1].file
	}
	held, e := originalStat(s.file)
	if e != nil || !originalFileEqual(held, s.version) {
		return ErrOriginalMetadataUnavailable
	}
	parts := strings.Split(s.name, "/")
	var named unix.Stat_t
	e = unix.Fstatat(int(parent.Fd()), parts[len(parts)-1], &named, unix.AT_SYMLINK_NOFOLLOW)
	if e != nil || !originalFileEqual(named, s.version) {
		return ErrOriginalMetadataUnavailable
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.ctx.Err() != nil || s.owner.ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	return nil
}
func (s *OriginalMetadataSource) Check(ctx context.Context) error {
	if e := s.enter(); e != nil {
		return e
	}
	defer s.pending.Done()
	e := s.checkPhysical(ctx)
	if e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
		s.SignalClose()
	}
	return e
}
func (s *OriginalMetadataSource) Context() context.Context {
	if !s.valid() {
		c, cancel := context.WithCancel(context.Background())
		cancel()
		return c
	}
	return s.ctx
}
func (s *OriginalMetadataSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil {
		return time.Time{}
	}
	return s.expiry
}
func (s *OriginalMetadataSource) Info(ctx context.Context) (OriginalMetadataInfo, error) {
	if e := s.enter(); e != nil {
		return OriginalMetadataInfo{}, e
	}
	defer s.pending.Done()
	if e := s.checkPhysical(ctx); e != nil {
		if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			s.SignalClose()
		}
		return OriginalMetadataInfo{}, e
	}
	return OriginalMetadataInfo{Size: s.meta.FileSize}, nil
}
func (s *OriginalMetadataSource) Open(ctx context.Context, start, length int64) (*OriginalMetadataStream, error) {
	if e := s.enter(); e != nil {
		return nil, e
	}
	defer s.pending.Done()
	s.mu.Lock()
	if s.opened {
		s.mu.Unlock()
		return nil, ErrOriginalMetadataUnavailable
	}
	s.opened = true
	s.mu.Unlock()
	published := false
	defer func() {
		if !published {
			s.SignalClose()
		}
	}()
	if ctx == nil || ctx.Err() != nil || start < 0 || length <= 0 || start > s.meta.FileSize || length > s.meta.FileSize-start {
		return nil, ErrOriginalMetadataUnavailable
	}
	work, finish, e := originalContext(ctx, 30*time.Second)
	if e != nil {
		return nil, e
	}
	defer finish()
	stop := originalLink(s.ctx, finish)
	defer stop()
	if e = s.checkPhysical(work); e != nil {
		s.SignalClose()
		return nil, e
	}
	// Reuse one exact raw scratch for the second digest. Original ownership charge
	// includes it, and the original tree is never decoded again or retargeted.
	raw := make([]byte, int(s.version.Size))
	if e = originalReadExact(work, s.file, raw); e != nil {
		return nil, e
	}
	if sha256.Sum256(raw) != s.digest {
		s.SignalClose()
		return nil, ErrOriginalMetadataUnavailable
	}
	if e = s.checkPhysical(work); e != nil {
		return nil, e
	}
	life, cancel := context.WithCancel(s.ctx)
	stopCaller := originalLink(ctx, cancel)
	if deadline, ok := ctx.Deadline(); ok {
		cancel()
		stopCaller()
		life, cancel = context.WithDeadline(s.ctx, deadline)
		stopCaller = originalLink(ctx, cancel)
	}
	request, e := s.owner.fs.NewRequestSource(life, RequestSourceBudgets{})
	if e != nil {
		stopCaller()
		cancel()
		return nil, e
	}
	args := utils.NewPathWithArgs(s.name)
	args.SetRange(fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	vmf := &MetadataVirtualFile{name: s.name, fileMeta: s.meta, args: args, poolManager: s.decoder.manager, maxWorkers: s.decoder.workers, maxCacheSizeMB: s.decoder.cacheMB, rcloneCipher: s.decoder.rclone, aesCipher: s.decoder.encryptionHeaders, globalPassword: s.decoder.password, globalSalt: s.decoder.salt, position: start, originalRangeEnd: start + length - 1}
	// Both actual health writers are deliberately absent for this purpose. An
	// old ArticleNotFound cannot update a different same-name metadata/health row.
	attachSourceLifetime(request.ctx, cancel, vmf)
	rawFile := &requestFile{File: vmf, owner: request, ownsSource: true}
	request.mu.Lock()
	denied := request.retired || request.ctx.Err() != nil
	if !denied {
		request.files[rawFile] = struct{}{}
	}
	request.mu.Unlock()
	if denied {
		_ = JoinSourceCleanup(vmf)
		_ = request.CloseAndJoin()
		stopCaller()
		cancel()
		return nil, ErrOriginalMetadataUnavailable
	}
	stopSource := originalLink(request.ctx, s.SignalClose)
	r := &OriginalMetadataStream{originalMetadataStreamState: &originalMetadataStreamState{readMu: &sync.Mutex{}, source: s, raw: rawFile, ctx: request.ctx, remaining: length, stop: func() { stopSource(); stopCaller(); cancel() }}}
	r.self = r
	s.mu.Lock()
	denied = s.closed || s.ctx.Err() != nil
	if !denied {
		s.stream = r
	}
	s.mu.Unlock()
	if denied {
		_ = SignalSourceClose(rawFile)
		_ = JoinSourceCleanup(rawFile)
		r.stop()
		return nil, ErrOriginalMetadataUnavailable
	}
	if e = s.checkPhysical(work); e != nil {
		s.SignalClose()
		return nil, e
	}
	published = true
	return r, nil
}
func (s *OriginalMetadataSource) SignalClose() {
	if !s.valid() {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.cancel()
	r := s.stream
	s.mu.Unlock()
	if r != nil {
		_ = SignalSourceClose(r.raw)
	}
}
func (s *OriginalMetadataSource) retire() {
	<-s.ctx.Done()
	s.SignalClose()
	<-s.ready
	s.pending.Wait()
	s.mu.Lock()
	r := s.stream
	s.mu.Unlock()
	if r != nil {
		s.err = errors.Join(s.err, JoinSourceCleanup(r.raw))
		done, e := SourceCleanupDone(r.raw)
		if e != nil {
			s.err = errors.Join(s.err, e)
			return
		}
		<-done
		r.stop()
		// The concrete actor remains the same, but closed caller-held streams must
		// not retain the charged metadata tree or decoder credentials after refund.
		vmf := r.raw.File.(*MetadataVirtualFile)
		vmf.fileMeta = nil
		vmf.poolManager = nil
		vmf.rcloneCipher = nil
		vmf.aesCipher = nil
		vmf.globalPassword = ""
		vmf.globalSalt = ""
	}
	if s.stopNamespace != nil {
		s.stopNamespace()
	}
	if s.file != nil {
		s.err = errors.Join(s.err, s.file.Close())
	}
	s.err = errors.Join(s.err, originalCloseNodes(s.nodes))
	s.meta = nil
	s.decoder = originalDecoderInputs{}
	n := s.owner
	n.mu.Lock()
	delete(n.sources, s)
	n.used -= s.charge
	n.mu.Unlock()
	close(s.done)
}
func (s *OriginalMetadataSource) CloseAndJoin() error {
	if !s.valid() {
		return ErrOriginalMetadataUnavailable
	}
	s.SignalClose()
	<-s.done
	return s.err
}
func (r *OriginalMetadataStream) valid() bool {
	return r != nil && r.originalMetadataStreamState != nil && r.self == r && r.source != nil && r.source.valid() && r.raw != nil
}
func (r *OriginalMetadataStream) Context() context.Context {
	if !r.valid() {
		return (*OriginalMetadataSource)(nil).Context()
	}
	return r.ctx
}
func (r *OriginalMetadataStream) Check(ctx context.Context) error {
	if !r.valid() || r.ctx.Err() != nil {
		return ErrOriginalMetadataUnavailable
	}
	return r.source.Check(ctx)
}
func (r *OriginalMetadataStream) Read(p []byte) (n int, err error) {
	originalBuffer := p
	var pendingOwner *OriginalMetadataSource
	defer func() {
		if err != nil && err != io.EOF {
			clear(originalBuffer)
			n = 0
		}
		if pendingOwner != nil {
			pendingOwner.pending.Done()
		}
	}()
	if !r.valid() {
		return 0, ErrOriginalMetadataUnavailable
	}
	s := r.source
	if e := s.enter(); e != nil {
		return 0, e
	}
	pendingOwner = s
	if e := s.checkPhysical(r.ctx); e != nil {
		s.SignalClose()
		return 0, e
	}
	r.readMu.Lock()
	if r.remaining == 0 {
		r.readMu.Unlock()
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, e := r.raw.Read(p)
	r.remaining -= int64(n)
	r.readMu.Unlock()
	if s.afterRead != nil {
		s.afterRead()
	}
	if after := s.checkPhysical(r.ctx); after != nil {
		s.SignalClose()
		return 0, after
	}
	if e != nil && e != io.EOF {
		s.SignalClose()
	}
	return n, e
}
func (r *OriginalMetadataStream) Close() error {
	if !r.valid() {
		return ErrOriginalMetadataUnavailable
	}
	return r.source.CloseAndJoin()
}
func (r *OriginalMetadataStream) CleanupDone() <-chan struct{} {
	if !r.valid() {
		return nil
	}
	return r.source.done
}

func (OriginalMetadataNamespace) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataNamespace(opaque)"))
}
func (OriginalMetadataNamespace) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataNamespace) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }

func (OriginalMetadataSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataSource(opaque)"))
}
func (OriginalMetadataSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataSource) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }

func (OriginalMetadataStream) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataStream(opaque)"))
}
func (OriginalMetadataStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataStream) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }
