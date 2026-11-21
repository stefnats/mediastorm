package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"novastream/services/kidsvod"
)

var errParentFileHLS = errors.New("parent-approved HLS unavailable")

type ParentFileHLSOptions struct{ Directory, FFmpegPath, FFprobePath string }
type parentHLSNode struct {
	path string
	info os.FileInfo
}
type parentFileHLSManager struct {
	mu            sync.Mutex
	handler       *ParentLocalFileHandler
	options       ParentFileHLSOptions
	current       func(context.Context) (ParentFileHLSOptions, error)
	ctx           context.Context
	cancel        context.CancelFunc
	parent, root  *os.Root
	name          string
	info          os.FileInfo
	nodes         []parentHLSNode
	sessions      map[string]*parentFileHLSSession
	closed        bool
	storageClosed bool
	remove        func(*os.Root, string) error
	once          sync.Once
}
type parentHLSOutputOwner struct {
	cancel context.CancelFunc
	done   chan struct{}
}
type parentFileHLSSession struct {
	mu                     sync.Mutex
	manager                *parentFileHLSManager
	id, profile, item      string
	lease                  *kidsvod.ParentFileProcessLease
	ctx                    context.Context
	cancel                 context.CancelFunc
	root                   *os.Root
	dir                    *os.File
	info                   os.FileInfo
	once                   sync.Once
	closed                 chan struct{}
	outputDone             chan struct{}
	workerDone             chan struct{}
	setupDone, watchDone   chan struct{}
	started, completed     bool
	readers                map[*parentHLSOutputOwner]bool
	segments               map[string]os.FileInfo
	stopManager, stopLease func()
	cleanupPending         bool
	cleanupTimer           *time.Timer
}

func parentHLSID() (string, error) {
	var raw [16]byte
	_, err := rand.Read(raw[:])
	return hex.EncodeToString(raw[:]), err
}
func parentHLSAfterFunc(ctx context.Context, callback func()) func() {
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
func parentHLSNodes(path string) ([]parentHLSNode, error) {
	var out []parentHLSNode
	for {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || len(out) >= 64 {
			return nil, errParentFileHLS
		}
		out = append(out, parentHLSNode{path, info})
		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}
	return out, nil
}
func validParentHLSOptions(o ParentFileHLSOptions) bool {
	return filepath.IsAbs(o.Directory) && filepath.Clean(o.Directory) == o.Directory && o.FFmpegPath != "" && o.FFprobePath != "" && len(o.Directory) <= 4096 && len(o.FFmpegPath) <= 4096 && len(o.FFprobePath) <= 4096
}

// ConfigureHLS is explicit server configuration. Main deliberately does not
// call it. Replacement joins original workers/readers before returning.
func (h *ParentLocalFileHandler) ConfigureHLS(ctx context.Context, options ParentFileHLSOptions, current func(context.Context) (ParentFileHLSOptions, error)) error {
	if runtime.GOOS != "linux" || h == nil || h.core == nil || ctx == nil || ctx.Err() != nil || current == nil || !validParentHLSOptions(options) {
		return errParentFileHLS
	}
	if h.httpRuntime != nil {
		work, finish, err := h.httpRuntime.begin(ctx, true)
		if err != nil {
			return errParentFileHLS
		}
		defer finish()
		ctx = work
	}
	now, err := current(ctx)
	if err != nil || now != options {
		return errParentFileHLS
	}
	nodes, err := parentHLSNodes(options.Directory)
	if err != nil {
		return err
	}
	parent, err := os.OpenRoot(options.Directory)
	if err != nil {
		return errParentFileHLS
	}
	name, err := parentHLSID()
	if err != nil {
		parent.Close()
		return err
	}
	name = ".parent-file-hls-" + name
	if parent.Mkdir(name, 0700) != nil {
		parent.Close()
		return errParentFileHLS
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		_ = parent.Remove(name)
		parent.Close()
		return errParentFileHLS
	}
	info, err := parent.Lstat(name)
	if err != nil {
		root.Close()
		parent.Close()
		return errParentFileHLS
	}
	original := context.Background()
	if h.httpRuntime != nil {
		original = h.httpRuntime.ctx
	}
	life, cancel := context.WithCancel(original)
	m := &parentFileHLSManager{handler: h, options: options, current: current, ctx: life, cancel: cancel, parent: parent, root: root, name: name, info: info, nodes: nodes, sessions: map[string]*parentFileHLSSession{}}
	if h.httpRuntime != nil {
		h.httpRuntime.mu.Lock()
		if h.httpRuntime.closed || h.httpRuntime.ctx.Err() != nil || ctx.Err() != nil {
			h.httpRuntime.mu.Unlock()
			m.close()
			return errParentFileHLS
		}
	}
	h.hlsMu.Lock()
	old := h.hls
	h.hls = m
	h.hlsMu.Unlock()
	if h.httpRuntime != nil {
		h.httpRuntime.mu.Unlock()
	}
	if old != nil {
		old.close()
	}
	if ctx.Err() != nil {
		m.close()
		return errParentFileHLS
	}
	return nil
}
func (h *ParentLocalFileHandler) CloseHLS() {
	if h == nil {
		return
	}
	h.hlsMu.Lock()
	m := h.hls
	h.hls = nil
	h.hlsMu.Unlock()
	if m != nil {
		m.close()
	}
}
func (h *ParentLocalFileHandler) hlsManager() *parentFileHLSManager {
	h.hlsMu.Lock()
	defer h.hlsMu.Unlock()
	return h.hls
}
func (m *parentFileHLSManager) check(ctx context.Context) error {
	if m == nil || ctx == nil || ctx.Err() != nil || m.ctx.Err() != nil || m.handler.hlsManager() != m {
		return errParentFileHLS
	}
	options, err := m.current(ctx)
	if err != nil || options != m.options {
		return errParentFileHLS
	}
	for _, node := range m.nodes {
		info, e := os.Lstat(node.path)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, node.info) {
			return errParentFileHLS
		}
	}
	named, err := m.parent.Lstat(m.name)
	held, e := m.root.Stat(".")
	if err != nil || e != nil || !named.IsDir() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, m.info) || !os.SameFile(held, m.info) || ctx.Err() != nil {
		return errParentFileHLS
	}
	return nil
}
func (m *parentFileHLSManager) signalClose() {
	m.cancel()
	m.mu.Lock()
	m.closed = true
	sessions := make([]*parentFileHLSSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.cancel()
		s.lease.Cancel()
	}
}
func (m *parentFileHLSManager) close() {
	m.once.Do(func() {
		m.signalClose()
		m.mu.Lock()
		var sessions []*parentFileHLSSession
		for _, session := range m.sessions {
			sessions = append(sessions, session)
		}
		m.mu.Unlock()
		for _, session := range sessions {
			session.cancel()
			session.lease.Cancel()
		}
		for _, session := range sessions {
			session.close()
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.finishStorageLocked()
	})
}
func (m *parentFileHLSManager) finishStorageLocked() {
	if !m.closed || len(m.sessions) != 0 || m.storageClosed {
		return
	}
	m.storageClosed = true
	_ = m.root.Close()
	named, err := m.parent.Lstat(m.name)
	if err == nil && os.SameFile(named, m.info) {
		_ = m.parent.Remove(m.name)
	}
	_ = m.parent.Close()
}
func (m *parentFileHLSManager) newSession(ctx context.Context, profile, item string, lease *kidsvod.ParentFileProcessLease) (*parentFileHLSSession, error) {
	if m.check(ctx) != nil {
		return nil, errParentFileHLS
	}
	id, err := parentHLSID()
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(lease.Context())
	s := &parentFileHLSSession{manager: m, id: id, profile: profile, item: item, lease: lease, ctx: life, cancel: cancel, closed: make(chan struct{}), outputDone: make(chan struct{}), workerDone: make(chan struct{}), setupDone: make(chan struct{}), watchDone: make(chan struct{}), readers: map[*parentHLSOutputOwner]bool{}, segments: map[string]os.FileInfo{}}
	if lease.BindOutputRetirement(cancel, s.outputDone) != nil {
		cancel()
		return nil, errParentFileHLS
	}
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil || len(m.sessions) >= 16 {
		m.mu.Unlock()
		close(s.outputDone)
		cancel()
		return nil, errParentFileHLS
	}
	m.sessions[id] = s
	m.mu.Unlock()
	defer close(s.setupDone)
	// Every later exit owns output retirement, including blocked setup.
	s.stopManager = parentHLSAfterFunc(m.ctx, cancel)
	s.stopLease = parentHLSAfterFunc(lease.Context(), cancel)
	go func() { <-s.ctx.Done(); s.close() }()
	go func() {
		defer close(s.watchDone)
		<-s.setupDone
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				if s.check(s.ctx) != nil || s.outputBounds() != nil {
					s.cancel()
					return
				}
			}
		}
	}()
	m.mu.Lock()
	if s.ctx.Err() != nil || m.root.Mkdir(id, 0700) != nil {
		m.mu.Unlock()
		s.cancel()
		return nil, errParentFileHLS
	}
	s.mu.Lock()
	s.info, err = m.root.Lstat(id)
	if err == nil {
		s.root, err = m.root.OpenRoot(id)
	}
	if err == nil {
		s.dir, err = s.root.Open(".")
	}
	s.mu.Unlock()
	m.mu.Unlock()
	if err != nil {
		s.cancel()
		return nil, errParentFileHLS
	}
	if s.check(ctx) != nil {
		s.cancel()
		return nil, errParentFileHLS
	}
	return s, nil
}
func (s *parentFileHLSSession) check(ctx context.Context) error {
	if s == nil || ctx == nil || ctx.Err() != nil || s.ctx.Err() != nil {
		return errParentFileHLS
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if s.manager.check(ctx) != nil || s.lease.Check(ctx) != nil {
		return errParentFileHLS
	}
	s.manager.mu.Lock()
	registered := s.manager.sessions[s.id] == s
	s.manager.mu.Unlock()
	s.mu.Lock()
	root, info := s.root, s.info
	s.mu.Unlock()
	if !registered || root == nil || info == nil {
		return errParentFileHLS
	}
	named, e := s.manager.root.Lstat(s.id)
	held, f := root.Stat(".")
	if e != nil || f != nil || !named.IsDir() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, info) || !os.SameFile(held, info) || s.ctx.Err() != nil || ctx.Err() != nil {
		return errParentFileHLS
	}
	return nil
}
func (s *parentFileHLSSession) close() {
	s.once.Do(func() {
		s.cancel()
		s.lease.Cancel()
		<-s.setupDone
		<-s.watchDone
		s.mu.Lock()
		started := s.started
		var readers []*parentHLSOutputOwner
		for reader := range s.readers {
			readers = append(readers, reader)
		}
		s.mu.Unlock()
		for _, reader := range readers {
			reader.cancel()
		}
		if started {
			<-s.workerDone
		}
		for _, reader := range readers {
			<-reader.done
		}
		// Readers and the child process have actually stopped. Residual private
		// files keep the global reservation, but do not hold the source FD open.
		s.manager.mu.Lock()
		clean := s.cleanupLocked()
		if !clean {
			s.cleanupPending = true
			s.lease.RetainOutputReservation()
		}
		s.manager.mu.Unlock()
		if s.stopManager != nil {
			s.stopManager()
		}
		if s.stopLease != nil {
			s.stopLease()
		}
		close(s.outputDone)
		_ = s.lease.Close()
		close(s.closed)
		if !clean {
			s.scheduleCleanup()
		}
	})
	<-s.closed
}

// cleanupLocked only removes files through the original held private root. A
// replaced canonical directory is never removed. Failure retains charged state.
func (s *parentFileHLSSession) cleanupLocked() bool {
	m := s.manager
	remove := m.remove
	if remove == nil {
		remove = func(root *os.Root, name string) error { return root.Remove(name) }
	}
	// Root/dir pointers are immutable once setupDone closes. os.Root methods
	// safely overlap Close; the original Start stack may still be unwinding.
	root := s.root
	if root == nil && s.info != nil {
		named, err := m.root.Lstat(s.id)
		if err != nil || !os.SameFile(named, s.info) {
			return false
		}
		root, err = m.root.OpenRoot(s.id)
		if err != nil {
			return false
		}
		defer root.Close()
	}
	if root != nil {
		directory, err := root.Open(".")
		if err != nil {
			return false
		}
		ok := true
		for {
			entries, readErr := directory.ReadDir(64)
			for _, entry := range entries {
				if entry.IsDir() || remove(root, entry.Name()) != nil {
					ok = false
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					ok = false
				}
				break
			}
			if len(entries) == 0 {
				break
			}
		}
		_ = directory.Close()
		if !ok {
			return false
		}
	}
	if s.info != nil {
		named, err := m.root.Lstat(s.id)
		if err == nil && os.SameFile(named, s.info) {
			if remove(m.root, s.id) != nil {
				return false
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	if s.root != nil {
		_ = s.root.Close()
	}
	if s.dir != nil {
		_ = s.dir.Close()
	}
	delete(m.sessions, s.id)
	s.cleanupPending = false
	m.finishStorageLocked()
	return true
}
func (s *parentFileHLSSession) scheduleCleanup() {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()
	if s.cleanupPending && s.cleanupTimer == nil {
		s.cleanupTimer = time.AfterFunc(time.Second, s.retryCleanup)
	}
}
func (s *parentFileHLSSession) retryCleanup() {
	m := s.manager
	m.mu.Lock()
	if s.cleanupTimer != nil {
		s.cleanupTimer.Stop()
		s.cleanupTimer = nil
	}
	if !s.cleanupPending {
		m.mu.Unlock()
		return
	}
	clean := s.cleanupLocked()
	if !clean {
		s.cleanupTimer = time.AfterFunc(time.Second, s.retryCleanup)
	}
	m.mu.Unlock()
	if clean {
		s.lease.ReleaseRetainedOutputReservation()
	}
}

// A bounded writer stops untrusted tool JSON/log acquisition before allocation.
type parentHLSCapture struct {
	mu       sync.Mutex
	data     []byte
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

func (b *parentHLSCapture) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) > b.limit-len(b.data) {
		b.overflow = true
		if b.cancel != nil {
			b.cancel()
		}
		return 0, errParentFileHLS
	}
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *parentHLSCapture) bytes() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte{}, b.data...), b.overflow
}

func parentHLSSegmentName(index int) string { return fmt.Sprintf("segment%06d.ts", index) }
func parentHLSSegmentIndex(name string) (int, bool) {
	if len(name) != len("segment000000.ts") || !strings.HasPrefix(name, "segment") || !strings.HasSuffix(name, ".ts") {
		return 0, false
	}
	var n int
	for _, r := range name[7:13] {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, n < 2048
}

var _ io.Writer = (*parentHLSCapture)(nil)
