package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"novastream/internal/nzb/utils"
	"novastream/internal/requestsecurity"
	"novastream/internal/streamheaders"
	"novastream/services/kidsvod"
	"novastream/services/streaming"
)

// Runtime identity is assigned only by normal server publication, never a
// serialized session. It identifies an object, not a source/file/age grant.
type hlsDirectoryClaim struct {
	manager   *HLSManager
	id, path  string
	published *HLSSession
	discarded bool
}

// Unique physical directories isolate replacement IDs even before publication.
// A claim is private and one-shot, never a caller path or serialized grant.
func (m *HLSManager) claimHLSSessionDirectory(id string) (*hlsDirectoryClaim, error) {
	if m == nil || id == "" || len(id) > 128 || filepath.Base(id) != id || strings.ContainsAny(id, "/\\\x00") || strings.TrimSpace(id) != id {
		return nil, errors.New("invalid HLS session directory identity")
	}
	m.sessionLifecycleMu.Lock()
	defer m.sessionLifecycleMu.Unlock()
	path, err := os.MkdirTemp(m.baseDir, id+"-")
	if err != nil {
		return nil, err
	}
	return &hlsDirectoryClaim{manager: m, id: id, path: path}, nil
}

func (m *HLSManager) discardHLSSessionDirectory(claim *hlsDirectoryClaim) {
	if m == nil || claim == nil {
		return
	}
	m.sessionLifecycleMu.Lock()
	if claim.manager != m || claim.published != nil || claim.discarded {
		m.sessionLifecycleMu.Unlock()
		return
	}
	claim.discarded = true
	path := claim.path
	m.sessionLifecycleMu.Unlock()
	_ = os.RemoveAll(path)
}

type hlsSessionIdentity struct {
	manager     *HLSManager
	session     *HLSSession
	id, profile string
	directory   *hlsDirectoryClaim
}
type hlsSessionSnapshot struct {
	manager                                                *HLSManager
	session                                                *HLSSession
	identity                                               *hlsSessionIdentity
	directory                                              *hlsDirectoryClaim
	input                                                  *vodHLSInput
	id, profile, client, path, original, requested, output string
	generation                                             int
	stopped                                                bool
}

func (s hlsSessionSnapshot) String() string               { return "private HLS session snapshot" }
func (s hlsSessionSnapshot) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (s *hlsSessionSnapshot) UnmarshalJSON([]byte) error {
	*s = hlsSessionSnapshot{}
	return errVODOwner
}
func (s hlsSessionSnapshot) valid() bool { return s.validFor(false) }
func (s hlsSessionSnapshot) validFor(cleanup bool) bool {
	return s.profile != "" && s.validRuntime() && (cleanup || !s.stopped)
}
func (s hlsSessionSnapshot) validRuntime() bool {
	return s.manager != nil && s.session != nil && s.identity != nil && s.identity.manager == s.manager && s.identity.session == s.session && s.identity.id == s.id && s.identity.profile == s.profile && s.id != "" && s.generation >= 0 && s.resource() != "" && s.directory != nil && s.identity.directory == s.directory && s.directory.manager == s.manager && s.directory.id == s.id && s.directory.path == s.output && s.directory.published == s.session
}
func snapshotHLSLocked(m *HLSManager, s *HLSSession) hlsSessionSnapshot {
	return hlsSessionSnapshot{manager: m, session: s, identity: s.vodIdentity, input: s.vodInput, directory: s.vodDirectory, id: s.ID, profile: s.ProfileID, client: s.ClientID, path: s.Path, original: s.OriginalPath, requested: s.RequestedSource, output: s.OutputDir, generation: s.SeekGeneration, stopped: s.Stopped}
}

func (m *HLSManager) publishHLSSession(s *HLSSession) bool {
	m.sessionLifecycleMu.Lock()
	defer m.sessionLifecycleMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	claim := s.vodDirectory
	if claim == nil || claim.manager != m || claim.id != s.ID || claim.path != s.OutputDir || claim.published != nil || claim.discarded {
		return false
	}
	s.vodIdentity = &hlsSessionIdentity{manager: m, session: s, id: s.ID, profile: s.ProfileID, directory: claim}
	if s.vodInput != nil {
		if !s.vodInput.bindPublication(s) {
			s.vodIdentity = nil
			return false
		}
	}
	claim.published = s
	m.sessions[s.ID] = s
	return true
}

// An explicit restart replaces only its captured runtime/generation. Directory
// publication and map replacement share the existing lifecycle lock.
func (m *HLSManager) replaceHLSSession(old hlsSessionSnapshot, s *HLSSession) bool {
	if !old.valid() || old.manager != m || s == nil || s.ID != old.id || s.vodInput == nil {
		return false
	}
	m.sessionLifecycleMu.Lock()
	defer m.sessionLifecycleMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.sessions[old.id]
	if !ok || current != old.session {
		return false
	}
	current.mu.RLock()
	same := snapshotHLSLocked(m, current) == old
	current.mu.RUnlock()
	if !same {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	claim := s.vodDirectory
	if claim == nil || claim.manager != m || claim.id != s.ID || claim.path != s.OutputDir || claim.published != nil || claim.discarded || claim == old.directory || s.SeekGeneration != old.generation+1 || s.ProfileID != old.profile || s.ClientID != old.client {
		return false
	}
	s.vodIdentity = &hlsSessionIdentity{manager: m, session: s, id: s.ID, profile: s.ProfileID, directory: claim}
	if !s.vodInput.bindPublication(s) {
		s.vodIdentity = nil
		return false
	}
	claim.published = s
	m.sessions[s.ID] = s
	return true
}

// Pure peek: no GetSession/LastAccess, provider, cache refresh or filesystem.
func (m *HLSManager) snapshotHLSSession(id string) (hlsSessionSnapshot, bool) {
	if m == nil {
		return hlsSessionSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok || s == nil {
		return hlsSessionSnapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return snapshotHLSLocked(m, s), true
}
func (s hlsSessionSnapshot) current() bool {
	if !s.validRuntime() {
		return false
	}
	latest, ok := s.manager.snapshotHLSSession(s.id)
	return ok && latest == s
}
func (s hlsSessionSnapshot) resource() string {
	if s.requested != "" {
		return s.requested
	}
	if s.original != "" {
		return s.original
	}
	return s.path
}

type vodHLSScope struct {
	handler  *VideoHandler
	owner    *vodOwnerContext
	snapshot hlsSessionSnapshot
	cleanup  bool
}
type vodHLSScopeKey struct{}

func (s *vodHLSScope) check(ctx context.Context) error {
	if s == nil || s.handler == nil || s.owner == nil || s.owner.issuer != s.handler.vodBoundary || !s.snapshot.validFor(s.cleanup) || !s.snapshot.current() {
		return errVODOwner
	}
	op := vodMedia
	if s.cleanup {
		op = vodCleanup
	}
	if err := s.owner.recheck(ctx, op); err != nil {
		return err
	}
	if !s.cleanup {
		if s.handler.hlsManager.hlsInputConfigured() && (s.snapshot.input == nil || s.snapshot.input.check(ctx) != nil) {
			return errVODOwner
		}
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		r := s.owner.request.Clone(bounded)
		q := r.URL.Query()
		q.Set("profileId", s.snapshot.profile)
		r.URL.RawQuery = q.Encode()
		seen := map[string]bool{}
		for _, source := range []string{s.snapshot.path, s.snapshot.original, s.snapshot.requested} {
			if source == "" || seen[source] {
				continue
			}
			seen[source] = true
			if (isRemoteMediaProviderPath(source) || strings.HasPrefix(strings.TrimSpace(source), "localmedia:")) && s.handler.libraryAccess == nil {
				return errVODOwner
			}
			if !s.handler.requireLibraryStreamAccess(&vodDiscardWriter{header: make(http.Header)}, r, source) || bounded.Err() != nil {
				return errVODOwner
			}
		}
	}
	if err := s.owner.recheck(ctx, op); err != nil {
		return err
	}
	if !s.snapshot.current() {
		return errVODOwner
	}
	return nil
}

func (h *VideoHandler) beginHLSVOD(w http.ResponseWriter, r *http.Request, id string, cleanup bool) (http.ResponseWriter, *http.Request, func(), bool) {
	noop := func() {}
	if h.hlsManager != nil {
		snapshot, exists := h.hlsManager.snapshotHLSSession(id)
		if exists && snapshot.session.shareOwned != nil {
			return h.beginSharedHLS(w, r, snapshot, cleanup)
		}
		if shareMediaFrom(r.Context()) != nil {
			shareMediaError(w, errVODOwner)
			return w, r, noop, false
		}
	}
	if h.vodBoundary == nil {
		if h.hlsManager != nil && h.hlsManager.hlsInputConfigured() {
			vodDeny(w, kidsvod.CodeContextChanged)
			return w, r, noop, false
		}
		return w, r, noop, true
	}
	if h.hlsManager == nil || !h.hlsManager.hlsInputConfigured() {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return w, r, noop, false
	}
	snapshot, exists := h.hlsManager.snapshotHLSSession(id)
	if !exists {
		http.NotFound(w, r)
		return w, r, noop, false
	}
	if !snapshot.validFor(cleanup) {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return w, r, noop, false
	}
	var selectors []string
	for name, values := range r.URL.Query() {
		if strings.EqualFold(name, "profileId") || strings.EqualFold(name, "userId") {
			selectors = append(selectors, values...)
		}
	}
	op := vodMedia
	if cleanup {
		op = vodCleanup
	}
	owner, ok := h.vodBoundary.requestOwner(w, r, op, snapshot.profile, selectors...)
	if !ok {
		return w, r, noop, false
	}
	if captureHLSAuthValues(r, owner) != nil {
		owner.release()
		vodDeny(w, kidsvod.CodeAccessDenied)
		return w, r, noop, false
	}
	if snapshot.client != "" && owner.client != snapshot.client {
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	key := "hls-http:" + hex.EncodeToString(nonce[:])
	b := h.vodBoundary
	b.mu.Lock()
	if len(b.active) >= 512 {
		b.mu.Unlock()
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	b.active[key] = owner
	b.mu.Unlock()
	unregister := func() {
		b.mu.Lock()
		if b.active[key] == owner {
			delete(b.active, key)
		}
		b.mu.Unlock()
		owner.release()
	}
	scope := &vodHLSScope{handler: h, owner: owner, snapshot: snapshot, cleanup: cleanup}
	ctx, stop := vodWorkerContext(r.Context(), owner)
	ctx = context.WithValue(ctx, vodHLSScopeKey{}, scope)
	r = h.vodBoundary.checkedRequest(r, owner).WithContext(ctx)
	if scope.check(ctx) != nil {
		stop()
		unregister()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	finish := func() { stop(); unregister() }
	if cleanup {
		return w, r, finish, true
	}
	return &vodHLSWriter{ResponseWriter: w, scope: scope, ctx: ctx, baseline: w.Header().Clone()}, r, finish, true
}

func hlsScope(r *http.Request) *vodHLSScope {
	s, _ := r.Context().Value(vodHLSScopeKey{}).(*vodHLSScope)
	return s
}
func checkHLSSessionHTTP(w http.ResponseWriter, r *http.Request, id string) bool {
	if ref := shareHLSRef(r); ref != nil {
		if ref.owner.id != id || ref.check(r.Context()) != nil {
			shareMediaError(w, errVODOwner)
			return false
		}
		return true
	}
	s := hlsScope(r)
	if s == nil {
		return true
	}
	if s.snapshot.id != id || s.check(r.Context()) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return false
	}
	return true
}
func (m *HLSManager) sessionForHTTP(w http.ResponseWriter, r *http.Request, id string) (*HLSSession, bool) {
	if ref := shareHLSRef(r); ref != nil {
		if ref.owner.manager != m || ref.owner.id != id || !checkHLSSessionHTTP(w, r, id) {
			return nil, false
		}
		session := ref.owner.session
		session.mu.Lock()
		session.LastAccess = time.Now()
		session.mu.Unlock()
		return session, true
	}
	if hlsScope(r) == nil {
		return m.GetSession(id)
	}
	if !checkHLSSessionHTTP(w, r, id) {
		return nil, false
	}
	expected := hlsScope(r).snapshot
	m.mu.RLock()
	session, ok := m.sessions[id]
	if !ok || session == nil {
		m.mu.RUnlock()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	session.mu.Lock()
	if snapshotHLSLocked(m, session) != expected {
		session.mu.Unlock()
		m.mu.RUnlock()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	session.LastAccess = time.Now()
	session.mu.Unlock()
	m.mu.RUnlock()
	return session, true
}
func configuredHLSRequest(r *http.Request) bool { return hlsScope(r) != nil }

// Publication is serialized with cleanup so an exact-instance stop cannot
// remove a newly published replacement's files while it owns the same ID.
func (m *HLSManager) cleanupHLSSessionSnapshot(snapshot hlsSessionSnapshot) bool {
	if m == nil || !snapshot.validRuntime() || snapshot.manager != m {
		return false
	}
	m.sessionLifecycleMu.Lock()
	m.mu.Lock()
	session, ok := m.sessions[snapshot.id]
	if !ok || session == nil {
		m.mu.Unlock()
		m.sessionLifecycleMu.Unlock()
		return false
	}
	session.mu.Lock()
	if snapshotHLSLocked(m, session) != snapshot {
		session.mu.Unlock()
		m.mu.Unlock()
		m.sessionLifecycleMu.Unlock()
		return false
	}
	delete(m.sessions, snapshot.id)
	session.Stopped = true
	session.mu.Unlock()
	m.mu.Unlock()
	m.sessionLifecycleMu.Unlock()
	m.cleanupDetachedHLSSession(snapshot.id, session, snapshot.output)
	return true
}

type vodHLSWriter struct {
	http.ResponseWriter
	scope           *vodHLSScope
	ctx             context.Context
	baseline        http.Header
	started, denied bool
}

func (w *vodHLSWriter) check() bool {
	if w.denied {
		return false
	}
	if w.scope.check(w.ctx) == nil {
		return true
	}
	w.denied = true
	if !w.started {
		clear(w.Header())
		for name, values := range w.baseline {
			w.Header()[name] = append([]string(nil), values...)
		}
		vodDeny(w.ResponseWriter, kidsvod.CodeContextChanged)
	}
	return false
}
func (w *vodHLSWriter) WriteHeader(status int) {
	if w.check() {
		w.started = true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *vodHLSWriter) Write(data []byte) (int, error) {
	if !w.check() {
		return 0, errVODOwner
	}
	w.started = true
	return w.ResponseWriter.Write(data)
}
func (w *vodHLSWriter) Flush() {
	if w.check() {
		w.started = true
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// Configured requests recheck before every filesystem retry; legacy callers keep
// the original subtitle wait semantics. This does not start an extractor.
func waitForHLSSubtitleFile(w http.ResponseWriter, r *http.Request, id, path string, maxWait time.Duration) (os.FileInfo, []byte, error) {
	if !configuredHLSRequest(r) && shareHLSRef(r) == nil {
		return waitForSubtitleFile(path, true, maxWait)
	}
	deadline := time.Now().Add(maxWait)
	for {
		if !checkHLSSessionHTTP(w, r, id) {
			return nil, nil, errVODOwner
		}
		stat, err := os.Stat(path)
		if err == nil {
			if !checkHLSSessionHTTP(w, r, id) {
				return nil, nil, errVODOwner
			}
			content, readErr := os.ReadFile(path)
			if readErr == nil && strings.Contains(string(content), "-->") {
				return stat, content, nil
			}
		} else if !os.IsNotExist(err) {
			return nil, nil, err
		}
		if time.Now().After(deadline) {
			return nil, nil, os.ErrNotExist
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-r.Context().Done():
			timer.Stop()
			return nil, nil, r.Context().Err()
		case <-timer.C:
		}
	}
}

// shareHLSOwned is the concrete original scoped source actor. Its DATA never
// supplies a replacement frame, recipient, source permission or renewed expiry.
type shareHLSOwned struct {
	self                                 *shareHLSOwned
	manager                              *HLSManager
	guard                                *shareMediaGuard
	profile, resource, input, id, output string
	session                              *HLSSession
	mu                                   sync.Mutex
	changing, closing                    bool
	run                                  context.Context
	cancel                               context.CancelFunc
	refs                                 map[*shareHLSHTTPRef]struct{}
	workers                              sync.WaitGroup
	setupOnce                            sync.Once
	closeOnce                            sync.Once
	done                                 chan struct{}
	cleanupErr                           error
}
type shareHLSContextKey struct{}
type shareHLSHTTPKey struct{}
type shareHLSHTTPRef struct {
	owner      *shareHLSOwned
	cancel     context.CancelFunc
	generation int
	mutation   bool
	suspended  bool
}

func sharedHLSContext(ctx context.Context) *shareHLSOwned {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(shareHLSContextKey{}).(*shareHLSOwned)
	return value
}
func (m *HLSManager) prepareSharedHLS(ctx context.Context, path, original, profile string) (context.Context, context.CancelFunc, *shareHLSOwned, error) {
	guard := shareMediaFrom(ctx)
	if guard == nil {
		next, cancel := context.WithCancel(context.Background())
		return next, cancel, nil, nil
	}
	source := original
	if source == "" {
		source = path
	}
	if guard.handler == nil || guard.handler.hlsManager != m || guard.current(ctx) != nil ||
		profile != guard.owner.ProfileID() || normalizeShareResource(source) != guard.owner.Resource() ||
		normalizeShareResource(path) != normalizeShareResource(source) {
		return nil, nil, nil, errVODOwner
	}
	fixed, err := guard.transfer(profile, source)
	if err != nil {
		return nil, nil, nil, err
	}
	owned := &shareHLSOwned{manager: m, guard: guard, profile: profile, resource: normalizeShareResource(source), input: path, refs: make(map[*shareHLSHTTPRef]struct{}), done: make(chan struct{})}
	owned.self = owned
	run, cancel := context.WithCancel(fixed)
	owned.run = context.WithValue(run, shareHLSContextKey{}, owned)
	owned.cancel = cancel
	// Setup is a real work reference: publication/cancellation cannot refund a
	// source before its actual setup returns or its worker has registered.
	owned.workers.Add(1)
	return owned.run, cancel, owned, nil
}
func (o *shareHLSOwned) finishSetup(success bool) {
	if o == nil {
		return
	}
	o.setupOnce.Do(func() { o.workers.Done() })
	if !success {
		if o.session != nil {
			o.manager.cleanupShareHLSInstance(o)
		} else {
			o.close()
		}
	}
}
func (o *shareHLSOwned) attach(s *HLSSession) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.session, o.id, o.output = s, s.ID, s.OutputDir
	o.mu.Unlock()
}
func (o *shareHLSOwned) watch() {
	if o == nil {
		return
	}
	context.AfterFunc(o.guard.ctx, func() {
		// Cleanup runs outside every source/HTTP work reference. A stop never
		// waits for itself, and a replaced ID cannot remove a new instance.
		o.manager.cleanupShareHLSInstance(o)
	})
}
func (m *HLSManager) cleanupShareHLSInstance(o *shareHLSOwned) bool {
	if o == nil || o.self != o || o.manager != m {
		return false
	}
	m.sessionLifecycleMu.Lock()
	m.mu.Lock()
	matched := false
	if current := m.sessions[o.id]; current == o.session && current != nil {
		current.mu.RLock()
		same := current.shareOwned == o
		current.mu.RUnlock()
		if same {
			delete(m.sessions, o.id)
			matched = true
		}
	}
	m.mu.Unlock()
	m.sessionLifecycleMu.Unlock()
	_ = o.close()
	return matched && o.cleanupErr == nil
}
func (o *shareHLSOwned) check(ctx context.Context, mutation bool) error {
	if o == nil || o.self != o || ctx == nil || ctx.Err() != nil || o.guard.current(ctx) != nil {
		return errVODOwner
	}
	o.mu.Lock()
	denied := o.closing || o.changing && !mutation
	s, id, output := o.session, o.id, o.output
	o.mu.Unlock()
	if denied || s == nil || id == "" || output == "" {
		return errVODOwner
	}
	o.manager.mu.RLock()
	current := o.manager.sessions[id]
	o.manager.mu.RUnlock()
	s.mu.RLock()
	same := current == s && s.shareOwned == o && s.ID == id && s.OutputDir == output &&
		s.ProfileID == o.profile && normalizeShareResource(s.RequestedSource) == o.resource && !s.Stopped
	s.mu.RUnlock()
	if !same {
		return errVODOwner
	}
	return nil
}
func shareHLSWorkerContext(ctx context.Context, session *HLSSession) (context.Context, context.CancelFunc) {
	if session.shareOwned == nil {
		return context.WithCancel(context.Background())
	}
	o := session.shareOwned
	if sharedHLSContext(ctx) != o {
		failed, cancel := context.WithCancel(o.guard.ctx)
		cancel()
		return failed, cancel
	}
	return context.WithCancel(ctx)
}

// Registration happens before go starts. Every such call is inside the original
// registered setup, HTTP reference or worker; Wait cannot observe a false zero.
func shareHLSGo(session *HLSSession, work func()) {
	if session.shareOwned == nil {
		go work()
		return
	}
	o := session.shareOwned
	o.workers.Add(1)
	go func() { defer o.workers.Done(); work() }()
}
func (o *shareHLSOwned) beginChange(ctx context.Context) (context.Context, context.CancelFunc, func(), error) {
	noop := func() {}
	if o == nil || o.guard.current(ctx) != nil {
		return nil, nil, noop, errVODOwner
	}
	o.mu.Lock()
	if o.closing || o.changing {
		o.mu.Unlock()
		return nil, nil, noop, errVODOwner
	}
	o.changing = true
	cancel := o.cancel
	refs := make([]*shareHLSHTTPRef, 0, len(o.refs))
	for ref := range o.refs {
		refs = append(refs, ref)
	}
	o.mu.Unlock()
	cancel()
	for _, ref := range refs {
		ref.cancel()
	}
	o.workers.Wait()
	if o.guard.current(ctx) != nil {
		o.mu.Lock()
		o.changing = false
		o.mu.Unlock()
		return nil, nil, noop, errVODOwner
	}
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return nil, nil, noop, errVODOwner
	}
	run, stop := context.WithCancel(o.guard.ctx)
	o.run = context.WithValue(run, shareHLSContextKey{}, o)
	o.cancel = stop
	o.workers.Add(1) // actual mutation/setup reference, released on handler return
	next := o.run
	o.mu.Unlock()
	var once sync.Once
	return next, stop, func() { once.Do(func() { o.mu.Lock(); o.changing = false; o.mu.Unlock(); o.workers.Done() }) }, nil
}
func (o *shareHLSOwned) close() error {
	if o == nil {
		return errVODOwner
	}
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closing = true
		cancel := o.cancel
		refs := make([]*shareHLSHTTPRef, 0, len(o.refs))
		for ref := range o.refs {
			refs = append(refs, ref)
		}
		s, output := o.session, o.output
		o.mu.Unlock()
		if s != nil {
			s.mu.Lock()
			s.Stopped = true
			s.mu.Unlock()
		}
		cancel()
		for _, ref := range refs {
			ref.cancel()
		}
		stopped := make(chan struct{})
		go func() { o.guard.stopAndWait(); close(stopped) }()
		o.workers.Wait()
		<-stopped
		// Child Wait, source bodies/readers and real HTTP descriptors have ended
		// before output deletion or the original one-shot lease is released.
		if output != "" {
			if err := os.RemoveAll(output); err != nil {
				// Keep the lease charged on unknown physical output cleanup.
				o.cleanupErr = err
				close(o.done)
				return
			}
		}
		o.guard.Close()
		close(o.done)
	})
	<-o.done
	return o.cleanupErr
}
func (h *VideoHandler) beginSharedHLS(w http.ResponseWriter, r *http.Request, snapshot hlsSessionSnapshot, cleanup bool) (http.ResponseWriter, *http.Request, func(), bool) {
	noop := func() {}
	o := snapshot.session.shareOwned
	if o == nil || o.manager != h.hlsManager || o.guard.handler != h || o.guard.owner.CheckRequest(r, o.profile, o.resource) != nil || o.check(r.Context(), cleanup) != nil {
		shareMediaError(w, errVODOwner)
		return w, r, noop, false
	}
	if cleanup {
		ref := &shareHLSHTTPRef{owner: o, generation: snapshot.generation, mutation: true}
		return w, r.WithContext(context.WithValue(r.Context(), shareHLSHTTPKey{}, ref)), noop, true
	}
	mutation := r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/seek")
	out, request, flushBorrowed, cleanupBorrowed, err := o.guard.borrowHTTP(w, r, o.profile, o.resource)
	if err != nil {
		shareMediaError(w, err)
		return w, r, noop, false
	}
	ctx, cancel := context.WithCancel(request.Context())
	ref := &shareHLSHTTPRef{owner: o, cancel: cancel, generation: snapshot.generation, mutation: mutation}
	o.mu.Lock()
	if o.closing || o.changing {
		o.mu.Unlock()
		// A registration race must intend denial before borrowed cleanup can
		// flush an implicit successful response while the source stays current.
		shareMediaError(out, errVODOwner)
		flushBorrowed()
		cancel()
		cleanupBorrowed()
		return w, r, noop, false
	}
	if !mutation {
		o.workers.Add(1)
		o.refs[ref] = struct{}{}
	}
	o.mu.Unlock()
	request = request.WithContext(context.WithValue(ctx, shareHLSHTTPKey{}, ref))
	transport := http.NewResponseController(out)
	interrupted := make(chan struct{})
	stopIO := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		_ = transport.SetReadDeadline(time.Now())
		_ = transport.SetWriteDeadline(time.Now())
	})
	writer := &shareHLSWriter{ResponseWriter: out, ref: ref, ctx: ctx}
	var once sync.Once
	release := func() {
		once.Do(func() {
			writer.finishHeaders()
			// Both real interruption links remain armed through physical Flush.
			// Disarm this child before the lower cleanup intentionally cancels
			// its parent, so net/http can still send the final response chunk.
			flushBorrowed()
			if !stopIO() {
				<-interrupted
			}
			cancel()
			cleanupBorrowed()
			if !mutation {
				o.mu.Lock()
				_, counted := o.refs[ref]
				delete(o.refs, ref)
				o.mu.Unlock()
				if counted {
					o.workers.Done()
				}
			}
		})
	}
	return writer, request, release, true
}
func shareHLSRef(r *http.Request) *shareHLSHTTPRef {
	ref, _ := r.Context().Value(shareHLSHTTPKey{}).(*shareHLSHTTPRef)
	return ref
}
func (ref *shareHLSHTTPRef) check(ctx context.Context) error {
	if ref == nil || ref.owner.check(ctx, ref.mutation) != nil {
		return errVODOwner
	}
	ref.owner.mu.Lock()
	suspended, generation := ref.suspended, ref.generation
	ref.owner.mu.Unlock()
	if suspended {
		return errVODOwner
	}
	s := ref.owner.session
	s.mu.RLock()
	same := ref.mutation || s.SeekGeneration == generation
	s.mu.RUnlock()
	if !same {
		return errVODOwner
	}
	return nil
}

type shareHLSWriter struct {
	http.ResponseWriter
	ref             *shareHLSHTTPRef
	ctx             context.Context
	started, denied bool
}

func (w *shareHLSWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *shareHLSWriter) deny(err error) {
	if w.denied {
		return
	}
	w.denied = true
	if w.started {
		return
	}
	w.started = true
	for _, header := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Accept-Ranges", "Location"} {
		w.Header().Del(header)
	}
	shareMediaError(w.ResponseWriter, err)
}
func (w *shareHLSWriter) finishHeaders() {
	if !w.started {
		if err := w.ref.check(w.ctx); err != nil {
			w.deny(err)
		}
	}
}
func (w *shareHLSWriter) WriteHeader(status int) {
	if w.denied {
		return
	}
	if err := w.ref.check(w.ctx); err != nil {
		w.deny(err)
		return
	}
	w.started = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *shareHLSWriter) Write(p []byte) (int, error) {
	if w.denied {
		return 0, errVODOwner
	}
	if err := w.ref.check(w.ctx); err != nil {
		w.deny(err)
		return 0, err
	}
	w.started = true
	return w.ResponseWriter.Write(p)
}
func (w *shareHLSWriter) Flush() {
	if w.denied {
		return
	}
	if err := w.ref.check(w.ctx); err != nil {
		w.deny(err)
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

type shareHLSBody struct {
	io.ReadCloser
	stop   func() bool
	joined chan struct{}
	once   sync.Once
	err    error
}

func (b *shareHLSBody) Close() error {
	b.once.Do(func() {
		b.err = b.ReadCloser.Close()
		if !b.stop() {
			<-b.joined
		}
	})
	return b.err
}
func sharedHLSBody(ctx context.Context, s *HLSSession, body io.ReadCloser) (io.ReadCloser, error) {
	if s.shareOwned == nil {
		return body, nil
	}
	o := s.shareOwned
	if sharedHLSContext(ctx) != o || o.check(ctx, false) != nil {
		_ = body.Close()
		return nil, errVODOwner
	}
	wrapped, err := o.guard.wrapBody(ctx, body)
	if err != nil {
		return nil, err
	}
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(joined); _ = wrapped.Close() })
	return &shareHLSBody{ReadCloser: wrapped, stop: stop, joined: joined}, nil
}

func (o *shareHLSOwned) endChange() {
	if o != nil {
		o.mu.Lock()
		o.changing = false
		o.mu.Unlock()
	}
}
func (o *shareHLSOwned) checkSource(ctx context.Context) error {
	if o == nil || o.self != o || sharedHLSContext(ctx) != o || o.guard.current(ctx) != nil {
		return errVODOwner
	}
	o.mu.Lock()
	closing, changing, s := o.closing, o.changing, o.session
	o.mu.Unlock()
	if closing || changing {
		return errVODOwner
	}
	if s != nil {
		return o.check(ctx, false)
	}
	return nil
}
func (o *shareHLSOwned) openInput(ctx context.Context, method, rangeHeader string) (*streaming.Response, error) {
	if o.checkSource(ctx) != nil {
		return nil, errVODOwner
	}
	var response *streaming.Response
	var err error
	providerBacked := false
	if strings.HasPrefix(o.input, "http://") || strings.HasPrefix(o.input, "https://") {
		rawURL, headers := streamheaders.Extract(o.input)
		encodedURL, e := utils.EncodeURLWithSpaces(rawURL)
		if e != nil {
			return nil, e
		}
		request, e := http.NewRequestWithContext(ctx, method, encodedURL, nil)
		if e != nil {
			return nil, e
		}
		streamheaders.Apply(request.Header, headers)
		o.manager.applyExternalUsenetWebDAVAuth(request)
		if rangeHeader != "" {
			request.Header.Set("Range", rangeHeader)
		}
		if o.checkSource(ctx) != nil {
			return nil, errVODOwner
		}
		handler := o.guard.handler
		if handler == nil || handler.externalProxyHTTPClient == nil {
			return nil, errVODOwner
		}
		if e := requestsecurity.ValidateOutboundURL(ctx, request.URL.String(), handler.configuredExternalHostPolicy()); e != nil {
			return nil, e
		}
		// Reuse the actual established safe transport, including its dial-time
		// restricted-address checks. A client copy only adds original ownership;
		// it cannot loosen the configured callback or the ten-hop ceiling.
		client := *handler.externalProxyHTTPClient
		previous := client.CheckRedirect
		client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
			if o.checkSource(next.Context()) != nil || len(via) >= 10 {
				return errVODOwner
			}
			if previous != nil {
				if e := previous(next, via); e != nil {
					return e
				}
			}
			if o.checkSource(next.Context()) != nil {
				return errVODOwner
			}
			return requestsecurity.ValidateOutboundURL(next.Context(), next.URL.String(), handler.configuredExternalHostPolicy())
		}
		if o.checkSource(ctx) != nil {
			return nil, errVODOwner
		}
		result, e := client.Do(request)
		err = e
		if result != nil {
			response = &streaming.Response{Body: result.Body, Headers: result.Header, Status: result.StatusCode, ContentLength: result.ContentLength}
		}
	} else if o.manager.streamer != nil {
		providerBacked = true
		response, err = o.manager.streamer.Stream(ctx, streaming.Request{Path: o.input, Method: method, RangeHeader: rangeHeader})
	} else {
		return nil, errors.New("shared HLS input unavailable")
	}
	if response != nil {
		copy := *response
		copy.Headers = response.Headers.Clone()
		response = &copy
	}
	if response != nil && response.Body != nil {
		var body io.ReadCloser
		var e error
		if providerBacked {
			body, e = o.guard.wrapProviderBody(ctx, response.Body, o.manager.streamer, method, response.Status)
		} else {
			body, e = o.guard.wrapBody(ctx, response.Body)
		}
		if e != nil {
			return nil, e
		}
		joined := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { defer close(joined); _ = body.Close() })
		response.Body = &shareHLSBody{ReadCloser: body, stop: stop, joined: joined}
	}
	if err != nil || o.checkSource(ctx) != nil {
		if response != nil {
			_ = response.Close()
		}
		if err != nil {
			return nil, err
		}
		return nil, errVODOwner
	}
	if response == nil || response.Status != http.StatusOK && response.Status != http.StatusPartialContent {
		if response != nil {
			_ = response.Close()
		}
		return nil, errors.New("shared HLS input unavailable")
	}
	return response, nil
}

type shareHLSPipe struct {
	source  io.ReadCloser
	writer  io.WriteCloser
	reader  io.Reader
	mu      sync.Mutex
	started bool
	done    chan struct{}
	once    sync.Once
	err     error
}

func newShareHLSPipe(cmd *exec.Cmd, source io.ReadCloser, reader io.Reader) (*shareHLSPipe, error) {
	writer, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	return &shareHLSPipe{source: source, reader: reader, writer: writer, done: make(chan struct{})}, nil
}
func (p *shareHLSPipe) start() {
	p.mu.Lock()
	p.started = true
	p.mu.Unlock()
	go func() { defer close(p.done); _, p.err = io.Copy(p.writer, p.reader); _ = p.writer.Close() }()
}
func (p *shareHLSPipe) closeAndJoin() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		_ = p.source.Close()
		_ = p.writer.Close()
		p.mu.Lock()
		started := p.started
		p.mu.Unlock()
		if started {
			<-p.done
		}
	})
}

type shareHLSCapture struct{ bytes.Buffer }

func (b *shareHLSCapture) Write(p []byte) (int, error) {
	if len(p) > 1<<20-b.Len() {
		return 0, errors.New("shared HLS metadata bound exceeded")
	}
	return b.Buffer.Write(p)
}
func (m *HLSManager) probeSharedHLS(ctx context.Context, o *shareHLSOwned) (*UnifiedProbeResult, error) {
	probe, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	source, err := o.openInput(probe, http.MethodGet, "")
	if err != nil {
		return nil, err
	}
	defer source.Close()
	if source.Body == nil {
		return nil, errors.New("shared HLS input body unavailable")
	}
	args := append([]string{"-v", "error", "-probesize", "1000000", "-analyzeduration", "500000"}, vodHLSPipeOptions...)
	args = append(args, "-print_format", "json", "-show_format", "-show_streams", "-i", "pipe:0")
	cmd := exec.CommandContext(probe, m.ffprobePath, args...)
	var output shareHLSCapture
	cmd.Stdout = &output
	pipe, err := newShareHLSPipe(cmd, source.Body, source.Body)
	if err != nil {
		return nil, err
	}
	defer pipe.closeAndJoin()
	if o.checkSource(probe) != nil {
		return nil, errVODOwner
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	pipe.start()
	err = cmd.Wait()
	pipe.closeAndJoin()
	if err != nil || o.checkSource(probe) != nil {
		return nil, errors.New("shared HLS confined probe unavailable")
	}
	result, err := m.parseUnifiedProbeOutput(output.Bytes())
	if err != nil || result == nil || len(result.AudioStreams) > 128 || len(result.SubtitleStreams) > 128 {
		return nil, errors.New("shared HLS metadata unavailable")
	}
	return result, nil
}
func (m *HLSManager) sharedSubtitle(ctx context.Context, s *HLSSession, track int, output string) error {
	o := s.shareOwned
	if o == nil || o.checkSource(ctx) != nil {
		return errVODOwner
	}
	s.mu.RLock()
	probe := s.ProbeData
	offset := s.TranscodingOffset
	if offset <= 0 {
		offset = s.StartOffset
	}
	s.mu.RUnlock()
	if probe == nil {
		return errors.New("shared subtitle inventory unavailable")
	}
	found := false
	for _, item := range probe.SubtitleStreams {
		if item.Index == track && isTextSubtitleCodec(item.Codec) {
			found = true
		}
	}
	if !found {
		return errors.New("shared subtitle track unavailable")
	}
	source, err := o.openInput(ctx, http.MethodGet, "")
	if err != nil {
		return err
	}
	defer source.Close()
	args := append([]string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}, vodHLSPipeOptions...)
	args = append(args, "-i", "pipe:0")
	if offset > 0 {
		args = append(args, "-ss", fmt.Sprintf("%.3f", offset))
	}
	args = append(args, "-map", fmt.Sprintf("0:%d", track), "-c", "webvtt", "-f", "webvtt", "-flush_packets", "1", output)
	cmd := exec.CommandContext(ctx, m.ffmpegPath, args...)
	pipe, err := newShareHLSPipe(cmd, source.Body, source.Body)
	if err != nil {
		return err
	}
	defer pipe.closeAndJoin()
	if err = cmd.Start(); err != nil {
		return err
	}
	pipe.start()
	err = cmd.Wait()
	pipe.closeAndJoin()
	if err != nil || o.checkSource(ctx) != nil {
		return errors.New("shared subtitle extraction unavailable")
	}
	s.setSubtitleExtractionOffset(track, offset)
	return nil
}

// A segment-triggered cast restart releases ONLY its own real file/HTTP work
// reference before joining the old generation. It cannot wait for itself.
func (ref *shareHLSHTTPRef) suspend() error {
	if ref == nil || ref.mutation {
		return errVODOwner
	}
	o := ref.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.refs[ref]; !ok || o.closing || o.changing {
		return errVODOwner
	}
	delete(o.refs, ref)
	ref.suspended = true
	o.workers.Done()
	return nil
}
func (ref *shareHLSHTTPRef) resume(ctx context.Context) error {
	o := ref.owner
	if o.check(ctx, false) != nil {
		return errVODOwner
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing || o.changing || ctx.Err() != nil || !ref.suspended {
		return errVODOwner
	}
	o.session.mu.RLock()
	ref.generation = o.session.SeekGeneration
	o.session.mu.RUnlock()
	ref.suspended = false
	o.refs[ref] = struct{}{}
	o.workers.Add(1)
	return nil
}
