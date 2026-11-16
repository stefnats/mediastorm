package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"novastream/services/kidsvod"
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
	return hlsSessionSnapshot{manager: m, session: s, identity: s.vodIdentity, directory: s.vodDirectory, id: s.ID, profile: s.ProfileID, client: s.ClientID, path: s.Path, original: s.OriginalPath, requested: s.RequestedSource, output: s.OutputDir, generation: s.SeekGeneration, stopped: s.Stopped}
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
	claim.published = s
	s.vodIdentity = &hlsSessionIdentity{manager: m, session: s, id: s.ID, profile: s.ProfileID, directory: claim}
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
	if h.vodBoundary == nil {
		return w, r, noop, true
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
	if !configuredHLSRequest(r) {
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
