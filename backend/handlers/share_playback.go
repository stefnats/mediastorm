package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/integration"
	"novastream/internal/nzbfilesystem"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
)

type sharePlaybackInstallation struct {
	handler *VideoHandler
	runtime *datastore.ShareLinkRedeemer
	pins    *profileunlock.PGProfilePins
}
type partyPlaybackInstallation struct {
	handler *VideoHandler
	runtime *datastore.WatchRoomAuthority
	pins    *profileunlock.PGProfilePins
}

// This private concrete dispatcher holds exactly one genuine scoped core. It
// supplies no validator callback or public permission interface.
type scopedMediaOwner struct {
	self  *scopedMediaOwner
	share *profileunlock.SharePlaybackOwner
	party *profileunlock.WatchPartyPlaybackOwner
}
type scopedMediaLease struct {
	self  *scopedMediaLease
	share *profileunlock.SharePlaybackSourceLease
	party *profileunlock.WatchPartyPlaybackSourceLease
}

func newSharedMediaOwner(owner *profileunlock.SharePlaybackOwner) *scopedMediaOwner {
	o := &scopedMediaOwner{share: owner}
	o.self = o
	return o
}
func newPartyMediaOwner(owner *profileunlock.WatchPartyPlaybackOwner) *scopedMediaOwner {
	o := &scopedMediaOwner{party: owner}
	o.self = o
	return o
}
func (o *scopedMediaOwner) valid() bool {
	return o != nil && o.self == o && (o.share != nil) != (o.party != nil)
}
func (o *scopedMediaOwner) Context() context.Context {
	if o.valid() {
		if o.share != nil {
			return o.share.Context()
		}
		return o.party.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (o *scopedMediaOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	if o.share != nil {
		return o.share.ExpiresAt()
	}
	return o.party.ExpiresAt()
}
func (o *scopedMediaOwner) ProfileID() string {
	if !o.valid() {
		return ""
	}
	if o.share != nil {
		return o.share.ProfileID()
	}
	return o.party.ProfileID()
}
func (o *scopedMediaOwner) Resource() string {
	if !o.valid() {
		return ""
	}
	if o.share != nil {
		return o.share.Resource()
	}
	return o.party.Resource()
}
func (o *scopedMediaOwner) Check(ctx context.Context) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	if o.share != nil {
		return o.share.Check(ctx)
	}
	return o.party.Check(ctx)
}
func (o *scopedMediaOwner) CheckRequest(r *http.Request, profile, resource string) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	if o.share != nil {
		return o.share.CheckRequest(r, profile, resource)
	}
	return o.party.CheckRequest(r, profile, resource)
}
func (o *scopedMediaOwner) BindResourceRequest(r *http.Request) (*http.Request, error) {
	if !o.valid() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	if o.share != nil {
		return o.share.BindResourceRequest(r)
	}
	return o.party.BindResourceRequest(r)
}
func (o *scopedMediaOwner) Handoff(r *http.Request) (*scopedMediaLease, error) {
	if !o.valid() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	l := &scopedMediaLease{}
	l.self = l
	var e error
	if o.share != nil {
		l.share, e = o.share.Handoff(r)
	} else {
		l.party, e = o.party.Handoff(r)
	}
	if e != nil {
		return nil, e
	}
	return l, nil
}
func (o *scopedMediaOwner) Close() {
	if o.valid() {
		if o.share != nil {
			o.share.Close()
		} else {
			o.party.Close()
		}
	}
}
func (l *scopedMediaLease) Close() {
	if l != nil && l.self == l && (l.share != nil) != (l.party != nil) {
		if l.share != nil {
			l.share.Close()
		} else {
			l.party.Close()
		}
	}
}
func (l *scopedMediaLease) Context() context.Context {
	if l != nil && l.self == l && (l.share != nil) != (l.party != nil) {
		if l.share != nil {
			return l.share.Context()
		}
		return l.party.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type shareMediaKey struct{}

// One actual consumer owns one original source lease. It may transfer that
// SAME lease once to its real async producer, never refresh authentication.
type shareMediaGuard struct {
	handler                 *VideoHandler
	owner                   *scopedMediaOwner
	original                *http.Request
	ctx                     context.Context
	cancel                  context.CancelFunc
	stopOriginal, stopOwner func()
	mu                      sync.Mutex
	lease                   *scopedMediaLease
	transferred, closed     bool
	bodies                  map[*shareMediaBody]struct{}
	ioActive                int
	operations              sync.WaitGroup
	watchDone               chan struct{}
	once                    sync.Once
	stopOnce                sync.Once
	stopDone                chan struct{}
	done                    chan struct{}
}

func (h *VideoHandler) SetManagedSharePlayback(runtime *datastore.ShareLinkRedeemer, pins *profileunlock.PGProfilePins) error {
	h.sharePlayback = &sharePlaybackInstallation{handler: h, runtime: runtime, pins: pins}
	if runtime == nil || pins == nil || !pins.SharePlaybackReady(runtime) {
		return datastore.ErrSharePlaybackUnavailable
	}
	return nil
}
func (h *VideoHandler) SetManagedWatchPartyPlayback(runtime *datastore.WatchRoomAuthority, pins *profileunlock.PGProfilePins) error {
	h.partyPlayback = &partyPlaybackInstallation{handler: h, runtime: runtime, pins: pins}
	if runtime == nil || pins == nil || !pins.WatchPartyPlaybackReady(runtime) {
		return datastore.ErrWatchRoomUnavailable
	}
	return nil
}

func shareMediaFrom(ctx context.Context) *shareMediaGuard {
	if ctx == nil {
		return nil
	}
	g, _ := ctx.Value(shareMediaKey{}).(*shareMediaGuard)
	return g
}
func shareMediaError(w http.ResponseWriter, err error) {
	status := http.StatusForbidden
	if errors.Is(err, datastore.ErrSharePlaybackUnavailable) {
		status = http.StatusServiceUnavailable
	}
	apierror.Write(w, status, "share_playback_unavailable", "Shared playback is unavailable.")
}
func shareInitialPath(path string) bool {
	if path == "/api/video/stream" || strings.HasPrefix(path, "/api/video/stream/") {
		return true
	}
	switch path {
	case "/api/video/metadata", "/api/video/direct-url", "/api/video/cropdetect", "/api/video/thumbnails/start", "/api/video/credits/detect", "/api/video/hls/start", "/api/video/subtitles/tracks", "/api/video/subtitles/start", "/api/live/hls/start", "/api/live/stream":
		return true
	}
	return false
}
func shareSelectedProfile(r *http.Request) (string, error) {
	if r == nil || r.URL == nil {
		return "", datastore.ErrSharePlaybackChanged
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", datastore.ErrSharePlaybackChanged
	}
	selected := ""
	for key, values := range query {
		if !strings.EqualFold(key, "profileId") && !strings.EqualFold(key, "userId") {
			continue
		}
		for _, v := range values {
			if v == "" || v != strings.TrimSpace(v) || len(v) > 256 || selected != "" && selected != v {
				return "", datastore.ErrSharePlaybackChanged
			}
			selected = v
		}
	}
	return selected, err
}

// ManagedSharePlaybackEntry is installed after actual AccountAuth and before
// ownership/body/source middleware. Ordinary full/party routes stay unchanged.
func (h *VideoHandler) ManagedSharePlaybackEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		installed, partyInstalled := h.sharePlayback, h.partyPlayback
		session, ok := auth.GetSession(r)
		if !ok || session.Scope != models.SessionScopeStream || installed == nil && partyInstalled == nil {
			next.ServeHTTP(w, r)
			return
		}
		selected, err := shareSelectedProfile(r)
		if err != nil {
			shareMediaError(w, err)
			return
		}
		var owner *scopedMediaOwner
		if partyInstalled != nil && partyInstalled.pins != nil {
			actual, e := partyInstalled.pins.CaptureWatchPartyPlaybackOwner(r, selected, session.ScopeResource)
			if e == nil && actual != nil {
				if partyInstalled.handler != h || partyInstalled.runtime == nil || !partyInstalled.pins.WatchPartyPlaybackReady(partyInstalled.runtime) {
					actual.Close()
					shareMediaError(w, datastore.ErrSharePlaybackUnavailable)
					return
				}
				owner = newPartyMediaOwner(actual)
			} else if !errors.Is(e, profileunlock.ErrWatchPartyNotOriginal) {
				shareMediaError(w, e)
				return
			}
		}
		if owner == nil {
			if installed == nil || installed.pins == nil {
				shareMediaError(w, datastore.ErrSharePlaybackUnavailable)
				return
			}
			actual, e := installed.pins.CaptureSharePlaybackOwner(r, selected, session.ScopeResource)
			if errors.Is(e, datastore.ErrSharePlaybackParty) {
				if partyInstalled != nil {
					shareMediaError(w, datastore.ErrSharePlaybackUnavailable)
					return
				}
				// Only explicit isolated legacy composition reaches this branch.
				next.ServeHTTP(w, r)
				return
			}
			if e != nil || actual == nil {
				shareMediaError(w, e)
				return
			}
			if installed.handler != h || installed.runtime == nil || !installed.pins.SharePlaybackReady(installed.runtime) {
				actual.Close()
				shareMediaError(w, datastore.ErrSharePlaybackUnavailable)
				return
			}
			owner = newSharedMediaOwner(actual)
		}
		if shareInitialPath(r.URL.Path) && selected == "" {
			owner.Close()
			apierror.Write(w, http.StatusBadRequest, "share_profile_required", "The shared source profile is required.")
			return
		}
		if selected != "" && selected != owner.ProfileID() {
			owner.Close()
			shareMediaError(w, datastore.ErrSharePlaybackChanged)
			return
		}
		original := r
		bounded, err := owner.BindResourceRequest(r)
		if err != nil {
			owner.Close()
			shareMediaError(w, err)
			return
		}
		sourceCtx, cancel := context.WithCancel(owner.Context())
		g := &shareMediaGuard{handler: h, owner: owner, original: original, ctx: sourceCtx, cancel: cancel, bodies: make(map[*shareMediaBody]struct{}), watchDone: make(chan struct{}), done: make(chan struct{}), stopDone: make(chan struct{})}
		g.stopOriginal = shareSignal(original.Context(), cancel)
		g.stopOwner = shareSignal(owner.Context(), cancel)
		go g.watch()
		// Preserve the actual frame and request values. This child only links source
		// cancellation to the current HTTP request; it never replaces authority.
		httpCtx, httpCancel := context.WithCancel(bounded.Context())
		stopHTTP := shareSignal(sourceCtx, httpCancel)
		request := bounded.WithContext(context.WithValue(httpCtx, shareMediaKey{}, g))
		transport := http.NewResponseController(w)
		_ = transport.SetWriteDeadline(owner.ExpiresAt())
		if request.Body != nil && request.Body != http.NoBody {
			deadline := time.Now().Add(30 * time.Second)
			if owner.ExpiresAt().Before(deadline) {
				deadline = owner.ExpiresAt()
			}
			_ = transport.SetReadDeadline(deadline)
		}
		interrupted := make(chan struct{})
		stopIO := context.AfterFunc(httpCtx, func() {
			defer close(interrupted)
			_ = transport.SetReadDeadline(time.Now())
			_ = transport.SetWriteDeadline(time.Now())
		})
		writer := &shareMediaWriter{ResponseWriter: w, guard: g, request: request}
		defer func() {
			writer.finishHeaders()
			_ = transport.Flush()
			if !stopIO() {
				<-interrupted
			}
			stopHTTP()
			httpCancel()
			g.mu.Lock()
			transferred := g.transferred
			g.mu.Unlock()
			if !transferred {
				g.Close()
			}
		}()
		next.ServeHTTP(writer, request)
	})
}
func shareSignal(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancel() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
func (g *shareMediaGuard) watch() {
	defer close(g.watchDone)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-g.ctx.Done():
			g.closeBodies()
			return
		case <-tick.C:
			if g.owner.Check(g.ctx) != nil {
				g.cancel()
			}
		}
	}
}
func (g *shareMediaGuard) current(ctx context.Context) error {
	if g == nil || g.owner == nil || g.ctx.Err() != nil || ctx == nil || ctx.Err() != nil {
		return datastore.ErrSharePlaybackChanged
	}
	return g.owner.Check(ctx)
}
func (g *shareMediaGuard) ensureLease() error {
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil {
		g.mu.Unlock()
		return datastore.ErrSharePlaybackChanged
	}
	if g.lease != nil {
		g.mu.Unlock()
		return nil
	}
	original := g.original
	g.operations.Add(1)
	g.mu.Unlock()
	defer g.operations.Done()
	// Producer setup is serialized by the real caller; two concurrent attempts
	// cannot turn the original core's one-shot handoff into a new permission.
	lease, err := g.owner.Handoff(original)
	if err != nil {
		return err
	}
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil || g.lease != nil {
		g.mu.Unlock()
		lease.Close()
		return datastore.ErrSharePlaybackChanged
	}
	g.lease = lease
	g.mu.Unlock()
	return nil
}
func (g *shareMediaGuard) transfer(profile, resource string) (context.Context, error) {
	if g == nil || profile != g.owner.ProfileID() || normalizeShareResource(resource) != g.owner.Resource() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	if err := g.ensureLease(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.closed || g.transferred || g.ctx.Err() != nil || g.original == nil || g.original.Context().Err() != nil {
		g.mu.Unlock()
		return nil, datastore.ErrSharePlaybackChanged
	}
	g.transferred = true
	stop := g.stopOriginal
	g.stopOriginal = nil
	g.original = nil
	g.mu.Unlock()
	if stop != nil {
		stop()
	}
	if g.ctx.Err() != nil {
		g.mu.Lock()
		g.transferred = false
		g.mu.Unlock()
		return nil, datastore.ErrSharePlaybackChanged
	}
	return context.WithValue(g.ctx, shareMediaKey{}, g), nil
}
func normalizeShareResource(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "webdav/") {
		return "/" + strings.TrimPrefix(s, "webdav/")
	}
	if strings.HasPrefix(s, "/webdav/") {
		return strings.TrimPrefix(s, "/webdav")
	}
	return s
}
func (g *shareMediaGuard) closeBodies() {
	g.mu.Lock()
	bodies := make([]*shareMediaBody, 0, len(g.bodies))
	for b := range g.bodies {
		bodies = append(bodies, b)
	}
	g.mu.Unlock()
	for _, b := range bodies {
		_ = b.Close()
	}
}

// stopAndWait retires actual source IO and borrowed HTTP refs but deliberately
// keeps the lease charged. The async producer must join its real children and
// finish its output/descriptor cleanup before Close completes that lease.
func (g *shareMediaGuard) stopAndWait() {
	if g == nil {
		return
	}
	g.stopOnce.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.cancel()
		stopOriginal, stopOwner := g.stopOriginal, g.stopOwner
		g.mu.Unlock()
		if stopOriginal != nil {
			stopOriginal()
		}
		if stopOwner != nil {
			stopOwner()
		}
		g.closeBodies()
		<-g.watchDone
		g.operations.Wait()
		close(g.stopDone)
	})
	<-g.stopDone
}
func (g *shareMediaGuard) Close() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.stopAndWait()
		g.mu.Lock()
		lease := g.lease
		g.mu.Unlock()
		if lease != nil {
			lease.Close()
		}
		g.owner.Close()
		close(g.done)
	})
	<-g.done
}

// borrowHTTP retains the exact original async source until this real request's
// file/response IO ends. Cleanup/stop calls validate separately without a ref,
// so the command that waits for cleanup cannot accidentally wait for itself.
// The sole HLS caller flushes, disarms its upper IO link, then invokes cleanup.
// These private lifecycle callbacks never supply source permission.
func (g *shareMediaGuard) borrowHTTP(w http.ResponseWriter, r *http.Request, profile, resource string) (http.ResponseWriter, *http.Request, func(), func(), error) {
	noop := func() {}
	if g == nil || profile == "" || resource == "" || g.owner.CheckRequest(r, profile, resource) != nil {
		return w, r, noop, noop, datastore.ErrSharePlaybackChanged
	}
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil || g.owner.party != nil && g.ioActive+len(g.bodies) >= 32 {
		g.mu.Unlock()
		return w, r, noop, noop, datastore.ErrSharePlaybackChanged
	}
	g.ioActive++
	g.operations.Add(1)
	g.mu.Unlock()
	ctx, cancel := context.WithCancel(r.Context())
	stopSource := shareSignal(g.ctx, cancel)
	request := r.WithContext(context.WithValue(ctx, shareMediaKey{}, g))
	transport := http.NewResponseController(w)
	_ = transport.SetWriteDeadline(g.owner.ExpiresAt())
	done := make(chan struct{})
	stopIO := context.AfterFunc(ctx, func() {
		defer close(done)
		_ = transport.SetReadDeadline(time.Now())
		_ = transport.SetWriteDeadline(time.Now())
	})
	var finished sync.Once
	cleanup := func() {
		finished.Do(func() {
			if !stopIO() {
				<-done
			}
			stopSource()
			cancel()
			g.mu.Lock()
			g.ioActive--
			g.mu.Unlock()
			g.operations.Done()
		})
	}
	writer := &shareMediaWriter{ResponseWriter: w, guard: g, request: request}
	// The caller must keep its upper interruption callback armed through Flush,
	// then disarm/join it before this lower cleanup intentionally cancels ctx.
	flush := func() { writer.finishHeaders(); _ = transport.Flush() }
	if ctx.Err() != nil {
		cleanup()
		return w, r, noop, noop, datastore.ErrSharePlaybackChanged
	}
	return writer, request, flush, cleanup, nil
}

type scopedSourceOpenKey struct{}
type scopedSourceOpen struct {
	self                *scopedSourceOpen
	guard               *shareMediaGuard
	transferred, closed bool
}

// The pending slot owns only actual IO lifetime. Permission is still the
// concrete original owner's current check; this context value cannot grant it.
func (g *shareMediaGuard) beginSourceOperation(ctx context.Context) (context.Context, func(), error) {
	noop := func() {}
	if g == nil || ctx == nil || ctx.Err() != nil {
		return ctx, noop, datastore.ErrSharePlaybackChanged
	}
	if e := g.current(ctx); e != nil {
		return ctx, noop, e
	}
	if e := g.ensureLease(); e != nil {
		return ctx, noop, e
	}
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil || g.owner.party != nil && g.ioActive+len(g.bodies) >= 32 {
		g.mu.Unlock()
		return ctx, noop, errors.Join(datastore.ErrSharePlaybackUnavailable, datastore.ErrSharePlaybackBusy)
	}
	op := &scopedSourceOpen{guard: g}
	op.self = op
	g.ioActive++
	g.operations.Add(1)
	g.mu.Unlock()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			g.mu.Lock()
			if !op.transferred {
				g.ioActive--
			}
			op.closed = true
			g.mu.Unlock()
			g.operations.Done()
		})
	}
	return context.WithValue(ctx, scopedSourceOpenKey{}, op), finish, nil
}

func (g *shareMediaGuard) wrapProviderBody(ctx context.Context, body io.ReadCloser, provider streaming.Provider, method string, status int) (io.ReadCloser, error) {
	_, nzb := provider.(*integration.NzbSystem)
	return g.wrapBodyKind(ctx, body, nzb && method != http.MethodHead && status >= 200 && status < 300)
}
func (g *shareMediaGuard) wrapBody(ctx context.Context, body io.ReadCloser) (io.ReadCloser, error) {
	return g.wrapBodyKind(ctx, body, false)
}
func (g *shareMediaGuard) wrapBodyKind(ctx context.Context, body io.ReadCloser, expectedUsenet bool) (io.ReadCloser, error) {
	if body == nil {
		return nil, nil
	}
	ready := nzbfilesystem.RequireSourceCleanup(body)
	genuine := ready == nil || !errors.Is(ready, nzbfilesystem.ErrSourceCleanupUnsupported)
	switch body.(type) {
	case *nzbfilesystem.MetadataVirtualFile, *nzbfilesystem.MetadataVirtualDirectory:
		genuine = true
	}
	b := &shareMediaBody{guard: g, ctx: ctx, body: body, genuineNZB: genuine, expectedNZB: expectedUsenet, done: make(chan struct{})}
	// An opener can return after its original source was retired. Retain and
	// physically join that actual body too; ordinary Close is not an NZB join.
	pending, _ := ctx.Value(scopedSourceOpenKey{}).(*scopedSourceOpen)
	g.mu.Lock()
	reserved := pending != nil && pending.self == pending && pending.guard == g && !pending.closed && !pending.transferred
	retired := g.closed || g.ctx.Err() != nil
	if retired || g.owner.party != nil && !reserved {
		// The actual original pending/HLS producer still owns this late body.
		// Join it before returning to that producer; never Add after Close/Wait.
		g.mu.Unlock()
		_ = b.Close()
		return nil, datastore.ErrSharePlaybackChanged
	}
	if reserved {
		pending.transferred = true
		g.ioActive--
	} else if g.owner.party != nil && g.ioActive+len(g.bodies) >= 32 {
		g.mu.Unlock()
		_ = b.Close()
		return nil, datastore.ErrSharePlaybackBusy
	}
	// Pending -> actual registered body is atomic under the same closed gate.
	// Its charge remains until the genuine body/worker/ref cleanup completes.
	g.bodies[b] = struct{}{}
	g.mu.Unlock()
	if err := g.current(ctx); err != nil {
		_ = b.Close()
		return nil, err
	}
	if err := g.ensureLease(); err != nil {
		_ = b.Close()
		return nil, err
	}
	if ready != nil && (genuine || expectedUsenet) {
		_ = b.Close()
		return nil, ready
	}
	return b, nil
}

type shareMediaBody struct {
	guard                   *shareMediaGuard
	ctx                     context.Context
	body                    io.ReadCloser
	genuineNZB, expectedNZB bool
	mu                      sync.Mutex
	closed                  bool
	reads                   sync.WaitGroup
	once                    sync.Once
	done                    chan struct{}
	err                     error
}

func (b *shareMediaBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, datastore.ErrSharePlaybackChanged
	}
	b.reads.Add(1)
	b.mu.Unlock()
	defer b.reads.Done()
	if err := b.guard.current(b.ctx); err != nil {
		return 0, err
	}
	n, err := b.body.Read(p)
	if changed := b.guard.current(b.ctx); changed != nil {
		clear(p[:n])
		return 0, changed
	}
	return n, err
}
func (b *shareMediaBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		// Probe the concrete close protocol again even when readiness rejected a
		// retired genuine actor. Signal accepts that same initialized actor after
		// Close; a closed genuine file must never become a structural Close fallback.
		signalErr := nzbfilesystem.SignalSourceClose(b.body)
		if signalErr == nil || b.genuineNZB || b.expectedNZB {
			closeErr := b.body.Close()
			b.reads.Wait()
			joinErr := nzbfilesystem.JoinSourceCleanup(b.body)
			b.err = errors.Join(signalErr, closeErr, joinErr)
			if errors.Is(signalErr, nzbfilesystem.ErrSourceCleanupUnsupported) || errors.Is(joinErr, nzbfilesystem.ErrSourceCleanupUnsupported) {
				b.guard.cancel()
				return // no actual end witness: exact lease stays charged
			}
		} else {
			b.err = b.body.Close()
			b.reads.Wait()
		}

		b.guard.mu.Lock()
		delete(b.guard.bodies, b)
		b.guard.mu.Unlock()
		close(b.done)
	})
	<-b.done
	return b.err
}

type shareMediaWriter struct {
	http.ResponseWriter
	guard           *shareMediaGuard
	request         *http.Request
	started, denied bool
}

func (w *shareMediaWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Only a pre-header failure may produce an error response. This response flag
// is never permission; the exact current source check remains the authority.
func (w *shareMediaWriter) deny(err error) {
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
func (w *shareMediaWriter) finishHeaders() {
	if !w.started {
		if err := w.guard.current(w.request.Context()); err != nil {
			w.deny(err)
		}
	}
}
func (w *shareMediaWriter) WriteHeader(status int) {
	if w.denied {
		return
	}
	if err := w.guard.current(w.request.Context()); err != nil {
		w.deny(err)
		return
	}
	w.started = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *shareMediaWriter) Write(data []byte) (int, error) {
	if w.denied {
		return 0, datastore.ErrSharePlaybackChanged
	}
	if err := w.guard.current(w.request.Context()); err != nil {
		w.deny(err)
		return 0, err
	}
	w.started = true
	return w.ResponseWriter.Write(data)
}
func (w *shareMediaWriter) Flush() {
	if w.denied {
		return
	}
	if err := w.guard.current(w.request.Context()); err != nil {
		w.deny(err)
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Secondary legacy managers have no genuine source/child completion actor yet.
// Refuse only actual managed shares before any source or background job exists.
func rejectShareAncillary(w http.ResponseWriter, r *http.Request) bool {
	if shareMediaFrom(r.Context()) == nil {
		return false
	}
	apierror.Write(w, http.StatusNotImplemented, "share_source_unsupported", "This shared playback capability is unavailable.")
	return true
}

func (h *VideoHandler) runShareMetadataProbe(ctx context.Context, source string) (*ffprobeOutput, error) {
	g := shareMediaFrom(ctx)
	if g == nil || g.handler != h || normalizeShareResource(source) != g.owner.Resource() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	var response *streaming.Response
	var err error
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		request, e := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
		if e != nil {
			return nil, e
		}
		request.Header.Set("Range", "bytes=0-16777215")
		result, e := h.vodHTTPDo(h.externalProxyHTTPClient, request)
		if e != nil {
			return nil, e
		}
		response = &streaming.Response{Body: result.Body, Headers: result.Header, Status: result.StatusCode, ContentLength: result.ContentLength}
	} else {
		response, err = h.vodOpenStream(ctx, streaming.Request{Path: source, Method: http.MethodGet, RangeHeader: "bytes=0-16777215"})
		if err != nil {
			return nil, err
		}
	}
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("shared metadata source unavailable")
	}
	defer response.Close()
	sample, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	// Source completion precedes the bounded in-memory child input. cmd.Run joins
	// its actual stdin copier and child; protocol+demux confinement forbids new URLs.
	if err = response.Close(); err != nil {
		return nil, err
	}
	if len(sample) == 0 {
		return nil, fmt.Errorf("shared metadata source empty")
	}
	if err = g.current(ctx); err != nil {
		return nil, err
	}
	return h.runFFProbe(ctx, "pipe:0", bytes.NewReader(sample))
}
