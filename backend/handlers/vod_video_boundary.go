package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"novastream/models"
	"novastream/services/kidsvod"
)

// This raw-video port is deliberately not installed in Main. It correlates an
// original owner; it cannot issue a regional grant or prove a catalogue/file.
type vodRawOwner struct {
	handler *VideoHandler
	owner   *vodOwnerContext
	source  string
	key     string
}
type vodRawOwnerKey struct{}

func (h *VideoHandler) beginVideoVOD(w http.ResponseWriter, r *http.Request, source string) (http.ResponseWriter, *http.Request, func(), bool) {
	noop := func() {}
	if h.vodBoundary == nil {
		return w, r, noop, true
	}
	var selectors []string
	for key, values := range r.URL.Query() {
		if strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId") {
			selectors = append(selectors, values...)
		}
	}
	owner, ok := h.vodBoundary.requestOwner(w, r, vodMedia, "", selectors...)
	if !ok {
		return w, r, noop, false
	}
	// A disabled regional policy does not widen an existing scoped token.
	// Only the already resource-bound stream scope can preserve legacy access.
	if owner.session.Scope != "" {
		normalize := func(value string) string {
			value = strings.TrimSpace(value)
			if strings.HasPrefix(value, "/webdav/") {
				return strings.TrimPrefix(value, "/webdav")
			}
			if strings.HasPrefix(value, "webdav/") {
				return "/" + strings.TrimPrefix(value, "webdav/")
			}
			return value
		}
		if owner.session.Scope != models.SessionScopeStream || normalize(owner.session.ScopeResource) == "" || normalize(owner.session.ScopeResource) != normalize(source) {
			owner.release()
			vodDeny(w, kidsvod.CodeAccessDenied)
			return w, r, noop, false
		}
	}
	// Disabled-policy legacy recording calls may omit their selector. Capture
	// that actual repository owner now, before any input, so later ownership
	// changes cannot relabel an already-open recording under another profile.
	if strings.HasPrefix(strings.TrimSpace(source), "recording:") && owner.profile.ID == "" {
		if h.recordingAccess == nil || h.recordingAccess.owners == nil {
			owner.release()
			vodDeny(w, kidsvod.CodeUnsupportedSource)
			return w, r, noop, false
		}
		stored, err := h.recordingAccess.owners.SnapshotStreamOwner(r.Context(), source)
		if err != nil {
			owner.release()
			vodDeny(w, kidsvod.CodeAccessDenied)
			return w, r, noop, false
		}
		owner.release()
		owner, ok = h.vodBoundary.requestOwner(w, r, vodMedia, stored.ProfileID, selectors...)
		if !ok {
			return w, r, noop, false
		}
	}
	if !owner.notApplicable() {
		owner.release()
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return w, r, noop, false
	}
	for _, client := range r.URL.Query()["clientId"] {
		if !h.vodBoundary.selectClient(w, owner, client) {
			owner.release()
			return w, r, noop, false
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	raw := &vodRawOwner{handler: h, owner: owner, source: source, key: "raw-video:" + hex.EncodeToString(nonce[:])}
	b := h.vodBoundary
	b.mu.Lock()
	if len(b.active) >= 512 {
		b.mu.Unlock()
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return w, r, noop, false
	}
	b.active[raw.key] = owner
	b.mu.Unlock()
	finish := func() {
		b.mu.Lock()
		if b.active[raw.key] == owner {
			delete(b.active, raw.key)
		}
		b.mu.Unlock()
		owner.release()
	}
	ctx, stop := vodWorkerContext(r.Context(), owner)
	ctx = context.WithValue(ctx, vodRawOwnerKey{}, raw)
	request := b.checkedRequest(r, owner).WithContext(ctx)
	if owner.profile.ID != "" {
		q := request.URL.Query()
		q.Set("profileId", owner.profile.ID)
		request.URL.RawQuery = q.Encode()
	}
	writer := &vodRawWriter{ResponseWriter: w, handler: h, ctx: ctx, baseline: w.Header().Clone()}
	return writer, request, func() { stop(); finish() }, true
}

// Shared helper callers not yet wired to this port remain unsupported when
// configured; a missing original context must never mean disabled policy.
// A stale registration is local to a private input attempt. Public raw video
// retains its existing fail-closed owner-release behavior through vodRawCheck.
var errVODRawStaleRegistration = errors.New("stale private raw registration")

func (h *VideoHandler) vodRawCheck(ctx context.Context) error {
	err := h.checkVODRawOwner(ctx)
	if err != nil {
		raw, ok := ctx.Value(vodRawOwnerKey{}).(*vodRawOwner)
		if ok && raw != nil && raw.handler == h && raw.owner != nil && raw.owner.issuer == h.vodBoundary && raw.owner.notApplicable() {
			raw.owner.release()
		}
	}
	if errors.Is(err, errVODRawStaleRegistration) {
		return errVODOwner
	}
	return err
}

// checkVODRawOwner is shared with HLS attempts. It never retires an owner:
// callers distinguish stale attempt registration from real authority failure.
func (h *VideoHandler) checkVODRawOwner(ctx context.Context) error {
	if h.vodBoundary == nil {
		return nil
	}
	raw, ok := ctx.Value(vodRawOwnerKey{}).(*vodRawOwner)
	if !ok || raw == nil || raw.handler != h || raw.owner == nil || raw.owner.issuer != h.vodBoundary || !raw.owner.notApplicable() {
		return errVODOwner
	}
	b := h.vodBoundary
	b.mu.Lock()
	current := b.active[raw.key] == raw.owner
	b.mu.Unlock()
	if !current {
		return errVODRawStaleRegistration
	}
	if err := raw.owner.recheck(ctx, vodMedia); err != nil {
		return err
	}
	if isRemoteMediaProviderPath(raw.source) || strings.HasPrefix(strings.TrimSpace(raw.source), "localmedia:") {
		if h.libraryAccess == nil {
			return errVODOwner
		}
	}
	// Main supplies repository-only library item readers. This is current ACL,
	// not fresh item reassignment, provider transport identity or file proof.
	sourceCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	request := raw.owner.request.Clone(sourceCtx)
	q := request.URL.Query()
	if raw.owner.profile.ID != "" {
		q.Set("profileId", raw.owner.profile.ID)
	}
	request.URL.RawQuery = q.Encode()
	if !h.requireLibraryStreamAccess(&vodDiscardWriter{header: make(http.Header)}, request, raw.source) || sourceCtx.Err() != nil {
		return errVODOwner
	}
	// Current ACL lookup may block or invoke a repository callback. Do not
	// publish its result under an original owner that changed while it ran.
	if err := raw.owner.recheck(ctx, vodMedia); err != nil {
		return err
	}
	return nil
}

type vodRawWriter struct {
	http.ResponseWriter
	handler  *VideoHandler
	ctx      context.Context
	baseline http.Header
	started  bool
	denied   bool
}

func (w *vodRawWriter) check() bool {
	if w.denied {
		return false
	}
	if w.handler.vodRawCheck(w.ctx) == nil {
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
func (w *vodRawWriter) WriteHeader(status int) {
	if w.check() {
		w.started = true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *vodRawWriter) Write(data []byte) (int, error) {
	if !w.check() {
		return 0, errVODOwner
	}
	w.started = true
	return w.ResponseWriter.Write(data)
}
func (w *vodRawWriter) Flush() {
	if w.check() {
		w.started = true
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}
