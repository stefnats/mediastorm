package handlers

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"novastream/internal/auth"
	"novastream/services/kidsvod"
)

// A restart consumes one private successor reservation, never a new owner.
// The old runtime remains the exact compare-and-swap predecessor until the new
// normal pipe is published or the owned failed setup is cleaned up after join.
type vodHLSRestart struct {
	input                *vodHLSInput
	snapshot             hlsSessionSnapshot
	authority            *vodHLSAuthority
	stopped, transferred bool
}

func validateVODHLSRestartQuery(r *http.Request, owner *vodOwnerContext) bool {
	if r == nil || owner == nil || r.Method != http.MethodPost || len(r.URL.RawQuery) > 16384 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		return false
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(q["time"]) != 1 || q["time"][0] != "0" {
		return false
	}
	explicit := false
	for k, values := range q {
		switch {
		case k == "time":
		case strings.EqualFold(k, "profileId"), strings.EqualFold(k, "userId"):
			for _, v := range values {
				if v != owner.profile.ID || v == "" {
					return false
				}
				explicit = true
			}
		case strings.EqualFold(k, "clientId"):
			for _, v := range values {
				if normalizeClientID(v) != owner.client {
					return false
				}
			}
		case strings.EqualFold(k, "token"), strings.EqualFold(k, "profileUnlock"):
			// Actual authentication and PIN validation below consume these values.
		default:
			return false
		}
	}
	return explicit
}

// Preserve the captured trusted auth values as well as its unchanged expiry.
// A fresh caller unlock can never replace an expired/revoked original receipt.
func (h *VideoHandler) checkOriginalVODHLSPIN(ctx context.Context, owner *vodOwnerContext) error {
	if h == nil || owner == nil || owner.request == nil || owner.issuer != h.vodBoundary || owner.issuer.pin == nil || ctx == nil || ctx.Err() != nil {
		return errVODOwner
	}
	stable, stopStable := vodWorkerContext(owner.request.Context(), owner)
	defer stopStable()
	bounded, cancel := context.WithTimeout(stable, 500*time.Millisecond)
	defer cancel()
	stopRequest := context.AfterFunc(ctx, cancel)
	defer stopRequest()
	if !owner.issuer.pin(&vodDiscardWriter{header: make(http.Header)}, owner.request.Clone(bounded), owner.profile.ID) {
		return errVODOwner
	}
	return owner.recheck(bounded, vodMedia)
}

func (h *VideoHandler) checkVODHLSRestart(r *http.Request, snapshot hlsSessionSnapshot, owner *vodOwnerContext) bool {
	if h == nil || h.vodBoundary == nil || h.hlsManager == nil || !h.hlsManager.hlsInputConfigured() || !snapshot.valid() || !snapshot.current() || owner == nil || owner.issuer != h.vodBoundary || owner.operation != vodMedia || !owner.notApplicable() || owner.state.mode != vodExplicitInactive || owner.ownerState.mode != vodExplicitInactive || owner.profile.ID != snapshot.profile || owner.session.Scope != "" || owner.session.ScopeResource != "" || !validateVODHLSRestartQuery(r, owner) {
		return false
	}
	claim, ok := auth.GetSession(r)
	if !ok || auth.GetAccountID(r) != owner.session.AccountID || claim.Token != owner.session.Token || claim.AccountID != owner.session.AccountID || claim.ClientID != owner.session.ClientID || claim.Scope != "" || claim.ScopeResource != "" || !claim.ExpiresAt.Equal(owner.session.ExpiresAt) || !claim.CreatedAt.Equal(owner.session.CreatedAt) {
		return false
	}
	for _, value := range r.Header.Values("X-Client-ID") {
		if normalizeClientID(value) != owner.client {
			return false
		}
	}
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	if client == "" {
		client = claim.ClientID
	}
	if client != owner.client || captureHLSAuthValues(r, owner) != nil || owner.recheck(r.Context(), vodMedia) != nil || h.vodBoundary.pin == nil {
		return false
	}
	bounded, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	stop := context.AfterFunc(owner.ctx, cancel)
	defer stop()
	current := h.vodBoundary.checkedRequest(r, owner).Clone(bounded)
	if !h.vodBoundary.pin(&vodDiscardWriter{header: make(http.Header)}, current, snapshot.profile) || owner.recheck(bounded, vodMedia) != nil {
		return false
	}
	// Source remains the original private attempt's locator, never a request path
	// or a retrospective metadata/file claim. Existing source ACL stays required.
	sources := []string{snapshot.path, snapshot.original, snapshot.requested}
	seen := map[string]bool{}
	for _, source := range sources {
		if source == "" || seen[source] {
			continue
		}
		seen[source] = true
		if (isRemoteMediaProviderPath(source) || strings.HasPrefix(strings.TrimSpace(source), "localmedia:")) && h.libraryAccess == nil {
			return false
		}
		if !h.requireLibraryStreamAccess(&vodDiscardWriter{header: make(http.Header)}, current, source) || bounded.Err() != nil {
			return false
		}
	}
	return h.checkOriginalVODHLSPIN(bounded, owner) == nil && owner.recheck(bounded, vodMedia) == nil && snapshot.current()
}

func reserveVODHLSRestart(snapshot hlsSessionSnapshot) *vodHLSRestart {
	c := snapshot.input
	if c == nil || c.authority == nil || c.authority.owner != c.owner {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != 1 || !c.published || c.snapshot != snapshot || c.restartUsed || c.restartHeld || c.ctx.Err() != nil || !c.authority.retain() {
		return nil
	}
	c.restartUsed = true
	c.restartHeld = true
	return &vodHLSRestart{input: c, snapshot: snapshot, authority: c.authority}
}
func (a *vodHLSRestart) cleanup() {
	if !a.transferred {
		a.authority.release()
	}
	if !a.stopped {
		a.input.mu.Lock()
		a.input.restartHeld = false
		retired := a.input.phase == 2
		a.input.mu.Unlock()
		if !retired {
			return
		}
		// Original invalidation may have won before our local cancel. If it
		// already joined while held, finish its deferred exact cleanup too.
	}
	cleanup := func() {
		<-a.input.done
		a.input.releaseRetired()
		b := a.input.owner.issuer
		b.mu.Lock()
		if b.active[a.input.raw.key] == a.input.owner {
			delete(b.active, a.input.raw.key)
		}
		b.mu.Unlock()
		if !a.snapshot.manager.cleanupHLSSessionSnapshot(a.snapshot) {
			a.snapshot.manager.removeDetachedHLSInputOutput(a.snapshot)
		}
	}
	select {
	case <-a.input.done:
		cleanup()
	default:
		go cleanup()
	}
}
func (h *VideoHandler) restartVODHLSZero(w http.ResponseWriter, r *http.Request, id string) {
	if h.vodBoundary == nil || h.hlsManager == nil || !h.hlsManager.hlsInputConfigured() {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	snapshot, exists := h.hlsManager.snapshotHLSSession(id)
	if !exists {
		http.NotFound(w, r)
		return
	}
	c := snapshot.input
	if c == nil || c.handler != h || c.manager != h.hlsManager || !h.checkVODHLSRestart(r, snapshot, c.owner) {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	reservation := reserveVODHLSRestart(snapshot)
	if reservation == nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	defer reservation.cleanup()
	// Recheck after winning the reservation but before any touch/cancel/input.
	if !h.checkVODHLSRestart(r, snapshot, c.owner) {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	reservation.stopped = true
	c.retire()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-c.done:
	case <-r.Context().Done():
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	case <-timer.C:
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	// releaseRetired removes the old input slot only after its real pump/process
	// joined. The reserved authority reference prevents a lifetime gap here.
	c.releaseRetired()
	if !h.checkVODHLSRestart(r, snapshot, c.owner) {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	r = h.vodBoundary.checkedRequest(r, c.owner)
	reservation.transferred = true
	successor, ok := h.newVODHLSPipeAttempt(w, r, c.raw.source, c.owner, reservation.authority, snapshot)
	if !ok {
		return
	}
	defer successor.finishSetup()
	c.mu.Lock()
	audio := c.audioIndex
	c.mu.Unlock()
	h.runVODPipeHLSAttempt(w, r, successor, audio)
}
