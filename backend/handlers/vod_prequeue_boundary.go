package handlers

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"novastream/internal/auth"
	"novastream/services/kidsvod"
	"novastream/services/playback"
)

func (h *PrequeueHandler) vodEntry(w http.ResponseWriter, r *http.Request, id string, op vodOperation) (*playback.PrequeueEntry, *vodOwnerContext, bool) {
	if h.vodBoundary == nil {
		return nil, nil, true
	}
	entry, _, ok := h.store.SnapshotForAuthorization(id)
	if !ok {
		http.NotFound(w, r)
		return nil, nil, false
	}
	owner, ok := h.vodBoundary.requestOwner(w, r, op, entry.UserID)
	return entry, owner, ok
}
func (b *vodBoundary) bindWorker(id string, o *vodOwnerContext) bool {
	if b == nil {
		return true
	}
	if id == "" || o == nil || o.issuer != b || o.recheck(context.Background(), vodDemand) != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, old := range b.workers {
		if old.ctx.Err() != nil || !old.expires.After(b.now()) {
			old.cancel()
			delete(b.workers, key)
		}
	}
	if existing := b.workers[id]; existing != nil {
		return existing == o
	}
	if len(b.workers) >= 512 {
		return false
	}
	copy := *o
	copy.ctx, copy.cancel = context.WithDeadline(context.Background(), o.expires)
	b.workers[id] = &copy
	return true
}
func (o *vodOwnerContext) checkBackground(ctx context.Context, state vodAccountApplicability) error {
	if state.mode != vodExplicitInactive || o.profile.ID == "" {
		return errVODOwner
	}
	profile, err := o.issuer.identity.CurrentProfile(ctx, o.profile.ID)
	if err != nil || profile != o.profile || profile.AccountID != o.session.AccountID {
		return errVODOwner
	}
	return nil
}
func (b *vodBoundary) backgroundOwner(ctx context.Context, profileID, client string) (*vodOwnerContext, error) {
	if b == nil {
		return nil, nil
	}
	if b.identity == nil || ctx == nil || ctx.Err() != nil {
		return nil, errVODOwner
	}
	profile, err := b.identity.CurrentProfile(ctx, profileID)
	if err != nil {
		return nil, errVODOwner
	}
	state, err := b.state(ctx, profile.AccountID)
	if err != nil || state.mode != vodExplicitInactive {
		return nil, errVODOwner
	}
	expires := b.now().Add(15 * time.Minute)
	ownerCtx, cancel := context.WithDeadline(context.Background(), expires)
	o := &vodOwnerContext{issuer: b, operation: vodDemand, disposition: vodNotApplicable, state: state, ownerState: state, profile: profile, client: client, expires: expires, ctx: ownerCtx, cancel: cancel, background: true}
	o.session.AccountID = profile.AccountID
	if err = o.recheck(ctx, vodDemand); err != nil {
		cancel()
		return nil, err
	}
	return o, nil
}
func (h *PrequeueHandler) vodWorker(id, user, client string) (*vodOwnerContext, error) {
	b := h.vodBoundary
	if b == nil {
		return nil, nil
	}
	entry, _, ok := h.store.SnapshotForAuthorization(id)
	if !ok || entry.UserID != user {
		return nil, errVODOwner
	}
	b.mu.Lock()
	owner := b.workers[id]
	delete(b.workers, id)
	b.mu.Unlock()
	if owner == nil {
		var err error
		owner, err = b.backgroundOwner(context.Background(), entry.UserID, client)
		if err != nil {
			return nil, err
		}
	}
	if owner.profile.ID != entry.UserID || owner.client != normalizeClientID(client) || owner.recheck(context.Background(), vodDemand) != nil {
		owner.release()
		return nil, errVODOwner
	}
	owner.superseded = &atomic.Bool{}
	owner.workerCurrent = func() bool {
		entry, _, ok := h.store.SnapshotForAuthorization(id)
		return ok && entry.UserID == owner.profile.ID && !entry.ReuseDisabled && !entry.MigrationAdopted
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if old := b.active[id]; old != nil {
		old.superseded.Store(true)
		old.release()
	}
	if len(b.active) >= 512 && b.active[id] == nil {
		owner.release()
		return nil, errVODOwner
	}
	b.active[id] = owner
	return owner, nil
}
func (h *PrequeueHandler) finishVODWorker(id string, owner *vodOwnerContext) {
	if h.vodBoundary == nil || owner == nil {
		return
	}
	b := h.vodBoundary
	b.mu.Lock()
	if b.workers[id] == owner {
		delete(b.workers, id)
	}
	if b.active[id] == owner {
		delete(b.active, id)
	}
	b.mu.Unlock()
	owner.cancel()
}
func (h *PrequeueHandler) rejectVODWorker(id, user string, owners ...*vodOwnerContext) {
	if h.vodBoundary == nil {
		return
	}
	if len(owners) == 0 || owners[0] == nil {
		return
	}
	owner := owners[0]
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner {
		return
	}
	entry, _, ok := h.store.SnapshotForAuthorization(id)
	if !ok || entry.UserID != user {
		return
	}
	if entry.Status == playback.PrequeueStatusReady {
		return
	}
	h.store.UpdateWorker(id, func(e *playback.PrequeueEntry) {
		e.Status = playback.PrequeueStatusFailed
		e.Error = kidsvod.CodeContextChanged
		e.StreamPath = ""
		e.HLSPlaylistURL = ""
		e.HLSSessionID = ""
		e.MediaEvidence = nil
		e.MediaEvidenceConfigurationKey = ""
	})
}

func (h *PrequeueHandler) publishVODReady(id string, owner *vodOwnerContext, update func(*playback.PrequeueEntry)) bool {
	if h.vodBoundary == nil {
		return h.store.UpdateWorker(id, update)
	}
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if owner == nil || b.active[id] != owner || owner.recheck(context.Background(), vodDemand) != nil {
		return false
	}
	return h.store.UpdateWorker(id, update)
}

// A worker uses a private handler copy so its callbacks/goroutines retain the
// original owner. Looking up the latest owner by ID would let a late callback
// accidentally acquire its successor's authority.
func (h *PrequeueHandler) updateVODWorker(id string, update func(*playback.PrequeueEntry)) bool {
	owner := h.vodCurrentWorker
	if owner == nil || h.vodBoundary == nil {
		return h.store.UpdateWorker(id, update)
	}
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || owner.recheck(context.Background(), vodDemand) != nil {
		return false
	}
	return h.store.UpdateWorker(id, update)
}
func (h *PrequeueHandler) recordVODPreparationEvidence(id, client string, probe *VideoFullResult, configuration string) {
	owner := h.vodCurrentWorker
	if owner == nil || h.vodBoundary == nil {
		h.recordPreparationEvidence(id, client, probe, configuration)
		return
	}
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || owner.recheck(context.Background(), vodDemand) != nil {
		return
	}
	h.recordPreparationEvidence(id, client, probe, configuration)
}

func (h *PrequeueHandler) beginVODAttempt(id string, owner *vodOwnerContext) (playback.AuthorizationStamp, bool) {
	if h.vodBoundary == nil {
		stamp, ok := h.store.BeginAuthorizationAttempt(id)
		return stamp, ok
	}
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if owner == nil || b.active[id] != owner || owner.recheck(context.Background(), vodDemand) != nil {
		return playback.AuthorizationStamp{}, false
	}
	stamp, ok := h.store.BeginAuthorizationAttempt(id)
	if !ok {
		return stamp, false
	}
	previous := owner.workerCurrent
	owner.workerCurrent = func() bool {
		_, current, ok := h.store.SnapshotForAuthorization(id)
		return ok && current == stamp && (previous == nil || previous())
	}
	return stamp, true
}
func (h *PrequeueHandler) setVODCancel(id string, owner *vodOwnerContext, cancel context.CancelFunc) bool {
	if h.vodBoundary == nil {
		h.store.SetCancelFunc(id, cancel)
		return true
	}
	b := h.vodBoundary
	b.mu.Lock()
	defer b.mu.Unlock()
	if owner == nil || b.active[id] != owner || owner.recheck(context.Background(), vodDemand) != nil {
		return false
	}
	h.store.SetCancelFunc(id, cancel)
	return true
}

// Future protected mutators must call this trusted cancellation seam after
// changing current identity/policy prerequisites. It is not publicly wired.
func (b *vodBoundary) invalidateProfile(profile string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, registry := range []map[string]*vodOwnerContext{b.workers, b.active} {
		for _, owner := range registry {
			if owner.profile.ID == profile {
				if owner.superseded != nil {
					owner.superseded.Store(true)
				}
				owner.release()
			}
		}
	}
}

type vodWorkerKey struct{}

func vodWorkerCheck(ctx context.Context) error {
	if owner, ok := ctx.Value(vodWorkerKey{}).(*vodOwnerContext); ok {
		return owner.recheck(ctx, owner.operation)
	}
	return nil
}
func vodWorkerContext(parent context.Context, owner *vodOwnerContext) (context.Context, context.CancelFunc) {
	if owner == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithDeadline(parent, owner.expires)
	stop := context.AfterFunc(owner.ctx, cancel)
	if owner.ctx.Err() != nil {
		cancel()
	}
	return context.WithValue(ctx, vodWorkerKey{}, owner), func() { stop(); cancel() }
}

// Cleanup uses only current ownership/PIN and the immutable stored owner. It
// never validates the source or opens/stat's a library file merely to stop it.
func (h *PrequeueHandler) vodCleanupEntry(w http.ResponseWriter, r *http.Request, id string) (*playback.PrequeueEntry, bool) {
	entry, owner, ok := h.vodEntry(w, r, id, vodCleanup)
	defer owner.release()
	if !ok {
		return nil, false
	}
	if h.vodBoundary == nil {
		return nil, true
	}
	if !h.vodBoundary.recheckHTTP(w, r, owner, vodCleanup) {
		return nil, false
	}
	if session, ok := auth.GetSession(r); !ok || session.Scope != "" {
		vodDeny(w, kidsvod.CodeAccessDenied)
		return nil, false
	}
	return entry, true
}
