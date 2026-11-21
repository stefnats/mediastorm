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

// A reservation only serializes the same private registry slot. It grants no
// owner and cannot replace the actual original request/current checks.
type vodWorkerBinding struct{ owner *vodOwnerContext }

func (b *vodBoundary) bindWorker(id string, o *vodOwnerContext, original ...*http.Request) bool {
	if o == nil {
		return b == nil
	}
	return b.bindWorkerContext(o.ctx, o.ctx, id, o, original...)
}
func (b *vodBoundary) bindWorkerContext(ctx, lifetime context.Context, id string, o *vodOwnerContext, original ...*http.Request) bool {
	if b == nil {
		return true
	}
	if ctx == nil || lifetime == nil || ctx.Err() != nil || lifetime.Err() != nil || id == "" || o == nil || o.issuer != b {
		return false
	}
	var retired []*vodOwnerContext
	slot := &vodWorkerBinding{owner: o}
	b.mu.Lock()
	for key, old := range b.workers {
		if old.ctx.Err() != nil || !old.expires.After(b.now()) {
			retired = append(retired, old)
			delete(b.workers, key)
		}
	}
	if b.bindings == nil {
		b.bindings = make(map[string]*vodWorkerBinding)
	}
	existing := b.workers[id]
	admitted := existing == nil && b.bindings[id] == nil && len(b.workers)+len(b.bindings) < 512
	if admitted {
		b.bindings[id] = slot
	}
	b.mu.Unlock()
	// Close may wait for actual SQL/HTTP work. Never hold the registry lock.
	for _, old := range retired {
		old.release()
	}
	if !admitted {
		return existing == o
	}
	installed := false
	var worker *vodOwnerContext
	defer func() {
		b.mu.Lock()
		if b.bindings[id] == slot {
			delete(b.bindings, id)
		}
		b.mu.Unlock()
		if !installed && worker != nil {
			worker.release()
		}
	}()
	if o.recheck(ctx, vodDemand) != nil {
		return false
	}
	copy := *o
	parent := lifetime
	if o.original != nil {
		if o.originalRef != nil || len(original) != 1 || original[0] == nil || o.original.Handoff(original[0]) != nil {
			return false
		}
		parent = o.original.Context()
		life := &vodOriginalLifetime{core: o.original, references: 2}
		o.originalRef = &vodOriginalReference{lifetime: life}
		copy.originalRef = &vodOriginalReference{lifetime: life}
	}
	copy.ctx, copy.cancel = context.WithDeadline(parent, o.expires)
	worker = &copy
	if ctx.Err() != nil || lifetime.Err() != nil || copy.ctx.Err() != nil || copy.recheck(ctx, vodDemand) != nil {
		return false
	}
	b.mu.Lock()
	if b.bindings[id] == slot && b.workers[id] == nil && ctx.Err() == nil && lifetime.Err() == nil && copy.ctx.Err() == nil {
		delete(b.bindings, id)
		b.workers[id] = worker
		installed = true
	}
	b.mu.Unlock()
	return installed
}
func (b *vodBoundary) discardBoundWorker(id string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	old := b.workers[id]
	delete(b.workers, id)
	delete(b.bindings, id)
	b.mu.Unlock()
	if old != nil {
		old.release()
	}
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

// Kept for isolated legacy tests. Production always supplies admitted work.
func (h *PrequeueHandler) vodWorker(id, user, client string) (*vodOwnerContext, error) {
	return h.vodWorkerForContext(context.Background(), id, user, client)
}
func (h *PrequeueHandler) vodWorkerForContext(ctx context.Context, id, user, client string) (*vodOwnerContext, error) {
	b := h.vodBoundary
	if b == nil {
		return nil, nil
	}
	if ctx == nil || ctx.Err() != nil {
		return nil, errVODOwner
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
		owner, err = b.backgroundOwner(ctx, entry.UserID, client)
		if err != nil {
			return nil, err
		}
	}
	if owner.profile.ID != entry.UserID || owner.client != normalizeClientID(client) || owner.recheck(ctx, vodDemand) != nil {
		owner.release()
		return nil, errVODOwner
	}
	owner.superseded = &atomic.Bool{}
	owner.workerCurrent = func() bool {
		entry, _, ok := h.store.SnapshotForAuthorization(id)
		return ok && entry.UserID == owner.profile.ID && !entry.ReuseDisabled && !entry.MigrationAdopted
	}
	b.mu.Lock()
	old := b.active[id]
	admitted := ctx.Err() == nil && (old != nil || len(b.active) < 512)
	if admitted {
		if old != nil {
			old.superseded.Store(true)
		}
		b.active[id] = owner
	}
	b.mu.Unlock()
	if !admitted {
		owner.release()
		return nil, errVODOwner
	}
	if old != nil {
		old.release()
	}
	return owner, nil
}

func (h *PrequeueHandler) vodWorkerCheckContext(owner *vodOwnerContext) context.Context {
	if h.currentWork != nil {
		return h.currentWork.ctx
	}
	if owner != nil {
		return owner.ctx
	}
	return nil
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
	owner.release()
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
	if owner == nil || owner.recheck(h.vodWorkerCheckContext(owner), vodDemand) != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || h.vodWorkerCheckContext(owner).Err() != nil {
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
	if owner.recheck(h.vodWorkerCheckContext(owner), vodDemand) != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || h.vodWorkerCheckContext(owner).Err() != nil {
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
	if owner.recheck(h.vodWorkerCheckContext(owner), vodDemand) != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || h.vodWorkerCheckContext(owner).Err() != nil {
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
	if owner == nil || owner.recheck(h.vodWorkerCheckContext(owner), vodDemand) != nil {
		return playback.AuthorizationStamp{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || h.vodWorkerCheckContext(owner).Err() != nil {
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
	if owner == nil || owner.recheck(h.vodWorkerCheckContext(owner), vodDemand) != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[id] != owner || h.vodWorkerCheckContext(owner).Err() != nil {
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
	var retired []*vodOwnerContext
	b.mu.Lock()
	for _, registry := range []map[string]*vodOwnerContext{b.workers, b.active} {
		for _, owner := range registry {
			if owner.profile.ID == profile {
				if owner.superseded != nil {
					owner.superseded.Store(true)
				}
				retired = append(retired, owner)
			}
		}
	}
	for id, slot := range b.bindings {
		if slot.owner.profile.ID == profile {
			delete(b.bindings, id)
		}
	}
	b.mu.Unlock()
	for _, owner := range retired {
		owner.release()
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
