package datastore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"novastream/models"
)

type FamilyMediaMode uint8

const (
	FamilyMediaUnknown FamilyMediaMode = iota
	FamilyMediaInactive
	FamilyMediaActiveAdult
)

var (
	ErrFamilyMediaUnavailable       = errors.New("current family media authority unavailable")
	ErrFamilyMediaChanged           = errors.New("original family media authority changed")
	ErrFamilyMediaProfileRequired   = errors.New("explicit family media profile required")
	ErrFamilyMediaPINRequired       = errors.New("current adult profile PIN unlock required")
	ErrFamilyMediaChildDenied       = errors.New("generic child media authority unavailable")
	ErrFamilyMediaScopedUnsupported = errors.New("scoped family worker authority unsupported")
	ErrFamilyMediaBusy              = errors.New("family media owner capacity unavailable")
)

const familyMediaCheckBudget = 5 * time.Second

var familyMediaAdmission = struct {
	sync.Mutex
	owners   map[*FamilyMediaOwner]string
	accounts map[string]int
}{owners: map[*FamilyMediaOwner]string{}, accounts: map[string]int{}}

type familyMediaSnapshot struct {
	tuple        profilePinAccessTuple
	outcome      ProfilePinAccessOutcome
	actor, owner RegionalAccountSnapshot
	profile      RegionalProfileSnapshot
	mode         FamilyMediaMode
	expires      time.Time
}

// FamilyMediaOwner is a server-only original full-session lifetime. It is not
// a source, file, classification, scoped token or parent-password permission.
// The runtime owner must Close only AFTER its actual process/source/IO joins.
type FamilyMediaOwner struct {
	*familyMediaOwnerState
}

type familyMediaOwnerState struct {
	mu             sync.Mutex
	self           *FamilyMediaOwner
	store          *DataStore
	original       models.Session
	target         string
	hashes         []string
	snapshot       familyMediaSnapshot
	ctx            context.Context
	cancel         context.CancelFunc
	setupFrame     *pgRequestFrame
	setupStop      func() bool
	setupDone      chan struct{}
	handed, closed bool
	once           sync.Once
}

func (FamilyMediaOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyMediaOwner) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyMediaOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyMediaOwner(opaque)"))
}
func (o *FamilyMediaOwner) valid() bool {
	return o != nil && o.familyMediaOwnerState != nil && o.self == o
}
func (o *FamilyMediaOwner) Context() context.Context {
	if !o.valid() {
		return familyMediaClosedContext()
	}
	return o.ctx
}
func familyMediaClosedContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (o *FamilyMediaOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.snapshot.expires
}
func (o *FamilyMediaOwner) Mode() FamilyMediaMode {
	if !o.valid() || o.ctx == nil || o.ctx.Err() != nil {
		return FamilyMediaUnknown
	}
	return o.snapshot.mode
}
func familyMediaClient(r *http.Request, original models.Session) bool {
	if r == nil || len(r.Header.Values("X-Client-ID")) > 1 {
		return false
	}
	client := r.Header.Get("X-Client-ID")
	return len(client) <= 256 && client == strings.TrimSpace(client) && (client == "" || client == original.ClientID)
}
func sameFamilyMediaSnapshot(a, b familyMediaSnapshot) bool {
	return sameParentModeTuple(a.tuple, b.tuple) && a.outcome == b.outcome && a.actor == b.actor && a.owner == b.owner &&
		a.profile.ProfileID == b.profile.ProfileID && a.profile.AccountID == b.profile.AccountID && a.profile.Revision == b.profile.Revision && a.profile.IsKids == b.profile.IsKids && a.mode == b.mode && a.expires.Equal(b.expires)
}

// Capture reserves bounded admission before any blocking SQL. Only a genuine
// original PG frame can mint this owner; raw auth context cannot replace it.
func (ds *DataStore) CaptureFamilyMediaOwner(r *http.Request, target string, tokens []string) (owner *FamilyMediaOwner, err error) {
	if ds == nil || ds.pool == nil || r == nil || r.Context().Err() != nil || target != "" && !parentMutationID(target) {
		return nil, ErrFamilyMediaUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || frame.ctx == nil || frame.ctx.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	if frame.session.Scope != "" || frame.session.ScopeResource != "" {
		return nil, ErrFamilyMediaScopedUnsupported
	}
	if !familyMediaClient(r, frame.session) {
		return nil, ErrFamilyMediaChanged
	}
	hashes, e := parentModeHashes(tokens)
	if e != nil {
		return nil, ErrFamilyMediaUnavailable
	}
	if target == "" {
		hashes = nil
	}
	expiry := parentMutationMin(frame.session.ExpiresAt, frame.actorExpiry)
	life, cancel := context.WithDeadline(context.Background(), expiry)
	o := &FamilyMediaOwner{familyMediaOwnerState: &familyMediaOwnerState{store: ds, original: frame.session, target: target, hashes: hashes, ctx: life, cancel: cancel, setupFrame: frame, setupDone: make(chan struct{})}}
	o.self = o
	familyMediaAdmission.Lock()
	account := frame.session.AccountID
	if len(familyMediaAdmission.owners) >= 512 || familyMediaAdmission.accounts[account] >= 32 {
		familyMediaAdmission.Unlock()
		cancel()
		return nil, ErrFamilyMediaBusy
	}
	familyMediaAdmission.owners[o] = account
	familyMediaAdmission.accounts[account]++
	familyMediaAdmission.Unlock()
	o.setupStop = context.AfterFunc(r.Context(), func() {
		defer close(o.setupDone)
		o.mu.Lock()
		if !o.handed {
			cancel()
		}
		o.mu.Unlock()
	})
	defer func() {
		if owner == nil {
			o.Close()
		}
	}()
	ctx, stop := context.WithTimeout(r.Context(), familyMediaCheckBudget)
	defer stop()
	if ds.CheckProfilePinRequestFrame(r.WithContext(ctx)) != nil {
		return nil, ErrFamilyMediaChanged
	}
	snapshot, e := ds.readFamilyMediaSnapshot(ctx, o.original, target, hashes)
	if e != nil {
		return nil, e
	}
	if !sameParentMutationRow(frame.sessionRow, snapshot.tuple.session) || !sameParentMutationRow(frame.actorRow, snapshot.tuple.actor) || life.Err() != nil || ctx.Err() != nil || ds.CheckProfilePinRequestFrame(r.WithContext(ctx)) != nil {
		return nil, ErrFamilyMediaChanged
	}
	bounded, finish := context.WithDeadline(life, snapshot.expires)
	o.ctx = bounded
	o.cancel = func() { finish(); cancel() }
	o.snapshot = snapshot
	if bounded.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	return o, nil
}

// Current household readers provide the existing physical global fence. Two
// exact PIN/frame reads bracket household/profile facts, without adopting a
// later tuple or pretending SQL/filesystem operations are globally atomic.
func (ds *DataStore) readFamilyMediaSnapshot(ctx context.Context, original models.Session, target string, hashes []string) (familyMediaSnapshot, error) {
	var out familyMediaSnapshot
	actor, e := ds.CurrentRegionalAccount(ctx, original.AccountID)
	if e != nil {
		return out, ErrFamilyMediaUnavailable
	}
	var deadline time.Time
	read := func() (profilePinAccessTuple, ProfilePinAccessOutcome, time.Time, error) {
		if target != "" {
			return readProfilePinAccess(ctx, ds.pool, original, target, hashes, true)
		}
		frame, _, e := readPGRequestFrame(ctx, ds.pool, original.Token)
		if e != nil || frame.session != original {
			return profilePinAccessTuple{}, ProfilePinUnavailable, time.Time{}, ErrFamilyMediaChanged
		}
		return profilePinAccessTuple{frameCurrent: true, session: frame.sessionRow, actor: frame.actorRow}, ProfilePinUnavailable, parentMutationMin(original.ExpiresAt, frame.actorExpiry), nil
	}
	out.tuple, out.outcome, deadline, e = read()
	if e != nil {
		return out, ErrFamilyMediaChanged
	}
	out.actor = actor
	out.owner = actor
	out.mode = FamilyMediaInactive
	if target != "" {
		if out.outcome != ProfilePinNoPin && out.outcome != ProfilePinLocked && out.outcome != ProfilePinUnlocked {
			return out, ErrFamilyMediaChanged
		}
		out.profile, e = ds.CurrentRegionalProfile(ctx, target)
		if e != nil || out.profile.ProfileID != target {
			return out, ErrFamilyMediaUnavailable
		}
		if out.profile.AccountID != original.AccountID {
			out.owner, e = ds.CurrentRegionalAccount(ctx, out.profile.AccountID)
			if e != nil {
				return out, ErrFamilyMediaUnavailable
			}
		}
		if out.profile.IsKids {
			return out, ErrFamilyMediaChildDenied
		}
	}
	if actor.HasChildren || out.owner.HasChildren {
		if target == "" {
			return out, ErrFamilyMediaProfileRequired
		}
		if out.outcome != ProfilePinUnlocked {
			return out, ErrFamilyMediaPINRequired
		}
		out.mode = FamilyMediaActiveAdult
	}
	latest, outcome, expiry, e := read()
	if e != nil || !sameParentModeTuple(out.tuple, latest) || out.outcome != outcome || !expiry.Equal(deadline) {
		return out, ErrFamilyMediaChanged
	}
	after, e := ds.CurrentRegionalAccount(ctx, original.AccountID)
	if e != nil || after != actor {
		return out, ErrFamilyMediaChanged
	}
	if out.owner.AccountID != actor.AccountID {
		owner, e := ds.CurrentRegionalAccount(ctx, out.owner.AccountID)
		if e != nil || owner != out.owner {
			return out, ErrFamilyMediaChanged
		}
	}
	if ctx.Err() != nil || !deadline.After(time.Now()) {
		return out, ErrFamilyMediaChanged
	}
	out.expires = deadline
	return out, nil
}

// Check reuses only captured original hashes/outcome/rows/expiry. It signals
// denial but never synchronously joins an IO worker from its own watcher.
func (o *FamilyMediaOwner) Check(ctx context.Context) error {
	if !o.valid() || o.store == nil || ctx == nil || ctx.Err() != nil || o.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return ErrFamilyMediaChanged
	}
	bounded, cancel := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer cancel()
	stop := familyMediaAfterFunc(o.ctx, cancel)
	defer stop()
	current, e := o.store.readFamilyMediaSnapshot(bounded, o.original, o.target, o.hashes)
	if e != nil || !sameFamilyMediaSnapshot(o.snapshot, current) || bounded.Err() != nil || o.ctx.Err() != nil {
		// A departed consumer is not a change to the original worker proof.
		// Current proof/own-budget failures still retire that original owner.
		if ctx.Err() == nil {
			o.cancel()
		}
		return ErrFamilyMediaChanged
	}
	return nil
}

// CheckRequest requires a fresh genuine PG frame under the original exact
// full session/client/target. The facade additionally checks fresh active PIN
// presentation; background Check never fabricates an authenticated request.
func (o *FamilyMediaOwner) CheckRequest(r *http.Request, target string) error {
	if !o.valid() || r == nil || target != o.target || !familyMediaClient(r, o.original) {
		return ErrFamilyMediaChanged
	}
	if o.store.CheckProfilePinRequestFrame(r) != nil {
		return ErrFamilyMediaChanged
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.session != o.original || !sameParentMutationRow(frame.sessionRow, o.snapshot.tuple.session) || !sameParentMutationRow(frame.actorRow, o.snapshot.tuple.actor) {
		return ErrFamilyMediaChanged
	}
	return o.Check(r.Context())
}
func (o *FamilyMediaOwner) Handoff(r *http.Request) error {
	if !o.valid() || r == nil || o.CheckRequest(r, o.target) != nil {
		return ErrFamilyMediaChanged
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.handed || o.setupStop == nil || frame != o.setupFrame {
		return ErrFamilyMediaChanged
	}
	stopped := o.setupStop()
	// Own a successful stop immediately, including later rejected handoff.
	if stopped {
		o.setupStop = nil
		close(o.setupDone)
		o.setupFrame = nil
	}
	if !stopped || r.Context().Err() != nil || o.ctx.Err() != nil {
		o.cancel()
		return ErrFamilyMediaChanged
	}
	o.handed = true
	return nil
}
func familyMediaAfterFunc(ctx context.Context, callback func()) func() {
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
func (o *FamilyMediaOwner) Close() {
	if !o.valid() {
		return
	}
	o.once.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.cancel()
		stop := o.setupStop
		o.setupStop = nil
		o.setupFrame = nil
		o.mu.Unlock()
		if stop != nil {
			if stop() {
				close(o.setupDone)
			}
			<-o.setupDone
		}
		familyMediaAdmission.Lock()
		account, ok := familyMediaAdmission.owners[o]
		if ok {
			delete(familyMediaAdmission.owners, o)
			familyMediaAdmission.accounts[account]--
			if familyMediaAdmission.accounts[account] == 0 {
				delete(familyMediaAdmission.accounts, account)
			}
		}
		familyMediaAdmission.Unlock()
	})
}
