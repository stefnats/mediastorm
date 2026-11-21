package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
)

const sharePlaybackCheckBudget = 2 * time.Second
const sharePlaybackOwnerBytes = 16 << 20

type sharePlaybackSnapshot struct {
	session                                                                            models.Session
	sessionRow, actorRow                                                               parentMutationRow
	binding, link, creator, creatorInc, profile, profileInc, owner, ownerInc, resource string
	creatorHouse, ownerHouse                                                           string
	global                                                                             int64
	profileGeneration                                                                  int64
	libraryReceipt                                                                     string
	expires                                                                            time.Time
}

func shareSnapshotSame(a, b sharePlaybackSnapshot) bool {
	return a.session == b.session && sameParentMutationRow(a.sessionRow, b.sessionRow) && sameParentMutationRow(a.actorRow, b.actorRow) && a.binding == b.binding && a.link == b.link && a.creator == b.creator && a.creatorInc == b.creatorInc && a.profile == b.profile && a.profileInc == b.profileInc && a.owner == b.owner && a.ownerInc == b.ownerInc && a.resource == b.resource && a.creatorHouse == b.creatorHouse && a.ownerHouse == b.ownerHouse && a.global == b.global && a.profileGeneration == b.profileGeneration && a.libraryReceipt == b.libraryReceipt && a.expires.Equal(b.expires)
}

// A SharePlaybackOwner is an original scoped source lifetime. It is never a
// full family-media/PIN/master permission, and its DATA cannot make a frame.
type SharePlaybackOwner struct {
	self                   *SharePlaybackOwner
	runtime                *ShareLinkRedeemer
	original               models.Session
	snapshot               sharePlaybackSnapshot
	policy                 *config.SecurityPolicyObservation
	ctx                    context.Context
	cancel                 context.CancelFunc
	mu                     sync.Mutex
	operations             sync.WaitGroup
	closed, handed         bool
	setupFrame             *pgRequestFrame
	setupRequest           *http.Request
	stopSetup, stopRuntime func()
	lease                  *SharePlaybackSourceLease
	bytes                  int
	once                   sync.Once
	done                   chan struct{}
}

// A lease closes only after its genuine consumer's body, child and descriptor
// have ended. It cannot mint a token, renew a deadline or replace a frame.
type SharePlaybackSourceLease struct {
	self  *SharePlaybackSourceLease
	owner *SharePlaybackOwner
	once  sync.Once
	done  chan struct{}
}

func (SharePlaybackOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SharePlaybackOwner(opaque)"))
}
func (SharePlaybackOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SharePlaybackOwner) UnmarshalJSON([]byte) error  { return ErrSharePlaybackChanged }
func (SharePlaybackSourceLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SharePlaybackSourceLease(opaque)"))
}
func (SharePlaybackSourceLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SharePlaybackSourceLease) UnmarshalJSON([]byte) error  { return ErrSharePlaybackChanged }
func (o *SharePlaybackOwner) valid() bool {
	return o != nil && o.self == o && o.runtime != nil && o.ctx != nil
}
func (o *SharePlaybackOwner) Context() context.Context {
	if !o.valid() {
		return familyMediaClosedContext()
	}
	return o.ctx
}
func (o *SharePlaybackOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.original.ExpiresAt
}

// ProfileID and Resource are descriptive correlation DATA only. Every consumer
// must still CheckRequest against the actual original token/private binding.
func (o *SharePlaybackOwner) ProfileID() string {
	if !o.valid() {
		return ""
	}
	return o.snapshot.profile
}
func (o *SharePlaybackOwner) Resource() string {
	if !o.valid() {
		return ""
	}
	return o.snapshot.resource
}
func (l *SharePlaybackSourceLease) Context() context.Context {
	if l == nil || l.self != l || l.owner == nil {
		return familyMediaClosedContext()
	}
	return l.owner.Context()
}
func (l *SharePlaybackSourceLease) Close() {
	if l != nil && l.self == l {
		l.once.Do(func() { close(l.done) })
	}
}
func (o *SharePlaybackOwner) enter() bool {
	if !o.valid() {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.ctx.Err() != nil || !o.runtime.usable() {
		return false
	}
	o.operations.Add(1)
	return true
}
func (o *SharePlaybackOwner) Close() {
	if !o.valid() {
		return
	}
	o.once.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.cancel()
		lease := o.lease
		stopSetup, stopRuntime := o.stopSetup, o.stopRuntime
		o.mu.Unlock()
		if stopSetup != nil {
			stopSetup()
		}
		if stopRuntime != nil {
			stopRuntime()
		}
		o.operations.Wait()
		if lease != nil {
			<-lease.done
		}
		o.runtime.mu.Lock()
		delete(o.runtime.owners, o)
		o.runtime.mu.Unlock()
		close(o.done)
	})
	<-o.done
}
func shareOriginalFrame(c *ShareLinkRedeemer, r *http.Request) (*pgRequestFrame, error) {
	if !c.usable() || r == nil || r.URL == nil || r.Context().Err() != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != c.store || frame.ctx == nil || frame.ctx.Err() != nil {
		return nil, ErrSharePlaybackChanged
	}
	supplied, ok := auth.GetSession(r)
	if !ok || supplied != effectivePinFrameSession(frame) || auth.GetAccountID(r) != frame.session.AccountID || auth.IsMaster(r) != frame.session.IsMaster {
		return nil, ErrSharePlaybackChanged
	}
	if frame.session.Scope == models.SessionScopeStream && strings.HasPrefix(frame.session.ScopeResource, "watch-party-playback:v1:") {
		frame.scopeMu.RLock()
		resolved := frame.effectiveStream
		frame.scopeMu.RUnlock()
		if resolved == "" {
			return nil, ErrSharePlaybackChanged
		}
		return nil, ErrSharePlaybackParty
	}
	if frame.session.Scope != models.SessionScopeStream || frame.session.IsMaster || !shareValidResource(frame.session.ScopeResource) {
		return nil, ErrSharePlaybackForbidden
	}
	var nonce struct {
		ID string `json:"share_binding_id"`
	}
	if json.Unmarshal(frame.sessionRow.raw, &nonce) != nil || !shareCanonicalUUID(nonce.ID) {
		return nil, ErrSharePlaybackChanged
	}
	if len(r.Header.Values("X-Client-ID")) > 1 {
		return nil, ErrSharePlaybackChanged
	}
	client := r.Header.Get("X-Client-ID")
	if len(client) > 256 || client != strings.TrimSpace(client) || frame.session.ClientID != "" && client != "" && client != frame.session.ClientID {
		return nil, ErrSharePlaybackChanged
	}
	return frame, nil
}

// ValidateSharePlaybackRequest retains the actual frame. A separate genuine
// party envelope is resolved by the established room/guest validator instead.
func (c *ShareLinkRedeemer) ValidateSharePlaybackRequest(r *http.Request) error {
	frame, e := shareOriginalFrame(c, r)
	if e != nil {
		return e
	}
	owner, e := c.CaptureSharePlaybackOwner(r, "", frame.session.ScopeResource)
	if owner != nil {
		owner.Close()
	}
	return e
}

func (c *ShareLinkRedeemer) CaptureSharePlaybackOwner(r *http.Request, profile, resource string) (owner *SharePlaybackOwner, err error) {
	frame, e := shareOriginalFrame(c, r)
	if e != nil {
		return nil, e
	}
	if profile != "" && !parentMutationID(profile) || resource != "" && !shareValidResource(shareNormalizeResource(resource)) {
		return nil, ErrSharePlaybackChanged
	}
	if !c.enter() {
		return nil, ErrSharePlaybackUnavailable
	}
	defer c.operations.Done()
	expiry := parentMutationMin(frame.session.ExpiresAt, frame.actorExpiry)
	life, cancel := context.WithDeadline(context.Background(), expiry)
	o := &SharePlaybackOwner{runtime: c, original: frame.session, ctx: life, cancel: cancel, setupFrame: frame, setupRequest: r, bytes: 4*len(frame.session.ScopeResource) + (64 << 10), done: make(chan struct{})}
	o.self = o
	// Initialize signal-only callbacks before publishing this owner to Close.
	o.stopSetup = familyMediaAfterFunc(frame.ctx, cancel)
	o.stopRuntime = familyMediaAfterFunc(c.ctx, cancel)
	// Reserve the original owner's complete bounded memory/admission before SQL.
	c.mu.Lock()
	total, account := 0, 0
	for current := range c.owners {
		total += current.bytes
		if current.original.AccountID == o.original.AccountID {
			account++
		}
	}
	if c.closed || len(c.owners) >= 256 || account >= 32 || total+o.bytes > sharePlaybackOwnerBytes {
		c.mu.Unlock()
		cancel()
		o.stopSetup()
		o.stopRuntime()
		return nil, ErrSharePlaybackBusy
	}
	o.operations.Add(1)
	c.owners[o] = struct{}{}
	c.mu.Unlock()
	defer func() {
		if err != nil {
			o.Close()
			owner = nil
		}
	}()
	defer o.operations.Done()
	ctx, stop := context.WithTimeout(r.Context(), sharePlaybackCheckBudget)
	end := familyMediaAfterFunc(o.ctx, stop)
	budgetSignal := familyMediaAfterFunc(ctx, cancel)
	// The policy is captured once on the original fixed source context. Its
	// initial work is actively bounded; the temporary work signal retires only
	// after actual setup SQL/physical cleanup, before intentional work cancel.
	defer func() {
		if err == nil && (r.Context().Err() != nil || frame.ctx.Err() != nil || o.ctx.Err() != nil) {
			err = ErrSharePlaybackChanged
		}
	}()
	defer func() { budgetSignal(); end(); stop() }()
	policy, e := c.security.CapturePolicy(o.ctx)
	if e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	g, e := c.store.beginSettingsProgressTx(ctx, c.coordinator)
	if e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	defer func() {
		if closeErr := finishShareCreationTransaction(g); err == nil && closeErr != nil {
			err = errors.Join(ErrSharePlaybackUnavailable, closeErr)
		}
	}()
	snapshot, e := c.shareCurrentSnapshot(g, frame.session, policy)
	if e != nil {
		return nil, e
	}
	if !sameParentMutationRow(snapshot.sessionRow, frame.sessionRow) || !sameParentMutationRow(snapshot.actorRow, frame.actorRow) || profile != "" && profile != snapshot.profile || resource != "" && shareNormalizeResource(resource) != snapshot.resource {
		return nil, ErrSharePlaybackChanged
	}
	if ctx.Err() != nil || frame.ctx.Err() != nil || o.ctx.Err() != nil {
		return nil, ErrSharePlaybackChanged
	}
	o.snapshot = snapshot
	o.policy = policy
	return o, nil
}

// All facts below come from this same original settings-shared transaction.
// No previously returned Session/Profile DTO may authorize a scoped source.
func (c *ShareLinkRedeemer) shareCurrentSnapshot(g *settingsProgressTx, original models.Session, policy *config.SecurityPolicyObservation) (out sharePlaybackSnapshot, err error) {
	ctx, tx := g.ctx, g.tx
	var locked string
	if tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR SHARE`, original.Token).Scan(&locked) != nil {
		return out, ErrSharePlaybackChanged
	}
	frame, _, e := readPGRequestFrame(ctx, tx, original.Token)
	if e != nil || frame.session != original || frame.session.IsMaster || frame.session.Scope != models.SessionScopeStream {
		return out, ErrSharePlaybackChanged
	}
	var nonce struct {
		ID string `json:"share_binding_id"`
	}
	if json.Unmarshal(frame.sessionRow.raw, &nonce) != nil || !shareCanonicalUUID(nonce.ID) {
		return out, ErrSharePlaybackChanged
	}
	var created time.Time
	const query = `SELECT b.binding_id::text,b.share_token,b.creator_id,b.creator_incarnation::text,b.source_profile_id,b.source_profile_incarnation::text,b.source_account_id,b.source_owner_incarnation::text,CASE WHEN octet_length(b.resource)<=1048576 THEN b.resource ELSE NULL END,b.created_at,b.expires_at FROM share_playback_bindings b JOIN sessions s ON s.token=b.session_token AND s.share_binding_id=b.binding_id WHERE b.session_token=$1 FOR SHARE OF b`
	e = tx.QueryRow(ctx, query, original.Token).Scan(&out.binding, &out.link, &out.creator, &out.creatorInc, &out.profile, &out.profileInc, &out.owner, &out.ownerInc, &out.resource, &created, &out.expires)
	if e != nil || out.binding != nonce.ID || !shareCanonicalUUID(out.binding) || !shareCanonicalUUID(out.creatorInc) || !shareCanonicalUUID(out.profileInc) || !shareCanonicalUUID(out.ownerInc) || !parentMutationID(out.creator) || !parentMutationID(out.owner) || !parentMutationID(out.profile) || out.owner != original.AccountID || !shareValidResource(out.resource) || out.resource != original.ScopeResource || !out.expires.Equal(original.ExpiresAt) || !out.expires.After(time.Now()) || out.expires.After(created.Add(6*time.Hour)) {
		return out, ErrSharePlaybackChanged
	}
	link, e := shareReadDelegation(ctx, tx, out.link, false)
	if e != nil || link.creator != out.creator || link.profile != out.profile || link.owner != out.owner || link.creatorInc != out.creatorInc || link.profileInc != out.profileInc || link.ownerInc != out.ownerInc || link.expires.Before(out.expires) {
		return out, ErrSharePlaybackChanged
	}
	var params map[string]string
	if json.Unmarshal(link.params, &params) != nil {
		return out, ErrSharePlaybackChanged
	}
	source := shareNormalizeResource(params["sourcePath"])
	if source == "" {
		source = shareNormalizeResource(params["movie"])
	}
	if source != out.resource {
		return out, ErrSharePlaybackChanged
	}
	ids := []string{out.creator}
	if out.owner != out.creator {
		ids = append(ids, out.owner)
	}
	// Do not acquire row locks after this original session lock: these bounded
	// relation guards fail NOWAIT and freeze all membership/security writes.
	if e = shareLockPolicyRelations(ctx, tx); e != nil {
		return out, e
	}
	for _, id := range ids {
		a, e := shareAccount(ctx, tx, id, false)
		if e != nil {
			return out, e
		}
		if id == out.creator && (a.inc != out.creatorInc || out.creator != out.owner && !a.master) || id == out.owner && a.inc != out.ownerInc {
			return out, ErrSharePlaybackChanged
		}
		if a.expires != nil && a.expires.Before(out.expires) {
			return out, ErrSharePlaybackChanged
		}
	}
	if e = tx.QueryRow(ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1`, out.profile).Scan(&out.profileGeneration); e != nil || out.profileGeneration < 0 {
		return out, ErrSharePlaybackUnavailable
	}
	out.libraryReceipt, e = shareLibraryReceipt(ctx, tx, out.resource, out.owner, out.profile)
	if e != nil {
		return out, e
	}
	var owner, inc string
	var allowed, kids, pin bool
	if e = tx.QueryRow(ctx, `SELECT account_id,share_incarnation::text,allow_share_links,is_kids_profile,pin_hash<>'' FROM users WHERE id=$1`, out.profile).Scan(&owner, &inc, &allowed, &kids, &pin); e != nil || owner != out.owner || inc != out.profileInc || !allowed || kids || pin {
		return out, ErrSharePlaybackForbidden
	}
	if e = tx.QueryRow(ctx, `SELECT last_value FROM regional_policy_revision_seq`).Scan(&out.global); e != nil || out.global < 1 {
		return out, ErrSharePlaybackUnavailable
	}
	out.creatorHouse, e = shareHousehold(ctx, tx, out.creator)
	if e != nil {
		return out, e
	}
	if out.owner == out.creator {
		out.ownerHouse = out.creatorHouse
	} else if out.ownerHouse, e = shareHousehold(ctx, tx, out.owner); e != nil {
		return out, e
	}
	fence, e := c.security.BindPolicy(ctx, policy, g.hold)
	if e != nil {
		return out, ErrSharePlaybackChanged
	}
	if _, e = fence.EnforceProfilePins(); e != nil {
		return out, ErrSharePlaybackChanged
	}
	out.session = frame.session
	out.sessionRow = frame.sessionRow
	out.actorRow = frame.actorRow
	if ctx.Err() != nil || !c.usable() || !out.expires.After(time.Now()) {
		return out, ErrSharePlaybackChanged
	}
	return out, nil
}
func (o *SharePlaybackOwner) check(caller context.Context) (err error) {
	if caller == nil || caller.Err() != nil || !o.enter() {
		return ErrSharePlaybackChanged
	}
	defer o.operations.Done()
	ctx, cancel := context.WithTimeout(caller, sharePlaybackCheckBudget)
	defer cancel()
	stop := familyMediaAfterFunc(o.ctx, cancel)
	defer stop()
	g, e := o.runtime.store.beginSettingsProgressTx(ctx, o.runtime.coordinator)
	if e != nil {
		return ErrSharePlaybackUnavailable
	}
	defer func() {
		if closeErr := finishShareCreationTransaction(g); err == nil && closeErr != nil {
			err = errors.Join(ErrSharePlaybackUnavailable, closeErr)
		}
	}()
	current, readErr := o.runtime.shareCurrentSnapshot(g, o.original, o.policy)
	if readErr != nil || !shareSnapshotSame(o.snapshot, current) || ctx.Err() != nil || o.ctx.Err() != nil {
		return ErrSharePlaybackChanged
	}
	return nil
}
func (o *SharePlaybackOwner) Check(caller context.Context) error {
	e := o.check(caller)
	if e != nil && o.valid() && caller != nil && caller.Err() == nil {
		o.cancel()
	}
	return e
}
func (o *SharePlaybackOwner) CheckRequest(r *http.Request, profile, resource string) error {
	if !o.valid() || r == nil {
		return ErrSharePlaybackChanged
	}
	frame, e := shareOriginalFrame(o.runtime, r)
	if e != nil || frame.session != o.original || profile != "" && profile != o.snapshot.profile || resource != "" && shareNormalizeResource(resource) != o.snapshot.resource {
		return ErrSharePlaybackChanged
	}
	// A fresh request can only correlate this existing fixed source owner; it
	// cannot refresh the original nonce, identity snapshot, policy or expiry.
	return o.Check(r.Context())
}
func (o *SharePlaybackOwner) Handoff(r *http.Request) (*SharePlaybackSourceLease, error) {
	if !o.valid() || r == nil {
		return nil, ErrSharePlaybackChanged
	}
	frame, e := shareOriginalFrame(o.runtime, r)
	if e != nil {
		return nil, e
	}
	if e = o.CheckRequest(r, o.snapshot.profile, o.snapshot.resource); e != nil {
		return nil, e
	}
	o.mu.Lock()
	if o.closed || o.handed || o.ctx.Err() != nil || frame != o.setupFrame || r != o.setupRequest || frame.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, ErrSharePlaybackChanged
	}
	o.handed = true
	stop := o.stopSetup
	o.stopSetup = nil
	o.setupFrame = nil
	o.setupRequest = nil
	lease := &SharePlaybackSourceLease{owner: o, done: make(chan struct{})}
	lease.self = lease
	o.lease = lease
	o.mu.Unlock()
	if stop != nil {
		stop()
	}
	if o.ctx.Err() != nil {
		lease.Close()
		return nil, ErrSharePlaybackChanged
	}
	return lease, nil
}
