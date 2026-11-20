package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"novastream/internal/auth"
	"novastream/models"
)

// FamilyScopedApplicability proves only that one original Jellyfin-scoped
// session's exact owned profile remains in an inactive household. It is not a
// full session, source, classification, PIN grant, or media permission.
type FamilyScopedApplicability struct {
	state *familyScopedApplicabilityState
}

type familyScopedApplicabilityState struct {
	mu             sync.Mutex
	self           *FamilyScopedApplicability
	store          *DataStore
	original       models.Session
	snapshot       familyScopedSnapshot
	ctx            context.Context
	cancel         context.CancelFunc
	setupFrame     *pgRequestFrame
	setupContext   context.Context
	setupStop      func() bool
	setupFrameStop func() bool
	setupDone      chan struct{}
	setupFrameDone chan struct{}
	handed, closed bool
	operations     int
	pending        sync.WaitGroup
	once           sync.Once
}
type familyScopedSnapshot struct {
	session, actor parentMutationRow
	account        RegionalAccountSnapshot
	profile        RegionalProfileSnapshot
	expires        time.Time
}

var familyScopedAdmission = struct {
	sync.Mutex
	owners     map[*FamilyScopedApplicability]string
	accounts   map[string]int
	operations int
}{owners: map[*FamilyScopedApplicability]string{}, accounts: map[string]int{}}

func (FamilyScopedApplicability) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyScopedApplicability) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyScopedApplicability) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyScopedApplicability(opaque)"))
}
func (o *FamilyScopedApplicability) valid() bool {
	return o != nil && o.state != nil && o.state.self == o
}
func (o *FamilyScopedApplicability) Context() context.Context {
	if !o.valid() {
		return familyMediaClosedContext()
	}
	return o.state.ctx
}
func (o *FamilyScopedApplicability) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.state.snapshot.expires
}

// Capture admits before any PG work. The only profile selector is the raw
// authenticated SQL session's existing Jellyfin scope_resource. Even a master
// cannot substitute a profile owned by another account.
func (ds *DataStore) CaptureInactiveJellyfinApplicability(r *http.Request) (proof *FamilyScopedApplicability, err error) {
	if ds == nil || ds.pool == nil || r == nil || r.URL == nil || r.Context().Err() != nil {
		return nil, ErrFamilyMediaUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || frame.ctx == nil || frame.ctx.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	original := frame.session
	if original.Scope != models.SessionScopeJellyfin || !parentMutationID(original.ScopeResource) || !familyMediaClient(r, original) {
		return nil, ErrFamilyMediaScopedUnsupported
	}
	expiry := parentMutationMin(original.ExpiresAt, frame.actorExpiry)
	life, cancel := context.WithDeadline(context.Background(), expiry)
	o := &FamilyScopedApplicability{state: &familyScopedApplicabilityState{store: ds, original: original, ctx: life, cancel: cancel, setupFrame: frame, setupContext: r.Context(), setupDone: make(chan struct{}), setupFrameDone: make(chan struct{})}}
	o.state.self = o
	familyScopedAdmission.Lock()
	if len(familyScopedAdmission.owners) >= 512 || familyScopedAdmission.accounts[original.AccountID] >= 32 {
		familyScopedAdmission.Unlock()
		cancel()
		return nil, ErrFamilyMediaBusy
	}
	familyScopedAdmission.owners[o] = original.AccountID
	familyScopedAdmission.accounts[original.AccountID]++
	familyScopedAdmission.Unlock()
	o.state.setupStop = context.AfterFunc(r.Context(), func() {
		defer close(o.state.setupDone)
		o.state.mu.Lock()
		if !o.state.handed {
			cancel()
		}
		o.state.mu.Unlock()
	})
	o.state.setupFrameStop = context.AfterFunc(frame.ctx, func() {
		defer close(o.state.setupFrameDone)
		o.state.mu.Lock()
		if !o.state.handed {
			cancel()
		}
		o.state.mu.Unlock()
	})
	defer func() {
		if proof == nil {
			o.Close()
		}
	}()
	bounded, finish, e := o.begin(r.Context(), frame.ctx)
	if e != nil {
		return nil, e
	}
	// Finish the owned query/cancel work before error cleanup calls Close.
	e = func() error {
		defer finish()
		if ds.CheckProfilePinRequestFrame(r.WithContext(bounded)) != nil {
			return ErrFamilyMediaChanged
		}
		current, e := ds.readFamilyScopedSnapshot(bounded, original)
		if e != nil {
			return e
		}
		if !sameParentMutationRow(frame.sessionRow, current.session) || !sameParentMutationRow(frame.actorRow, current.actor) || !current.expires.Equal(expiry) || ds.CheckProfilePinRequestFrame(r.WithContext(bounded)) != nil || bounded.Err() != nil || life.Err() != nil || !expiry.After(time.Now()) {
			return ErrFamilyMediaChanged
		}
		o.state.snapshot = current
		return nil
	}()
	if e != nil {
		return nil, e
	}
	return o, nil
}

func sameFamilyScopedSnapshot(a, b familyScopedSnapshot) bool {
	return sameParentMutationRow(a.session, b.session) && sameParentMutationRow(a.actor, b.actor) && a.account == b.account && a.profile.ProfileID == b.profile.ProfileID && a.profile.AccountID == b.profile.AccountID && a.profile.IsKids == b.profile.IsKids && a.profile.Revision == b.profile.Revision && a.expires.Equal(b.expires)
}
func (ds *DataStore) readFamilyScopedSnapshot(ctx context.Context, original models.Session) (out familyScopedSnapshot, err error) {
	first, _, err := readPGRequestFrame(ctx, ds.pool, original.Token)
	if err != nil {
		return out, err
	}
	if first.session != original || original.Scope != models.SessionScopeJellyfin {
		return out, ErrFamilyMediaChanged
	}
	account, err := ds.readFamilyScopedHousehold(ctx, original.AccountID)
	if err != nil {
		return out, ErrFamilyMediaUnavailable
	}
	profile, err := ds.CurrentRegionalProfile(ctx, original.ScopeResource)
	if err != nil {
		return out, ErrFamilyMediaUnavailable
	}
	if profile.ProfileID != original.ScopeResource || profile.AccountID != original.AccountID || profile.IsKids || account.HasChildren {
		return out, ErrFamilyMediaChildDenied
	}
	again, err := ds.CurrentRegionalProfile(ctx, original.ScopeResource)
	if err != nil || again.ProfileID != profile.ProfileID || again.AccountID != profile.AccountID || again.IsKids != profile.IsKids || again.Revision != profile.Revision {
		return out, ErrFamilyMediaChanged
	}
	after, err := ds.readFamilyScopedHousehold(ctx, original.AccountID)
	if err != nil || after != account {
		return out, ErrFamilyMediaChanged
	}
	last, _, err := readPGRequestFrame(ctx, ds.pool, original.Token)
	expiry := parentMutationMin(original.ExpiresAt, first.actorExpiry)
	if err != nil || last.session != original || !sameParentMutationRow(first.sessionRow, last.sessionRow) || !sameParentMutationRow(first.actorRow, last.actorRow) || ctx.Err() != nil || !expiry.After(time.Now()) {
		return out, ErrFamilyMediaChanged
	}
	// These bracketed current observations do not claim a transaction spanning
	// independent repositories or atomicity with a later media/source operation.
	return familyScopedSnapshot{session: first.sessionRow, actor: first.actorRow, account: account, profile: profile, expires: expiry}, nil
}

const familyScopedHouseholdBytes = 64 << 10

// Limit before aggregation and sanitize identifiers before constructing JSON.
// For admitted households this is the complete canonical original member set;
// a 257th row or malformed identifier/boolean makes the observation unusable.
const familyScopedHouseholdCTE = `WITH limited_members AS MATERIALIZED (
 SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN
 CASE WHEN octet_length(convert_to(id,'UTF8'))<=256 THEN id ELSE NULL END ELSE NULL END AS bounded_id,
 is_kids_profile AS child,concat(tableoid::oid,':',xmin::text,':',ctid::text) AS version
 FROM users WHERE account_id=$1 LIMIT 257
), member_facts AS MATERIALIZED (
 SELECT count(*) AS n,COALESCE(bool_or(bounded_id IS NULL OR child IS NULL),false) AS invalid,
 COALESCE(bool_or(child),false) AS children,
 COALESCE(jsonb_agg(jsonb_build_array(bounded_id,child,version) ORDER BY bounded_id),'[]'::jsonb) AS members
 FROM limited_members
), bounded_receipt AS MATERIALIZED (
 SELECT CASE WHEN octet_length(a.id) BETWEEN 1 AND 256 THEN
 CASE WHEN m.n<=256 AND NOT m.invalid AND octet_length(convert_to(a.id,'UTF8'))<=256 THEN
 jsonb_build_object('accountId',a.id,'version',concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),
 'expiry',a.expires_at,'members',m.members,'hasChildren',m.children,'rowCount',m.n) ELSE NULL END ELSE NULL END AS body
 FROM accounts a CROSS JOIN member_facts m WHERE a.id=$1
) `
const familyScopedHouseholdSQL = familyScopedHouseholdCTE + `SELECT CASE
 WHEN current_setting('client_encoding')='UTF8' AND octet_length(convert_to(body::text,'UTF8'))<=65536
 THEN body::text ELSE NULL END AS receipt FROM bounded_receipt`

func (ds *DataStore) readFamilyScopedHousehold(ctx context.Context, id string) (out RegionalAccountSnapshot, err error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || !parentMutationID(id) {
		return out, ErrFamilyMediaUnavailable
	}
	var before, after int64
	if ds.pool.QueryRow(ctx, "SELECT last_value FROM regional_policy_revision_seq").Scan(&before) != nil {
		return out, ErrFamilyMediaUnavailable
	}
	var raw []byte
	if ds.pool.QueryRow(ctx, familyScopedHouseholdSQL, id).Scan(&raw) != nil || len(raw) == 0 || len(raw) > familyScopedHouseholdBytes {
		return out, ErrFamilyMediaUnavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 6 {
		return out, ErrFamilyMediaUnavailable
	}
	var account, version *string
	var children *bool
	var count *int
	var expiry *time.Time
	if json.Unmarshal(fields["accountId"], &account) != nil || account == nil || *account != id || json.Unmarshal(fields["version"], &version) != nil || version == nil || len(*version) == 0 || len(*version) > 96 || json.Unmarshal(fields["hasChildren"], &children) != nil || children == nil || json.Unmarshal(fields["rowCount"], &count) != nil || count == nil || *count < 0 || *count > 256 || json.Unmarshal(fields["expiry"], &expiry) != nil || expiry != nil && !expiry.After(time.Now()) {
		return out, ErrFamilyMediaUnavailable
	}
	var members []json.RawMessage
	if string(fields["members"]) == "null" || json.Unmarshal(fields["members"], &members) != nil || len(members) != *count {
		return out, ErrFamilyMediaUnavailable
	}
	seen := map[string]bool{}
	hasChildren := false
	for _, member := range members {
		var tuple []json.RawMessage
		if json.Unmarshal(member, &tuple) != nil || len(tuple) != 3 {
			return out, ErrFamilyMediaUnavailable
		}
		var memberID, physical *string
		var child *bool
		if json.Unmarshal(tuple[0], &memberID) != nil || memberID == nil || !parentMutationID(*memberID) || seen[*memberID] || json.Unmarshal(tuple[1], &child) != nil || child == nil || json.Unmarshal(tuple[2], &physical) != nil || physical == nil || len(*physical) == 0 || len(*physical) > 96 {
			return out, ErrFamilyMediaUnavailable
		}
		seen[*memberID] = true
		hasChildren = hasChildren || *child
	}
	if hasChildren != *children || ds.pool.QueryRow(ctx, "SELECT last_value FROM regional_policy_revision_seq").Scan(&after) != nil || after != before || ctx.Err() != nil || expiry != nil && !expiry.After(time.Now()) {
		return out, ErrFamilyMediaUnavailable
	}
	return RegionalAccountSnapshot{AccountID: id, HasChildren: hasChildren, Revision: regionalHash([]any{id, *version, before, json.RawMessage(fields["members"])})}, nil
}

// Every operation is reserved under the closed-state lock before its first
// query. It remains charged through query return and actual joined callbacks.
func (o *FamilyScopedApplicability) begin(caller context.Context, frameLife context.Context) (context.Context, func(), error) {
	if !o.valid() || caller == nil || caller.Err() != nil {
		return nil, nil, ErrFamilyMediaChanged
	}
	st := o.state
	st.mu.Lock()
	if st.closed || st.ctx.Err() != nil {
		st.mu.Unlock()
		return nil, nil, ErrFamilyMediaChanged
	}
	if st.operations >= 8 {
		st.mu.Unlock()
		return nil, nil, ErrFamilyMediaBusy
	}
	familyScopedAdmission.Lock()
	if familyScopedAdmission.operations >= 1024 {
		familyScopedAdmission.Unlock()
		st.mu.Unlock()
		return nil, nil, ErrFamilyMediaBusy
	}
	familyScopedAdmission.operations++
	familyScopedAdmission.Unlock()
	st.operations++
	st.pending.Add(1)
	st.mu.Unlock()
	bounded, cancel := context.WithTimeout(caller, familyMediaCheckBudget)
	stop := familyMediaAfterFunc(st.ctx, cancel)
	stopFrame := func() {}
	if frameLife != nil {
		stopFrame = familyMediaAfterFunc(frameLife, cancel)
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			cancel()
			stopFrame()
			stop()
			// DataStore.New's query lifecycle tracer joins already-closed pgx
			// CleanupDone before these PG calls return. Healthy cancel drains server
			// ReadyForQuery; fallback proves only local cleanup, not remote rollback.
			familyScopedAdmission.Lock()
			familyScopedAdmission.operations--
			familyScopedAdmission.Unlock()
			st.mu.Lock()
			st.operations--
			st.mu.Unlock()
			st.pending.Done()
		})
	}
	return bounded, finish, nil
}
func (o *FamilyScopedApplicability) check(ctx context.Context) error {
	current, err := o.state.store.readFamilyScopedSnapshot(ctx, o.state.original)
	if err != nil || !sameFamilyScopedSnapshot(o.state.snapshot, current) || ctx.Err() != nil || o.state.ctx.Err() != nil || !o.state.snapshot.expires.After(time.Now()) {
		return ErrFamilyMediaChanged
	}
	return nil
}
func (o *FamilyScopedApplicability) Check(caller context.Context) error {
	ctx, finish, err := o.begin(caller, nil)
	if err != nil {
		return err
	}
	defer finish()
	err = o.check(ctx)
	if err != nil && caller.Err() == nil {
		o.state.cancel()
	}
	return err
}
func (o *FamilyScopedApplicability) requestFrame(r *http.Request) (*pgRequestFrame, error) {
	if !o.valid() || r == nil || r.URL == nil || r.Context().Err() != nil || !familyMediaClient(r, o.state.original) {
		return nil, ErrFamilyMediaChanged
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != o.state.store || frame.ctx == nil || frame.ctx.Err() != nil || frame.session != o.state.original || !sameParentMutationRow(frame.sessionRow, o.state.snapshot.session) || !sameParentMutationRow(frame.actorRow, o.state.snapshot.actor) {
		return nil, ErrFamilyMediaChanged
	}
	effective := effectivePinFrameSession(frame)
	claimed, ok := auth.GetSession(r)
	master, masterPresent := r.Context().Value(auth.ContextKeyIsMaster).(bool)
	// This port never unwraps or promotes a scoped frame. All local claims must
	// agree before any PG work or decision to retire the original proof.
	if effective != frame.session || !ok || claimed != effective || auth.GetAccountID(r) != frame.session.AccountID || !masterPresent || master != effective.IsMaster {
		return nil, ErrFamilyMediaChanged
	}
	return frame, nil
}

func (o *FamilyScopedApplicability) checkRequest(r *http.Request, ctx context.Context) error {
	if _, err := o.requestFrame(r); err != nil {
		return err
	}
	if o.state.store.CheckProfilePinRequestFrame(r.WithContext(ctx)) != nil {
		return ErrFamilyMediaChanged
	}
	if _, err := o.requestFrame(r); err != nil {
		return err
	}
	if err := o.check(ctx); err != nil {
		return err
	}
	if _, err := o.requestFrame(r); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	return nil
}

func (o *FamilyScopedApplicability) CheckRequest(r *http.Request) error {
	frame, err := o.requestFrame(r)
	if err != nil {
		return err
	}
	ctx, finish, err := o.begin(r.Context(), frame.ctx)
	if err != nil {
		return err
	}
	defer finish()
	err = o.checkRequest(r, ctx)
	// Pure selector/frame conflicts were rejected before this operation. A
	// departed consumer cannot retire the original; a current PG mismatch or
	// unavailable observation under a healthy caller does signal retirement.
	if err != nil {
		if _, localErr := o.requestFrame(r); localErr == nil {
			o.state.cancel()
		}
	}
	return err
}
func (o *FamilyScopedApplicability) Handoff(r *http.Request) error {
	frame, err := o.requestFrame(r)
	if err != nil {
		return err
	}
	ctx, finish, err := o.begin(r.Context(), frame.ctx)
	if err != nil {
		return err
	}
	defer finish()
	if o.checkRequest(r, ctx) != nil {
		if _, localErr := o.requestFrame(r); localErr == nil {
			o.state.cancel()
		}
		return ErrFamilyMediaChanged
	}
	st := o.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || st.handed || frame != st.setupFrame || r.Context() != st.setupContext || st.setupStop == nil || st.setupFrameStop == nil {
		return ErrFamilyMediaChanged
	}
	if _, localErr := o.requestFrame(r); localErr != nil || ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	stopped := st.setupStop()
	if stopped {
		st.setupStop = nil
		close(st.setupDone)
	}
	// Take ownership of every prevented callback immediately. A later stop can
	// fail without leaving Close waiting for a callback that will never run.
	stoppedFrame := st.setupFrameStop()
	if stoppedFrame {
		st.setupFrameStop = nil
		close(st.setupFrameDone)
	}
	if !stopped || !stoppedFrame || frame.ctx.Err() != nil || r.Context().Err() != nil || st.ctx.Err() != nil || !st.snapshot.expires.After(time.Now()) {
		st.cancel()
		return ErrFamilyMediaChanged
	}
	st.setupFrame = nil
	st.setupContext = nil
	st.handed = true
	return nil
}
func (o *FamilyScopedApplicability) Close() {
	if !o.valid() {
		return
	}
	st := o.state
	st.once.Do(func() {
		st.mu.Lock()
		st.closed = true
		st.cancel()
		stop := st.setupStop
		stopFrame := st.setupFrameStop
		st.setupStop = nil
		st.setupFrameStop = nil
		st.setupFrame = nil
		st.setupContext = nil
		st.mu.Unlock()
		if stop != nil && stop() {
			close(st.setupDone)
		}
		if stopFrame != nil && stopFrame() {
			close(st.setupFrameDone)
		}
		<-st.setupDone
		<-st.setupFrameDone
		st.pending.Wait()
		familyScopedAdmission.Lock()
		if account, ok := familyScopedAdmission.owners[o]; ok {
			delete(familyScopedAdmission.owners, o)
			familyScopedAdmission.accounts[account]--
			if familyScopedAdmission.accounts[account] == 0 {
				delete(familyScopedAdmission.accounts, account)
			}
		}
		familyScopedAdmission.Unlock()
	})
}
