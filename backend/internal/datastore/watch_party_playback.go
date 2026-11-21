package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
)

const partyPlaybackPrefix = "watch-party-playback:v1:"
const partyPlaybackTTL = 6 * time.Hour
const partyPlaybackOwnerBytes = 16 << 20

type partySourceSnapshot struct {
	nonce, physical, profile, profileInc, account, accountInc, resource, paramsHash string
	generation                                                                      int64
	library                                                                         string
}
type partyPlaybackSnapshot struct {
	session              models.Session
	sessionRow, actorRow parentMutationRow
	guest                partyGuestSnapshot
	binding              string
	source               partySourceSnapshot
	household            string
	global               int64
	expires              time.Time
}

// A grant result is only copied wire DATA for the completed known-ACK mint.
// No later media/guest operation accepts it as authority.
type WatchPartyPlaybackGrant struct {
	Session          models.Session    `json:"-"`
	Params           map[string]string `json:"-"`
	RoomID           string            `json:"-"`
	Position         float64           `json:"-"`
	PublicationUntil time.Time         `json:"-"`
}

func (o *partyOperation) source() (out partySourceSnapshot, params map[string]string, err error) {
	var raw []byte
	var roomInc, storedPhysical, hash string
	var bound, originalBound time.Time
	const query = `SELECT p.source_nonce::text,p.physical_receipt,concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),p.room_incarnation::text,p.source_profile_id,p.source_profile_incarnation::text,p.source_account_id,p.source_account_incarnation::text,CASE WHEN octet_length(s.resource)<=1048576 THEN s.resource ELSE NULL END,CASE WHEN octet_length(s.params::text)<=1048576 THEN s.params ELSE NULL END,p.params_sha256,p.source_sha256,s.bound_at,p.bound_at FROM watch_room_external_sources s JOIN watch_room_source_provenance p ON p.room_id=s.room_id AND p.version=1 WHERE s.room_id=$1 FOR UPDATE OF s,p`
	e := o.g.tx.QueryRow(o.ctx, query, o.room.id).Scan(&out.nonce, &storedPhysical, &out.physical, &roomInc, &out.profile, &out.profileInc, &out.account, &out.accountInc, &out.resource, &raw, &out.paramsHash, &hash, &bound, &originalBound)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, nil, ErrWatchRoomConflict
	}
	if e != nil || len(raw) == 0 || json.Unmarshal(raw, &params) != nil {
		return out, nil, ErrWatchRoomUnavailable
	}
	owned, _, e := watchRoomParams(params)
	if e != nil {
		return out, nil, e
	}
	params = owned
	digest, e := watchRoomJSONHash(params)
	if e != nil {
		return out, nil, ErrWatchRoomUnavailable
	}
	sourceHash, e := watchRoomJSONHash([]any{o.room.id, o.room.inc, out.profile, out.profileInc, out.account, out.accountInc, out.resource, params})
	if e != nil || roomInc != o.room.inc || !shareCanonicalUUID(out.nonce) || storedPhysical != out.physical || !bound.Equal(originalBound) || out.profile != o.profile.id || out.profileInc != o.profile.inc || out.account != o.account.id || out.accountInc != o.account.inc || !shareValidResource(out.resource) || params["profileId"] != out.profile || digest != out.paramsHash || sourceHash != hash {
		return out, nil, ErrWatchRoomChanged
	}
	if e = o.g.tx.QueryRow(o.ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1`, out.profile).Scan(&out.generation); e != nil || out.generation < 0 {
		return out, nil, ErrWatchRoomUnavailable
	}
	out.library, e = shareLibraryReceipt(o.ctx, o.g.tx, out.resource, out.account, out.profile)
	if e != nil {
		return out, nil, ErrWatchRoomForbidden
	}
	return out, params, nil
}
func partyEnvelope(room, guest, resource string) (string, error) {
	raw, e := json.Marshal(struct {
		RoomID   string `json:"roomId"`
		GuestID  string `json:"guestId"`
		Resource string `json:"resource"`
	}{room, guest, resource})
	if e != nil {
		return "", e
	}
	return partyPlaybackPrefix + string(raw), nil
}
func (o *partyOperation) existingPlayback(token string) (out partyPlaybackSnapshot, err error) {
	var created time.Time
	const query = `SELECT b.binding_id::text,b.guest_session_token,b.guest_binding_id::text,b.room_id,b.room_incarnation::text,b.guest_id,b.guest_incarnation::text,b.creator_profile_id,b.creator_profile_incarnation::text,b.creator_account_id,b.creator_account_incarnation::text,b.source_nonce::text,b.source_physical,b.source_profile_id,b.source_profile_incarnation::text,b.source_account_id,b.source_account_incarnation::text,CASE WHEN octet_length(b.resource)<=1048576 THEN b.resource ELSE NULL END,b.params_sha256,b.profile_generation,b.created_at,b.expires_at FROM watch_party_playback_bindings b JOIN sessions s ON s.token=b.session_token AND s.watch_party_playback_binding_id=b.binding_id WHERE b.session_token=$1 AND b.version=1 FOR UPDATE OF b,s`
	var guestToken string
	e := o.g.tx.QueryRow(o.ctx, query, token).Scan(&out.binding, &guestToken, &out.guest.binding, &out.guest.room, &out.guest.roomInc, &out.guest.guest, &out.guest.guestInc, &out.guest.profile, &out.guest.profileInc, &out.guest.account, &out.guest.accountInc, &out.source.nonce, &out.source.physical, &out.source.profile, &out.source.profileInc, &out.source.account, &out.source.accountInc, &out.source.resource, &out.source.paramsHash, &out.source.generation, &created, &out.expires)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrWatchRoomForbidden
	}
	if e != nil {
		return out, ErrWatchRoomUnavailable
	}
	guest, e := o.guestFromToken(guestToken, true)
	if e != nil {
		return out, e
	}
	if out.guest.binding != guest.binding || out.guest.room != guest.room || out.guest.roomInc != guest.roomInc || out.guest.guest != guest.guest || out.guest.guestInc != guest.guestInc || out.guest.profile != guest.profile || out.guest.profileInc != guest.profileInc || out.guest.account != guest.account || out.guest.accountInc != guest.accountInc {
		return out, ErrWatchRoomChanged
	}
	out.guest = guest
	source, _, e := o.source()
	if e != nil {
		return out, e
	}
	if out.source.nonce != source.nonce || out.source.physical != source.physical || out.source.profile != source.profile || out.source.profileInc != source.profileInc || out.source.account != source.account || out.source.accountInc != source.accountInc || out.source.resource != source.resource || out.source.paramsHash != source.paramsHash || out.source.generation != source.generation {
		return out, ErrWatchRoomChanged
	}
	out.source = source
	frame, _, e := readPGRequestFrame(o.ctx, o.g.tx, token)
	if e != nil {
		return out, ErrWatchRoomChanged
	}
	var nonce struct {
		ID string `json:"watch_party_playback_binding_id"`
	}
	expected, e := partyEnvelope(guest.room, guest.guest, source.resource)
	if e != nil || json.Unmarshal(frame.sessionRow.raw, &nonce) != nil || nonce.ID != out.binding || !shareCanonicalUUID(out.binding) || frame.session.Scope != models.SessionScopeStream || frame.session.ScopeResource != expected || frame.session.IsMaster || frame.session.AccountID != source.account || !frame.session.CreatedAt.Equal(created) || !frame.session.ExpiresAt.Equal(out.expires) || out.expires.After(created.Add(partyPlaybackTTL)) || out.expires.After(guest.expires) || out.expires.After(o.room.expires) || o.account.expires != nil && out.expires.After(*o.account.expires) || !out.expires.After(time.Now()) {
		return out, ErrWatchRoomChanged
	}
	out.session = frame.session
	out.sessionRow = frame.sessionRow
	out.actorRow = frame.actorRow
	out.household = o.household
	if e = o.g.tx.QueryRow(o.ctx, `SELECT last_value FROM regional_policy_revision_seq`).Scan(&out.global); e != nil || out.global < 1 {
		return out, ErrWatchRoomUnavailable
	}
	o.tighten(out.expires)
	return out, o.fence()
}
func (a *WatchRoomAuthority) GrantPartyPlayback(r *http.Request, roomID string) (result *WatchPartyPlaybackGrant, err error) {
	if r == nil || r.Method != http.MethodPost || len(r.UserAgent()) > 4096 || !utf8.ValidString(r.UserAgent()) || len(r.RemoteAddr) > 256 {
		return nil, ErrWatchRoomInvalid
	}
	op, guest, e := a.beginOriginalGuest(r, roomID)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			result = nil
		}
	}()
	if op.room.status != models.WatchRoomStatusPlaying {
		return nil, ErrWatchRoomConflict
	}
	source, params, e := op.source()
	if e != nil {
		return nil, e
	}
	room, e := op.roomData()
	if e != nil {
		return nil, e
	}
	// Bound the actual encoded publication before any new session row can grow.
	values := url.Values{}
	for key, value := range params {
		values.Set(key, value)
	}
	values.Set("token", strings.Repeat("x", 43))
	values.Set("shareMode", "1")
	values.Set("sharedSource", "1")
	values.Set("watchPartyRoomId", roomID)
	values.Set("startOffset", fmt.Sprintf("%f", room.Position))
	if len(values.Encode())+512 > watchPartyRoomBytes {
		return nil, ErrWatchRoomUnavailable
	}
	rows, e := op.g.tx.Query(op.ctx, `SELECT session_token FROM watch_party_playback_bindings WHERE guest_binding_id=$1::uuid AND expires_at>clock_timestamp() ORDER BY created_at,session_token LIMIT 3`, guest.binding)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	tokens := make([]string, 0, 2)
	for rows.Next() {
		var token string
		if e = rows.Scan(&token); e != nil || !partyCanonicalToken(token) {
			rows.Close()
			return nil, ErrWatchRoomUnavailable
		}
		tokens = append(tokens, token)
	}
	e = rows.Err()
	rows.Close()
	if e != nil || len(tokens) > 2 {
		return nil, ErrWatchRoomUnavailable
	}
	for _, token := range tokens {
		current, e := op.existingPlayback(token)
		if e == nil && current.guest.binding == guest.binding && current.source == source {
			if e = op.fence(); e != nil {
				return nil, e
			}
			return &WatchPartyPlaybackGrant{Session: current.session, Params: params, RoomID: roomID, Position: room.Position, PublicationUntil: op.until}, nil
		}
		if e != nil && !errors.Is(e, ErrWatchRoomChanged) && !errors.Is(e, ErrWatchRoomForbidden) && !errors.Is(e, ErrWatchRoomConflict) {
			return nil, e
		}
	}
	if len(tokens) >= 2 {
		return nil, ErrWatchRoomBusy
	}
	// Expired rows belong only to this original guest. Their removal does not
	// recreate or renew any existing binding, and all SQL remains the same TX.
	if _, e = op.g.tx.Exec(op.ctx, `DELETE FROM sessions s USING watch_party_playback_bindings b WHERE b.session_token=s.token AND b.guest_binding_id=$1::uuid AND b.expires_at<=clock_timestamp()`, guest.binding); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	token, e := partyNewToken()
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	nonce, e := uuid.NewRandom()
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	scope, e := partyEnvelope(roomID, guest.guest, source.resource)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := parentMutationMin(now.Add(partyPlaybackTTL), &guest.expires, &op.room.expires, op.account.expires)
	if !expiry.After(now) {
		return nil, ErrWatchRoomChanged
	}
	session := models.Session{Token: token, AccountID: source.account, Scope: models.SessionScopeStream, ScopeResource: scope, CreatedAt: now, ExpiresAt: expiry, UserAgent: r.UserAgent(), IPAddress: watchRoomActualPeer(r)}
	if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO sessions(token,account_id,is_master,scope,scope_resource,created_at,expires_at,user_agent,ip_address,client_id,watch_party_playback_binding_id)VALUES($1,$2,false,'stream',$3,$4,$5,$6,$7,'',$8::uuid)`, token, source.account, scope, now, expiry, session.UserAgent, session.IPAddress, nonce.String()); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_party_playback_bindings(session_token,binding_id,guest_session_token,guest_binding_id,room_id,room_incarnation,guest_id,guest_incarnation,creator_profile_id,creator_profile_incarnation,creator_account_id,creator_account_incarnation,source_nonce,source_physical,source_profile_id,source_profile_incarnation,source_account_id,source_account_incarnation,resource,params_sha256,profile_generation,created_at,expires_at)VALUES($1,$2::uuid,$3,$4::uuid,$5,$6::uuid,$7,$8::uuid,$9,$10::uuid,$11,$12::uuid,$13::uuid,$14,$15,$16::uuid,$17,$18::uuid,$19,$20,$21,$22,$23)`, token, nonce.String(), guest.session.Token, guest.binding, roomID, op.room.inc, guest.guest, guest.guestInc, op.profile.id, op.profile.inc, op.account.id, op.account.inc, source.nonce, source.physical, source.profile, source.profileInc, source.account, source.accountInc, source.resource, source.paramsHash, source.generation, now, expiry); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	op.tighten(expiry)
	if e = op.commit(); e != nil {
		return nil, e
	}
	return &WatchPartyPlaybackGrant{Session: session, Params: params, RoomID: roomID, Position: room.Position, PublicationUntil: op.until}, nil
}

// WatchPartyPlaybackOwner is a distinct original scoped source lifetime. It
// cannot become full-session, PIN, share-link or master authority by copying DATA.
type WatchPartyPlaybackOwner struct {
	self                   *WatchPartyPlaybackOwner
	runtime                *WatchRoomAuthority
	original               models.Session
	snapshot               partyPlaybackSnapshot
	policy                 *config.SecurityPolicyObservation
	ctx                    context.Context
	cancel                 context.CancelFunc
	mu                     sync.Mutex
	operations             sync.WaitGroup
	closed, handed         bool
	setupFrame             *pgRequestFrame
	setupRequest           *http.Request
	stopSetup, stopRuntime func()
	lease                  *WatchPartyPlaybackSourceLease
	bytes                  int
	guestKey               string
	once                   sync.Once
	done                   chan struct{}
}
type WatchPartyPlaybackSourceLease struct {
	self  *WatchPartyPlaybackSourceLease
	owner *WatchPartyPlaybackOwner
	once  sync.Once
	done  chan struct{}
}

func (WatchPartyPlaybackOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchPartyPlaybackOwner(opaque)"))
}
func (WatchPartyPlaybackOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchPartyPlaybackOwner) UnmarshalJSON([]byte) error  { return ErrWatchRoomChanged }
func (WatchPartyPlaybackSourceLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchPartyPlaybackSourceLease(opaque)"))
}
func (WatchPartyPlaybackSourceLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchPartyPlaybackSourceLease) UnmarshalJSON([]byte) error  { return ErrWatchRoomChanged }
func (o *WatchPartyPlaybackOwner) valid() bool {
	return o != nil && o.self == o && o.runtime != nil && o.ctx != nil
}
func (o *WatchPartyPlaybackOwner) Context() context.Context {
	if !o.valid() {
		return familyMediaClosedContext()
	}
	return o.ctx
}
func (o *WatchPartyPlaybackOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.original.ExpiresAt
}
func (o *WatchPartyPlaybackOwner) ProfileID() string {
	if !o.valid() {
		return ""
	}
	return o.snapshot.source.profile
}
func (o *WatchPartyPlaybackOwner) Resource() string {
	if !o.valid() {
		return ""
	}
	return o.snapshot.source.resource
}
func (l *WatchPartyPlaybackSourceLease) Context() context.Context {
	if l == nil || l.self != l || l.owner == nil {
		return familyMediaClosedContext()
	}
	return l.owner.Context()
}
func (l *WatchPartyPlaybackSourceLease) Close() {
	if l != nil && l.self == l {
		l.once.Do(func() { close(l.done) })
	}
}
func (o *WatchPartyPlaybackOwner) enter() bool {
	if !o.valid() {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.ctx.Err() != nil || !o.runtime.valid() {
		return false
	}
	o.operations.Add(1)
	return true
}
func (o *WatchPartyPlaybackOwner) Close() {
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
		delete(o.runtime.partyOwners, o)
		o.runtime.mu.Unlock()
		o.runtime.operations.Done()
		close(o.done)
	})
	<-o.done
}
func partyEnvelopeParts(scope string) (room, guest, resource string, err error) {
	if !strings.HasPrefix(scope, partyPlaybackPrefix) || len(scope) > 6*(1<<20)+2048 {
		return "", "", "", ErrWatchRoomForbidden
	}
	var value struct {
		RoomID   string `json:"roomId"`
		GuestID  string `json:"guestId"`
		Resource string `json:"resource"`
	}
	if json.Unmarshal([]byte(strings.TrimPrefix(scope, partyPlaybackPrefix)), &value) != nil || !parentMutationID(value.RoomID) || !parentMutationID(value.GuestID) || !shareValidResource(value.Resource) {
		return "", "", "", ErrWatchRoomChanged
	}
	canonical, e := partyEnvelope(value.RoomID, value.GuestID, value.Resource)
	if e != nil || canonical != scope {
		return "", "", "", ErrWatchRoomChanged
	}
	return value.RoomID, value.GuestID, value.Resource, nil
}
func (a *WatchRoomAuthority) partyOriginalFrame(r *http.Request) (*pgRequestFrame, error) {
	if !a.valid() || r == nil || r.URL == nil || r.Context().Err() != nil || a.store.CheckProfilePinRequestFrame(r) != nil {
		return nil, ErrWatchRoomUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != a.store || frame.ctx == nil || frame.ctx.Err() != nil || frame.session.Scope != models.SessionScopeStream || frame.session.IsMaster {
		return nil, ErrWatchRoomForbidden
	}
	supplied, ok := auth.GetSession(r)
	if !ok || supplied != effectivePinFrameSession(frame) || auth.GetAccountID(r) != frame.session.AccountID || auth.IsMaster(r) {
		return nil, ErrWatchRoomForbidden
	}
	if _, _, _, e := partyEnvelopeParts(frame.session.ScopeResource); e != nil {
		return nil, e
	}
	if len(r.Header.Values("X-Client-ID")) > 1 {
		return nil, ErrWatchRoomChanged
	}
	client := r.Header.Get("X-Client-ID")
	if len(client) > 256 || client != strings.TrimSpace(client) || frame.session.ClientID != "" && client != "" && client != frame.session.ClientID {
		return nil, ErrWatchRoomChanged
	}
	return frame, nil
}
func (a *WatchRoomAuthority) CapturePartyPlaybackOwner(r *http.Request, profile, resource string) (owner *WatchPartyPlaybackOwner, err error) {
	frame, e := a.partyOriginalFrame(r)
	if e != nil {
		return nil, e
	}
	room, guest, boundResource, e := partyEnvelopeParts(frame.session.ScopeResource)
	if e != nil {
		return nil, e
	}
	if profile != "" && !parentMutationID(profile) || resource != "" && shareNormalizeResource(resource) != boundResource {
		return nil, ErrWatchRoomChanged
	}
	expiry := parentMutationMin(frame.session.ExpiresAt, frame.actorExpiry)
	life, cancel := context.WithDeadline(context.Background(), expiry)
	o := &WatchPartyPlaybackOwner{runtime: a, original: frame.session, ctx: life, cancel: cancel, setupFrame: frame, setupRequest: r, bytes: 4*len(frame.session.ScopeResource) + 3*len(frame.sessionRow.raw) + 3*len(frame.actorRow.raw) + (64 << 10), guestKey: room + "\x00" + guest, done: make(chan struct{})}
	o.self = o
	o.stopSetup = familyMediaAfterFunc(frame.ctx, cancel)
	o.stopRuntime = familyMediaAfterFunc(a.ctx, cancel)
	a.mu.Lock()
	bytes, account, guests := 0, 0, 0
	for current := range a.partyOwners {
		bytes += current.bytes
		if current.original.AccountID == o.original.AccountID {
			account++
		}
		if current.guestKey == o.guestKey {
			guests++
		}
	}
	if a.closed || a.ctx.Err() != nil || len(a.partyOwners) >= 256 || account >= 32 || guests >= 8 || bytes+o.bytes > partyPlaybackOwnerBytes {
		a.mu.Unlock()
		cancel()
		o.stopSetup()
		o.stopRuntime()
		return nil, ErrWatchRoomBusy
	}
	o.operations.Add(1)
	a.operations.Add(1)
	a.partyOwners[o] = struct{}{}
	a.mu.Unlock()
	defer func() {
		if err != nil {
			o.Close()
			owner = nil
		}
	}()
	defer o.operations.Done()
	// The once-captured policy belongs to the fixed genuine owner's lifetime,
	// never the intentionally retired temporary request/transaction context.
	o.policy, e = a.security.CapturePolicy(o.ctx)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	ctx, stop := context.WithTimeout(r.Context(), 2*time.Second)
	defer stop()
	end := familyMediaAfterFunc(o.ctx, stop)
	defer end()
	op, e := a.beginPartyWork(ctx, frame.session.AccountID, o.policy)
	if e != nil {
		return nil, e
	}
	op.frame = frame
	var snapshot partyPlaybackSnapshot
	e = op.currentRoom(room)
	if e == nil {
		snapshot, e = op.existingPlayback(frame.session.Token)
	}
	if e == nil && (!sameParentMutationRow(snapshot.sessionRow, frame.sessionRow) || !sameParentMutationRow(snapshot.actorRow, frame.actorRow) || snapshot.session != frame.session || snapshot.guest.guest != guest || snapshot.source.resource != boundResource || profile != "" && profile != snapshot.source.profile) {
		e = ErrWatchRoomChanged
	}
	e = op.finish(e)
	if e != nil {
		return nil, e
	}
	if ctx.Err() != nil || o.ctx.Err() != nil || r.Context().Err() != nil {
		return nil, ErrWatchRoomChanged
	}
	o.snapshot = snapshot
	return o, nil
}
func partyGuestSame(a, b partyGuestSnapshot) bool {
	return a.session == b.session && sameParentMutationRow(a.sessionRow, b.sessionRow) && sameParentMutationRow(a.actorRow, b.actorRow) && a.binding == b.binding && a.room == b.room && a.roomInc == b.roomInc && a.guest == b.guest && a.guestInc == b.guestInc && a.profile == b.profile && a.profileInc == b.profileInc && a.account == b.account && a.accountInc == b.accountInc && a.created.Equal(b.created) && a.expires.Equal(b.expires)
}
func partyPlaybackSame(a, b partyPlaybackSnapshot) bool {
	return a.session == b.session && sameParentMutationRow(a.sessionRow, b.sessionRow) && sameParentMutationRow(a.actorRow, b.actorRow) && partyGuestSame(a.guest, b.guest) && a.binding == b.binding && a.source == b.source && a.household == b.household && a.global == b.global && a.expires.Equal(b.expires)
}

// The lane is a bounded work budget, never source permission. Its single
// actual retained check leaves the second existing SQL slot for public/control
// work. Waiting owns the original source operation but no connection or locks.
type partyPlaybackCheckLane struct {
	count   int
	active  bool
	changed chan struct{}
}

func (o *WatchPartyPlaybackOwner) acquireCheckLane(ctx context.Context) (func(), error) {
	a := o.runtime
	if ctx == nil || ctx.Err() != nil || o.ctx.Err() != nil {
		return nil, ErrWatchRoomChanged
	}
	account := o.original.AccountID
	a.mu.Lock()
	if a.closed || a.ctx.Err() != nil {
		a.mu.Unlock()
		return nil, ErrWatchRoomUnavailable
	}
	if a.partyCheckLanes == nil {
		a.partyCheckLanes = make(map[string]*partyPlaybackCheckLane)
	}
	lane := a.partyCheckLanes[account]
	if a.partyCheckCount >= 128 || lane != nil && lane.count >= 16 {
		a.mu.Unlock()
		return nil, ErrWatchRoomBusy
	}
	if lane == nil {
		lane = &partyPlaybackCheckLane{changed: make(chan struct{})}
		a.partyCheckLanes[account] = lane
	}
	lane.count++
	a.partyCheckCount++
	a.mu.Unlock()
	var once sync.Once
	refund := func(active bool) {
		once.Do(func() {
			a.mu.Lock()
			if active {
				lane.active = false
			}
			lane.count--
			a.partyCheckCount--
			close(lane.changed)
			lane.changed = make(chan struct{})
			if lane.count == 0 {
				delete(a.partyCheckLanes, account)
			}
			a.mu.Unlock()
		})
	}
	for {
		a.mu.Lock()
		if a.closed || a.ctx.Err() != nil || ctx.Err() != nil || o.ctx.Err() != nil {
			a.mu.Unlock()
			refund(false)
			return nil, ErrWatchRoomChanged
		}
		if !lane.active {
			lane.active = true
			a.mu.Unlock()
			return func() { refund(true) }, nil
		}
		changed := lane.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		case <-o.ctx.Done():
		case <-a.ctx.Done():
		}
	}
}

func (o *WatchPartyPlaybackOwner) check(caller context.Context) (err error) {
	if caller == nil || caller.Err() != nil || !o.enter() {
		return ErrWatchRoomChanged
	}
	defer o.operations.Done()
	ctx, cancel := context.WithTimeout(caller, 2*time.Second)
	defer cancel()
	stop := familyMediaAfterFunc(o.ctx, cancel)
	defer stop()
	release, e := o.acquireCheckLane(ctx)
	if e != nil {
		return e
	}
	defer release()
	op, e := o.runtime.beginPartyWork(ctx, o.original.AccountID, o.policy)
	if e != nil {
		return e
	}
	e = op.currentRoom(o.snapshot.guest.room)
	var current partyPlaybackSnapshot
	if e == nil {
		current, e = op.existingPlayback(o.original.Token)
	}
	if e == nil && (!partyPlaybackSame(current, o.snapshot) || o.ctx.Err() != nil || ctx.Err() != nil) {
		e = ErrWatchRoomChanged
	}
	return op.finish(e)
}
func (o *WatchPartyPlaybackOwner) Check(caller context.Context) error {
	e := o.check(caller)
	if e != nil && o.valid() && caller != nil && caller.Err() == nil {
		o.cancel()
	}
	return e
}
func (o *WatchPartyPlaybackOwner) CheckRequest(r *http.Request, profile, resource string) error {
	if !o.valid() || r == nil || profile != "" && profile != o.ProfileID() || resource != "" && shareNormalizeResource(resource) != o.Resource() {
		return ErrWatchRoomChanged
	}
	frame, e := o.runtime.partyOriginalFrame(r)
	if e != nil {
		return e
	}
	if frame.session != o.original || !sameParentMutationRow(frame.sessionRow, o.snapshot.sessionRow) || !sameParentMutationRow(frame.actorRow, o.snapshot.actorRow) {
		return ErrWatchRoomChanged
	}
	return o.Check(r.Context())
}
func (o *WatchPartyPlaybackOwner) Handoff(r *http.Request) (*WatchPartyPlaybackSourceLease, error) {
	if !o.valid() || r == nil {
		return nil, ErrWatchRoomChanged
	}
	o.mu.Lock()
	same := !o.closed && !o.handed && o.setupRequest == r && o.setupFrame != nil && o.setupFrame.ctx.Err() == nil
	o.mu.Unlock()
	if !same {
		return nil, ErrWatchRoomChanged
	}
	if e := o.CheckRequest(r, o.ProfileID(), o.Resource()); e != nil {
		return nil, e
	}
	o.mu.Lock()
	if o.closed || o.handed || o.ctx.Err() != nil || o.setupRequest != r || o.setupFrame == nil || o.setupFrame.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, ErrWatchRoomChanged
	}
	lease := &WatchPartyPlaybackSourceLease{owner: o, done: make(chan struct{})}
	lease.self = lease
	o.lease = lease
	o.handed = true
	stop := o.stopSetup
	o.stopSetup = nil
	o.setupFrame = nil
	o.setupRequest = nil
	o.mu.Unlock()
	if stop != nil {
		stop()
	}
	if o.ctx.Err() != nil {
		lease.Close()
		return nil, ErrWatchRoomChanged
	}
	return lease, nil
}
func (a *WatchRoomAuthority) ResolveOriginalPartyRequest(r *http.Request) (*http.Request, error) {
	frame, e := a.partyOriginalFrame(r)
	if e != nil {
		return nil, e
	}
	owner, e := a.CapturePartyPlaybackOwner(r, "", "")
	if e != nil {
		return nil, e
	}
	resource := owner.Resource()
	owner.Close()
	if e = a.store.CheckProfilePinRequestFrame(r); e != nil {
		return nil, e
	}
	frame.scopeMu.Lock()
	if frame.effectiveStream != "" && frame.effectiveStream != resource {
		frame.scopeMu.Unlock()
		return nil, ErrWatchRoomChanged
	}
	frame.effectiveStream = resource
	frame.scopeMu.Unlock()
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, effectivePinFrameSession(frame))), nil
}
