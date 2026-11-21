package datastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
)

const watchPartyRoomBytes = 1 << 20
const watchPartyRoomMembers = 256

// These are copied publication DATA, never proof for subsequent guest/media
// operations. Only actual private SQL bindings can authorize those operations.
type WatchPartyInvitationView struct {
	RoomID, Title, CreatorName, ShortCode string
	ExpiresAt                             time.Time
	PublicationUntil                      time.Time `json:"-"`
}
type WatchPartyGuestRoomData struct {
	Room             *models.WatchRoom `json:"-"`
	PlaybackReady    bool              `json:"-"`
	PublicationUntil time.Time         `json:"-"`
}
type WatchPartyJoinResult struct {
	Session          models.Session `json:"-"`
	RoomID           string         `json:"-"`
	GuestID          string         `json:"-"`
	PublicationUntil time.Time      `json:"-"`
}
type partyGuestSnapshot struct {
	session                                                                           models.Session
	sessionRow, actorRow                                                              parentMutationRow
	binding, room, roomInc, guest, guestInc, profile, profileInc, account, accountInc string
	created, expires                                                                  time.Time
}
type partyAdmission struct {
	room, roomInc, nonce, hash, tokenHash, code, physical string
	created, expires                                      time.Time
}

// Anonymous invitation credentials have their own purpose-specific operation.
// This lifetime deliberately does not synthesize a full authenticated frame.
type partyOperation struct {
	authority    *WatchRoomAuthority
	request      *http.Request
	requestCtx   context.Context
	frame        *pgRequestFrame
	ctx          context.Context
	cancel       context.CancelFunc
	stopRuntime  func()
	children     []context.CancelFunc
	until        time.Time
	policy       *config.SecurityPolicyObservation
	g            *settingsProgressTx
	admissionKey string
	room         watchRoomOriginal
	profile      watchRoomProfile
	account      shareAccountRow
	household    string
}

func (a *WatchRoomAuthority) beginParty(r *http.Request) (*partyOperation, error) {
	if r == nil || r.URL == nil {
		return nil, ErrWatchRoomUnavailable
	}
	op, e := a.beginPartyWork(r.Context(), "peer:"+watchRoomActualPeer(r), nil)
	if op != nil {
		op.request = r
	}
	return op, e
}

// This private SQL-lifetime helper supplies no frame or permission. Current
// original owner checks still re-read actual private session/source bindings.
func (a *WatchRoomAuthority) beginPartyWork(original context.Context, key string, policy *config.SecurityPolicyObservation) (o *partyOperation, err error) {
	if !a.valid() || original == nil || original.Err() != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if !a.enter(key) {
		return nil, ErrWatchRoomBusy
	}
	until := time.Now().Add(30 * time.Second)
	if at, ok := original.Deadline(); ok && at.Before(until) {
		until = at
	}
	ctx, cancel := context.WithDeadline(original, until)
	o = &partyOperation{authority: a, requestCtx: original, ctx: ctx, cancel: cancel, until: until, admissionKey: key, policy: policy}
	o.stopRuntime = familyMediaAfterFunc(a.ctx, cancel)
	defer func() {
		if err != nil {
			_ = o.finish(err)
			o = nil
		}
	}()
	if o.policy == nil {
		o.policy, err = a.security.CapturePolicy(ctx)
		if err != nil {
			return o, ErrWatchRoomUnavailable
		}
	}
	o.g, err = a.store.beginSettingsProgressTx(ctx, a.coordinator)
	if err != nil {
		return o, ErrWatchRoomUnavailable
	}
	o.ctx = o.g.ctx
	return o, nil
}

func (o *partyOperation) tighten(at time.Time) {
	if at.Before(o.until) {
		o.until = at
		ctx, cancel := context.WithDeadline(o.ctx, at)
		o.children = append(o.children, cancel)
		o.ctx = ctx
	}
}
func (o *partyOperation) chargeAccount(account string) error {
	a := o.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx.Err() != nil || !parentMutationID(account) {
		return ErrWatchRoomUnavailable
	}
	if o.admissionKey == account {
		return nil
	}
	if a.actors[account] >= 2 {
		return ErrWatchRoomBusy
	}
	a.actors[o.admissionKey]--
	if a.actors[o.admissionKey] == 0 {
		delete(a.actors, o.admissionKey)
	}
	a.actors[account]++
	o.admissionKey = account
	return nil
}
func (o *partyOperation) finish(previous error) error {
	workErr := o.ctx.Err()
	if o.g != nil {
		if e := finishShareCreationTransaction(o.g); previous == nil && e != nil {
			previous = errors.Join(ErrWatchRoomUnavailable, e)
		}
	}
	if previous == nil && (workErr != nil || o.requestCtx.Err() != nil || o.frame != nil && o.frame.ctx.Err() != nil || !o.authority.valid() || !o.until.After(time.Now())) {
		previous = ErrWatchRoomChanged
	}
	for _, stop := range o.children {
		stop()
	}
	o.stopRuntime()
	o.cancel()
	o.authority.leave(o.admissionKey)
	return previous
}
func (o *partyOperation) fence() error {
	if o.ctx.Err() != nil || o.requestCtx.Err() != nil || o.frame != nil && o.frame.ctx.Err() != nil || !o.authority.valid() || !o.until.After(time.Now()) {
		return ErrWatchRoomChanged
	}
	bound, e := o.authority.security.BindPolicy(o.ctx, o.policy, o.g.hold)
	if e != nil {
		return ErrWatchRoomChanged
	}
	if _, e = bound.EnforceProfilePins(); e != nil {
		return ErrWatchRoomChanged
	}
	return nil
}
func (o *partyOperation) commit() error {
	if e := o.fence(); e != nil {
		return e
	}
	if e := o.g.tx.Commit(o.ctx); e != nil {
		if errors.Is(e, pgx.ErrTxCommitRollback) {
			return ErrWatchRoomUnavailable
		}
		return ErrWatchRoomUnknown
	}
	return nil
}
func partyCanonicalToken(token string) bool {
	if len(token) != 43 {
		return false
	}
	raw, e := base64.RawURLEncoding.Strict().DecodeString(token)
	if e != nil {
		return false
	}
	defer clear(raw)
	return len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == token
}
func partyNewToken() (string, error) {
	raw := make([]byte, 32)
	if _, e := rand.Read(raw); e != nil {
		return "", e
	}
	defer clear(raw)
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func partyGuestID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:16])
}
func partySecret(secret string, code bool) (string, error) {
	if len(secret) > 64 || !utf8.ValidString(secret) {
		return "", ErrWatchRoomInvalid
	}
	if !code {
		if !partyCanonicalToken(secret) {
			return "", ErrWatchRoomInvalid
		}
		sum := sha256.Sum256([]byte(secret))
		return base64.RawURLEncoding.EncodeToString(sum[:]), nil
	}
	clean := strings.ToUpper(strings.TrimSpace(secret))
	clean = strings.ReplaceAll(clean, "-", "")
	clean = strings.ReplaceAll(clean, " ", "")
	if len(clean) != 8 {
		return "", ErrWatchRoomInvalid
	}
	for _, c := range clean {
		if !(c >= 'A' && c <= 'Z' || c >= '2' && c <= '7') {
			return "", ErrWatchRoomInvalid
		}
	}
	return clean[:4] + "-" + clean[4:], nil
}
func (o *partyOperation) readRoom(id string, locked bool) (out watchRoomOriginal, err error) {
	if !parentMutationID(id) {
		return out, ErrWatchRoomInvalid
	}
	q := `SELECT r.id,CASE WHEN octet_length(r.creator_profile_id) BETWEEN 1 AND 256 THEN r.creator_profile_id ELSE NULL END,r.room_incarnation::text,r.created_at,r.expires_at,CASE WHEN octet_length(r.status)<=16 THEN r.status ELSE NULL END,concat(r.tableoid::oid,':',r.xmin::text,':',r.ctid::text),CASE WHEN octet_length(p.creator_account_id) BETWEEN 1 AND 256 THEN p.creator_account_id ELSE NULL END,p.creator_profile_incarnation::text,p.creator_account_incarnation::text FROM watch_rooms r JOIN watch_room_provenance p ON p.room_id=r.id AND p.version=1 AND p.room_incarnation=r.room_incarnation AND p.creator_profile_id=r.creator_profile_id AND p.created_at=r.created_at AND p.expires_at=r.expires_at WHERE r.id=$1`
	if locked {
		q += ` FOR UPDATE OF r,p`
	}
	e := o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.profile, &out.inc, &out.created, &out.expires, &out.status, &out.physical, &out.account, &out.profileInc, &out.accountInc)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if e != nil || !parentMutationID(out.id) || !parentMutationID(out.profile) || !parentMutationID(out.account) || !shareCanonicalUUID(out.inc) || !shareCanonicalUUID(out.profileInc) || !shareCanonicalUUID(out.accountInc) {
		return out, ErrWatchRoomUnavailable
	}
	if !out.expires.After(time.Now()) || out.status != models.WatchRoomStatusLobby && out.status != models.WatchRoomStatusPlaying && out.status != models.WatchRoomStatusPaused {
		return out, ErrWatchRoomChanged
	}
	return out, nil
}
func (o *partyOperation) readProfile(id string, locked bool) (out watchRoomProfile, err error) {
	q := `SELECT id,CASE WHEN octet_length(account_id) BETWEEN 1 AND 256 THEN account_id ELSE NULL END,share_incarnation::text,CASE WHEN octet_length(name)<=4096 THEN name ELSE NULL END,CASE WHEN octet_length(color)<=64 THEN color ELSE NULL END,CASE WHEN octet_length(icon_url)<=8192 THEN icon_url ELSE NULL END,allow_share_links,is_kids_profile,CASE WHEN pin_hash='' AND pin_length=0 THEN false WHEN octet_length(pin_hash) BETWEEN 1 AND 4096 AND pin_length BETWEEN 0 AND 72 THEN true ELSE NULL END,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id=$1`
	if locked {
		q += ` FOR UPDATE`
	}
	e := o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.owner, &out.inc, &out.name, &out.color, &out.icon, &out.share, &out.kids, &out.pin, &out.physical)
	if e != nil || !parentMutationID(out.id) || !parentMutationID(out.owner) || !shareCanonicalUUID(out.inc) {
		return out, ErrWatchRoomUnavailable
	}
	return out, nil
}
func (o *partyOperation) currentRoom(id string) error {
	first, e := o.readRoom(id, false)
	if e != nil {
		return e
	}
	o.tighten(first.expires)
	if e = o.chargeAccount(first.account); e != nil {
		return e
	}
	observed, e := o.readProfile(first.profile, false)
	if e != nil || observed.owner != first.account {
		return ErrWatchRoomChanged
	}
	account, e := shareAccount(o.ctx, o.g.tx, first.account, false)
	if e != nil {
		return ErrWatchRoomChanged
	}
	if account.expires != nil {
		o.tighten(*account.expires)
	}
	o.account, e = shareAccount(o.ctx, o.g.tx, first.account, true)
	if e != nil || o.account.id != account.id || o.account.inc != account.inc || o.account.master != account.master || (o.account.expires == nil) != (account.expires == nil) || o.account.expires != nil && !o.account.expires.Equal(*account.expires) {
		return ErrWatchRoomChanged
	}
	if o.account.expires != nil {
		o.tighten(*o.account.expires)
	}
	o.profile, e = o.readProfile(first.profile, true)
	if e != nil || !watchRoomProfileSame(observed, o.profile) || o.profile.owner != first.account {
		return ErrWatchRoomChanged
	}
	o.room, e = o.readRoom(id, true)
	if e != nil || o.room != first {
		return ErrWatchRoomChanged
	}
	if o.room.profileInc != o.profile.inc || o.room.accountInc != o.account.inc {
		return ErrWatchRoomRecreate
	}
	if !o.profile.share || o.profile.pin || o.profile.kids {
		return ErrWatchRoomForbidden
	}
	if e = shareLockPolicyRelations(o.ctx, o.g.tx); e != nil {
		return ErrWatchRoomUnavailable
	}
	if e = shareLockRelations(o.ctx, o.g.tx, []string{"watch_rooms", "watch_room_provenance", "watch_room_members", "watch_room_guest_members", "watch_room_external_invites", "watch_room_invite_provenance", "watch_room_external_sources", "watch_room_source_provenance", "watch_party_guest_bindings", "watch_party_playback_bindings", "sessions"}); e != nil {
		return ErrWatchRoomUnavailable
	}
	o.household, e = shareHousehold(o.ctx, o.g.tx, o.room.account)
	if e != nil {
		return ErrWatchRoomForbidden
	}
	return o.fence()
}
func (o *partyOperation) readAdmission(selector string, byCode, locked bool) (out partyAdmission, err error) {
	predicate := `i.token_hash=$1`
	if byCode {
		predicate = `i.short_code=$1`
	}
	q := `SELECT CASE WHEN octet_length(i.room_id) BETWEEN 1 AND 256 THEN i.room_id ELSE NULL END,p.room_incarnation::text,p.admission_nonce::text,p.admission_sha256,i.token_hash,i.short_code,p.physical_receipt,concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text),i.created_at,i.expires_at FROM watch_room_external_invites i JOIN watch_room_invite_provenance p ON p.room_id=i.room_id AND p.version=1 AND p.token_hash=i.token_hash AND p.short_code=i.short_code AND p.created_at=i.created_at AND p.expires_at=i.expires_at WHERE i.active AND ` + predicate
	if locked {
		q += ` FOR UPDATE OF i,p`
	}
	var actual string
	e := o.g.tx.QueryRow(o.ctx, q, selector).Scan(&out.room, &out.roomInc, &out.nonce, &out.hash, &out.tokenHash, &out.code, &out.physical, &actual, &out.created, &out.expires)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if e != nil {
		return out, ErrWatchRoomUnavailable
	}
	hash, e := watchRoomJSONHash([]any{out.room, out.roomInc, out.tokenHash, out.code, true, out.created.UTC(), out.expires.UTC()})
	if e != nil || out.physical != actual || hash != out.hash || !parentMutationID(out.room) || !shareCanonicalUUID(out.roomInc) || !shareCanonicalUUID(out.nonce) || !out.expires.After(time.Now()) {
		return out, ErrWatchRoomChanged
	}
	return out, nil
}
func (o *partyOperation) invitation(secret string, byCode bool) (partyAdmission, error) {
	selector, e := partySecret(secret, byCode)
	if e != nil {
		return partyAdmission{}, e
	}
	first, e := o.readAdmission(selector, byCode, false)
	if e != nil {
		return first, e
	}
	o.tighten(first.expires)
	if e = o.currentRoom(first.room); e != nil {
		return first, e
	}
	current, e := o.readAdmission(selector, byCode, true)
	if e != nil || current != first || current.roomInc != o.room.inc {
		return current, ErrWatchRoomChanged
	}
	return current, nil
}
func (a *WatchRoomAuthority) ResolveInvitation(r *http.Request, secret string, byCode bool) (view *WatchPartyInvitationView, err error) {
	if _, e := partySecret(secret, byCode); e != nil {
		return nil, e
	}
	op, e := a.beginParty(r)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			view = nil
		}
	}()
	admission, e := op.invitation(secret, byCode)
	if e != nil {
		return nil, e
	}
	var title string
	if e = op.g.tx.QueryRow(op.ctx, `SELECT CASE WHEN octet_length(title)<=4096 THEN title ELSE NULL END FROM watch_rooms WHERE id=$1`, op.room.id).Scan(&title); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = op.fence(); e != nil {
		return nil, e
	}
	view = &WatchPartyInvitationView{RoomID: op.room.id, Title: title, CreatorName: op.profile.name, ShortCode: admission.code, ExpiresAt: admission.expires, PublicationUntil: op.until}
	return view, nil
}

func (o *partyOperation) memberCount() (int, error) {
	var count int
	e := o.g.tx.QueryRow(o.ctx, `SELECT (SELECT count(*) FROM watch_room_members WHERE room_id=$1)+(SELECT count(*) FROM watch_room_guest_members WHERE room_id=$1)`, o.room.id).Scan(&count)
	if e != nil || count < 0 || count > watchPartyRoomMembers {
		return 0, ErrWatchRoomUnavailable
	}
	return count, nil
}
func (o *partyOperation) guestFromToken(token string, locked bool) (out partyGuestSnapshot, err error) {
	if !partyCanonicalToken(token) {
		return out, ErrWatchRoomForbidden
	}
	q := `SELECT b.binding_id::text,b.room_id,b.room_incarnation::text,b.guest_id,b.guest_incarnation::text,b.creator_profile_id,b.creator_profile_incarnation::text,b.creator_account_id,b.creator_account_incarnation::text,b.created_at,b.expires_at FROM watch_party_guest_bindings b JOIN sessions s ON s.token=b.session_token AND s.watch_party_guest_binding_id=b.binding_id JOIN watch_room_guest_members g ON g.room_id=b.room_id AND g.guest_id=b.guest_id AND g.guest_incarnation=b.guest_incarnation WHERE b.session_token=$1 AND b.version=1 AND s.scope='watch_party' AND s.scope_resource=b.room_id AND NOT s.is_master AND s.account_id=b.creator_account_id AND s.created_at=b.created_at AND s.expires_at=b.expires_at`
	if locked {
		q += ` FOR UPDATE OF b,s,g`
	}
	e := o.g.tx.QueryRow(o.ctx, q, token).Scan(&out.binding, &out.room, &out.roomInc, &out.guest, &out.guestInc, &out.profile, &out.profileInc, &out.account, &out.accountInc, &out.created, &out.expires)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrWatchRoomForbidden
	}
	if e != nil {
		return out, ErrWatchRoomUnavailable
	}
	if out.room != o.room.id || out.roomInc != o.room.inc || out.profile != o.profile.id || out.profileInc != o.profile.inc || out.account != o.account.id || out.accountInc != o.account.inc || out.guest != partyGuestID(token) || !shareCanonicalUUID(out.binding) || !shareCanonicalUUID(out.guestInc) || !out.expires.After(time.Now()) {
		return out, ErrWatchRoomChanged
	}
	o.tighten(out.expires)
	frame, _, e := readPGRequestFrame(o.ctx, o.g.tx, token)
	if e != nil {
		return out, ErrWatchRoomChanged
	}
	out.session = frame.session
	out.sessionRow = frame.sessionRow
	out.actorRow = frame.actorRow
	if out.session.Scope != models.SessionScopeWatchParty || out.session.ScopeResource != out.room || out.session.IsMaster || !out.session.ExpiresAt.Equal(out.expires) || !out.session.CreatedAt.Equal(out.created) {
		return out, ErrWatchRoomChanged
	}
	return out, nil
}
func (a *WatchRoomAuthority) JoinInvitation(r *http.Request, secret string, byCode bool, name, existingGuestToken string) (result *WatchPartyJoinResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}
	if _, e := partySecret(secret, byCode); e != nil {
		return nil, e
	}
	name = strings.TrimSpace(name)
	if name == "" || !utf8.ValidString(name) || len(name) > 160 || utf8.RuneCountInString(name) > 40 || len(r.UserAgent()) > 4096 || !utf8.ValidString(r.UserAgent()) || len(r.RemoteAddr) > 256 || len(existingGuestToken) > 256 {
		return nil, ErrWatchRoomInvalid
	}
	op, e := a.beginParty(r)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			result = nil
		}
	}()
	admission, e := op.invitation(secret, byCode)
	if e != nil {
		return nil, e
	}
	var session models.Session
	var guest string
	if existingGuestToken != "" {
		// An invalid cookie does not select another room or inherit an old identity.
		// A genuine same-room guest retains the exact original fixed session lifetime.
		previous, e := op.guestFromToken(existingGuestToken, true)
		if e == nil {
			session = previous.session
			guest = previous.guest
		} else if !errors.Is(e, ErrWatchRoomForbidden) {
			return nil, e
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	caps := []byte(`{"nativePlayback":false,"stateSync":true,"protocolVersion":1}`)
	if session.Token == "" {
		count, e := op.memberCount()
		if e != nil {
			return nil, e
		}
		if count >= watchPartyRoomMembers {
			return nil, ErrWatchRoomBusy
		}
		token, e := partyNewToken()
		if e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		guest = partyGuestID(token)
		expiry := parentMutationMin(op.room.expires, op.account.expires)
		if !expiry.After(now) {
			return nil, ErrWatchRoomChanged
		}
		session = models.Session{Token: token, AccountID: op.account.id, Scope: models.SessionScopeWatchParty, ScopeResource: op.room.id, CreatedAt: now, ExpiresAt: expiry, UserAgent: r.UserAgent(), IPAddress: watchRoomActualPeer(r)}
		binding, e := uuid.NewRandom()
		if e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		var guestInc string
		if e = op.g.tx.QueryRow(op.ctx, `INSERT INTO watch_room_guest_members(room_id,guest_id,name,client_id,joined_at,last_seen_at,capabilities)VALUES($1,$2,$3,$4,$5,$5,$6)RETURNING guest_incarnation::text`, op.room.id, guest, name, r.UserAgent(), now, caps).Scan(&guestInc); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		if !shareCanonicalUUID(guestInc) {
			return nil, ErrWatchRoomUnavailable
		}
		if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO sessions(token,account_id,is_master,scope,scope_resource,created_at,expires_at,user_agent,ip_address,client_id,watch_party_guest_binding_id)VALUES($1,$2,false,'watch_party',$3,$4,$5,$6,$7,'',$8::uuid)`, token, op.account.id, op.room.id, now, expiry, session.UserAgent, session.IPAddress, binding.String()); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_party_guest_bindings(session_token,binding_id,room_id,room_incarnation,guest_id,guest_incarnation,creator_profile_id,creator_profile_incarnation,creator_account_id,creator_account_incarnation,admission_nonce,admission_sha256,created_at,expires_at)VALUES($1,$2::uuid,$3,$4::uuid,$5,$6::uuid,$7,$8::uuid,$9,$10::uuid,$11::uuid,$12,$13,$14)`, token, binding.String(), op.room.id, op.room.inc, guest, guestInc, op.profile.id, op.profile.inc, op.account.id, op.account.inc, admission.nonce, admission.hash, now, expiry); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	} else {
		if _, e = op.g.tx.Exec(op.ctx, `UPDATE watch_room_guest_members SET name=$3,client_id=$4,last_seen_at=$5,capabilities=$6 WHERE room_id=$1 AND guest_id=$2`, op.room.id, guest, name, r.UserAgent(), now, caps); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	op.tighten(session.ExpiresAt)
	if e = op.commit(); e != nil {
		return nil, e
	}
	return &WatchPartyJoinResult{Session: session, RoomID: op.room.id, GuestID: guest, PublicationUntil: op.until}, nil
}
func (a *WatchRoomAuthority) AuthenticateGuestRequest(r *http.Request, token, roomID string) (checked *http.Request, release context.CancelFunc, err error) {
	if !a.valid() || r == nil || !partyCanonicalToken(token) || !parentMutationID(roomID) {
		return nil, nil, ErrWatchRoomForbidden
	}
	actual, stop, e := a.store.AuthenticateProfilePinRequest(r, token, "")
	if e != nil {
		if errors.Is(e, ErrProfilePinAccessChanged) {
			return nil, nil, ErrWatchRoomForbidden
		}
		return nil, nil, errors.Join(ErrWatchRoomUnavailable, e)
	}
	defer func() {
		if err != nil {
			stop()
		}
	}()
	op, e := a.beginParty(actual)
	if e != nil {
		return nil, nil, e
	}
	op.frame, _ = actual.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if e = op.currentRoom(roomID); e == nil {
		var guest partyGuestSnapshot
		guest, e = op.guestFromToken(token, true)
		if e == nil && (op.frame == nil || op.frame.self != op.frame || op.frame.store != a.store || op.frame.session != guest.session || !sameParentMutationRow(op.frame.sessionRow, guest.sessionRow) || !sameParentMutationRow(op.frame.actorRow, guest.actorRow)) {
			e = ErrWatchRoomChanged
		}
	}
	e = op.finish(e)
	if e != nil {
		return nil, nil, e
	}
	return actual, stop, nil
}
func (a *WatchRoomAuthority) beginOriginalGuest(r *http.Request, roomID string) (*partyOperation, partyGuestSnapshot, error) {
	var empty partyGuestSnapshot
	if !a.valid() || r == nil || a.store.CheckProfilePinRequestFrame(r) != nil {
		return nil, empty, ErrWatchRoomForbidden
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	claimed, ok := auth.GetSession(r)
	if !ok || frame == nil || frame.self != frame || frame.store != a.store || frame.session != claimed || claimed.Scope != models.SessionScopeWatchParty || claimed.ScopeResource != roomID || claimed.IsMaster {
		return nil, empty, ErrWatchRoomForbidden
	}
	op, e := a.beginParty(r)
	if e != nil {
		return nil, empty, e
	}
	op.frame = frame
	if e = op.currentRoom(roomID); e != nil {
		_ = op.finish(e)
		return nil, empty, e
	}
	guest, e := op.guestFromToken(claimed.Token, true)
	if e != nil || guest.session != claimed || !sameParentMutationRow(guest.sessionRow, frame.sessionRow) || !sameParentMutationRow(guest.actorRow, frame.actorRow) {
		if e == nil {
			e = ErrWatchRoomChanged
		}
		_ = op.finish(e)
		return nil, empty, e
	}
	return op, guest, nil
}

func (o *partyOperation) roomData() (*models.WatchRoom, error) {
	count, e := o.memberCount()
	if e != nil {
		return nil, e
	}
	var bytes int64
	e = o.g.tx.QueryRow(o.ctx, `SELECT 6*(octet_length(r.id)+octet_length(r.creator_profile_id)+octet_length(r.title)+octet_length(r.media_type)+octet_length(r.item_id)+octet_length(r.poster_url)+octet_length(r.backdrop_url)+octet_length(r.params::text)+octet_length(r.updated_by)+COALESCE((SELECT sum(octet_length(u.id)+octet_length(u.name)+octet_length(u.color)+octet_length(u.icon_url)+octet_length(m.client_id)+octet_length(m.capabilities::text)) FROM watch_room_members m JOIN users u ON u.id=m.profile_id WHERE m.room_id=r.id),0)+COALESCE((SELECT sum(octet_length(g.guest_id)+octet_length(g.name)+octet_length(g.client_id)+octet_length(g.capabilities::text)) FROM watch_room_guest_members g WHERE g.room_id=r.id),0))+$2::bigint*2048+65536 FROM watch_rooms r WHERE r.id=$1`, o.room.id, count).Scan(&bytes)
	if e != nil || bytes < 0 || bytes > watchPartyRoomBytes {
		return nil, ErrWatchRoomUnavailable
	}
	out := &models.WatchRoom{ID: o.room.id, CreatorProfileID: o.room.profile, CreatorName: o.profile.name, CreatedAt: o.room.created, ExpiresAt: o.room.expires, Members: make([]models.WatchRoomMember, 0, count)}
	var params []byte
	e = o.g.tx.QueryRow(o.ctx, `SELECT CASE WHEN octet_length(title)<=4096 THEN title ELSE NULL END,CASE WHEN octet_length(media_type)<=64 THEN media_type ELSE NULL END,CASE WHEN octet_length(item_id)<=1024 THEN item_id ELSE NULL END,CASE WHEN octet_length(poster_url)<=8192 THEN poster_url ELSE NULL END,CASE WHEN octet_length(backdrop_url)<=8192 THEN backdrop_url ELSE NULL END,CASE WHEN octet_length(params::text)<=1048576 THEN params ELSE NULL END,status,waiting_for_ready,position,duration,revision,CASE WHEN octet_length(updated_by)<=262 THEN updated_by ELSE NULL END,anchor_updated_at FROM watch_rooms WHERE id=$1`, o.room.id).Scan(&out.Title, &out.MediaType, &out.ItemID, &out.PosterURL, &out.BackdropURL, &params, &out.Status, &out.WaitingForReady, &out.Position, &out.Duration, &out.Revision, &out.UpdatedBy, &out.AnchorUpdatedAt)
	if e != nil || len(params) == 0 || json.Unmarshal(params, &out.Params) != nil {
		return nil, ErrWatchRoomUnavailable
	}
	rows, e := o.g.tx.Query(o.ctx, `SELECT false,CASE WHEN octet_length(u.id) BETWEEN 1 AND 256 THEN u.id ELSE NULL END,CASE WHEN octet_length(u.name)<=4096 THEN u.name ELSE NULL END,CASE WHEN octet_length(u.color)<=64 THEN u.color ELSE NULL END,CASE WHEN octet_length(u.icon_url)<=8192 THEN u.icon_url ELSE NULL END,CASE WHEN octet_length(m.client_id)<=4096 THEN m.client_id ELSE NULL END,m.is_creator,m.ready,m.buffering,m.joined_at,m.last_seen_at,CASE WHEN octet_length(m.capabilities::text)<=4096 THEN m.capabilities ELSE NULL END FROM watch_room_members m JOIN users u ON u.id=m.profile_id WHERE m.room_id=$1 UNION ALL SELECT true,CASE WHEN octet_length(g.guest_id) BETWEEN 1 AND 256 THEN 'guest:'||g.guest_id ELSE NULL END,CASE WHEN octet_length(g.name)<=160 THEN g.name ELSE NULL END,'','',CASE WHEN octet_length(g.client_id)<=4096 THEN g.client_id ELSE NULL END,false,g.ready,g.buffering,g.joined_at,g.last_seen_at,CASE WHEN octet_length(g.capabilities::text)<=4096 THEN g.capabilities ELSE NULL END FROM watch_room_guest_members g WHERE g.room_id=$1`, o.room.id)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	defer rows.Close()
	now := time.Now().UTC()
	for rows.Next() {
		if len(out.Members) >= watchPartyRoomMembers {
			return nil, ErrWatchRoomUnavailable
		}
		var m models.WatchRoomMember
		var caps []byte
		if e = rows.Scan(&m.IsGuest, &m.ProfileID, &m.Name, &m.Color, &m.IconURL, &m.ClientID, &m.IsCreator, &m.Ready, &m.Buffering, &m.JoinedAt, &m.LastSeenAt, &caps); e != nil || len(caps) == 0 || json.Unmarshal(caps, &m.Capabilities) != nil {
			return nil, ErrWatchRoomUnavailable
		}
		m.Joined = true
		m.Present = now.Sub(m.LastSeenAt) <= 15*time.Second
		out.Members = append(out.Members, m)
	}
	if rows.Err() != nil || len(out.Members) != count {
		return nil, ErrWatchRoomUnavailable
	}
	sort.SliceStable(out.Members, func(i, j int) bool {
		a, b := out.Members[i], out.Members[j]
		if a.IsGuest != b.IsGuest {
			return !a.IsGuest
		}
		if !a.IsGuest && a.IsCreator != b.IsCreator {
			return a.IsCreator
		}
		if !a.JoinedAt.Equal(b.JoinedAt) {
			return a.JoinedAt.Before(b.JoinedAt)
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ProfileID < b.ProfileID
	})
	if out.Status == models.WatchRoomStatusPlaying && !out.WaitingForReady {
		out.Position += now.Sub(out.AnchorUpdatedAt).Seconds()
		if out.Duration > 0 && out.Position > out.Duration {
			out.Position = out.Duration
		}
	}
	encoded, e := json.Marshal(out)
	if e != nil || len(encoded) > watchPartyRoomBytes {
		return nil, ErrWatchRoomUnavailable
	}
	return out, nil
}
func (a *WatchRoomAuthority) ReadGuestRoom(r *http.Request, roomID string) (result *WatchPartyGuestRoomData, err error) {
	if r == nil || len(r.UserAgent()) > 4096 || !utf8.ValidString(r.UserAgent()) {
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
	if r.Method == http.MethodGet {
		buffering, _ := strconv.ParseBool(r.URL.Query().Get("buffering"))
		if _, e = op.g.tx.Exec(op.ctx, `UPDATE watch_room_guest_members SET client_id=$3,buffering=$4,last_seen_at=$5 WHERE room_id=$1 AND guest_id=$2`, roomID, guest.guest, r.UserAgent(), buffering, time.Now().UTC().Truncate(time.Microsecond)); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	room, e := op.roomData()
	if e != nil {
		return nil, e
	}
	var present bool
	if e = op.g.tx.QueryRow(op.ctx, `SELECT EXISTS(SELECT 1 FROM watch_room_external_sources WHERE room_id=$1)`, roomID).Scan(&present); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	ready := false
	if present {
		if _, _, e = op.source(); e != nil {
			return nil, e
		}
		ready = true
	}
	if r.Method == http.MethodGet {
		e = op.commit()
	} else {
		e = op.fence()
	}
	if e != nil {
		return nil, e
	}
	return &WatchPartyGuestRoomData{Room: room, PlaybackReady: ready, PublicationUntil: op.until}, nil
}

func (a *WatchRoomAuthority) SetGuestReady(r *http.Request, roomID string, ready bool) (err error) {
	if r == nil || r.Method != http.MethodPost {
		return ErrWatchRoomInvalid
	}
	op, guest, e := a.beginOriginalGuest(r, roomID)
	if e != nil {
		return e
	}
	defer func() { err = op.finish(err) }()
	if _, e = op.memberCount(); e != nil {
		return e
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, e = op.g.tx.Exec(op.ctx, `UPDATE watch_room_guest_members SET ready=$3,last_seen_at=$4 WHERE room_id=$1 AND guest_id=$2`, roomID, guest.guest, ready, now); e != nil {
		return ErrWatchRoomUnavailable
	}
	if ready {
		if _, e = op.g.tx.Exec(op.ctx, `UPDATE watch_rooms SET waiting_for_ready=false,status='playing',revision=revision+1,updated_by='guest:'||$2,anchor_updated_at=$3 WHERE id=$1 AND waiting_for_ready AND NOT EXISTS(SELECT 1 FROM watch_room_members WHERE room_id=$1 AND NOT ready) AND NOT EXISTS(SELECT 1 FROM watch_room_guest_members WHERE room_id=$1 AND NOT ready)`, roomID, guest.guest, now); e != nil {
			return ErrWatchRoomUnavailable
		}
	}
	return op.commit()
}
func (a *WatchRoomAuthority) HeartbeatGuest(r *http.Request, roomID string, buffering bool) (err error) {
	if r == nil || r.Method != http.MethodPost || len(r.UserAgent()) > 4096 || !utf8.ValidString(r.UserAgent()) {
		return ErrWatchRoomInvalid
	}
	op, guest, e := a.beginOriginalGuest(r, roomID)
	if e != nil {
		return e
	}
	defer func() { err = op.finish(err) }()
	if _, e = op.g.tx.Exec(op.ctx, `UPDATE watch_room_guest_members SET client_id=$3,buffering=$4,last_seen_at=$5 WHERE room_id=$1 AND guest_id=$2`, roomID, guest.guest, r.UserAgent(), buffering, time.Now().UTC().Truncate(time.Microsecond)); e != nil {
		return ErrWatchRoomUnavailable
	}
	return op.commit()
}
