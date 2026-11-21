package datastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrWatchRoomUnavailable = errors.New("watch room operation unavailable")
	ErrWatchRoomChanged     = errors.New("watch room changed or expired")
	ErrWatchRoomForbidden   = errors.New("watch room operation not permitted")
	ErrWatchRoomRecreate    = errors.New("watch room requires recreation")
	ErrWatchRoomConflict    = errors.New("watch room source conflicts")
	ErrWatchRoomInvalid     = errors.New("watch room input invalid")
	ErrWatchRoomUnknown     = errors.New("watch room commit outcome unknown")
	ErrWatchRoomBusy        = errors.New("watch room operation capacity unavailable")
)

// WatchRoomAuthority writes original room/invite/source provenance DATA. It
// cannot admit a guest, mint media/PIN permission or replace a scoped owner.
type WatchRoomAuthority struct {
	self        *WatchRoomAuthority
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	active      int
	httpActive  int
	httpPeers   map[string]int
	actors      map[string]int
	partyOwners map[*WatchPartyPlaybackOwner]struct{}
	operations  sync.WaitGroup
	once        sync.Once
	done        chan struct{}
}

func NewWatchRoomAuthority(lifetime context.Context, store *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*WatchRoomAuthority, error) {
	if lifetime == nil || lifetime.Err() != nil || !store.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrWatchRoomUnavailable
	}
	ctx, cancel := context.WithCancel(lifetime)
	a := &WatchRoomAuthority{store: store, manager: manager, security: security, coordinator: coordinator, ctx: ctx, cancel: cancel, actors: make(map[string]int), partyOwners: make(map[*WatchPartyPlaybackOwner]struct{}), httpPeers: make(map[string]int), done: make(chan struct{})}
	a.self = a
	return a, nil
}
func (a *WatchRoomAuthority) valid() bool {
	return a != nil && a.self == a && a.ctx != nil && a.ctx.Err() == nil && a.store.SettingsCommitReady(a.coordinator) && a.security.Ready(a.manager, a.coordinator)
}
func (a *WatchRoomAuthority) MatchesStore(store *DataStore) bool {
	return a.valid() && a.store == store
}
func (a *WatchRoomAuthority) Context() context.Context {
	if !a.valid() {
		return familyMediaClosedContext()
	}
	return a.ctx
}
func (WatchRoomAuthority) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchRoomAuthority(opaque)"))
}
func (WatchRoomAuthority) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchRoomAuthority) UnmarshalJSON([]byte) error  { return ErrWatchRoomUnavailable }
func (a *WatchRoomAuthority) Close() {
	if a == nil || a.self != a {
		return
	}
	a.once.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.cancel()
		owners := make([]*WatchPartyPlaybackOwner, 0, len(a.partyOwners))
		for owner := range a.partyOwners {
			owners = append(owners, owner)
		}
		a.mu.Unlock()
		for _, owner := range owners {
			owner.Close()
		}
		a.operations.Wait()
		close(a.done)
	})
	<-a.done
}
func (a *WatchRoomAuthority) enter(actor string) bool {
	if !a.valid() {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx.Err() != nil || a.active >= 32 || a.actors[actor] >= 2 {
		return false
	}
	a.active++
	a.actors[actor]++
	a.operations.Add(1)
	return true
}
func (a *WatchRoomAuthority) leave(actor string) {
	a.mu.Lock()
	a.active--
	a.actors[actor]--
	if a.actors[actor] == 0 {
		delete(a.actors, actor)
	}
	a.mu.Unlock()
	a.operations.Done()
}

type watchRoomOperation struct {
	authority   *WatchRoomAuthority
	request     *http.Request
	frame       *pgRequestFrame
	original    models.Session
	ctx         context.Context
	cancel      context.CancelFunc
	stopRuntime func()
	children    []context.CancelFunc
	deadline    time.Time
	policy      *config.SecurityPolicyObservation
	g           *settingsProgressTx
	profile     string
	account     shareAccountRow
}

func (a *WatchRoomAuthority) begin(r *http.Request, profile string) (o *watchRoomOperation, err error) {
	if !a.valid() || r == nil || r.URL == nil || r.Context().Err() != nil || !parentMutationID(profile) {
		return nil, ErrWatchRoomUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != a.store || frame.ctx == nil || frame.ctx.Err() != nil {
		return nil, ErrWatchRoomForbidden
	}
	original, ok := auth.GetSession(r)
	if !ok || original != frame.session || original.Scope != "" || original.ScopeResource != "" || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster {
		return nil, ErrWatchRoomForbidden
	}
	if !a.enter(original.AccountID) {
		return nil, ErrWatchRoomBusy
	}
	deadline := parentMutationMin(time.Now().Add(30*time.Second), &original.ExpiresAt, frame.actorExpiry)
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	o = &watchRoomOperation{authority: a, request: r, frame: frame, original: original, ctx: ctx, cancel: cancel, deadline: deadline, profile: profile}
	o.stopRuntime = familyMediaAfterFunc(a.ctx, cancel)
	defer func() {
		if err != nil {
			_ = o.finish(err)
			o = nil
		}
	}()
	o.policy, err = a.security.CapturePolicy(ctx)
	if err != nil {
		return o, ErrWatchRoomUnavailable
	}
	o.g, err = a.store.beginSettingsProgressTx(ctx, a.coordinator)
	if err != nil {
		return o, ErrWatchRoomUnavailable
	}
	o.ctx = o.g.ctx
	account, e := shareAccount(o.ctx, o.g.tx, original.AccountID, false)
	if e != nil {
		return o, ErrWatchRoomChanged
	}
	if account.expires != nil {
		o.tighten(*account.expires)
	}
	return o, nil
}
func (o *watchRoomOperation) tighten(at time.Time) {
	if at.Before(o.deadline) {
		o.deadline = at
		next, stop := context.WithDeadline(o.ctx, at)
		o.children = append(o.children, stop)
		o.ctx = next
	}
}
func (o *watchRoomOperation) finish(previous error) error {
	// Save actual work cancellation before the intentional transaction cleanup
	// retires its private context; never interpret that retirement as revocation.
	workErr := o.ctx.Err()
	if o.g != nil {
		if e := finishShareCreationTransaction(o.g); previous == nil && e != nil {
			previous = errors.Join(ErrWatchRoomUnavailable, e)
		}
	}
	if previous == nil && (workErr != nil || o.request.Context().Err() != nil || o.frame.ctx.Err() != nil || !o.authority.valid() || !o.deadline.After(time.Now())) {
		previous = ErrWatchRoomChanged
	}
	for _, stop := range o.children {
		stop()
	}
	o.stopRuntime()
	o.cancel()
	o.authority.leave(o.original.AccountID)
	return previous
}
func (o *watchRoomOperation) lockActor() error {
	account, e := shareAccount(o.ctx, o.g.tx, o.original.AccountID, true)
	if e != nil {
		return ErrWatchRoomChanged
	}
	o.account = account
	if account.expires != nil {
		o.tighten(*account.expires)
	}
	var token string
	if e = o.g.tx.QueryRow(o.ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, o.original.Token).Scan(&token); e != nil {
		return ErrWatchRoomChanged
	}
	current, _, e := readPGRequestFrame(o.ctx, o.g.tx, token)
	if e != nil || current.session != o.original || !sameParentMutationRow(current.sessionRow, o.frame.sessionRow) || !sameParentMutationRow(current.actorRow, o.frame.actorRow) {
		return ErrWatchRoomChanged
	}
	return nil
}
func (o *watchRoomOperation) fence() error {
	if o.ctx.Err() != nil || o.request.Context().Err() != nil || o.frame.ctx.Err() != nil || !o.authority.valid() || !o.deadline.After(time.Now()) {
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
func (o *watchRoomOperation) commit() error {
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
func watchRoomJSONHash(value any) (string, error) {
	raw, e := json.Marshal(value)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func watchRoomText(v string, max int, required bool) bool {
	return utf8.ValidString(v) && len(v) <= max && (!required || v != "") && !strings.ContainsRune(v, 0)
}
func watchRoomParams(params map[string]string) (map[string]string, []byte, error) {
	if len(params) > 64 {
		return nil, nil, ErrWatchRoomInvalid
	}
	copy := make(map[string]string, len(params))
	for k, v := range params {
		if !watchRoomText(k, 128, true) || !watchRoomText(v, 1<<20, false) {
			return nil, nil, ErrWatchRoomInvalid
		}
		copy[k] = v
	}
	raw, e := json.Marshal(copy)
	if e != nil || len(raw) > 1<<20 {
		return nil, nil, ErrWatchRoomInvalid
	}
	return copy, raw, nil
}

type watchRoomProfile struct {
	id, owner, inc, name, color, icon, physical string
	share, kids, pin                            bool
}

func (o *watchRoomOperation) readProfile(id string, locked bool) (out watchRoomProfile, err error) {
	q := `SELECT id,CASE WHEN octet_length(account_id) BETWEEN 1 AND 256 THEN account_id ELSE NULL END,share_incarnation::text,CASE WHEN octet_length(name)<=4096 THEN name ELSE NULL END,CASE WHEN octet_length(color)<=64 THEN color ELSE NULL END,CASE WHEN octet_length(icon_url)<=8192 THEN icon_url ELSE NULL END,allow_share_links,is_kids_profile,CASE WHEN pin_hash='' AND pin_length=0 THEN false WHEN octet_length(pin_hash) BETWEEN 1 AND 4096 AND pin_length BETWEEN 0 AND 72 THEN true ELSE NULL END,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id=$1`
	if locked {
		q += ` FOR UPDATE`
	}
	err = o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.owner, &out.inc, &out.name, &out.color, &out.icon, &out.share, &out.kids, &out.pin, &out.physical)
	if err != nil {
		return out, err
	}
	if out.id != id || !parentMutationID(out.owner) || !shareCanonicalUUID(out.inc) || !watchRoomText(out.name, 4096, false) || !watchRoomText(out.color, 64, false) || !watchRoomText(out.icon, 8192, false) {
		return out, ErrWatchRoomUnavailable
	}
	return out, nil
}
func watchRoomProfileSame(a, b watchRoomProfile) bool { return a == b }
func (a *WatchRoomAuthority) CreateRoom(r *http.Request, creator string, in models.WatchRoomCreate) (room *models.WatchRoom, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}
	in.Title = strings.TrimSpace(in.Title)
	in.MediaType = strings.TrimSpace(in.MediaType)
	in.ItemID = strings.TrimSpace(in.ItemID)
	in.PosterURL = strings.TrimSpace(in.PosterURL)
	in.BackdropURL = strings.TrimSpace(in.BackdropURL)
	in.ClientID = strings.TrimSpace(in.ClientID)
	if !watchRoomText(in.Title, 4096, true) || !watchRoomText(in.MediaType, 64, true) || !watchRoomText(in.ItemID, 1024, true) || !watchRoomText(in.PosterURL, 8192, false) || !watchRoomText(in.BackdropURL, 8192, false) || !watchRoomText(in.ClientID, 256, false) || !in.Capabilities.NativePlayback || !in.Capabilities.StateSync || in.Capabilities.ProtocolVersion < 1 || len(in.InviteeProfileIDs) > 64 {
		return nil, ErrWatchRoomInvalid
	}
	owned, params, e := watchRoomParams(in.Params)
	if e != nil {
		return nil, e
	}
	ids := []string{creator}
	seen := map[string]bool{creator: true}
	for _, id := range in.InviteeProfileIDs {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		if !parentMutationID(id) {
			return nil, ErrWatchRoomInvalid
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	op, e := a.begin(r, creator)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			room = nil
		}
	}()
	first, e := op.readProfile(creator, false)
	if e != nil || first.owner != op.original.AccountID {
		return nil, ErrWatchRoomForbidden
	}
	if e = op.lockActor(); e != nil {
		return nil, e
	}
	invitees := make([]string, 0, len(ids))
	profiles := make([]watchRoomProfile, 0, len(ids))
	var selected watchRoomProfile
	for _, id := range ids {
		profile, e := op.readProfile(id, true)
		if errors.Is(e, pgx.ErrNoRows) && id != creator {
			continue
		}
		if e != nil || profile.owner != op.original.AccountID {
			return nil, ErrWatchRoomForbidden
		}
		if id == creator {
			selected = profile
		} else {
			invitees = append(invitees, id)
			profiles = append(profiles, profile)
		}
	}
	if !watchRoomProfileSame(first, selected) {
		return nil, ErrWatchRoomChanged
	}
	if e = shareLockPolicyRelations(op.ctx, op.g.tx); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = shareLockRelations(op.ctx, op.g.tx, []string{"watch_rooms", "watch_room_members", "watch_room_invites", "watch_room_provenance"}); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = op.fence(); e != nil {
		return nil, e
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := parentMutationMin(now.Add(24*time.Hour), op.account.expires)
	if !expiry.After(now) {
		return nil, ErrWatchRoomChanged
	}
	id, e := uuid.NewRandom()
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	room = &models.WatchRoom{ID: id.String(), CreatorProfileID: creator, CreatorName: selected.name, Title: in.Title, MediaType: in.MediaType, ItemID: in.ItemID, PosterURL: in.PosterURL, BackdropURL: in.BackdropURL, Params: owned, Status: models.WatchRoomStatusLobby, UpdatedBy: creator, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: expiry}
	var incarnation string
	e = op.g.tx.QueryRow(op.ctx, `INSERT INTO watch_rooms(id,creator_profile_id,title,media_type,item_id,poster_url,backdrop_url,params,status,position,duration,revision,updated_by,anchor_updated_at,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'lobby',0,0,0,$2,$9,$9,$10) RETURNING room_incarnation::text`, room.ID, creator, in.Title, in.MediaType, in.ItemID, in.PosterURL, in.BackdropURL, params, now, expiry).Scan(&incarnation)
	if e != nil || !shareCanonicalUUID(incarnation) {
		return nil, ErrWatchRoomUnavailable
	}
	if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_room_provenance(room_id,room_incarnation,creator_profile_id,creator_profile_incarnation,creator_account_id,creator_account_incarnation,created_at,expires_at)VALUES($1,$2::uuid,$3,$4::uuid,$5,$6::uuid,$7,$8)`, room.ID, incarnation, creator, selected.inc, op.original.AccountID, op.account.inc, now, expiry); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	caps, e := json.Marshal(in.Capabilities)
	if e != nil {
		return nil, ErrWatchRoomInvalid
	}
	if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_room_members(room_id,profile_id,client_id,is_creator,ready,joined_at,last_seen_at,capabilities)VALUES($1,$2,$3,true,true,$4,$4,$5)`, room.ID, creator, in.ClientID, now, caps); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	for _, profile := range invitees {
		if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_room_invites(room_id,profile_id,invited_at)VALUES($1,$2,$3)`, room.ID, profile, now); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	// Construct only descriptive bounded model DATA from the exact locked
	// original profiles, never unbounded generic metadata reads or another pool.
	room.Members = []models.WatchRoomMember{{ProfileID: creator, Name: selected.name, Color: selected.color, IconURL: selected.icon, ClientID: in.ClientID, IsCreator: true, Ready: true, Joined: true, Present: true, JoinedAt: now, LastSeenAt: now, Capabilities: in.Capabilities}}
	sort.Slice(profiles, func(i, j int) bool {
		if profiles[i].name == profiles[j].name {
			return profiles[i].id < profiles[j].id
		}
		return profiles[i].name < profiles[j].name
	})
	for _, profile := range profiles {
		room.Members = append(room.Members, models.WatchRoomMember{ProfileID: profile.id, Name: profile.name, Color: profile.color, IconURL: profile.icon, JoinedAt: now, LastSeenAt: now.Add(-24 * time.Hour)})
	}
	if err = op.commit(); err != nil {
		return nil, err
	}
	return room, nil
}

// This result only bounds delivery of already-committed descriptive DATA. It
// cannot authorize a guest, reconstruct an original frame or acquire media.
type WatchRoomSourceBindingResult struct {
	Source           *models.WatchRoomExternalSource `json:"-"`
	PublicationUntil time.Time                       `json:"-"`
}
type watchRoomOriginal struct {
	id, profile, account, inc, profileInc, accountInc, physical, status string
	created, expires                                                    time.Time
}

func (o *watchRoomOperation) readRoom(id string, locked bool) (out watchRoomOriginal, err error) {
	q := `SELECT r.id,CASE WHEN octet_length(r.creator_profile_id) BETWEEN 1 AND 256 THEN r.creator_profile_id ELSE NULL END,r.room_incarnation::text,r.created_at,r.expires_at,CASE WHEN octet_length(r.status)<=16 THEN r.status ELSE NULL END,concat(r.tableoid::oid,':',r.xmin::text,':',r.ctid::text),CASE WHEN octet_length(p.creator_account_id) BETWEEN 1 AND 256 THEN p.creator_account_id ELSE NULL END,p.creator_profile_incarnation::text,p.creator_account_incarnation::text
 FROM watch_rooms r JOIN watch_room_provenance p ON p.room_id=r.id AND p.version=1 AND p.room_incarnation=r.room_incarnation AND p.creator_profile_id=r.creator_profile_id AND p.created_at=r.created_at AND p.expires_at=r.expires_at WHERE r.id=$1`
	if locked {
		q += ` FOR UPDATE OF r,p`
	}
	err = o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.profile, &out.inc, &out.created, &out.expires, &out.status, &out.physical, &out.account, &out.profileInc, &out.accountInc)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if err != nil || !parentMutationID(out.id) || !parentMutationID(out.profile) || !parentMutationID(out.account) || !shareCanonicalUUID(out.inc) || !shareCanonicalUUID(out.profileInc) || !shareCanonicalUUID(out.accountInc) {
		return out, ErrWatchRoomUnavailable
	}
	if out.profile != o.profile || out.account != o.original.AccountID || out.status == models.WatchRoomStatusEnded || !out.expires.After(time.Now()) {
		return out, ErrWatchRoomChanged
	}
	if out.status != models.WatchRoomStatusLobby && out.status != models.WatchRoomStatusPlaying && out.status != models.WatchRoomStatusPaused {
		return out, ErrWatchRoomChanged
	}
	return out, nil
}
func (o *watchRoomOperation) externalRoom(roomID string) (room watchRoomOriginal, profile watchRoomProfile, err error) {
	if !parentMutationID(roomID) {
		return room, profile, ErrWatchRoomInvalid
	}
	first, e := o.readRoom(roomID, false)
	if e != nil {
		return room, profile, e
	}
	o.tighten(first.expires)
	observed, e := o.readProfile(o.profile, false)
	if e != nil || observed.owner != o.original.AccountID {
		return room, profile, ErrWatchRoomForbidden
	}
	if e = o.lockActor(); e != nil {
		return room, profile, e
	}
	profile, e = o.readProfile(o.profile, true)
	if e != nil || profile.owner != o.original.AccountID || !watchRoomProfileSame(observed, profile) {
		return room, profile, ErrWatchRoomChanged
	}
	room, e = o.readRoom(roomID, true)
	if e != nil || room != first {
		return room, profile, ErrWatchRoomChanged
	}
	if room.profileInc != profile.inc || room.accountInc != o.account.inc {
		return room, profile, ErrWatchRoomRecreate
	}
	if !profile.share || profile.kids || profile.pin {
		return room, profile, ErrWatchRoomForbidden
	}
	if e = shareLockPolicyRelations(o.ctx, o.g.tx); e != nil {
		return room, profile, ErrWatchRoomUnavailable
	}
	if e = shareLockRelations(o.ctx, o.g.tx, []string{"watch_rooms", "watch_room_provenance", "watch_room_external_invites", "watch_room_invite_provenance", "watch_room_external_sources", "watch_room_source_provenance"}); e != nil {
		return room, profile, ErrWatchRoomUnavailable
	}
	if _, e = shareHousehold(o.ctx, o.g.tx, o.original.AccountID); e != nil {
		return room, profile, ErrWatchRoomForbidden
	}
	if e = o.fence(); e != nil {
		return room, profile, e
	}
	return room, profile, nil
}
func (a *WatchRoomAuthority) ReplaceExternalInvite(r *http.Request, profile, roomID string) (invite *models.WatchRoomExternalInvite, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}
	op, e := a.begin(r, profile)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			invite = nil
		}
	}()
	room, _, e := op.externalRoom(roomID)
	if e != nil {
		return nil, e
	}
	raw := make([]byte, 32)
	if _, e = rand.Read(raw); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	code := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:5])
	clear(raw)
	code = code[:4] + "-" + code[4:8]
	digest := sha256.Sum256([]byte(token))
	tokenHash := base64.RawURLEncoding.EncodeToString(digest[:])
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := parentMutationMin(room.expires, op.account.expires)
	if !expiry.After(now) {
		return nil, ErrWatchRoomChanged
	}
	var physical string
	e = op.g.tx.QueryRow(op.ctx, `INSERT INTO watch_room_external_invites(room_id,token_hash,short_code,active,created_at,expires_at)VALUES($1,$2,$3,true,$4,$5)ON CONFLICT(room_id)DO UPDATE SET token_hash=EXCLUDED.token_hash,short_code=EXCLUDED.short_code,active=true,created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at RETURNING concat(tableoid::oid,':',xmin::text,':',ctid::text)`, roomID, tokenHash, code, now, expiry).Scan(&physical)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	hash, e := watchRoomJSONHash([]any{roomID, room.inc, tokenHash, code, true, now, expiry})
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_room_invite_provenance(room_id,room_incarnation,token_hash,short_code,admission_sha256,physical_receipt,created_at,expires_at)VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8) ON CONFLICT(room_id)DO UPDATE SET room_incarnation=EXCLUDED.room_incarnation,admission_nonce=pg_catalog.gen_random_uuid(),token_hash=EXCLUDED.token_hash,short_code=EXCLUDED.short_code,admission_sha256=EXCLUDED.admission_sha256,physical_receipt=EXCLUDED.physical_receipt,created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at`, roomID, room.inc, tokenHash, code, hash, physical, now, expiry); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if err = op.commit(); err != nil {
		return nil, err
	}
	invite = &models.WatchRoomExternalInvite{RoomID: roomID, AccountID: op.original.AccountID, Token: token, TokenHash: tokenHash, ShortCode: code, Active: true, CreatedAt: now, ExpiresAt: expiry}
	return invite, nil
}
func (a *WatchRoomAuthority) RevokeExternalInvite(r *http.Request, profile, roomID string) (err error) {
	if r == nil || r.Method != http.MethodDelete {
		return ErrWatchRoomInvalid
	}
	op, e := a.begin(r, profile)
	if e != nil {
		return e
	}
	defer func() { err = op.finish(err) }()
	if _, _, e = op.externalRoom(roomID); e != nil {
		return e
	}
	tag, e := op.g.tx.Exec(op.ctx, `UPDATE watch_room_external_invites SET active=false WHERE room_id=$1 AND active`, roomID)
	if e != nil {
		return ErrWatchRoomUnavailable
	}
	if tag.RowsAffected() != 1 {
		return ErrWatchRoomChanged
	}
	if _, e = op.g.tx.Exec(op.ctx, `DELETE FROM watch_room_invite_provenance WHERE room_id=$1`, roomID); e != nil {
		return ErrWatchRoomUnavailable
	}
	// Existing guests and sessions deliberately have no admission dependency.
	return op.commit()
}
func (a *WatchRoomAuthority) BindExternalSource(r *http.Request, profile, roomID, resource string, params map[string]string) (result *WatchRoomSourceBindingResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}
	resource = shareNormalizeResource(resource)
	if !shareValidResource(resource) {
		return nil, ErrWatchRoomInvalid
	}
	owned, raw, e := watchRoomParams(params)
	if e != nil {
		return nil, e
	}
	if supplied := owned["profileId"]; supplied != "" && supplied != profile {
		return nil, ErrWatchRoomConflict
	}
	owned["profileId"] = profile
	raw, e = json.Marshal(owned)
	if e != nil || len(raw) > 1<<20 {
		return nil, ErrWatchRoomInvalid
	}
	paramsHash, e := watchRoomJSONHash(owned)
	if e != nil {
		return nil, ErrWatchRoomInvalid
	}
	op, e := a.begin(r, profile)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = op.finish(err)
		if err != nil {
			result = nil
		}
	}()
	room, selected, e := op.externalRoom(roomID)
	if e != nil {
		return nil, e
	}
	if _, e = shareLibraryReceipt(op.ctx, op.g.tx, resource, op.original.AccountID, profile); e != nil {
		return nil, ErrWatchRoomForbidden
	}
	sourceHash, e := watchRoomJSONHash([]any{roomID, room.inc, profile, selected.inc, op.original.AccountID, op.account.inc, resource, owned})
	if e != nil {
		return nil, ErrWatchRoomInvalid
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	var physical string
	e = op.g.tx.QueryRow(op.ctx, `INSERT INTO watch_room_external_sources(room_id,resource,params,bound_at)VALUES($1,$2,$3,$4) ON CONFLICT(room_id)DO NOTHING RETURNING concat(tableoid::oid,':',xmin::text,':',ctid::text)`, roomID, resource, raw, now).Scan(&physical)
	if errors.Is(e, pgx.ErrNoRows) {
		var existingResource, existingParamsHash, existingSourceHash, storedPhysical, actualPhysical, inc, profileID, profileInc, accountID, accountInc string
		var bound time.Time
		e = op.g.tx.QueryRow(op.ctx, `SELECT CASE WHEN octet_length(s.resource)<=1048576 THEN s.resource ELSE NULL END,p.params_sha256,p.source_sha256,p.physical_receipt,concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),p.room_incarnation::text,p.source_profile_id,p.source_profile_incarnation::text,p.source_account_id,p.source_account_incarnation::text,s.bound_at FROM watch_room_external_sources s JOIN watch_room_source_provenance p ON p.room_id=s.room_id AND p.version=1 WHERE s.room_id=$1 FOR UPDATE OF s,p`, roomID).Scan(&existingResource, &existingParamsHash, &existingSourceHash, &storedPhysical, &actualPhysical, &inc, &profileID, &profileInc, &accountID, &accountInc, &bound)
		if e != nil || existingResource != resource || existingParamsHash != paramsHash || existingSourceHash != sourceHash || storedPhysical != actualPhysical || inc != room.inc || profileID != profile || profileInc != selected.inc || accountID != op.original.AccountID || accountInc != op.account.inc {
			return nil, ErrWatchRoomConflict
		}
		now = bound
	} else if e != nil {
		return nil, ErrWatchRoomUnavailable
	} else {
		if _, e = op.g.tx.Exec(op.ctx, `INSERT INTO watch_room_source_provenance(room_id,room_incarnation,source_profile_id,source_profile_incarnation,source_account_id,source_account_incarnation,resource,params_sha256,source_sha256,physical_receipt,bound_at)VALUES($1,$2::uuid,$3,$4::uuid,$5,$6::uuid,$7,$8,$9,$10,$11)`, roomID, room.inc, profile, selected.inc, op.original.AccountID, op.account.inc, resource, paramsHash, sourceHash, physical, now); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	if err = op.commit(); err != nil {
		return nil, err
	}
	result = &WatchRoomSourceBindingResult{Source: &models.WatchRoomExternalSource{RoomID: roomID, Resource: resource, Params: owned, BoundAt: now}, PublicationUntil: op.deadline}
	return result, nil
}

// WatchRoomHTTPEntry owns only pending ingress/output work. It has no PG frame
// or auth values, and cannot authorize a room, a guest or a media source.
type WatchRoomHTTPEntry struct {
	self                      *WatchRoomHTTPEntry
	authority                 *WatchRoomAuthority
	ctx                       context.Context
	cancel                    context.CancelFunc
	deadline                  time.Time
	peer                      string
	stopOriginal, stopRuntime func()
	once                      sync.Once
	done                      chan struct{}
}

func watchRoomActualPeer(r *http.Request) string {
	if r == nil {
		return "unknown"
	}
	value := r.RemoteAddr
	var address netip.Addr
	if pair, err := netip.ParseAddrPort(value); err == nil {
		address = pair.Addr()
	} else if parsed, err := netip.ParseAddr(value); err == nil {
		address = parsed
	}
	if !address.IsValid() || address.Zone() != "" {
		return "unknown"
	}
	return address.Unmap().String()
}

func (a *WatchRoomAuthority) OpenHTTPEntry(r *http.Request) (*WatchRoomHTTPEntry, error) {
	if !a.valid() || r == nil || r.Context().Err() != nil {
		return nil, ErrWatchRoomUnavailable
	}
	peer := watchRoomActualPeer(r)
	deadline := time.Now().Add(30 * time.Second)
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	a.mu.Lock()
	if a.closed || a.ctx.Err() != nil || a.httpActive >= 32 || a.httpPeers[peer] >= 4 {
		a.mu.Unlock()
		return nil, ErrWatchRoomBusy
	}
	a.httpActive++
	a.httpPeers[peer]++
	a.operations.Add(1)
	a.mu.Unlock()
	// Lifetime values intentionally start empty: the handler retains its actual
	// original request and links cancellation without exporting authentication.
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	entry := &WatchRoomHTTPEntry{authority: a, ctx: ctx, cancel: cancel, deadline: deadline, peer: peer, done: make(chan struct{})}
	entry.self = entry
	entry.stopOriginal = familyMediaAfterFunc(r.Context(), cancel)
	entry.stopRuntime = familyMediaAfterFunc(a.ctx, cancel)
	if ctx.Err() != nil || !a.valid() {
		entry.Close()
		return nil, ErrWatchRoomUnavailable
	}
	return entry, nil
}
func (e *WatchRoomHTTPEntry) valid() bool {
	return e != nil && e.self == e && e.authority != nil && e.ctx != nil
}
func (e *WatchRoomHTTPEntry) Context() context.Context {
	if !e.valid() {
		return familyMediaClosedContext()
	}
	return e.ctx
}
func (e *WatchRoomHTTPEntry) ExpiresAt() time.Time {
	if !e.valid() {
		return time.Time{}
	}
	return e.deadline
}
func (WatchRoomHTTPEntry) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchRoomHTTPEntry(opaque)"))
}
func (WatchRoomHTTPEntry) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchRoomHTTPEntry) UnmarshalJSON([]byte) error  { return ErrWatchRoomUnavailable }
func (e *WatchRoomHTTPEntry) Close() {
	if !e.valid() {
		return
	}
	e.once.Do(func() {
		e.stopOriginal()
		e.stopRuntime()
		e.cancel()
		a := e.authority
		a.mu.Lock()
		a.httpActive--
		a.httpPeers[e.peer]--
		if a.httpPeers[e.peer] == 0 {
			delete(a.httpPeers, e.peer)
		}
		a.mu.Unlock()
		a.operations.Done()
		close(e.done)
	})
	<-e.done
}
