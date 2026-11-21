package datastore

import (
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/models"
)

const nativeRoomListLimit = 256

// These results describe already-checked/committed DATA. They cannot be used as
// original authentication, profile/PIN permission, or a media source owner.
type WatchRoomNativeResult struct {
	Room             *models.WatchRoom `json:"-"`
	PublicationUntil time.Time         `json:"-"`
}
type WatchRoomNativeRoomsResult struct {
	Rooms            []models.WatchRoom `json:"-"`
	PublicationUntil time.Time          `json:"-"`
}
type WatchRoomNativeInvitesResult struct {
	Invites          []models.WatchRoomAccountInvite `json:"-"`
	PublicationUntil time.Time                       `json:"-"`
}
type WatchRoomNativeMutationResult struct {
	PublicationUntil time.Time `json:"-"`
}
type WatchRoomNativeInviteResult struct {
	Invite           *models.WatchRoomAccountInvite `json:"-"`
	PublicationUntil time.Time                      `json:"-"`
}

type nativeRoomMode uint8

const (
	nativeActiveRoom nativeRoomMode = iota
	nativeAuditRoom
	nativeRecipientAdmission
)

type nativeRoomAccount struct {
	shareAccountRow
	username, physical string
}
type nativeRoomAdmission struct {
	kind, nonce, profileInc, account, accountInc, roomInc, inviteReceipt string
	member                                                               *string
	joined                                                               *time.Time
	expires                                                              time.Time
}
type nativeRoomState struct {
	room              watchRoomOriginal
	creator, selected watchRoomProfile
	account           nativeRoomAccount
	admission         nativeRoomAdmission
}

func (o *watchRoomOperation) insertNativeAdmission(room, roomInc string, profile watchRoomProfile, account shareAccountRow, kind string, now, expiry time.Time, origin, inviteReceipt, member string, joined *time.Time) error {
	var originValue, receiptValue, memberValue any
	if origin != "" {
		originValue = origin
	}
	if inviteReceipt != "" {
		receiptValue = inviteReceipt
	}
	if member != "" {
		memberValue = member
	}
	_, e := o.g.tx.Exec(o.ctx, `INSERT INTO watch_room_native_admissions(room_id,profile_id,room_incarnation,profile_incarnation,account_id,account_incarnation,admission_kind,admitted_at,expires_at,origin_invite_nonce,native_invite_receipt,member_incarnation,member_joined_at) VALUES($1,$2,$3::uuid,$4::uuid,$5,$6::uuid,$7,$8,$9,$10::uuid,$11,$12::uuid,$13)`, room, profile.id, roomInc, profile.inc, account.id, account.inc, kind, now, expiry, originValue, receiptValue, memberValue, joined)
	if e != nil {
		return ErrWatchRoomUnavailable
	}
	return nil
}
func (o *watchRoomOperation) nativeAccount(id string, locked bool) (out nativeRoomAccount, err error) {
	q := `SELECT id,share_incarnation::text,is_master,expires_at,CASE WHEN octet_length(username)<=4096 THEN username ELSE NULL END,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM accounts WHERE id=$1`
	if locked {
		q += ` FOR UPDATE`
	}
	err = o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.inc, &out.master, &out.expires, &out.username, &out.physical)
	if err != nil || out.id != id || !shareCanonicalUUID(out.inc) {
		return out, ErrWatchRoomChanged
	}
	return out, nil
}
func nativeAccountSame(a, b nativeRoomAccount) bool {
	return a.id == b.id && a.inc == b.inc && a.master == b.master && a.username == b.username && a.physical == b.physical && ((a.expires == nil && b.expires == nil) || (a.expires != nil && b.expires != nil && a.expires.Equal(*b.expires)))
}
func nativeRoomSame(a, b watchRoomOriginal) bool {
	return a.id == b.id && a.profile == b.profile && a.account == b.account && a.inc == b.inc && a.profileInc == b.profileInc && a.accountInc == b.accountInc && a.physical == b.physical && a.status == b.status && a.created.Equal(b.created) && a.expires.Equal(b.expires)
}
func (o *watchRoomOperation) nativeRoom(id string, locked bool) (out watchRoomOriginal, err error) {
	q := `SELECT r.id,r.creator_profile_id,r.room_incarnation::text,r.created_at,r.expires_at,r.status,concat(r.tableoid::oid,':',r.xmin::text,':',r.ctid::text),p.creator_account_id,p.creator_profile_incarnation::text,p.creator_account_incarnation::text FROM watch_rooms r JOIN watch_room_provenance p ON p.room_id=r.id AND p.version=1 AND p.room_incarnation=r.room_incarnation AND p.creator_profile_id=r.creator_profile_id AND p.created_at=r.created_at AND p.expires_at=r.expires_at WHERE r.id=$1 AND octet_length(r.id) BETWEEN 1 AND 256 AND octet_length(r.creator_profile_id) BETWEEN 1 AND 256 AND octet_length(p.creator_account_id) BETWEEN 1 AND 256 AND octet_length(r.status)<=16`
	if locked {
		q += ` FOR UPDATE OF r,p`
	}
	err = o.g.tx.QueryRow(o.ctx, q, id).Scan(&out.id, &out.profile, &out.inc, &out.created, &out.expires, &out.status, &out.physical, &out.account, &out.profileInc, &out.accountInc)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if err != nil || !parentMutationID(out.id) || !shareCanonicalUUID(out.inc) || !shareCanonicalUUID(out.profileInc) || !shareCanonicalUUID(out.accountInc) {
		return out, ErrWatchRoomUnavailable
	}
	switch out.status {
	case models.WatchRoomStatusLobby, models.WatchRoomStatusPlaying, models.WatchRoomStatusPaused, models.WatchRoomStatusEnded:
	default:
		return out, ErrWatchRoomUnavailable
	}
	return out, nil
}
func (o *watchRoomOperation) nativeGuards() error {
	if shareLockPolicyRelations(o.ctx, o.g.tx) != nil || shareLockRelations(o.ctx, o.g.tx, []string{"sessions", "watch_rooms", "watch_room_provenance", "watch_room_members", "watch_room_invites", "watch_room_native_admissions", "watch_room_account_invites", "watch_room_account_invite_provenance", "watch_room_guest_members"}) != nil {
		return ErrWatchRoomUnavailable
	}
	return o.fence()
}
func (o *watchRoomOperation) nativeAdmission(room watchRoomOriginal, profile watchRoomProfile, account nativeRoomAccount, locked bool) (out nativeRoomAdmission, err error) {
	q := `SELECT admission_kind,admission_nonce::text,profile_incarnation::text,account_id,account_incarnation::text,room_incarnation::text,COALESCE(native_invite_receipt,''),member_incarnation::text,member_joined_at,expires_at FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2 AND version=1 AND octet_length(account_id) BETWEEN 1 AND 256 AND octet_length(admission_kind)<=16 AND octet_length(COALESCE(native_invite_receipt,''))<=256`
	if locked {
		q += ` FOR UPDATE`
	}
	err = o.g.tx.QueryRow(o.ctx, q, room.id, profile.id).Scan(&out.kind, &out.nonce, &out.profileInc, &out.account, &out.accountInc, &out.roomInc, &out.inviteReceipt, &out.member, &out.joined, &out.expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if err != nil || out.profileInc != profile.inc || out.account != account.id || out.accountInc != account.inc || out.roomInc != room.inc || !out.expires.Equal(room.expires) || !shareCanonicalUUID(out.nonce) || (out.member == nil) != (out.joined == nil) {
		return out, ErrWatchRoomRecreate
	}
	if profile.id == room.profile {
		if out.kind != "creator" || out.inviteReceipt != "" {
			return out, ErrWatchRoomRecreate
		}
	} else {
		if out.kind != "household" && out.kind != "account" {
			return out, ErrWatchRoomRecreate
		}
		var receipt string
		q = `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM watch_room_invites WHERE room_id=$1 AND profile_id=$2`
		if locked {
			q += ` FOR UPDATE`
		}
		if e := o.g.tx.QueryRow(o.ctx, q, room.id, profile.id).Scan(&receipt); e != nil || receipt != out.inviteReceipt {
			return out, ErrWatchRoomRecreate
		}
	}
	return out, nil
}

func (o *watchRoomOperation) nativeAuditCutoff(room watchRoomOriginal) error {
	now := time.Now().UTC()
	if room.status != models.WatchRoomStatusEnded && room.expires.After(now) {
		// A still-active read/end never publishes beyond its fixed room expiry.
		o.tighten(room.expires)
		return nil
	}
	base := room.expires
	if room.status == models.WatchRoomStatusEnded {
		var ended *time.Time
		if err := o.g.tx.QueryRow(o.ctx, `SELECT ended_at FROM watch_rooms WHERE id=$1`, room.id).Scan(&ended); err != nil || ended == nil {
			return ErrWatchRoomUnavailable
		}
		if ended.Before(base) {
			base = *ended
		}
	}
	// Expiry is a fixed original boundary. Late maintenance or an audit read
	// writing EndExpired(now) must never restart the room's audit window.
	if base.After(now) {
		return ErrWatchRoomUnavailable
	}
	if !base.After(now.Add(-24 * time.Hour)) {
		return ErrWatchRoomRecreate
	}
	o.tighten(base.Add(24 * time.Hour))
	return nil
}

// Discover the bounded original set first, then acquire all current account,
// profile and room locks in a deterministic order. No cached selector is proof.
func (o *watchRoomOperation) lockNativeRooms(ids []string, selected string, mode nativeRoomMode, extraAccounts ...string) (map[string]*nativeRoomState, error) {
	if len(ids) > nativeRoomListLimit || selected != "" && !parentMutationID(selected) {
		return nil, ErrWatchRoomInvalid
	}
	firstRooms := make(map[string]watchRoomOriginal)
	firstProfiles := make(map[string]watchRoomProfile)
	firstAccounts := make(map[string]nativeRoomAccount)
	if selected != "" {
		p, e := o.readProfile(selected, false)
		if e != nil || p.owner != o.original.AccountID {
			return nil, ErrWatchRoomForbidden
		}
		firstProfiles[p.id] = p
	}
	accountIDs := map[string]bool{o.original.AccountID: true}
	for _, id := range extraAccounts {
		if !parentMutationID(id) {
			return nil, ErrWatchRoomInvalid
		}
		accountIDs[id] = true
	}
	for _, id := range ids {
		if !parentMutationID(id) {
			return nil, ErrWatchRoomInvalid
		}
		room, e := o.nativeRoom(id, false)
		if e != nil {
			return nil, e
		}
		p, e := o.readProfile(room.profile, false)
		if e != nil || p.owner != room.account || p.inc != room.profileInc {
			return nil, ErrWatchRoomRecreate
		}
		firstRooms[id] = room
		firstProfiles[p.id] = p
		accountIDs[room.account] = true
	}
	orderedAccounts := make([]string, 0, len(accountIDs))
	for id := range accountIDs {
		orderedAccounts = append(orderedAccounts, id)
	}
	sort.Strings(orderedAccounts)
	for _, id := range orderedAccounts {
		a, e := o.nativeAccount(id, false)
		if e != nil {
			return nil, e
		}
		firstAccounts[id] = a
	}
	for _, id := range orderedAccounts {
		now, e := o.nativeAccount(id, true)
		if e != nil || !nativeAccountSame(firstAccounts[id], now) {
			return nil, ErrWatchRoomChanged
		}
		if now.expires != nil {
			if id == o.original.AccountID || mode != nativeAuditRoom {
				if !now.expires.After(time.Now()) {
					return nil, ErrWatchRoomChanged
				}
				o.tighten(*now.expires)
			}
		}
	}
	orderedProfiles := make([]string, 0, len(firstProfiles))
	for id := range firstProfiles {
		orderedProfiles = append(orderedProfiles, id)
	}
	sort.Strings(orderedProfiles)
	for _, id := range orderedProfiles {
		now, e := o.readProfile(id, true)
		if e != nil || !watchRoomProfileSame(now, firstProfiles[id]) {
			return nil, ErrWatchRoomChanged
		}
	}
	if e := o.lockActor(); e != nil {
		return nil, e
	}
	orderedRooms := make([]string, 0, len(firstRooms))
	for id := range firstRooms {
		orderedRooms = append(orderedRooms, id)
	}
	sort.Strings(orderedRooms)
	out := make(map[string]*nativeRoomState, len(ids))
	for _, id := range orderedRooms {
		room, e := o.nativeRoom(id, true)
		first := firstRooms[id]
		creator := firstProfiles[first.profile]
		account := firstAccounts[first.account]
		if e != nil || !nativeRoomSame(first, room) || account.inc != room.accountInc || creator.inc != room.profileInc {
			return nil, ErrWatchRoomRecreate
		}
		if mode != nativeAuditRoom {
			if room.status == models.WatchRoomStatusEnded || !room.expires.After(time.Now()) {
				return nil, ErrWatchRoomChanged
			}
			o.tighten(room.expires)
		} else if e = o.nativeAuditCutoff(room); e != nil {
			return nil, e
		}
		if _, e = o.nativeAdmission(room, creator, account, true); e != nil {
			return nil, e
		}
		state := &nativeRoomState{room: room, creator: creator, account: account}
		if selected != "" {
			state.selected = firstProfiles[selected]
			if mode != nativeRecipientAdmission {
				state.admission, e = o.nativeAdmission(room, state.selected, firstAccounts[o.original.AccountID], true)
				if e != nil {
					return nil, e
				}
			}
		}
		out[id] = state
	}
	if e := o.nativeGuards(); e != nil {
		return nil, e
	}
	return out, nil
}
func (a *WatchRoomAuthority) beginNativeRoom(r *http.Request, selected, room string, mode nativeRoomMode) (*watchRoomOperation, *nativeRoomState, error) {
	o, e := a.begin(r, selected)
	if e != nil {
		return nil, nil, e
	}
	states, e := o.lockNativeRooms([]string{room}, selected, mode)
	if e != nil {
		_ = o.finish(e)
		return nil, nil, e
	}
	return o, states[room], nil
}
func (o *watchRoomOperation) nativeMember(state *nativeRoomState) error {
	if state.admission.member == nil || state.admission.joined == nil {
		return ErrWatchRoomForbidden
	}
	var incarnation string
	var joined time.Time
	e := o.g.tx.QueryRow(o.ctx, `SELECT member_incarnation::text,joined_at FROM watch_room_members WHERE room_id=$1 AND profile_id=$2 FOR UPDATE`, state.room.id, state.selected.id).Scan(&incarnation, &joined)
	if e != nil || incarnation != *state.admission.member || !joined.Equal(*state.admission.joined) {
		return ErrWatchRoomRecreate
	}
	return nil
}
func finishNativeMutation(o *watchRoomOperation, result **WatchRoomNativeMutationResult, err *error) {
	*err = o.finish(*err)
	if *err != nil {
		*result = nil
	}
}
func finishNativeRoom(o *watchRoomOperation, result **WatchRoomNativeResult, err *error) {
	*err = o.finish(*err)
	if *err != nil {
		*result = nil
	}
}
func (a *WatchRoomAuthority) ReadNativeRoom(r *http.Request, profile, room string) (result *WatchRoomNativeResult, err error) {
	if r == nil || r.Method != http.MethodGet {
		return nil, ErrWatchRoomInvalid
	}

	o, state, e := a.beginNativeRoom(r, profile, room, nativeAuditRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeRoom(o, &result, &err)
	if state.room.status != models.WatchRoomStatusEnded && !state.room.expires.After(time.Now()) {
		if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_rooms SET status='ended',revision=revision+1,anchor_updated_at=$2,ended_at=COALESCE(ended_at,$2),end_reason=CASE WHEN end_reason='' THEN 'expired' ELSE end_reason END WHERE id=$1 AND room_incarnation=$3::uuid AND expires_at<=$2 AND status<>'ended'`, room, time.Now().UTC(), state.room.inc); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	view, e := o.nativeRoomData(state)
	if e != nil {
		return nil, e
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeResult{view, o.deadline}, nil
}
func (a *WatchRoomAuthority) JoinNativeRoom(r *http.Request, profile, room, client string, caps models.WatchRoomClientCapabilities) (result *WatchRoomNativeResult, err error) {
	if r == nil || r.Method != http.MethodPost || !caps.NativePlayback || !caps.StateSync || caps.ProtocolVersion < 1 || !watchRoomText(client, 256, false) {
		return nil, ErrWatchRoomInvalid
	}
	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeRoom(o, &result, &err)
	now := time.Now().UTC().Truncate(time.Microsecond)
	var encoded []byte
	encoded, e = nativeJSONBytes(caps, 4096)
	if e != nil {
		return nil, e
	}
	if state.admission.member != nil {
		if e = o.nativeMember(state); e != nil {
			return nil, e
		}
		_, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_members SET client_id=$3,capabilities=$4,last_seen_at=$5 WHERE room_id=$1 AND profile_id=$2`, room, profile, strings.TrimSpace(client), encoded, now)
	} else {
		var exists bool
		var count int
		if e = o.g.tx.QueryRow(o.ctx, `SELECT EXISTS(SELECT 1 FROM watch_room_members WHERE room_id=$1 AND profile_id=$2),(SELECT count(*) FROM watch_room_members WHERE room_id=$1)+(SELECT count(*) FROM watch_room_guest_members WHERE room_id=$1)`, room, profile).Scan(&exists, &count); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		if exists {
			return nil, ErrWatchRoomRecreate
		}
		if count >= 256 {
			return nil, ErrWatchRoomBusy
		}
		var inc string
		e = o.g.tx.QueryRow(o.ctx, `INSERT INTO watch_room_members(room_id,profile_id,client_id,joined_at,last_seen_at,capabilities)VALUES($1,$2,$3,$4,$4,$5) RETURNING member_incarnation::text`, room, profile, strings.TrimSpace(client), now, encoded).Scan(&inc)
		if e == nil {
			_, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_native_admissions SET member_incarnation=$3::uuid,member_joined_at=$4 WHERE room_id=$1 AND profile_id=$2`, room, profile, inc, now)
		}
	}
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	view, e := o.nativeRoomData(state)
	if e != nil {
		return nil, e
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeResult{view, o.deadline}, nil
}
func (a *WatchRoomAuthority) SetNativeReady(r *http.Request, profile, room string, ready bool) (result *WatchRoomNativeResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}

	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeRoom(o, &result, &err)
	if e = o.nativeMember(state); e != nil {
		return nil, e
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_members SET ready=$3,last_seen_at=$4 WHERE room_id=$1 AND profile_id=$2`, room, profile, ready, now); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if ready {
		if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_rooms SET waiting_for_ready=false,status='playing',revision=revision+1,updated_by=$2,anchor_updated_at=$3 WHERE id=$1 AND waiting_for_ready AND NOT EXISTS(SELECT 1 FROM watch_room_members WHERE room_id=$1 AND NOT ready) AND NOT EXISTS(SELECT 1 FROM watch_room_guest_members WHERE room_id=$1 AND NOT ready)`, room, profile, now); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	view, e := o.nativeRoomData(state)
	if e != nil {
		return nil, e
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeResult{view, o.deadline}, nil
}
func (a *WatchRoomAuthority) UpdateNativeState(r *http.Request, profile, room string, update models.WatchRoomStateUpdate) (result *WatchRoomNativeResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}

	if update.Status != models.WatchRoomStatusPlaying && update.Status != models.WatchRoomStatusPaused || math.IsNaN(update.Position) || math.IsInf(update.Position, 0) || update.Position < 0 || math.IsNaN(update.Duration) || math.IsInf(update.Duration, 0) || update.Duration < 0 {
		return nil, ErrWatchRoomInvalid
	}
	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeRoom(o, &result, &err)
	if e = o.nativeMember(state); e != nil {
		return nil, e
	}
	var revision int64
	if e = o.g.tx.QueryRow(o.ctx, `SELECT revision FROM watch_rooms WHERE id=$1`, room).Scan(&revision); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if update.ExpectedRevision != nil && revision != *update.ExpectedRevision {
		return nil, ErrWatchRoomConflict
	}
	starting := state.room.status == models.WatchRoomStatusLobby && update.Status == models.WatchRoomStatusPlaying
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_rooms SET status=$2,waiting_for_ready=CASE WHEN $3 THEN true ELSE waiting_for_ready END,position=$4,duration=GREATEST(duration,$5),revision=revision+1,updated_by=$6,anchor_updated_at=$7 WHERE id=$1 AND revision=$8`, room, update.Status, starting, update.Position, update.Duration, profile, now, revision); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if starting {
		if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_members SET ready=false WHERE room_id=$1`, room); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_guest_members SET ready=false WHERE room_id=$1`, room); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	view, e := o.nativeRoomData(state)
	if e != nil {
		return nil, e
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeResult{view, o.deadline}, nil
}
func (a *WatchRoomAuthority) HeartbeatNativeRoom(r *http.Request, profile, room, client string, buffering bool) (result *WatchRoomNativeMutationResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}

	if !watchRoomText(client, 256, false) {
		return nil, ErrWatchRoomInvalid
	}
	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeMutation(o, &result, &err)
	if e = o.nativeMember(state); e != nil {
		return nil, e
	}
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_members SET client_id=$3,buffering=$4,last_seen_at=$5 WHERE room_id=$1 AND profile_id=$2`, room, profile, strings.TrimSpace(client), buffering, time.Now().UTC()); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeMutationResult{o.deadline}, nil
}
func (a *WatchRoomAuthority) LeaveNativeRoom(r *http.Request, profile, room string) (result *WatchRoomNativeMutationResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}

	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeMutation(o, &result, &err)
	if e = o.nativeMember(state); e != nil {
		return nil, e
	}
	if profile != state.room.profile {
		if _, e = o.g.tx.Exec(o.ctx, `DELETE FROM watch_room_members WHERE room_id=$1 AND profile_id=$2 AND NOT is_creator`, room, profile); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
		if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_native_admissions SET member_incarnation=NULL,member_joined_at=NULL WHERE room_id=$1 AND profile_id=$2`, room, profile); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeMutationResult{o.deadline}, nil
}
func (a *WatchRoomAuthority) EndNativeRoom(r *http.Request, profile, room string) (result *WatchRoomNativeMutationResult, err error) {
	if r == nil || r.Method != http.MethodPost {
		return nil, ErrWatchRoomInvalid
	}

	o, state, e := a.beginNativeRoom(r, profile, room, nativeAuditRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeMutation(o, &result, &err)
	if profile != state.room.profile {
		return nil, ErrWatchRoomForbidden
	}
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_rooms SET status='ended',revision=revision+1,updated_by=$2,anchor_updated_at=$3,ended_at=$3,end_reason='host_ended' WHERE id=$1 AND status<>'ended'`, room, profile, time.Now().UTC()); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeMutationResult{o.deadline}, nil
}

type nativeAccountInvitation struct {
	invite                                                                                                     models.WatchRoomAccountInvite
	inc, roomInc, creatorProfile, creatorProfileInc, creatorAccount, creatorAccountInc, recipientInc, physical string
}

func (o *watchRoomOperation) nativeAccountInvitation(id string, locked bool) (out nativeAccountInvitation, err error) {
	q := `SELECT i.id,i.room_id,i.inviter_account_id,i.invitee_account_id,COALESCE(i.accepted_profile_id,''),i.status,i.created_at,i.expires_at,i.responded_at,i.revoked_at,i.invite_incarnation::text,p.room_incarnation::text,p.creator_profile_id,p.creator_profile_incarnation::text,p.creator_account_id,p.creator_account_incarnation::text,p.recipient_account_incarnation::text,concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text) FROM watch_room_account_invites i JOIN watch_room_account_invite_provenance p ON p.invite_id=i.id AND p.version=1 AND p.invite_incarnation=i.invite_incarnation AND p.creator_account_id=i.inviter_account_id AND p.recipient_account_id=i.invitee_account_id AND p.created_at=i.created_at AND p.expires_at=i.expires_at WHERE i.id=$1 AND octet_length(i.id) BETWEEN 1 AND 256 AND octet_length(i.room_id) BETWEEN 1 AND 256 AND octet_length(i.inviter_account_id) BETWEEN 1 AND 256 AND octet_length(i.invitee_account_id) BETWEEN 1 AND 256 AND octet_length(COALESCE(i.accepted_profile_id,''))<=256 AND octet_length(i.status)<=16 AND octet_length(p.creator_profile_id) BETWEEN 1 AND 256`
	if locked {
		q += ` FOR UPDATE OF i,p`
	}
	v := &out.invite
	err = o.g.tx.QueryRow(o.ctx, q, id).Scan(&v.ID, &v.RoomID, &v.InviterAccountID, &v.InviteeAccountID, &v.AcceptedProfileID, &v.Status, &v.CreatedAt, &v.ExpiresAt, &v.RespondedAt, &v.RevokedAt, &out.inc, &out.roomInc, &out.creatorProfile, &out.creatorProfileInc, &out.creatorAccount, &out.creatorAccountInc, &out.recipientInc, &out.physical)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrWatchRoomRecreate
	}
	if err != nil || !shareCanonicalUUID(out.inc) || !shareCanonicalUUID(out.recipientInc) {
		return out, ErrWatchRoomUnavailable
	}
	return out, nil
}
func (o *watchRoomOperation) validateNativeInvitation(inv nativeAccountInvitation, state *nativeRoomState, recipient nativeRoomAccount) error {
	if inv.invite.Status != models.WatchRoomAccountInvitePending || !inv.invite.ExpiresAt.After(time.Now()) || inv.roomInc != state.room.inc || inv.creatorProfile != state.room.profile || inv.creatorProfileInc != state.creator.inc || inv.creatorAccount != state.room.account || inv.creatorAccountInc != state.account.inc || inv.invite.InviteeAccountID != recipient.id || inv.recipientInc != recipient.inc || recipient.expires != nil && !recipient.expires.After(time.Now()) {
		return ErrWatchRoomChanged
	}
	o.tighten(inv.invite.ExpiresAt)
	return nil
}
func (o *watchRoomOperation) nativeInvitationIDs(room string) ([]string, error) {
	var count int
	var size int64
	e := o.g.tx.QueryRow(o.ctx, `SELECT count(*),COALESCE(sum(octet_length(i.id)+octet_length(i.room_id)+octet_length(i.status)+octet_length(COALESCE(i.accepted_profile_id,''))+octet_length(target.username)+octet_length(creator.name)+octet_length(r.title)+octet_length(r.media_type)+octet_length(r.item_id)+octet_length(r.poster_url)+octet_length(r.backdrop_url)),0)*6+count(*)*2048+65536 FROM watch_room_account_invites i JOIN watch_rooms r ON r.id=i.room_id JOIN users creator ON creator.id=r.creator_profile_id JOIN accounts target ON target.id=i.invitee_account_id WHERE i.status='pending' AND i.expires_at>clock_timestamp() AND (($1<>'' AND i.room_id=$1) OR ($1='' AND i.invitee_account_id=$2))`, room, o.original.AccountID).Scan(&count, &size)
	if e != nil || count > 256 || size > 1<<20 {
		return nil, ErrWatchRoomUnavailable
	}
	rows, e := o.g.tx.Query(o.ctx, `SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN id ELSE NULL END FROM watch_room_account_invites WHERE status='pending' AND expires_at>clock_timestamp() AND (($1<>'' AND room_id=$1) OR ($1='' AND invitee_account_id=$2)) ORDER BY created_at,id LIMIT 257`, room, o.original.AccountID)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	defer rows.Close()
	ids := make([]string, 0, count)
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil || len(ids) >= 256 {
			return nil, ErrWatchRoomUnavailable
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil || len(ids) != count {
		return nil, ErrWatchRoomChanged
	}
	return ids, nil
}
func (a *WatchRoomAuthority) InviteNativeAccount(r *http.Request, profile, room, username string) (result *WatchRoomNativeInviteResult, err error) {
	username = strings.TrimSpace(username)
	if r == nil || r.Method != http.MethodPost || !watchRoomText(username, 4096, true) {
		return nil, ErrWatchRoomInvalid
	}
	o, e := a.begin(r, profile)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = o.finish(err)
		if err != nil {
			result = nil
		}
	}()
	// Ambiguous case-folded names are never resolved by choosing the first row.
	rows, e := o.g.tx.Query(o.ctx, `SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN id ELSE NULL END FROM accounts WHERE lower(username)=lower($1) LIMIT 2`, username)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	var target string
	count := 0
	for rows.Next() {
		count++
		if e = rows.Scan(&target); e != nil {
			rows.Close()
			return nil, ErrWatchRoomUnavailable
		}
	}
	rows.Close()
	if rows.Err() != nil || count != 1 || target == o.original.AccountID {
		return nil, ErrWatchRoomForbidden
	}
	states, e := o.lockNativeRooms([]string{room}, profile, nativeActiveRoom, target)
	if e != nil {
		return nil, e
	}
	state := states[room]
	if profile != state.room.profile {
		return nil, ErrWatchRoomForbidden
	}
	recipient, e := o.nativeAccount(target, false)
	if e != nil || recipient.expires != nil && !recipient.expires.After(time.Now()) || !strings.EqualFold(recipient.username, username) {
		return nil, ErrWatchRoomChanged
	}
	var inbox, roomCount, roster int
	if e = o.g.tx.QueryRow(o.ctx, `SELECT (SELECT count(*) FROM watch_room_account_invites WHERE invitee_account_id=$1 AND status='pending' AND expires_at>clock_timestamp()),(SELECT count(*) FROM watch_room_account_invites WHERE room_id=$2 AND status='pending' AND expires_at>clock_timestamp()),(SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$2)`, target, room).Scan(&inbox, &roomCount, &roster); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if inbox >= 256 || roomCount >= 128 || roster >= 256 {
		return nil, ErrWatchRoomBusy
	}
	var oldID, oldStatus string
	e = o.g.tx.QueryRow(o.ctx, `SELECT id,status FROM watch_room_account_invites WHERE room_id=$1 AND invitee_account_id=$2 FOR UPDATE`, room, target).Scan(&oldID, &oldStatus)
	if e == nil {
		if oldStatus != models.WatchRoomAccountInviteDeclined && oldStatus != models.WatchRoomAccountInviteRevoked {
			return nil, ErrWatchRoomConflict
		}
		if _, e = o.g.tx.Exec(o.ctx, `DELETE FROM watch_room_account_invites WHERE id=$1`, oldID); e != nil {
			return nil, ErrWatchRoomUnavailable
		}
	} else if !errors.Is(e, pgx.ErrNoRows) {
		return nil, ErrWatchRoomUnavailable
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := parentMutationMin(state.room.expires, recipient.expires)
	id, e := uuid.NewRandom()
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	var inc string
	e = o.g.tx.QueryRow(o.ctx, `INSERT INTO watch_room_account_invites(id,room_id,inviter_account_id,invitee_account_id,status,created_at,expires_at)VALUES($1,$2,$3,$4,'pending',$5,$6)RETURNING invite_incarnation::text`, id.String(), room, o.original.AccountID, target, now, expiry).Scan(&inc)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if _, e = o.g.tx.Exec(o.ctx, `INSERT INTO watch_room_account_invite_provenance(invite_id,invite_incarnation,room_incarnation,creator_profile_id,creator_profile_incarnation,creator_account_id,creator_account_incarnation,recipient_account_id,recipient_account_incarnation,created_at,expires_at)VALUES($1,$2::uuid,$3::uuid,$4,$5::uuid,$6,$7::uuid,$8,$9::uuid,$10,$11)`, id.String(), inc, state.room.inc, profile, state.creator.inc, o.original.AccountID, state.account.inc, target, recipient.inc, now, expiry); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	o.tighten(expiry)
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeInviteResult{&models.WatchRoomAccountInvite{ID: id.String(), RoomID: room, InviterAccountID: o.original.AccountID, InviteeAccountID: target, InviteeUsername: recipient.username, Status: models.WatchRoomAccountInvitePending, CreatedAt: now, ExpiresAt: expiry}, o.deadline}, nil
}
func (a *WatchRoomAuthority) AcceptNativeAccountInvitation(r *http.Request, profile, id string) (result *WatchRoomNativeResult, err error) {
	if r == nil || r.Method != http.MethodPost || !parentMutationID(id) {
		return nil, ErrWatchRoomInvalid
	}
	o, e := a.begin(r, profile)
	if e != nil {
		return nil, e
	}
	defer finishNativeRoom(o, &result, &err)
	first, e := o.nativeAccountInvitation(id, false)
	if e != nil {
		return nil, e
	}
	if first.invite.InviteeAccountID != o.original.AccountID {
		return nil, ErrWatchRoomForbidden
	}
	states, e := o.lockNativeRooms([]string{first.invite.RoomID}, profile, nativeRecipientAdmission)
	if e != nil {
		return nil, e
	}
	state := states[first.invite.RoomID]
	current, e := o.nativeAccountInvitation(id, true)
	if e != nil || current.physical != first.physical || current.inc != first.inc {
		return nil, ErrWatchRoomChanged
	}
	recipient, e := o.nativeAccount(o.original.AccountID, false)
	if e != nil {
		return nil, e
	}
	if e = o.validateNativeInvitation(current, state, recipient); e != nil {
		return nil, e
	}
	var count int
	var exists bool
	if e = o.g.tx.QueryRow(o.ctx, `SELECT count(*),EXISTS(SELECT 1 FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2) FROM watch_room_native_admissions WHERE room_id=$1`, state.room.id, profile).Scan(&count, &exists); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if exists {
		return nil, ErrWatchRoomConflict
	}
	if count >= 256 {
		return nil, ErrWatchRoomBusy
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	var receipt string
	if e = o.g.tx.QueryRow(o.ctx, `INSERT INTO watch_room_invites(room_id,profile_id,invited_at)VALUES($1,$2,$3)RETURNING concat(tableoid::oid,':',xmin::text,':',ctid::text)`, state.room.id, profile, now).Scan(&receipt); e != nil {
		return nil, ErrWatchRoomRecreate
	}
	if e = o.insertNativeAdmission(state.room.id, state.room.inc, state.selected, recipient.shareAccountRow, "account", now, state.room.expires, current.inc, receipt, "", nil); e != nil {
		return nil, e
	}
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_account_invites SET status='accepted',accepted_profile_id=$2,responded_at=$3 WHERE id=$1 AND status='pending' AND invite_incarnation=$4::uuid`, id, profile, now, current.inc); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	view, e := o.nativeRoomData(state)
	if e != nil {
		return nil, e
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeResult{view, o.deadline}, nil
}
func (a *WatchRoomAuthority) DeclineNativeAccountInvitation(r *http.Request, id string) (result *WatchRoomNativeMutationResult, err error) {
	if r == nil || r.Method != http.MethodPost || !parentMutationID(id) {
		return nil, ErrWatchRoomInvalid
	}
	o, e := a.beginOriginalRoomOperation(r)
	if e != nil {
		return nil, e
	}
	defer finishNativeMutation(o, &result, &err)
	first, e := o.nativeAccountInvitation(id, false)
	if e != nil {
		return nil, e
	}
	if first.invite.InviteeAccountID != o.original.AccountID {
		return nil, ErrWatchRoomForbidden
	}
	states, e := o.lockNativeRooms([]string{first.invite.RoomID}, "", nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	state := states[first.invite.RoomID]
	current, e := o.nativeAccountInvitation(id, true)
	if e != nil || current.physical != first.physical {
		return nil, ErrWatchRoomChanged
	}
	account, e := o.nativeAccount(o.original.AccountID, false)
	if e != nil {
		return nil, e
	}
	if e = o.validateNativeInvitation(current, state, account); e != nil {
		return nil, e
	}
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_account_invites SET status='declined',responded_at=$2 WHERE id=$1 AND status='pending'`, id, time.Now().UTC()); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeMutationResult{o.deadline}, nil
}
func (a *WatchRoomAuthority) RevokeNativeAccountInvitation(r *http.Request, profile, room, id string) (result *WatchRoomNativeMutationResult, err error) {
	if r == nil || r.Method != http.MethodDelete || !parentMutationID(id) {
		return nil, ErrWatchRoomInvalid
	}
	o, state, e := a.beginNativeRoom(r, profile, room, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	defer finishNativeMutation(o, &result, &err)
	if profile != state.room.profile {
		return nil, ErrWatchRoomForbidden
	}
	inv, e := o.nativeAccountInvitation(id, true)
	if e != nil {
		return nil, e
	}
	if inv.invite.RoomID != room || inv.roomInc != state.room.inc || inv.creatorProfileInc != state.creator.inc || inv.creatorAccountInc != state.account.inc || inv.invite.Status != models.WatchRoomAccountInvitePending {
		return nil, ErrWatchRoomChanged
	}
	if _, e = o.g.tx.Exec(o.ctx, `UPDATE watch_room_account_invites SET status='revoked',revoked_at=$2 WHERE id=$1 AND status='pending'`, id, time.Now().UTC()); e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	if e = o.commit(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeMutationResult{o.deadline}, nil
}
func (a *WatchRoomAuthority) ListNativeInvitations(r *http.Request, profile string) (result *WatchRoomNativeRoomsResult, err error) {
	if r == nil || r.Method != http.MethodGet {
		return nil, ErrWatchRoomInvalid
	}

	o, e := a.begin(r, profile)
	if e != nil {
		return nil, e
	}
	defer func() {
		err = o.finish(err)
		if err != nil {
			result = nil
		}
	}()
	var count int
	var size int64
	e = o.g.tx.QueryRow(o.ctx, `SELECT count(*),COALESCE(sum(octet_length(r.id)+octet_length(r.title)+octet_length(r.params::text)),0)*6+count(*)*4096+65536 FROM watch_rooms r WHERE r.status<>'ended' AND r.expires_at>clock_timestamp() AND (r.creator_profile_id=$1 OR EXISTS(SELECT 1 FROM watch_room_invites i WHERE i.room_id=r.id AND i.profile_id=$1))`, profile).Scan(&count, &size)
	if e != nil || count > 256 || size > 1<<20 {
		return nil, ErrWatchRoomUnavailable
	}
	rows, e := o.g.tx.Query(o.ctx, `SELECT CASE WHEN octet_length(r.id) BETWEEN 1 AND 256 THEN r.id ELSE NULL END FROM watch_rooms r WHERE r.status<>'ended' AND r.expires_at>clock_timestamp() AND (r.creator_profile_id=$1 OR EXISTS(SELECT 1 FROM watch_room_invites i WHERE i.room_id=r.id AND i.profile_id=$1)) ORDER BY r.created_at DESC,r.id LIMIT 257`, profile)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	ids := make([]string, 0, count)
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil || len(ids) >= 256 {
			rows.Close()
			return nil, ErrWatchRoomUnavailable
		}
		ids = append(ids, id)
	}
	rows.Close()
	if rows.Err() != nil || len(ids) != count {
		return nil, ErrWatchRoomChanged
	}
	states, e := o.lockNativeRooms(ids, profile, nativeActiveRoom)
	if e != nil {
		return nil, e
	}
	if e = o.nativeRoomsDataBudget(ids); e != nil {
		return nil, e
	}
	views := make([]models.WatchRoom, 0, len(ids))
	for _, id := range ids {
		view, e := o.nativeRoomData(states[id])
		if e != nil {
			return nil, e
		}
		views = append(views, *view)
	}
	if _, e = nativeJSONBytes(views, 1<<20); e != nil {
		return nil, e
	}
	if e = o.fence(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeRoomsResult{views, o.deadline}, nil
}
func (a *WatchRoomAuthority) ListNativeAccountInvitations(r *http.Request) (*WatchRoomNativeInvitesResult, error) {
	return a.listNativeAccountInvitations(r, "", "")
}
func (a *WatchRoomAuthority) ListNativeRoomAccountInvitations(r *http.Request, profile, room string) (*WatchRoomNativeInvitesResult, error) {
	if !parentMutationID(profile) || !parentMutationID(room) {
		return nil, ErrWatchRoomInvalid
	}
	return a.listNativeAccountInvitations(r, profile, room)
}
func (a *WatchRoomAuthority) listNativeAccountInvitations(r *http.Request, profile, room string) (result *WatchRoomNativeInvitesResult, err error) {
	if r == nil || r.Method != http.MethodGet {
		return nil, ErrWatchRoomInvalid
	}
	var o *watchRoomOperation
	var e error
	if profile == "" {
		o, e = a.beginOriginalRoomOperation(r)
	} else {
		o, e = a.begin(r, profile)
	}
	if e != nil {
		return nil, e
	}
	defer func() {
		err = o.finish(err)
		if err != nil {
			result = nil
		}
	}()
	ids, e := o.nativeInvitationIDs(room)
	if e != nil {
		return nil, e
	}
	first := make(map[string]nativeAccountInvitation)
	roomIDs := make([]string, 0, len(ids))
	seen := make(map[string]bool)
	extras := make([]string, 0, len(ids))
	for _, id := range ids {
		inv, e := o.nativeAccountInvitation(id, false)
		if e != nil {
			return nil, e
		}
		if room == "" && inv.invite.InviteeAccountID != o.original.AccountID {
			return nil, ErrWatchRoomForbidden
		}
		first[id] = inv
		extras = append(extras, inv.invite.InviteeAccountID)
		if !seen[inv.invite.RoomID] {
			roomIDs = append(roomIDs, inv.invite.RoomID)
			seen[inv.invite.RoomID] = true
		}
	}
	if room != "" && !seen[room] {
		roomIDs = append(roomIDs, room)
	}
	// Inbox has no guessed profile. Creator outbox uses its actual own profile
	// only on the selected room, never as a selector for recipient accounts.
	states, e := o.lockNativeRooms(roomIDs, profile, nativeActiveRoom, extras...)
	if e != nil {
		return nil, e
	}
	if room != "" && states[room].room.profile != profile {
		return nil, ErrWatchRoomForbidden
	}
	out := make([]models.WatchRoomAccountInvite, 0, len(ids))
	for _, id := range ids {
		inv, e := o.nativeAccountInvitation(id, true)
		if e != nil || inv.physical != first[id].physical {
			return nil, ErrWatchRoomChanged
		}
		state := states[inv.invite.RoomID]
		recipient, e := o.nativeAccount(inv.invite.InviteeAccountID, false)
		if e != nil {
			return nil, e
		}
		if e = o.validateNativeInvitation(inv, state, recipient); e != nil {
			return nil, e
		}
		v := inv.invite
		if room != "" {
			v.InviteeUsername = recipient.username
		} else {
			v.CreatorName = state.creator.name
			if e = o.g.tx.QueryRow(o.ctx, `SELECT CASE WHEN octet_length(title)<=4096 THEN title ELSE NULL END,CASE WHEN octet_length(media_type)<=64 THEN media_type ELSE NULL END,CASE WHEN octet_length(item_id)<=1024 THEN item_id ELSE NULL END,CASE WHEN octet_length(poster_url)<=8192 THEN poster_url ELSE NULL END,CASE WHEN octet_length(backdrop_url)<=8192 THEN backdrop_url ELSE NULL END FROM watch_rooms WHERE id=$1`, state.room.id).Scan(&v.Title, &v.MediaType, &v.ItemID, &v.PosterURL, &v.BackdropURL); e != nil {
				return nil, ErrWatchRoomUnavailable
			}
			v.InviterAccountID = ""
			v.InviteeAccountID = ""
		}
		out = append(out, v)
	}
	if _, e = nativeJSONBytes(out, 1<<20); e != nil {
		return nil, e
	}
	if e = o.fence(); e != nil {
		return nil, e
	}
	return &WatchRoomNativeInvitesResult{out, o.deadline}, nil
}
