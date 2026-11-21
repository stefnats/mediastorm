//go:build linux

package datastore

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/models"
)

type nativeRoomFixture struct {
	f              *watchRoomFixture
	recipientToken string
}

func newNativeRoomFixture(t *testing.T, kind string, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), life context.Context) *nativeRoomFixture {
	t.Helper()
	if trace == nil {
		trace = &bridgeSessionTransactionTrace{}
	}
	f := newWatchRoomFixture(t, trace, dial, life)
	f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('household','actor','Synthetic Household'),('household2','actor','Synthetic Household Two'),('recipient','other','Synthetic Recipient');INSERT INTO user_settings_versions(user_id,generation)VALUES('household',0),('household2',0),('recipient',0) ON CONFLICT(user_id)DO NOTHING`)
	if kind == "ordinary-share-disabled" {
		f.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
	}
	if kind == "ordinary-protected-profile" {
		hash, e := bcrypt.GenerateFromPassword([]byte("3141"), bcrypt.MinCost)
		if e != nil {
			t.Fatal(e)
		}
		f.base.exec(t, `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='target'`, string(hash))
	}
	if kind == "expired-read-same-tx-terminal-data" {
		f.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(5*time.Second).Truncate(time.Microsecond))
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	token := "synthetic-original-native-recipient"
	if e := f.base.ds.Sessions().Create(t.Context(), &models.Session{Token: token, AccountID: "other", CreatedAt: now, ExpiresAt: now.Add(time.Hour), ClientID: "native-recipient"}); e != nil {
		t.Fatal(e)
	}
	in := watchRoomInput()
	in.InviteeProfileIDs = []string{"household", "household2"}
	room, e := f.authority.CreateRoom(f.request(t, "POST", t.Context()), "target", in)
	if e != nil || room == nil {
		t.Fatal("actual original native admissions", e)
	}
	f.room = room
	return &nativeRoomFixture{f, token}
}
func (f *nativeRoomFixture) request(t *testing.T, method string, other bool, ctx context.Context) *http.Request {
	t.Helper()
	if !other {
		return f.f.request(t, method, ctx)
	}
	r := httptest.NewRequest(method, "http://synthetic.test/api/users/recipient/watch-rooms", nil).WithContext(ctx)
	r.Header.Set("X-Client-ID", "native-recipient")
	framed, release, e := f.f.base.ds.AuthenticateProfilePinRequest(r, f.recipientToken, "native-recipient")
	if e != nil {
		t.Fatal("actual original recipient frame", e)
	}
	t.Cleanup(release)
	return framed
}
func (f *nativeRoomFixture) invite(t *testing.T) *models.WatchRoomAccountInvite {
	t.Helper()
	result, e := f.f.authority.InviteNativeAccount(f.request(t, "POST", false, t.Context()), "target", f.f.room.ID, "Synthetic Other")
	if e != nil || result == nil || result.Invite == nil {
		t.Fatal("actual original native account invite", e)
	}
	return result.Invite
}
func (f *nativeRoomFixture) accept(t *testing.T, invite *models.WatchRoomAccountInvite) *models.WatchRoom {
	t.Helper()
	result, e := f.f.authority.AcceptNativeAccountInvitation(f.request(t, "POST", true, t.Context()), "recipient", invite.ID)
	if e != nil || result == nil {
		t.Fatal("actual same-TX native acceptance", e)
	}
	return result.Room
}
func (f *nativeRoomFixture) join(t *testing.T, profile string, other bool) *models.WatchRoom {
	t.Helper()
	result, e := f.f.authority.JoinNativeRoom(f.request(t, "POST", other, t.Context()), profile, f.f.room.ID, "synthetic-native-client", watchRoomInput().Capabilities)
	if e != nil || result == nil {
		t.Fatal("actual original native join", e)
	}
	return result.Room
}
func nativeMemberByID(t *testing.T, room *models.WatchRoom, id string) models.WatchRoomMember {
	t.Helper()
	for _, member := range room.Members {
		if member.ProfileID == id {
			return member
		}
	}
	t.Fatal("original native member omitted", id)
	return models.WatchRoomMember{}
}

// This controlled synthetic clock fixture first receives an actual Core-made
// creator-only room. Only its already-produced date fields are shifted. The
// own-schema date-identity triggers are restored inside the SAME transaction;
// no legacy sidecar, nonce, actor, policy callback or production grant is made.
func nativeAgeOriginalRoom(t *testing.T, f *nativeRoomFixture, room string, offset time.Duration) {
	t.Helper()
	tx, e := f.f.base.ds.pool.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer recordingStateRollback(tx)
	var invites int
	if e = tx.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_invites WHERE room_id=$1`, room).Scan(&invites); e != nil || invites != 0 {
		t.Fatal("aging fixture must be genuine creator-only room", e)
	}
	const snapshot = `SELECT jsonb_build_array(to_jsonb(r)-ARRAY['created_at','expires_at','anchor_updated_at']::text[],to_jsonb(m)-ARRAY['joined_at','last_seen_at']::text[],to_jsonb(p)-ARRAY['created_at','expires_at']::text[],to_jsonb(n)-ARRAY['admitted_at','expires_at','member_joined_at']::text[])::text,r.created_at,r.expires_at,r.anchor_updated_at,m.joined_at,m.last_seen_at,p.created_at,p.expires_at,n.admitted_at,n.expires_at,n.member_joined_at FROM watch_rooms r JOIN watch_room_members m ON m.room_id=r.id AND m.profile_id=r.creator_profile_id JOIN watch_room_provenance p ON p.room_id=r.id JOIN watch_room_native_admissions n ON n.room_id=r.id AND n.profile_id=r.creator_profile_id AND n.admission_kind='creator' WHERE r.id=$1`
	read := func() (string, [10]time.Time) {
		var logical string
		var dates [10]time.Time
		e := tx.QueryRow(t.Context(), snapshot, room).Scan(&logical, &dates[0], &dates[1], &dates[2], &dates[3], &dates[4], &dates[5], &dates[6], &dates[7], &dates[8], &dates[9])
		if e != nil {
			t.Fatal("actual original aging fixture snapshot", e)
		}
		return logical, dates
	}
	rooms := pgx.Identifier{f.f.base.schema, "watch_rooms"}.Sanitize()
	members := pgx.Identifier{f.f.base.schema, "watch_room_members"}.Sanitize()
	triggers := func() string {
		var state string
		e := tx.QueryRow(t.Context(), `SELECT COALESCE(jsonb_agg(jsonb_build_array(c.relname,t.tgname,t.tgenabled) ORDER BY c.relname,t.tgname),'[]'::jsonb)::text FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid WHERE NOT t.tgisinternal AND t.tgrelid IN (pg_catalog.to_regclass($1),pg_catalog.to_regclass($2))`, rooms, members).Scan(&state)
		if e != nil {
			t.Fatal(e)
		}
		return state
	}
	before, dates := read()
	beforeTriggers := triggers()
	for _, change := range []struct{ table, name string }{{rooms, "watch_rooms_original_incarnation"}, {members, "watch_room_members_original_identity"}} {
		if _, e = tx.Exec(t.Context(), `ALTER TABLE `+change.table+` DISABLE TRIGGER `+pgx.Identifier{change.name}.Sanitize()); e != nil {
			t.Fatal(e)
		}
	}
	seconds := offset.Seconds()
	for _, q := range []string{
		`UPDATE watch_rooms SET created_at=created_at-make_interval(secs=>$2),expires_at=expires_at-make_interval(secs=>$2),anchor_updated_at=anchor_updated_at-make_interval(secs=>$2) WHERE id=$1`,
		`UPDATE watch_room_members SET joined_at=joined_at-make_interval(secs=>$2),last_seen_at=last_seen_at-make_interval(secs=>$2) WHERE room_id=$1`,
		`UPDATE watch_room_provenance SET created_at=created_at-make_interval(secs=>$2),expires_at=expires_at-make_interval(secs=>$2) WHERE room_id=$1`,
		`UPDATE watch_room_native_admissions SET admitted_at=admitted_at-make_interval(secs=>$2),expires_at=expires_at-make_interval(secs=>$2),member_joined_at=member_joined_at-make_interval(secs=>$2) WHERE room_id=$1`,
	} {
		if _, e = tx.Exec(t.Context(), q, room, seconds); e != nil {
			t.Fatal(e)
		}
	}
	for _, change := range []struct{ table, name string }{{rooms, "watch_rooms_original_incarnation"}, {members, "watch_room_members_original_identity"}} {
		if _, e = tx.Exec(t.Context(), `ALTER TABLE `+change.table+` ENABLE TRIGGER `+pgx.Identifier{change.name}.Sanitize()); e != nil {
			t.Fatal(e)
		}
	}
	after, aged := read()
	if before != after || triggers() != beforeTriggers {
		t.Fatal("clock aging changed original non-date identity/nonce or trigger state")
	}
	for i := range dates {
		if !aged[i].Equal(dates[i].Add(-offset)) {
			t.Fatal("controlled original fixture date shift inconsistent", i)
		}
	}
	var enabled int
	if e = tx.QueryRow(t.Context(), `SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid IN (pg_catalog.to_regclass($1),pg_catalog.to_regclass($2)) AND tgname IN ('watch_rooms_original_incarnation','watch_room_members_original_identity') AND tgenabled='O'`, rooms, members).Scan(&enabled); e != nil || enabled != 2 {
		t.Fatal("original identity triggers not restored before fixture commit", e)
	}
	if e = tx.Commit(t.Context()); e != nil {
		t.Fatal(e)
	}
}

func TestNativeRoomOriginalMutations(t *testing.T) {
	for _, kind := range []string{"creator-original-read", "household-original-join", "accepted-other-account-join", "ordinary-share-disabled", "ordinary-protected-profile", "unjoined-cannot-command", "ready-original-member", "state-current-revision", "stale-revision-no-write", "finite-state-validation", "heartbeat-original-member", "leave-preserves-original-admission", "rejoin-same-original-invite", "creator-leave-unchanged", "creator-only-end", "already-ended-idempotent", "expired-read-same-tx-terminal-data", "playing-position-bounded-data"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeRoomFixture(t, kind, nil, nil, t.Context())
			a, room := f.f.authority, f.f.room.ID
			if kind == "accepted-other-account-join" || kind == "expired-read-same-tx-terminal-data" {
				f.accept(t, f.invite(t))
				if kind == "accepted-other-account-join" {
					view := f.join(t, "recipient", true)
					if !nativeMemberByID(t, view, "recipient").Joined {
						t.Fatal("accepted original recipient not joined")
					}
					return
				}
			}
			if kind == "expired-read-same-tx-terminal-data" {
				if delay := time.Until(f.f.room.ExpiresAt); delay > 0 {
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-t.Context().Done():
						t.Fatal("original room expiry wait cancelled")
					}
				}
				result, e := a.ReadNativeRoom(f.request(t, "GET", true, t.Context()), "recipient", room)
				if e != nil || result == nil || result.Room.Status != models.WatchRoomStatusEnded || result.Room.EndedAt == nil || result.Room.EndReason != "expired" {
					t.Fatal("expired original audit read did not commit terminal DATA", e)
				}
				var status string
				if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT status FROM watch_rooms WHERE id=$1`, room).Scan(&status); e != nil || status != "ended" {
					t.Fatal("terminal audit DATA not in same actual store", e)
				}
				// Controlled own-schema fixture clock aging follows genuine Core creation;
				// it is not historical production evidence or fresh legacy attestation.
				for _, offset := range []time.Duration{24*time.Hour + 30*time.Minute, 49 * time.Hour} {
					original, e := a.CreateRoom(f.request(t, "POST", true, t.Context()), "recipient", watchRoomInput())
					if e != nil || original == nil {
						t.Fatal("genuine recipient-owned aging control", e)
					}
					nativeAgeOriginalRoom(t, f, original.ID, offset)
					aged, e := a.ReadNativeRoom(f.request(t, "GET", true, t.Context()), "recipient", original.ID)
					if offset == 24*time.Hour+30*time.Minute {
						if e != nil || aged == nil || aged.Room.Status != models.WatchRoomStatusEnded || aged.Room.EndReason != "expired" {
							t.Fatal("consistent within-window fixture failed positive audit control", e)
						}
					} else {
						if !errors.Is(e, ErrWatchRoomRecreate) || aged != nil {
							t.Fatal("longexpired original fixture revived audit DATA", e)
						}
						var state string
						var ended *time.Time
						if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT status,ended_at FROM watch_rooms WHERE id=$1`, original.ID).Scan(&state, &ended); e != nil || state != "lobby" || ended != nil {
							t.Fatal("longexpired denial wrote a fresh EndExpired window", state, e)
						}
					}
				}
				return
			}
			if kind == "household-original-join" {
				view := f.join(t, "household", false)
				member := nativeMemberByID(t, view, "household")
				if !member.Joined || member.IsCreator || member.ClientID != "synthetic-native-client" {
					t.Fatal("actual original household membership")
				}
				return
			}
			if kind == "unjoined-cannot-command" {
				if result, e := a.UpdateNativeState(f.request(t, "POST", false, t.Context()), "household", room, models.WatchRoomStateUpdate{Status: "playing"}); e == nil || result != nil {
					t.Fatal("unjoined invitation authorized playback command")
				}
				return
			}
			if kind == "finite-state-validation" {
				for _, update := range []models.WatchRoomStateUpdate{{Status: "ended"}, {Status: "playing", Position: math.NaN()}, {Status: "playing", Position: math.Inf(1)}, {Status: "playing", Duration: -1}} {
					if result, e := a.UpdateNativeState(f.request(t, "POST", false, t.Context()), "target", room, update); e == nil || result != nil {
						t.Fatal("invalid command changed original native room")
					}
				}
				return
			}
			if kind == "ready-original-member" {
				f.join(t, "household", false)
				if _, e := a.UpdateNativeState(f.request(t, "POST", false, t.Context()), "target", room, models.WatchRoomStateUpdate{Status: "playing"}); e != nil {
					t.Fatal(e)
				}
				result, e := a.SetNativeReady(f.request(t, "POST", false, t.Context()), "household", room, true)
				if e != nil || !result.Room.WaitingForReady {
					t.Fatal("native ready released before original creator", e)
				}
				result, e = a.SetNativeReady(f.request(t, "POST", false, t.Context()), "target", room, true)
				if e != nil || result.Room.WaitingForReady || !nativeMemberByID(t, result.Room, "household").Ready {
					t.Fatal("current original ready barrier did not release", e)
				}
				return
			}
			if kind == "state-current-revision" || kind == "stale-revision-no-write" {
				revision := int64(0)
				if kind == "stale-revision-no-write" {
					revision = 99
				}
				result, e := a.UpdateNativeState(f.request(t, "POST", false, t.Context()), "target", room, models.WatchRoomStateUpdate{Status: "playing", Position: 42, Duration: 100, ExpectedRevision: &revision})
				if kind == "stale-revision-no-write" {
					if !errors.Is(e, ErrWatchRoomConflict) || result != nil {
						t.Fatal("stale original revision accepted", e)
					}
					var actual int64
					if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT revision FROM watch_rooms WHERE id=$1`, room).Scan(&actual); e != nil || actual != 0 {
						t.Fatal("stale revision wrote DATA", actual, e)
					}
				} else if e != nil || result == nil || result.Room.Revision != 1 || result.Room.Position != 42 || !result.Room.WaitingForReady {
					t.Fatal("actual revision CAS semantics changed", e)
				}
				return
			}
			if kind == "heartbeat-original-member" {
				before := f.f.room.Members[0].LastSeenAt
				result, e := a.HeartbeatNativeRoom(f.request(t, "POST", false, t.Context()), "target", room, "current-native-client", true)
				if e != nil || result == nil {
					t.Fatal(e)
				}
				var client string
				var buffering bool
				var at time.Time
				if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT client_id,buffering,last_seen_at FROM watch_room_members WHERE room_id=$1 AND profile_id='target'`, room).Scan(&client, &buffering, &at); e != nil || client != "current-native-client" || !buffering || at.Before(before) {
					t.Fatal("heartbeat did not update actual original member", e)
				}
				return
			}
			if kind == "leave-preserves-original-admission" || kind == "rejoin-same-original-invite" {
				f.join(t, "household", false)
				if _, e := a.LeaveNativeRoom(f.request(t, "POST", false, t.Context()), "household", room); e != nil {
					t.Fatal(e)
				}
				var admissions, members int
				e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id='household' AND member_incarnation IS NULL),(SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id='household')`, room).Scan(&admissions, &members)
				if e != nil || admissions != 1 || members != 0 {
					t.Fatal("leave lost original invitation or retained joined permission", e)
				}
				if kind == "rejoin-same-original-invite" {
					if !nativeMemberByID(t, f.join(t, "household", false), "household").Joined {
						t.Fatal("original invitation could not rejoin")
					}
				}
				return
			}
			if kind == "creator-leave-unchanged" {
				if _, e := a.LeaveNativeRoom(f.request(t, "POST", false, t.Context()), "target", room); e != nil {
					t.Fatal(e)
				}
				var n int
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id='target'`, room).Scan(&n); e != nil || n != 1 {
					t.Fatal("creator leave changed existing semantics", e)
				}
				return
			}
			if kind == "creator-only-end" {
				f.join(t, "household", false)
				if result, e := a.EndNativeRoom(f.request(t, "POST", false, t.Context()), "household", room); e == nil || result != nil {
					t.Fatal("native member ended another creator's room")
				}
				return
			}
			if kind == "already-ended-idempotent" {
				if _, e := a.EndNativeRoom(f.request(t, "POST", false, t.Context()), "target", room); e != nil {
					t.Fatal(e)
				}
				if _, e := a.EndNativeRoom(f.request(t, "POST", false, t.Context()), "target", room); e != nil {
					t.Fatal(e)
				}
				var revision int64
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT revision FROM watch_rooms WHERE id=$1`, room).Scan(&revision); e != nil || revision != 1 {
					t.Fatal("repeated end wrote another revision", revision, e)
				}
				if view, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room); e != nil || view == nil || view.Room.Status != models.WatchRoomStatusEnded {
					t.Fatal("current original ended audit view denied", e)
				}
				edge := time.Now().UTC().Add(-24*time.Hour + 3*time.Second).Truncate(time.Microsecond)
				f.f.base.exec(t, `UPDATE watch_rooms SET ended_at=$2 WHERE id=$1`, room, edge)
				if view, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room); e != nil || view == nil || !view.PublicationUntil.Equal(edge.Add(24*time.Hour)) {
					t.Fatal("audit output exceeded the exact fixed24h boundary", e)
				}
				f.f.base.exec(t, `UPDATE watch_rooms SET ended_at=$2 WHERE id=$1`, room, time.Now().UTC().Add(-25*time.Hour))
				if view, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room); !errors.Is(e, ErrWatchRoomRecreate) || view != nil {
					t.Fatal("maintenance delay retained an old ended audit payload", e)
				}
				if result, e := a.EndNativeRoom(f.request(t, "POST", false, t.Context()), "target", room); !errors.Is(e, ErrWatchRoomRecreate) || result != nil {
					t.Fatal("repeated End renewed an old audit window", e)
				}
				return
			}
			if kind == "playing-position-bounded-data" {
				f.f.base.exec(t, `UPDATE watch_rooms SET status='playing',waiting_for_ready=false,position=10,duration=12,anchor_updated_at=$2 WHERE id=$1`, room, time.Now().Add(-5*time.Second))
			}
			result, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room)
			if e != nil || result == nil || result.Room.ID != room || len(result.Room.Members) != 3 {
				t.Fatal("actual original bounded native DATA", e)
			}
			if kind == "playing-position-bounded-data" {
				if result.Room.Position != 12 {
					t.Fatal("native current position escaped actual duration", result.Room.Position)
				}
			} else if nativeMemberByID(t, result.Room, "household").Joined {
				t.Fatal("unjoined original invite lost native wire semantics")
			}
		})
	}
}

func TestNativeRoomAccountInvitations(t *testing.T) {
	for _, kind := range []string{"current-username-original-account", "same-account-denied", "unknown-account-denied", "expired-recipient-denied", "creator-only-invite", "recipient-inbox-current-account", "room-inbox-current-creator", "accept-original-recipient-profile", "accept-other-account-denied", "same-tx-accept-admission", "decline-current-recipient", "revoke-current-creator", "accepted-member-independent-of-invite", "repeat-pending-already-invited", "fresh-after-decline-or-revoke", "recipient-pending-cap", "room-invitation-cap", "account-list-byte-budget"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeRoomFixture(t, kind, nil, nil, t.Context())
			a, room := f.f.authority, f.f.room.ID
			if kind == "same-account-denied" || kind == "unknown-account-denied" || kind == "expired-recipient-denied" || kind == "creator-only-invite" {
				username, profile := "Synthetic Other", "target"
				switch kind {
				case "same-account-denied":
					username = "Synthetic Actor"
				case "unknown-account-denied":
					username = "Synthetic Missing"
				case "expired-recipient-denied":
					f.f.base.exec(t, `UPDATE accounts SET expires_at=now()-interval '1 minute' WHERE id='other'`)
				case "creator-only-invite":
					profile = "household"
				}
				if result, e := a.InviteNativeAccount(f.request(t, "POST", false, t.Context()), profile, room, username); e == nil || result != nil {
					t.Fatal("invalid native recipient/creator minted invitation")
				}
				return
			}
			if kind == "recipient-pending-cap" {
				f.f.base.exec(t, `INSERT INTO watch_rooms(id,creator_profile_id,title,media_type,item_id,expires_at)SELECT 'synthetic-inbox-'||i,'target','Synthetic inbox','movie','synthetic',now()+interval '1 hour' FROM generate_series(1,256)i;INSERT INTO watch_room_account_invites(id,room_id,inviter_account_id,invitee_account_id,expires_at)SELECT 'synthetic-pending-'||i,'synthetic-inbox-'||i,'actor','other',now()+interval '1 hour' FROM generate_series(1,256)i`)
				if result, e := a.InviteNativeAccount(f.request(t, "POST", false, t.Context()), "target", room, "Synthetic Other"); !errors.Is(e, ErrWatchRoomBusy) || result != nil {
					t.Fatal("recipient pending invitation cap bypassed", e)
				}
				return
			}
			if kind == "room-invitation-cap" {
				f.f.base.exec(t, `INSERT INTO accounts(id,username,password_hash)SELECT 'synthetic-native-recipient-'||i,'Synthetic Native Recipient '||i,'synthetic-unused-hash' FROM generate_series(1,128)i`)
				f.f.base.exec(t, `INSERT INTO watch_room_account_invites(id,room_id,inviter_account_id,invitee_account_id,expires_at)SELECT 'synthetic-room-pending-'||i,$1,'actor','synthetic-native-recipient-'||i,now()+interval '1 hour' FROM generate_series(1,128)i`, room)
				if result, e := a.InviteNativeAccount(f.request(t, "POST", false, t.Context()), "target", room, "Synthetic Other"); !errors.Is(e, ErrWatchRoomBusy) || result != nil {
					t.Fatal("room pending invitation cap bypassed", e)
				}
				return
			}
			invite := f.invite(t)
			switch kind {
			case "current-username-original-account":
				if invite.InviteeAccountID != "other" || invite.InviteeUsername != "Synthetic Other" {
					t.Fatal("actual current recipient not captured")
				}
				var count int
				e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_account_invites i JOIN watch_room_account_invite_provenance p ON p.invite_id=i.id AND p.invite_incarnation=i.invite_incarnation JOIN accounts a ON a.id=i.invitee_account_id AND a.share_incarnation=p.recipient_account_incarnation WHERE i.id=$1`, invite.ID).Scan(&count)
				if e != nil || count != 1 {
					t.Fatal("original recipient private witness missing", e)
				}
			case "recipient-inbox-current-account":
				result, e := a.ListNativeAccountInvitations(f.request(t, "GET", true, t.Context()))
				if e != nil || result == nil || len(result.Invites) != 1 || result.Invites[0].ID != invite.ID || result.Invites[0].Title != f.f.room.Title {
					t.Fatal("current own-account inbox", e)
				}
			case "room-inbox-current-creator":
				result, e := a.ListNativeRoomAccountInvitations(f.request(t, "GET", false, t.Context()), "target", room)
				if e != nil || result == nil || len(result.Invites) != 1 || result.Invites[0].InviteeUsername != "Synthetic Other" {
					t.Fatal("current creator outbox", e)
				}
			case "accept-original-recipient-profile", "same-tx-accept-admission":
				view := f.accept(t, invite)
				if nativeMemberByID(t, view, "recipient").Joined {
					t.Fatal("accept implicitly joined native player")
				}
				var count int
				e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_account_invites i JOIN watch_room_native_admissions n ON n.room_id=i.room_id AND n.profile_id=i.accepted_profile_id AND n.origin_invite_nonce=i.invite_incarnation JOIN watch_room_invites v ON v.room_id=n.room_id AND v.profile_id=n.profile_id AND concat(v.tableoid::oid,':',v.xmin::text,':',v.ctid::text)=n.native_invite_receipt WHERE i.id=$1 AND i.status='accepted' AND n.account_id='other'`, invite.ID).Scan(&count)
				if e != nil || count != 1 {
					t.Fatal("same-TX accepted profile admission missing", e)
				}
			case "accept-other-account-denied":
				if result, e := a.AcceptNativeAccountInvitation(f.request(t, "POST", false, t.Context()), "household", invite.ID); e == nil || result != nil {
					t.Fatal("another full account accepted original invitation")
				}
			case "decline-current-recipient", "fresh-after-decline-or-revoke":
				if _, e := a.DeclineNativeAccountInvitation(f.request(t, "POST", true, t.Context()), invite.ID); e != nil {
					t.Fatal(e)
				}
				if result, e := a.AcceptNativeAccountInvitation(f.request(t, "POST", true, t.Context()), "recipient", invite.ID); e == nil || result != nil {
					t.Fatal("declined original invite accepted")
				}
				if kind == "fresh-after-decline-or-revoke" {
					fresh := f.invite(t)
					if fresh.ID == invite.ID {
						t.Fatal("explicit reissue adopted old invite ID")
					}
					if _, e := a.RevokeNativeAccountInvitation(f.request(t, "DELETE", false, t.Context()), "target", room, fresh.ID); e != nil {
						t.Fatal(e)
					}
					again := f.invite(t)
					if again.ID == fresh.ID {
						t.Fatal("revoked invite did not get fresh original identity")
					}
				}
			case "revoke-current-creator":
				if _, e := a.RevokeNativeAccountInvitation(f.request(t, "DELETE", false, t.Context()), "target", room, invite.ID); e != nil {
					t.Fatal(e)
				}
				if result, e := a.AcceptNativeAccountInvitation(f.request(t, "POST", true, t.Context()), "recipient", invite.ID); e == nil || result != nil {
					t.Fatal("revoked original invite accepted")
				}
			case "accepted-member-independent-of-invite":
				f.accept(t, invite)
				f.f.base.exec(t, `UPDATE watch_room_account_invites SET status='revoked',revoked_at=now() WHERE id=$1`, invite.ID)
				fresh := f.invite(t)
				if fresh.ID == invite.ID {
					t.Fatal("account invitation not genuinely reissued")
				}
				if !nativeMemberByID(t, f.join(t, "recipient", true), "recipient").Joined {
					t.Fatal("account invite reissue revoked original accepted membership")
				}
			case "repeat-pending-already-invited":
				if result, e := a.InviteNativeAccount(f.request(t, "POST", false, t.Context()), "target", room, "synthetic other"); !errors.Is(e, ErrWatchRoomConflict) || result != nil {
					t.Fatal("pending original invitation duplicated", e)
				}
			case "account-list-byte-budget":
				f.f.base.exec(t, `UPDATE watch_rooms SET poster_url=repeat('x',1048576) WHERE id=$1`, room)
				if result, e := a.ListNativeAccountInvitations(f.request(t, "GET", true, t.Context())); e == nil || result != nil {
					t.Fatal("oversized inbox DATA copied before bounded scalar denial")
				}
			}
		})
	}
}

func TestNativeRoomIdentityContinuity(t *testing.T) {
	for _, kind := range []string{"missing-092-recreate", "generic-room-no-upgrade", "generic-native-invite-no-upgrade", "generic-account-invite-no-upgrade", "profile-delete-recreate", "account-delete-recreate", "orphan-profile-current-owner", "member-delete-recreate", "explicit-copied-member-nonce", "invite-delete-recreate", "explicit-copied-account-invite-nonce", "original-role-change", "original-session-revoked", "foreign-store-frame", "copied-authority", "dto-frame-denied", "row-security-unknown", "same-value-actor-physical-change"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeRoomFixture(t, kind, nil, nil, t.Context())
			a, room, profile := f.f.authority, f.f.room.ID, "target"
			r := f.request(t, "GET", false, t.Context())
			var invite *models.WatchRoomAccountInvite
			if kind == "generic-account-invite-no-upgrade" || kind == "explicit-copied-account-invite-nonce" || kind == "account-delete-recreate" {
				invite = f.invite(t)
				r = f.request(t, "POST", true, t.Context())
			}
			switch kind {
			case "missing-092-recreate":
				f.f.base.exec(t, `DELETE FROM watch_room_native_admissions WHERE room_id=$1`, room)
			case "generic-room-no-upgrade":
				ordinary := *f.f.room
				ordinary.ID = "synthetic-unattested-native-room"
				if e := f.f.base.ds.WatchRooms().Create(t.Context(), &ordinary, []string{"household"}, "synthetic", watchRoomInput().Capabilities); e != nil {
					t.Fatal(e)
				}
				room = ordinary.ID
			case "generic-native-invite-no-upgrade":
				profile = "household"
				f.f.base.exec(t, `DELETE FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id='household'`, room)
			case "generic-account-invite-no-upgrade":
				f.f.base.exec(t, `DELETE FROM watch_room_account_invite_provenance WHERE invite_id=$1`, invite.ID)
			case "profile-delete-recreate":
				profile = "household"
				f.f.base.exec(t, `DELETE FROM users WHERE id='household'`)
				f.f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('household','actor','Synthetic Household')`)
			case "account-delete-recreate":
				var username, password string
				var master bool
				var maxStreams int
				var expiry *time.Time
				var created, updated time.Time
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT username,password_hash,is_master,max_streams,expires_at,created_at,updated_at FROM accounts WHERE id='other'`).Scan(&username, &password, &master, &maxStreams, &expiry, &created, &updated); e != nil {
					t.Fatal(e)
				}
				var before, after string
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id='recipient'`).Scan(&before); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `DELETE FROM accounts WHERE id='other'`)
				f.f.base.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master,max_streams,expires_at,created_at,updated_at)VALUES('other',$1,$2,$3,$4,$5,$6,$7)`, username, password, master, maxStreams, expiry, created, updated)
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id='recipient'`).Scan(&after); e != nil || before != after {
					t.Fatal("012 orphan-profile witness changed during actual account ABA", e)
				}
			case "orphan-profile-current-owner":
				profile = "household"
				f.f.base.exec(t, `UPDATE users SET account_id='synthetic-missing-native-owner' WHERE id='household'`)
			case "member-delete-recreate", "explicit-copied-member-nonce":
				profile = "household"
				f.join(t, profile, false)
				var inc string
				var joined time.Time
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT member_incarnation::text,joined_at FROM watch_room_members WHERE room_id=$1 AND profile_id='household'`, room).Scan(&inc, &joined); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `DELETE FROM watch_room_members WHERE room_id=$1 AND profile_id='household'`, room)
				if kind == "explicit-copied-member-nonce" {
					f.f.base.exec(t, `INSERT INTO watch_room_members(room_id,profile_id,joined_at,member_incarnation)VALUES($1,'household',$2,$3::uuid)`, room, joined, inc)
				} else {
					f.f.base.exec(t, `INSERT INTO watch_room_members(room_id,profile_id,joined_at)VALUES($1,'household',$2)`, room, joined)
				}
				var current string
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT member_incarnation::text FROM watch_room_members WHERE room_id=$1 AND profile_id='household'`, room).Scan(&current); e != nil || current == inc {
					t.Fatal("actual member insertion adopted old nonce", e)
				}
			case "invite-delete-recreate":
				profile = "household"
				var invited time.Time
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT invited_at FROM watch_room_invites WHERE room_id=$1 AND profile_id='household'`, room).Scan(&invited); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `DELETE FROM watch_room_invites WHERE room_id=$1 AND profile_id='household'`, room)
				f.f.base.exec(t, `INSERT INTO watch_room_invites(room_id,profile_id,invited_at)VALUES($1,'household',$2)`, room, invited)
			case "explicit-copied-account-invite-nonce":
				var before, after string
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT invite_incarnation::text FROM watch_room_account_invites WHERE id=$1`, invite.ID).Scan(&before); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `UPDATE watch_room_account_invites SET invite_incarnation=invite_incarnation WHERE id=$1`, invite.ID)
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT invite_incarnation::text FROM watch_room_account_invites WHERE id=$1`, invite.ID).Scan(&after); e != nil || before == after {
					t.Fatal("explicit same-value nonce assignment restored identity", e)
				}
			case "original-role-change":
				f.f.base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
			case "original-session-revoked":
				f.f.base.exec(t, `DELETE FROM sessions WHERE token='synthetic-share-frame'`)
			case "foreign-store-frame":
				other, e := New(t.Context(), f.f.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(other.Close)
				var release context.CancelFunc
				r, release, e = other.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/users/target/watch-rooms", nil), "synthetic-share-frame", "synthetic-share-client")
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(release)
			case "copied-authority":
				a = &WatchRoomAuthority{self: a, store: a.store, manager: a.manager, security: a.security, coordinator: a.coordinator, ctx: a.ctx}
			case "dto-frame-denied":
				session, ok := auth.GetSession(r)
				if !ok {
					t.Fatal("actual source frame absent")
				}
				raw := httptest.NewRequest("GET", "http://synthetic.test/api/users/target/watch-rooms", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, session.IsMaster)
				r = raw.WithContext(ctx)
			case "row-security-unknown":
				f.f.base.exec(t, `ALTER TABLE users ENABLE ROW LEVEL SECURITY;ALTER TABLE users FORCE ROW LEVEL SECURITY`)
			case "same-value-actor-physical-change":
				var actor, profileRow, sessionRow, inc string
				query := `SELECT concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),a.share_incarnation::text FROM accounts a JOIN users u ON u.account_id=a.id AND u.id='target' JOIN sessions s ON s.account_id=a.id AND s.token='synthetic-share-frame' WHERE a.id='actor'`
				if e := f.f.base.ds.pool.QueryRow(t.Context(), query).Scan(&actor, &profileRow, &sessionRow, &inc); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `UPDATE accounts SET username=username WHERE id='actor'`)
				var nextActor, nextProfile, nextSession, nextInc string
				if e := f.f.base.ds.pool.QueryRow(t.Context(), query).Scan(&nextActor, &nextProfile, &nextSession, &nextInc); e != nil || nextActor == actor || nextProfile != profileRow || nextSession != sessionRow || nextInc != inc {
					t.Fatal("same logical actor update did not isolate physical receipt", e)
				}
			}
			if invite != nil {
				if result, e := a.AcceptNativeAccountInvitation(r, "recipient", invite.ID); e == nil || result != nil {
					t.Fatal("unattested/recreated original account invite authorized acceptance", kind)
				}
				return
			}
			if kind == "member-delete-recreate" || kind == "explicit-copied-member-nonce" {
				if result, e := a.SetNativeReady(f.request(t, "POST", false, t.Context()), profile, room, true); e == nil || result != nil {
					t.Fatal("recreated member adopted current command permission", kind)
				}
				return
			}
			if result, e := a.ReadNativeRoom(r, profile, room); e == nil || result != nil {
				t.Fatal("missing/foreign/recreated native identity adopted room permission", kind)
			}
		})
	}
}

func nativeSQLWait(t *testing.T, f *nativeRoomFixture, needle string, holderPID uint32) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		var waiting bool
		e := f.f.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity waiting WHERE waiting.application_name=$1 AND waiting.wait_event_type='Lock' AND waiting.query LIKE $2 AND $3::integer=ANY(pg_catalog.pg_blocking_pids(waiting.pid)))`, f.f.base.schema, "%"+needle+"%", holderPID).Scan(&waiting)
		if e != nil {
			t.Fatal(e)
		}
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual original native SQL did not wait", needle)
}
func nativeHeldTransaction(t *testing.T, kind string) {
	t.Helper()
	f := newNativeRoomFixture(t, kind, nil, nil, t.Context())
	room := f.f.room.ID
	if kind == "held-current-member-caller-cancel" {
		f.join(t, "household", false)
	}
	var invite *models.WatchRoomAccountInvite
	if kind == "held-account-invite-caller-cancel" {
		invite = f.invite(t)
	}
	holder, e := f.f.base.admin.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var closeDone chan struct{}
	var err error
	var success bool
	started := false
	t.Cleanup(func() {
		cancel()
		recordingStateRollback(holder)
		if started {
			bridgeSessionAwait(t, done)
		}
		if closeDone != nil {
			bridgeSessionAwait(t, closeDone)
		}
	})
	needle := "watch_room_provenance"
	switch kind {
	case "held-current-member-caller-cancel":
		needle = "watch_room_members"
		_, e = holder.Exec(t.Context(), `SELECT profile_id FROM `+pgx.Identifier{f.f.base.schema, "watch_room_members"}.Sanitize()+` WHERE room_id=$1 AND profile_id='household' FOR UPDATE`, room)
	case "held-account-invite-caller-cancel":
		needle = "watch_room_account_invites"
		_, e = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.f.base.schema, "watch_room_account_invites"}.Sanitize()+` WHERE id=$1 FOR UPDATE`, invite.ID)
	default:
		_, e = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.f.base.schema, "watch_rooms"}.Sanitize()+` WHERE id=$1 FOR UPDATE`, room)
	}
	if e != nil {
		t.Fatal(e)
	}
	request := f.request(t, "POST", invite != nil, ctx)
	started = true
	go func() {
		defer close(done)
		if invite != nil {
			result, e := f.f.authority.AcceptNativeAccountInvitation(request, "recipient", invite.ID)
			err = e
			success = result != nil
		} else if kind == "held-current-member-caller-cancel" {
			result, e := f.f.authority.HeartbeatNativeRoom(request, "household", room, "synthetic-held", true)
			err = e
			success = result != nil
		} else {
			result, e := f.f.authority.JoinNativeRoom(request, "household", room, "synthetic-held", watchRoomInput().Capabilities)
			err = e
			success = result != nil
		}
	}()
	nativeSQLWait(t, f, needle, holder.Conn().PgConn().PID())
	if kind == "held-room-runtime-cancel" {
		closeDone = make(chan struct{})
		go func() { defer close(closeDone); f.f.authority.Close() }()
	} else {
		cancel()
	}
	bridgeSessionAwait(t, done)
	if closeDone != nil {
		bridgeSessionAwait(t, closeDone)
	}
	if err == nil || success {
		t.Fatal("held cancelled native mutation published success", kind, err)
	}
}

func TestNativeRoomTransactionLifetime(t *testing.T) {
	for _, kind := range []string{"held-current-member-caller-cancel", "held-room-runtime-cancel", "held-account-invite-caller-cancel", "known-rollback-no-partial-admission", "unknown-ack-no-payload", "late-known-ack-no-success", "physical-cleanup-before-refund", "writer-first-late-guard", "reader-first-writer-blocked", "combined-member-last-slot", "complete-native-guest-data-budget", "healthy-physical-connection-reuse"} {
		t.Run(kind, func(t *testing.T) {
			if strings.HasPrefix(kind, "held-") {
				nativeHeldTransaction(t, kind)
				return
			}
			var armed, enabled, dropped atomic.Bool
			var outputCancel context.CancelFunc
			entered, proceed := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(proceed) }) }
			var gateMu sync.Mutex
			var gates []*queryLifecycleGatedConn
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, data pgx.TraceQueryEndData) {
				if data.Err != nil {
					return
				}
				if kind == "late-known-ack-no-success" && strings.EqualFold(strings.TrimSpace(q), "COMMIT") && armed.CompareAndSwap(true, false) {
					outputCancel()
				}
				if kind == "reader-first-writer-blocked" && strings.Contains(q, `"users" IN SHARE MODE NOWAIT`) && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "unknown-ack-no-payload" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					return &bridgeSessionDropACK{Conn: conn, enabled: &enabled, actual: &parentDropCommit{Conn: conn, dropped: &dropped}}, nil
				}
			}
			if kind == "physical-cleanup-before-refund" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					gate := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
					gateMu.Lock()
					gates = append(gates, gate)
					gateMu.Unlock()
					return gate, nil
				}
			}
			f := newNativeRoomFixture(t, kind, trace, dial, t.Context())
			a, room := f.f.authority, f.f.room.ID
			if kind == "known-rollback-no-partial-admission" || kind == "unknown-ack-no-payload" || kind == "late-known-ack-no-success" {
				invite := f.invite(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				outputCancel = cancel
				request := f.request(t, "POST", true, ctx)
				if kind == "known-rollback-no-partial-admission" {
					f.f.base.exec(t, `CREATE FUNCTION synthetic_native_admission_rollback()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.admission_kind='account' THEN RAISE EXCEPTION 'synthetic original native admission rollback';END IF;RETURN NEW;END$$;CREATE TRIGGER synthetic_native_admission_rollback BEFORE INSERT ON watch_room_native_admissions FOR EACH ROW EXECUTE FUNCTION synthetic_native_admission_rollback()`)
				}
				armed.Store(true)
				enabled.Store(true)
				result, operationErr := a.AcceptNativeAccountInvitation(request, "recipient", invite.ID)
				if operationErr == nil || result != nil {
					t.Fatal("failed/late native acknowledgement published room DATA", kind, operationErr)
				}
				var status string
				var admissions int
				if e := f.f.base.admin.QueryRow(t.Context(), `SELECT i.status,(SELECT count(*) FROM `+pgx.Identifier{f.f.base.schema, "watch_room_native_admissions"}.Sanitize()+` n WHERE n.room_id=i.room_id AND n.profile_id='recipient') FROM `+pgx.Identifier{f.f.base.schema, "watch_room_account_invites"}.Sanitize()+` i WHERE i.id=$1`, invite.ID).Scan(&status, &admissions); e != nil {
					t.Fatal(e)
				}
				if kind == "known-rollback-no-partial-admission" {
					if status != "pending" || admissions != 0 {
						t.Fatal("known rollback retained partial native acceptance", status, admissions)
					}
				} else if status != "accepted" || admissions != 1 {
					t.Fatal("lost output incorrectly claimed rollback", status, admissions)
				}
				if kind == "unknown-ack-no-payload" && (!errors.Is(operationErr, ErrWatchRoomUnknown) || !dropped.Load()) {
					t.Fatal("actual unknown native COMMIT ACK classification lost", operationErr, dropped.Load())
				}
				if kind == "known-rollback-no-partial-admission" && !errors.Is(operationErr, ErrWatchRoomUnavailable) {
					t.Fatal("known native rollback error classification", operationErr)
				}
				if kind == "late-known-ack-no-success" && !errors.Is(operationErr, ErrWatchRoomChanged) {
					t.Fatal("late original request did not deny publication", operationErr)
				}
				return
			}
			if kind == "physical-cleanup-before-refund" {
				request := f.request(t, "GET", false, t.Context())
				conn, e := f.f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				physical := conn.Conn().PgConn()
				gate, ok := physical.Conn().(*queryLifecycleGatedConn)
				conn.Release()
				if !ok {
					t.Fatal("actual native PG transport not retained")
				}
				done := make(chan struct{})
				var result *WatchRoomNativeResult
				var readErr error
				t.Cleanup(func() {
					gateMu.Lock()
					all := append([]*queryLifecycleGatedConn(nil), gates...)
					gateMu.Unlock()
					for _, gate := range all {
						gate.release()
					}
					bridgeSessionAwait(t, done)
				})
				gate.armed.Store(true)
				go func() { defer close(done); result, readErr = a.ReadNativeRoom(request, "target", room) }()
				bridgeSessionAwait(t, gate.closeEntered)
				a.mu.Lock()
				charged := a.active
				a.mu.Unlock()
				if charged != 1 {
					t.Fatal("native operation refunded before physical cleanup", charged)
				}
				select {
				case <-done:
					t.Fatal("native read returned before transport end")
				default:
				}
				select {
				case <-physical.CleanupDone():
					t.Fatal("native cleanup declared done while actual Close held")
				default:
				}
				gate.release()
				bridgeSessionAwait(t, physical.CleanupDone())
				bridgeSessionAwait(t, done)
				if readErr == nil || result != nil {
					t.Fatal("broken actual native SQL certified DATA")
				}
				return
			}
			if kind == "writer-first-late-guard" {
				r := f.request(t, "GET", false, t.Context())
				holder, e := f.f.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer recordingStateRollback(holder)
				if _, e = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.f.base.schema, "users"}.Sanitize()+` IN ROW EXCLUSIVE MODE`); e != nil {
					t.Fatal(e)
				}
				if result, e := a.ReadNativeRoom(r, "target", room); e == nil || result != nil {
					t.Fatal("late actual table writer was silently adopted")
				}
				return
			}
			if kind == "reader-first-writer-blocked" {
				ctx, cancel := context.WithCancel(t.Context())
				done, writerDone := make(chan struct{}), make(chan struct{})
				var result *WatchRoomNativeResult
				var readErr, writerErr error
				writerStarted := false
				readerStarted := false
				t.Cleanup(func() {
					cancel()
					release()
					if readerStarted {
						bridgeSessionAwait(t, done)
					}
					if writerStarted {
						bridgeSessionAwait(t, writerDone)
					}
				})
				r := f.request(t, "GET", false, ctx)
				armed.Store(true)
				readerStarted = true
				go func() { defer close(done); result, readErr = a.ReadNativeRoom(r, "target", room) }()
				bridgeSessionAwait(t, entered)
				writerStarted = true
				go func() {
					defer close(writerDone)
					_, writerErr = f.f.base.admin.Exec(ctx, `UPDATE `+pgx.Identifier{f.f.base.schema, "accounts"}.Sanitize()+` SET expires_at=now()+interval '30 minutes' WHERE id='actor'`)
				}()
				until := time.Now().Add(time.Second)
				waiting := false
				for time.Now().Before(until) {
					if e := f.f.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE $1)`, `UPDATE `+pgx.Identifier{f.f.base.schema, "accounts"}.Sanitize()+` SET expires_at=%`).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("actual writer did not block behind original current native guard")
				}
				select {
				case <-writerDone:
					t.Fatal("writer crossed original native transaction")
				default:
				}
				release()
				bridgeSessionAwait(t, done)
				bridgeSessionAwait(t, writerDone)
				if readErr != nil || result == nil || writerErr != nil {
					t.Fatal("original reader/writer closure failed", readErr, writerErr)
				}
				return
			}
			if kind == "combined-member-last-slot" || kind == "complete-native-guest-data-budget" {
				f.f.base.exec(t, `INSERT INTO watch_room_guest_members(room_id,guest_id,name)SELECT $1,'synthetic-native-guest-'||i,'Synthetic native guest' FROM generate_series(1,254)i`, room)
				if kind == "complete-native-guest-data-budget" {
					view, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room)
					if e != nil || view == nil || len(view.Room.Members) != 257 {
						t.Fatal("complete bounded native/invited/guest view failed", e)
					}
					f.f.base.exec(t, `UPDATE watch_room_guest_members SET name=repeat('x',1048576) WHERE room_id=$1 AND guest_id='synthetic-native-guest-1'`, room)
					if result, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room); e == nil || result != nil {
						t.Fatal("oversized native guest view crossed scalar budget")
					}
					return
				}
				var wg sync.WaitGroup
				var success, busy atomic.Int32
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				requests := []*http.Request{f.request(t, "POST", false, ctx), f.request(t, "POST", false, ctx)}
				done := make(chan struct{})
				t.Cleanup(func() { cancel(); bridgeSessionAwait(t, done) })
				for index, profile := range []string{"household", "household2"} {
					wg.Add(1)
					go func(i int, id string) {
						defer wg.Done()
						result, e := a.JoinNativeRoom(requests[i], id, room, "synthetic-final-slot", watchRoomInput().Capabilities)
						if e == nil && result != nil {
							success.Add(1)
						} else if errors.Is(e, ErrWatchRoomBusy) {
							busy.Add(1)
						} else {
							t.Error("actual final native slot unexpected outcome", e)
						}
					}(index, profile)
				}
				go func() { wg.Wait(); close(done) }()
				bridgeSessionAwait(t, done)
				var count int
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_members WHERE room_id=$1)+(SELECT count(*) FROM watch_room_guest_members WHERE room_id=$1)`, room).Scan(&count); e != nil || count != 256 || success.Load() != 1 || busy.Load() != 1 {
					t.Fatal("same-room locked last native slot not serialized", count, success.Load(), busy.Load(), e)
				}
				return
			}
			if kind == "healthy-physical-connection-reuse" {
				conn, e := f.f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				before := conn.Conn().PgConn().PID()
				conn.Release()
				result, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", room)
				if e != nil || result == nil {
					t.Fatal(e)
				}
				conn, e = f.f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				after := conn.Conn().PgConn().PID()
				conn.Release()
				if before != after {
					t.Fatal("healthy native current transaction discarded reusable actual connection", before, after)
				}
			}
		})
	}
}
