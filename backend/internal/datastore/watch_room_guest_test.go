//go:build linux

package datastore

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/internal/auth"
	"novastream/models"
)

type partyDSFixture struct {
	room   *watchRoomFixture
	invite *models.WatchRoomExternalInvite
	joined *WatchPartyJoinResult
}

func newPartyDS(t *testing.T, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), life context.Context) *partyDSFixture {
	t.Helper()
	f := newWatchRoomFixture(t, trace, dial, life)
	f.create(t)
	invite, e := f.authority.ReplaceExternalInvite(f.request(t, "POST", t.Context()), "target", f.room.ID)
	if e != nil {
		t.Fatal("actual original party admission", e)
	}
	return &partyDSFixture{room: f, invite: invite}
}
func (f *partyDSFixture) request(ctx context.Context, method string) *http.Request {
	r := httptest.NewRequest(method, "http://synthetic.test/watch-party/"+f.invite.Token, nil).WithContext(ctx)
	r.RemoteAddr = "192.0.2.7:41000"
	r.Header.Set("User-Agent", "Synthetic party client")
	return r
}
func (f *partyDSFixture) join(t *testing.T) {
	t.Helper()
	joined, e := f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), f.invite.Token, false, "Synthetic guest", "")
	if e != nil || joined == nil {
		t.Fatal("actual original party guest", e)
	}
	f.joined = joined
}
func (f *partyDSFixture) guestFrame(t *testing.T, ctx context.Context, method string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.test/api/watch-party/room/"+f.room.room.ID, nil).WithContext(ctx)
	actual, release, e := f.room.authority.AuthenticateGuestRequest(r, f.joined.Session.Token, f.room.room.ID)
	if e != nil {
		t.Fatal("actual original guest frame", e)
	}
	t.Cleanup(release)
	return actual
}
func (f *partyDSFixture) bind(t *testing.T) {
	t.Helper()
	result, e := f.room.authority.BindExternalSource(f.room.request(t, "POST", t.Context()), "target", f.room.room.ID, "synthetic:original-party.wav", map[string]string{"profileId": "target", "titleId": "synthetic-party"})
	if e != nil || result == nil {
		t.Fatal("actual original first source", e)
	}
	f.room.base.exec(t, `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, f.room.room.ID)
}
func (f *partyDSFixture) guestCounts(t *testing.T) (int, int) {
	t.Helper()
	var guests, sessions int
	if e := f.room.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_guest_members WHERE room_id=$1),(SELECT count(*) FROM sessions WHERE scope='watch_party')`, f.room.room.ID).Scan(&guests, &sessions); e != nil {
		t.Fatal(e)
	}
	return guests, sessions
}
func TestWatchPartyPublicJoin(t *testing.T) {
	for _, kind := range []string{"token-original", "code-original", "lobby-no-source", "first-source-after-join", "invalid-secret", "inactive-invite", "room-ended", "creator-share-disabled", "creator-pin-unknown", "guest-session-nonmaster", "same-tx-session-member", "existing-original-rejoin"} {
		t.Run(kind, func(t *testing.T) {
			f := newPartyDS(t, nil, nil, t.Context())
			secret, code := f.invite.Token, false
			switch kind {
			case "code-original":
				secret = " " + strings.ToLower(strings.ReplaceAll(f.invite.ShortCode, "-", " ")) + " "
				code = true
			case "invalid-secret":
				secret = strings.Repeat("x", 42)
			case "inactive-invite":
				if e := f.room.authority.RevokeExternalInvite(f.room.request(t, "DELETE", t.Context()), "target", f.room.room.ID); e != nil {
					t.Fatal(e)
				}
			case "room-ended":
				f.room.base.exec(t, `UPDATE watch_rooms SET status='ended' WHERE id=$1`, f.room.room.ID)
			case "creator-share-disabled":
				f.room.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
			case "creator-pin-unknown":
				f.room.base.exec(t, `UPDATE users SET pin_hash='',pin_length=4 WHERE id='target'`)
			case "guest-session-nonmaster":
				// Preserve actual ordinary master ownership while the delegated row stays
				// nonmaster. Recreate original room/admission under that current role.
				f.room.base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.room.create(t)
				var e error
				f.invite, e = f.room.authority.ReplaceExternalInvite(f.room.request(t, "POST", t.Context()), "target", f.room.room.ID)
				if e != nil {
					t.Fatal(e)
				}
				secret = f.invite.Token
			}
			result, e := f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), secret, code, "Synthetic guest", "")
			denied := kind == "invalid-secret" || kind == "inactive-invite" || kind == "room-ended" || kind == "creator-share-disabled" || kind == "creator-pin-unknown"
			if denied {
				if e == nil || result != nil {
					t.Fatal("invalid original public admission succeeded", kind)
				}
				if g, s := f.guestCounts(t); g != 0 || s != 0 {
					t.Fatal("denied join left member/session", g, s)
				}
				return
			}
			if e != nil || result == nil || result.Session.IsMaster || result.Session.Scope != models.SessionScopeWatchParty || result.Session.ScopeResource != f.room.room.ID || result.Session.AccountID != "actor" || !partyCanonicalToken(result.Session.Token) {
				t.Fatal("actual public guest wire/purpose", e)
			}
			f.joined = result
			var correlated bool
			e = f.room.base.ds.pool.QueryRow(t.Context(), `SELECT s.watch_party_guest_binding_id=b.binding_id AND b.guest_incarnation=g.guest_incarnation AND b.room_incarnation=r.room_incarnation AND b.admission_nonce=p.admission_nonce AND s.expires_at=b.expires_at AND NOT s.is_master FROM sessions s JOIN watch_party_guest_bindings b ON b.session_token=s.token JOIN watch_room_guest_members g ON g.room_id=b.room_id AND g.guest_id=b.guest_id JOIN watch_rooms r ON r.id=b.room_id JOIN watch_room_invite_provenance p ON p.room_id=b.room_id WHERE s.token=$1`, result.Session.Token).Scan(&correlated)
			if e != nil || !correlated {
				t.Fatal("guest/session/original admission not committed together", e)
			}
			if kind == "lobby-no-source" {
				view, e := f.room.authority.ReadGuestRoom(f.guestFrame(t, t.Context(), "GET"), f.room.room.ID)
				if e != nil || view.PlaybackReady || view.Room.Status != models.WatchRoomStatusLobby {
					t.Fatal("lobby incorrectly required/claimed source permission", e)
				}
			}
			if kind == "first-source-after-join" {
				f.bind(t)
				grant, e := f.room.authority.GrantPartyPlayback(f.guestFrame(t, t.Context(), "POST"), f.room.room.ID)
				if e != nil || grant == nil || grant.Session.Scope != models.SessionScopeStream || grant.Session.IsMaster {
					t.Fatal("genuine first source after lobby admission", e)
				}
			}
			if kind == "existing-original-rejoin" {
				again, e := f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), secret, code, "Synthetic renamed guest", result.Session.Token)
				if e != nil || again == nil || again.Session.Token != result.Session.Token || again.GuestID != result.GuestID || !again.Session.ExpiresAt.Equal(result.Session.ExpiresAt) {
					t.Fatal("returning guest identity/lifetime renewed", e)
				}
				if g, s := f.guestCounts(t); g != 1 || s != 1 {
					t.Fatal("rejoin minted duplicate guest", g, s)
				}
			}
			if kind == "same-tx-session-member" {
				// Fill descriptive rows, never fabricate another guest's private session.
				f.room.base.exec(t, `INSERT INTO watch_room_guest_members(room_id,guest_id,name)SELECT $1,'synthetic-cap-'||n,'Synthetic bounded member' FROM generate_series(1,253)n`, f.room.room.ID)
				started := make(chan struct{})
				done := make(chan struct{})
				var a, b *WatchPartyJoinResult
				var ae, be error
				t.Cleanup(func() { bridgeSessionAwait(t, done) })
				go func() {
					defer close(done)
					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						<-started
						a, ae = f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), secret, code, "Synthetic capacity A", "")
					}()
					go func() {
						defer wg.Done()
						<-started
						b, be = f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), secret, code, "Synthetic capacity B", "")
					}()
					close(started)
					wg.Wait()
				}()
				bridgeSessionAwait(t, done)
				admitted := 0
				if ae == nil && a != nil {
					admitted++
				}
				if be == nil && b != nil {
					admitted++
				}
				if admitted != 1 {
					t.Fatal("serialized final combined member slot was not unique", admitted, ae, be)
				}
				if g, s := f.guestCounts(t); g != 255 || s != 2 {
					t.Fatal("combined256 cap or sameTX count changed", g, s)
				}
				view, e := f.room.authority.ReadGuestRoom(f.guestFrame(t, t.Context(), "GET"), f.room.room.ID)
				if e != nil || view == nil || len(view.Room.Members) != 256 {
					t.Fatal("complete small-field256 member DATA unavailable", e)
				}
				f.room.base.exec(t, `UPDATE watch_room_guest_members SET name=$2 WHERE room_id=$1 AND guest_id='synthetic-cap-1'`, f.room.room.ID, strings.Repeat("x", 1<<20))
				if oversized, e := f.room.authority.ReadGuestRoom(f.guestFrame(t, t.Context(), "GET"), f.room.room.ID); e == nil || oversized != nil {
					t.Fatal("oversized complete room DATA escaped staged budget")
				}

			}
		})
	}
}
func TestWatchPartyAdmissionContinuity(t *testing.T) {
	for _, kind := range []string{"invite-rotate-existing-guest", "invite-revoke-existing-guest", "old-secret-no-new-join", "generic-invite-unattested", "legacy-room-recreate", "room-delete-recreate", "creator-profile-aba", "creator-account-aba", "guest-delete-recreate", "explicit-copied-guest-nonce", "existing-fixed-expiry", "row-security-unknown"} {
		t.Run(kind, func(t *testing.T) {
			f := newPartyDS(t, nil, nil, t.Context())
			f.join(t)
			if kind == "invite-rotate-existing-guest" || kind == "invite-revoke-existing-guest" || kind == "old-secret-no-new-join" {
				var e error
				if kind == "invite-revoke-existing-guest" {
					e = f.room.authority.RevokeExternalInvite(f.room.request(t, "DELETE", t.Context()), "target", f.room.room.ID)
				} else {
					_, e = f.room.authority.ReplaceExternalInvite(f.room.request(t, "POST", t.Context()), "target", f.room.room.ID)
				}
				if e != nil {
					t.Fatal(e)
				}
				if next, e := f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), f.invite.Token, false, "Synthetic new admission", ""); e == nil || next != nil {
					t.Fatal("retired invitation admitted another guest")
				}
				if kind != "old-secret-no-new-join" {
					view, e := f.room.authority.ReadGuestRoom(f.guestFrame(t, t.Context(), "GET"), f.room.room.ID)
					if e != nil || view == nil {
						t.Fatal("admission mutation revoked original guest identity", e)
					}
				}
				return
			}
			if kind == "explicit-copied-guest-nonce" {
				var original, current string
				if e := f.room.base.ds.pool.QueryRow(t.Context(), `SELECT guest_incarnation::text FROM watch_room_guest_members WHERE room_id=$1 AND guest_id=$2`, f.room.room.ID, f.joined.GuestID).Scan(&original); e != nil {
					t.Fatal(e)
				}
				if e := f.room.base.ds.pool.QueryRow(t.Context(), `INSERT INTO watch_room_guest_members(room_id,guest_id,name,guest_incarnation)VALUES($1,'synthetic-copied-guest','Synthetic copied identity',$2::uuid)RETURNING guest_incarnation::text`, f.room.room.ID, original).Scan(&current); e != nil || current == original {
					t.Fatal("explicit INSERT copied original guest identity", e)
				}
				return
			}
			if kind == "existing-fixed-expiry" {
				frame := f.guestFrame(t, t.Context(), "GET")
				before := f.joined.Session.ExpiresAt
				if e := f.room.authority.HeartbeatGuest(f.guestFrame(t, t.Context(), "POST"), f.room.room.ID, false); e != nil {
					t.Fatal(e)
				}
				if e := f.room.authority.SetGuestReady(f.guestFrame(t, t.Context(), "POST"), f.room.room.ID, true); e != nil {
					t.Fatal(e)
				}
				if _, e := f.room.authority.ReadGuestRoom(frame, f.room.room.ID); e != nil {
					t.Fatal(e)
				}
				var after time.Time
				if e := f.room.base.ds.pool.QueryRow(t.Context(), `SELECT expires_at FROM sessions WHERE token=$1`, f.joined.Session.Token).Scan(&after); e != nil || !after.Equal(before) {
					t.Fatal("guest polling/ready extended original expiry", e)
				}
				return
			}
			switch kind {
			case "generic-invite-unattested":
				f.room.base.exec(t, `DELETE FROM watch_room_invite_provenance WHERE room_id=$1`, f.room.room.ID)
			case "legacy-room-recreate":
				f.room.base.exec(t, `DELETE FROM watch_room_provenance WHERE room_id=$1`, f.room.room.ID)
			case "room-delete-recreate":
				f.room.base.exec(t, `DELETE FROM watch_rooms WHERE id=$1`, f.room.room.ID)
				if e := f.room.base.ds.WatchRooms().Create(t.Context(), f.room.room, nil, "", watchRoomInput().Capabilities); e != nil {
					t.Fatal(e)
				}
			case "creator-profile-aba":
				shareCopyPrincipal(t, &sharePlaybackDSFixture{base: f.room.base}, "users", "target")
			case "creator-account-aba":
				shareCopyPrincipal(t, &sharePlaybackDSFixture{base: f.room.base}, "accounts", "actor")
			case "guest-delete-recreate":
				f.room.base.exec(t, `DELETE FROM watch_room_guest_members WHERE room_id=$1 AND guest_id=$2`, f.room.room.ID, f.joined.GuestID)
				if e := f.room.base.ds.WatchRooms().JoinExternalGuest(t.Context(), f.room.room.ID, f.joined.GuestID, "Synthetic recreated guest", "", watchRoomInput().Capabilities, time.Now().UTC()); e != nil {
					t.Fatal(e)
				}
			case "row-security-unknown":
				f.room.base.exec(t, `ALTER TABLE watch_party_guest_bindings ENABLE ROW LEVEL SECURITY;ALTER TABLE watch_party_guest_bindings FORCE ROW LEVEL SECURITY;CREATE POLICY synthetic_party_visible ON watch_party_guest_bindings USING(true)WITH CHECK(true)`)
			}
			if kind == "generic-invite-unattested" || kind == "row-security-unknown" {
				if result, e := f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), f.invite.Token, false, "Synthetic new guest", ""); e == nil || result != nil {
					t.Fatal("unknown/unattested admission gained private guest", kind)
				}
			}
			if kind != "generic-invite-unattested" {
				r := httptest.NewRequest("GET", "http://synthetic.test/api/watch-party/room/"+f.room.room.ID, nil)
				if actual, release, e := f.room.authority.AuthenticateGuestRequest(r, f.joined.Session.Token, f.room.room.ID); e == nil || actual != nil {
					if release != nil {
						release()
					}
					t.Fatal("replaced/unknown original guest still authenticated", kind)
				}
			}
		})
	}
}

func partyJoinPhysicalCleanup(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var gate *queryLifecycleGatedConn
	var first atomic.Bool
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if first.CompareAndSwap(false, true) {
			value := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			gate = value
			mu.Unlock()
			return value, nil
		}
		return conn, nil
	}
	f := newPartyDS(t, nil, dial, t.Context())
	mu.Lock()
	actual := gate
	mu.Unlock()
	if actual == nil {
		t.Fatal("actual original physical transport missing")
	}
	t.Cleanup(actual.release)
	conn, e := f.room.base.ds.pool.Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	physical := conn.Conn().PgConn()
	conn.Release()
	actual.armed.Store(true)
	done := make(chan struct{})
	var result *WatchPartyJoinResult
	var err error
	t.Cleanup(func() { actual.release(); bridgeSessionAwait(t, done) })
	go func() {
		defer close(done)
		result, err = f.room.authority.JoinInvitation(f.request(t.Context(), "POST"), f.invite.Token, false, "Synthetic held cleanup", "")
	}()
	bridgeSessionAwait(t, actual.closeEntered)
	f.room.authority.mu.Lock()
	charged := f.room.authority.active
	f.room.authority.mu.Unlock()
	if charged != 1 {
		t.Fatal("original join admission refunded before physical end")
	}
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical end bypassed genuine held Close")
	default:
	}
	select {
	case <-done:
		t.Fatal("original join returned before physical end")
	default:
	}
	actual.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, done)
	if err == nil || result != nil {
		t.Fatal("broken physical transport emitted guest cookie DATA", err)
	}
}
func TestWatchPartyJoinTransactions(t *testing.T) {
	for _, kind := range []string{"caller-cancel-before-body", "held-invite-caller-cancel", "held-invite-runtime-cancel", "source-absent-no-media", "known-rollback-no-partial-session", "unknown-ack-no-cookie", "late-known-ack-no-cookie", "physical-cleanup-joins", "writer-first", "healthy-reuse"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "physical-cleanup-joins" {
				partyJoinPhysicalCleanup(t)
				return
			}
			var enabled, dropped, armed atomic.Bool
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			defer release()
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, d pgx.TraceQueryEndData) {
				if kind == "late-known-ack-no-cookie" && q == "commit" && d.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &bridgeSessionDropACK{Conn: conn, enabled: &enabled, actual: &parentDropCommit{Conn: conn, dropped: &dropped}}, nil
			}
			var useDial func(context.Context, string, string) (net.Conn, error)
			if kind == "unknown-ack-no-cookie" {
				useDial = dial
			}
			life, retire := context.WithCancel(t.Context())
			defer retire()
			f := newPartyDS(t, trace, useDial, life)
			if kind == "source-absent-no-media" {
				f.join(t)
				frame := f.guestFrame(t, t.Context(), "POST")
				if grant, e := f.room.authority.GrantPartyPlayback(frame, f.room.room.ID); e == nil || grant != nil {
					t.Fatal("lobby guest acquired media before first source")
				}
				claimed := f.joined.Session
				claimed.Scope = models.SessionScopeStream
				claimed.ScopeResource, _ = partyEnvelope(f.room.room.ID, f.joined.GuestID, "synthetic:forged-source")
				raw := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, claimed)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				if owner, e := f.room.authority.CapturePartyPlaybackOwner(raw.WithContext(ctx), "target", "synthetic:forged-source"); e == nil || owner != nil {
					if owner != nil {
						owner.Close()
					}
					t.Fatal("guest/session DTO became original source authority")
				}
				var count int
				if e := f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE scope='stream'`).Scan(&count); e != nil || count != 0 {
					t.Fatal("source-absent stage minted media", count, e)
				}
				return
			}
			if kind == "healthy-reuse" {
				before, e := f.room.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				pid := before.Conn().PgConn().PID()
				before.Release()
				for range 2 {
					if view, e := f.room.authority.ResolveInvitation(f.request(t.Context(), "GET"), f.invite.Token, false); e != nil || view == nil {
						t.Fatal("healthy original lookup", e)
					}
				}
				f.join(t)
				after, e := f.room.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer after.Release()
				if after.Conn().PgConn().PID() != pid {
					t.Fatal("healthy completed join destroyed actual PG connection")
				}
				return
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if kind == "late-known-ack-no-cookie" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
			}
			if kind == "caller-cancel-before-body" {
				cancel()
			}
			if kind == "known-rollback-no-partial-session" {
				f.room.base.exec(t, `CREATE FUNCTION synthetic_party_join_rollback()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.scope='watch_party' THEN RAISE EXCEPTION 'synthetic rollback';END IF;RETURN NEW;END$$;CREATE TRIGGER synthetic_party_join_rollback BEFORE INSERT ON sessions FOR EACH ROW EXECUTE FUNCTION synthetic_party_join_rollback()`)
			}
			if kind == "writer-first" {
				writer, e := f.room.base.coordinator.TryWrite(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer writer.Close()
				result, e := f.room.authority.JoinInvitation(f.request(ctx, "POST"), f.invite.Token, false, "Synthetic writer first", "")
				if e == nil || result != nil {
					t.Fatal("join bypassed original exclusive native writer")
				}
				return
			}
			var holder pgx.Tx
			if strings.HasPrefix(kind, "held-invite-") {
				var e error
				holder, e = f.room.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { recordingStateRollback(holder) })
				if _, e = holder.Exec(t.Context(), `SELECT room_id FROM `+pgx.Identifier{f.room.base.schema, "watch_room_external_invites"}.Sanitize()+` WHERE room_id=$1 FOR UPDATE`, f.room.room.ID); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "unknown-ack-no-cookie" {
				enabled.Store(true)
			}
			armed.Store(true)
			done := make(chan struct{})
			var result *WatchPartyJoinResult
			var err error
			t.Cleanup(func() {
				cancel()
				retire()
				release()
				if holder != nil {
					recordingStateRollback(holder)
				}
				bridgeSessionAwait(t, done)
			})
			go func() {
				defer close(done)
				result, err = f.room.authority.JoinInvitation(f.request(ctx, "POST"), f.invite.Token, false, "Synthetic original transaction", "")
			}()
			if holder != nil {
				waiting := false
				until := time.Now().Add(5 * time.Second)
				for time.Now().Before(until) {
					if e := f.room.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%watch_room_invite_provenance%' AND query LIKE '%FOR UPDATE%')`, f.room.base.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("actual original invitation lock never waited")
				}
				if kind == "held-invite-caller-cancel" {
					cancel()
				} else {
					retire()
				}
			}
			if kind == "late-known-ack-no-cookie" {
				bridgeSessionAwait(t, entered)
				var count int
				if e := f.room.base.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.room.base.schema, "watch_party_guest_bindings"}.Sanitize()).Scan(&count); e != nil || count != 1 {
					t.Fatal("held actual ACK did not commit private guest", count, e)
				}
				bridgeSessionAwait(t, ctx.Done())
				release()
			}
			bridgeSessionAwait(t, done)
			enabled.Store(false)
			if result != nil || err == nil {
				t.Fatal("failed/late/unknown original join emitted cookie DATA", kind, err)
			}
			if holder != nil {
				recordingStateRollback(holder)
			}
			guests, sessions := f.guestCounts(t)
			if kind == "unknown-ack-no-cookie" {
				if !errors.Is(err, ErrWatchRoomUnknown) || !dropped.Load() || guests != 1 || sessions != 1 {
					t.Fatal("lost actual ACK retried or claimed rollback", err, guests, sessions)
				}
			} else if kind == "late-known-ack-no-cookie" {
				if guests != 1 || sessions != 1 || errors.Is(err, ErrWatchRoomUnknown) {
					t.Fatal("known late ACK outcome falsified", err, guests, sessions)
				}
			} else if guests != 0 || sessions != 0 {
				t.Fatal("failed join left partial member/session", kind, guests, sessions)
			}
		})
	}
}
