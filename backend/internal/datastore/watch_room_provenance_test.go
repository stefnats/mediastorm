//go:build linux

package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/internal/auth"
	"novastream/internal/settingscommit"
	"novastream/models"
)

type watchRoomFixture struct {
	base      *bridgeSessionFixture
	authority *WatchRoomAuthority
	room      *models.WatchRoom
}

func newWatchRoomFixture(t *testing.T, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), life context.Context) *watchRoomFixture {
	t.Helper()
	base, _ := shareProvenanceFixture(t, trace, dial, t.Context())
	authority, e := NewWatchRoomAuthority(life, base.ds, base.manager, base.security, base.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(authority.Close)
	return &watchRoomFixture{base: base, authority: authority}
}
func (f *watchRoomFixture) request(t *testing.T, method string, ctx context.Context) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.invalid/api/users/target/watch-rooms", nil).WithContext(ctx)
	r.Header.Set("X-Client-ID", "synthetic-share-client")
	framed, release, e := f.base.ds.AuthenticateProfilePinRequest(r, "synthetic-share-frame", "synthetic-share-client")
	if e != nil {
		t.Fatal("actual full room frame", e)
	}
	t.Cleanup(release)
	return framed
}
func watchRoomInput() models.WatchRoomCreate {
	return models.WatchRoomCreate{Title: "Synthetic native room", MediaType: "movie", ItemID: "synthetic-title", ClientID: "native-client", Params: map[string]string{"titleId": "synthetic-title"}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}}
}
func (f *watchRoomFixture) create(t *testing.T) {
	t.Helper()
	room, e := f.authority.CreateRoom(f.request(t, http.MethodPost, t.Context()), "target", watchRoomInput())
	if e != nil || room == nil {
		t.Fatal("actual original room creation", e)
	}
	f.room = room
}
func (f *watchRoomFixture) counts(t *testing.T) (rooms, provenance int) {
	t.Helper()
	if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_rooms),(SELECT count(*) FROM watch_room_provenance)`).Scan(&rooms, &provenance); e != nil {
		t.Fatal(e)
	}
	return
}
func TestWatchRoomOriginalCreation(t *testing.T) {
	for _, kind := range []string{"own-native", "ordinary-share-disabled", "master-own", "foreign-profile-denied", "capabilities-denied", "invitees-current-owned", "foreign-invitee-denied", "dto-frame", "foreign-frame", "copied-authority", "actor-profile-aba", "orphan-creator"} {
		t.Run(kind, func(t *testing.T) {
			var armed atomic.Bool
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, d pgx.TraceQueryEndData) {
				if kind == "actor-profile-aba" && d.Err == nil && strings.Contains(q, "FROM users WHERE id=$1") && !strings.Contains(q, "FOR UPDATE") && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			f := newWatchRoomFixture(t, trace, nil, t.Context())
			defer release()
			input := watchRoomInput()
			profile := "target"
			switch kind {
			case "ordinary-share-disabled":
				f.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
			case "master-own":
				f.base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
			case "foreign-profile-denied":
				f.base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('foreign-room-profile','other','Synthetic foreign')`)
				profile = "foreign-room-profile"
			case "capabilities-denied":
				input.Capabilities.NativePlayback = false
			case "invitees-current-owned":
				f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('own-room-profile','actor','Synthetic own participant')`)
				input.InviteeProfileIDs = []string{"own-room-profile", "own-room-profile", "missing-profile"}
			case "foreign-invitee-denied":
				f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('foreign-room-profile','other','Synthetic foreign')`)
				input.InviteeProfileIDs = []string{"foreign-room-profile"}
			}
			r := f.request(t, http.MethodPost, t.Context())
			authority := f.authority
			switch kind {
			case "dto-frame":
				raw := httptest.NewRequest("POST", "http://synthetic.invalid/api/users/target/watch-rooms", nil)
				session, _ := auth.GetSession(r)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				r = raw.WithContext(ctx)
			case "foreign-frame":
				store, e := New(t.Context(), f.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer store.Close()
				var stop context.CancelFunc
				r, stop, e = store.AuthenticateProfilePinRequest(httptest.NewRequest("POST", "http://synthetic.invalid/api/users/target/watch-rooms", nil), "synthetic-share-frame", "synthetic-share-client")
				if e != nil {
					t.Fatal(e)
				}
				defer stop()
			case "copied-authority":
				authority = &WatchRoomAuthority{self: f.authority, store: f.base.ds, manager: f.base.manager, security: f.base.security, coordinator: f.base.coordinator, ctx: f.authority.ctx}
			case "orphan-creator":
				f.base.exec(t, `DELETE FROM accounts WHERE id='actor'`)
				var remaining int
				if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target'`).Scan(&remaining); e != nil || remaining != 1 {
					t.Fatal("actual012 orphan not retained", remaining, e)
				}
			case "actor-profile-aba":
				armed.Store(true)
				done := make(chan struct{})
				var room *models.WatchRoom
				var err error
				t.Cleanup(func() { release(); bridgeSessionAwait(t, done) })
				go func() { defer close(done); room, err = authority.CreateRoom(r, profile, input) }()
				bridgeSessionAwait(t, entered)
				shareCopyPrincipal(t, &sharePlaybackDSFixture{base: f.base}, "users", "target")
				release()
				bridgeSessionAwait(t, done)
				if err == nil || room != nil {
					t.Fatal("actual profile ABA after original discovery was adopted")
				}
				if a, b := f.counts(t); a != 0 || b != 0 {
					t.Fatal("ABA denial left room/provenance", a, b)
				}
				return
			}
			room, e := authority.CreateRoom(r, profile, input)
			allowed := kind == "own-native" || kind == "ordinary-share-disabled" || kind == "master-own" || kind == "invitees-current-owned"
			if !allowed {
				if e == nil || room != nil {
					t.Fatal("untrusted native room creation admitted", kind)
				}
				if a, b := f.counts(t); a != 0 || b != 0 {
					t.Fatal("denied creation left rows", a, b)
				}
				return
			}
			if e != nil || room == nil || room.CreatorProfileID != "target" || room.Status != models.WatchRoomStatusLobby || room.Revision != 0 || room.ExpiresAt.Sub(room.CreatedAt) != 24*time.Hour {
				t.Fatal("actual native room wire/lifetime changed", e)
			}
			if a, b := f.counts(t); a != 1 || b != 1 {
				t.Fatal("room/provenance were not committed together", a, b)
			}
			if kind == "invitees-current-owned" {
				if len(room.Members) != 2 {
					t.Fatal("native current owned invitation dedup/missing semantics changed")
				}
			} else if len(room.Members) != 1 || !room.Members[0].IsCreator || !room.Members[0].Joined {
				t.Fatal("native creator member missing")
			}
		})
	}
}

func TestWatchRoomPrivateProvenance(t *testing.T) {
	for _, kind := range []string{"new-generated-nonce", "generic-room-unattested", "legacy-room-no-adoption", "room-delete-recreate", "profile-delete-recreate", "account-delete-recreate", "source-owner-away-back", "insert-cannot-copy-nonce", "private-json-omitted", "row-security-unknown"} {
		t.Run(kind, func(t *testing.T) {
			f := newWatchRoomFixture(t, nil, nil, t.Context())
			f.create(t)
			var original string
			if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT room_incarnation::text FROM watch_rooms WHERE id=$1`, f.room.ID).Scan(&original); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "new-generated-nonce":
				var matches bool
				e := f.base.ds.pool.QueryRow(t.Context(), `SELECT p.room_incarnation=r.room_incarnation AND p.creator_profile_id=u.id AND p.creator_profile_incarnation=u.share_incarnation AND p.creator_account_id=a.id AND p.creator_account_incarnation=a.share_incarnation FROM watch_room_provenance p JOIN watch_rooms r ON r.id=p.room_id JOIN users u ON u.id=p.creator_profile_id JOIN accounts a ON a.id=p.creator_account_id WHERE r.id=$1`, f.room.ID).Scan(&matches)
				if e != nil || !matches || !shareCanonicalUUID(original) {
					t.Fatal("original room did not capture real principal/DB nonce", e)
				}
				return
			case "generic-room-unattested":
				copy := *f.room
				copy.ID = "synthetic-generic-room"
				copy.Params = map[string]string{"roomIncarnation": original, "creatorProfileId": "target", "creatorAccountId": "actor", "provenanceVersion": "1"}
				if e := f.base.ds.WatchRooms().Create(t.Context(), &copy, nil, "native-client", watchRoomInput().Capabilities); e != nil {
					t.Fatal(e)
				}
				f.room = &copy
			case "legacy-room-no-adoption":
				f.base.exec(t, `DELETE FROM watch_room_provenance WHERE room_id=$1`, f.room.ID)
			case "room-delete-recreate":
				f.base.exec(t, `DELETE FROM watch_rooms WHERE id=$1`, f.room.ID)
				if e := f.base.ds.WatchRooms().Create(t.Context(), f.room, nil, "native-client", watchRoomInput().Capabilities); e != nil {
					t.Fatal(e)
				}
				var current string
				if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT room_incarnation::text FROM watch_rooms WHERE id=$1`, f.room.ID).Scan(&current); e != nil || current == original {
					t.Fatal("recreated room adopted old original DB nonce", e)
				}
			case "profile-delete-recreate":
				shareCopyPrincipal(t, &sharePlaybackDSFixture{base: f.base}, "users", "target")
				if e := f.base.ds.WatchRooms().Create(t.Context(), f.room, nil, "native-client", watchRoomInput().Capabilities); e != nil {
					t.Fatal(e)
				}
			case "account-delete-recreate":
				shareCopyPrincipal(t, &sharePlaybackDSFixture{base: f.base}, "accounts", "actor")
				now := time.Now().UTC().Truncate(time.Microsecond)
				session := models.Session{Token: "synthetic-share-frame", AccountID: "actor", ClientID: "synthetic-share-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
				if e := f.base.ds.Sessions().Create(t.Context(), &session); e != nil {
					t.Fatal(e)
				}
				var preserved int
				if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target' AND account_id='actor'`).Scan(&preserved); e != nil || preserved != 1 {
					t.Fatal("actual012 orphan/reinsert fixture changed", preserved, e)
				}
			case "source-owner-away-back":
				f.base.exec(t, `UPDATE users SET account_id='other' WHERE id='target';UPDATE users SET account_id='actor' WHERE id='target'`)
			case "insert-cannot-copy-nonce":
				var current string
				e := f.base.ds.pool.QueryRow(t.Context(), `INSERT INTO watch_rooms(id,creator_profile_id,title,media_type,item_id,room_incarnation,expires_at)VALUES('synthetic-explicit-nonce','target','Synthetic copied nonce','movie','synthetic-item',$1::uuid,clock_timestamp()+interval '24 hours')RETURNING room_incarnation::text`, original).Scan(&current)
				if e != nil || current == original {
					t.Fatal("explicit INSERT copied original room nonce", e)
				}
				f.room.ID = "synthetic-explicit-nonce"
			case "private-json-omitted":
				actual, e := f.base.ds.WatchRooms().Get(t.Context(), f.room.ID)
				if e != nil {
					t.Fatal(e)
				}
				raw, e := json.Marshal(actual)
				if e != nil {
					t.Fatal(e)
				}
				for _, key := range []string{"room_incarnation", "roomIncarnation", "creator_profile_incarnation", "creator_account_incarnation", "admission_nonce", "source_nonce"} {
					if strings.Contains(string(raw), key) {
						t.Fatal("private original room witness exported", key)
					}
				}
				return
			case "row-security-unknown":
				f.base.exec(t, `ALTER TABLE users ENABLE ROW LEVEL SECURITY;ALTER TABLE users FORCE ROW LEVEL SECURITY;CREATE POLICY synthetic_room_visible ON users USING(true) WITH CHECK(true)`)
			}
			invite, e := f.authority.ReplaceExternalInvite(f.request(t, http.MethodPost, t.Context()), "target", f.room.ID)
			if e == nil || invite != nil {
				t.Fatal("unattested/replaced original room gained external invite", kind)
			}
			var invitations int
			if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_external_invites WHERE room_id=$1`, f.room.ID).Scan(&invitations); e != nil || invitations != 0 {
				t.Fatal("denied provenance left invitation", invitations, e)
			}
		})
	}
}

func watchRoomInviteCount(t *testing.T, f *watchRoomFixture) int {
	t.Helper()
	var count int
	if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_room_external_invites WHERE room_id=$1`, f.room.ID).Scan(&count); e != nil {
		t.Fatal(e)
	}
	return count
}
func TestWatchRoomInviteMutation(t *testing.T) {
	for _, kind := range []string{"new-original-invite", "rotate-new-secret", "rotate-preserves-guest", "revoke-preserves-guest", "share-flag-current", "creator-current-role", "expiry-room-account-min", "missing-old-provenance", "known-rollback", "invite-physical-replace"} {
		t.Run(kind, func(t *testing.T) {
			f := newWatchRoomFixture(t, nil, nil, t.Context())
			if kind == "expiry-room-account-min" {
				f.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond))
			}
			f.create(t)
			first, e := f.authority.ReplaceExternalInvite(f.request(t, "POST", t.Context()), "target", f.room.ID)
			if e != nil || first == nil {
				t.Fatal("original invite", e)
			}
			if kind == "new-original-invite" || kind == "expiry-room-account-min" {
				var exact bool
				e = f.base.ds.pool.QueryRow(t.Context(), `SELECT i.token_hash=p.token_hash AND i.short_code=p.short_code AND p.room_incarnation=r.room_incarnation AND p.physical_receipt=concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text) AND i.expires_at=r.expires_at FROM watch_room_external_invites i JOIN watch_room_invite_provenance p USING(room_id) JOIN watch_rooms r ON r.id=i.room_id WHERE i.room_id=$1`, f.room.ID).Scan(&exact)
				if e != nil || !exact || first.Token == "" || first.ExpiresAt.After(f.room.ExpiresAt) {
					t.Fatal("actual admission did not retain original room/expiry", e)
				}
				return
			}
			if kind == "rotate-preserves-guest" || kind == "revoke-preserves-guest" {
				if e = f.base.ds.WatchRooms().JoinExternalGuest(t.Context(), f.room.ID, "synthetic-persistent-guest", "Synthetic guest", "synthetic-guest-client", watchRoomInput().Capabilities, time.Now().UTC()); e != nil {
					t.Fatal(e)
				}
				now := time.Now().UTC()
				guest := models.Session{Token: "synthetic-existing-party-session", AccountID: "actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour), Scope: "stream", ScopeResource: "watch-party-playback:v1:synthetic-existing"}
				if e = f.base.ds.Sessions().Create(t.Context(), &guest); e != nil {
					t.Fatal(e)
				}
				var guestBefore, sessionBefore string
				if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT row_to_json(g)::text FROM watch_room_guest_members g WHERE room_id=$1`, f.room.ID).Scan(&guestBefore); e != nil {
					t.Fatal(e)
				}
				if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT row_to_json(s)::text FROM sessions s WHERE token=$1`, guest.Token).Scan(&sessionBefore); e != nil {
					t.Fatal(e)
				}
				if kind == "revoke-preserves-guest" {
					e = f.authority.RevokeExternalInvite(f.request(t, "DELETE", t.Context()), "target", f.room.ID)
				} else {
					_, e = f.authority.ReplaceExternalInvite(f.request(t, "POST", t.Context()), "target", f.room.ID)
				}
				if e != nil {
					t.Fatal(e)
				}
				var guestAfter, sessionAfter string
				if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT row_to_json(g)::text FROM watch_room_guest_members g WHERE room_id=$1`, f.room.ID).Scan(&guestAfter); e != nil {
					t.Fatal(e)
				}
				if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT row_to_json(s)::text FROM sessions s WHERE token=$1`, guest.Token).Scan(&sessionAfter); e != nil || guestBefore != guestAfter || sessionBefore != sessionAfter {
					t.Fatal("admission mutation changed existing guest/session identity", e)
				}
				return
			}
			switch kind {
			case "share-flag-current":
				f.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
			case "creator-current-role":
				f.base.exec(t, `UPDATE accounts SET is_master=NOT is_master WHERE id='actor'`)
			case "missing-old-provenance":
				f.base.exec(t, `DELETE FROM watch_room_provenance WHERE room_id=$1`, f.room.ID)
			case "known-rollback":
				f.base.exec(t, `CREATE FUNCTION synthetic_room_invite_rollback()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic rollback';END$$;CREATE TRIGGER synthetic_room_invite_rollback BEFORE UPDATE ON watch_room_external_invites FOR EACH ROW EXECUTE FUNCTION synthetic_room_invite_rollback()`)
			case "invite-physical-replace":
				// Rotation is an explicit new admission, so replacing only the old invite
				// must produce a fresh private nonce rather than adopt the old receipt.
				f.base.exec(t, `UPDATE watch_room_external_invites SET active=active WHERE room_id=$1`, f.room.ID)
			}
			var nonceBefore string
			if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT admission_nonce::text FROM watch_room_invite_provenance WHERE room_id=$1`, f.room.ID).Scan(&nonceBefore); e != nil {
				t.Fatal(e)
			}
			// Keep the original framed request before role change so the current receipt
			// cannot be replaced by an intentionally fresh request.
			if kind == "creator-current-role" {
				f.base.exec(t, `UPDATE accounts SET is_master=NOT is_master WHERE id='actor'`)
				r := f.request(t, "POST", t.Context())
				f.base.exec(t, `UPDATE accounts SET is_master=NOT is_master WHERE id='actor'`)
				_, e = f.authority.ReplaceExternalInvite(r, "target", f.room.ID)
			} else {
				_, e = f.authority.ReplaceExternalInvite(f.request(t, "POST", t.Context()), "target", f.room.ID)
			}
			allowed := kind == "rotate-new-secret" || kind == "invite-physical-replace"
			if !allowed {
				if e == nil {
					t.Fatal("stale/failed admission mutation succeeded", kind)
				}
				if watchRoomInviteCount(t, f) != 1 {
					t.Fatal("failed rotation lost old committed admission")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			var nonceAfter, tokenAfter string
			if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT p.admission_nonce::text,i.token_hash FROM watch_room_invite_provenance p JOIN watch_room_external_invites i USING(room_id) WHERE p.room_id=$1`, f.room.ID).Scan(&nonceAfter, &tokenAfter); e != nil || nonceAfter == nonceBefore || tokenAfter == first.TokenHash {
				t.Fatal("rotation reused original admission", e)
			}
		})
	}
}
func TestWatchRoomSourceBinding(t *testing.T) {
	for _, kind := range []string{"initial-same-tx", "exact-idempotence", "changed-resource-conflict", "changed-params-conflict", "changed-profile-conflict", "params-owned-copy", "room-current-creator", "source-flag-stale", "library-cache-not-authority", "generic-source-unattested", "source-physical-replace"} {
		t.Run(kind, func(t *testing.T) {
			f := newWatchRoomFixture(t, nil, nil, t.Context())
			f.create(t)
			resource := "synthetic:original-room-source"
			params := map[string]string{"profileId": "target", "titleId": "synthetic-title"}
			if kind == "library-cache-not-authority" {
				resource = "localmedia:room-item/room.wav"
				f.base.exec(t, `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('room-lib','Synthetic room','movie','/synthetic');INSERT INTO local_media_items(id,library_id,relative_path,file_path,library_type)VALUES('room-item','room-lib','room.wav','/synthetic/room.wav','movie');INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('room-lib','restricted');INSERT INTO media_library_profile_grants(library_id,profile_id)VALUES('room-lib','target')`)
			}
			if kind == "generic-source-unattested" {
				if yes, e := f.base.ds.WatchRooms().BindExternalSource(t.Context(), f.room.ID, "target", resource, params, time.Now().UTC()); e != nil || !yes {
					t.Fatal(e)
				}
			}
			first, e := f.authority.BindExternalSource(f.request(t, "POST", t.Context()), "target", f.room.ID, resource, params)
			if kind == "generic-source-unattested" {
				if e == nil || first != nil {
					t.Fatal("generic source was silently attested")
				}
				return
			}
			if e != nil || first == nil {
				t.Fatal("original source", e)
			}
			var nonce, physical string
			if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT p.source_nonce::text,p.physical_receipt FROM watch_room_source_provenance p WHERE room_id=$1`, f.room.ID).Scan(&nonce, &physical); e != nil {
				t.Fatal(e)
			}
			if kind == "initial-same-tx" {
				var exact bool
				e = f.base.ds.pool.QueryRow(t.Context(), `SELECT p.room_incarnation=r.room_incarnation AND p.source_profile_incarnation=u.share_incarnation AND p.source_account_incarnation=a.share_incarnation AND p.physical_receipt=concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text) FROM watch_room_source_provenance p JOIN watch_rooms r ON r.id=p.room_id JOIN users u ON u.id=p.source_profile_id JOIN accounts a ON a.id=p.source_account_id JOIN watch_room_external_sources s ON s.room_id=p.room_id WHERE p.room_id=$1`, f.room.ID).Scan(&exact)
				if e != nil || !exact || !first.PublicationUntil.After(time.Now()) {
					t.Fatal("source original provenance not committed atomically", e)
				}
				return
			}
			switch kind {
			case "changed-resource-conflict":
				resource = "synthetic:other-room-source"
			case "changed-params-conflict":
				params["titleId"] = "synthetic-other-title"
			case "changed-profile-conflict":
				params["profileId"] = "other"
			case "params-owned-copy":
				params["titleId"] = "caller-mutated"
				if first.Source.Params["titleId"] != "synthetic-title" {
					t.Fatal("returned source retained caller alias")
				}
				params["titleId"] = "synthetic-title"
			case "room-current-creator":
				f.base.exec(t, `INSERT INTO users(id,account_id,name)VALUES('other-profile','actor','Synthetic other creator')`)
				f.base.exec(t, `UPDATE watch_rooms SET creator_profile_id='other-profile' WHERE id=$1`, f.room.ID)
			case "source-flag-stale":
				f.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
			case "library-cache-not-authority":
				// The same already-allowed source must now consult actual SQL, not retain
				// its earlier grant as a cache/provenance permission.
				f.base.exec(t, `DELETE FROM media_library_profile_grants WHERE library_id='room-lib'`)
			case "source-physical-replace":
				f.base.exec(t, `UPDATE watch_room_external_sources SET resource=resource WHERE room_id=$1`, f.room.ID)
			}
			result, e := f.authority.BindExternalSource(f.request(t, "POST", t.Context()), "target", f.room.ID, resource, params)
			allowed := kind == "exact-idempotence" || kind == "params-owned-copy"
			if kind == "source-flag-stale" {
				f.base.exec(t, `UPDATE users SET allow_share_links=true,pin_hash='',pin_length=4 WHERE id='target'`)
				if unknown, err := f.authority.BindExternalSource(f.request(t, "POST", t.Context()), "target", f.room.ID, resource, params); err == nil || unknown != nil {
					t.Fatal("inconsistent empty-hash PIN shape authorized source")
				}
				if invitation, err := f.authority.ReplaceExternalInvite(f.request(t, "POST", t.Context()), "target", f.room.ID); err == nil || invitation != nil {
					t.Fatal("inconsistent empty-hash PIN shape authorized invite")
				}
				f.base.exec(t, `UPDATE users SET pin_hash='synthetic-protected-pin',pin_length=4 WHERE id='target'`)
				if native, err := f.authority.CreateRoom(f.request(t, "POST", t.Context()), "target", watchRoomInput()); err != nil || native == nil {
					t.Fatal("protected native room incorrectly required external consent", err)
				}
			}
			if !allowed {
				if e == nil || result != nil {
					t.Fatal("changed/stale source accepted", kind)
				}
				return
			}
			var after, afterPhysical string
			if e != nil || result == nil || !result.Source.BoundAt.Equal(first.Source.BoundAt) {
				t.Fatal("genuine exact idempotence changed source", e)
			}
			if e = f.base.ds.pool.QueryRow(t.Context(), `SELECT source_nonce::text,physical_receipt FROM watch_room_source_provenance WHERE room_id=$1`, f.room.ID).Scan(&after, &afterPhysical); e != nil || after != nonce || afterPhysical != physical {
				t.Fatal("idempotence reminted source provenance", e)
			}
		})
	}
}

func watchRoomPhysicalCleanup(t *testing.T) {
	var mu sync.Mutex
	var gate *queryLifecycleGatedConn
	var first atomic.Bool
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		v, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if first.CompareAndSwap(false, true) {
			g := &queryLifecycleGatedConn{Conn: v, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			gate = g
			mu.Unlock()
			return g, nil
		}
		return v, nil
	}
	f := newWatchRoomFixture(t, nil, dial, t.Context())
	f.create(t)
	r := f.request(t, "POST", t.Context())
	mu.Lock()
	actual := gate
	mu.Unlock()
	if actual == nil {
		t.Fatal("genuine physical transport absent")
	}
	t.Cleanup(actual.release)
	conn, e := f.base.ds.pool.Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	physical := conn.Conn().PgConn()
	conn.Release()
	actual.armed.Store(true)
	done := make(chan struct{})
	var invite *models.WatchRoomExternalInvite
	var err error
	t.Cleanup(func() { actual.release(); bridgeSessionAwait(t, done) })
	go func() { defer close(done); invite, err = f.authority.ReplaceExternalInvite(r, "target", f.room.ID) }()
	bridgeSessionAwait(t, actual.closeEntered)
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical cleanup bypassed actual Close")
	default:
	}
	f.authority.mu.Lock()
	charged := f.authority.actors["actor"]
	f.authority.mu.Unlock()
	if charged != 1 {
		t.Fatal("room admission refunded before physical cleanup")
	}
	hold, e := f.base.coordinator.TryWrite(t.Context())
	if hold != nil {
		_ = hold.Close()
	}
	if !errors.Is(e, settingscommit.ErrBusy) {
		t.Fatal("native hold ended before physical cleanup", e)
	}
	select {
	case <-done:
		t.Fatal("room returned before actual physical end")
	default:
	}
	actual.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, done)
	if err == nil || invite != nil || !errors.Is(err, ErrWatchRoomUnavailable) {
		t.Fatal("broken physical cleanup published admission", err)
	}
}
func TestWatchRoomOriginalLifetime(t *testing.T) {
	for _, kind := range []string{"cancel-before-query", "held-room-caller-cancel", "held-room-runtime-cancel", "writer-first", "actor-source-expiry", "native-policy-retired", "unknown-ack-no-token", "late-known-ack", "physical-cleanup-joins", "healthy-connection-reuse", "close-joins-sql", "after-close-denied"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "physical-cleanup-joins" {
				watchRoomPhysicalCleanup(t)
				return
			}
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			defer release()
			var armed, enabled, dropped atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, d pgx.TraceQueryEndData) {
				selected := (kind == "late-known-ack" && q == "commit") || (kind == "native-policy-retired" && strings.Contains(q, "FROM users WHERE id=$1") && !strings.Contains(q, "FOR UPDATE"))
				if selected && d.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				v, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &bridgeSessionDropACK{Conn: v, enabled: &enabled, actual: &parentDropCommit{Conn: v, dropped: &dropped}}, nil
			}
			var useDial func(context.Context, string, string) (net.Conn, error)
			if kind == "unknown-ack-no-token" {
				useDial = dial
			}
			life, retire := context.WithCancel(t.Context())
			defer retire()
			f := newWatchRoomFixture(t, trace, useDial, life)
			if kind == "actor-source-expiry" {
				f.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(3*time.Second).Truncate(time.Microsecond))
			}
			f.create(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if kind == "late-known-ack" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
			}
			r := f.request(t, "POST", ctx)
			if kind == "healthy-connection-reuse" {
				c, e := f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				pid := c.Conn().PgConn().PID()
				c.Release()
				for range 2 {
					if invite, e := f.authority.ReplaceExternalInvite(r, "target", f.room.ID); e != nil || invite == nil {
						t.Fatal("healthy actual room operation", e)
					}
				}
				c, e = f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer c.Release()
				if c.Conn().PgConn().PID() != pid {
					t.Fatal("healthy actual connection unnecessarily destroyed")
				}
				return
			}
			if kind == "cancel-before-query" {
				cancel()
			}
			if kind == "after-close-denied" {
				f.authority.Close()
			}
			var writer *settingscommit.WriteGuard
			if kind == "writer-first" {
				var e error
				writer, e = f.base.coordinator.TryWrite(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer writer.Close()
			}
			var holder pgx.Tx
			if strings.HasPrefix(kind, "held-room-") || kind == "close-joins-sql" || kind == "actor-source-expiry" {
				var e error
				holder, e = f.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { recordingStateRollback(holder) })
				if _, e = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.base.schema, "watch_rooms"}.Sanitize()+` WHERE id=$1 FOR UPDATE`, f.room.ID); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "unknown-ack-no-token" {
				enabled.Store(true)
			}
			armed.Store(true)
			done := make(chan struct{})
			var invite *models.WatchRoomExternalInvite
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
			go func() { defer close(done); invite, err = f.authority.ReplaceExternalInvite(r, "target", f.room.ID) }()
			if holder != nil {
				waiting := false
				until := time.Now().Add(5 * time.Second)
				for time.Now().Before(until) {
					if e := f.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%watch_room_provenance%' AND query LIKE '%FOR UPDATE%')`, f.base.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("actual original room lock did not wait")
				}
				if kind == "held-room-caller-cancel" {
					cancel()
				} else if kind == "held-room-runtime-cancel" {
					retire()
				} else if kind == "actor-source-expiry" {
					bridgeSessionAwait(t, r.Context().Done())
				} else {
					closed := make(chan struct{})
					t.Cleanup(func() { recordingStateRollback(holder); bridgeSessionAwait(t, closed) })
					go func() { defer close(closed); f.authority.Close() }()
					bridgeSessionAwait(t, f.authority.Context().Done())
					bridgeSessionAwait(t, closed)
				}
			}
			if kind == "native-policy-retired" {
				bridgeSessionAwait(t, entered)
				path := f.base.manager.ConfigPath()
				raw, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(path, path+".room-original"); e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					if e := os.Rename(path+".room-original", path); e != nil {
						t.Error(e)
					}
				})
				if e = os.WriteFile(path, raw, 0600); e != nil {
					t.Fatal(e)
				}
				release()
			}
			if kind == "late-known-ack" {
				bridgeSessionAwait(t, entered)
				if watchRoomInviteCount(t, f) != 1 {
					t.Fatal("held real ACK not durable")
				}
				bridgeSessionAwait(t, ctx.Done())
				release()
			}
			bridgeSessionAwait(t, done)
			if err == nil || invite != nil {
				t.Fatal("late/retired/unavailable work emitted admission", kind, err)
			}
			if holder != nil {
				recordingStateRollback(holder)
			}
			count := watchRoomInviteCount(t, f)
			if kind == "unknown-ack-no-token" {
				if !errors.Is(err, ErrWatchRoomUnknown) || !dropped.Load() || count != 1 {
					t.Fatal("lost actual ACK retried/claimed rollback", err, count)
				}
			} else if kind == "late-known-ack" {
				if count != 1 || errors.Is(err, ErrWatchRoomUnknown) {
					t.Fatal("known late ACK lost outcome", err, count)
				}
			} else if count != 0 {
				t.Fatal("failed room operation committed admission", kind, count)
			}
		})
	}
}
