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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/internal/auth"
	"novastream/models"
)

type partyPlaybackDSFixture struct {
	f     *partyDSFixture
	grant *WatchPartyPlaybackGrant
	raw   *http.Request
}

func partyRecreateAdmission(t *testing.T, f *partyDSFixture) {
	t.Helper()
	f.room.create(t)
	invite, e := f.room.authority.ReplaceExternalInvite(f.room.request(t, "POST", t.Context()), "target", f.room.room.ID)
	if e != nil {
		t.Fatal(e)
	}
	f.invite = invite
}
func newPartyPlaybackDS(t *testing.T, kind string, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), life context.Context) *partyPlaybackDSFixture {
	t.Helper()
	f := newPartyDS(t, trace, dial, life)
	if kind == "room-expired" || kind == "grant-expiry-min" || kind == "no-renewal-after-expiry" {
		duration := time.Hour
		if kind != "grant-expiry-min" {
			duration = 5 * time.Second
		}
		f.room.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(duration).Truncate(time.Microsecond))
		partyRecreateAdmission(t, f)
	}
	f.join(t)
	if kind == "library-cached-allow-revoked" {
		f.room.base.exec(t, `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('party-lib','Synthetic party library','movie','/synthetic');INSERT INTO local_media_items(id,library_id,relative_path,file_path,library_type)VALUES('party-item','party-lib','party.wav','/synthetic/party.wav','movie');INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('party-lib','restricted');INSERT INTO media_library_profile_grants(library_id,profile_id)VALUES('party-lib','target')`)
		if result, e := f.room.authority.BindExternalSource(f.room.request(t, "POST", t.Context()), "target", f.room.room.ID, "localmedia:party-item/party.wav", map[string]string{"profileId": "target", "titleId": "synthetic-party"}); e != nil || result == nil {
			t.Fatal(e)
		}
		f.room.base.exec(t, `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, f.room.room.ID)
	} else {
		f.bind(t)
	}
	grant, e := f.room.authority.GrantPartyPlayback(f.guestFrame(t, t.Context(), "POST"), f.room.room.ID)
	if e != nil || grant == nil {
		t.Fatal("actual original party source grant", e)
	}
	raw, release, e := f.room.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), grant.Session.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	return &partyPlaybackDSFixture{f, grant, raw}
}
func (f *partyPlaybackDSFixture) owner(t *testing.T) *WatchPartyPlaybackOwner {
	t.Helper()
	owner, e := f.f.room.authority.CapturePartyPlaybackOwner(f.raw, "target", f.grant.Params["sourcePath"])
	// The existing public params intentionally do not contain sourcePath. The
	// original private envelope supplies the actual fixed resource to the core.
	if e != nil || owner == nil {
		t.Fatal("actual original party owner", e)
	}
	t.Cleanup(owner.Close)
	return owner
}
func (f *partyPlaybackDSFixture) streamCount(t *testing.T) int {
	t.Helper()
	var n int
	if e := f.f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE scope='stream'`).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestWatchPartyPlaybackGrant(t *testing.T) {
	for _, kind := range []string{"genuine-guest-first-source", "lobby-source-missing", "room-not-playing", "original-resource-params-profile", "same-tx-restricted-mint", "generic-guest-no-private-binding", "forged-guest-session-dto", "foreign-store-frame", "copied-authority", "source-delete-recreate", "source-physical-replace", "grant-expiry-min"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "lobby-source-missing" {
				f := newPartyDS(t, nil, nil, t.Context())
				f.join(t)
				if result, e := f.room.authority.GrantPartyPlayback(f.guestFrame(t, t.Context(), "POST"), f.room.room.ID); e == nil || result != nil {
					t.Fatal("lobby acquired media without source")
				}
				return
			}
			f := newPartyDS(t, nil, nil, t.Context())
			if kind == "grant-expiry-min" {
				f.room.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond))
				partyRecreateAdmission(t, f)
			}
			f.join(t)
			f.bind(t)
			r := f.guestFrame(t, t.Context(), "POST")
			authority := f.room.authority
			switch kind {
			case "room-not-playing":
				f.room.base.exec(t, `UPDATE watch_rooms SET status='lobby' WHERE id=$1`, f.room.room.ID)
			case "generic-guest-no-private-binding":
				f.room.base.exec(t, `DELETE FROM watch_party_guest_bindings WHERE session_token=$1`, f.joined.Session.Token)
			case "forged-guest-session-dto":
				raw := httptest.NewRequest("POST", "http://synthetic.test/api/watch-party/room/"+f.room.room.ID+"/playback-grant", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, f.joined.Session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				r = raw.WithContext(ctx)
			case "foreign-store-frame":
				store, e := New(t.Context(), f.room.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer store.Close()
				var release context.CancelFunc
				r, release, e = store.AuthenticateProfilePinRequest(httptest.NewRequest("POST", "http://synthetic.test/api/watch-party/room/"+f.room.room.ID+"/playback-grant", nil), f.joined.Session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
			case "copied-authority":
				authority = &WatchRoomAuthority{self: f.room.authority, store: f.room.base.ds, manager: f.room.base.manager, security: f.room.base.security, coordinator: f.room.base.coordinator, ctx: f.room.authority.ctx}
			case "source-delete-recreate":
				f.room.base.exec(t, `DELETE FROM watch_room_external_sources WHERE room_id=$1`, f.room.room.ID)
				if yes, e := f.room.base.ds.WatchRooms().BindExternalSource(t.Context(), f.room.room.ID, "target", "synthetic:original-party.wav", map[string]string{"profileId": "target", "titleId": "synthetic-party"}, time.Now().UTC()); e != nil || !yes {
					t.Fatal(e)
				}
			case "source-physical-replace":
				f.room.base.exec(t, `UPDATE watch_room_external_sources SET resource=resource WHERE room_id=$1`, f.room.room.ID)
			}
			grant, e := authority.GrantPartyPlayback(r, f.room.room.ID)
			allowed := kind == "genuine-guest-first-source" || kind == "original-resource-params-profile" || kind == "same-tx-restricted-mint" || kind == "grant-expiry-min"
			if !allowed {
				if e == nil || grant != nil {
					t.Fatal("untrusted/stale guest/source minted playback", kind)
				}
				var n int
				if e = f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE scope='stream'`).Scan(&n); e != nil || n != 0 {
					t.Fatal("denied source left stream", n, e)
				}
				return
			}
			if e != nil || grant == nil || grant.Session.IsMaster || grant.Session.Scope != models.SessionScopeStream || grant.Session.AccountID != "actor" || !partyCanonicalToken(grant.Session.Token) {
				t.Fatal("actual restricted party grant", e)
			}
			if kind == "grant-expiry-min" && !grant.Session.ExpiresAt.Equal(f.joined.Session.ExpiresAt) {
				t.Fatal("grant escaped original guest/account/room min")
			}
			if kind == "original-resource-params-profile" {
				grant.Params["profileId"] = "other"
				var original string
				if e = f.room.base.ds.pool.QueryRow(t.Context(), `SELECT params->>'profileId' FROM watch_room_external_sources WHERE room_id=$1`, f.room.room.ID).Scan(&original); e != nil || original != "target" {
					t.Fatal("copied publication Params mutated original source", e)
				}
			}
			if kind == "same-tx-restricted-mint" {
				var matched bool
				e = f.room.base.ds.pool.QueryRow(t.Context(), `SELECT b.binding_id=s.watch_party_playback_binding_id AND b.guest_binding_id=g.binding_id AND b.source_nonce=p.source_nonce AND b.source_profile_id=p.source_profile_id AND b.source_account_id=p.source_account_id AND b.params_sha256=p.params_sha256 AND NOT s.is_master FROM sessions s JOIN watch_party_playback_bindings b ON b.session_token=s.token JOIN watch_party_guest_bindings g ON g.binding_id=b.guest_binding_id JOIN watch_room_source_provenance p ON p.room_id=b.room_id WHERE s.token=$1`, grant.Session.Token).Scan(&matched)
				if e != nil || !matched {
					t.Fatal("stream/private original source not sameTX", e)
				}
				again, e := authority.GrantPartyPlayback(r, f.room.room.ID)
				if e != nil || again == nil || again.Session.Token != grant.Session.Token || !again.Session.ExpiresAt.Equal(grant.Session.ExpiresAt) {
					t.Fatal("repeated matching grant grew/renewed session", e)
				}
				f.room.base.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES('target','{"playback":{"preferredAudioLanguage":"fr"}}') ON CONFLICT(user_id)DO UPDATE SET settings=EXCLUDED.settings`)
				fresh, e := authority.GrantPartyPlayback(r, f.room.room.ID)
				if e != nil || fresh == nil || fresh.Session.Token == grant.Session.Token {
					t.Fatal("fresh explicit changed-settings grant unavailable", e)
				}
				f.room.base.exec(t, `UPDATE user_settings SET settings='{"playback":{"preferredAudioLanguage":"es"}}' WHERE user_id='target'`)
				if third, e := authority.GrantPartyPlayback(r, f.room.room.ID); !errors.Is(e, ErrWatchRoomBusy) || third != nil {
					t.Fatal("unexpired original guest grant budget grew beyond2", e)
				}
				var n int
				if e = f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_party_playback_bindings`).Scan(&n); e != nil || n != 2 {
					t.Fatal("grant cap actual rows", n, e)
				}
			}
		})
	}
}
func TestWatchPartyPlaybackOriginalOwner(t *testing.T) {
	for _, kind := range []string{"original-raw-frame", "resolved-frame-same-original", "dto-frame", "foreign-frame", "copied-owner", "plain-lookalike-envelope", "missing-playback-nonce", "wrong-private-nonce", "resource-substitution", "profile-substitution", "other-recipient", "one-shot-handoff", "setup-retired-after-handoff", "repeated-concurrent-close"} {
		t.Run(kind, func(t *testing.T) {
			f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
			r := f.raw
			profile, resource := "target", "synthetic:original-party.wav"
			switch kind {
			case "resolved-frame-same-original":
				var e error
				r, e = f.f.room.authority.ResolveOriginalPartyRequest(r)
				if e != nil {
					t.Fatal(e)
				}
			case "dto-frame":
				raw := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, f.grant.Session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				r = raw.WithContext(ctx)
			case "foreign-frame":
				store, e := New(t.Context(), f.f.room.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer store.Close()
				var release context.CancelFunc
				r, release, e = store.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), f.grant.Session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
			case "plain-lookalike-envelope":
				copied := f.grant.Session
				var e error
				copied.Token, e = partyNewToken()
				if e != nil {
					t.Fatal(e)
				}
				if e = f.f.room.base.ds.Sessions().Create(t.Context(), &copied); e != nil {
					t.Fatal(e)
				}
				var release context.CancelFunc
				r, release, e = f.f.room.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), copied.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
			case "missing-playback-nonce":
				f.f.room.base.exec(t, `DELETE FROM watch_party_playback_bindings WHERE session_token=$1`, f.grant.Session.Token)
			case "wrong-private-nonce":
				if _, e := f.f.room.base.ds.pool.Exec(t.Context(), `UPDATE sessions SET watch_party_playback_binding_id=$2::uuid WHERE token=$1`, f.grant.Session.Token, uuid.NewString()); e == nil {
					t.Fatal("actual composite nonce FK allowed copied binding")
				}
			case "resource-substitution":
				resource = "synthetic:substituted-party.wav"
			case "profile-substitution":
				profile = "other"
			}
			owner, e := f.f.room.authority.CapturePartyPlaybackOwner(r, profile, resource)
			denied := kind == "dto-frame" || kind == "foreign-frame" || kind == "plain-lookalike-envelope" || kind == "missing-playback-nonce" || kind == "resource-substitution" || kind == "profile-substitution"
			if denied {
				if e == nil || owner != nil {
					if owner != nil {
						owner.Close()
					}
					t.Fatal("untrusted model/frame/source became original owner", kind)
				}
				return
			}
			if e != nil || owner == nil {
				t.Fatal("actual original party owner", e)
			}
			t.Cleanup(owner.Close)
			if kind == "copied-owner" {
				copy := &WatchPartyPlaybackOwner{self: owner, runtime: owner.runtime, ctx: owner.ctx}
				if copy.Check(t.Context()) == nil {
					t.Fatal("opaque owner copy certified source")
				}
				if owner.Check(t.Context()) != nil {
					t.Fatal("copy denial retired genuine original")
				}
			}
			if kind == "other-recipient" {
				other, e := f.f.room.authority.JoinInvitation(f.f.request(t.Context(), "POST"), f.f.invite.Token, false, "Synthetic other recipient", "")
				if e != nil {
					t.Fatal(e)
				}
				original := f.f.joined
				f.f.joined = other
				otherFrame := f.f.guestFrame(t, t.Context(), "POST")
				f.f.joined = original
				grant, e := f.f.room.authority.GrantPartyPlayback(otherFrame, f.f.room.room.ID)
				if e != nil {
					t.Fatal(e)
				}
				actual, release, e := f.f.room.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), grant.Session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				if owner.CheckRequest(actual, "target", resource) == nil {
					t.Fatal("different recipient adopted original actor")
				}
				if owner.Check(t.Context()) != nil {
					t.Fatal("foreign request retired valid original")
				}
			}
			if kind == "one-shot-handoff" || kind == "setup-retired-after-handoff" || kind == "repeated-concurrent-close" {
				lease, e := owner.Handoff(r)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(lease.Close)
				if repeated, e := owner.Handoff(r); e == nil || repeated != nil {
					if repeated != nil {
						repeated.Close()
					}
					t.Fatal("original source handoff renewed")
				}
				if kind == "setup-retired-after-handoff" {
					frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
					frameContext := frame.ctx
					// A real authentication release ends that original request context. The
					// retained genuine source/policy must remain its own fixed lifetime.
					if deadline, ok := frameContext.Deadline(); !ok || deadline.Before(time.Now()) {
						t.Fatal("original real frame lifetime absent")
					}
					// Use a fresh genuinely authenticated caller with a controllable original
					// context for the same still-original token, not a cloned frame.
					ctx, cancel := context.WithCancel(t.Context())
					actual, release, e := f.f.room.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil).WithContext(ctx), f.grant.Session.Token, "")
					if e != nil {
						t.Fatal(e)
					}
					next, e := f.f.room.authority.CapturePartyPlaybackOwner(actual, "target", resource)
					if e != nil {
						release()
						cancel()
						t.Fatal(e)
					}
					t.Cleanup(next.Close)
					held, e := next.Handoff(actual)
					if e != nil {
						release()
						cancel()
						t.Fatal(e)
					}
					t.Cleanup(held.Close)
					release()
					cancel()
					if next.Context().Err() != nil || next.Check(t.Context()) != nil {
						t.Fatal("healthy setup retirement killed fixed original source policy")
					}
					held.Close()
				}
				if kind == "repeated-concurrent-close" {
					a, b := make(chan struct{}), make(chan struct{})
					t.Cleanup(func() { lease.Close(); bridgeSessionAwait(t, a); bridgeSessionAwait(t, b) })
					go func() { defer close(a); owner.Close() }()
					go func() { defer close(b); owner.Close() }()
					bridgeSessionAwait(t, owner.Context().Done())
					select {
					case <-a:
						t.Fatal("source close passed still-held actual lease")
					default:
					}
					select {
					case <-b:
						t.Fatal("concurrent close bypassed physical source lease")
					default:
					}
					owner.runtime.mu.Lock()
					_, charged := owner.runtime.partyOwners[owner]
					owner.runtime.mu.Unlock()
					if !charged {
						t.Fatal("source admission refunded before lease end")
					}
					lease.Close()
					bridgeSessionAwait(t, a)
					bridgeSessionAwait(t, b)
				}
				lease.Close()
			}
		})
	}
}

func TestWatchPartyPlaybackSourceLifetime(t *testing.T) {
	for _, kind := range []string{"room-ended", "room-expired", "guest-left", "guest-session-revoked", "creator-account-orphan", "creator-role-aba", "creator-pin-current", "creator-share-current", "household-child-current", "native-policy-retired", "library-cached-allow-revoked", "source-params-replaced"} {
		t.Run(kind, func(t *testing.T) {
			f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
			owner := f.owner(t)
			if e := owner.Check(t.Context()); e != nil {
				t.Fatal("original source did not start current", e)
			}
			switch kind {
			case "room-ended":
				f.f.room.base.exec(t, `UPDATE watch_rooms SET status='ended' WHERE id=$1`, f.f.room.room.ID)
			case "room-expired":
				bridgeSessionAwait(t, owner.Context().Done())
			case "guest-left":
				if e := f.f.room.base.ds.WatchRooms().LeaveExternalGuest(t.Context(), f.f.room.room.ID, f.f.joined.GuestID); e != nil {
					t.Fatal(e)
				}
			case "guest-session-revoked":
				f.f.room.base.exec(t, `DELETE FROM sessions WHERE token=$1`, f.f.joined.Session.Token)
			case "creator-account-orphan":
				f.f.room.base.exec(t, `DELETE FROM accounts WHERE id='actor'`)
				var retained int
				if e := f.f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target'`).Scan(&retained); e != nil || retained != 1 {
					t.Fatal("actual012 orphan fixture changed", retained, e)
				}
			case "creator-role-aba":
				f.f.room.base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';UPDATE accounts SET is_master=false WHERE id='actor'`)
			case "creator-pin-current":
				f.f.room.base.exec(t, `UPDATE users SET pin_hash='synthetic-protected',pin_length=4 WHERE id='target'`)
			case "creator-share-current":
				f.f.room.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target';UPDATE users SET allow_share_links=true WHERE id='target'`)
			case "household-child-current":
				f.f.room.base.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('synthetic-party-child','actor','Synthetic child',true)`)
			case "native-policy-retired":
				if e := f.f.room.base.manager.Save(f.f.room.base.settings); e != nil {
					t.Fatal(e)
				}
			case "library-cached-allow-revoked":
				if owner.snapshot.source.library == "" {
					t.Fatal("actual initial SQL allow receipt absent")
				}
				f.f.room.base.exec(t, `DELETE FROM media_library_profile_grants WHERE library_id='party-lib'`)
			case "source-params-replaced":
				f.f.room.base.exec(t, `UPDATE watch_room_external_sources SET params=jsonb_set(params,'{titleId}','"synthetic-replaced"') WHERE room_id=$1`, f.f.room.room.ID)
			}
			if owner.Check(t.Context()) == nil {
				t.Fatal("retired current source stayed authorized", kind)
			}
			if owner.Context().Err() == nil {
				t.Fatal("actual source denial did not retire fixed original actor", kind)
			}
			if kind == "household-child-current" {
				f.f.room.base.exec(t, `DELETE FROM users WHERE id='synthetic-party-child'`)
				if owner.Check(t.Context()) == nil {
					t.Fatal("restored household renewed retired original actor")
				}
			}
		})
	}
}
func partyPlaybackPhysicalCleanup(t *testing.T) {
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
	f := newPartyPlaybackDS(t, "physical-cleanup-before-refund", nil, dial, t.Context())
	owner := f.owner(t)
	mu.Lock()
	actual := gate
	mu.Unlock()
	if actual == nil {
		t.Fatal("actual source PG transport missing")
	}
	t.Cleanup(actual.release)
	conn, e := f.f.room.base.ds.pool.Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	physical := conn.Conn().PgConn()
	conn.Release()
	actual.armed.Store(true)
	done := make(chan struct{})
	var err error
	t.Cleanup(func() { actual.release(); bridgeSessionAwait(t, done) })
	go func() { defer close(done); err = owner.Check(t.Context()) }()
	bridgeSessionAwait(t, actual.closeEntered)
	owner.runtime.mu.Lock()
	_, charged := owner.runtime.partyOwners[owner]
	owner.runtime.mu.Unlock()
	if !charged {
		t.Fatal("source owner admission refunded before physical end")
	}
	select {
	case <-physical.CleanupDone():
		t.Fatal("source SQL cleanup bypassed actual held transport")
	default:
	}
	select {
	case <-done:
		t.Fatal("source check returned before physical end")
	default:
	}
	actual.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, done)
	if err == nil {
		t.Fatal("broken source transport certified original source")
	}
}
func TestWatchPartyPlaybackTransactionLifetime(t *testing.T) {
	for _, kind := range []string{"held-check-caller-cancel", "held-check-runtime-close", "grant-known-rollback", "grant-unknown-ack", "late-known-ack", "physical-cleanup-before-refund", "startup-close-held-source", "no-renewal-after-expiry"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "physical-cleanup-before-refund" {
				partyPlaybackPhysicalCleanup(t)
				return
			}
			if kind == "no-renewal-after-expiry" {
				f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
				owner := f.owner(t)
				before := owner.ExpiresAt()
				bridgeSessionAwait(t, owner.Context().Done())
				if owner.Check(t.Context()) == nil || !owner.ExpiresAt().Equal(before) {
					t.Fatal("expired actor renewed original authority")
				}
				return
			}
			if kind == "startup-close-held-source" {
				f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
				owner := f.owner(t)
				lease, e := owner.Handoff(f.raw)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(lease.Close)
				ended, proceed := make(chan struct{}), make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(proceed) }) }
				worker := make(chan struct{})
				t.Cleanup(func() { release(); bridgeSessionAwait(t, worker); lease.Close(); bridgeSessionAwait(t, ended) })
				go func() { defer close(worker); <-proceed }()
				go func() { defer close(ended); f.f.room.authority.Close() }()
				bridgeSessionAwait(t, owner.Context().Done())
				select {
				case <-ended:
					t.Fatal("runtime source owner returned before actual held consumer lease")
				default:
				}
				release()
				bridgeSessionAwait(t, worker)
				lease.Close()
				bridgeSessionAwait(t, ended)
				return
			}
			if strings.HasPrefix(kind, "held-check-") {
				f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
				owner := f.owner(t)
				holder, e := f.f.room.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { recordingStateRollback(holder) })
				if _, e = holder.Exec(t.Context(), `SELECT token FROM `+pgx.Identifier{f.f.room.base.schema, "sessions"}.Sanitize()+` WHERE token=$1 FOR UPDATE`, f.grant.Session.Token); e != nil {
					t.Fatal(e)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan struct{})
				var checkErr error
				t.Cleanup(func() { cancel(); recordingStateRollback(holder); bridgeSessionAwait(t, done) })
				go func() { defer close(done); checkErr = owner.Check(ctx) }()
				waiting := false
				until := time.Now().Add(5 * time.Second)
				for time.Now().Before(until) {
					if e = f.f.room.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%watch_party_playback_bindings%' AND query LIKE '%FOR UPDATE%')`, f.f.room.base.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("actual original stream/private binding lock never waited")
				}
				if kind == "held-check-caller-cancel" {
					cancel()
				} else {
					closed := make(chan struct{})
					t.Cleanup(func() { recordingStateRollback(holder); bridgeSessionAwait(t, closed) })
					go func() { defer close(closed); f.f.room.authority.Close() }()
					bridgeSessionAwait(t, owner.Context().Done())
					bridgeSessionAwait(t, closed)
				}
				bridgeSessionAwait(t, done)
				recordingStateRollback(holder)
				if checkErr == nil {
					t.Fatal("retired held original check passed")
				}
				if kind == "held-check-caller-cancel" && owner.Check(t.Context()) != nil {
					t.Fatal("one cancelled caller retired otherwise current original source")
				}
				return
			}
			var enabled, dropped, armed atomic.Bool
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			defer release()
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, d pgx.TraceQueryEndData) {
				if kind == "late-known-ack" && q == "commit" && d.Err == nil && armed.CompareAndSwap(true, false) {
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
			if kind == "grant-unknown-ack" {
				useDial = dial
			}
			f := newPartyDS(t, trace, useDial, t.Context())
			f.join(t)
			f.bind(t)
			if kind == "grant-known-rollback" {
				f.room.base.exec(t, `CREATE FUNCTION synthetic_party_grant_rollback()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.scope='stream' THEN RAISE EXCEPTION 'synthetic rollback';END IF;RETURN NEW;END$$;CREATE TRIGGER synthetic_party_grant_rollback BEFORE INSERT ON sessions FOR EACH ROW EXECUTE FUNCTION synthetic_party_grant_rollback()`)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if kind == "late-known-ack" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
			}
			frame := f.guestFrame(t, ctx, "POST")
			if kind == "grant-unknown-ack" {
				enabled.Store(true)
			}
			armed.Store(true)
			done := make(chan struct{})
			var grant *WatchPartyPlaybackGrant
			var grantErr error
			t.Cleanup(func() { cancel(); release(); bridgeSessionAwait(t, done) })
			go func() {
				defer close(done)
				grant, grantErr = f.room.authority.GrantPartyPlayback(frame, f.room.room.ID)
			}()
			if kind == "late-known-ack" {
				bridgeSessionAwait(t, entered)
				var n int
				if e := f.room.base.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.room.base.schema, "watch_party_playback_bindings"}.Sanitize()).Scan(&n); e != nil || n != 1 {
					t.Fatal("held genuine grant ACK was not durable", n, e)
				}
				bridgeSessionAwait(t, ctx.Done())
				release()
			}
			bridgeSessionAwait(t, done)
			enabled.Store(false)
			if grant != nil || grantErr == nil {
				t.Fatal("failed/unknown/late actual grant emitted token DATA", kind, grantErr)
			}
			var count int
			if e := f.room.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_party_playback_bindings`).Scan(&count); e != nil {
				t.Fatal(e)
			}
			if kind == "grant-unknown-ack" {
				if !errors.Is(grantErr, ErrWatchRoomUnknown) || !dropped.Load() || count != 1 {
					t.Fatal("actual dropped ACK outcome falsified/retried", grantErr, count)
				}
			} else if kind == "late-known-ack" {
				if count != 1 || errors.Is(grantErr, ErrWatchRoomUnknown) {
					t.Fatal("known late grant claimed unknown/rollback", grantErr, count)
				}
			} else if count != 0 {
				t.Fatal("rolled-back grant left private token", count)
			}
		})
	}
}
