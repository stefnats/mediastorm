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
	"novastream/models"
)

type sharePlaybackDSFixture struct {
	base              *bridgeSessionFixture
	creator           *ShareLinkCreator
	runtime           *ShareLinkRedeemer
	profile, resource string
	link              *models.ShareLink
}

func newSharePlaybackDS(t *testing.T, kind string) *sharePlaybackDSFixture {
	return newSharePlaybackDSWith(t, kind, nil, nil, t.Context())
}
func newSharePlaybackDSWith(t *testing.T, kind string, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), lifetime context.Context) *sharePlaybackDSFixture {
	t.Helper()
	base, creator := shareProvenanceFixture(t, trace, dial, t.Context())
	f := &sharePlaybackDSFixture{base: base, creator: creator, profile: "target", resource: "synthetic:share-playback"}
	if kind == "master-foreign-source" || kind == "current-creator-demoted" || kind == "source-expiry" || kind == "creator-expiry" {
		base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';INSERT INTO users(id,account_id,name,allow_share_links)VALUES('foreign-source','other','Synthetic foreign source',true)`)
		f.profile = "foreign-source"
	}
	if kind == "master-own-source-nonmaster" {
		base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
	}
	if kind == "source-expiry" || kind == "creator-expiry" {
		owner := "actor"
		if kind == "source-expiry" {
			owner = "other"
		}
		base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id=$2`, time.Now().UTC().Add(20*time.Second).Truncate(time.Microsecond), owner)
	}
	original := shareProvenanceRequest(t, base, t.Context())
	uses := 4
	if kind == "max-use-concurrent" || kind == "limit-does-not-revoke-issued" {
		uses = 1
	}
	link, err := creator.Create(original, f.profile, map[string]string{"sourcePath": f.resource, "profileId": f.profile}, 24*time.Hour, uses, "")
	if err != nil {
		t.Fatal("actual original creator", err)
	}
	f.link = link
	f.runtime, err = NewShareLinkRedeemer(lifetime, base.ds, base.manager, base.security, base.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.runtime.Close)
	return f
}
func (f *sharePlaybackDSFixture) redeem(ctx context.Context) (*ShareLinkRedemption, error) {
	return f.runtime.Redeem(httptest.NewRequestWithContext(ctx, "GET", "http://synthetic.test/share/"+f.link.Token, nil), f.link.Token)
}
func (f *sharePlaybackDSFixture) counts(t *testing.T) (int, int) {
	t.Helper()
	var sessions, bindings int
	if err := f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE scope=$1`, models.SessionScopeStream).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	return sessions, bindings
}
func (f *sharePlaybackDSFixture) sourceFrame(t *testing.T, result *ShareLinkRedemption) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil)
	checked, release, err := f.base.ds.AuthenticateProfilePinRequest(r, result.Session.Token, "")
	if err != nil {
		t.Fatal("actual scoped frame", err)
	}
	return checked, release
}
func shareCopyPrincipal(t *testing.T, f *sharePlaybackDSFixture, table, id string) {
	t.Helper()
	var raw []byte
	if err := f.base.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(x) FROM `+pgx.Identifier{table}.Sanitize()+` x WHERE id=$1`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	f.base.exec(t, `DELETE FROM `+pgx.Identifier{table}.Sanitize()+` WHERE id=$1`, id)
	f.base.exec(t, `INSERT INTO `+pgx.Identifier{table}.Sanitize()+` SELECT (jsonb_populate_record(NULL::`+pgx.Identifier{table}.Sanitize()+`,$1::jsonb)).*`, raw)
}

func TestSharePlaybackIdentityContinuity(t *testing.T) {
	for _, kind := range []string{"new-original-witness", "ids-only-recreate", "null-legacy-recreate", "account-delete-reinsert", "profile-delete-reinsert", "orphan-source-owner", "owner-away-back", "role-away-back", "password-change", "source-pin-change", "share-flag-away-back", "child-policy-change", "cosmetic-stable", "insert-cannot-copy-incarnation", "qualified-trigger-schema", "row-security-unknown"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharePlaybackDS(t, kind)
			base := f.base
			switch kind {
			case "row-security-unknown":
				base.exec(t, `ALTER TABLE users ENABLE ROW LEVEL SECURITY;CREATE POLICY synthetic_share_visible ON users USING(true) WITH CHECK(true)`)
				for _, force := range []bool{false, true} {
					if force {
						base.exec(t, `ALTER TABLE users FORCE ROW LEVEL SECURITY`)
					}
					var visible int
					if e := base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target'`).Scan(&visible); e != nil || visible != 1 {
						t.Fatal("RLS canary lost real current source visibility", e)
					}
					if result, e := f.redeem(t.Context()); e == nil || result != nil {
						t.Fatal("unknown row-security relation minted a recipient", force)
					}
					if a, b := f.counts(t); a != 0 || b != 0 {
						t.Fatal("row-security denial left minted sidecar/session", a, b)
					}
				}
				return
			case "new-original-witness":
				var original bool
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT l.delegation_version=1 AND l.creator_incarnation=a.share_incarnation AND l.source_owner_incarnation=b.share_incarnation AND l.source_profile_incarnation=u.share_incarnation FROM share_links l JOIN accounts a ON a.id=l.account_id JOIN accounts b ON b.id=l.source_account_id JOIN users u ON u.id=l.source_profile_id WHERE token=$1`, f.link.Token).Scan(&original); err != nil || !original {
					t.Fatal("original creator did not persist its actual witnesses", err)
				}
			case "ids-only-recreate":
				base.exec(t, `UPDATE share_links SET delegation_version=NULL,creator_incarnation=NULL,source_owner_incarnation=NULL,source_profile_incarnation=NULL WHERE token=$1`, f.link.Token)
			case "null-legacy-recreate":
				base.exec(t, `UPDATE share_links SET provenance_version=NULL,source_profile_id=NULL,source_account_id=NULL,delegation_version=NULL,creator_incarnation=NULL,source_owner_incarnation=NULL,source_profile_incarnation=NULL WHERE token=$1`, f.link.Token)
			case "account-delete-reinsert":
				shareCopyPrincipal(t, f, "accounts", "actor")
			case "profile-delete-reinsert":
				shareCopyPrincipal(t, f, "users", "target")
			case "orphan-source-owner":
				base.exec(t, `DELETE FROM accounts WHERE id='actor'`)
				var n int
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target' AND account_id='actor'`).Scan(&n); err != nil || n != 1 {
					t.Fatal("actual012 orphan fixture was not retained", n, err)
				}
			case "owner-away-back":
				base.exec(t, `UPDATE users SET account_id='other' WHERE id='target';UPDATE users SET account_id='actor' WHERE id='target'`)
			case "role-away-back":
				base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';UPDATE accounts SET is_master=false WHERE id='actor'`)
			case "password-change":
				base.exec(t, `UPDATE accounts SET password_hash=password_hash||'synthetic-change' WHERE id='actor'`)
			case "source-pin-change":
				base.exec(t, `UPDATE users SET pin_hash='synthetic-unsupported-pin',pin_length=4 WHERE id='target'`)
			case "share-flag-away-back":
				base.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target';UPDATE users SET allow_share_links=true WHERE id='target'`)
			case "child-policy-change":
				base.exec(t, `UPDATE users SET kids_max_movie_rating='synthetic-changed' WHERE id='target'`)
			case "cosmetic-stable":
				var a, u, b, v string
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT a.share_incarnation::text,u.share_incarnation::text FROM accounts a JOIN users u ON u.account_id=a.id WHERE u.id='target'`).Scan(&a, &u); err != nil {
					t.Fatal(err)
				}
				base.exec(t, `UPDATE accounts SET username='Synthetic cosmetic actor',updated_at=clock_timestamp() WHERE id='actor';UPDATE users SET name='Synthetic cosmetic source',color='#123456',updated_at=clock_timestamp() WHERE id='target'`)
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT a.share_incarnation::text,u.share_incarnation::text FROM accounts a JOIN users u ON u.account_id=a.id WHERE u.id='target'`).Scan(&b, &v); err != nil || a != b || u != v {
					t.Fatal("cosmetic fields rotated delegation identity", err)
				}
			case "insert-cannot-copy-incarnation":
				var previous, next string
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT share_incarnation::text FROM users WHERE id='target'`).Scan(&previous); err != nil {
					t.Fatal(err)
				}
				base.exec(t, `INSERT INTO users(id,account_id,name,share_incarnation)VALUES('copied-source','actor','Synthetic copied source',$1::uuid)`, previous)
				if err := base.ds.pool.QueryRow(t.Context(), `SELECT share_incarnation::text FROM users WHERE id='copied-source'`).Scan(&next); err != nil || next == previous {
					t.Fatal("explicit INSERT copied old identity", err)
				}
			case "qualified-trigger-schema":
				original, err := f.redeem(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				tx, err := base.ds.pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer recordingStateRollback(tx)
				if _, err = tx.Exec(t.Context(), `CREATE TEMP TABLE share_playback_bindings(session_token TEXT);INSERT INTO pg_temp.share_playback_bindings VALUES('synthetic-shadow');SET LOCAL search_path=pg_temp,`+pgx.Identifier{base.schema}.Sanitize()+`,pg_catalog`); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(t.Context(), `UPDATE `+pgx.Identifier{base.schema, "share_links"}.Sanitize()+` SET active=false WHERE token=$1`, f.link.Token); err != nil {
					t.Fatal(err)
				}
				var real, shadow int
				if err = tx.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM `+pgx.Identifier{base.schema, "share_playback_bindings"}.Sanitize()+` WHERE session_token=$1),(SELECT count(*) FROM pg_temp.share_playback_bindings)`, original.Session.Token).Scan(&real, &shadow); err != nil || real != 0 || shadow != 1 {
					t.Fatal("trigger used ambient shadow relation", real, shadow, err)
				}
				return
			}
			result, err := f.redeem(t.Context())
			allowed := kind == "new-original-witness" || kind == "cosmetic-stable" || kind == "insert-cannot-copy-incarnation"
			if allowed {
				if err != nil || result == nil {
					t.Fatal("current original delegation denied", kind, err)
				}
			} else {
				if err == nil || result != nil {
					t.Fatal("unknown/replaced original identity revived delegation", kind)
				}
				if a, b := f.counts(t); a != 0 || b != 0 {
					t.Fatal("denied original witness left a recipient", a, b)
				}
			}
		})
	}
}

func TestSharePlaybackAtomicRedemption(t *testing.T) {
	for _, kind := range []string{"own-source", "master-foreign-source", "master-own-source-nonmaster", "max-use-concurrent", "limit-does-not-revoke-issued", "fixed-session-expiry", "source-expiry", "creator-expiry", "missing-owner", "current-flag-denied", "current-creator-demoted", "malformed-resource", "oversized-database-row"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharePlaybackDS(t, kind)
			switch kind {
			case "missing-owner":
				f.base.exec(t, `DELETE FROM accounts WHERE id='actor'`)
			case "current-flag-denied":
				f.base.exec(t, `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profile)
			case "current-creator-demoted":
				f.base.exec(t, `UPDATE accounts SET is_master=false WHERE id='actor'`)
			case "malformed-resource":
				raw, _ := json.Marshal(map[string]string{"sourcePath": ""})
				f.base.exec(t, `UPDATE share_links SET params=$2 WHERE token=$1`, f.link.Token, raw)
			case "oversized-database-row":
				raw, _ := json.Marshal(map[string]string{"sourcePath": strings.Repeat("x", (1<<20)+1)})
				f.base.exec(t, `UPDATE share_links SET params=$2 WHERE token=$1`, f.link.Token, raw)
			}
			if kind == "max-use-concurrent" {
				type outcome struct {
					result *ShareLinkRedemption
					err    error
				}
				results := make(chan outcome, 2)
				var wg sync.WaitGroup
				wg.Add(2)
				for n := 0; n < 2; n++ {
					go func() { defer wg.Done(); r, e := f.redeem(t.Context()); results <- outcome{r, e} }()
				}
				wg.Wait()
				close(results)
				passed := 0
				for out := range results {
					if out.err == nil && out.result != nil {
						passed++
					}
				}
				a, b := f.counts(t)
				if passed != 1 || a != 1 || b != 1 {
					t.Fatal("sameTX use limit did not have exactly one stored winner", passed, a, b)
				}
				return
			}
			result, err := f.redeem(t.Context())
			denied := kind == "missing-owner" || kind == "current-flag-denied" || kind == "current-creator-demoted" || kind == "malformed-resource" || kind == "oversized-database-row"
			if denied {
				if err == nil || result != nil {
					t.Fatal("invalid current delegation minted", kind)
				}
				if a, b := f.counts(t); a != 0 || b != 0 {
					t.Fatal("failed transaction retained partial mint", a, b)
				}
				return
			}
			if err != nil || result == nil {
				t.Fatal("actual atomic redemption", err)
			}
			wantOwner := "actor"
			if kind == "master-foreign-source" {
				wantOwner = "other"
			}
			if result.Session.AccountID != wantOwner || result.Session.IsMaster || result.Session.Scope != models.SessionScopeStream || result.Session.ScopeResource != f.resource || result.Params["profileId"] != f.profile {
				t.Fatal("creator/source identity or restricted scope conflated")
			}
			var count int
			var nonce, source, creator string
			if err = f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings b JOIN sessions s ON s.token=b.session_token AND s.share_binding_id=b.binding_id WHERE s.token=$1`, result.Session.Token).Scan(&count); err != nil || count != 1 {
				t.Fatal("no actual private nonce correlation", count, err)
			}
			if err = f.base.ds.pool.QueryRow(t.Context(), `SELECT binding_id::text,source_account_id,creator_id FROM share_playback_bindings WHERE session_token=$1`, result.Session.Token).Scan(&nonce, &source, &creator); err != nil || !shareCanonicalUUID(nonce) || source != wantOwner || creator != "actor" {
				t.Fatal("actual durable actor/source binding", err)
			}
			if kind == "fixed-session-expiry" && result.Session.ExpiresAt.Sub(result.Session.CreatedAt) != 6*time.Hour {
				t.Fatal("session lifetime was replaced by temporary proof budget")
			}
			if kind == "source-expiry" || kind == "creator-expiry" {
				var expiry time.Time
				if err = f.base.ds.pool.QueryRow(t.Context(), `SELECT expires_at FROM accounts WHERE id=$1`, func() string {
					if kind == "source-expiry" {
						return "other"
					}
					return "actor"
				}()).Scan(&expiry); err != nil || !result.Session.ExpiresAt.Equal(expiry) {
					t.Fatal("known account expiry not fixed on actual session", err)
				}
			}
			if kind == "limit-does-not-revoke-issued" {
				if extra, e := f.redeem(t.Context()); e == nil || extra != nil {
					t.Fatal("used link minted a second recipient")
				}
				r, release := f.sourceFrame(t, result)
				defer release()
				if err = f.runtime.ValidateSharePlaybackRequest(r); err != nil {
					t.Fatal("maxUses invalidated the final legitimately issued recipient", err)
				}
			}
		})
	}
}

func TestSharePlaybackRedemptionOriginalLifetime(t *testing.T) {
	for _, kind := range []string{"held-link-caller-cancel", "held-link-runtime-cancel", "writer-first", "redeemer-first", "changed-original-policy", "known-rollback", "mint-unknown-ack", "late-known-ack", "physical-cleanup-before-refund", "healthy-connection-reuse", "copied-runtime", "foreign-store"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var armed, enabled, dropped atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				match := kind == "late-known-ack" && sql == "commit" || (kind == "redeemer-first" || kind == "changed-original-policy") && strings.HasPrefix(sql, "INSERT INTO sessions")
				if match && d.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-release
				}
			}}
			var gate *queryLifecycleGatedConn
			var gateMu sync.Mutex
			var first atomic.Bool
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "mint-unknown-ack" || kind == "physical-cleanup-before-refund" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					c, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					if kind == "mint-unknown-ack" {
						return &bridgeSessionDropACK{Conn: c, enabled: &enabled, actual: &parentDropCommit{Conn: c, dropped: &dropped}}, nil
					}
					if first.CompareAndSwap(false, true) {
						value := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
						gateMu.Lock()
						gate = value
						gateMu.Unlock()
						return value, nil
					}
					return c, nil
				}
			}
			f := newSharePlaybackDSWith(t, kind, trace, dial, life)
			if kind == "foreign-store" {
				foreign, e := New(t.Context(), f.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				value, e := NewShareLinkRedeemer(t.Context(), foreign, f.base.manager, f.base.security, f.base.coordinator)
				if e == nil || value != nil {
					t.Fatal("foreign actual store acquired source runtime")
				}
				return
			}
			if kind == "copied-runtime" {
				original := f.runtime
				f.runtime = &ShareLinkRedeemer{self: original, store: original.store, manager: original.manager, security: original.security, coordinator: original.coordinator, ctx: original.ctx}
				if value, e := f.redeem(t.Context()); e == nil || value != nil {
					t.Fatal("copied runtime minted")
				}
				return
			}
			if kind == "healthy-connection-reuse" {
				connection, e := f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				physical := connection.Conn().PgConn()
				connection.Release()
				if _, e = f.redeem(t.Context()); e != nil {
					t.Fatal(e)
				}
				if physical.IsClosed() {
					t.Fatal("healthy source mint destroyed original connection")
				}
				if f.base.ds.pool.Stat().AcquiredConns() != 0 {
					t.Fatal("completed mint retained connection")
				}
				return
			}
			if kind == "writer-first" {
				guard, e := f.base.coordinator.TryWrite(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer guard.Close()
				if value, e := f.redeem(t.Context()); e == nil || value != nil {
					t.Fatal("original writer did not exclude public mint")
				}
				return
			}
			if kind == "known-rollback" {
				f.base.exec(t, `CREATE FUNCTION synthetic_share_mint_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic share mint rollback'; END $$;CREATE TRIGGER synthetic_share_mint_reject BEFORE INSERT ON sessions FOR EACH ROW EXECUTE FUNCTION synthetic_share_mint_reject()`)
			}
			var holder pgx.Tx
			if strings.HasPrefix(kind, "held-link-") {
				var e error
				holder, e = f.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { recordingStateRollback(holder) })
				var token string
				if e = holder.QueryRow(t.Context(), `SELECT token FROM `+pgx.Identifier{f.base.schema, "share_links"}.Sanitize()+` WHERE token=$1 FOR UPDATE`, f.link.Token).Scan(&token); e != nil {
					t.Fatal(e)
				}
			}
			var actual *queryLifecycleGatedConn
			var physicalCleanup <-chan struct{}
			if kind == "physical-cleanup-before-refund" {
				gateMu.Lock()
				actual = gate
				gateMu.Unlock()
				if actual == nil {
					t.Fatal("actual primary transport absent")
				}
				t.Cleanup(actual.release)
				connection, e := f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				if connection.Conn().PgConn().Conn() != actual {
					connection.Release()
					t.Fatal("gated transport was not actual mint backend")
				}
				physicalCleanup = connection.Conn().PgConn().CleanupDone()
				connection.Release()
				actual.armed.Store(true)
			}
			timeout := 10 * time.Second
			if kind == "late-known-ack" {
				timeout = 1500 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			var result *ShareLinkRedemption
			var err error
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				retire()
				unblock()
				if actual != nil {
					actual.release()
				}
				if holder != nil {
					recordingStateRollback(holder)
				}
				bridgeSessionAwait(t, done)
			})
			armed.Store(true)
			enabled.Store(kind == "mint-unknown-ack")
			go func() { result, err = f.redeem(ctx); close(done) }()
			if holder != nil {
				until := time.Now().Add(5 * time.Second)
				waiting := false
				for time.Now().Before(until) {
					if e := f.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM share_links WHERE token%FOR UPDATE%')`, f.base.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("original link lock wait absent")
				}
				if kind == "held-link-caller-cancel" {
					cancel()
				} else {
					retire()
				}
			}
			if kind == "redeemer-first" || kind == "changed-original-policy" {
				bridgeSessionAwait(t, entered)
				settings := f.base.settings
				settings.Security.EnforceProfilePins = true
				if kind == "changed-original-policy" {
					raw, e := json.Marshal(settings)
					if e != nil {
						t.Fatal(e)
					}
					if e = os.WriteFile(f.base.manager.ConfigPath(), raw, 0600); e != nil {
						t.Fatal(e)
					}
				} else if e := f.base.manager.Save(settings); e == nil {
					t.Fatal("native source hold admitted writer")
				}
				unblock()
			}
			if kind == "late-known-ack" {
				bridgeSessionAwait(t, entered)
				if a, b := f.counts(t); a != 1 || b != 1 {
					t.Fatal("real knownACK transaction absent", a, b)
				}
				bridgeSessionAwait(t, ctx.Done())
				unblock()
			}
			if actual != nil {
				bridgeSessionAwait(t, actual.closeEntered)
				select {
				case <-physicalCleanup:
					t.Fatal("physical cleanup bypassed actual held close")
				default:
				}
				f.runtime.mu.Lock()
				charged := f.runtime.active
				f.runtime.mu.Unlock()
				if charged != 1 {
					t.Fatal("source admission refunded before physical end")
				}
				select {
				case <-done:
					t.Fatal("public mint returned before actual close")
				default:
				}
				actual.release()
				bridgeSessionAwait(t, physicalCleanup)
			}
			bridgeSessionAwait(t, done)
			if holder != nil {
				recordingStateRollback(holder)
			}
			if err == nil || result != nil {
				t.Fatal("failed/canceled/late public mint published", kind)
			}
			if kind == "physical-cleanup-before-refund" && !errors.Is(err, ErrSharePlaybackUnavailable) {
				t.Fatal("actual physical source cleanup outage lost retryable class", err)
			}
			a, b := f.counts(t)
			if kind == "mint-unknown-ack" {
				if !dropped.Load() || !errors.Is(err, ErrSharePlaybackUnknown) || a != 1 || b != 1 {
					t.Fatal("real lost ACK was retried or claimed rollback", a, b, err)
				}
			} else if kind == "late-known-ack" {
				if errors.Is(err, ErrSharePlaybackUnknown) || a != 1 || b != 1 {
					t.Fatal("late genuine knownACK classified as unknown/rollback", a, b, err)
				}
			} else if a != 0 || b != 0 {
				t.Fatal("failed transaction retained partial source mint", a, b)
			}
		})
	}
}
