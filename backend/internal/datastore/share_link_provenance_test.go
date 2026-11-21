//go:build linux

package datastore

import (
	"bytes"
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

func shareProvenanceFixture(t *testing.T, trace pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), lifetime context.Context) (*bridgeSessionFixture, *ShareLinkCreator) {
	t.Helper()
	f := bridgeSessionPG(t, "", false, false, trace, dial)
	f.exec(t, `UPDATE users SET allow_share_links=true WHERE id='target'`)
	f.exec(t, `INSERT INTO sessions(token,account_id,is_master,created_at,expires_at,client_id)VALUES('synthetic-share-frame','actor',false,now(),now()+interval '1 hour','synthetic-share-client')`)
	c, err := NewShareLinkCreator(lifetime, f.ds, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	return f, c
}
func shareProvenanceRequest(t *testing.T, f *bridgeSessionFixture, ctx context.Context) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/private-share-create", nil)
	r.Header.Set("X-Client-ID", "synthetic-share-client")
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, "synthetic-share-frame", "synthetic-share-client")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return checked
}
func shareProvenanceCount(t *testing.T, f *bridgeSessionFixture) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.schema, "share_links"}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func shareProvenanceCreate(t *testing.T, f *bridgeSessionFixture, c *ShareLinkCreator, r *http.Request, profile string) *models.ShareLink {
	t.Helper()
	link, err := c.Create(r, profile, map[string]string{"sourcePath": "synthetic:movie.mkv"}, time.Hour, 1, "Synthetic share")
	if err != nil || link == nil {
		t.Fatal("actual provenance creation", err)
	}
	var actor, owner, selected string
	var version int
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT account_id,source_account_id,source_profile_id,provenance_version FROM share_links WHERE token=$1`, link.Token).Scan(&actor, &owner, &selected, &version); err != nil || actor != link.AccountID || selected != profile || version != 1 {
		t.Fatal("actual persisted provenance mismatch", err)
	}
	return link
}
func TestShareLinkProvenanceActualCreation(t *testing.T) {
	for _, kind := range []string{"own-profile", "master-foreign-source", "exact-limit-clamps", "own-expiry", "old-generic-unattested"} {
		t.Run(kind, func(t *testing.T) {
			f, c := shareProvenanceFixture(t, nil, nil, t.Context())
			profile := "target"
			if kind == "master-foreign-source" {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';INSERT INTO users(id,account_id,name,allow_share_links)VALUES('foreign-profile','other','Synthetic foreign',true)`)
				profile = "foreign-profile"
			}
			var expiry time.Time
			if kind == "own-expiry" {
				expiry = time.Now().Add(40 * time.Second).UTC().Truncate(time.Microsecond)
				f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, expiry)
			}
			r := shareProvenanceRequest(t, f, t.Context())
			if kind == "old-generic-unattested" {
				old := &models.ShareLink{Token: "synthetic-old-share", AccountID: "actor", Params: map[string]string{"profileId": "target", "provenance_version": "1"}, Active: true, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
				if err := f.ds.ShareLinks().Create(r.Context(), old); err != nil {
					t.Fatal(err)
				}
				var unknown bool
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT provenance_version IS NULL AND source_profile_id IS NULL AND source_account_id IS NULL FROM share_links WHERE token=$1`, old.Token).Scan(&unknown); err != nil || !unknown {
					t.Fatal("generic JSON attested a legacy row", err)
				}
				return
			}
			if kind == "exact-limit-clamps" {
				link, err := c.Create(r, profile, map[string]string{"movie": "synthetic:movie"}, 365*24*time.Hour, 100, "Synthetic")
				if err != nil || link == nil || link.MaxUses != models.ShareLinkMaxUses || link.ExpiresAt.Sub(link.CreatedAt) != 7*24*time.Hour {
					t.Fatal("ordinary share limits changed", err)
				}
				return
			}
			link := shareProvenanceCreate(t, f, c, r, profile)
			if kind == "master-foreign-source" {
				var owner string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT source_account_id FROM share_links WHERE token=$1`, link.Token).Scan(&owner); err != nil || owner != "other" || link.AccountID != "actor" || !link.IsMaster {
					t.Fatal("master actor and foreign source owner conflated", err)
				}
			}
			if kind == "own-expiry" && !link.ExpiresAt.Equal(expiry) {
				t.Fatal("persistent TTL ignored known account expiry")
			}
		})
	}
}

func TestShareLinkProvenanceCurrentAuthority(t *testing.T) {
	for _, kind := range []string{"forged-context", "foreign-frame", "scoped-session", "role-drift", "actor-aba", "profile-aba", "owner-reparent", "flag-change", "copied-creator", "foreign-store"} {
		t.Run(kind, func(t *testing.T) {
			var f *bridgeSessionFixture
			var armed atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if !strings.HasPrefix(sql, "SELECT u.account_id,concat") || d.Err != nil || !armed.CompareAndSwap(true, false) {
					return
				}
				switch kind {
				case "profile-aba":
					var raw []byte
					if e := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); e != nil {
						t.Fatal(e)
					}
					f.exec(t, `DELETE FROM users WHERE id='target'`)
					f.exec(t, `INSERT INTO users SELECT (jsonb_populate_record(NULL::users,$1::jsonb)).*`, raw)
				case "owner-reparent":
					f.exec(t, `UPDATE users SET account_id='other' WHERE id='target'`)
				case "flag-change":
					f.exec(t, `UPDATE users SET allow_share_links=false WHERE id='target'`)
				}
			}}
			var c *ShareLinkCreator
			f, c = shareProvenanceFixture(t, trace, nil, t.Context())
			if kind == "scoped-session" {
				f.exec(t, `UPDATE sessions SET scope='stream',scope_resource='synthetic:movie' WHERE token='synthetic-share-frame'`)
			}
			r := shareProvenanceRequest(t, f, t.Context())
			switch kind {
			case "forged-context":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
			case "foreign-frame", "foreign-store":
				other, e := New(t.Context(), f.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(other.Close)
				if kind == "foreign-store" {
					value, e := NewShareLinkCreator(t.Context(), other, f.manager, f.security, f.coordinator)
					if e == nil || value != nil {
						t.Fatal("foreign store acquired creator")
					}
					return
				}
				var release context.CancelFunc
				r, release, e = other.AuthenticateProfilePinRequest(httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/share", nil), "synthetic-share-frame", "synthetic-share-client")
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(release)
			case "role-drift":
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
			case "actor-aba":
				// Change only the genuine actor's physical tuple. Deleting this account
				// would cascade the original session and merely test its absence.
				statement := `SELECT to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),(SELECT concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM users u WHERE id='target'),(SELECT concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text) FROM sessions s WHERE token='synthetic-share-frame') FROM accounts a WHERE a.id='actor'`
				var before, after []byte
				var actorBefore, profileBefore, sessionBefore, actorAfter, profileAfter, sessionAfter string
				if e := f.ds.pool.QueryRow(t.Context(), statement).Scan(&before, &actorBefore, &profileBefore, &sessionBefore); e != nil {
					t.Fatal(e)
				}
				f.exec(t, `UPDATE accounts SET username=username WHERE id='actor'`)
				if e := f.ds.pool.QueryRow(t.Context(), statement).Scan(&after, &actorAfter, &profileAfter, &sessionAfter); e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(before, after) || actorBefore == actorAfter || profileBefore != profileAfter || sessionBefore != sessionAfter {
					t.Fatal("actor ABA fixture did not isolate original physical actor change")
				}

			case "copied-creator":
				copy := *c
				c = &copy
			}
			armed.Store(true)
			link, e := c.Create(r, "target", map[string]string{"sourcePath": "synthetic:movie"}, time.Hour, 1, "")
			if e == nil || link != nil || shareProvenanceCount(t, f) != 0 {
				t.Fatal("stale/foreign/ABA authority attested a row", kind, e)
			}
		})
	}
}

func TestShareLinkProvenanceOwnedData(t *testing.T) {
	for _, kind := range []string{"params-owned", "selectors-not-params", "missing-profile", "oversized-data"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if kind == "params-owned" && strings.HasPrefix(sql, "SELECT u.account_id,concat") && d.Err == nil {
					close(entered)
					<-proceed
				}
			}}
			f, c := shareProvenanceFixture(t, trace, nil, t.Context())
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			r := shareProvenanceRequest(t, f, ctx)
			params := map[string]string{"sourcePath": "synthetic:original", "profileId": "foreign", "source_account_id": "foreign", "is_master": "true"}
			profile := "target"
			if kind == "missing-profile" {
				profile = ""
			}
			if kind == "oversized-data" {
				params["title"] = strings.Repeat("x", (1<<20)+1)
			}
			var link *models.ShareLink
			var err error
			if kind == "params-owned" {
				done := make(chan struct{})
				t.Cleanup(func() { cancel(); unblock(); bridgeSessionAwait(t, done) })
				go func() { link, err = c.Create(r, profile, params, time.Hour, 1, ""); close(done) }()
				bridgeSessionAwait(t, entered)
				params["sourcePath"] = "synthetic:changed"
				unblock()
				bridgeSessionAwait(t, done)
			} else {
				link, err = c.Create(r, profile, params, time.Hour, 1, "")
			}
			if kind == "missing-profile" || kind == "oversized-data" {
				if err == nil || link != nil || shareProvenanceCount(t, f) != 0 {
					t.Fatal("unbounded/missing selector accepted", err)
				}
				return
			}
			if err != nil || link == nil {
				t.Fatal(err)
			}
			var raw []byte
			var actor, owner, selected string
			if e := f.ds.pool.QueryRow(t.Context(), `SELECT params,account_id,source_account_id,source_profile_id FROM share_links WHERE token=$1`, link.Token).Scan(&raw, &actor, &owner, &selected); e != nil {
				t.Fatal(e)
			}
			var saved map[string]string
			if json.Unmarshal(raw, &saved) != nil || saved["sourcePath"] != "synthetic:original" || saved["profileId"] != "target" || actor != "actor" || owner != "actor" || selected != "target" {
				t.Fatal("caller DATA altered actual origin or retained map alias")
			}
		})
	}
}

func shareProvenancePhysicalCleanup(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var gate *queryLifecycleGatedConn
	var first atomic.Bool
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		v, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if first.CompareAndSwap(false, true) {
			value := &queryLifecycleGatedConn{Conn: v, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			gate = value
			mu.Unlock()
			return value, nil
		}
		return v, nil
	}
	f, c := shareProvenanceFixture(t, nil, dial, t.Context())
	r := shareProvenanceRequest(t, f, t.Context())
	mu.Lock()
	actual := gate
	mu.Unlock()
	if actual == nil {
		t.Fatal("actual primary transport missing")
	}
	t.Cleanup(actual.release)
	conn, e := f.ds.pool.Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	physical := conn.Conn().PgConn()
	conn.Release()
	actual.armed.Store(true)
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() { actual.release(); <-joined })
	go func() {
		defer close(joined)
		_, err := c.Create(r, "target", map[string]string{"sourcePath": "synthetic:movie"}, time.Hour, 1, "")
		done <- err
	}()
	bridgeSessionAwait(t, actual.closeEntered)
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical cleanup bypassed held Close")
	default:
	}
	shareCreationAdmission.Lock()
	charged := shareCreationAdmission.accounts["actor"]
	shareCreationAdmission.Unlock()
	if charged != 1 {
		t.Fatal("admission refunded before actual physical cleanup")
	}
	select {
	case <-joined:
		t.Fatal("creation returned before physical end")
	default:
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if guard != nil {
		_ = guard.Close()
	}
	if !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("native hold refunded early", err)
	}
	actual.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, joined)
	if err = <-done; err == nil {
		t.Fatal("failed physical transport produced provenance")
	}
}
func TestShareLinkProvenanceTransactionLifetime(t *testing.T) {
	for _, kind := range []string{"held-insert-caller-cancel", "held-insert-startup-cancel", "actual-physical-cleanup", "known-rollback", "unknown-ack-no-retry", "known-ack-after-expiry"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "actual-physical-cleanup" {
				shareProvenancePhysicalCleanup(t)
				return
			}
			var enabled, dropped atomic.Bool
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				v, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &bridgeSessionDropACK{Conn: v, enabled: &enabled, actual: &parentDropCommit{Conn: v, dropped: &dropped}}, nil
			}
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			var armed atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if kind == "known-ack-after-expiry" && sql == "commit" && d.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			life, retire := context.WithCancel(t.Context())
			defer retire()
			var useDial func(context.Context, string, string) (net.Conn, error)
			if kind == "unknown-ack-no-retry" {
				useDial = dial
			}
			f, c := shareProvenanceFixture(t, trace, useDial, life)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if kind == "known-ack-after-expiry" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
			}
			r := shareProvenanceRequest(t, f, ctx)
			if kind == "known-rollback" {
				f.exec(t, `CREATE FUNCTION synthetic_share_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic share rollback'; END $$;CREATE TRIGGER synthetic_share_reject BEFORE INSERT ON share_links FOR EACH ROW EXECUTE FUNCTION synthetic_share_reject()`)
			}
			var holder pgx.Tx
			if strings.HasPrefix(kind, "held-insert-") {
				var e error
				holder, e = f.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { recordingStateRollback(holder) })
				if _, e = holder.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "share_links"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "unknown-ack-no-retry" {
				enabled.Store(true)
			}
			armed.Store(true)
			var link *models.ShareLink
			var err error
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				retire()
				unblock()
				if holder != nil {
					recordingStateRollback(holder)
				}
				bridgeSessionAwait(t, done)
			})
			go func() {
				link, err = c.Create(r, "target", map[string]string{"sourcePath": "synthetic:movie"}, time.Hour, 1, "")
				close(done)
			}()
			if holder != nil {
				until := time.Now().Add(5 * time.Second)
				waiting := false
				for time.Now().Before(until) {
					if e := f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'INSERT INTO share_links%')`, f.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("actual share INSERT never blocked")
				}
				if kind == "held-insert-caller-cancel" {
					cancel()
				} else {
					retire()
				}
			}
			if kind == "known-ack-after-expiry" {
				bridgeSessionAwait(t, entered)
				if shareProvenanceCount(t, f) != 1 {
					t.Fatal("held actual ACK not committed")
				}
				bridgeSessionAwait(t, ctx.Done())
				unblock()
			}
			bridgeSessionAwait(t, done)
			if err == nil || link != nil {
				t.Fatal("failed/late/unknown creation published token", err)
			}
			if holder != nil {
				recordingStateRollback(holder)
			}
			count := shareProvenanceCount(t, f)
			if kind == "unknown-ack-no-retry" {
				if !errors.Is(err, ErrShareProvenanceUnknown) || !dropped.Load() || count != 1 {
					t.Fatal("actual lost ACK retried or claimed rollback", err, count)
				}
			} else if kind == "known-ack-after-expiry" {
				if count != 1 || errors.Is(err, ErrShareProvenanceUnknown) {
					t.Fatal("late known ACK classified as unknown/rollback", err)
				}
			} else if count != 0 {
				t.Fatal("failed creation committed a row")
			}
		})
	}
}

func TestShareLinkProvenancePolicyFence(t *testing.T) {
	for _, kind := range []string{"changed-policy", "writer-first", "creator-first"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			var armed atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if strings.HasPrefix(sql, "INSERT INTO share_links") && d.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			f, c := shareProvenanceFixture(t, trace, nil, t.Context())
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			r := shareProvenanceRequest(t, f, ctx)
			if kind == "writer-first" {
				guard, e := f.coordinator.TryWrite(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer guard.Close()
				link, e := c.Create(r, "target", map[string]string{"sourcePath": "synthetic:movie"}, time.Hour, 1, "")
				if e == nil || link != nil || shareProvenanceCount(t, f) != 0 {
					t.Fatal("actual writer did not exclude creation", e)
				}
				return
			}
			armed.Store(true)
			done := make(chan struct{})
			var link *models.ShareLink
			var err error
			t.Cleanup(func() { cancel(); unblock(); bridgeSessionAwait(t, done) })
			go func() {
				link, err = c.Create(r, "target", map[string]string{"sourcePath": "synthetic:movie"}, time.Hour, 1, "")
				close(done)
			}()
			bridgeSessionAwait(t, entered)
			settings := f.settings
			settings.Security.EnforceProfilePins = true
			if kind == "changed-policy" {
				raw, e := json.Marshal(settings)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(f.manager.ConfigPath(), raw, 0600); e != nil {
					t.Fatal(e)
				}
			} else if e := f.manager.Save(settings); !errors.Is(e, settingscommit.ErrBusy) {
				t.Fatal("actual native hold did not exclude writer", e)
			}
			unblock()
			bridgeSessionAwait(t, done)
			if err == nil || link != nil || shareProvenanceCount(t, f) != 0 {
				t.Fatal("creator adopted changed/retired original policy", err)
			}
		})
	}
}
