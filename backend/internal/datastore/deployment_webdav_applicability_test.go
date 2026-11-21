//go:build linux

package datastore

import (
	"context"
	"encoding/json"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/settingscommit"
)

func deploymentWebDAVPG(t *testing.T, tracer pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error), poolOne bool) (*bridgeSessionFixture, *DeploymentWebDAVIssuer) {
	t.Helper()
	f := bridgeSessionPG(t, "", false, false, tracer, dial)
	if poolOne {
		if err := f.security.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.coordinator.Close(); err != nil {
			t.Fatal(err)
		}
		cfg := f.ds.pool.Config()
		cfg.MaxConns = 1
		cfg.MinConns = 0
		f.ds.pool.Close()
		actual, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		f.ds.pool = actual
		objects, err := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
		if err != nil {
			t.Fatal(err)
		}
		f.coordinator, err = settingscommit.NewForPool(t.Context(), actual, objects)
		if err != nil {
			t.Fatal(err)
		}
		guard, err := f.coordinator.TryWrite(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err = f.coordinator.Arm(guard); err != nil {
			t.Fatal(err)
		}
		f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
		_ = guard.Close()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.security.Close(); _ = f.coordinator.Close(); _ = objects.Close() })
	}
	f.settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	issuer, err := NewDeploymentWebDAVIssuer(t.Context(), f.ds, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(issuer.Close)
	return f, issuer
}
func deploymentWebDAVRequest(t *testing.T) (*http.Request, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodGet, "http://synthetic.test/webdav/movie", nil).WithContext(ctx)
	r.RemoteAddr = "127.0.0.1:22001"
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	return r, cancel
}
func deploymentWebDAVCapture(t *testing.T, f *bridgeSessionFixture, issuer *DeploymentWebDAVIssuer, r *http.Request) *DeploymentWebDAVApplicability {
	t.Helper()
	native, err := f.security.CaptureWebDAVBasicPolicy(r)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := issuer.Capture(r, native)
	if err != nil || proof == nil {
		t.Fatal("actual complete deployment capture", err)
	}
	t.Cleanup(proof.Close)
	return proof
}
func deploymentWebDAVPending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("original operation ended before owned work joined")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestDeploymentWebDAVCompleteSnapshot(t *testing.T) {
	for _, name := range []string{"small-complete-inactive", "empty-complete-deployment", "foreign-active-household", "orphan-profile", "null-or-malformed-member", "account-overflow", "member-overflow", "wire-byte-overflow", "rls-or-catalog-unknown"} {
		t.Run(name, func(t *testing.T) {
			f, issuer := deploymentWebDAVPG(t, nil, nil, false)
			r, _ := deploymentWebDAVRequest(t)
			switch name {
			case "empty-complete-deployment":
				f.exec(t, `DELETE FROM users;DELETE FROM accounts`)
			case "foreign-active-household":
				f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('foreign-child','other','Synthetic child',true)`)
			case "orphan-profile":
				f.exec(t, `UPDATE users SET account_id='missing-original-account' WHERE id='target'`)
			case "null-or-malformed-member":
				f.exec(t, `ALTER TABLE users ALTER COLUMN is_kids_profile DROP NOT NULL;UPDATE users SET is_kids_profile=NULL WHERE id='target'`)
			case "account-overflow":
				f.exec(t, `INSERT INTO accounts(id,username,password_hash)SELECT 'account-'||n,'Synthetic account '||n,'synthetic-unused-hash' FROM generate_series(1,257)n`)
			case "member-overflow":
				f.exec(t, `INSERT INTO users(id,account_id,name)SELECT 'member-'||n,'actor','Synthetic member' FROM generate_series(1,4097)n`)
			case "wire-byte-overflow":
				f.exec(t, `INSERT INTO users(id,account_id,name)SELECT repeat('x',220)||lpad(n::text,5,'0'),'actor','Synthetic member' FROM generate_series(1,400)n`)
			case "rls-or-catalog-unknown":
				f.exec(t, `ALTER TABLE users ENABLE ROW LEVEL SECURITY`)
			}
			native, err := f.security.CaptureWebDAVBasicPolicy(r)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := issuer.Capture(r, native)
			if name == "small-complete-inactive" || name == "empty-complete-deployment" {
				if err != nil || proof == nil {
					t.Fatal(err)
				}
				defer proof.Close()
				if proof.Prefix() != "/webdav" {
					t.Fatal("original native route prefix was not captured")
				}
				if err = proof.Check(r); err != nil {
					t.Fatal(err)
				}
				if f.count(t) != 0 {
					t.Fatal("deployment observation minted a session")
				}
				copy := *proof
				if copy.Check(r) == nil || copy.Prefix() != "" {
					t.Fatal("copied applicability accepted")
				}
				var zero DeploymentWebDAVApplicability
				if zero.Check(r) == nil || json.Unmarshal([]byte("{}"), &zero) == nil || zero.Prefix() != "" || (*DeploymentWebDAVApplicability)(nil).Prefix() != "" {
					t.Fatal("imported applicability accepted")
				}
				proof.Close()
				if proof.Prefix() != "/webdav" || proof.Check(r) == nil {
					t.Fatal("closed prefix DATA was renewed into current applicability")
				}
				return
			}
			if proof != nil {
				proof.Close()
				t.Fatal("partial/unknown/active deployment accepted")
			}
			if name == "foreign-active-household" {
				if !errors.Is(err, ErrDeploymentWebDAVUnsupported) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrDeploymentWebDAVUnavailable) {
				t.Fatal(err)
			}
		})
	}
}

func TestDeploymentWebDAVCurrentFence(t *testing.T) {
	for _, name := range []string{"late-child-insert", "equal-update-physical-change", "delete-recreate-aba", "writer-started-before-capture-then-commit", "truncate-or-table-replacement", "native-attempt-or-digest-change"} {
		t.Run(name, func(t *testing.T) {
			f, issuer := deploymentWebDAVPG(t, nil, nil, false)
			r, _ := deploymentWebDAVRequest(t)
			if name == "writer-started-before-capture-then-commit" {
				tx, err := f.ds.pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				if _, err = tx.Exec(t.Context(), `UPDATE users SET name=name WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
				native, err := f.security.CaptureWebDAVBasicPolicy(r)
				if err != nil {
					t.Fatal(err)
				}
				if proof, e := issuer.Capture(r, native); !errors.Is(e, ErrDeploymentWebDAVBusy) || proof != nil {
					if proof != nil {
						proof.Close()
					}
					t.Fatalf("already-started writer bypassed actual late NOWAIT guard: %v", e)
				}
				if _, err = tx.Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
				if proof, e := issuer.Capture(r, native); !errors.Is(e, ErrDeploymentWebDAVUnsupported) || proof != nil {
					if proof != nil {
						proof.Close()
					}
					t.Fatalf("committed original writer became inactive snapshot: %v", e)
				}
				return
			}
			proof := deploymentWebDAVCapture(t, f, issuer, r)
			var originalFence int64
			if err := f.ds.pool.QueryRow(t.Context(), "SELECT last_value FROM regional_policy_revision_seq").Scan(&originalFence); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "late-child-insert":
				f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('late-child','other','Synthetic child',true)`)
			case "equal-update-physical-change":
				var before, after string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id='target'`).Scan(&before); err != nil {
					t.Fatal(err)
				}
				f.exec(t, `UPDATE users SET name=name WHERE id='target'`)
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id='target'`).Scan(&after); err != nil || before == after {
					t.Fatal("actual tuple did not change")
				}
			case "delete-recreate-aba":
				var before, after string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_object('id',id,'username',username,'password_hash',password_hash,'is_master',is_master,'max_streams',max_streams,'expires_at',expires_at,'created_at',created_at,'updated_at',updated_at)::text FROM accounts WHERE id='actor'`).Scan(&before); err != nil {
					t.Fatal(err)
				}
				// No users FK exists after migration012. Preserve all eight logical account
				// fields and the original profile while changing the actual account tuple.
				var row map[string]json.RawMessage
				if json.Unmarshal([]byte(before), &row) != nil {
					t.Fatal("account JSON")
				}
				var accountTuple, profileTuple, accountAfter, profileAfter string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM accounts a JOIN users u ON u.account_id=a.id WHERE a.id='actor' AND u.id='target'`).Scan(&accountTuple, &profileTuple); err != nil {
					t.Fatal(err)
				}
				f.exec(t, `DELETE FROM accounts WHERE id='actor'`)
				f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master,max_streams,expires_at,created_at,updated_at) SELECT id,username,password_hash,is_master,max_streams,expires_at,created_at,updated_at FROM jsonb_populate_record(NULL::accounts,$1::jsonb)`, before)
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_object('id',id,'username',username,'password_hash',password_hash,'is_master',is_master,'max_streams',max_streams,'expires_at',expires_at,'created_at',created_at,'updated_at',updated_at)::text FROM accounts WHERE id='actor'`).Scan(&after); err != nil || before != after {
					t.Fatal("ABA changed logical account values")
				}
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM accounts a JOIN users u ON u.account_id=a.id WHERE a.id='actor' AND u.id='target'`).Scan(&accountAfter, &profileAfter); err != nil || accountAfter == accountTuple || profileAfter != profileTuple {
					t.Fatal("account ABA did not isolate changed account and unchanged original profile")
				}
			case "truncate-or-table-replacement":
				f.exec(t, `TRUNCATE users CASCADE`)
			case "native-attempt-or-digest-change":
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			if name == "equal-update-physical-change" || name == "delete-recreate-aba" {
				f.exec(t, "SELECT setval('regional_policy_revision_seq',$1,true)", originalFence)
			}
			if err := proof.Check(r); err == nil {
				t.Fatal("current complete receipt adopted changed authority")
			}
			if proof.Context().Err() == nil {
				t.Fatal("changed receipt did not signal original source")
			}
			if name == "native-attempt-or-digest-change" {
				// Keep the equal-settings attempt invalidation above. Now change the
				// genuine native prefix while this same URL remains under both routes.
				current := deploymentWebDAVCapture(t, f, issuer, r)
				if current.Prefix() != "/webdav" {
					t.Fatal("fresh original prefix DATA mismatch")
				}
				f.settings.WebDAV.Prefix = "/"
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
				if current.Check(r) == nil || current.Prefix() != "/webdav" || proof.Prefix() != "/webdav" {
					t.Fatal("changed native prefix mutated or renewed original route DATA")
				}
				fresh := deploymentWebDAVCapture(t, f, issuer, r)
				if fresh.Prefix() != "/" || fresh.Check(r) != nil {
					t.Fatal("fresh capture substituted cached startup prefix")
				}
			}
		})
	}
}

func deploymentWebDAVPhysicalCleanup(t *testing.T) {
	t.Helper()
	var first atomic.Bool
	var mu sync.Mutex
	var actual *queryLifecycleGatedConn
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			gate := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			actual = gate
			mu.Unlock()
			return gate, nil
		}
		return conn, nil
	}
	f, issuer := deploymentWebDAVPG(t, nil, dial, false)
	r, _ := deploymentWebDAVRequest(t)
	proof := deploymentWebDAVCapture(t, f, issuer, r)
	mu.Lock()
	gate := actual
	mu.Unlock()
	if gate == nil {
		t.Fatal("actual primary socket missing")
	}
	t.Cleanup(gate.release)
	connection, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	physical := connection.Conn().PgConn()
	connection.Release()
	gate.armed.Store(true)
	checked := make(chan struct{})
	var checkErr error
	go func() { defer close(checked); checkErr = proof.Check(r) }()
	bridgeSessionAwait(t, gate.closeEntered)
	closed := make(chan struct{})
	go func() { defer close(closed); proof.Close() }()
	t.Cleanup(func() { gate.release(); bridgeSessionAwait(t, checked); bridgeSessionAwait(t, closed) })
	select {
	case <-physical.CleanupDone():
		t.Fatal("actual primary cleanup completed before held physical Close")
	default:
	}
	deploymentWebDAVPending(t, closed)
	deploymentWebDAVAdmission.Lock()
	_, retained := deploymentWebDAVAdmission.owners[proof]
	deploymentWebDAVAdmission.Unlock()
	if !retained {
		t.Fatal("source admission refunded before physical cleanup")
	}
	gate.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, checked)
	bridgeSessionAwait(t, closed)
	if checkErr == nil {
		t.Fatal("closed original socket preserved applicability")
	}
}
func TestDeploymentWebDAVActualLocks(t *testing.T) {
	for _, name := range []string{"held-query-caller-cancel", "actual-nowait-contention", "cross-store-settings-domain", "pool-one-no-held-transaction", "physical-cleanup-before-refund"} {
		t.Run(name, func(t *testing.T) {
			if name == "physical-cleanup-before-refund" {
				deploymentWebDAVPhysicalCleanup(t)
				return
			}
			var armed atomic.Bool
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			tracer := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, data pgx.TraceQueryEndData) {
				if data.Err == nil && strings.HasPrefix(sql, "WITH a AS MATERIALIZED(") && armed.CompareAndSwap(true, false) {
					close(entered)
					<-release
				}
			}}
			f, issuer := deploymentWebDAVPG(t, tracer, nil, name == "pool-one-no-held-transaction")
			r, cancel := deploymentWebDAVRequest(t)
			proof := deploymentWebDAVCapture(t, f, issuer, r)
			switch name {
			case "held-query-caller-cancel":
				checked, closed := make(chan struct{}), make(chan struct{})
				checkStarted, closeStarted := false, false
				var err error
				// Register before launch: a failed entry witness must release a
				// late QueryEnd before earlier receipt/fixture cleanup can wait.
				t.Cleanup(func() {
					cancel()
					unblock()
					if checkStarted {
						bridgeSessionAwait(t, checked)
					}
					if closeStarted {
						bridgeSessionAwait(t, closed)
					}
				})
				armed.Store(true)
				checkStarted = true
				go func() { defer close(checked); err = proof.Check(r) }()
				bridgeSessionAwait(t, entered)
				cancel()
				closeStarted = true
				go func() { defer close(closed); proof.Close() }()
				deploymentWebDAVPending(t, closed)
				deploymentWebDAVAdmission.Lock()
				_, retained := deploymentWebDAVAdmission.owners[proof]
				deploymentWebDAVAdmission.Unlock()
				if !retained {
					t.Fatal("admission refunded before actual original QueryEnd")
				}
				unblock()
				bridgeSessionAwait(t, checked)
				bridgeSessionAwait(t, closed)
				if err == nil {
					t.Fatal("cancelled actual query became permission")
				}
			case "actual-nowait-contention":
				tx, err := f.ds.pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				if _, err = tx.Exec(t.Context(), `UPDATE users SET name=name WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
				if err = proof.Check(r); !errors.Is(err, ErrDeploymentWebDAVBusy) {
					t.Fatalf("actual late relation conflict was not Busy: %v", err)
				}
			case "cross-store-settings-domain":
				foreign, err := New(t.Context(), f.settings.Database.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer foreign.Close()
				if other, e := NewDeploymentWebDAVIssuer(t.Context(), foreign, f.manager, f.security, f.coordinator); e == nil || other != nil {
					if other != nil {
						other.Close()
					}
					t.Fatal("foreign actual PG pool accepted original coordinator")
				}
			case "pool-one-no-held-transaction":
				if f.ds.pool.Config().MaxConns != 1 {
					t.Fatal("canary did not use pool one")
				}
				for i := 0; i < 3; i++ {
					if err := proof.Check(r); err != nil {
						t.Fatal(err)
					}
					ctx, stop := context.WithTimeout(t.Context(), time.Second)
					var value int
					err := f.ds.pool.QueryRow(ctx, "SELECT 42").Scan(&value)
					stop()
					if err != nil || value != 42 {
						t.Fatal("applicability retained an SQL transaction/native hold across media lifetime", err)
					}
				}
				guard, err := f.coordinator.TryWrite(t.Context())
				if err != nil {
					t.Fatal("bounded checks retained native hold", err)
				}
				_ = guard.Close()
			}
		})
	}
}
