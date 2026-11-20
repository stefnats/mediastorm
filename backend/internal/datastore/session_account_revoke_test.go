package datastore

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type accountRevokeResult struct {
	count int64
	err   error
}

type accountRevokeTracer struct {
	start func(context.Context, *pgx.Conn, pgx.TraceQueryStartData)
	end   func(context.Context, *pgx.Conn, pgx.TraceQueryEndData)
}

func (tr *accountRevokeTracer) TraceQueryStart(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if tr.start != nil {
		tr.start(ctx, c, data)
	}
	return ctx
}
func (tr *accountRevokeTracer) TraceQueryEnd(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryEndData) {
	if tr.end != nil {
		tr.end(ctx, c, data)
	}
}
func accountRevokeSeed(t *testing.T, f *ratingPGFixture, token, account string) {
	t.Helper()
	f.exec(t, `INSERT INTO sessions(token,account_id,expires_at,created_at,scope,scope_resource,client_id) VALUES($1,$2,clock_timestamp()+interval '1 hour',clock_timestamp(),'jellyfin','target','synthetic-device')`, token, account)
}
func accountRevokeCount(t *testing.T, f *ratingPGFixture, account string) int {
	t.Helper()
	var n int
	if err := f.ds.pool.QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE account_id=$1", account).Scan(&n); err != nil {
		t.Fatal("count actual synthetic sessions", err)
	}
	return n
}
func accountRevokeAwait(t *testing.T, result <-chan accountRevokeResult) accountRevokeResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(4 * time.Second):
		t.Fatal("actual revoke did not finish and join its PG work")
		return accountRevokeResult{}
	}
}
func accountRevokeNoPending(t *testing.T, f *ratingPGFixture, relation uint32) {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&n); err != nil || n != 0 {
		t.Fatal("revoke returned before actual query cancel/drain", err, n)
	}
}

func accountRevokeWaitBlocked(t *testing.T, f *ratingPGFixture, waiter, holder uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var blocked bool
		if err := f.admin.QueryRow(t.Context(), "SELECT $1::integer=ANY(pg_blocking_pids($2::integer))", int32(holder), int32(waiter)).Scan(&blocked); err != nil {
			t.Fatal("observe exact owned account-row lock wait", err)
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("exact current query never waited for original account holder")
		}
		time.Sleep(time.Millisecond)
	}
}
func accountRevokeWaitPID(t *testing.T, ready <-chan uint32) uint32 {
	t.Helper()
	select {
	case pid := <-ready:
		return pid
	case <-time.After(3 * time.Second):
		t.Fatal("actual PG query PID not observed")
		return 0
	}
}

func TestAccountSessionRevokeActualPGCountAndExactKeep(t *testing.T) {
	for _, mode := range []string{"all", "except", "empty keep", "missing account"} {
		t.Run(mode, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			accountRevokeSeed(t, f, "", "actor")
			accountRevokeSeed(t, f, "synthetic-extra", "actor")
			accountRevokeSeed(t, f, "synthetic-foreign", "other")
			var n int64
			var err error
			switch mode {
			case "all":
				n, err = f.ds.RevokeAccountSessions(t.Context(), "actor")
			case "except":
				n, err = f.ds.RevokeAccountSessionsExcept(t.Context(), "actor", f.session.Token)
			case "empty keep":
				n, err = f.ds.RevokeAccountSessionsExcept(t.Context(), "actor", "")
			case "missing account":
				n, err = f.ds.RevokeAccountSessions(t.Context(), "missing")
			}
			want := int64(2)
			if mode == "all" {
				want = 3
			} else if mode == "missing account" {
				want = 0
			}
			if err != nil || n != want || accountRevokeCount(t, f, "actor") != 3-int(want) || accountRevokeCount(t, f, "other") != 1 {
				t.Fatal("PG DELETE count, exact keep, or account isolation changed", err, n)
			}
			if mode == "except" || mode == "empty keep" {
				keep := f.session.Token
				if mode == "empty keep" {
					keep = ""
				}
				row, err := f.ds.Sessions().Get(t.Context(), keep)
				if err != nil || row == nil {
					t.Fatal("exact kept row was removed", err)
				}
			}
		})
	}
}

func TestAccountSessionRevokeExceptKeepsOriginalFrameAndCredentialProof(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	original := familyScopedCapture(t, f, r)
	credentialRequest := bridgeCredentialRequest(t.Context())
	intent := bridgeCredentialPrepare(t, f, credentialRequest)
	credentials, err := f.ds.VerifyBridgeAccountPassword(credentialRequest, intent, parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	accountRevokeSeed(t, f, "synthetic-other-device", "actor")
	var before string
	if err = f.ds.pool.QueryRow(t.Context(), "SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM accounts WHERE id='actor'").Scan(&before); err != nil {
		t.Fatal(err)
	}
	kept, err := f.ds.Sessions().Get(t.Context(), f.session.Token)
	if err != nil || kept == nil {
		t.Fatal("read actual kept session", err)
	}
	n, err := f.ds.RevokeAccountSessionsExcept(t.Context(), "actor", f.session.Token)
	var after string
	if readErr := f.ds.pool.QueryRow(t.Context(), "SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM accounts WHERE id='actor'").Scan(&after); readErr != nil {
		t.Fatal(readErr)
	}
	current, readErr := f.ds.Sessions().Get(t.Context(), f.session.Token)
	if err != nil || n != 1 || before != after || readErr != nil || current == nil || *current != *kept || original.Check(t.Context()) != nil || credentials.Check(t.Context()) != nil || f.ds.CheckProfilePinRequestFrame(r) != nil {
		t.Fatal("existing-session revoke changed kept frame, password evidence or account revision", err, n)
	}
}

func TestAccountSessionRevokeActualAccountLockCommitOrders(t *testing.T) {
	for _, order := range []string{"insert first", "revoke first"} {
		t.Run(order, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			if order == "insert first" {
				holder, err := f.ds.pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer rollbackPinTx(holder, t.Context())
				var id string
				if err = holder.QueryRow(t.Context(), "SELECT id FROM accounts WHERE id='actor' FOR UPDATE").Scan(&id); err != nil {
					t.Fatal(err)
				}
				if _, err = holder.Exec(t.Context(), `INSERT INTO sessions(token,account_id,expires_at,created_at)VALUES('synthetic-before','actor',clock_timestamp()+interval '1 hour',clock_timestamp())`); err != nil {
					t.Fatal(err)
				}
				ready := make(chan uint32, 1)
				trace := &accountRevokeTracer{start: func(_ context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
					if data.SQL == "SELECT id FROM accounts WHERE id=$1 FOR UPDATE" {
						ready <- c.PgConn().PID()
					}
				}}
				cfg := f.cfg.Copy()
				cfg.ConnConfig.Tracer = trace
				configureQueryLifecycle(cfg.ConnConfig)
				pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { rollbackPinTx(holder, t.Context()); pool.Close() }()
				ds := &DataStore{pool: pool}
				done := make(chan accountRevokeResult, 1)
				go func() { n, e := ds.RevokeAccountSessions(t.Context(), "actor"); done <- accountRevokeResult{n, e} }()
				accountRevokeWaitBlocked(t, f, accountRevokeWaitPID(t, ready), holder.Conn().PgConn().PID())
				if err = holder.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
				got := accountRevokeAwait(t, done)
				if got.err != nil || got.count != 2 || accountRevokeCount(t, f, "actor") != 0 {
					t.Fatal("revoke serialized after INSERT missed actual committed session", got.err, got.count)
				}
				return
			}
			entered, proceed := make(chan struct{}), make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(proceed) })
			var armed atomic.Bool
			var holderPID atomic.Uint32
			trace := &accountRevokeTracer{end: func(_ context.Context, c *pgx.Conn, data pgx.TraceQueryEndData) {
				if data.Err == nil && data.CommandTag.String() == "SELECT 1" && armed.CompareAndSwap(true, false) {
					holderPID.Store(c.PgConn().PID())
					close(entered)
					<-proceed
				}
			}}
			cfg := f.cfg.Copy()
			cfg.ConnConfig.Tracer = trace
			configureQueryLifecycle(cfg.ConnConfig)
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { release.Do(func() { close(proceed) }); pool.Close() }()
			ds := &DataStore{pool: pool}
			armed.Store(true)
			done := make(chan accountRevokeResult, 1)
			go func() { n, e := ds.RevokeAccountSessions(t.Context(), "actor"); done <- accountRevokeResult{n, e} }()
			bridgeCredentialWait(t, entered, "revoke did not hold genuine selected account lock")
			inserted := make(chan error, 1)
			ready := make(chan uint32, 1)
			go func() {
				tx, e := f.ds.pool.Begin(t.Context())
				if e != nil {
					inserted <- e
					return
				}
				defer rollbackPinTx(tx, t.Context())
				var id string
				ready <- tx.Conn().PgConn().PID()
				e = tx.QueryRow(t.Context(), "SELECT id FROM accounts WHERE id='actor' FOR UPDATE").Scan(&id)
				if e == nil {
					_, e = tx.Exec(t.Context(), `INSERT INTO sessions(token,account_id,expires_at,created_at)VALUES('synthetic-after','actor',clock_timestamp()+interval '1 hour',clock_timestamp())`)
				}
				if e == nil {
					e = tx.Commit(t.Context())
				}
				inserted <- e
			}()
			accountRevokeWaitBlocked(t, f, accountRevokeWaitPID(t, ready), holderPID.Load())
			release.Do(func() { close(proceed) })
			got := accountRevokeAwait(t, done)
			select {
			case err = <-inserted:
			case <-time.After(4 * time.Second):
				t.Fatal("serialized later INSERT did not complete")
			}
			if err != nil || got.err != nil || got.count != 1 || accountRevokeCount(t, f, "actor") != 1 {
				t.Fatal("revoke incorrectly forbade later fresh-session INSERT", err, got.err, got.count)
			}
		})
	}
}

func TestAccountSessionRevokeCancellationJoinsActualHeldPG(t *testing.T) {
	for _, relationName := range []string{"accounts", "sessions"} {
		t.Run(relationName, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			var relation uint32
			if err := f.ds.pool.QueryRow(t.Context(), "SELECT $1::regclass::oid", relationName).Scan(&relation); err != nil {
				t.Fatal(err)
			}
			holder, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackPinTx(holder, t.Context())
			if _, err = holder.Exec(t.Context(), "LOCK TABLE "+relationName+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan accountRevokeResult, 1)
			go func() { n, e := f.ds.RevokeAccountSessions(ctx, "actor"); done <- accountRevokeResult{n, e} }()
			familyScopedWaitPending(t, f, relation)
			cancel()
			got := accountRevokeAwait(t, done)
			if got.count != 0 || !errors.Is(got.err, context.Canceled) {
				t.Fatal("canceled held query reported a committed revoke", got.err, got.count)
			}
			accountRevokeNoPending(t, f, relation)
			if err = holder.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			if accountRevokeCount(t, f, "actor") != 1 {
				t.Fatal("cancel before COMMIT changed stored sessions")
			}
		})
	}
}

func TestAccountSessionRevokeCancellationAfterActualDeleteRollsBack(t *testing.T) {
	deleted, proceed := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(proceed) })
	trace := &accountRevokeTracer{end: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
		if data.CommandTag.String() == "DELETE 1" {
			close(deleted)
			<-proceed
		}
	}}
	f := familyScopedPG(t, trace)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan accountRevokeResult, 1)
	go func() { n, e := f.ds.RevokeAccountSessions(ctx, "actor"); done <- accountRevokeResult{n, e} }()
	bridgeCredentialWait(t, deleted, "actual DELETE never ran before the cancellation canary")
	cancel()
	release.Do(func() { close(proceed) })
	got := accountRevokeAwait(t, done)
	if got.count != 0 || !errors.Is(got.err, context.Canceled) || accountRevokeCount(t, f, "actor") != 1 {
		t.Fatal("actual uncommitted DELETE survived cancellation/rollback", got.err, got.count)
	}
}

func TestAccountSessionRevokeUnavailableAndAlreadyCancelledDoNotWrite(t *testing.T) {
	for _, phase := range []string{"already cancelled", "accounts unavailable", "sessions unavailable"} {
		t.Run(phase, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if phase == "already cancelled" {
				cancel()
			} else {
				table := strings.Fields(phase)[0]
				f.exec(t, "ALTER TABLE "+table+" RENAME TO unavailable_account_revoke")
				defer f.exec(t, "ALTER TABLE unavailable_account_revoke RENAME TO "+table)
			}
			n, err := f.ds.RevokeAccountSessions(ctx, "actor")
			if n != 0 || err == nil || errors.Is(err, ErrSessionAccountRevokeUnknown) {
				t.Fatal("pre-COMMIT unavailable/canceled work was acknowledged", err, n)
			}
		})
	}
}

func TestAccountSessionRevokeActualServerCommitRollbackIsKnown(t *testing.T) {
	var armed, aborted atomic.Bool
	trace := &accountRevokeTracer{start: func(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
		if strings.EqualFold(data.SQL, "commit") && armed.CompareAndSwap(true, false) {
			_, err := c.PgConn().Exec(ctx, "SELECT 1/0").ReadAll()
			aborted.Store(err != nil)
		}
	}}
	f := familyScopedPG(t, trace)
	armed.Store(true)
	n, err := f.ds.RevokeAccountSessions(t.Context(), "actor")
	if n != 0 || !errors.Is(err, ErrSessionAccountRevokeUnavailable) || errors.Is(err, ErrSessionAccountRevokeUnknown) || !aborted.Load() || accountRevokeCount(t, f, "actor") != 1 {
		t.Fatal("server's actual ROLLBACK was misreported as commit or unknown", err, n)
	}
}

func TestAccountSessionRevokeActualLostCommitAcknowledgementMayDelete(t *testing.T) {
	f := familyScopedPG(t, nil)
	cfg := f.cfg.Copy()
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &parentDropCommit{Conn: c, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	n, err := ds.RevokeAccountSessions(t.Context(), "actor")
	if n != 0 || !errors.Is(err, ErrSessionAccountRevokeUnknown) || !dropped.Load() || accountRevokeCount(t, f, "actor") != 0 {
		t.Fatal("lost actual COMMIT ACK claimed rollback/successful count", err, n)
	}
}

func TestAccountSessionRevokeJoinsActualFailedConnectionCleanup(t *testing.T) {
	f := familyScopedPG(t, nil)
	cfg := f.cfg.Copy()
	var first atomic.Bool
	var gate *queryLifecycleGatedConn
	var gateMu sync.Mutex
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			actual := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			gateMu.Lock()
			gate = actual
			gateMu.Unlock()
			return actual, nil
		}
		return c, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owned, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pg := owned.Conn().PgConn()
	owned.Release()
	gateMu.Lock()
	actual := gate
	gateMu.Unlock()
	if actual == nil {
		t.Fatal("genuine primary socket was not captured")
	}
	defer actual.release()
	actual.armed.Store(true)
	ds := &DataStore{pool: pool}
	done := make(chan accountRevokeResult, 1)
	go func() { n, e := ds.RevokeAccountSessions(t.Context(), "actor"); done <- accountRevokeResult{n, e} }()
	bridgeCredentialWait(t, actual.closeEntered, "actual failed BEGIN query never reached physical socket close")
	if !pg.IsClosed() {
		t.Fatal("primary query connection did not actually fail")
	}
	select {
	case <-pg.CleanupDone():
		t.Fatal("driver cleanup completed while actual close remains held")
	case <-done:
		t.Fatal("revoke returned before actual CleanupDone")
	default:
	}
	actual.release()
	got := accountRevokeAwait(t, done)
	select {
	case <-pg.CleanupDone():
	default:
		t.Fatal("actual cleanup remained outstanding after return")
	}
	if got.count != 0 || got.err == nil || accountRevokeCount(t, f, "actor") != 1 {
		t.Fatal("failed pre-COMMIT connection acknowledged or changed sessions", got.err, got.count)
	}
}

func TestAccountSessionRevokeConcurrentCountsOnlyCommittedDeletedRows(t *testing.T) {
	f := familyScopedPG(t, nil)
	accountRevokeSeed(t, f, "synthetic-second", "actor")
	accountRevokeSeed(t, f, "synthetic-foreign", "other")
	var wait sync.WaitGroup
	var total atomic.Int64
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			n, err := f.ds.RevokeAccountSessions(t.Context(), "actor")
			if err != nil {
				t.Error("serialized revoke failed", err)
			}
			total.Add(n)
		}()
	}
	wait.Wait()
	if total.Load() != 2 || accountRevokeCount(t, f, "actor") != 0 || accountRevokeCount(t, f, "other") != 1 {
		t.Fatal("parallel revokes duplicated counts or changed another account")
	}
}
