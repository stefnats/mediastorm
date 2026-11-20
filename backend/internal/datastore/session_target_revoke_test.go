//go:build linux

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

func targetRevokeCall(ds *DataStore, ctx context.Context, kind, token string) (int64, error) {
	if kind == "client" {
		return ds.RevokeClientSessions(ctx, "synthetic-target-device")
	}
	return ds.RevokeSessionToken(ctx, token)
}
func targetRevokeFixture(t *testing.T, trace pgx.QueryTracer) *ratingPGFixture {
	t.Helper()
	f := familyScopedPG(t, trace)
	f.exec(t, `UPDATE sessions SET client_id='synthetic-target-device' WHERE token=$1`, f.session.Token)
	return f
}
func targetRevokeAwait(t *testing.T, done <-chan accountRevokeResult) accountRevokeResult {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-time.After(8 * time.Second):
		t.Fatal("actual target revoke did not join")
		return accountRevokeResult{}
	}
}

func TestSessionTargetRevokeExactStoredRows(t *testing.T) {
	for _, kind := range []string{"token", "empty-token", "padded-token", "client", "blank-client"} {
		t.Run(kind, func(t *testing.T) {
			f := targetRevokeFixture(t, nil)
			accountRevokeSeed(t, f, "", "actor")
			accountRevokeSeed(t, f, " padded ", "actor")
			accountRevokeSeed(t, f, "synthetic-foreign", "other")
			f.exec(t, `UPDATE sessions SET client_id='other-device' WHERE token<>$1`, f.session.Token)
			var count int64
			var err error
			want := int64(1)
			switch kind {
			case "token":
				count, err = f.ds.RevokeSessionToken(t.Context(), f.session.Token)
			case "empty-token":
				count, err = f.ds.RevokeSessionToken(t.Context(), "")
			case "padded-token":
				count, err = f.ds.RevokeSessionToken(t.Context(), " padded ")
			case "client":
				count, err = f.ds.RevokeClientSessions(t.Context(), " synthetic-target-device ")
			case "blank-client":
				count, err = f.ds.RevokeClientSessions(t.Context(), " \t ")
				want = 0
			}
			if err != nil || count != want || accountRevokeCount(t, f, "actor") != 3-int(want) || accountRevokeCount(t, f, "other") != 1 {
				t.Fatal("exact stored selector/count changed", err, count)
			}
		})
	}
}

func TestSessionTargetRevokeUnavailableBeforeWrite(t *testing.T) {
	for _, kind := range []string{"nil-store", "nil-context", "canceled", "missing-table"} {
		t.Run(kind, func(t *testing.T) {
			f := targetRevokeFixture(t, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ds := f.ds
			switch kind {
			case "nil-store":
				ds = nil
			case "nil-context":
				ctx = nil
			case "canceled":
				cancel()
			case "missing-table":
				f.exec(t, `ALTER TABLE sessions RENAME TO unavailable_target_sessions`)
				defer f.exec(t, `ALTER TABLE unavailable_target_sessions RENAME TO sessions`)
			}
			for _, mode := range []string{"token", "client"} {
				count, err := targetRevokeCall(ds, ctx, mode, f.session.Token)
				if count != 0 || err == nil || errors.Is(err, ErrSessionTargetRevokeUnknown) {
					t.Fatal("pre-COMMIT failure reported success/unknown", err, count)
				}
			}
		})
	}
}

func TestSessionTargetRevokeCanceledDeleteRollsBack(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var armed atomic.Bool
			var once sync.Once
			trace := &accountRevokeTracer{end: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
				if data.CommandTag.String() == "DELETE 1" && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			f := targetRevokeFixture(t, trace)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			t.Cleanup(func() { once.Do(func() { close(proceed) }) })
			armed.Store(true)
			done := make(chan accountRevokeResult, 1)
			go func() { n, e := targetRevokeCall(f.ds, ctx, kind, f.session.Token); done <- accountRevokeResult{n, e} }()
			bridgeCredentialWait(t, entered, "real DELETE never completed before cancellation")
			cancel()
			once.Do(func() { close(proceed) })
			got := targetRevokeAwait(t, done)
			if got.count != 0 || !errors.Is(got.err, context.Canceled) || accountRevokeCount(t, f, "actor") != 1 {
				t.Fatal("canceled uncommitted DELETE persisted", got.err, got.count)
			}
		})
	}
}

func TestSessionTargetRevokeActualCommitOutcomes(t *testing.T) {
	for _, kind := range []string{"known", "known-rollback", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var armed, dropped atomic.Bool
			trace := &accountRevokeTracer{start: func(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
				if kind == "known-rollback" && strings.EqualFold(data.SQL, "commit") && armed.CompareAndSwap(true, false) {
					_, _ = c.PgConn().Exec(ctx, "SELECT 1/0").ReadAll()
				}
			}, end: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
				if kind == "known" && data.Err == nil && data.CommandTag.String() == "COMMIT" && armed.CompareAndSwap(true, false) {
					cancel()
				}
			}}
			f := targetRevokeFixture(t, trace)
			ds := f.ds
			if kind == "unknown" {
				cfg := f.cfg.Copy()
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
				t.Cleanup(pool.Close)
				ds = &DataStore{pool: pool}
			}
			armed.Store(true)
			n, err := ds.RevokeSessionToken(ctx, f.session.Token)
			switch kind {
			case "known":
				if n != 1 || err != nil || ctx.Err() == nil || accountRevokeCount(t, f, "actor") != 0 {
					t.Fatal("known ACK was lost after caller departure", err, n)
				}
			case "known-rollback":
				if n != 0 || !errors.Is(err, ErrSessionTargetRevokeUnavailable) || errors.Is(err, ErrSessionTargetRevokeUnknown) || accountRevokeCount(t, f, "actor") != 1 {
					t.Fatal("actual server ROLLBACK was not known", err, n)
				}
			case "unknown":
				if n != 0 || !errors.Is(err, ErrSessionTargetRevokeUnknown) || !dropped.Load() || accountRevokeCount(t, f, "actor") != 0 {
					t.Fatal("actual lost ACK became success or rollback", err, n)
				}
			}
		})
	}
}

func TestSessionTargetRevokeBlockedQueryJoins(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := targetRevokeFixture(t, nil)
			var relation uint32
			var schema string
			if err := f.ds.pool.QueryRow(t.Context(), `SELECT c.oid, n.nspname
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.oid='sessions'::regclass`).Scan(&relation, &schema); err != nil {
				t.Fatal("actual relation namespace missing", err)
			}
			holder, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { rollbackPinTx(holder, t.Context()) })
			if _, err = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{schema, "sessions"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan accountRevokeResult, 1)
			go func() { n, e := targetRevokeCall(f.ds, ctx, kind, f.session.Token); done <- accountRevokeResult{n, e} }()
			familyScopedWaitPending(t, f, relation)
			cancel()
			got := targetRevokeAwait(t, done)
			if got.count != 0 || !errors.Is(got.err, context.Canceled) {
				t.Fatal("held cancellation acknowledged deletion", got.err, got.count)
			}
			accountRevokeNoPending(t, f, relation)
			if holder.Rollback(t.Context()) != nil || accountRevokeCount(t, f, "actor") != 1 {
				t.Fatal("canceled DELETE changed stored row")
			}
		})
	}
}

func TestSessionTargetRevokeFailedConnectionCleanup(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := targetRevokeFixture(t, nil)
			cfg := f.cfg.Copy()
			var first atomic.Bool
			var gateMu sync.Mutex
			var gate *queryLifecycleGatedConn
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				c, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				if first.CompareAndSwap(false, true) {
					v := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
					gateMu.Lock()
					gate = v
					gateMu.Unlock()
					return v, nil
				}
				return c, nil
			}
			pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(pool.Close)
			owned, e := pool.Acquire(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			physical := owned.Conn().PgConn()
			owned.Release()
			gateMu.Lock()
			actual := gate
			gateMu.Unlock()
			if actual == nil {
				t.Fatal("actual primary transport absent")
			}
			t.Cleanup(actual.release)
			actual.armed.Store(true)
			done := make(chan accountRevokeResult, 1)
			go func() {
				n, e := targetRevokeCall(&DataStore{pool: pool}, t.Context(), kind, f.session.Token)
				done <- accountRevokeResult{n, e}
			}()
			bridgeCredentialWait(t, actual.closeEntered, "failed actual primary socket Close not reached")
			if !physical.IsClosed() {
				t.Fatal("primary connection did not fail")
			}
			select {
			case <-physical.CleanupDone():
				t.Fatal("physical cleanup passed held close")
			case <-done:
				t.Fatal("revoke returned before physical cleanup")
			default:
			}
			actual.release()
			got := targetRevokeAwait(t, done)
			select {
			case <-physical.CleanupDone():
			default:
				t.Fatal("revoke returned before actual CleanupDone")
			}
			if got.count != 0 || got.err == nil || accountRevokeCount(t, f, "actor") != 1 {
				t.Fatal("failed BEGIN changed rows or count", got.err, got.count)
			}
		})
	}
}

func TestSessionTargetRevokeConcurrentCounts(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := targetRevokeFixture(t, nil)
			var wg sync.WaitGroup
			var count atomic.Int64
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					n, e := targetRevokeCall(f.ds, t.Context(), kind, f.session.Token)
					if e != nil {
						t.Error("actual revoke failed", e)
					}
					count.Add(n)
				}()
			}
			wg.Wait()
			if count.Load() != 1 || accountRevokeCount(t, f, "actor") != 0 {
				t.Fatal("parallel DELETE fabricated counts")
			}
		})
	}
}

func TestSessionTargetRevokeClientActualMintSnapshotOrder(t *testing.T) {
	for _, order := range []string{"before-snapshot", "uncommitted-at-snapshot", "after-delete"} {
		t.Run(order, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var armed atomic.Bool
			var sessionInsert atomic.Bool
			trace := &accountRevokeTracer{start: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) {
				if strings.HasPrefix(data.SQL, "INSERT INTO sessions(") {
					sessionInsert.Store(true)
				}
			}, end: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
				match := order == "uncommitted-at-snapshot" && strings.HasPrefix(data.CommandTag.String(), "INSERT ") && sessionInsert.CompareAndSwap(true, false) || order == "after-delete" && data.CommandTag.String() == "DELETE 1"
				if match && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			r := bridgeCredentialRequest(t.Context())
			_, proof, observation := f.evidence(t, r, bridgeCredentialAccount)
			t.Cleanup(func() { once.Do(func() { close(proceed) }) })
			mint := func() *BridgeLoginSessionReceipt {
				v, e := f.issuer.Commit(r, proof, observation)
				if e != nil {
					t.Fatal("actual Mint96fa failed", e)
				}
				t.Cleanup(v.Close)
				return v
			}
			if order == "before-snapshot" {
				receipt := mint()
				session, e := f.issuer.Release(r, receipt)
				if e != nil {
					t.Fatal(e)
				}
				receipt.Close()
				n, e := f.ds.RevokeClientSessions(t.Context(), "synthetic-device")
				row, re := f.ds.Sessions().Get(t.Context(), session.Token)
				if e != nil || n != 1 || re != nil || row != nil {
					t.Fatal("DELETE missed earlier committed Mint", e, n)
				}
				return
			}
			if order == "uncommitted-at-snapshot" {
				type mintResult struct {
					v *BridgeLoginSessionReceipt
					e error
				}
				done := make(chan mintResult, 1)
				armed.Store(true)
				go func() { v, e := f.issuer.Commit(r, proof, observation); done <- mintResult{v, e} }()
				bridgeSessionAwait(t, entered)
				n, e := f.ds.RevokeClientSessions(t.Context(), "synthetic-device")
				if e != nil || n != 0 {
					t.Fatal("DELETE adopted an invisible uncommitted Mint", e, n)
				}
				select {
				case <-done:
					t.Fatal("Mint was not actually held before commit")
				default:
				}
				once.Do(func() { close(proceed) })
				var result mintResult
				select {
				case result = <-done:
				case <-time.After(8 * time.Second):
					t.Fatal("actual Mint did not finish")
				}
				if result.e != nil {
					t.Fatal(result.e)
				}
				t.Cleanup(result.v.Close)
				session, e := f.issuer.Release(r, result.v)
				if e != nil {
					t.Fatal(e)
				}
				result.v.Close()
				row, re := f.ds.Sessions().Get(t.Context(), session.Token)
				if re != nil || row == nil {
					t.Fatal("overlapping fresh password login was forbidden", re)
				}
				return
			}
			f.exec(t, `INSERT INTO sessions(token,account_id,client_id,expires_at,created_at)VALUES('synthetic-prior','actor','synthetic-device',now()+interval '1 hour',now())`)
			done := make(chan accountRevokeResult, 1)
			armed.Store(true)
			go func() {
				n, e := f.ds.RevokeClientSessions(t.Context(), "synthetic-device")
				done <- accountRevokeResult{n, e}
			}()
			bridgeSessionAwait(t, entered)
			receipt := mint()
			session, e := f.issuer.Release(r, receipt)
			if e != nil {
				t.Fatal(e)
			}
			receipt.Close()
			once.Do(func() { close(proceed) })
			got := targetRevokeAwait(t, done)
			row, re := f.ds.Sessions().Get(t.Context(), session.Token)
			old, oe := f.ds.Sessions().Get(t.Context(), "synthetic-prior")
			if got.err != nil || got.count != 1 || re != nil || row == nil || oe != nil || old != nil {
				t.Fatal("later actual login or original DELETE snapshot changed", got.err, got.count)
			}
		})
	}
}
