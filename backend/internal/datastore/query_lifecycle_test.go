package datastore

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type queryLifecycleTraceKey string
type queryLifecycleExistingTrace struct{ events []string }

func (q *queryLifecycleExistingTrace) start(ctx context.Context, name string) context.Context {
	q.events = append(q.events, name+"-start")
	return context.WithValue(ctx, queryLifecycleTraceKey(name), true)
}
func (q *queryLifecycleExistingTrace) end(ctx context.Context, name string) {
	if ctx.Value(queryLifecycleTraceKey(name)) != true {
		panic("existing trace context was replaced")
	}
	q.events = append(q.events, name+"-end")
}
func (q *queryLifecycleExistingTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return q.start(ctx, "query")
}
func (q *queryLifecycleExistingTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	q.end(ctx, "query")
}
func (q *queryLifecycleExistingTrace) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	return q.start(ctx, "prepare")
}
func (q *queryLifecycleExistingTrace) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareEndData) {
	q.end(ctx, "prepare")
}
func (q *queryLifecycleExistingTrace) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return q.start(ctx, "connect")
}
func (q *queryLifecycleExistingTrace) TraceConnectEnd(ctx context.Context, _ pgx.TraceConnectEndData) {
	q.end(ctx, "connect")
}
func (q *queryLifecycleExistingTrace) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return q.start(ctx, "batch")
}
func (q *queryLifecycleExistingTrace) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchQueryData) {
	if ctx.Value(queryLifecycleTraceKey("batch")) != true {
		panic("batch context replaced")
	}
	q.events = append(q.events, "batch-query")
}
func (q *queryLifecycleExistingTrace) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
	q.end(ctx, "batch")
}
func (q *queryLifecycleExistingTrace) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return q.start(ctx, "copy")
}
func (q *queryLifecycleExistingTrace) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromEndData) {
	q.end(ctx, "copy")
}
func (q *queryLifecycleExistingTrace) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return q.start(ctx, "acquire")
}
func (q *queryLifecycleExistingTrace) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireEndData) {
	q.end(ctx, "acquire")
}
func (q *queryLifecycleExistingTrace) TraceRelease(_ *pgxpool.Pool, _ pgxpool.TraceReleaseData) {
	q.events = append(q.events, "release")
}

func TestQueryLifecycleConfigurationPreservesExistingTraceInterfacesAndContexts(t *testing.T) {
	config, err := pgx.ParseConfig("postgresql://synthetic_unused@/synthetic_unused?host=/synthetic-unused")
	if err != nil {
		t.Fatal(err)
	}
	existing := &queryLifecycleExistingTrace{}
	config.Tracer = existing
	configureQueryLifecycle(config)
	handler, ok := config.BuildContextWatcherHandler(nil).(*pgconn.CancelRequestContextWatcherHandler)
	if !ok || handler.CancelRequestDelay != 0 || handler.DeadlineDelay != 5*time.Second {
		t.Fatal("query cancellation/drain policy not configured")
	}
	ctx := config.Tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{})
	config.Tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	prepare := config.Tracer.(pgx.PrepareTracer)
	ctx = prepare.TracePrepareStart(t.Context(), nil, pgx.TracePrepareStartData{})
	prepare.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})
	connect := config.Tracer.(pgx.ConnectTracer)
	ctx = connect.TraceConnectStart(t.Context(), pgx.TraceConnectStartData{})
	connect.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})
	batch := config.Tracer.(pgx.BatchTracer)
	ctx = batch.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	batch.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{})
	batch.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})
	copyTrace := config.Tracer.(pgx.CopyFromTracer)
	ctx = copyTrace.TraceCopyFromStart(t.Context(), nil, pgx.TraceCopyFromStartData{})
	copyTrace.TraceCopyFromEnd(ctx, nil, pgx.TraceCopyFromEndData{})
	acquire := config.Tracer.(pgxpool.AcquireTracer)
	ctx = acquire.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
	acquire.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
	config.Tracer.(pgxpool.ReleaseTracer).TraceRelease(nil, pgxpool.TraceReleaseData{})
	want := []string{"query-start", "query-end", "prepare-start", "prepare-end", "connect-start", "connect-end", "batch-start", "batch-query", "batch-end", "copy-start", "copy-end", "acquire-start", "acquire-end", "release"}
	if !reflect.DeepEqual(existing.events, want) {
		t.Fatal("existing tracer hooks were removed/reordered", existing.events)
	}
}

func TestQueryLifecycleDoesNotJoinHandshakeOrUnavailableQueryConnection(t *testing.T) {
	configureQueryLifecycle(nil)
	config, err := pgx.ParseConfig("postgresql://synthetic_unused@/synthetic_unused?host=/synthetic-unused")
	if err != nil {
		t.Fatal(err)
	}
	configureQueryLifecycle(config)
	if _, ok := config.Tracer.(pgx.ConnectTracer); ok {
		t.Fatal("query cleanup policy installed a handshake callback")
	}
	if _, ok := config.Tracer.(pgx.PrepareTracer); ok {
		t.Fatal("query cleanup policy installed a separate prepare lifecycle")
	}
	queryLifecycleTracer{}.TraceQueryEnd(t.Context(), nil, pgx.TraceQueryEndData{})
	queryLifecycleTracer{}.TraceQueryEnd(t.Context(), &pgx.Conn{}, pgx.TraceQueryEndData{})
}

func queryLifecycleWait(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}

func TestQueryLifecycleActualNewStoreNormalConnectionAndExplicitPrepareRemainReusable(t *testing.T) {
	f := newSettingsDS(t)
	conn, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pg := conn.Conn().PgConn()
	pid := pg.PID()
	handler, ok := conn.Conn().Config().BuildContextWatcherHandler(pg).(*pgconn.CancelRequestContextWatcherHandler)
	if !ok || handler.Conn != pg || handler.CancelRequestDelay != 0 || handler.DeadlineDelay != queryCancelFallback {
		t.Fatal("New store did not install actual connection policy")
	}
	if _, err = conn.Conn().Prepare(t.Context(), "synthetic_lifecycle_prepare", "SELECT $1::int"); err != nil {
		t.Fatal("ordinary explicit prepare failed", err)
	}
	for i := 1; i <= 3; i++ {
		var got int
		if err = conn.QueryRow(t.Context(), "synthetic_lifecycle_prepare", i).Scan(&got); err != nil || got != i {
			t.Fatal("ordinary prepared query changed", err)
		}
	}
	if pg.IsClosed() || pg.IsBusy() || pg.TxStatus() != 'I' || pg.PID() != pid {
		t.Fatal("normal query destroyed or replaced actual connection")
	}
	select {
	case <-pg.CleanupDone():
		t.Fatal("healthy connection was incorrectly cleaned up")
	default:
	}
}

func TestQueryLifecycleActualLockedQueryCancelsAndDrainsBeforeConnectionReuse(t *testing.T) {
	f := newSettingsDS(t)
	f.exec(t, "CREATE TABLE query_lifecycle_probe(id integer PRIMARY KEY)")
	conn, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pg := conn.Conn().PgConn()
	pid := pg.PID()
	if _, err = conn.Conn().Prepare(t.Context(), "synthetic_lifecycle_locked", "SELECT count(*) FROM query_lifecycle_probe WHERE id=$1"); err != nil {
		t.Fatal(err)
	}
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE query_lifecycle_probe IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	returned := make(chan struct{})
	var queryErr error
	go func() {
		defer close(returned)
		var n int
		queryErr = conn.QueryRow(ctx, "synthetic_lifecycle_locked", 1).Scan(&n)
	}()
	t.Cleanup(func() {
		cancel()
		_ = tx.Rollback(context.Background())
		queryLifecycleWait(t, returned, "actual query survived fixture cleanup")
	})
	pending := func() bool {
		t.Helper()
		var wait bool
		if e := f.ds.pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND relation='query_lifecycle_probe'::regclass AND NOT granted)`, pid).Scan(&wait); e != nil {
			t.Fatal("observe exact original query lock", e)
		}
		return wait
	}
	until := time.Now().Add(3 * time.Second)
	for !pending() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !pending() {
		t.Fatal("actual original query never blocked on held relation")
	}
	cancel()
	queryLifecycleWait(t, returned, "actual query cancellation failed to drain server reply")
	var pgErr *pgconn.PgError
	if !errors.As(queryErr, &pgErr) || pgErr.Code != "57014" {
		t.Fatal("normal cancellation did not receive actual PostgreSQL ErrorResponse", queryErr)
	}
	if pending() {
		t.Fatal("query returned while original backend still held pending lock")
	}
	if pg.IsClosed() || pg.IsBusy() || pg.TxStatus() != 'I' {
		t.Fatal("normal canceled query did not finish ReadyForQuery on same connection")
	}
	var value int
	if err = conn.QueryRow(t.Context(), "SELECT 42").Scan(&value); err != nil || value != 42 || pg.PID() != pid {
		t.Fatal("drained actual query connection could not be reused", err)
	}
}

type queryLifecycleGatedConn struct {
	net.Conn
	armed                   atomic.Bool
	closeEntered, closeGate chan struct{}
	enterOnce, gateOnce     sync.Once
}

func (c *queryLifecycleGatedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.armed.Load() {
		clear(p[:n])
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}
func (c *queryLifecycleGatedConn) Close() error {
	if c.armed.Load() {
		c.enterOnce.Do(func() { close(c.closeEntered) })
		<-c.closeGate
	}
	return c.Conn.Close()
}
func (c *queryLifecycleGatedConn) release() { c.gateOnce.Do(func() { close(c.closeGate) }) }

func TestQueryLifecycleActualClosedQueryJoinsOwnedPhysicalCleanupBeforeReturning(t *testing.T) {
	f := newSettingsDS(t)
	config := f.ds.pool.Config().ConnConfig.Copy()
	dial := config.DialFunc
	var first atomic.Bool
	var actual *queryLifecycleGatedConn
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			actual = &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			return actual, nil
		}
		// The driver's cancellation connection is real and never gated.
		return conn, nil
	}
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatal("connect actual owned synthetic socket", err)
	}
	if actual == nil {
		t.Fatal("actual query connection was not observed")
	}
	t.Cleanup(func() { actual.release(); _ = conn.Close(context.Background()) })
	pg := conn.PgConn()
	actual.armed.Store(true)
	returned := make(chan struct{})
	var queryErr error
	go func() { defer close(returned); var n int; queryErr = conn.QueryRow(t.Context(), "SELECT 1").Scan(&n) }()
	t.Cleanup(func() {
		actual.release()
		queryLifecycleWait(t, returned, "closed query survived owned cleanup release")
	})
	queryLifecycleWait(t, actual.closeEntered, "actual query failure did not enter physical net cleanup")
	if !pg.IsClosed() {
		t.Fatal("query cleanup witness was not a closed actual connection")
	}
	select {
	case <-pg.CleanupDone():
		t.Fatal("driver reported cleanup before actual net Close returned")
	default:
	}
	select {
	case <-returned:
		t.Fatal("query-end hook returned before owned physical cleanup")
	default:
	}
	actual.release()
	queryLifecycleWait(t, returned, "actual query did not return after physical cleanup joined")
	if queryErr == nil {
		t.Fatal("network-failed original query unexpectedly succeeded")
	}
	select {
	case <-pg.CleanupDone():
	default:
		t.Fatal("query returned without actual CleanupDone")
	}
}
