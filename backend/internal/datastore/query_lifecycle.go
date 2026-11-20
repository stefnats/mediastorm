package datastore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

const queryCancelFallback = 5 * time.Second

func configureQueryLifecycle(config *pgx.ConnConfig) {
	if config == nil {
		return
	}
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		// Let a reachable PostgreSQL report its canceled query and ReadyForQuery
		// before returning. The socket deadline remains a bounded fallback.
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, CancelRequestDelay: 0, DeadlineDelay: queryCancelFallback}
	}
	if config.Tracer == nil {
		config.Tracer = queryLifecycleTracer{}
	} else {
		// Keep existing query, prepare, connect, batch/copy and pool hooks.
		config.Tracer = multitracer.New(config.Tracer, queryLifecycleTracer{})
	}
}

type queryLifecycleTracer struct{}

func (queryLifecycleTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (queryLifecycleTracer) TraceQueryEnd(_ context.Context, conn *pgx.Conn, _ pgx.TraceQueryEndData) {
	if conn == nil || conn.PgConn() == nil || !conn.PgConn().IsClosed() {
		return
	}
	// pgx may return a failed query before its asynchronous physical cleanup.
	// Join only that already-closed query connection, never a connect/handshake
	// callback or a healthy connection whose CleanupDone stays open until Close.
	// This proves local cleanup, not remote ReadyForQuery after network failure.
	<-conn.PgConn().CleanupDone()
}
