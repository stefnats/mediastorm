package datastore

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrSessionTargetRevokeUnavailable = errors.New("session target revocation unavailable")
	ErrSessionTargetRevokeUnknown     = errors.New("session target revocation commit outcome unknown")
)

const sessionTargetRevokeBudget = 5 * time.Second

// RevokeSessionToken deletes the exact stored token, including a legacy empty
// token. The caller supplies an already authorized selector, never a cache row.
func (ds *DataStore) RevokeSessionToken(ctx context.Context, token string) (int64, error) {
	return ds.revokeSessionTarget(ctx, token, false)
}

// RevokeClientSessions preserves the service's trimmed, nonempty device selector.
// It revokes currently visible sessions, not credentials or future logins.
func (ds *DataStore) RevokeClientSessions(ctx context.Context, clientID string) (int64, error) {
	return ds.revokeSessionTarget(ctx, clientID, true)
}

func (ds *DataStore) revokeSessionTarget(caller context.Context, selector string, client bool) (int64, error) {
	if caller == nil || ds == nil || ds.pool == nil {
		return 0, ErrSessionTargetRevokeUnavailable
	}
	if err := caller.Err(); err != nil {
		return 0, err
	}
	if client {
		selector = strings.TrimSpace(selector)
		if selector == "" {
			return 0, nil
		}
	}
	ctx, cancel := context.WithTimeout(caller, sessionTargetRevokeBudget)
	defer cancel()
	connection, err := ds.pool.Acquire(ctx)
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionTargetRevokeUnavailable)
	}
	// Actual query/rollback work and a failed connection's asynchronous physical
	// cleanup end before the original connection is returned to its pool.
	defer func() {
		physical := connection.Conn().PgConn()
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
	}()
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionTargetRevokeUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	statement := `DELETE FROM sessions WHERE token=$1`
	if client {
		statement = `DELETE FROM sessions WHERE client_id=$1`
	}
	// The normal DELETE statement snapshot determines which concurrent Mint
	// INSERTs are visible. Later/overlapping genuine logins are not forbidden.
	tag, err := tx.Exec(ctx, statement, selector)
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionTargetRevokeUnavailable)
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return 0, ErrSessionTargetRevokeUnavailable
		}
		// The server may have committed. Do not retry or publish a success count.
		return 0, ErrSessionTargetRevokeUnknown
	}
	// A known successful ACK remains known after subsequent caller cancellation.
	return tag.RowsAffected(), nil
}
