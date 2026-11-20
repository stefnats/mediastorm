package datastore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrSessionAccountRevokeUnavailable = errors.New("account session revocation unavailable")
	ErrSessionAccountRevokeUnknown     = errors.New("account session revocation commit outcome unknown")
)

const sessionAccountRevokeBudget = 5 * time.Second

// RevokeAccountSessions deletes current PG sessions even when no process cache
// has observed them. It revokes sessions, not an account password or future login.
func (ds *DataStore) RevokeAccountSessions(ctx context.Context, accountID string) (int64, error) {
	return ds.revokeAccountSessions(ctx, accountID, "", false)
}

// RevokeAccountSessionsExcept preserves precisely keepToken, including an empty
// legacy token if one exists. It never updates the account or surviving sessions.
func (ds *DataStore) RevokeAccountSessionsExcept(ctx context.Context, accountID, keepToken string) (int64, error) {
	return ds.revokeAccountSessions(ctx, accountID, keepToken, true)
}

func (ds *DataStore) revokeAccountSessions(caller context.Context, accountID, keepToken string, except bool) (int64, error) {
	if caller == nil || ds == nil || ds.pool == nil {
		return 0, ErrSessionAccountRevokeUnavailable
	}
	if err := caller.Err(); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(caller, sessionAccountRevokeBudget)
	defer cancel()
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionAccountRevokeUnavailable)
	}
	// This runs before cancel(), using the established bounded rollback and
	// DataStore.New's actual query-end/CleanupDone policy. Cancellation alone
	// is never treated as completion of owned PG work.
	defer rollbackPinTx(tx, ctx)
	var found string
	err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, accountID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ctx.Err() // No write or COMMIT was sent; absent account is a no-op.
	}
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionAccountRevokeUnavailable)
	}
	// Future genuine mint transactions must take this same account lock before
	// INSERT. A mint committed before this revoke is deleted; one serialized
	// afterward remains a legitimate fresh password login. No artificial epoch
	// or no-op account UPDATE invalidates unrelated current frames.
	statement := `DELETE FROM sessions WHERE account_id=$1`
	args := []any{accountID}
	if except {
		statement += ` AND token<>$2`
		args = append(args, keepToken)
	}
	tag, err := tx.Exec(ctx, statement, args...)
	if err != nil {
		return 0, pinContextError(ctx, ErrSessionAccountRevokeUnavailable)
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return 0, ErrSessionAccountRevokeUnavailable
		}
		// A failed COMMIT acknowledgement may follow an actual commit. Never
		// return a successful count, retry, or claim rollback from cancellation.
		return 0, ErrSessionAccountRevokeUnknown
	}
	// A known successful acknowledgement stays known even if the caller's
	// context is canceled immediately afterward. The DELETE count is PG data.
	return tag.RowsAffected(), nil
}
