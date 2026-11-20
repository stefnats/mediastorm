package sessions

import (
	"context"

	"novastream/internal/datastore"
)

// RevokeAllForAccountContext is the checked PG-only account revoke. No cache
// mutex is held while acquiring or waiting for database locks or COMMIT.
func (s *Service) RevokeAllForAccountContext(ctx context.Context, accountID string) (int64, error) {
	return s.revokePGAccount(ctx, accountID, "", false)
}

// RevokeAllForAccountExceptContext preserves the exact token without changing
// its row, expiry, profile grants, or account's physical revision.
func (s *Service) RevokeAllForAccountExceptContext(ctx context.Context, accountID, keepToken string) (int64, error) {
	return s.revokePGAccount(ctx, accountID, keepToken, true)
}

func (s *Service) revokePGAccount(ctx context.Context, accountID, keepToken string, except bool) (int64, error) {
	if s == nil || s.store == nil {
		return 0, datastore.ErrSessionAccountRevokeUnavailable
	}
	var count int64
	var err error
	if except {
		count, err = s.store.RevokeAccountSessionsExcept(ctx, accountID, keepToken)
	} else {
		count, err = s.store.RevokeAccountSessions(ctx, accountID)
	}
	if err != nil {
		// The cache is only a projection. In particular Unknown does not prove
		// that the rows remain: all PG-backed validation still rereads PG.
		return 0, err
	}
	s.mu.Lock()
	for token, session := range s.sessions {
		if session.AccountID == accountID && (!except || token != keepToken) {
			delete(s.sessions, token)
		}
	}
	s.mu.Unlock()
	return count, nil
}
