package sessions

import (
	"context"
	"strings"

	"novastream/internal/datastore"
)

// RevokeContext is the checked PG-only exact-token revoke. File and memory
// services retain their existing behavior through Revoke, not this method.
func (s *Service) RevokeContext(ctx context.Context, token string) error {
	if s == nil || s.store == nil {
		return datastore.ErrSessionTargetRevokeUnavailable
	}
	count, err := s.store.RevokeSessionToken(ctx, token)
	if err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
	if count == 0 {
		return ErrSessionNotFound
	}
	return nil
}

// RevokeAllForClientContext returns the actual committed DELETE count. Cache
// entries are only a projection; no service mutex is held during database IO.
func (s *Service) RevokeAllForClientContext(ctx context.Context, clientID string) (int64, error) {
	if s == nil || s.store == nil {
		return 0, datastore.ErrSessionTargetRevokeUnavailable
	}
	clientID = strings.TrimSpace(clientID)
	count, err := s.store.RevokeClientSessions(ctx, clientID)
	if err != nil {
		return 0, err
	}
	if clientID != "" {
		s.mu.Lock()
		for token, session := range s.sessions {
			if session.ClientID == clientID {
				delete(s.sessions, token)
			}
		}
		s.mu.Unlock()
	}
	return count, nil
}

// UsesPostgres reports the configured storage mode, not its availability.
// Callers decide compatibility before IO; database errors never select memory.
func (s *Service) UsesPostgres() bool { return s != nil && s.store != nil }
