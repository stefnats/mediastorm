package sessions

import (
	"context"
	"novastream/models"
)

// SnapshotForAuthorization reads the current authoritative session without
// refreshing its lifetime, updating the startup cache, cleaning up expired
// rows or running playback/provider validators. The caller must check scope,
// expiry and the current account role before granting access.
func (s *Service) SnapshotForAuthorization(ctx context.Context, token string) (*models.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || token == "" {
		return nil, ErrSessionNotFound
	}
	var item *models.Session
	if s.sessionReader != nil {
		var err error
		item, err = s.sessionReader.Get(ctx, token)
		if err != nil {
			return nil, err
		}
	} else {
		s.mu.RLock()
		value, ok := s.sessions[token]
		s.mu.RUnlock()
		if ok {
			item = &value
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if item == nil || item.Token != token {
		return nil, ErrSessionNotFound
	}
	copy := *item
	return &copy, nil
}
