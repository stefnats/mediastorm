package users

import (
	"errors"
	"strings"
	"time"

	"novastream/models"
)

var ErrLivePermissions = errors.New("live takeover requires a non-child profile assigned to an account")

// SetLivePermissions is called only by account management. Self-service user
// settings never grant either right. A persistence failure restores memory.
func (s *Service) SetLivePermissions(id string, live, recordings bool) (models.User, error) {
	id = strings.TrimSpace(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, ok := s.users[id]
	if !ok {
		return models.User{}, ErrUserNotFound
	}
	if (live || recordings) && (previous.IsKidsProfile || previous.AccountID == "") {
		return models.User{}, ErrLivePermissions
	}
	user := previous
	user.AllowLiveTakeover = live
	user.AllowRecordingInterruption = recordings
	user.UpdatedAt = time.Now().UTC()
	s.users[id] = user
	if err := s.saveLocked(); err != nil {
		s.users[id] = previous
		return models.User{}, err
	}
	return user, nil
}
