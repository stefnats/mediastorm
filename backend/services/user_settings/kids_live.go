package user_settings

import (
	"errors"
	"novastream/models"
	"strings"
)

// UpdateKidsLive updates only the dedicated parent-confirmed Live choice.
// Ordinary Update calls preserve it, including older clients' full PUTs.
func (s *Service) UpdateKidsLive(userID string, policy models.KidsLiveAgePolicy) error {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return ErrUserIDRequired
	}
	if !policy.Valid() {
		return errors.New("invalid Live programme age policy")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.useDB() {
		ctx, cancel := settingsStorageContext()
		defer cancel()
		if err := s.ensurePGTargetLocked(ctx, userID); err != nil {
			return err
		}
	}
	previous, existed := s.settings[userID]
	updated := previous
	updated.LiveTV.KidsAgeLimit = policy
	s.settings[userID] = updated
	var err error
	if s.useDB() {
		ctx, cancel := settingsStorageContext()
		defer cancel()
		err = s.persistPGLocked(ctx, []string{userID}, nil)
	} else {
		err = s.saveLocked()
	}
	if err != nil && !s.useDB() {
		if existed {
			s.settings[userID] = previous
		} else {
			delete(s.settings, userID)
		}
	}
	return err
}
