package playback

import "strings"

// RetireSourceSelection preserves an active ready ID for reporting and recovery
// while preventing a changed source from being offered as a new preparation.
func (s *PrequeueStore) RetireSourceSelection(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[id]
	if !ok {
		return
	}
	if entry.Status != PrequeueStatusReady {
		s.deleteLocked(id)
		return
	}
	key := titleUserKey(entry.TitleID, entry.UserID, entry.SettingsScopeKey)
	if s.byTitleUser[key] == id {
		delete(s.byTitleUser, key)
	}
	entry.ReuseDisabled = true
	s.saveReadyEntry(entry)
	s.generation.Add(1)
}

// InvalidateSourceSelectionByUser cancels unfinished source selection and retires
// ready entries from future reuse. Ready IDs and their selected-release metadata
// remain available to playback, reporting and recovery for their normal lifetime.
func (s *PrequeueStore) InvalidateSourceSelectionByUser(userID string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.entries {
		if entry.UserID != userID {
			continue
		}
		if entry.Status != PrequeueStatusReady {
			s.deleteLocked(id)
			continue
		}
		key := titleUserKey(entry.TitleID, entry.UserID, entry.SettingsScopeKey)
		if s.byTitleUser[key] == id {
			delete(s.byTitleUser, key)
		}
		entry.ReuseDisabled = true
		s.saveReadyEntry(entry)
	}
	s.generation.Add(1)
}
