package user_settings

import (
	"errors"
	"sort"
	"strings"

	"novastream/models"
)

var ErrInvalidFavoriteStation = errors.New("invalid station identity")
var ErrFavoriteStationLimit = errors.New("station favorite limit reached; remove a station favorite")

// SetFavoriteStation changes only this profile's station bookmarks. The same
// lock serializes general settings writes, which preserve this dedicated field.
// Like the existing settings service, this operation is process-local atomic.
func (s *Service) SetFavoriteStation(userID, stationID string, favorite bool) ([]string, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, ErrUserIDRequired
	}
	if !models.ValidFavoriteStationID(stationID) {
		return nil, ErrInvalidFavoriteStation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, existed := s.settings[userID]
	ids := make(map[string]bool, len(previous.LiveTV.FavoriteStationIDs))
	for _, id := range previous.LiveTV.FavoriteStationIDs {
		ids[id] = true
	}
	if ids[stationID] == favorite {
		return append([]string{}, previous.LiveTV.FavoriteStationIDs...), nil
	}
	if favorite && len(ids) >= models.MaxFavoriteStations {
		return nil, ErrFavoriteStationLimit
	}
	if favorite {
		ids[stationID] = true
	} else {
		delete(ids, stationID)
	}
	next := make([]string, 0, len(ids))
	for id := range ids {
		next = append(next, id)
	}
	sort.Strings(next)
	updated := previous
	updated.LiveTV.FavoriteStationIDs = next
	if isSettingsEmpty(updated) {
		delete(s.settings, userID)
	} else {
		s.settings[userID] = updated
	}
	if err := s.saveLocked(); err != nil {
		if existed {
			s.settings[userID] = previous
		} else {
			delete(s.settings, userID)
		}
		return nil, err
	}
	return append([]string{}, next...), nil
}
