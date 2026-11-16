package models

import "strings"

const MaxFavoriteStations = 10000

// ValidFavoriteStationID accepts only identities emitted by the existing
// server station resolver. A bookmark never grants access to a source.
func ValidFavoriteStationID(id string) bool {
	if len(id) != len("station:")+24 || !strings.HasPrefix(id, "station:") {
		return false
	}
	for _, r := range strings.TrimPrefix(id, "station:") {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}
