package content_preferences

import (
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"novastream/models"
)

var ErrInvalidPreference = errors.New("invalid content preference")
var scopedContentID = regexp.MustCompile(`^(.+):season:([0-9]+)(?::episode:([0-9]+))?$`)

func validatePreference(pref models.ContentPreference) error {
	if len(pref.ContentID) > 1024 {
		return ErrInvalidPreference
	}
	if pref.ContentType != "" && pref.ContentType != "movie" && pref.ContentType != "series" {
		return ErrInvalidPreference
	}
	if !strings.Contains(pref.ContentID, ":season:") {
		return nil
	}
	match := scopedContentID.FindStringSubmatch(pref.ContentID)
	if match == nil || strings.Contains(match[1], ":season:") || pref.ContentType == "movie" {
		return ErrInvalidPreference
	}
	season, err := strconv.Atoi(match[2])
	if err != nil || season < 0 || season > 10000 || strconv.Itoa(season) != match[2] {
		return ErrInvalidPreference
	}
	if match[3] != "" {
		episode, err := strconv.Atoi(match[3])
		if err != nil || episode <= 0 || episode > 100000 || strconv.Itoa(episode) != match[3] {
			return ErrInvalidPreference
		}
	}
	return nil
}

func copyPreference(pref models.ContentPreference) models.ContentPreference {
	if pref.PreferLocalFiles != nil {
		value := *pref.PreferLocalFiles
		pref.PreferLocalFiles = &value
	}
	return pref
}

// ResolveLocalPreference keeps explicit false distinct from inheritance. File,
// account, kids and quality checks remain the responsibility of normal playback.
func (s *Service) ResolveLocalPreference(userID, titleID string, episode *models.EpisodeReference, fallback bool) (bool, error) {
	for _, id := range models.ContentPreferenceSourceIDs(titleID, episode) {
		pref, err := s.Get(userID, id)
		if err != nil {
			return fallback, err
		}
		if pref != nil && pref.PreferLocalFiles != nil {
			return *pref.PreferLocalFiles, nil
		}
	}
	return fallback, nil
}

// LocalOverrides returns a stable fingerprint input for every child scope of a
// title. Conservatively invalidating a title's other episodes avoids reusing a
// title-indexed prewarm after a season or episode override changes.
func (s *Service) LocalOverrides(userID, titleID string) ([]models.ContentLocalOverride, error) {
	titleID = strings.ToLower(strings.TrimSpace(titleID))
	prefs, err := s.List(userID)
	if err != nil {
		return nil, err
	}
	var overrides []models.ContentLocalOverride
	for _, pref := range prefs {
		if pref.PreferLocalFiles == nil {
			continue
		}
		if pref.ContentID == titleID || strings.HasPrefix(pref.ContentID, titleID+":season:") {
			overrides = append(overrides, models.ContentLocalOverride{ContentID: pref.ContentID, PreferLocalFiles: *pref.PreferLocalFiles})
		}
	}
	sort.Slice(overrides, func(i, j int) bool { return overrides[i].ContentID < overrides[j].ContentID })
	return overrides, nil
}
