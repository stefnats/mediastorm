package models

import (
	"fmt"
	"strings"
	"time"
)

// ContentPreference stores per-content track and source preferences.
// Track preferences retain their series/movie scope. Local source ordering may
// additionally override a particular season or episode using a scoped ID.
type ContentPreference struct {
	ContentID        string    `json:"contentId"`                  // e.g., "tmdb:tv:12345" for series, "tmdb:movie:67890" for movies
	ContentType      string    `json:"contentType"`                // "series" or "movie"
	AudioLanguage    string    `json:"audioLanguage,omitempty"`    // ISO 639-2 code (eng, jpn, spa, etc.)
	SubtitleLanguage string    `json:"subtitleLanguage,omitempty"` // ISO 639-2 code or empty
	SubtitleMode     string    `json:"subtitleMode,omitempty"`     // "off", "on", "forced-only", "foreign-audio"
	PreferLocalFiles *bool     `json:"preferLocalFiles,omitempty"` // nil inherits; false keeps normal search-first selection
	UpdatedAt        time.Time `json:"updatedAt"`
}

// ContentPreferenceUpdate represents a request to update content preferences.
type ContentPreferenceUpdate struct {
	ContentID        string `json:"contentId"`
	ContentType      string `json:"contentType"`
	AudioLanguage    string `json:"audioLanguage,omitempty"`
	SubtitleLanguage string `json:"subtitleLanguage,omitempty"`
	SubtitleMode     string `json:"subtitleMode,omitempty"`
	PreferLocalFiles *bool  `json:"preferLocalFiles,omitempty"`
}

// ContentLocalOverride is the source-selection-only part of a scoped preference.
// Scoped IDs use <series>:season:<n>[:episode:<n>]; series/movie IDs stay unchanged.
type ContentLocalOverride struct {
	ContentID        string `json:"contentId"`
	PreferLocalFiles bool   `json:"preferLocalFiles"`
}

// ContentPreferenceSourceIDs is ordered from the most specific override to the
// series/movie setting. It does not authorize a file or change its episode match.
func ContentPreferenceSourceIDs(titleID string, episode *EpisodeReference) []string {
	titleID = strings.ToLower(strings.TrimSpace(titleID))
	if titleID == "" {
		return nil
	}
	ids := make([]string, 0, 3)
	if episode != nil && episode.SeasonNumber >= 0 {
		seasonID := fmt.Sprintf("%s:season:%d", titleID, episode.SeasonNumber)
		if episode.EpisodeNumber > 0 {
			ids = append(ids, fmt.Sprintf("%s:episode:%d", seasonID, episode.EpisodeNumber))
		}
		ids = append(ids, seasonID)
	}
	return append(ids, titleID)
}
