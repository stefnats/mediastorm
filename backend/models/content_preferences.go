package models

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ContentPreference stores per-content track and source preferences.
// Track preferences retain their series/movie scope. Local source ordering may
// additionally override a particular season or episode using a scoped ID.
type ContentPreference struct {
	PosterURL        string    `json:"posterUrl,omitempty"`
	BackdropURL      string    `json:"backdropUrl,omitempty"`
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
	if OfficialEpisodeCoordinates(titleID, episode) {
		seasonID := fmt.Sprintf("%s:season:%d", titleID, episode.SeasonNumber)
		if episode.EpisodeNumber > 0 {
			ids = append(ids, fmt.Sprintf("%s:episode:%d", seasonID, episode.EpisodeNumber))
		}
		ids = append(ids, seasonID)
	}
	return append(ids, titleID)
}

// CanonicalSeriesIdentity folds only known historical series aliases. It does
// not infer relationships between different metadata providers.
func CanonicalSeriesIdentity(id string) string {
	raw := strings.TrimSpace(id)
	parts := strings.Split(strings.ToLower(raw), ":")
	positive := func(value string) bool { n, err := strconv.ParseInt(value, 10, 64); return err == nil && n > 0 }
	if len(parts) == 3 && positive(parts[2]) && (parts[1] == "series" || parts[0] == "tmdb" && parts[1] == "tv") && (parts[0] == "tmdb" || parts[0] == "tvdb") {
		return parts[0] + ":series:" + parts[2]
	}
	if len(parts) == 2 && parts[0] == "tvdb" && positive(parts[1]) {
		return "tvdb:series:" + parts[1]
	}
	return raw
}

// OfficialEpisodeCoordinates requires provenance for child-specific rules.
// verifiedSeriesIDs must come from trusted metadata, not client-supplied aliases.
// Unproven legacy coordinates retain title inheritance.
func OfficialEpisodeCoordinates(titleID string, episode *EpisodeReference, verifiedSeriesIDs ...string) bool {
	if episode == nil || episode.Numbering == nil || episode.SeasonNumber < 0 || episode.EpisodeNumber <= 0 {
		return false
	}
	order := strings.ToLower(strings.TrimSpace(episode.Numbering.Ordering))
	if order != "" && order != "official" {
		return false
	}
	owner := CanonicalSeriesIdentity(episode.Numbering.SeriesID)
	if owner == "" {
		return false
	}
	for _, id := range append([]string{titleID}, verifiedSeriesIDs...) {
		if owner == CanonicalSeriesIdentity(id) {
			return true
		}
	}
	return false
}
