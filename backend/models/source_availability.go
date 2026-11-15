package models

import "time"

// SourceAvailability describes a filtered search in explicitly selected
// indexers. A found release is a candidate, not proof of playable media.
type SourceAvailability struct {
	EpisodeCoverage *EpisodeSourceCoverage `json:"episodeCoverage,omitempty"`
	SourceType      string                 `json:"sourceType,omitempty"` // local only after file/track checks
	State           string                 `json:"state"`                // unknown, found, not_found
	CheckedAt       *time.Time             `json:"checkedAt,omitempty"`
	ExpiresAt       *time.Time             `json:"expiresAt,omitempty"`
	Providers       []string               `json:"providers,omitempty"`
	Complete        bool                   `json:"complete"` // All selected, authorized providers completed
	Reason          string                 `json:"reason,omitempty"`
}

// EpisodeSourceCoverage summarizes observed coordinates, never a catalogue's
// complete episode inventory. Partial requires both found and missing episodes.
type EpisodeSourceCoverage struct {
	State             string                 `json:"state"` // unknown, partial
	FoundEpisodes     int                    `json:"foundEpisodes"`
	NotFoundEpisodes  int                    `json:"notFoundEpisodes"`
	UnknownEpisodes   int                    `json:"unknownEpisodes"`
	CatalogueComplete bool                   `json:"catalogueComplete"`
	Seasons           []SeasonSourceCoverage `json:"seasons"`
}

type SeasonSourceCoverage struct {
	SeasonNumber     int    `json:"seasonNumber"`
	State            string `json:"state"` // unknown, partial
	FoundEpisodes    int    `json:"foundEpisodes"`
	NotFoundEpisodes int    `json:"notFoundEpisodes"`
	UnknownEpisodes  int    `json:"unknownEpisodes"`
}
