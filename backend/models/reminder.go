package models

import "time"

const (
	ReminderRelease        = "release"
	ReminderNewEpisode     = "new_episode"
	ReminderMatchingSource = "matching_source"
)

type ReminderEpisode struct {
	EpisodeID     string           `json:"episodeId"`
	Numbering     EpisodeNumbering `json:"numbering"`
	SeasonNumber  int              `json:"seasonNumber"`
	EpisodeNumber int              `json:"episodeNumber"`
}
type Reminder struct {
	ID                     string           `json:"id"`
	ContentID              string           `json:"contentId"`
	ContentType            string           `json:"contentType"`
	EventKind              string           `json:"eventKind"`
	Title                  Title            `json:"title"`
	ReleaseType            string           `json:"releaseType,omitempty"`
	Episode                *ReminderEpisode `json:"episode,omitempty"`
	State                  string           `json:"state"`
	CreatedOnCurrentDevice bool             `json:"createdOnCurrentDevice"`
	CreatedAt              time.Time        `json:"createdAt"`
	LastObservedAt         *time.Time       `json:"lastObservedAt,omitempty"`
	// Internal fields never enter client DTOs.
	ClientID  string     `json:"-"`
	TargetKey string     `json:"-"`
	Aliases   []string   `json:"-"`
	RevokedAt *time.Time `json:"-"`
}
type ReminderEvent struct {
	ID            string           `json:"id"`
	ReminderID    string           `json:"reminderId"`
	EventKind     string           `json:"eventKind"`
	ContentID     string           `json:"contentId"`
	ContentType   string           `json:"contentType"`
	Title         Title            `json:"title"`
	Episode       *ReminderEpisode `json:"episode,omitempty"`
	ReleaseType   string           `json:"releaseType,omitempty"`
	OccurredAt    time.Time        `json:"occurredAt"`
	ReadAt        *time.Time       `json:"readAt,omitempty"`
	DismissedAt   *time.Time       `json:"dismissedAt,omitempty"`
	CandidateOnly bool             `json:"candidateOnly"`
	MilestoneKey  string           `json:"-"`
}

// ReminderCheckpoint records observed provenance, not a claim about absent data.
type ReminderCheckpoint struct {
	ReminderID   string           `json:"reminderId"`
	MilestoneKey string           `json:"milestoneKey"`
	State        string           `json:"state"`
	ObservedAt   time.Time        `json:"observedAt"`
	Fired        bool             `json:"fired"`
	Episode      *ReminderEpisode `json:"episode,omitempty"`
	Ambiguous    bool             `json:"ambiguous,omitempty"`
}

// Storage wrappers keep routing identities out of public responses.
type StoredReminder struct {
	Reminder     Reminder   `json:"reminder"`
	ClientID     string     `json:"clientId"`
	TargetKey    string     `json:"targetKey"`
	Aliases      []string   `json:"aliases"`
	RevokedAt    *time.Time `json:"revokedAt,omitempty"`
	SearchTitle  Title      `json:"searchTitle"`
	ObserveAfter time.Time  `json:"observeAfter"`
}
type ReminderProfileState struct {
	Subscriptions []StoredReminder     `json:"subscriptions"`
	Checkpoints   []ReminderCheckpoint `json:"checkpoints"`
	Events        []ReminderEvent      `json:"events"`
	EventKeys     map[string]string    `json:"eventKeys,omitempty"`
}
