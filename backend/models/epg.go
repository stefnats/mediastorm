package models

import (
	"time"
)

// EPGContentRating preserves the guide's claimed rating system and raw value.
// It is EPG metadata, not an independently verified content classification.
type EPGContentRating struct {
	System string `json:"system"`
	Value  string `json:"value"`
}

type EPGProgramSource struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	Name       string    `json:"name,omitempty"`
	ObservedAt time.Time `json:"observedAt"`
}

// EPGAgeDecision describes only the current cached guide entry. It does not
// enable playback. States are within_limit, too_old, and unknown.
type EPGAgeDecision struct {
	State      string      `json:"state"`
	Reason     string      `json:"reason"`
	Country    string      `json:"country"`
	MaximumAge int         `json:"maximumAge"`
	Age        *int        `json:"age,omitempty"`
	ChannelID  string      `json:"channelId"`
	At         time.Time   `json:"at"`
	Program    *EPGProgram `json:"program,omitempty"`
}

// EPGProgram represents a single program in the EPG schedule.
type EPGProgram struct {
	ChannelID   string             `json:"channelId"` // Links to LiveChannel.TvgID
	Title       string             `json:"title"`
	Description string             `json:"description,omitempty"`
	Start       time.Time          `json:"start"`
	Stop        time.Time          `json:"stop"`
	Icon        string             `json:"icon,omitempty"`
	Categories  []string           `json:"categories,omitempty"`
	Episode     string             `json:"episode,omitempty"` // Episode number in standard format (e.g., "S01E05")
	Rating      string             `json:"rating,omitempty"`
	Ratings     []EPGContentRating `json:"ratings,omitempty"`
	Source      *EPGProgramSource  `json:"source,omitempty"`
	// Airing flags from XMLTV. New covers <new/>, <premiere> and <live/>;
	// PreviouslyShown is <previously-shown>. Most guides set neither.
	New             bool `json:"new,omitempty"`
	PreviouslyShown bool `json:"previouslyShown,omitempty"`
}

// EPGChannel represents a channel's metadata from EPG data.
type EPGChannel struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Icon    string   `json:"icon,omitempty"`
	Aliases []string `json:"aliases,omitempty"` // XMLTV display names, including tuner channel numbers/callsigns
}

// EPGSchedule holds the complete EPG data for all channels.
type EPGSchedule struct {
	Channels    map[string]EPGChannel   `json:"channels"` // channelId -> channel metadata
	Programs    map[string][]EPGProgram `json:"programs"` // channelId -> sorted programs
	LastUpdated time.Time               `json:"lastUpdated"`
	SourceType  string                  `json:"sourceType"` // "xmltv" or "xtream"
}

// EPGNowPlaying represents the current and next program for a channel.
type EPGNowPlaying struct {
	ChannelID string      `json:"channelId"`
	Current   *EPGProgram `json:"current,omitempty"`
	Next      *EPGProgram `json:"next,omitempty"`
}

// EPGStatus represents the status of the EPG service.
type EPGStatus struct {
	Enabled      bool       `json:"enabled"`
	LastRefresh  *time.Time `json:"lastRefresh,omitempty"`
	LastError    string     `json:"lastError,omitempty"`
	ChannelCount int        `json:"channelCount"`
	ProgramCount int        `json:"programCount"`
	Refreshing   bool       `json:"refreshing"`
	SourceCount  int        `json:"sourceCount"`
}
