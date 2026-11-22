package models

import "strings"

// KidsLiveAgePolicy describes a deliberately selected guide, not an independent
// classification of a broadcast. The zero value leaves Live TV disabled.
type KidsLiveAgePolicy struct {
	Enabled       bool   `json:"enabled"`
	Country       string `json:"country"`
	MaximumAge    int    `json:"maximumAge"`
	GuideSourceID string `json:"guideSourceId"`
}

func (p KidsLiveAgePolicy) Valid() bool {
	age := p.MaximumAge == 0 || p.MaximumAge == 6 || p.MaximumAge == 12 || p.MaximumAge == 16 || p.MaximumAge == 18
	return p.Country == "DE" && age && len(p.GuideSourceID) <= 128 && strings.TrimSpace(p.GuideSourceID) == p.GuideSourceID && (!p.Enabled || p.GuideSourceID != "")
}
