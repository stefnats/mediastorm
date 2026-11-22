package epg

import (
	"novastream/models"
	"testing"
	"time"
)

func TestKidsLiveSelectedGuideUsesActualProgrammeTime(t *testing.T) {
	service, calls := programmeAgeGuide(t)
	now := programmeAgeTime(10, 30)
	initial := service.CurrentProgramAgeDecision("allowed", 6, now)
	selected := initial.Program.Source.ID
	foreign := *initial.Program
	foreign.Source = &models.EPGProgramSource{ID: "other-guide", Type: "xmltv", Name: "Other source", ObservedAt: now}
	foreign.Ratings = []models.EPGContentRating{{System: "FSK", Value: "18"}}
	service.schedule.Programs["allowed"] = append(service.schedule.Programs["allowed"], foreign)
	for _, tc := range []struct {
		name, source string
		at           time.Time
		state        string
	}{
		{"chosen-guide", selected, now, "within_limit"},
		{"other-guide", "other-guide", now, "too_old"},
		{"missing-choice", "", now, "unknown"},
		{"unknown-source", "absent", now, "unknown"},
		{"next-programme-boundary", selected, programmeAgeTime(11, 0), "too_old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := service.CurrentProgramAgeDecisionForSource("allowed", tc.source, 6, tc.at)
			if got.State != tc.state {
				t.Fatalf("decision %+v", got)
			}
		})
	}
	// Presentation offsets query other times; a Live decision always uses actual
	// UTC and cannot inherit a viewer's guide-display shift.
	for _, offset := range []time.Duration{-time.Hour, time.Hour} {
		_ = service.GetNowPlaying([]string{"allowed"}, offset)
		if got := service.CurrentProgramAgeDecisionForSource("allowed", selected, 6, now); got.State != "within_limit" {
			t.Fatal("presentation offset changed Live decision", got)
		}
	}
	sources := service.CachedProgrammeSources()
	if len(sources) != 2 || calls.Load() != 1 {
		t.Fatal("sources fetched again or lost", sources, calls.Load())
	}
	sources[0].ID = "changed-copy"
	if got := service.CurrentProgramAgeDecisionForSource("allowed", selected, 6, now); got.State != "within_limit" {
		t.Fatal("descriptive copy changed stored source")
	}
}

func TestKidsLiveSelectedGuideRequiresExactChannelID(t *testing.T) {
	service, calls := programmeAgeGuide(t)
	now := programmeAgeTime(10, 30)
	original := service.CurrentProgramAgeDecision("allowed", 6, now)
	source := original.Program.Source.ID
	channel := service.schedule.Channels["allowed"]
	channel.Name = "Synthetic Station"
	service.schedule.Channels["allowed"] = channel
	for _, alias := range []string{"Synthetic Station", "allowed HD"} {
		described := service.CurrentProgramAgeDecision(alias, 6, now)
		if described.State != "within_limit" || described.ChannelID != "allowed" {
			t.Fatal("existing descriptive alias stopped working", alias, described)
		}
		playback := service.CurrentProgramAgeDecisionForSource(alias, source, 6, now)
		if playback.State != "unknown" || playback.Reason != "channel_unknown" || playback.Program != nil {
			t.Fatal("playback adopted a similar channel", alias, playback)
		}
	}
	for _, id := range []string{"allowed", "ALLOWED"} {
		playback := service.CurrentProgramAgeDecisionForSource(id, source, 6, now)
		if playback.State != "within_limit" || playback.ChannelID != "allowed" {
			t.Fatal("exact case-normalized channel stopped working", id, playback)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("channel checks fetched the guide")
	}
}
