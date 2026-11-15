package badstreams

import (
	"novastream/models"
	"path/filepath"
	"testing"
)

func TestLibraryReportAliasesRetainExactScopeAndVersion(t *testing.T) {
	for _, direction := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy to modern", true: "modern to legacy"}[direction], func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "reports.json"))
			stored, current := "tmdb:tv:123", "tmdb:series:123"
			if direction {
				stored, current = current, stored
			}
			result := models.NZBResult{Title: "Same.Movie.Name.mkv", Link: "plexmedia:part-1", ServiceType: models.ContentServiceType("plex"), Attributes: map[string]string{"provider": "plex"}}
			ctx := PlaybackContext{TitleID: stored, ProfileID: "viewer", ClientID: "device", AudioLanguage: "fra", Episode: &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 3, Numbering: &models.EpisodeNumbering{SeriesID: stored, Ordering: "official"}}}
			report, err := s.Report(ReportRequest{Result: result, Context: ctx, Reason: "missing_language", ReporterID: "viewer"})
			if err != nil {
				t.Fatal(err)
			}
			// Simulate a persisted row written before alias-aware scope matching.
			legacy := s.entries[report.ID]
			scope := *legacy.Scope
			scope.TitleID = stored
			scope.NumberingKey = stored + "|official"
			legacy.Scope = &scope
			s.entries[report.ID] = legacy
			legacyRevision := s.RevisionFor(ctx)
			ctx.TitleID = current
			ctx.Episode = &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 3, Numbering: &models.EpisodeNumbering{SeriesID: current, Ordering: "official"}}
			if s.MatchFor(result, ctx) == nil || s.RevisionFor(ctx) != legacyRevision {
				t.Fatal("same-provider alias bypassed existing report or invalidation")
			}
			other := result
			other.Link = "plexmedia:part-2"
			if s.MatchFor(other, ctx) != nil || SameRelease(result, other) {
				t.Fatal("same filename blocked another library version")
			}
			for _, mode := range []string{"profile", "language", "order", "episode", "cross-provider owner", "whole series"} {
				copy := ctx
				ep := *ctx.Episode
				copy.Episode = &ep
				switch mode {
				case "profile":
					copy.ProfileID = "other"
				case "language":
					copy.AudioLanguage = "eng"
				case "order":
					copy.Episode.Numbering = &models.EpisodeNumbering{SeriesID: current, Ordering: "dvd"}
				case "episode":
					copy.Episode.EpisodeNumber = 4
				case "cross-provider owner":
					copy.Episode.Numbering = &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}
				case "whole series":
					copy.Episode = nil
				}
				if s.MatchFor(result, copy) != nil {
					t.Fatalf("report crossed %s scope", mode)
				}
				if s.RevisionFor(copy) == legacyRevision {
					t.Fatalf("revision crossed %s scope", mode)
				}
			}
			deviceReport, err := s.Report(ReportRequest{Result: result, Context: ctx, Reason: "device_problem", ReporterID: "viewer"})
			if err != nil {
				t.Fatal(err)
			}
			otherDevice := ctx
			otherDevice.ClientID = "other-device"
			otherDevice.AudioLanguage = "eng"
			if match := s.MatchFor(result, otherDevice); match != nil && match.ID == deviceReport.ID {
				t.Fatal("device-specific report crossed device boundary")
			}
		})
	}
	if reportNumberingIdentity("opaque:Show|official") == reportNumberingIdentity("opaque:show|official") {
		t.Fatal("opaque case-sensitive owners were collapsed")
	}
}
