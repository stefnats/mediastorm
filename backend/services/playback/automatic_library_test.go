package playback

import (
	"context"
	"novastream/models"
	"path/filepath"
	"testing"
	"time"
)

func TestAutomaticLibraryEnvelopeSurvivesRestoreRetirementAndRetry(t *testing.T) {
	s := NewPrequeueStore(time.Hour)
	s.SetStoragePath(t.TempDir())
	episode := &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:456", Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:series:123", Ordering: "official"}}
	entry, _ := s.CreateScoped("tmdb:series:123", "Synthetic", "viewer", "series", 2024, episode, "manual", "automatic-scope", true)
	if current, ok := s.Get(entry.ID); !ok || !current.AutomaticLibrarySelection || !current.ToResponse().AutomaticLibrarySelection {
		t.Fatal("newly visible queued envelope lost the atomic source mode")
	}
	s.Update(entry.ID, func(e *PrequeueEntry) {
		e.Status = PrequeueStatusReady
		e.StreamPath = "plexmedia:version"
		e.LibraryItemID = "version"
		e.LibrarySourceType = "plex"
		e.LibraryIdentity = "synthetic-fingerprint"
		e.LibraryEpisodeVerified = true
		e.SelectedResult = &models.NZBResult{Title: "Synthetic version", Link: e.StreamPath, ServiceType: models.ContentServiceType("plex")}
	})
	restored := NewPrequeueStore(time.Hour)
	restored.SetStoragePath(filepath.Dir(s.storagePath))
	if err := restored.RestorePersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, ok := restored.Get(entry.ID)
	if !ok || !current.AutomaticLibrarySelection || current.LibraryItemID != "version" || current.LibrarySourceType != "plex" || current.LibraryIdentity != "synthetic-fingerprint" || !current.LibraryEpisodeVerified || current.TargetEpisode.EpisodeID != episode.EpisodeID {
		t.Fatal("persisted library source context was lost")
	}
	restored.RetireSourceSelection(entry.ID)
	if _, ok := restored.GetByTitleUserScopeForPlayback(entry.TitleID, entry.UserID, entry.SettingsScopeKey); ok {
		t.Fatal("retired library source remained reusable")
	}
	if kept, ok := restored.Get(entry.ID); !ok || kept.SelectedResult == nil || !kept.ReuseDisabled {
		t.Fatal("retirement deleted active reporting context")
	}
	next, created, err := restored.CreateReportReplacement(entry.ID, entry.SettingsScopeKey)
	if err != nil || !created || !next.AutomaticLibrarySelection || !next.ToResponse().AutomaticLibrarySelection || next.TargetEpisode.EpisodeID != episode.EpisodeID {
		t.Fatal("retry lost mode/leaf before worker initialization")
	}
}
