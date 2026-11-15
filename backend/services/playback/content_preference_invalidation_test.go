package playback

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"novastream/models"
)

func TestContentPreferenceInvalidationPreservesReadyPlaybackAndDisablesReuse(t *testing.T) {
	s := NewPrequeueStore(time.Minute)
	s.SetStoragePath(t.TempDir())
	ready, _ := s.Create("title", "Synthetic title", "viewer", "movie", 2026, nil, "manual")
	s.Update(ready.ID, func(e *PrequeueEntry) {
		e.Status = PrequeueStatusReady
		e.StreamPath = "/webdav/synthetic.mkv"
		e.SelectedResult = &models.NZBResult{Title: "Synthetic release"}
	})
	pending, _ := s.Create("another-title", "Synthetic pending", "viewer", "movie", 2026, nil, "manual")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.SetCancelFunc(pending.ID, cancel)
	other, _ := s.Create("title", "Other profile", "other", "movie", 2026, nil, "manual")
	before := s.Generation()
	s.InvalidateSourceSelectionByUser("viewer")
	if s.Generation() <= before {
		t.Fatal("dependent source caches were not invalidated")
	}
	if _, ok := s.Get(pending.ID); ok || ctx.Err() == nil {
		t.Fatal("unfinished selection survived source preference change")
	}
	if _, ok := s.Get(other.ID); !ok {
		t.Fatal("another profile was changed")
	}
	kept, ok := s.Get(ready.ID)
	if !ok || !kept.ReuseDisabled || kept.SelectedResult == nil || kept.StreamPath != "/webdav/synthetic.mkv" {
		t.Fatal("active playback/report context was deleted")
	}
	if _, ok := s.GetByTitleUserScopeForPlayback("title", "viewer", ready.SettingsScopeKey); ok {
		t.Fatal("retired preparation was still offered for playback reuse")
	}
	restored := NewPrequeueStore(time.Minute)
	restored.SetStoragePath(filepath.Dir(s.storagePath))
	if err := restored.RestorePersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if kept, ok := restored.Get(ready.ID); !ok || !kept.ReuseDisabled {
		t.Fatal("restart lost the reportable ready envelope")
	}
	if _, ok := restored.GetByTitleUserScopeForPlayback("title", "viewer", ready.SettingsScopeKey); ok {
		t.Fatal("restart resurrected retired preparation for reuse")
	}
}

// An earlier file snapshot may finish decoding after a preference update has
// already retired its live entry. Restoration must preserve the live retirement.
func TestContentPreferenceInvalidationSurvivesStaleDiskRestore(t *testing.T) {
	s := NewPrequeueStore(time.Hour)
	s.SetStoragePath(t.TempDir())
	ready, _ := s.Create("title", "Synthetic title", "viewer", "movie", 2026, nil, "prewarm")
	s.Update(ready.ID, func(e *PrequeueEntry) {
		e.Status = PrequeueStatusReady
		e.StreamPath = "/webdav/synthetic.mkv"
	})
	staleSnapshot, err := os.ReadFile(s.storagePath)
	if err != nil {
		t.Fatal(err)
	}
	s.InvalidateSourceSelectionByUser("viewer")
	// Supply the same snapshot an already-open restore would have decoded before
	// invalidation, without using timing or a production-only synchronization hook.
	if err := os.WriteFile(s.storagePath, staleSnapshot, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.RestorePersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	kept, ok := s.Get(ready.ID)
	if !ok || !kept.ReuseDisabled {
		t.Fatal("stale restore lost the live retirement flag")
	}
	if _, ok := s.GetByTitleUserScopeForPlayback("title", "viewer", ready.SettingsScopeKey); ok {
		t.Fatal("stale restore reindexed retired preparation")
	}
}
