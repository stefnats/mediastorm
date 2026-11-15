package prewarm

import (
	"context"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/playback"
)

func TestContentPreferenceInvalidationRejectsRetiredPreparationForWarmReuse(t *testing.T) {
	entry := &playback.PrequeueEntry{
		Status:      playback.PrequeueStatusReady,
		StreamPath:  "/webdav/synthetic.mkv",
		AudioTracks: []playback.AudioTrackInfo{{Index: 0, Language: "eng", Codec: "aac"}},
	}
	if !prequeueReadyForWarmReuse(entry) {
		t.Fatal("live preparation was not reusable")
	}
	entry.ReuseDisabled = true
	if prequeueReadyForWarmReuse(entry) {
		t.Fatal("reportable retired preparation was offered for warm reuse")
	}
}

func TestContentPreferenceInvalidationRewarmsAfterSourceScopeReturns(t *testing.T) {
	store := playback.NewPrequeueStore(time.Hour)
	svc := NewService(nil, "")
	svc.SetPrequeueStore(store)
	workerCalls := 0
	svc.SetScopedWorkerFunc(func(ctx context.Context, titleID, titleName, imdbID, mediaType string, year int, userID, clientID, scope string, episode *models.EpisodeReference) (string, error) {
		workerCalls++
		entry, _ := store.CreateScoped(titleID, titleName, userID, mediaType, year, episode, "prewarm", scope)
		store.Update(entry.ID, func(e *playback.PrequeueEntry) {
			e.Status = playback.PrequeueStatusReady
			e.StreamPath = "/webdav/synthetic.mkv"
			e.AudioTracks = []playback.AudioTrackInfo{{Index: 0, Language: "eng", Codec: "aac"}}
		})
		return entry.ID, nil
	})
	state := models.SeriesWatchState{SeriesID: "tmdb:tv:123", SeriesTitle: "Synthetic series", Year: 2026}
	user := models.User{ID: "viewer", Name: "Synthetic viewer"}
	episode := &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 1}
	result := &SyncResult{}
	resolveCount := 0
	svc.jitterFn = func() time.Duration { return 0 }
	warm := func(scope string) *WarmEntry {
		t.Helper()
		if err := svc.warmContinueWatchingScope(context.Background(), state, user, "series", "", episode, scope, "", time.Hour, &resolveCount, result); err != nil {
			t.Fatal(err)
		}
		entry := svc.entries[entryKey(state.SeriesID, user.ID, scope)]
		if entry == nil || entry.PrequeueID == "" || entry.Error != "" {
			t.Fatalf("scope %q failed to warm: %+v", scope, entry)
		}
		return entry
	}

	// A season override contributes no timestamp to its fingerprint, so changing
	// A -> B -> A intentionally revisits the exact original parent scope key.
	first := warm("source-scope-A")
	old, _ := store.Get(first.PrequeueID)
	oldExpiry := old.ExpiresAt
	store.InvalidateSourceSelectionByUser(user.ID)
	second := warm("source-scope-B")
	store.InvalidateSourceSelectionByUser(user.ID)
	third := warm("source-scope-A")
	if workerCalls != 3 || result.Warmed != 3 || result.Skipped != 0 || result.Failed != 0 {
		t.Fatalf("parent reused retired preparation instead of continuing: calls=%d result=%+v", workerCalls, result)
	}
	if third.PrequeueID == first.PrequeueID || third.PrequeueID == second.PrequeueID {
		t.Fatal("returning source scope resurrected a retired preparation")
	}
	for _, id := range []string{first.PrequeueID, second.PrequeueID} {
		entry, ok := store.Get(id)
		if !ok || !entry.ReuseDisabled {
			t.Fatal("rewarming removed the original playback/report context")
		}
	}
	old, _ = store.Get(first.PrequeueID)
	if !old.ExpiresAt.Equal(oldExpiry) {
		t.Fatal("warm parent extended the retired preparation lifetime")
	}
	if current, ok := store.GetByTitleUserScope(state.SeriesID, user.ID, "source-scope-A"); !ok || current.ID != third.PrequeueID {
		t.Fatal("fresh preparation was not indexed for the returning source scope")
	}
}
