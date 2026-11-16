package playback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

func authorizationSnapshotFixture(t *testing.T) (*PrequeueStore, string) {
	t.Helper()
	store := NewPrequeueStore(time.Hour)
	entry, _ := store.Create("tmdb:series:42", "Synthetic series", "profile-a", "series", 2026,
		&models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "leaf-2",
			Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:series:42", Ordering: "official"},
			Image:     &models.Image{URL: "https://example.invalid/episode"}}, "details")
	flag := false
	store.Update(entry.ID, func(e *PrequeueEntry) {
		e.Status = PrequeueStatusReady
		e.StreamPath = "/synthetic/ready.mkv"
		e.AudioTracks = []AudioTrackInfo{{Index: 1, Language: "fra"}}
		e.SubtitleTracks = []SubtitleTrackInfo{{Index: 2, Language: "spa"}}
		e.SubtitleSessions = map[int]*models.SubtitleSessionInfo{2: {SessionID: "original"}}
		e.ResultAttributes = map[string]string{"language": "fra"}
		e.SelectedResult = &models.NZBResult{Categories: []string{"movie"}, Attributes: map[string]string{"edition": "original"}}
		e.MigrationCandidates = []models.NZBResult{{Categories: []string{"series"}, Attributes: map[string]string{"edition": "original"}}}
		e.TrackSelection = &models.TrackSelection{AudioLanguage: "fra", Audio: &models.SelectedTrack{Language: "fra"},
			Subtitle: &models.SelectedTrack{Language: "spa"}, TextSubtitle: &models.SelectedTrack{Language: "spa"}}
		e.MediaEvidence = &models.MediaEvidence{Version: 1,
			TargetEpisode: &models.EpisodeReference{Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:series:42"}, Image: &models.Image{URL: "https://example.invalid/evidence"}},
			Candidate:     &models.MediaCandidateFacts{AudioLanguages: []string{"fra"}},
			Probed: &models.MediaProbeFacts{AudioStreams: []models.MediaAudioFacts{{Index: 1, AudioDescription: &flag}},
				SubtitleStreams: []models.MediaSubtitleFacts{{Index: 2, Forced: &flag}}}}
	})
	return store, entry.ID
}

func TestAuthorizationSnapshotNeverInvokesReadyValidator(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	ageValidation(store, id, "/synthetic/ready.mkv", 6*time.Minute)
	var calls atomic.Int32
	store.SetStreamPathValidator(func(context.Context, string) error {
		calls.Add(1)
		return errors.New("poison validator must never run during authorization")
	})
	stamp, ok := store.BeginAuthorizationAttempt(id)
	if !ok || !stamp.Valid() {
		t.Fatal("normal runtime attempt was not created")
	}
	for i := 0; i < 10; i++ {
		entry, current, exists := store.SnapshotForAuthorization(id)
		if !exists || current != stamp || entry.StreamPath != "/synthetic/ready.mkv" {
			t.Fatal("cache-only snapshot changed the current attempt or input")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("authorization invoked the input validator")
	}
	// Prove the poison is installed on a genuinely stale ready entry: the
	// legacy read performs validation and rejects the same input.
	if _, exists := store.Get(id); exists || calls.Load() != 1 {
		t.Fatal("control read did not invoke the installed validator")
	}
}

func TestAuthorizationSnapshotCannotMutateStoredIdentityOrMetadata(t *testing.T) {
	cases := map[string]func(*PrequeueEntry){
		"episode":                func(e *PrequeueEntry) { e.TargetEpisode.EpisodeID = "foreign-leaf" },
		"numbering":              func(e *PrequeueEntry) { e.TargetEpisode.Numbering.SeriesID = "foreign-series" },
		"episode image":          func(e *PrequeueEntry) { e.TargetEpisode.Image.URL = "foreign" },
		"audio":                  func(e *PrequeueEntry) { e.AudioTracks[0].Language = "eng" },
		"subtitle":               func(e *PrequeueEntry) { e.SubtitleTracks[0].Language = "eng" },
		"attributes":             func(e *PrequeueEntry) { e.ResultAttributes["language"] = "eng" },
		"selected categories":    func(e *PrequeueEntry) { e.SelectedResult.Categories[0] = "foreign" },
		"selected attributes":    func(e *PrequeueEntry) { e.SelectedResult.Attributes["edition"] = "foreign" },
		"migration categories":   func(e *PrequeueEntry) { e.MigrationCandidates[0].Categories[0] = "foreign" },
		"migration attributes":   func(e *PrequeueEntry) { e.MigrationCandidates[0].Attributes["edition"] = "foreign" },
		"selection":              func(e *PrequeueEntry) { e.TrackSelection.AudioLanguage = "eng" },
		"selected audio":         func(e *PrequeueEntry) { e.TrackSelection.Audio.Language = "eng" },
		"selected subtitle":      func(e *PrequeueEntry) { e.TrackSelection.Subtitle.Language = "eng" },
		"selected text subtitle": func(e *PrequeueEntry) { e.TrackSelection.TextSubtitle.Language = "eng" },
		"evidence numbering":     func(e *PrequeueEntry) { e.MediaEvidence.TargetEpisode.Numbering.SeriesID = "foreign" },
		"evidence image":         func(e *PrequeueEntry) { e.MediaEvidence.TargetEpisode.Image.URL = "foreign" },
		"candidate languages":    func(e *PrequeueEntry) { e.MediaEvidence.Candidate.AudioLanguages[0] = "eng" },
		"audio description":      func(e *PrequeueEntry) { *e.MediaEvidence.Probed.AudioStreams[0].AudioDescription = true },
		"forced subtitle":        func(e *PrequeueEntry) { *e.MediaEvidence.Probed.SubtitleStreams[0].Forced = true },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			store, id := authorizationSnapshotFixture(t)
			entry, stamp, ok := store.SnapshotForAuthorization(id)
			if !ok {
				t.Fatal("fixture missing")
			}
			before, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			mutate(entry)
			again, current, ok := store.SnapshotForAuthorization(id)
			after, err := json.Marshal(again)
			if err != nil {
				t.Fatal(err)
			}
			if !ok || current != stamp || !bytes.Equal(before, after) {
				t.Fatal("mutating a returned snapshot changed the authoritative entry")
			}
		})
	}
	t.Run("subtitle session excluded from JSON", func(t *testing.T) {
		store, id := authorizationSnapshotFixture(t)
		entry, _, _ := store.SnapshotForAuthorization(id)
		entry.SubtitleSessions[2].SessionID = "foreign"
		delete(entry.SubtitleSessions, 2)
		again, _, ok := store.SnapshotForAuthorization(id)
		if !ok || again.SubtitleSessions[2] == nil || again.SubtitleSessions[2].SessionID != "original" {
			t.Fatal("snapshot leaked a mutable subtitle-session pointer")
		}
	})
}

func TestAuthorizationSnapshotPreservesExpiryAndNeverRevivesExpiredEntry(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	deadline := time.Now().Add(time.Minute)
	store.ForceExpiry(id, deadline)
	for i := 0; i < 3; i++ {
		if _, ok := store.BeginAuthorizationAttempt(id); !ok {
			t.Fatal("fresh attempt missing")
		}
		entry, _, ok := store.SnapshotForAuthorization(id)
		if !ok || !entry.ExpiresAt.Equal(deadline) {
			t.Fatal("authorization refreshed the input expiry")
		}
	}
	store.ForceExpiry(id, time.Now().Add(-time.Second))
	entry, stamp, ok := store.SnapshotForAuthorization(id)
	if ok || entry != nil || stamp.Valid() {
		t.Fatal("expired input returned authorization context")
	}
	if stamp, ok := store.BeginAuthorizationAttempt(id); ok || stamp.Valid() {
		t.Fatal("expired input gained a new attempt")
	}
	store.mu.RLock()
	_, stillStored := store.entries[id]
	store.mu.RUnlock()
	if !stillStored {
		t.Fatal("pure snapshot performed cleanup rather than observing expiry")
	}
}

func TestAuthorizationAttemptCannotBeReusedAfterReplacementOrStoreChange(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	_, absent, ok := store.SnapshotForAuthorization(id)
	if !ok || absent.Valid() {
		t.Fatal("unobserved legacy entry acquired a runtime attempt")
	}
	first, _ := store.BeginAuthorizationAttempt(id)
	second, _ := store.BeginAuthorizationAttempt(id)
	if absent.Revision() != "" || first.Revision() == "" || first.Revision() == second.Revision() {
		t.Fatal("private revisions did not distinguish runtime attempts")
	}
	if !first.Valid() || !second.Valid() || first == second {
		t.Fatal("new worker reused an old runtime attempt")
	}
	_, current, _ := store.SnapshotForAuthorization(id)
	if current != second {
		t.Fatal("snapshot did not bind the newest worker")
	}
	other, otherID := authorizationSnapshotFixture(t)
	otherStamp, _ := other.BeginAuthorizationAttempt(otherID)
	if otherStamp.Revision() == first.Revision() {
		t.Fatal("private revision crossed store instances")
	}
	if otherStamp == first || otherStamp == second {
		t.Fatal("attempt crossed store instances")
	}
	store.DeleteAll()
	entry, _ := store.Create("tmdb:series:42", "Synthetic series", "profile-a", "series", 2026, nil, "details")
	third, _ := store.BeginAuthorizationAttempt(entry.ID)
	if third == first || third == second || !third.Valid() {
		t.Fatal("clear/recreation reused a stale worker attempt")
	}
	if entry, stamp, ok := store.SnapshotForAuthorization(id); ok || entry != nil || stamp.Valid() {
		t.Fatal("deleted input retained its runtime attempt")
	}
}

func TestAuthorizationSnapshotsRemainIndependentDuringWorkerUpdates(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	stamp, _ := store.BeginAuthorizationAttempt(id)
	var worker sync.WaitGroup
	worker.Add(1)
	defer worker.Wait()
	go func() {
		defer worker.Done()
		for i := 0; i < 100; i++ {
			store.UpdateWorker(id, func(e *PrequeueEntry) {
				e.TargetEpisode.Numbering.Ordering = "official"
				e.TrackSelection.Audio.Language = "fra"
				e.MediaEvidence.TargetEpisode.Image.URL = "https://example.invalid/evidence"
			})
		}
	}()
	for i := 0; i < 100; i++ {
		entry, current, ok := store.SnapshotForAuthorization(id)
		if !ok || current != stamp || entry.TargetEpisode.Numbering.Ordering != "official" || entry.TrackSelection.Audio.Language != "fra" || entry.MediaEvidence.TargetEpisode.Image.URL != "https://example.invalid/evidence" {
			t.Fatal("snapshot leaked another reader's mutation during worker updates")
		}
		entry.TargetEpisode.Numbering.Ordering = "foreign"
		entry.TrackSelection.Audio.Language = "eng"
		entry.MediaEvidence.TargetEpisode.Image.URL = "https://example.invalid/foreign"
	}
}

func TestAuthorizationSnapshotRetainsRetirementAndMigrationFlags(t *testing.T) {
	for _, flag := range []string{"retired", "migrated"} {
		t.Run(flag, func(t *testing.T) {
			store, id := authorizationSnapshotFixture(t)
			store.Update(id, func(e *PrequeueEntry) { e.ReuseDisabled = flag == "retired"; e.MigrationAdopted = flag == "migrated" })
			entry, _, ok := store.SnapshotForAuthorization(id)
			if !ok || entry.ReuseDisabled != (flag == "retired") || entry.MigrationAdopted != (flag == "migrated") {
				t.Fatal("snapshot concealed an unsafe reuse flag")
			}
			if stamp, ok := store.BeginAuthorizationAttempt(id); ok || stamp.Valid() {
				t.Fatal("retired/imported input gained a worker attempt")
			}
		})
	}
}

func TestAuthorizationAttemptCannotSurviveJSONOrActualRestore(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	stamp, _ := store.BeginAuthorizationAttempt(id)
	encoded, err := json.Marshal(stamp)
	if err != nil || string(encoded) != "null" {
		t.Fatal("private attempt was serialized")
	}
	for _, raw := range []string{"null", "{}", `{"attempt":1,"generation":0}`} {
		var imported AuthorizationStamp
		if err := json.Unmarshal([]byte(raw), &imported); err == nil || imported.Valid() {
			t.Fatal("serialized data minted a runtime attempt")
		}
	}
	entry, _, _ := store.SnapshotForAuthorization(id)
	encoded, err = json.Marshal([]*PrequeueEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bytes.ToLower(encoded), []byte("authorizationattempt")) {
		t.Fatal("entry serialization exposed a private worker marker")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "prequeue.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	restored := NewPrequeueStore(time.Hour)
	restored.SetStoragePath(dir)
	if err := restored.RestorePersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	copy, imported, ok := restored.SnapshotForAuthorization(id)
	if !ok || copy.StreamPath != entry.StreamPath || imported.Valid() || imported == stamp {
		t.Fatal("disk restore recreated a private runtime attempt")
	}
}

func TestAuthorizationSnapshotDoesNotWriteOrReloadRepository(t *testing.T) {
	store, id := authorizationSnapshotFixture(t)
	repo := &recordingPrequeueRepository{}
	store.repo = repo
	if _, ok := store.BeginAuthorizationAttempt(id); !ok {
		t.Fatal("attempt missing")
	}
	if _, _, ok := store.SnapshotForAuthorization(id); !ok {
		t.Fatal("snapshot missing")
	}
	if repo.ListCalls() != 0 || len(repo.DeletedIDs()) != 0 || len(repo.upsertIDs) != 0 {
		t.Fatal("cache-only authorization touched persistence")
	}
}
