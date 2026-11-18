package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"novastream/models"
)

func popularWatchesSnapshotMust(t *testing.T, service *Service, viewer string, users map[string]models.User, days int, at time.Time) *RecentWatchesSnapshot {
	t.Helper()
	snapshot, err := service.SnapshotPopularWatches(context.Background(), viewer, users, days, at)
	if err != nil {
		t.Fatalf("popular snapshot: %v", err)
	}
	return snapshot
}

func TestPopularWatchesSnapshotKeepsSeparatePublicWindows(t *testing.T) {
	service := &Service{}
	at := time.Now().UTC()
	for _, days := range []int{-1, 0, 1, 6, 7, 14, 90, 91, 365, 366} {
		t.Run(fmt.Sprintf("days=%d", days), func(t *testing.T) {
			popular, popularErr := service.SnapshotPopularWatches(context.Background(), "viewer", nil, days, at)
			if days >= 7 && days <= 365 {
				if popular == nil || popularErr != nil {
					t.Fatalf("popular window %d: snapshot=%v error=%v", days, popular, popularErr)
				}
			} else if popular != nil || popularErr == nil {
				t.Fatalf("invalid popular window %d: snapshot=%v error=%v", days, popular, popularErr)
			}
			recent, recentErr := service.SnapshotRecentWatches(context.Background(), "viewer", nil, days, at)
			if days >= 1 && days <= 90 {
				if recent == nil || recentErr != nil {
					t.Fatalf("recent window changed at %d: snapshot=%v error=%v", days, recent, recentErr)
				}
			} else if recent != nil || recentErr == nil {
				t.Fatalf("invalid recent window %d: snapshot=%v error=%v", days, recent, recentErr)
			}
		})
	}
}

func TestPopularWatchesSnapshotCopiesCompleteLongWindowAndPrivacy(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.FixedZone("synthetic", 3600))
	cutoff := at.UTC().AddDate(0, 0, -365)
	row := func(id string, when time.Time, watched bool, seconds float64) models.WatchHistoryItem {
		return models.WatchHistoryItem{MediaType: "movie", ItemID: id, Name: "Stored film", Watched: watched, WatchedAt: when, WatchedSeconds: seconds}
	}
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{"alice": {}}}
	for i := 0; i < 620; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", 100000+i)
		service.watchHistory["alice"][id] = row(id, at.AddDate(0, 0, -180).Add(-time.Duration(i)*time.Minute), true, 20)
	}
	service.watchHistory["alice"]["boundary"] = row("tmdb:movie:1", cutoff, true, 1)
	service.watchHistory["alice"]["old"] = row("tmdb:movie:2", cutoff.Add(-time.Nanosecond), true, 1)
	service.watchHistory["alice"]["not-watched"] = row("tmdb:movie:3", at, false, 20)
	service.watchHistory["alice"]["manual"] = row("tmdb:movie:4", at, true, 0)
	service.watchHistory["alice"]["negative"] = row("tmdb:movie:5", at, true, -1)
	service.watchHistory["alice"]["nan"] = row("tmdb:movie:6", at, true, math.NaN())
	// Keep the existing lower-bound-only window policy for future played rows.
	service.watchHistory["alice"]["future"] = row("tmdb:movie:7", at.Add(time.Hour), true, 1)
	for user, id := range map[string]string{"anonymous": "8", "private": "9", "unknown": "10", "not-listed": "11"} {
		service.watchHistory[user] = map[string]models.WatchHistoryItem{"film": row("tmdb:movie:"+id, at, true, 1)}
	}
	users := recentWatchesSnapshotUsers()
	users["anonymous"] = models.User{ID: "anonymous", Name: "PRIVATE_ANONYMOUS_NAME", ActivityPrivacy: " SHARED_ANONYMOUS "}
	users["private"] = models.User{ID: "private", Name: "Private", ActivityPrivacy: models.ActivityPrivacyNotShared}
	users["unknown"] = models.User{ID: "unknown", ActivityPrivacy: "unexpected"}
	snapshot := popularWatchesSnapshotMust(t, service, "viewer", users, 365, at)
	if len(snapshot.Items) != 623 || !snapshot.ObservedAt.Equal(at) || snapshot.ObservedAt.Location() != time.UTC {
		t.Fatalf("complete long-window count=%d observation=%v", len(snapshot.Items), snapshot.ObservedAt)
	}
	byID := map[string]RecentWatchSnapshotItem{}
	for _, item := range snapshot.Items {
		byID[item.Watch.ItemID] = item
	}
	for _, id := range []string{"tmdb:movie:1", "tmdb:movie:7", "tmdb:movie:100619"} {
		if _, found := byID[id]; !found {
			t.Fatalf("complete eligible window lost %s", id)
		}
	}
	anonymous := byID["tmdb:movie:8"]
	if anonymous.Group != 2 || anonymous.Watch.UserID != "" || anonymous.Watch.UserName != "Fellow user" || !anonymous.Watch.IsAnonymous {
		t.Fatal("anonymous projection disclosed contributor identity")
	}
	encoded, err := json.Marshal(anonymous.Watch)
	if err != nil || strings.Contains(string(encoded), "PRIVATE_ANONYMOUS_NAME") || strings.Contains(string(encoded), `"userId"`) {
		t.Fatalf("anonymous watch escaped: %s error=%v", encoded, err)
	}
	for _, value := range []any{anonymous, snapshot} {
		encoded, err := json.Marshal(value)
		if err != nil || string(encoded) != "{}" {
			t.Fatal("shared snapshot bookkeeping became public")
		}
	}
	if !service.RecentWatchesSnapshotCurrent(snapshot) {
		t.Fatal("new popular snapshot lost its actual history owner")
	}
}

func TestPopularWatchesSnapshotOwnsCopiedDataWithoutMutation(t *testing.T) {
	at := time.Now().UTC()
	sideIDs := map[string]string{"tmdb": "42", "imdb": "tt0000042"}
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{
		"alice": {"film": {MediaType: "movie", ItemID: "tmdb:movie:42", Name: "Stored film", Watched: true, WatchedAt: at.AddDate(0, 0, -180), WatchedSeconds: 20, ExternalIDs: sideIDs}},
	}}
	service.metadataService = recentWatchesSnapshotPoisonMetadata{}
	service.SetWatchStateChangedHook(func(string) { panic("popular snapshot must not publish mutation") })
	first := popularWatchesSnapshotMust(t, service, "viewer", recentWatchesSnapshotUsers(), 365, at)
	second := popularWatchesSnapshotMust(t, service, "viewer", recentWatchesSnapshotUsers(), 365, at)
	first.Items[0].Watch.Name = "Caller change"
	first.Items[0].Watch.ExternalIDs["tmdb"] = "999"
	service.mu.Lock()
	sideIDs["imdb"] = "later-service-value"
	service.mu.Unlock()
	if sideIDs["tmdb"] != "42" || second.Items[0].Watch.Name != "Stored film" || second.Items[0].Watch.ExternalIDs["tmdb"] != "42" || second.Items[0].Watch.ExternalIDs["imdb"] != "tt0000042" {
		t.Fatal("popular snapshots alias service data or one another")
	}
	if service.watchStateSeq != 0 || service.userWatchStateRev != nil || service.continueWatchingPools != nil {
		t.Fatal("popular snapshot initialized or mutated history state")
	}
}

func TestPopularWatchesSnapshotOwnerTracksActualRelevantMutations(t *testing.T) {
	at := time.Now().UTC()
	watched := true
	update := func(id string) models.WatchHistoryUpdate {
		return models.WatchHistoryUpdate{MediaType: "movie", ItemID: id, Name: "Stored film", Watched: &watched, WatchedAt: at.AddDate(0, 0, -180), WatchedSeconds: 20}
	}
	for _, change := range []string{"contributor", "viewer", "empty-shared", "private", "delete", "clear", "import", "bulk", "reload"} {
		t.Run(change, func(t *testing.T) {
			service, err := NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.UpdateWatchHistory("alice", update("tmdb:movie:1")); err != nil {
				t.Fatal(err)
			}
			users := recentWatchesSnapshotUsers()
			users["empty"] = models.User{ID: "empty", ActivityPrivacy: models.ActivityPrivacyShared}
			snapshot := popularWatchesSnapshotMust(t, service, "viewer", users, 365, at)
			if len(snapshot.Items) != 1 || !service.RecentWatchesSnapshotCurrent(snapshot) || (&Service{}).RecentWatchesSnapshotCurrent(snapshot) {
				t.Fatal("popular snapshot owner or long-window source is invalid")
			}
			switch change {
			case "contributor", "viewer", "empty-shared", "private":
				user := map[string]string{"contributor": "alice", "viewer": "viewer", "empty-shared": "empty", "private": "not-shared"}[change]
				_, err = service.UpdateWatchHistory(user, update("tmdb:movie:2"))
			case "delete":
				err = service.DeleteWatchHistoryItem("alice", "movie", "tmdb:movie:1")
			case "clear":
				_, err = service.ClearWatchHistory()
			case "import":
				_, err = service.ImportWatchHistory("alice", []models.WatchHistoryUpdate{update("tmdb:movie:2")})
			case "bulk":
				_, err = service.BulkUpdateWatchHistory("alice", []models.WatchHistoryUpdate{update("tmdb:movie:2")})
			case "reload":
				err = service.loadWatchHistory()
			}
			if err != nil {
				t.Fatal(err)
			}
			if current := service.RecentWatchesSnapshotCurrent(snapshot); current != (change == "private") {
				t.Fatalf("current=%t after %s", current, change)
			}
		})
	}
}

func TestPopularWatchesSnapshotCancellationAndLockBoundary(t *testing.T) {
	service := &Service{}
	at := time.Now().UTC()
	ctx := &recentWatchesLockCheckingContext{Context: context.Background(), service: service}
	if _, err := service.SnapshotPopularWatches(ctx, "viewer", recentWatchesSnapshotUsers(), 365, at); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if snapshot, err := service.SnapshotPopularWatches(canceled, "viewer", recentWatchesSnapshotUsers(), 365, at); snapshot != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled popular read returned snapshot=%v error=%v", snapshot, err)
	}
}

func TestPopularWatchesSnapshotRejectsInvalidArguments(t *testing.T) {
	at := time.Now().UTC()
	for _, tc := range []struct {
		name, viewer string
		ctx          context.Context
		at           time.Time
		users        map[string]models.User
	}{
		{"blank-viewer", "", context.Background(), at, nil},
		{"whitespace-viewer", " viewer ", context.Background(), at, nil},
		{"nil-context", "viewer", nil, at, nil},
		{"zero-observation", "viewer", context.Background(), time.Time{}, nil},
		{"wrong-contributor-key", "viewer", context.Background(), at, map[string]models.User{"alice": {ID: "other", ActivityPrivacy: models.ActivityPrivacyShared}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if snapshot, err := (&Service{}).SnapshotPopularWatches(tc.ctx, tc.viewer, tc.users, 365, tc.at); snapshot != nil || err == nil {
				t.Fatal("invalid popular snapshot arguments were accepted")
			}
		})
	}
	if snapshot, err := (*Service)(nil).SnapshotPopularWatches(context.Background(), "viewer", nil, 365, at); snapshot != nil || err == nil {
		t.Fatal("nil popular history service was accepted")
	}
}
