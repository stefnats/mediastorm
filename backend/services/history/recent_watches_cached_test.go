package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/models"
)

func recentWatchesSnapshotUsers() map[string]models.User {
	return map[string]models.User{
		"alice": {ID: "alice", Name: "Alice", ActivityPrivacy: models.ActivityPrivacyShared},
	}
}

func recentWatchesSnapshotMust(t *testing.T, service *Service, viewer string, users map[string]models.User, at time.Time) *RecentWatchesSnapshot {
	t.Helper()
	snapshot, err := service.SnapshotRecentWatches(context.Background(), viewer, users, 14, at)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snapshot
}

func TestRecentWatchesSnapshotKeepsAllRowsBeforeCap(t *testing.T) {
	at := time.Now().UTC()
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{"alice": {}}}
	for i := 0; i < 620; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", 100000+i)
		service.watchHistory["alice"]["movie:"+id] = models.WatchHistoryItem{
			MediaType: "movie", ItemID: id, Name: fmt.Sprintf("Stored title %d", i),
			Watched: true, WatchedAt: at.Add(-time.Duration(i) * time.Minute), WatchedSeconds: 10,
		}
	}
	users := recentWatchesSnapshotUsers()
	snapshot := recentWatchesSnapshotMust(t, service, "viewer", users, at)
	if len(snapshot.Items) != 620 {
		t.Fatalf("snapshot retained %d rows, want all 620", len(snapshot.Items))
	}
	if last := snapshot.Items[619]; last.Watch.ItemID != "tmdb:movie:100619" || last.Group != 1 {
		t.Fatalf("late stored row unavailable before filtering/cap: %+v", last)
	}
	if legacy := service.ListRecentWatches(users, 14, 3); len(legacy) != 3 {
		t.Fatalf("legacy cap changed: got %d, want 3", len(legacy))
	}
	if !service.RecentWatchesSnapshotCurrent(snapshot) {
		t.Fatal("unchanged snapshot is not current")
	}
}

func TestRecentWatchesSnapshotPlayedWindowAndPrivacy(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.FixedZone("synthetic", 3600))
	cutoff := at.UTC().AddDate(0, 0, -14)
	row := func(id string, when time.Time, watched bool, seconds float64) models.WatchHistoryItem {
		return models.WatchHistoryItem{MediaType: "movie", ItemID: id, Name: "Stored film", Watched: watched, WatchedAt: when, WatchedSeconds: seconds}
	}
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{
		"alice": {
			"boundary":    row("tmdb:movie:1", cutoff, true, 1),
			"old":         row("tmdb:movie:2", cutoff.Add(-time.Nanosecond), true, 1),
			"not-watched": row("tmdb:movie:3", at, false, 20),
			"marked":      row("tmdb:movie:4", at, true, 0),
			"negative":    row("tmdb:movie:5", at, true, -1),
			"nan":         row("tmdb:movie:6", at, true, math.NaN()),
			// The existing window has only a lower bound; keep a played future row.
			"future": row("tmdb:movie:7", at.Add(time.Hour), true, 1),
		},
		"anonymous":  {"movie": row("tmdb:movie:8", at, true, 5)},
		"private":    {"movie": row("tmdb:movie:9", at, true, 5)},
		"unknown":    {"movie": row("tmdb:movie:10", at, true, 5)},
		"not-listed": {"movie": row("tmdb:movie:11", at, true, 5)},
	}}
	users := recentWatchesSnapshotUsers()
	users["anonymous"] = models.User{ID: "anonymous", Name: "Private name", ActivityPrivacy: " SHARED_ANONYMOUS "}
	users["private"] = models.User{ID: "private", Name: "Private", ActivityPrivacy: models.ActivityPrivacyNotShared}
	users["unknown"] = models.User{ID: "unknown", Name: "Unknown", ActivityPrivacy: "unexpected"}
	snapshot := recentWatchesSnapshotMust(t, service, "viewer", users, at)
	if len(snapshot.Items) != 3 || !snapshot.ObservedAt.Equal(at) || snapshot.ObservedAt.Location() != time.UTC {
		t.Fatalf("window/privacy projection: %+v", snapshot)
	}
	want := []string{"tmdb:movie:7", "tmdb:movie:8", "tmdb:movie:1"}
	for i, item := range snapshot.Items {
		if item.Watch.ItemID != want[i] {
			t.Fatalf("item %d=%q, want %q", i, item.Watch.ItemID, want[i])
		}
	}
	anonymous := snapshot.Items[1]
	if anonymous.Group != 2 || anonymous.Watch.UserID != "" || anonymous.Watch.UserName != "Fellow user" || !anonymous.Watch.IsAnonymous {
		t.Fatalf("anonymous profile identity escaped: %+v", anonymous)
	}
	encoded, err := json.Marshal(anonymous.Watch)
	if err != nil || strings.Contains(string(encoded), "anonymous") || strings.Contains(string(encoded), "Private name") {
		t.Fatalf("anonymous wire projection: %s, %v", encoded, err)
	}
	for _, value := range []any{anonymous, snapshot} {
		encoded, err = json.Marshal(value)
		if err != nil || string(encoded) != "{}" {
			t.Fatalf("private snapshot bookkeeping became a wire contract: %s, %v", encoded, err)
		}
	}
}

func TestRecentWatchesSnapshotOwnsProjectionAndDoesNotWrite(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	ids := map[string]string{"imdb": "tt0000001"}
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{
		"alice": {"movie": {MediaType: "movie", ItemID: "tmdb:movie:1", Name: "Original", Watched: true, WatchedAt: at, WatchedSeconds: 5, ExternalIDs: ids}},
	}}
	// Any metadata method would panic through this nil embedded interface.
	service.metadataService = recentWatchesSnapshotPoisonMetadata{}
	service.SetWatchStateChangedHook(func(string) { panic("snapshot must not publish a history mutation") })
	users := recentWatchesSnapshotUsers()
	snapshot := recentWatchesSnapshotMust(t, service, "viewer", users, at)
	other := recentWatchesSnapshotMust(t, service, "viewer", users, at)
	snapshot.Items[0].Watch.ExternalIDs["imdb"] = "caller-change"
	snapshot.Items[0].Watch.Name = "Caller title"
	user := users["alice"]
	user.Name = "Later caller name"
	users["alice"] = user
	if ids["imdb"] != "tt0000001" || other.Items[0].Watch.ExternalIDs["imdb"] != "tt0000001" || other.Items[0].Watch.Name != "Original" || other.Items[0].Watch.UserName != "Alice" {
		t.Fatal("returned snapshot aliases service or caller projection state")
	}
	service.mu.Lock()
	ids["imdb"] = "later-service-value"
	service.mu.Unlock()
	if other.Items[0].Watch.ExternalIDs["imdb"] != "tt0000001" {
		t.Fatal("later backing-map change altered an already owned snapshot")
	}
	if service.watchStateSeq != 0 || service.userWatchStateRev != nil || service.continueWatchingPools != nil {
		t.Fatal("read-only snapshot initialized or changed service state")
	}
}

type recentWatchesSnapshotPoisonMetadata struct{ MetadataService }

func TestRecentWatchesSnapshotStableTies(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	row := func(id, name string) models.WatchHistoryItem {
		return models.WatchHistoryItem{MediaType: "movie", ItemID: id, Name: name, Watched: true, WatchedAt: at, WatchedSeconds: 1}
	}
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{
		"alice": {"second-key": row("tmdb:movie:1", "Second"), "first-key": row("tmdb:movie:1", "First"), "third": row("tmdb:movie:2", "Third")},
		"zulu":  {"first": row("tmdb:movie:1", "Anonymous film")},
	}}
	users := recentWatchesSnapshotUsers()
	users["zulu"] = models.User{ID: "zulu", Name: "Secret", ActivityPrivacy: models.ActivityPrivacySharedAnonymous}
	first := recentWatchesSnapshotMust(t, service, "viewer", users, at)
	var actual []string
	for _, item := range first.Items {
		actual = append(actual, fmt.Sprintf("%d/%s", item.Group, item.Watch.Name))
	}
	want := []string{"1/First", "1/Second", "2/Anonymous film", "1/Third"}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("stable identity/group/key ordering=%v, want %v", actual, want)
	}
	for i := 0; i < 20; i++ {
		if next := recentWatchesSnapshotMust(t, service, "viewer", users, at); !reflect.DeepEqual(first.Items, next.Items) {
			t.Fatal("map iteration changed tied snapshot ordering")
		}
	}
}

func TestRecentWatchesSnapshotCurrentOwnershipAndActualMutations(t *testing.T) {
	at := time.Now().UTC()
	watched := true
	update := func(id string) models.WatchHistoryUpdate {
		return models.WatchHistoryUpdate{MediaType: "movie", ItemID: id, Name: "Stored film", Watched: &watched, WatchedAt: at, WatchedSeconds: 20}
	}
	cases := []struct {
		name    string
		mutate  func(*Service) error
		current bool
	}{
		{"contributor-update", func(s *Service) error { _, err := s.UpdateWatchHistory("alice", update("tmdb:movie:2")); return err }, false},
		{"viewer-only-update", func(s *Service) error { _, err := s.UpdateWatchHistory("viewer", update("tmdb:movie:2")); return err }, false},
		{"empty-eligible-contributor-update", func(s *Service) error { _, err := s.UpdateWatchHistory("empty", update("tmdb:movie:2")); return err }, false},
		{"unrelated-private-update", func(s *Service) error { _, err := s.UpdateWatchHistory("outsider", update("tmdb:movie:2")); return err }, true},
		{"delete", func(s *Service) error { return s.DeleteWatchHistoryItem("alice", "movie", "tmdb:movie:1") }, false},
		{"clear", func(s *Service) error { _, err := s.ClearWatchHistory(); return err }, false},
		{"import", func(s *Service) error {
			_, err := s.ImportWatchHistory("alice", []models.WatchHistoryUpdate{update("tmdb:movie:2")})
			return err
		}, false},
		{"bulk", func(s *Service) error {
			_, err := s.BulkUpdateWatchHistory("alice", []models.WatchHistoryUpdate{update("tmdb:movie:2")})
			return err
		}, false},
		{"whole-store-load", func(s *Service) error { return s.loadWatchHistory() }, false},
		{"metadata-swap", func(s *Service) error { s.SetMetadataService(nil); return nil }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, err := NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.ImportWatchHistory("alice", []models.WatchHistoryUpdate{update("tmdb:movie:1")}); err != nil {
				t.Fatal(err)
			}
			users := recentWatchesSnapshotUsers()
			users["empty"] = models.User{ID: "empty", Name: "Empty", ActivityPrivacy: models.ActivityPrivacyShared}
			snapshot := recentWatchesSnapshotMust(t, service, "viewer", users, at)
			if !service.RecentWatchesSnapshotCurrent(snapshot) {
				t.Fatal("fresh snapshot is not current")
			}
			if (&Service{}).RecentWatchesSnapshotCurrent(snapshot) || service.RecentWatchesSnapshotCurrent(nil) || service.RecentWatchesSnapshotCurrent(&RecentWatchesSnapshot{ObservedAt: at}) {
				t.Fatal("foreign, nil or constructed snapshot treated as owned")
			}
			if err := tc.mutate(service); err != nil {
				t.Fatalf("actual mutation: %v", err)
			}
			if got := service.RecentWatchesSnapshotCurrent(snapshot); got != tc.current {
				t.Fatalf("current=%v, want%v", got, tc.current)
			}
		})
	}
}

type recentWatchesLockCheckingContext struct {
	context.Context
	service *Service
}

func (c *recentWatchesLockCheckingContext) unlocked() {
	if !c.service.mu.TryLock() {
		panic("context callback called under the history mutex")
	}
	c.service.mu.Unlock()
}
func (c *recentWatchesLockCheckingContext) Done() <-chan struct{} {
	c.unlocked()
	return c.Context.Done()
}
func (c *recentWatchesLockCheckingContext) Err() error { c.unlocked(); return c.Context.Err() }

type recentWatchesObservedContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *recentWatchesObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestRecentWatchesSnapshotCancellationAndContextLockBoundary(t *testing.T) {
	at := time.Now().UTC()
	service := &Service{watchHistory: map[string]map[string]models.WatchHistoryItem{"alice": {}}}
	ctx := &recentWatchesLockCheckingContext{Context: context.Background(), service: service}
	if _, err := service.SnapshotRecentWatches(ctx, "viewer", recentWatchesSnapshotUsers(), 14, at); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if snapshot, err := service.SnapshotRecentWatches(canceled, "viewer", recentWatchesSnapshotUsers(), 14, at); snapshot != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled snapshot=%v err=%v", snapshot, err)
	}

	// A read begun while a writer holds history must not publish after cancellation.
	blocked, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneObserved := make(chan struct{})
	waiting := &recentWatchesObservedContext{Context: blocked, observed: doneObserved}
	service.mu.Lock()
	locked := true
	defer func() {
		if locked {
			service.mu.Unlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, err := service.SnapshotRecentWatches(waiting, "viewer", recentWatchesSnapshotUsers(), 14, at)
		result <- err
	}()
	select {
	case <-doneObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot did not begin")
	}
	cancel()
	service.mu.Unlock()
	locked = false
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting snapshot error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled snapshot did not finish")
	}
}

func TestRecentWatchesSnapshotRejectsInvalidArguments(t *testing.T) {
	at := time.Now().UTC()
	service := &Service{}
	for _, tc := range []struct {
		name, viewer string
		days         int
		at           time.Time
		users        map[string]models.User
	}{
		{"blank-viewer", "", 14, at, nil}, {"whitespace-viewer", " viewer ", 14, at, nil},
		{"zero-window", "viewer", 0, at, nil}, {"large-window", "viewer", 91, at, nil},
		{"zero-observation", "viewer", 14, time.Time{}, nil},
		{"contributor-key-mismatch", "viewer", 14, at, map[string]models.User{"alice": {ID: "other", ActivityPrivacy: models.ActivityPrivacyShared}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := service.SnapshotRecentWatches(context.Background(), tc.viewer, tc.users, tc.days, tc.at); got != nil || err == nil {
				t.Fatalf("invalid request returned snapshot=%v err=%v", got, err)
			}
		})
	}
	if got, err := service.SnapshotRecentWatches(nil, "viewer", nil, 14, at); got != nil || err == nil {
		t.Fatal("nil context accepted")
	}
	if got, err := (*Service)(nil).SnapshotRecentWatches(context.Background(), "viewer", nil, 14, at); got != nil || err == nil {
		t.Fatal("nil service accepted")
	}
	if (*Service)(nil).RecentWatchesSnapshotCurrent(&RecentWatchesSnapshot{}) {
		t.Fatal("nil service current accepted")
	}
}
