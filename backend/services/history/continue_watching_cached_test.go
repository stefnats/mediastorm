package history

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"novastream/models"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type cachedWatchingDenyTransport struct{ calls atomic.Int32 }

func (d *cachedWatchingDenyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("synthetic fixture forbids all provider HTTP")
}
func cachedWatchingGuard(t *testing.T) *cachedWatchingDenyTransport {
	t.Helper()
	guard := &cachedWatchingDenyTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = old
		if guard.calls.Load() != 0 {
			t.Errorf("unexpected HTTP demand: %d", guard.calls.Load())
		}
	})
	return guard
}

type cachedWatchingMovies struct {
	MetadataService
	calls atomic.Int32
}

func (m *cachedWatchingMovies) MovieInfo(_ context.Context, q models.MovieDetailsQuery) (*models.Title, error) {
	m.calls.Add(1)
	return &models.Title{ID: q.TitleID, Name: q.Name, Year: q.Year, TMDBID: q.TMDBID, MediaType: "movie", Poster: &models.Image{URL: "https://private.invalid/art?token=synthetic"}}, nil
}
func cachedWatchingService(t *testing.T, n int) (*Service, *cachedWatchingMovies) {
	t.Helper()
	cachedWatchingGuard(t) // MUST precede every service constructor.
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := &cachedWatchingMovies{}
	s.SetMetadataService(meta)
	s.playbackProgress["profile-a"] = map[string]models.PlaybackProgress{}
	now := time.Now().UTC()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", i)
		s.playbackProgress["profile-a"][id] = models.PlaybackProgress{ID: id, ItemID: id, MediaType: "movie", MovieName: fmt.Sprintf("Observed %04d", i), Year: 2000, ExternalIDs: map[string]string{"tmdb": strconv.Itoa(i)}, Position: 120, Duration: 1200, PercentWatched: 10, UpdatedAt: now}
	}
	return s, meta
}
func cachedWatchingWarm(t *testing.T, s *Service) *CachedContinueWatchingPool {
	t.Helper()
	if _, err := s.ListContinueWatching("profile-a"); err != nil {
		t.Fatal(err)
	}
	pool, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
func TestCachedContinueWatchingNormalProducerKeepsCompletePoolAndNeverRenews(t *testing.T) {
	s, meta := cachedWatchingService(t, 701)
	pool := cachedWatchingWarm(t, s)
	if len(pool.Items) != 701 || !pool.Complete || meta.calls.Load() != 701 {
		t.Fatalf("normal observation: rows=%d calls=%d", len(pool.Items), meta.calls.Load())
	}
	for i := 0; i < 3; i++ {
		next, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
		if err != nil || next.ID != pool.ID || !next.ObservedAt.Equal(pool.ObservedAt) || !next.ExpiresAt.Equal(pool.ExpiresAt) {
			t.Fatal("getter renewed or lost observation")
		}
	}
	if meta.calls.Load() != 701 {
		t.Fatal("getter invoked metadata")
	}
	if pool.Items[0].Resume == nil || pool.Items[0].Resume.Position != 120 {
		t.Fatal("normal resume missing")
	}
}
func TestCachedContinueWatchingUnavailableNeverBuilds(t *testing.T) {
	s, meta := cachedWatchingService(t, 1)
	for _, user := range []string{"profile-a", "profile-b", ""} {
		if _, err := s.GetCachedContinueWatchingPool(user, time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
			t.Fatal("missing pool became success")
		}
	}
	if meta.calls.Load() != 0 {
		t.Fatal("getter invoked normal producer")
	}
}
func TestCachedContinueWatchingExpiryRevisionAndOwner(t *testing.T) {
	s, _ := cachedWatchingService(t, 1)
	pool := cachedWatchingWarm(t, s)
	if _, err := s.GetCachedContinueWatchingPool("profile-a", pool.ExpiresAt); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
		t.Fatal("expiry accepted")
	}
	if _, err := s.GetCachedContinueWatchingPool("profile-b", time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
		t.Fatal("foreign owner accepted")
	}
	s.mu.Lock()
	s.bumpWatchStateRevisionLocked("profile-a")
	s.mu.Unlock()
	if s.CachedContinueWatchingPoolCurrent("profile-a", pool.ID, time.Now()) {
		t.Fatal("revision lost")
	}
	if _, err := s.GetCachedContinueWatchingPool("profile-a", time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
		t.Fatal("revision did not retire pool")
	}
}
func TestCachedContinueWatchingProjectionAndGetterDoNotShareMutableRows(t *testing.T) {
	s, _ := cachedWatchingService(t, 1)
	pool := cachedWatchingWarm(t, s)
	original := pool.Items[0].ExternalIDs["tmdb"]
	pool.Items[0].ExternalIDs["tmdb"] = "changed"
	pool.Items[0].Resume.Position = 999
	s.mu.Lock()
	s.continueWatchingCache["profile-a"].items[0].ExternalIDs["tmdb"] = "raw-mutated"
	s.mu.Unlock()
	next, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil || next.Items[0].ExternalIDs["tmdb"] != original || next.Items[0].Resume.Position != 120 {
		t.Fatal("observation shared mutable state")
	}
	raw := string(s.continueWatchingPools["profile-a"].encoded)
	if strings.Contains(raw, "private.invalid") || strings.Contains(raw, "poster") {
		t.Fatal("private artwork escaped whitelist")
	}
}
func TestCachedContinueWatchingEpisodeCoordinatesAndDeepCopy(t *testing.T) {
	s, _ := cachedWatchingService(t, 0)
	now := time.Now().UTC()
	row := models.SeriesWatchState{SeriesID: "tmdb:tv:7", SeriesTitle: "Observed show", UpdatedAt: now, ExternalIDs: map[string]string{"tmdb": "7"}, NextEpisode: &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:9", Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:8", Ordering: "official"}}, LastWatched: models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 1}}
	want := MergeProgressIntoContinueWatching([]models.SeriesWatchState{row}, nil)
	s.mu.Lock()
	s.cacheContinueWatchingLocked("profile-a", []models.SeriesWatchState{row}, s.airtimeState.generation)
	s.mu.Unlock()
	pool, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ep := pool.Items[0].NextEpisode
	if ep.ItemID != want[0].NextEpisode.ItemID || ep.EpisodeID != row.NextEpisode.EpisodeID || ep.Numbering.SeriesID != row.NextEpisode.Numbering.SeriesID {
		t.Fatal("episode target changed")
	}
	ep.Numbering.SeriesID = "changed"
	ep.EpisodeID = "changed"
	row.NextEpisode.Numbering.Ordering = "changed"
	again, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil || again.Items[0].NextEpisode.Numbering.Ordering != "official" || again.Items[0].NextEpisode.EpisodeID != "tvdb:episode:9" {
		t.Fatal("episode pointer shared")
	}
}
func TestCachedContinueWatchingEstimatedAirtimeRetainsShortOriginalExpiry(t *testing.T) {
	s, _ := cachedWatchingService(t, 0)
	now := time.Now().UTC()
	row := models.SeriesWatchState{SeriesID: "tmdb:tv:7", SeriesTitle: "Observed show", UpdatedAt: now, NextEpisode: &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, AirTimeEstimated: true}}
	s.mu.Lock()
	s.cacheContinueWatchingLocked("profile-a", []models.SeriesWatchState{row}, s.airtimeState.generation)
	s.mu.Unlock()
	pool, err := s.GetCachedContinueWatchingPool("profile-a", time.Now())
	if err != nil || pool.ExpiresAt.Sub(pool.ObservedAt) != 30*time.Second {
		t.Fatal("estimated airtime TTL changed")
	}
}
func TestCachedContinueWatchingRejectsUnsafeTargetsAndWholeOverBudgetPool(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []models.SeriesWatchState
	}{
		{"private-target", []models.SeriesWatchState{{SeriesID: "https://private.invalid/file?token=synthetic", SeriesTitle: "Observed", UpdatedAt: time.Now()}}},
		{"row-budget", make([]models.SeriesWatchState, maxContinueWatchingPoolItems+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := cachedWatchingService(t, 0)
			s.mu.Lock()
			s.cacheContinueWatchingLocked("profile-a", tc.rows, s.airtimeState.generation)
			s.mu.Unlock()
			if _, err := s.GetCachedContinueWatchingPool("profile-a", time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
				t.Fatal("invalid pool partially admitted")
			}
		})
	}
	t.Run("byte-budget", func(t *testing.T) {
		s, _ := cachedWatchingService(t, 0)
		rows := make([]models.SeriesWatchState, 1100)
		for i := range rows {
			rows[i] = models.SeriesWatchState{SeriesID: fmt.Sprintf("tmdb:movie:%d", i+1), SeriesTitle: strings.Repeat("x", 4096), UpdatedAt: time.Now()}
		}
		s.mu.Lock()
		s.cacheContinueWatchingLocked("profile-a", rows, s.airtimeState.generation)
		s.mu.Unlock()
		if _, err := s.GetCachedContinueWatchingPool("profile-a", time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
			t.Fatal("byte budget truncated pool")
		}
	})
}
func TestCachedContinueWatchingProfilePoolBudgetAndObservedEmpty(t *testing.T) {
	s, _ := cachedWatchingService(t, 0)
	for i := 0; i <= maxContinueWatchingProfilePools; i++ {
		s.mu.Lock()
		s.cacheContinueWatchingLocked(fmt.Sprintf("profile-%d", i), nil, s.airtimeState.generation)
		s.mu.Unlock()
	}
	if len(s.continueWatchingPools) != maxContinueWatchingProfilePools {
		t.Fatal("profile pool budget lost")
	}
	if _, err := s.GetCachedContinueWatchingPool("profile-0", time.Now()); !errors.Is(err, ErrContinueWatchingPoolUnavailable) {
		t.Fatal("oldest profile not evicted")
	}
	latest, err := s.GetCachedContinueWatchingPool(fmt.Sprintf("profile-%d", maxContinueWatchingProfilePools), time.Now())
	if err != nil || latest.Items == nil || len(latest.Items) != 0 {
		t.Fatal("observed empty not preserved")
	}
}
