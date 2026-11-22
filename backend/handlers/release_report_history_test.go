package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"novastream/models"
	"novastream/services/history"
	"novastream/services/playback"
)

func reportHistoryService(t *testing.T, h *PrequeueHandler) *history.Service {
	t.Helper()
	svc, err := history.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.historySvc = svc
	return svc
}

func seedReportedHistory(t *testing.T, svc *history.Service, profile, item, series string, season, episode int) {
	t.Helper()
	kind := "movie"
	if series != "" {
		kind = "episode"
	}
	watched := true
	if _, err := svc.UpdateWatchHistory(profile, models.WatchHistoryUpdate{MediaType: kind, ItemID: item, SeriesID: series, SeasonNumber: season, EpisodeNumber: episode, Watched: &watched}); err != nil {
		t.Fatal(err)
	}
	// An in-progress rewatch can coexist with a previously watched item.
	if _, err := svc.UpdatePlaybackProgress(profile, models.PlaybackProgressUpdate{MediaType: kind, ItemID: item, SeriesID: series, SeasonNumber: season, EpisodeNumber: episode, Position: 120, Duration: 600}); err != nil {
		t.Fatal(err)
	}
}

func assertReportedHistory(t *testing.T, svc *history.Service, profile, kind, item string, watched, progress bool) {
	t.Helper()
	row, err := svc.GetWatchHistoryItem(profile, kind, item)
	if err != nil || row == nil || row.Watched != watched {
		t.Fatalf("watch state for %s/%s: %+v, %v", profile, item, row, err)
	}
	position, err := svc.GetPlaybackProgress(profile, kind, item)
	if err != nil || (position != nil) != progress {
		t.Fatalf("progress for %s/%s: %+v, %v", profile, item, position, err)
	}
}

func TestReleaseReportExplicitHistoryReset(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchecked-preserves-history", true: "explicit-reset-only-reported-profile"}[reset], func(t *testing.T) {
			h, entry, registry := releaseReportFixture(t)
			svc := reportHistoryService(t, h)
			seedReportedHistory(t, svc, entry.UserID, entry.TitleID, "", 0, 0)
			seedReportedHistory(t, svc, "other-profile", entry.TitleID, "", 0, 0)
			w := httptest.NewRecorder()
			h.ReportRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{Reason: "wrong_title", SourceVersion: sourceReportVersion(entry), ResetHistory: reset}, false))
			if w.Code != http.StatusOK || len(registry.List()) != 1 {
				t.Fatalf("report failed: %d %s", w.Code, w.Body.String())
			}
			var reply struct {
				HistoryReset bool `json:"historyReset"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.HistoryReset != reset {
				t.Fatalf("reset acknowledgement: %s %v", w.Body.String(), err)
			}
			assertReportedHistory(t, svc, entry.UserID, "movie", entry.TitleID, !reset, !reset)
			assertReportedHistory(t, svc, "other-profile", "movie", entry.TitleID, true, true)
		})
	}
}

func TestReleaseReportEpisodeResetPreservesOtherEpisodes(t *testing.T) {
	h, entry, _ := releaseReportFixture(t)
	svc := reportHistoryService(t, h)
	series := "tmdb:tv:321"
	selected := history.EpisodeItemID(series, nil, 1, 2)
	previous := history.EpisodeItemID(series, nil, 1, 1)
	next := history.EpisodeItemID(series, nil, 1, 3)
	for _, item := range []struct {
		id     string
		number int
	}{{previous, 1}, {selected, 2}, {next, 3}} {
		seedReportedHistory(t, svc, "profile", item.id, series, 1, item.number)
	}
	// Restore previous positions after marking later episodes watched, whose
	// existing normal completion behavior removes earlier progress.
	for _, item := range []struct {
		id     string
		number int
	}{{previous, 1}, {selected, 2}} {
		if _, err := svc.UpdatePlaybackProgress("profile", models.PlaybackProgressUpdate{MediaType: "episode", ItemID: item.id, SeriesID: series, SeasonNumber: 1, EpisodeNumber: item.number, Position: 120, Duration: 600}); err != nil {
			t.Fatal(err)
		}
	}
	h.store.Update(entry.ID, func(e *playback.PrequeueEntry) {
		e.TitleID, e.MediaType = series, "series"
		e.TargetEpisode = &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, ItemID: selected, Title: "Reported episode"}
	})
	entry, _ = h.store.Get(entry.ID)
	w := httptest.NewRecorder()
	h.ReportRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{Reason: "wrong_episode", SourceVersion: sourceReportVersion(entry), ResetHistory: true}, false))
	if w.Code != http.StatusOK {
		t.Fatalf("report failed: %d %s", w.Code, w.Body.String())
	}
	assertReportedHistory(t, svc, "profile", "episode", selected, false, false)
	assertReportedHistory(t, svc, "profile", "episode", previous, true, true)
	assertReportedHistory(t, svc, "profile", "episode", next, true, true)
}

func TestReleaseReportHistoryResetRequiresWrongContent(t *testing.T) {
	for _, reason := range []string{"missing_language", "quality_problem", ""} {
		t.Run(map[string]string{"missing_language": "language", "quality_problem": "quality", "": "manual-retry"}[reason], func(t *testing.T) {
			h, entry, registry := releaseReportFixture(t)
			svc := reportHistoryService(t, h)
			seedReportedHistory(t, svc, "profile", entry.TitleID, "", 0, 0)
			w := httptest.NewRecorder()
			r := reportRequest(t, "POST", entry.ID, releaseReportRequest{Reason: reason, SourceVersion: sourceReportVersion(entry), ResetHistory: true}, false)
			if reason == "" {
				h.RetryRelease(w, r)
			} else {
				h.ReportRelease(w, r)
			}
			if w.Code != http.StatusBadRequest || len(registry.List()) != 0 {
				t.Fatalf("unexpected action: %d %s", w.Code, w.Body.String())
			}
			assertReportedHistory(t, svc, "profile", "movie", entry.TitleID, true, true)
		})
	}
}

func TestReleaseReportHistoryResetMissingService(t *testing.T) {
	h, entry, registry := releaseReportFixture(t)
	w := httptest.NewRecorder()
	h.ReportRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{Reason: "wrong_title", SourceVersion: sourceReportVersion(entry), ResetHistory: true}, false))
	if w.Code != http.StatusServiceUnavailable || len(registry.List()) != 0 {
		t.Fatalf("missing history service claimed success: %d %s", w.Code, w.Body.String())
	}
}

func TestReleaseReportHistoryResetFailureDoesNotClaimSuccess(t *testing.T) {
	h, entry, registry := releaseReportFixture(t)
	dir := t.TempDir()
	svc, err := history.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	h.historySvc = svc
	seedReportedHistory(t, svc, entry.UserID, entry.TitleID, "", 0, 0)
	// A real failed persistence operation, rather than a fabricated reset result.
	name := filepath.Join(dir, "watched_items.json")
	if err := os.Rename(name, name+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ReportRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{Reason: "wrong_title", SourceVersion: sourceReportVersion(entry), ResetHistory: true, Retry: true}, false))
	var reply struct {
		HistoryReset      bool                       `json:"historyReset"`
		HistoryResetError string                     `json:"historyResetError"`
		Replacement       *playback.PrequeueResponse `json:"replacement"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &reply) != nil || reply.HistoryReset || reply.HistoryResetError != "history_reset_failed" || reply.Replacement != nil || len(registry.List()) != 1 {
		t.Fatalf("partial reset outcome was hidden: %d %s", w.Code, w.Body.String())
	}
}

func TestReleaseReportHistoryWriterRules(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required for the player history reset checks")
	}
	cmd := exec.CommandContext(t.Context(), node, "--test", "testdata/release_report_history.test.cjs")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("player history reset checks: %v\n%s", err, output)
	}
}
