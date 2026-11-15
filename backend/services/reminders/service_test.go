package reminders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"novastream/models"
)

type fixture struct {
	s       *Service
	repo    *FileRepository
	now     time.Time
	allowed map[string]bool
	title   models.Title
}

func setup(t *testing.T) *fixture {
	t.Helper()
	repo, err := NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{repo: repo, now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), allowed: map[string]bool{"one": true, "two": true}, title: models.Title{ID: "tmdb:movie:7", TMDBID: 7, Name: "Synthetic Movie", MediaType: "movie", OriginalName: "Original Synthetic", Language: "fra"}}
	f.s = New(repo, func(p string) bool { return f.allowed[p] })
	f.s.now = func() time.Time { return f.now }
	return f
}
func (f *fixture) create(t *testing.T, profile, client, kind string, ep *models.ReminderEpisode) models.Reminder {
	t.Helper()
	item, _, err := f.s.Create(t.Context(), profile, client, CreateRequest{ContentID: f.title.ID, ContentType: f.title.MediaType, EventKind: kind, Episode: ep}, f.title)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func (f *fixture) events(t *testing.T, profile string) []models.ReminderEvent {
	t.Helper()
	items, _, err := f.s.Events(t.Context(), profile, "", 100, false)
	if err != nil {
		t.Fatal(err)
	}
	return items
}
func (f *fixture) movie(t *testing.T, date string) {
	t.Helper()
	title := f.title
	title.Releases = []models.Release{{Type: "digital", Date: date}}
	if err := f.s.ObserveTitles(t.Context(), "one", "phone", []models.Title{title}); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) series(ep models.SeriesEpisode) *models.SeriesDetails {
	return &models.SeriesDetails{Title: f.title, ActiveOrdering: "official", Seasons: []models.SeriesSeason{{Number: ep.SeasonNumber, Episodes: []models.SeriesEpisode{ep}}}}
}
func canonicalTestEpisode(id string) models.SeriesEpisode {
	return models.SeriesEpisode{ID: id, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}, SeasonNumber: 2, EpisodeNumber: 3, AiredDate: "2026-11-16"}
}
func (f *fixture) source(t *testing.T, profile, client string, ep *models.SeriesEpisode) {
	t.Helper()
	checked, expires := f.now, f.now.Add(time.Hour)
	if err := f.s.ObserveSource(t.Context(), profile, client, f.title, ep, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires, Reason: "matching_candidates"}); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseUsesFreshDeliveredFutureBaselineAndDurableDateIndependentReceipt(t *testing.T) {
	f := setup(t)
	f.movie(t, "2026-11-16")
	observed := f.now
	f.now = f.now.Add(time.Minute)
	sub := f.create(t, "one", "phone", models.ReminderRelease, nil)
	state, _ := f.repo.Get(t.Context(), "one")
	if len(state.Checkpoints) != 1 || !state.Checkpoints[0].ObservedAt.Equal(observed) {
		t.Fatal("creation lost original future baseline")
	}
	f.now = f.now.Add(time.Minute)
	f.movie(t, "2026-11-20")
	if len(f.events(t, "one")) != 0 {
		t.Fatal("reschedule notified before release")
	}
	f.now = f.now.Add(24 * time.Hour)
	f.movie(t, "2026-11-16")
	events := f.events(t, "one")
	if len(events) != 1 || events[0].ReminderID != sub.ID || events[0].CandidateOnly || events[0].MilestoneKey != "digital" {
		t.Fatal("release milestone missing or mislabeled")
	}
	reopened, err := NewFileRepository(f.repo.directory)
	if err != nil {
		t.Fatal(err)
	}
	f.s = New(reopened, func(p string) bool { return true })
	f.s.now = func() time.Time { return f.now }
	f.movie(t, "2026-11-15")
	f.movie(t, "2026-11-20")
	f.now = f.now.Add(5 * 24 * time.Hour)
	f.movie(t, "2026-11-20")
	if len(f.events(t, "one")) != 1 {
		t.Fatal("restart or date change duplicated release")
	}
}
func TestReleasedUnknownStaleAndMissingMetadataNeverSeedNotifications(t *testing.T) {
	for _, mode := range []string{"released", "unknown", "stale", "none"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			switch mode {
			case "released":
				f.movie(t, "2026-11-14")
			case "unknown":
				f.movie(t, "")
			case "stale":
				f.movie(t, "2026-11-16")
				f.now = f.now.Add(time.Hour)
			}
			f.create(t, "one", "phone", models.ReminderRelease, nil)
			f.now = f.now.Add(time.Minute)
			f.movie(t, "2026-11-15")
			if len(f.events(t, "one")) != 0 {
				t.Fatal("first released snapshot flooded history")
			}
		})
	}
}
func TestCanonicalSeriesEpisodeBaselineRenamesAndNumberingAmbiguity(t *testing.T) {
	for _, id := range []string{"tmdb:series:123", "tvdb:series:74852"} {
		t.Run(id, func(t *testing.T) {
			f := setup(t)
			f.title = models.Title{ID: id, Name: "Synthetic Series", MediaType: "series"}
			ep := canonicalTestEpisode("A")
			f.s.ObserveSeries(t.Context(), "one", "phone", f.series(ep))
			f.now = f.now.Add(time.Minute)
			f.create(t, "one", "phone", models.ReminderNewEpisode, nil)
			f.now = f.now.Add(24 * time.Hour)
			ep.AiredDate = "2026-11-16"
			f.s.ObserveSeries(t.Context(), "one", "phone", f.series(ep))
			if len(f.events(t, "one")) != 1 {
				t.Fatal("canonical episode did not notify")
			}
			ep.ID = "B"
			f.s.ObserveSeries(t.Context(), "one", "phone", f.series(ep))
			if len(f.events(t, "one")) != 1 {
				t.Fatal("renamed episode became an extra milestone")
			}
		})
	}
	for _, mode := range []string{"rename-before-release", "owner-change", "duplicate-coordinate", "duplicate-id"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			f.title = models.Title{ID: "tmdb:series:123", Name: "Synthetic", MediaType: "series"}
			ep := canonicalTestEpisode("A")
			f.s.ObserveSeries(t.Context(), "one", "phone", f.series(ep))
			f.now = f.now.Add(time.Minute)
			f.create(t, "one", "phone", models.ReminderNewEpisode, nil)
			f.now = f.now.Add(time.Minute)
			details := f.series(ep)
			switch mode {
			case "rename-before-release":
				details.Seasons[0].Episodes[0].ID = "B"
			case "owner-change":
				details.Seasons[0].Episodes[0].Numbering = &models.EpisodeNumbering{SeriesID: "tmdb:series:123", Ordering: "official"}
			case "duplicate-coordinate":
				alias := ep
				alias.ID = "B"
				details.Seasons[0].Episodes = append(details.Seasons[0].Episodes, alias)
			case "duplicate-id":
				alias := ep
				alias.EpisodeNumber = 4
				details.Seasons[0].Episodes = append(details.Seasons[0].Episodes, alias)
			}
			f.s.ObserveSeries(t.Context(), "one", "phone", details)
			f.now = f.now.Add(24 * time.Hour)
			ep.AiredDate = "2026-11-16"
			f.s.ObserveSeries(t.Context(), "one", "phone", f.series(ep))
			if len(f.events(t, "one")) != 0 {
				t.Fatal("ambiguous identity notified")
			}
		})
	}
}
func TestMatchingSourceIsClientScopedOneShotAndRequiresPostCreationProof(t *testing.T) {
	f := setup(t)
	first := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	second := f.create(t, "one", "tv", models.ReminderMatchingSource, nil)
	if first.ID == second.ID {
		t.Fatal("different client intents silently converged")
	}
	same := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	if same.ID != first.ID {
		t.Fatal("same client interest duplicated")
	}
	f.source(t, "one", "phone", nil)
	if len(f.events(t, "one")) != 0 {
		t.Fatal("precreation/equal-time proof notified")
	}
	f.now = f.now.Add(time.Second)
	f.source(t, "one", "phone", nil)
	f.source(t, "one", "phone", nil)
	events := f.events(t, "one")
	if len(events) != 1 || !events[0].CandidateOnly || events[0].ReminderID != first.ID {
		t.Fatal("source proof not one candidate event")
	}
	f.source(t, "one", "tv", nil)
	if len(f.events(t, "one")) != 2 {
		t.Fatal("second creating client could not trigger its own interest")
	}
	items, err := f.s.List(t.Context(), "one", "phone")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.CreatedOnCurrentDevice != (item.ID == first.ID) {
			t.Fatal("safe device binding marker wrong")
		}
	}
}
func TestCurrentCandidateCreationDoesNotFloodAndUsesFullDeliveredSearchInputs(t *testing.T) {
	f := setup(t)
	f.movie(t, "2026-11-16")
	f.now = f.now.Add(time.Second)
	called := 0
	f.s.SetSourceCheck(func(_ context.Context, profile, client string, title models.Title, ep *models.SeriesEpisode) models.SourceAvailability {
		called++
		if title.OriginalName != f.title.OriginalName || title.Language != "fra" {
			t.Fatal("creation lost search projection")
		}
		checked, expires := f.now.Add(-time.Second), f.now.Add(time.Hour)
		return models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires}
	})
	_, current, err := f.s.Create(t.Context(), "one", "phone", CreateRequest{ContentID: f.title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, models.Title{ID: f.title.ID, TMDBID: 7, MediaType: "movie", Name: f.title.Name})
	if err != nil || current == nil || current.State != "found" || called != 1 || len(f.events(t, "one")) != 0 {
		t.Fatal("creation did not return existing candidate without an event")
	}
	f.s.List(t.Context(), "one", "phone")
	if len(f.events(t, "one")) != 0 {
		t.Fatal("reconciliation treated old checked time as new")
	}
}
func TestExactEpisodeCreationRejectsUnknownAlternateAndAmbiguousCoordinates(t *testing.T) {
	f := setup(t)
	f.title = models.Title{ID: "tvdb:series:74852", Name: "Synthetic", MediaType: "series"}
	ep := canonicalTestEpisode("A")
	details := f.series(ep)
	f.s.ObserveSeries(t.Context(), "one", "phone", details)
	f.now = f.now.Add(time.Second)
	sub := f.create(t, "one", "phone", models.ReminderMatchingSource, episodeRef(ep))
	if sub.Episode.EpisodeID != "A" {
		t.Fatal("wrong canonical episode")
	}
	for _, ref := range []*models.ReminderEpisode{nil, {EpisodeID: "A", Numbering: models.EpisodeNumbering{SeriesID: ep.Numbering.SeriesID, Ordering: "dvd"}, SeasonNumber: 2, EpisodeNumber: 3}, {EpisodeID: "B", Numbering: *ep.Numbering, SeasonNumber: 2, EpisodeNumber: 3}} {
		_, _, err := f.s.Create(t.Context(), "one", "phone", CreateRequest{ContentID: f.title.ID, ContentType: "series", EventKind: models.ReminderMatchingSource, Episode: ref}, f.title)
		if err == nil {
			t.Fatal("unverified episode accepted")
		}
	}
	alias := ep
	alias.ID = "B"
	details.Seasons[0].Episodes = append(details.Seasons[0].Episodes, alias)
	f.s.ObserveSeries(t.Context(), "one", "phone", details)
	_, _, err := f.s.Create(t.Context(), "one", "phone", CreateRequest{ContentID: f.title.ID, ContentType: "series", EventKind: models.ReminderMatchingSource, Episode: episodeRef(ep)}, f.title)
	if !errors.Is(err, ErrReload) {
		t.Fatal("ambiguous coordinate retained creation trust")
	}
}
func TestInboxReadDismissRevokeProfilesAndRestart(t *testing.T) {
	f := setup(t)
	sub := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	f.now = f.now.Add(time.Second)
	f.source(t, "one", "phone", nil)
	event := f.events(t, "one")[0]
	yes, no := true, false
	if _, err := f.s.Mark(t.Context(), "two", event.ID, &yes, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal("event mutation crossed profiles")
	}
	if _, err := f.s.Mark(t.Context(), "one", event.ID, &yes, nil); err != nil {
		t.Fatal(err)
	}
	unread, _, _ := f.s.Events(t.Context(), "one", "", 100, true)
	if len(unread) != 0 {
		t.Fatal("read flag ignored")
	}
	f.s.Mark(t.Context(), "one", event.ID, nil, &yes)
	if len(f.events(t, "one")) != 0 {
		t.Fatal("dismissed event remained in inbox")
	}
	f.s.Mark(t.Context(), "one", event.ID, &no, &no)
	if len(f.events(t, "one")) != 1 {
		t.Fatal("restoring inbox state lost receipt")
	}
	f.s.Revoke(t.Context(), "one", sub.ID)
	items, _ := f.s.List(t.Context(), "one", "phone")
	if len(items) != 0 {
		t.Fatal("revoked subscription listed")
	}
	f.now = f.now.Add(time.Hour)
	f.source(t, "one", "phone", nil)
	if len(f.events(t, "one")) != 1 {
		t.Fatal("revoke generated another event")
	}
	other, _, _ := f.s.Events(t.Context(), "two", "", 100, false)
	if len(other) != 0 {
		t.Fatal("private inbox leaked")
	}
	fresh, _ := NewFileRepository(f.repo.directory)
	state, err := fresh.Get(t.Context(), "one")
	if err != nil || len(state.Events) != 1 || state.Events[0].MilestoneKey == "" || state.Subscriptions[0].RevokedAt == nil {
		t.Fatal("restart lost receipts/revoke")
	}
}
func TestConcurrentIdempotencyAndRevocationSerialize(t *testing.T) {
	f := setup(t)
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			item, _, err := f.s.Create(context.Background(), "one", "phone", CreateRequest{ContentID: f.title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, f.title)
			ids <- item.ID
			failures <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(failures)
	id := ""
	for got := range ids {
		if id == "" {
			id = got
		}
		if id != got {
			t.Fatal("concurrent duplicate subscriptions")
		}
	}
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	f.now = f.now.Add(time.Second)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); f.source(t, "one", "phone", nil) }()
	}
	wg.Wait()
	if len(f.events(t, "one")) != 1 {
		t.Fatal("concurrent duplicate events")
	}
	f.s.Revoke(t.Context(), "one", id)
	f.source(t, "one", "phone", nil)
	if len(f.events(t, "one")) != 1 {
		t.Fatal("revoked receipt changed")
	}
}
func TestChildDisabledContextCancellationAndAtomicFileFailure(t *testing.T) {
	f := setup(t)
	f.allowed["one"] = false
	_, _, err := f.s.Create(t.Context(), "one", "phone", CreateRequest{ContentID: f.title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, f.title)
	if !errors.Is(err, ErrForbidden) {
		t.Fatal("restricted profile accepted")
	}
	if _, _, err = f.s.Events(t.Context(), "one", "", 10, false); !errors.Is(err, ErrForbidden) {
		t.Fatal("restricted profile inbox exposed")
	}
	f.allowed["one"] = true
	f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	before, _ := os.ReadFile(f.repo.path("one"))
	if err = f.repo.Update(t.Context(), "one", func(state *models.ReminderProfileState) error {
		state.Subscriptions = nil
		return errors.New("synthetic failure")
	}); err == nil {
		t.Fatal("failed mutation accepted")
	}
	after, _ := os.ReadFile(f.repo.path("one"))
	if string(before) != string(after) {
		t.Fatal("failed update changed durable data")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = f.repo.Update(ctx, "one", func(*models.ReminderProfileState) error { return nil }); err == nil {
		t.Fatal("cancelled write committed")
	}
	info, _ := os.Stat(f.repo.path("one"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("private file permissions wrong")
	}

	files, _ := os.ReadDir(f.repo.directory)
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".json" {
			t.Fatal("temporary file leaked")
		}
	}
}

func TestVerifiedTitleAliasConvergesButExactEpisodeAndDeviceIntentDoNot(t *testing.T) {
	f := setup(t)
	f.title.IMDBID = "tt0000007"
	first := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	alias := f.title
	alias.ID = "tt0000007"
	second, _, err := f.s.Create(t.Context(), "one", "phone", CreateRequest{ContentID: alias.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, alias)
	if err != nil || second.ID != first.ID || second.Title.ID != second.ContentID || second.ContentID != first.ContentID {
		t.Fatal("verified title alias did not preserve stable identity")
	}
	other, _, err := f.s.Create(t.Context(), "one", "tv", CreateRequest{ContentID: alias.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, alias)
	if err != nil || other.ID == first.ID {
		t.Fatal("title alias collapsed different device intent")
	}
}

func TestFileRestoreGatePreventsOldOrConcurrentSnapshotsOverwritingRestore(t *testing.T) {
	f := setup(t)
	f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.repo.Update(context.Background(), "one", func(state *models.ReminderProfileState) error {
			close(entered)
			<-release
			state.Subscriptions[0].ObserveAfter = time.Time{}
			return nil
		})
	}()
	<-entered
	restoring, finish := make(chan struct{}), make(chan struct{})
	restored := make(chan error, 1)
	boundary := f.now.Add(time.Hour)
	go func() {
		unlock := LockFileRestore()
		defer unlock()
		data, err := os.ReadFile(f.repo.path("one"))
		if err != nil {
			restored <- err
			return
		}
		var state models.ReminderProfileState
		if err = json.Unmarshal(data, &state); err != nil {
			restored <- err
			return
		}
		state.Subscriptions[0].ObserveAfter = boundary
		state.Subscriptions[0].RevokedAt = &boundary
		close(restoring)
		<-finish
		data, err = json.Marshal(state)
		if err == nil {
			err = os.WriteFile(f.repo.path("one"), data, 0600)
		}
		restored <- err
	}()
	select {
	case <-restoring:
		t.Fatal("restore bypassed an old writer")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-restoring
	reopened, err := NewFileRepository(f.repo.directory)
	if err != nil {
		t.Fatal(err)
	}
	lateRead := make(chan struct{})
	lateDone := make(chan error, 1)
	go func() {
		lateDone <- reopened.Update(context.Background(), "one", func(state *models.ReminderProfileState) error {
			close(lateRead)
			if !state.Subscriptions[0].ObserveAfter.Equal(boundary) || state.Subscriptions[0].RevokedAt == nil {
				return errors.New("writer read pre-restore state")
			}
			return nil
		})
	}()
	select {
	case <-lateRead:
		t.Fatal("writer read while restore held its gate")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	if err := <-restored; err != nil {
		t.Fatal(err)
	}
	if err := <-lateDone; err != nil {
		t.Fatal(err)
	}
}

func TestInboxCursorSurvivesReadAndDismissBetweenPages(t *testing.T) {
	for _, mode := range []string{"read", "dismiss"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
			f.create(t, "one", "tv", models.ReminderMatchingSource, nil)
			f.now = f.now.Add(time.Second)
			f.source(t, "one", "phone", nil)
			f.now = f.now.Add(time.Second)
			f.source(t, "one", "tv", nil)
			page, cursor, err := f.s.Events(t.Context(), "one", "", 1, mode == "read")
			if err != nil || len(page) != 1 || cursor == "" {
				t.Fatal("first page missing")
			}
			yes := true
			if mode == "read" {
				f.s.Mark(t.Context(), "one", page[0].ID, &yes, nil)
			} else {
				f.s.Mark(t.Context(), "one", page[0].ID, nil, &yes)
			}
			second, next, err := f.s.Events(t.Context(), "one", cursor, 1, mode == "read")
			if err != nil || len(second) != 1 || second[0].ID == page[0].ID || next != "" {
				t.Fatal("cursor depended on mutable visibility")
			}
		})
	}
}

func TestQuotaPrunesOnlyRevokedInvisibleGenerationsAndCannotReplayOldSource(t *testing.T) {
	f := setup(t)
	active := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	f.now = f.now.Add(time.Second)
	f.source(t, "one", "phone", nil)
	visible := f.events(t, "one")[0]
	f.s.Revoke(t.Context(), "one", active.ID)
	if err := f.repo.Update(t.Context(), "one", func(state *models.ReminderProfileState) error {
		for i := 1; i < MaxStored; i++ {
			item := models.StoredReminder{Reminder: models.Reminder{ID: fmt.Sprintf("revoked-%d", i)}, RevokedAt: &f.now}
			state.Subscriptions = append(state.Subscriptions, item)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Second)
	fresh := f.create(t, "one", "tv", models.ReminderMatchingSource, nil)
	state, _ := f.repo.Get(t.Context(), "one")
	if len(state.Subscriptions) != 2 || len(state.Events) != 1 {
		t.Fatal("cleanup removed visible history or kept empty revoked generations")
	}
	oldChecked, expires := f.now.Add(-time.Second), f.now.Add(time.Hour)
	f.s.ObserveSource(t.Context(), "one", "tv", f.title, nil, models.SourceAvailability{State: "found", CheckedAt: &oldChecked, ExpiresAt: &expires})
	if len(f.events(t, "one")) != 1 {
		t.Fatal("old proof resurrected into new generation")
	}
	yes := true
	f.s.Mark(t.Context(), "one", visible.ID, nil, &yes)
	f.s.Revoke(t.Context(), "one", fresh.ID)
	if err := f.repo.Update(t.Context(), "one", func(state *models.ReminderProfileState) error {
		for len(state.Subscriptions) < MaxStored {
			state.Subscriptions = append(state.Subscriptions, models.StoredReminder{Reminder: models.Reminder{ID: fmt.Sprintf("empty-%d", len(state.Subscriptions))}, RevokedAt: &f.now})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	repo, _ := NewFileRepository(f.repo.directory)
	f.s = New(repo, func(string) bool { return true })
	f.s.now = func() time.Time { return f.now }
	next := f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	state, _ = repo.Get(t.Context(), "one")
	if next.ID == active.ID || len(state.Subscriptions) != 1 || len(state.Events) != 0 || len(state.Checkpoints) != 0 {
		t.Fatal("dismissed revoked generation could not be cleared across restart")
	}
}

func TestSourceReconciliationAndCreationUseSharedBoundedValidationBudget(t *testing.T) {
	f := setup(t)
	calls := 0
	f.s.SetSourceCheck(func(ctx context.Context, _ string, _ string, _ models.Title, _ *models.SeriesEpisode) models.SourceAvailability {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 510*time.Millisecond {
			t.Fatal("validation budget missing")
		}
		return models.SourceAvailability{State: "unknown"}
	})
	f.create(t, "one", "phone", models.ReminderMatchingSource, nil)
	f.create(t, "one", "tv", models.ReminderMatchingSource, nil)
	calls = 0
	var first time.Time
	f.s.SetSourceCheck(func(ctx context.Context, _ string, _ string, _ models.Title, _ *models.SeriesEpisode) models.SourceAvailability {
		deadline, _ := ctx.Deadline()
		if first.IsZero() {
			first = deadline
		} else if !deadline.Equal(first) {
			t.Fatal("each saved interest received a fresh budget")
		}
		calls++
		return models.SourceAvailability{State: "unknown"}
	})
	f.s.List(t.Context(), "one", "phone")
	if calls != 2 {
		t.Fatal("reconciliation did not inspect the two waiting interests")
	}
}

type stepCancelledContext struct {
	context.Context
	calls, after int
	done         chan struct{}
}

func (c *stepCancelledContext) Err() error {
	c.calls++
	if c.calls >= c.after {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
		return context.Canceled
	}
	return nil
}
func (c *stepCancelledContext) Done() <-chan struct{} { return c.done }
func TestMetadataDeadlineStopsReductionAndRollsBackPartialCheckpointChanges(t *testing.T) {
	f := setup(t)
	f.create(t, "one", "phone", models.ReminderRelease, nil)
	before, _ := os.ReadFile(f.repo.path("one"))
	ctx := &stepCancelledContext{Context: context.Background(), after: 3, done: make(chan struct{})}
	observations := []observation{{title: f.title, kind: models.ReminderRelease, releaseType: "digital", milestone: "digital", state: "upcoming", observed: f.now.Add(time.Second)}, {title: f.title, kind: models.ReminderRelease, releaseType: "digital", milestone: "digital", state: "released", observed: f.now.Add(2 * time.Second)}}
	if err := f.s.applyBatch(ctx, "one", observations); !errors.Is(err, context.Canceled) {
		t.Fatal("metadata reduction ignored cancellation")
	}
	after, _ := os.ReadFile(f.repo.path("one"))
	if string(before) != string(after) {
		t.Fatal("cancelled reduction committed partial state")
	}
}

func TestRestoredMetadataRelationsRequirePostRestoreObservationBoundary(t *testing.T) {
	f := setup(t)
	sub := f.create(t, "one", "phone", models.ReminderRelease, nil)
	boundary := f.now.Add(time.Hour)
	if err := f.repo.Update(t.Context(), "one", func(state *models.ReminderProfileState) error {
		stored := &state.Subscriptions[0]
		stored.Aliases = []string{"movie:id:" + f.title.ID}
		stored.SearchTitle.IMDBID = ""
		stored.ObserveAfter = boundary
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	trusted := f.title
	trusted.IMDBID = "tt0000007"
	late := observation{title: trusted, kind: models.ReminderRelease, releaseType: "digital", milestone: "digital", state: "upcoming", observed: f.now.Add(time.Second)}
	if err := f.s.applyBatch(t.Context(), "one", []observation{late}); err != nil {
		t.Fatal(err)
	}
	state, _ := f.repo.Get(t.Context(), "one")
	if len(state.Subscriptions[0].Aliases) != 1 || state.Subscriptions[0].SearchTitle.IMDBID != "" || state.Subscriptions[0].Reminder.ID != sub.ID {
		t.Fatal("late pre-restore metadata reintroduced alias authority")
	}
	late.observed = boundary.Add(time.Second)
	if err := f.s.applyBatch(t.Context(), "one", []observation{late}); err != nil {
		t.Fatal(err)
	}
	state, _ = f.repo.Get(t.Context(), "one")
	if state.Subscriptions[0].SearchTitle.IMDBID != trusted.IMDBID || len(state.Subscriptions[0].Aliases) < 2 {
		t.Fatal("fresh post-restore exact-anchor metadata could not rehydrate")
	}
}
