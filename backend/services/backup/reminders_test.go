package backup

import (
	"archive/zip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/reminders"
)

func TestReminderFallbackArchiveRestoreRetainsReceiptsButInvalidatesProof(t *testing.T) {
	backup, cache := setupTestService(t)
	repo, err := reminders.NewFileRepository(filepath.Join(cache, "reminders"))
	if err != nil {
		t.Fatal(err)
	}
	service := reminders.New(repo, func(p string) bool { return p == "profile" })
	title := models.Title{ID: "tmdb:movie:7", Name: "Synthetic", MediaType: "movie"}
	for _, client := range []string{"phone", "tv"} {
		if _, _, err := service.Create(t.Context(), "profile", client, reminders.CreateRequest{ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, title); err != nil {
			t.Fatal(err)
		}
	}
	oldChecked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	value := models.SourceAvailability{State: "found", CheckedAt: &oldChecked, ExpiresAt: &expires}
	if err := service.ObserveSource(t.Context(), "profile", "phone", title, nil, value); err != nil {
		t.Fatal(err)
	}
	info, err := backup.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(cache, "backups", info.Filename)
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range reader.File {
		if isReminderBackupFile(file.Name) {
			found = true
		}
	}
	reader.Close()
	if !found {
		t.Fatal("fallback reminders omitted from archive")
	}
	items, err := service.List(t.Context(), "profile", "phone")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		service.Revoke(t.Context(), "profile", item.ID)
	}
	beforeRestore := time.Now().UTC()
	if err := backup.restoreBackupFile(archivePath, "synthetic-reminders.zip"); err != nil {
		t.Fatal(err)
	}
	state, err := repo.Get(t.Context(), "profile")
	if err != nil || len(state.Subscriptions) != 2 || len(state.Events) != 1 || state.Events[0].MilestoneKey == "" {
		t.Fatal("fallback restore lost controls or receipt identity")
	}
	for _, sub := range state.Subscriptions {
		if sub.RevokedAt != nil || sub.ObserveAfter.Before(beforeRestore) || sub.Reminder.LastObservedAt != nil {
			t.Fatal("restored controls acquired stale proof")
		}
	}
	for _, checkpoint := range state.Checkpoints {
		if checkpoint.State != "unknown" || !checkpoint.ObservedAt.IsZero() {
			t.Fatal("restored checkpoint asserted live proof")
		}
	}
	service.SetSourceCheck(func(context.Context, string, string, models.Title, *models.SeriesEpisode) models.SourceAvailability {
		return value
	})
	if _, err := service.List(t.Context(), "profile", "tv"); err != nil {
		t.Fatal(err)
	}
	events, _, _ := service.Events(t.Context(), "profile", "", 100, false)
	if len(events) != 1 {
		t.Fatal("old cached proof became fresh after restore")
	}
	checked := time.Now().UTC()
	value.CheckedAt = &checked
	if err := service.ObserveSource(t.Context(), "profile", "tv", title, nil, value); err != nil {
		t.Fatal(err)
	}
	events, _, _ = service.Events(t.Context(), "profile", "", 100, false)
	if len(events) != 2 {
		t.Fatal("fresh normal observation could not complete restored interest")
	}
	files, _ := os.ReadDir(filepath.Join(cache, "reminders"))
	for _, file := range files {
		stat, _ := file.Info()
		if stat.Mode().Perm() != 0600 {
			t.Fatal("restored reminder file is not private")
		}
	}
}
func TestReminderRestoreAllowlistAndSanitizerNeverRestoreObservationAuthority(t *testing.T) {
	for _, name := range []string{"reminders/../users.json", "reminders/.json", "reminders/" + strings.Repeat("G", 64) + ".json", "../reminders/" + strings.Repeat("a", 64) + ".json", "reminders/" + strings.Repeat("a", 64) + ".json/extra"} {
		if isRestorableBackupFile(name) {
			t.Fatal("unsafe fallback member accepted")
		}
	}
	now := time.Now().UTC()
	title := models.Title{ID: "tmdb:movie:7", Name: "Synthetic", MediaType: "movie", Poster: &models.Image{URL: "https://synthetic.invalid/private-artwork"}, Overview: "private overview"}
	state := models.ReminderProfileState{Subscriptions: []models.StoredReminder{{Reminder: models.Reminder{ID: "interest", ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderRelease, ReleaseType: "digital", State: "triggered", Title: title, LastObservedAt: &now}, SearchTitle: title}}, Checkpoints: []models.ReminderCheckpoint{{ReminderID: "interest", State: "upcoming", ObservedAt: now, Fired: true}}, Events: []models.ReminderEvent{{ID: "event", ReminderID: "interest", ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderRelease, ReleaseType: "digital", Title: title}}, EventKeys: map[string]string{"event": "digital"}}
	if err := sanitizeReminderState(&state, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !state.Checkpoints[0].Fired || state.Checkpoints[0].State != "unknown" || state.Events[0].MilestoneKey != "digital" || !state.Subscriptions[0].ObserveAfter.Equal(now.Add(time.Hour)) {
		t.Fatal("restore erased receipt or retained proof")
	}
	data, _ := json.Marshal(state)
	if strings.Contains(string(data), "private-artwork") || strings.Contains(string(data), "private overview") {
		t.Fatal("untrusted backup metadata retained unrelated data")
	}
	state.EventKeys = nil
	if sanitizeReminderState(&state, now) == nil {
		t.Fatal("receipt identity loss was silently accepted")
	}
}

func TestRestoredAliasesProviderRelationsAndDerivedIntentCannotRedirectVerifiedTitles(t *testing.T) {
	repo, err := reminders.NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := reminders.New(repo, func(p string) bool { return p == "profile" })
	a := models.Title{ID: "tmdb:movie:111", TMDBID: 111, Name: "Synthetic A", MediaType: "movie"}
	b := models.Title{ID: "tmdb:movie:222", TMDBID: 222, Name: "Synthetic B", MediaType: "movie"}
	create := func(title models.Title, kind string) models.Reminder {
		t.Helper()
		item, _, err := service.Create(t.Context(), "profile", "phone", reminders.CreateRequest{ContentID: title.ID, ContentType: title.MediaType, EventKind: kind}, title)
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	savedB := create(b, models.ReminderRelease)
	shadow := create(b, models.ReminderMatchingSource)
	state, err := repo.Get(t.Context(), "profile")
	if err != nil {
		t.Fatal(err)
	}
	var forgedKey string
	for _, sub := range state.Subscriptions {
		if sub.Reminder.ID == shadow.ID {
			forgedKey = sub.TargetKey
		}
	}
	state.Subscriptions = state.Subscriptions[:1]
	stored := &state.Subscriptions[0]
	stored.Aliases = []string{"movie:id:" + a.ID, "movie:tmdb:111"}
	stored.Reminder.Title.TMDBID = 111
	stored.Reminder.Title.IMDBID = "tt0000111"
	stored.SearchTitle.TMDBID = 111
	stored.SearchTitle.IMDBID = "tt0000111"
	stored.TargetKey = forgedKey
	if err := sanitizeReminderState(&state, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	stored = &state.Subscriptions[0]
	if len(stored.Aliases) != 1 || stored.Aliases[0] != "movie:id:"+b.ID || stored.Reminder.Title.TMDBID != 222 || stored.SearchTitle.TMDBID != 222 || stored.Reminder.Title.IMDBID != "" || stored.SearchTitle.IMDBID != "" || stored.TargetKey == forgedKey {
		t.Fatal("imported relations/intent retained authority")
	}
	if err := repo.Update(t.Context(), "profile", func(target *models.ReminderProfileState) error { *target = state; return nil }); err != nil {
		t.Fatal(err)
	}
	createdA := create(a, models.ReminderRelease)
	if createdA.ID == savedB.ID || createdA.ContentID != a.ID {
		t.Fatal("verified A creation returned imported B")
	}
	matchingB := create(b, models.ReminderMatchingSource)
	if matchingB.ID == savedB.ID || matchingB.EventKind != models.ReminderMatchingSource {
		t.Fatal("imported target hash changed request intent")
	}
	a.Releases = []models.Release{{Type: "digital", Date: "2099-01-01"}}
	service.ObserveTitles(t.Context(), "profile", "phone", []models.Title{a})
	a.Releases[0].Date = "2000-01-01"
	service.ObserveTitles(t.Context(), "profile", "phone", []models.Title{a})
	events, _, err := service.Events(t.Context(), "profile", "", 100, false)
	if err != nil || len(events) != 1 || events[0].ReminderID != createdA.ID || events[0].Title.ID != a.ID {
		t.Fatal("trusted A observation was mislabeled as B")
	}
	b.IMDBID = "tt0000222"
	b.Releases = []models.Release{{Type: "digital", Date: "2099-01-01"}}
	if err := service.ObserveTitles(t.Context(), "profile", "phone", []models.Title{b}); err != nil {
		t.Fatal(err)
	}
	aliasB := b
	aliasB.ID = "tt0000222"
	sameB := create(aliasB, models.ReminderRelease)
	if sameB.ID != savedB.ID || sameB.Title.ID != b.ID || !sameB.CreatedOnCurrentDevice {
		t.Fatal("fresh exact-anchor metadata could not restore genuine aliases")
	}
}
func TestReminderRestoreRejectsMismatchedAnchorsButKeepsLegacyTVSearchProjection(t *testing.T) {
	title := models.Title{ID: "tvdb:series:74852", Name: "Synthetic Series", MediaType: "series"}
	makeState := func() models.ReminderProfileState {
		return models.ReminderProfileState{Subscriptions: []models.StoredReminder{{Reminder: models.Reminder{ID: "interest", ContentID: title.ID, ContentType: "series", EventKind: models.ReminderNewEpisode, State: "waiting", Title: title}, SearchTitle: title}}}
	}
	for _, field := range []string{"public", "search"} {
		state := makeState()
		if field == "public" {
			state.Subscriptions[0].Reminder.Title.ID = "tmdb:series:123"
		} else {
			state.Subscriptions[0].SearchTitle.ID = "tmdb:series:123"
		}
		if sanitizeReminderState(&state, time.Now()) == nil {
			t.Fatal("mismatched restored title anchor accepted")
		}
	}
	state := makeState()
	state.Subscriptions[0].SearchTitle.MediaType = "tv"
	if err := sanitizeReminderState(&state, time.Now()); err != nil || state.Subscriptions[0].SearchTitle.MediaType != "series" {
		t.Fatal("legitimate legacy tv search projection rejected")
	}
}

func TestReminderRestoreRealTVDBMovieAndOpaqueAnchorsStayExact(t *testing.T) {
	for _, tc := range []struct {
		id, kind string
		tvdb     int64
	}{{"tvdb:movie:123", "movie", 123}, {"tvdb:series:74852", "series", 74852}, {"tvdb:movie:synthetic-film", "movie", 0}, {"tvdb:series:synthetic-show", "series", 0}, {"catalogue:series:opaque", "series", 0}, {"unusual-provider:observed-id", "movie", 0}} {
		t.Run(tc.id, func(t *testing.T) {
			title := models.Title{ID: tc.id, Name: "Synthetic", MediaType: tc.kind, TMDBID: 111, TVDBID: 111, IMDBID: "tt0000111"}
			kind := models.ReminderRelease
			release := "digital"
			if tc.kind == "series" {
				kind = models.ReminderNewEpisode
				release = ""
			}
			state := models.ReminderProfileState{Subscriptions: []models.StoredReminder{{Reminder: models.Reminder{ID: "interest", ContentID: tc.id, ContentType: tc.kind, EventKind: kind, ReleaseType: release, State: "waiting", Title: title}, SearchTitle: title}}}
			if err := sanitizeReminderState(&state, time.Now()); err != nil {
				t.Fatal(err)
			}
			sub := state.Subscriptions[0]
			for _, value := range []models.Title{sub.Reminder.Title, sub.SearchTitle} {
				if value.ID != tc.id || value.MediaType != tc.kind || value.TVDBID != tc.tvdb || value.TMDBID != 0 || value.IMDBID != "" {
					t.Fatal("anchor was rejected or retained imported provider relations")
				}
			}
			if len(sub.Aliases) != 1 || sub.Aliases[0] != tc.kind+":id:"+tc.id {
				t.Fatal("opaque anchor gained unverified aliases")
			}
		})
	}
}
