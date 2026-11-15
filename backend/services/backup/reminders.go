package backup

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"novastream/services/reminders"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"novastream/models"
	"novastream/services/titleratings"
)

func isReminderBackupFile(name string) bool {
	if !strings.HasPrefix(name, "reminders/") || !strings.HasSuffix(name, ".json") {
		return false
	}
	hash := strings.TrimSuffix(strings.TrimPrefix(name, "reminders/"), ".json")
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil && strings.ToLower(hash) == hash
}
func (s *Service) reminderBackupFiles() ([]string, error) {
	directory := filepath.Join(s.cacheDir, "reminders")
	info, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid reminder backup directory")
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, file := range files {
		name := "reminders/" + file.Name()
		if file.Type().IsRegular() && isReminderBackupFile(name) {
			out = append(out, name)
		}
	}
	return out, nil
}

// Imported controls are personal data, not provider/metadata proof. Keep fired
// receipts for durable dedup, but require a new normal observation after restore.
func sanitizeReminderState(state *models.ReminderProfileState, now time.Time) error {
	if len(state.Subscriptions) > 2000 || len(state.Checkpoints) > 10000 || len(state.Events) > 10000 {
		return errors.New("reminder restore exceeds limits")
	}
	subscriptions := map[string]models.Reminder{}
	for i := range state.Subscriptions {
		sub := &state.Subscriptions[i]
		if err := reminders.RebuildRestoredIntent(sub); err != nil {
			return errors.New("invalid restored reminder intent")
		}
		sub.ObserveAfter = now
		sub.Reminder.LastObservedAt = nil
		title, err := anchoredReminderTitle(sub.Reminder.Title, sub.Reminder.ContentID, sub.Reminder.ContentType)
		if err != nil {
			return err
		}
		sub.Reminder.Title = title
		search, err := anchoredReminderTitle(sub.SearchTitle, sub.Reminder.ContentID, sub.Reminder.ContentType)
		if err != nil {
			return err
		}
		t := sub.SearchTitle
		sub.SearchTitle = models.Title{ID: search.ID, Name: t.Name, OriginalName: t.OriginalName, MediaType: search.MediaType, Year: t.Year, Language: t.Language, TMDBID: search.TMDBID, TVDBID: search.TVDBID, IMDBID: search.IMDBID, IsDaily: t.IsDaily}
		sub.Aliases = []string{sub.Reminder.ContentType + ":id:" + sub.Reminder.ContentID}
		subscriptions[sub.Reminder.ID] = sub.Reminder
	}
	for i := range state.Checkpoints {
		state.Checkpoints[i].State = "unknown"
		state.Checkpoints[i].ObservedAt = time.Time{}
	}
	for i := range state.Events {
		event := &state.Events[i]
		sub, ok := subscriptions[event.ReminderID]
		if !ok || event.ContentID != sub.ContentID || event.ContentType != sub.ContentType || event.EventKind != sub.EventKind {
			return errors.New("restored reminder history anchor mismatch")
		}
		title, err := anchoredReminderTitle(event.Title, event.ContentID, event.ContentType)
		if err != nil {
			return err
		}
		event.Title = title
		event.MilestoneKey = state.EventKeys[event.ID]
		if event.MilestoneKey == "" {
			return errors.New("reminder receipt identity missing")
		}
	}
	return nil
}
func sanitizeRestoredReminders(export *databaseExport) error {
	now := time.Now().UTC()
	for profile, state := range export.Reminders {
		if err := sanitizeReminderState(&state, now); err != nil {
			return err
		}
		export.Reminders[profile] = state
	}
	return nil
}
func sanitizeRestoredReminderFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) > 32<<20 {
		return errors.New("reminder restore exceeds limits")
	}
	var state models.ReminderProfileState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	if err = sanitizeReminderState(&state, time.Now().UTC()); err != nil {
		return err
	}
	data, err = json.Marshal(state)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

// An imported title may label its exact saved anchor, but its external-ID
// relationships are not catalogue evidence. Rebuild only what that anchor says.
func anchoredReminderTitle(title models.Title, id, kind string) (models.Title, error) {
	if title.MediaType == "tv" {
		title.MediaType = "series"
	}
	if title.ID != id || title.MediaType != kind {
		return models.Title{}, errors.New("restored reminder title anchor mismatch")
	}
	t := titleratings.BareTitle(title)
	t.TMDBID = 0
	t.TVDBID = 0
	t.IMDBID = ""
	for _, prefix := range []string{"tmdb:movie:", "tmdb:series:", "tmdb:tv:"} {
		if strings.HasPrefix(id, prefix) {
			if number, err := strconv.ParseInt(strings.TrimPrefix(id, prefix), 10, 64); err == nil && number > 0 && ((kind == "movie" && prefix == "tmdb:movie:") || (kind == "series" && prefix != "tmdb:movie:")) {
				t.TMDBID = number
			}
		}
	}
	if strings.HasPrefix(id, "tvdb:") {
		raw := strings.TrimPrefix(id, "tvdb:")
		compatible := true
		if strings.HasPrefix(raw, "series:") {
			compatible = kind == "series"
			raw = strings.TrimPrefix(raw, "series:")
		} else if strings.HasPrefix(raw, "movie:") {
			compatible = kind == "movie"
			raw = strings.TrimPrefix(raw, "movie:")
		}
		if number, err := strconv.ParseInt(raw, 10, 64); err == nil && number > 0 && compatible {
			t.TVDBID = number
		}
	}
	raw := strings.TrimPrefix(id, "imdb:")
	if strings.HasPrefix(raw, "tt") {
		if _, err := strconv.ParseUint(strings.TrimPrefix(raw, "tt"), 10, 64); err == nil {
			t.IMDBID = raw
		}
	}
	return t, nil
}
