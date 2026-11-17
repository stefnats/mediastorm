package user_settings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
)

type mutatorDenyHTTP struct{ calls atomic.Int32 }

func (d *mutatorDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("network unavailable in settings persistence fixture")
}

func mutatorsNoHTTP(t *testing.T) {
	t.Helper()
	previous := http.DefaultTransport
	denied := &mutatorDenyHTTP{}
	http.DefaultTransport = denied
	t.Cleanup(func() {
		http.DefaultTransport = previous
		if denied.calls.Load() != 0 {
			t.Error("persistence mutator attempted HTTP")
		}
	})
}

func mutatorsSeed(t *testing.T, f *settingsPGFixture, id, raw string) {
	t.Helper()
	f.exec(t, `INSERT INTO user_settings(user_id,settings,updated_at) VALUES($1,$2,clock_timestamp())
 ON CONFLICT(user_id) DO UPDATE SET settings=EXCLUDED.settings,updated_at=EXCLUDED.updated_at`, id, []byte(raw))
}

func mutatorsRaw(t *testing.T, ds *datastore.DataStore, id string) ([]byte, string) {
	t.Helper()
	var raw []byte
	var version string
	err := ds.Pool().QueryRow(t.Context(), `SELECT settings,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM user_settings WHERE user_id=$1`, id).Scan(&raw, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ""
	}
	if err != nil {
		t.Fatal("read own settings observation")
	}
	return raw, version
}

func mutatorsObject(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		t.Fatal("settings observation is not an object")
	}
	return object
}

func mutatorsUnknownPreserved(t *testing.T, before, after []byte) {
	t.Helper()
	a, b := mutatorsObject(t, before), mutatorsObject(t, after)
	if !bytes.Equal(a["futureRoot"], b["futureRoot"]) {
		t.Fatal("unknown object, array or large number changed")
	}
	displayA, displayB := mutatorsObject(t, a["display"]), mutatorsObject(t, b["display"])
	if !bytes.Equal(displayA["futureNumber"], displayB["futureNumber"]) {
		t.Fatal("nested unknown number changed")
	}
	if string(displayB["simpleMode"]) != "false" {
		t.Fatal("explicit SimpleMode=false was dropped")
	}
}

const mutatorsDocument = `{"futureRoot":{"integer":9007199254740993,"array":[{"integer":18446744073709551615},null,true]},"metadata":{"primaryLanguage":"fra"},"playback":{"preferredAudioLanguage":"eng","nextEpisodeAutoPlay":false},"display":{"simpleMode":false,"futureNumber":9007199254740995,"appearance":{"accentColor":"#123456"}},"liveTV":{"favoriteChannels":["legacy:source"]}}`
const mutatorsOther = `{"futureRoot":{"integer":9007199254740997},"metadata":{"primaryLanguage":"ita"},"display":{"simpleMode":false,"futureNumber":9007199254740999}}`

func TestUserSettingsPersistenceActualMutatorsPreserveUnknownAndOtherRows(t *testing.T) {
	for _, kind := range []string{"skipping", "sports", "favorite", "update", "strip", "clear", "delete"} {
		t.Run(kind, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "target")
			f.user(t, "other")
			mutatorsSeed(t, f, "target", mutatorsDocument)
			mutatorsSeed(t, f, "other", mutatorsOther)
			s := f.service(t, f.ds)
			before, _ := mutatorsRaw(t, f.ds, "target")
			other, otherVersion := mutatorsRaw(t, f.ds, "other")
			var err error
			switch kind {
			case "skipping":
				err = s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)})
			case "sports":
				_, err = s.UpdateSportsPreferences("target", 0, models.EmptySportsPreferences())
			case "favorite":
				_, err = s.SetFavoriteStation("target", stationFixture(71), true)
			case "update":
				var value *models.UserSettings
				value, err = s.Get("target")
				if err == nil && value != nil {
					value.Metadata.PrimaryLanguage = "deu"
					err = s.Update("target", *value)
				} else {
					t.Fatal("missing acquired update target")
				}
			case "strip":
				global := config.DefaultSettings()
				global.Metadata.PrimaryLanguage = "fra"
				global.Metadata.Language = []string{"fra"}
				global.Display.SimpleMode = true
				s.StripRedundantOverrides(global, nil, nil)
			case "clear":
				var changed int
				changed, err = s.ClearAppearanceOverrides()
				if err == nil && changed != 1 {
					t.Fatal("appearance clear did not select exactly its actual target")
				}
			case "delete":
				err = s.Delete("target")
			}
			if err != nil {
				t.Fatalf("actual %s mutator failed: %v", kind, err)
			}
			after, _ := mutatorsRaw(t, f.ds, "target")
			otherAfter, otherVersionAfter := mutatorsRaw(t, f.ds, "other")
			if !bytes.Equal(other, otherAfter) || otherVersion != otherVersionAfter {
				t.Fatal("unrelated settings row was written")
			}
			if kind == "delete" {
				if after != nil {
					t.Fatal("explicit Delete did not reset the whole target")
				}
				return
			}
			mutatorsUnknownPreserved(t, before, after)
			stored, err := f.ds.UserSettings().Get(t.Context(), "target")
			if err != nil || stored == nil {
				t.Fatal("read actual persisted target")
			}
			switch kind {
			case "skipping":
				if stored.Playback.IntroAutoSkip == nil || !*stored.Playback.IntroAutoSkip {
					t.Fatal("skip write absent")
				}
			case "sports":
				snapshot, parseErr := sportsSnapshot(stored.SportsPreferences)
				if parseErr != nil || snapshot.Revision != 1 {
					t.Fatal("sports write absent")
				}
			case "favorite":
				if !reflect.DeepEqual(stored.LiveTV.FavoriteStationIDs, []string{stationFixture(71)}) {
					t.Fatal("favorite write absent")
				}
			case "update":
				if stored.Metadata.PrimaryLanguage != "deu" {
					t.Fatal("general update absent")
				}
			case "strip":
				if stored.Metadata.PrimaryLanguage != "" {
					t.Fatal("actual redundant metadata override not stripped")
				}
			case "clear":
				if appearanceSettingsSet(stored.Display.Appearance) {
					t.Fatal("actual appearance override not cleared")
				}
			}
		})
	}
}

func TestUserSettingsPersistenceImplicitEmptyMutatorsRetainFutureRawValues(t *testing.T) {
	for _, kind := range []string{"strip", "clear", "remove-last-favorite"} {
		t.Run(kind, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "target")
			known := `"metadata":{"primaryLanguage":"fra"}`
			if kind == "clear" {
				known = `"display":{"appearance":{"accentColor":"#123456"}}`
			} else if kind == "remove-last-favorite" {
				known = fmt.Sprintf(`"liveTV":{"favoriteStationIds":[%q]}`, stationFixture(72))
			}
			mutatorsSeed(t, f, "target", `{"futureRoot":{"integer":9007199254740993},`+known+`}`)
			s := f.service(t, f.ds)
			before, _ := mutatorsRaw(t, f.ds, "target")
			switch kind {
			case "strip":
				global := config.DefaultSettings()
				global.Metadata.PrimaryLanguage = "fra"
				global.Metadata.Language = []string{"fra"}
				s.StripRedundantOverrides(global, nil, nil)
			case "clear":
				if count, err := s.ClearAppearanceOverrides(); err != nil || count != 1 {
					t.Fatal("clear actual final known override")
				}
			case "remove-last-favorite":
				if _, err := s.SetFavoriteStation("target", stationFixture(72), false); err != nil {
					t.Fatal("remove actual final favorite")
				}
			}
			after, _ := mutatorsRaw(t, f.ds, "target")
			if after == nil || !bytes.Equal(mutatorsObject(t, before)["futureRoot"], mutatorsObject(t, after)["futureRoot"]) {
				t.Fatal("implicit empty typed projection deleted unknown stored values")
			}
			stored, err := f.ds.UserSettings().Get(t.Context(), "target")
			if err != nil || stored == nil || stored.Metadata.PrimaryLanguage != "" || appearanceSettingsSet(stored.Display.Appearance) || len(stored.LiveTV.FavoriteStationIDs) != 0 {
				t.Fatal("future values kept but requested known override remained")
			}
			if err := s.Delete("target"); err != nil {
				t.Fatal("explicit reset of future-only row")
			}
			if raw, _ := mutatorsRaw(t, f.ds, "target"); raw != nil {
				t.Fatal("explicit Delete did not remove future-only row")
			}
		})
	}
}

func TestUserSettingsPersistenceFavoriteNoopCannotConfirmStaleFalse(t *testing.T) {
	mutatorsNoHTTP(t)
	f := newSettingsPG(t)
	f.user(t, "target")
	mutatorsSeed(t, f, "target", `{"futureRoot":{"integer":9007199254740993},"display":{"simpleMode":false,"futureNumber":9007199254740995}}`)
	a := f.service(t, f.ds)
	dsB := f.open(t)
	defer dsB.Close()
	b := f.service(t, dsB)
	if _, err := b.SetFavoriteStation("target", stationFixture(73), true); err != nil {
		t.Fatal("second service favorite write")
	}
	confirmed, version := mutatorsRaw(t, f.ds, "target")
	if _, err := a.SetFavoriteStation("target", stationFixture(73), false); !errors.Is(err, datastore.ErrUserSettingsConflict) {
		t.Fatal("stale false was reported as successful favorite no-op")
	}
	if current, currentVersion := mutatorsRaw(t, f.ds, "target"); !bytes.Equal(current, confirmed) || currentVersion != version {
		t.Fatal("stale no-op wrote the target")
	}
	local, _ := a.Get("target")
	if local == nil || len(local.LiveTV.FavoriteStationIDs) != 0 {
		t.Fatal("conflict silently adopted newer favorite")
	}
	if err := a.ResyncUserSettings(t.Context()); err != nil {
		t.Fatal("explicit settings resync")
	}
	if _, err := a.SetFavoriteStation("target", stationFixture(73), false); err != nil {
		t.Fatal("confirmed removal after explicit resync")
	}
	after, _ := mutatorsRaw(t, f.ds, "target")
	mutatorsUnknownPreserved(t, confirmed, after)
}

func TestUserSettingsPersistenceNewUserAcquisitionDoesNotRebaseKnownTarget(t *testing.T) {
	for _, kind := range []string{"skipping", "favorite-noop", "delete"} {
		t.Run(kind, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "known")
			s := f.service(t, f.ds)
			// Both changes occur after construction. Only the genuinely unknown
			// new ID may be acquired by its first mutator, without a global rebase.
			mutatorsSeed(t, f, "known", mutatorsOther)
			f.user(t, "late")
			mutatorsSeed(t, f, "late", mutatorsDocument)
			known, knownVersion := mutatorsRaw(t, f.ds, "known")
			late, _ := mutatorsRaw(t, f.ds, "late")
			var err error
			switch kind {
			case "skipping":
				err = s.UpdateSkipping("late", SkipPreferences{Intro: models.BoolPtr(true)})
			case "favorite-noop":
				_, err = s.SetFavoriteStation("late", stationFixture(74), false)
			case "delete":
				err = s.Delete("late")
			}
			if err != nil {
				t.Fatal("newly created user was not acquired before mutator")
			}
			after, _ := mutatorsRaw(t, f.ds, "late")
			if kind == "delete" {
				if after != nil {
					t.Fatal("new target explicit Delete was skipped as cached absence")
				}
			} else {
				mutatorsUnknownPreserved(t, late, after)
			}
			if current, version := mutatorsRaw(t, f.ds, "known"); !bytes.Equal(known, current) || version != knownVersion {
				t.Fatal("acquisition touched unrelated known row")
			}
			if err := s.UpdateSkipping("known", SkipPreferences{Intro: models.BoolPtr(true)}); !errors.Is(err, datastore.ErrUserSettingsConflict) {
				t.Fatal("new target acquisition silently rebased a known stale target")
			}
			if err := s.ResyncUserSettings(t.Context()); err != nil {
				t.Fatal("explicit stale-target resync")
			}
			if err := s.UpdateSkipping("known", SkipPreferences{Intro: models.BoolPtr(true)}); err != nil {
				t.Fatal("mutation after explicit resync")
			}
		})
	}
}

func TestUserSettingsPersistenceActualMutatorsRequireResyncAfterLostCommit(t *testing.T) {
	mutatorsNoHTTP(t)
	f := newSettingsPG(t)
	f.user(t, "target")
	mutatorsSeed(t, f, "target", mutatorsDocument)
	uri, proxy := settingsLostCommitProxy(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := datastore.New(ctx, uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal("own literal-loopback-to-Unix proxy datastore")
	}
	defer ds.Close()
	s := f.service(t, ds)
	proxy.drop.Store(true)
	if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)}); !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) || !proxy.dropped.Load() {
		t.Fatal("actual committed response loss did not quarantine")
	}
	committed, version := mutatorsRaw(t, f.ds, "target")
	stored, err := f.ds.UserSettings().Get(ctx, "target")
	if err != nil || stored == nil || stored.Playback.IntroAutoSkip == nil || !*stored.Playback.IntroAutoSkip {
		t.Fatal("independent pool did not observe actual committed write")
	}
	mutations := []func() error{
		func() error { return s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(false)}) },
		func() error { _, e := s.SetFavoriteStation("target", stationFixture(75), false); return e },
		func() error {
			_, e := s.UpdateSportsPreferences("target", 0, models.EmptySportsPreferences())
			return e
		},
		func() error {
			return s.Update("target", models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}})
		},
		func() error { return s.Delete("target") },
	}
	for _, mutate := range mutations {
		if err := mutate(); err == nil {
			t.Fatal("quarantined target accepted a mutator without explicit resync")
		}
		if current, currentVersion := mutatorsRaw(t, f.ds, "target"); !bytes.Equal(committed, current) || currentVersion != version {
			t.Fatal("quarantined mutator changed committed row")
		}
	}
	local, readErr := s.Get("target")
	if local != nil || !errors.Is(readErr, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("public read did not reject the quarantined target")
	}
	// This inspects only retained bookkeeping; it is not a successful public
	// settings read or a grant to use the pre-uncertainty projection.
	s.mu.RLock()
	adopted := s.settings["target"].Playback.IntroAutoSkip != nil
	s.mu.RUnlock()
	if adopted {
		t.Fatal("uncertain result silently adopted into confirmed cache")
	}
	if err := s.ResyncUserSettings(ctx); err != nil {
		t.Fatal("explicit uncertain-target resync")
	}
	if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(false)}); err != nil {
		t.Fatal("confirmed mutation after resync")
	}
}
