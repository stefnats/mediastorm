package user_settings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
)

func startupSettingsRaw(t *testing.T, f *settingsPGFixture, id string) string {
	t.Helper()
	var raw string
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT COALESCE((SELECT to_jsonb(s)::text||'/'||concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text) FROM user_settings s WHERE user_id=$1),'absent')`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func startupSettingsProfile(t *testing.T, f *settingsPGFixture, id string, kids bool) {
	t.Helper()
	f.user(t, id)
	f.exec(t, `UPDATE users SET is_kids_profile=$2 WHERE id=$1`, id, kids)
}
func TestStartupKidsDefaultsServiceNilFalseUnknownAndTargetOnly(t *testing.T) {
	f := newSettingsPG(t)
	for _, id := range []string{"a", "b", "adult", "foreign"} {
		startupSettingsProfile(t, f, id, id != "adult")
	}
	s := f.service(t, f.ds)
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES
 ('a','{"future":{"number":123456789012345678901234567890.123456789},"display":{"future":{"n":9007199254740993}}}'),
 ('b','{"display":{"simpleMode":false},"future":[1,2,3]}')
 ON CONFLICT(user_id) DO UPDATE SET settings=excluded.settings`)
	if err := s.ResyncUserSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	dirty := s.settings["foreign"]
	dirty.Metadata.PrimaryLanguage = "synthetic-dirty"
	s.settings["foreign"] = dirty
	s.markUserSettingsUnknownLocked([]string{"foreign"})
	foreignRecord := s.pg.records["foreign"]
	foreignMarker := s.pg.unknownMarkers["foreign"]
	adultRecord := s.pg.records["adult"]
	s.mu.Unlock()
	beforeForeign := startupSettingsRaw(t, f, "foreign")
	beforeAdult := startupSettingsRaw(t, f, "adult")
	beforeB := startupSettingsRaw(t, f, "b")
	changed, err := s.ApplyStartupKidsProfileDefaults(t.Context(), " b ", "a", "adult", "a", "")
	if err != nil || changed != 1 {
		t.Fatal("wrong guarded batch result", changed, err)
	}
	if startupSettingsRaw(t, f, "b") != beforeB || startupSettingsRaw(t, f, "adult") != beforeAdult || startupSettingsRaw(t, f, "foreign") != beforeForeign {
		t.Fatal("no-op/adult/foreign row changed")
	}
	got, err := s.Get("a")
	if err != nil || got == nil || got.Display.SimpleMode == nil || !*got.Display.SimpleMode {
		t.Fatal("known target ACK missing", err)
	}
	*got.Display.SimpleMode = false
	again, err := s.Get("a")
	if err != nil || !*again.Display.SimpleMode {
		t.Fatal("confirmed settings pointer escaped")
	}
	var raw []byte
	if err = f.ds.Pool().QueryRow(t.Context(), `SELECT settings FROM user_settings WHERE user_id='a'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || !bytes.Contains(object["future"], []byte("123456789012345678901234567890.123456789")) {
		t.Fatal("unknown precise root JSON lost")
	}
	var display map[string]json.RawMessage
	if json.Unmarshal(object["display"], &display) != nil || !bytes.Contains(display["future"], []byte("9007199254740993")) {
		t.Fatal("unknown nested JSON lost")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !reflect.DeepEqual(s.settings["foreign"], dirty) || !reflect.DeepEqual(s.pg.records["foreign"], foreignRecord) || s.pg.unknownMarkers["foreign"] != foreignMarker || !s.pg.uncertain["foreign"] || !reflect.DeepEqual(s.pg.records["adult"], adultRecord) {
		t.Fatal("startup adopted an adult or changed foreign dirty/Unknown state")
	}
}
func TestStartupKidsDefaultsServiceCurrentAdultAndMissingCandidates(t *testing.T) {
	t.Run("cached Kids is now adult", func(t *testing.T) {
		f := newSettingsPG(t)
		startupSettingsProfile(t, f, "a", true)
		s := f.service(t, f.ds)
		f.exec(t, `UPDATE users SET is_kids_profile=false WHERE id='a'`)
		beforeDB := profileSettingsRecoveryDB(t, f.ds)
		beforeCache := profileSettingsRecoveryCache(s)
		n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "a")
		if err != nil || n != 0 {
			t.Fatal("current adult was not a checked no-op", err)
		}
		profileSettingsRecoveryNoWrite(t, f.ds, beforeDB)
		profileSettingsRecoveryUnchanged(t, s, beforeCache)
	})
	t.Run("outside initial candidate cut", func(t *testing.T) {
		f := newSettingsPG(t)
		startupSettingsProfile(t, f, "a", true)
		s := f.service(t, f.ds)
		startupSettingsProfile(t, f, "late", true)
		if n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "a"); err != nil || n != 1 {
			t.Fatal(err)
		}
		if startupSettingsRaw(t, f, "late") != "absent" {
			t.Fatal("startup invented a continuing creation-default policy")
		}
		before := profileSettingsRecoveryDB(t, f.ds)
		if n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "late"); n != 0 || !errors.Is(err, datastore.ErrUserSettingsConflict) {
			t.Fatal("unobserved target silently acquired", err)
		}
		profileSettingsRecoveryNoWrite(t, f.ds, before)
	})
}
func TestStartupKidsDefaultsServiceNoRebaseOrImplicitRecovery(t *testing.T) {
	for _, what := range []string{"newer false", "stale false no-op", "dirty", "quarantined", "absent tombstone"} {
		t.Run(what, func(t *testing.T) {
			f := newSettingsPG(t)
			startupSettingsProfile(t, f, "a", true)
			s := f.service(t, f.ds)
			if what == "stale false no-op" {
				value := models.UserSettings{}
				value.Display.SimpleMode = models.BoolPtr(false)
				if err := s.Update("a", value); err != nil {
					t.Fatal(err)
				}
			}
			switch what {
			case "newer false", "stale false no-op":
				second := f.service(t, f.ds)
				value := models.UserSettings{}
				value.Display.SimpleMode = models.BoolPtr(what == "stale false no-op")
				if err := second.Update("a", value); err != nil {
					t.Fatal(err)
				}
			case "dirty":
				s.mu.Lock()
				value := s.settings["a"]
				value.Metadata.PrimaryLanguage = "unsaved"
				s.settings["a"] = value
				s.mu.Unlock()
			case "quarantined":
				s.mu.Lock()
				s.markUserSettingsUnknownLocked([]string{"a"})
				s.mu.Unlock()
			case "absent tombstone":
				f.exec(t, `DELETE FROM users WHERE id='a'`)
				ctx := t.Context()
				read := prepareProfileSettingsRecovery(t, ctx, s, f.ds, "a")
				installProfileSettingsRecovery(t, ctx, s, read)
				startupSettingsProfile(t, f, "a", true)
			}
			before := profileSettingsRecoveryDB(t, f.ds)
			cache := profileSettingsRecoveryCache(s)
			n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "a")
			want := datastore.ErrUserSettingsConflict
			if what == "quarantined" {
				want = datastore.ErrUserSettingsOutcomeUnknown
			}
			if n != 0 || !errors.Is(err, want) {
				t.Fatal("known stale/dirty/Unknown target automatically rebased", err)
			}
			profileSettingsRecoveryNoWrite(t, f.ds, before)
			profileSettingsRecoveryUnchanged(t, s, cache)
		})
	}
}
func TestStartupKidsDefaultsServiceBatchRollbackAndCancel(t *testing.T) {
	for _, what := range []string{"second update failure", "already canceled"} {
		t.Run(what, func(t *testing.T) {
			f := newSettingsPG(t)
			for _, id := range []string{"a", "b"} {
				startupSettingsProfile(t, f, id, true)
			}
			s := f.service(t, f.ds)
			if what == "second update failure" {
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}'),('b','{}') ON CONFLICT(user_id) DO UPDATE SET settings=excluded.settings`)
				if err := s.ResyncUserSettings(t.Context()); err != nil {
					t.Fatal(err)
				}
				f.exec(t, `CREATE FUNCTION reject_startup_settings_b() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.user_id='b' THEN RAISE EXCEPTION 'synthetic startup failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_startup_settings_b BEFORE UPDATE ON user_settings FOR EACH ROW EXECUTE FUNCTION reject_startup_settings_b()`)
			}
			before := profileSettingsRecoveryDB(t, f.ds)
			cache := profileSettingsRecoveryCache(s)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if what == "already canceled" {
				cancel()
			}
			n, err := s.ApplyStartupKidsProfileDefaults(ctx, "b", "a")
			if n != 0 || err == nil {
				t.Fatal("failed batch reported success")
			}
			if what == "already canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("original cancellation lost", err)
			}
			profileSettingsRecoveryNoWrite(t, f.ds, before)
			profileSettingsRecoveryUnchanged(t, s, cache)
		})
	}
}
func TestStartupKidsDefaultsServiceActualLostCommitQuarantine(t *testing.T) {
	f := newSettingsPG(t)
	for _, id := range []string{"a", "b", "adult", "foreign"} {
		startupSettingsProfile(t, f, id, id != "adult")
	}
	uri, proxy := settingsLostCommitProxy(t, f)
	ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal("own proxy datastore", err)
	}
	t.Cleanup(ds.Close)
	s := f.service(t, ds)
	s.mu.Lock()
	dirty := s.settings["foreign"]
	dirty.Metadata.PrimaryLanguage = "keep-dirty"
	s.settings["foreign"] = dirty
	s.markUserSettingsUnknownLocked([]string{"foreign"})
	marker := s.pg.unknownMarkers["foreign"]
	aRecord, bRecord, adultRecord := s.pg.records["a"], s.pg.records["b"], s.pg.records["adult"]
	s.mu.Unlock()
	beforeForeign := startupSettingsRaw(t, f, "foreign")
	beforeAdult := startupSettingsRaw(t, f, "adult")
	proxy.drop.Store(true)
	n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "adult", "b", "a")
	if n != 0 || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) || !proxy.dropped.Load() {
		t.Fatal("actual lost COMMIT ACK not classified Unknown", err)
	}
	for _, id := range []string{"a", "b"} {
		if settingsText(t, f, id, "display.simpleMode") != "true" {
			t.Fatal("independent pool did not observe durable batch", id)
		}
		if got, err := s.Get(id); got != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
			t.Fatal("Unknown target readable as confirmed", id, err)
		}
	}
	before := profileSettingsRecoveryDB(t, f.ds)
	if n, err = s.ApplyStartupKidsProfileDefaults(t.Context(), "a"); n != 0 || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("Unknown commit was automatically retried", err)
	}
	profileSettingsRecoveryNoWrite(t, f.ds, before)
	s.mu.RLock()
	if !reflect.DeepEqual(s.pg.records["a"], aRecord) || !reflect.DeepEqual(s.pg.records["b"], bRecord) || !reflect.DeepEqual(s.pg.records["adult"], adultRecord) || s.pg.uncertain["adult"] || s.pg.unknownMarkers["foreign"] != marker || !reflect.DeepEqual(s.settings["foreign"], dirty) {
		s.mu.RUnlock()
		t.Fatal("Unknown quarantined/adopted unrelated state or published unconfirmed records")
	}
	s.mu.RUnlock()
	if startupSettingsRaw(t, f, "adult") != beforeAdult || startupSettingsRaw(t, f, "foreign") != beforeForeign {
		t.Fatal("Unknown batch wrote adult/foreign target")
	}
}
func TestStartupKidsDefaultsServicePGOnlyPort(t *testing.T) {
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ApplyStartupKidsProfileDefaults(t.Context(), "a"); n != 0 || !errors.Is(err, datastore.ErrUserSettingsUnavailable) {
		t.Fatal("PG error path downgraded to JSON", err)
	}
	if got, err := s.Get("a"); err != nil || got != nil {
		t.Fatal("PG-only port mutated JSON defaults", err)
	}
}
