package user_settings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

// These are internal historical read/adopt tests. They do not establish public
// authentication, current policy, or an atomic Users/Settings cache coordinator.
// All PostgreSQL work uses the existing own-schema, NOLOGIN-role fixture; its
// only TCP connection is the literal-loopback lost-COMMIT-response proxy.
type profileSettingsRecoveryRead struct {
	plan     *datastore.ProfileRecoveryRead
	ticket   *UserSettingsRecoveryTicket
	snapshot *datastore.ProfileRecoverySnapshot
}

func prepareProfileSettingsRecovery(t *testing.T, ctx context.Context, s *Service, ds *datastore.DataStore, id string) profileSettingsRecoveryRead {
	t.Helper()
	plan, err := ds.PrepareProfileRecovery(ctx, id)
	if err != nil {
		t.Fatal("prepare original recovery read", err)
	}
	t.Cleanup(plan.Close)
	ticket, err := s.PrepareUserSettingsRecovery(ctx, plan)
	if err != nil {
		t.Fatal("capture settings before SQL read", err)
	}
	t.Cleanup(ticket.Close)
	snapshot, err := ds.ReadProfileRecovery(plan)
	if err != nil {
		t.Fatal("read original recovery snapshot", err)
	}
	t.Cleanup(snapshot.Close)
	return profileSettingsRecoveryRead{plan, ticket, snapshot}
}

func installProfileSettingsRecovery(t *testing.T, ctx context.Context, s *Service, read profileSettingsRecoveryRead) datastore.ProfileRecoveryStatus {
	t.Helper()
	// Validation is explicitly non-consuming; the exact same ticket/snapshot
	// must remain usable by the subsequent historical installation.
	for range 2 {
		if err := s.ValidateUserSettingsRecovery(ctx, read.ticket, read.snapshot); err != nil {
			t.Fatal("validate original settings recovery", err)
		}
	}
	status, err := s.InstallUserSettingsRecovery(ctx, read.ticket, read.snapshot)
	if err != nil {
		t.Fatal("install original settings recovery", err)
	}
	return status
}

// Include physical row versions so even a same-value UPDATE/default application
// is observable. This checks both payload rows and the absence/lifecycle ledger.
func profileSettingsRecoveryDB(t *testing.T, ds *datastore.DataStore) []byte {
	t.Helper()
	var raw []byte
	err := ds.Pool().QueryRow(t.Context(), `SELECT jsonb_build_object(
      'users', (SELECT COALESCE(jsonb_agg(jsonb_build_object('row',to_jsonb(u),'version',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY u.id),'[]'::jsonb) FROM users u),
      'settings', (SELECT COALESCE(jsonb_agg(jsonb_build_object('row',to_jsonb(s),'version',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY s.user_id),'[]'::jsonb) FROM user_settings s),
      'versions', (SELECT COALESCE(jsonb_agg(jsonb_build_object('row',to_jsonb(v),'version',concat(v.tableoid::oid,':',v.xmin::text,':',v.ctid::text)) ORDER BY v.user_id),'[]'::jsonb) FROM user_settings_versions v))`).Scan(&raw)
	if err != nil {
		t.Fatal("read own physical recovery observations", err)
	}
	return raw
}

func profileSettingsRecoveryNoWrite(t *testing.T, ds *datastore.DataStore, before []byte) {
	t.Helper()
	if !bytes.Equal(before, profileSettingsRecoveryDB(t, ds)) {
		t.Fatal("read/adopt changed a stored row, lifecycle version, or default")
	}
}

func profileRecoveryMap[K comparable, V any](source map[K]V) map[K]V {
	copied := make(map[K]V, len(source))
	for k, v := range source {
		copied[k] = v
	}
	return copied
}

type profileSettingsCacheObservation struct {
	current     map[string]models.UserSettings
	confirmed   map[string]models.UserSettings
	records     map[string]datastore.UserSettingsRecord
	uncertainty any
	markers     map[string]*userSettingsUnknownMarker
}

func profileSettingsRecoveryCache(s *Service) profileSettingsCacheObservation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := profileSettingsCacheObservation{
		current:     make(map[string]models.UserSettings, len(s.settings)),
		confirmed:   make(map[string]models.UserSettings, len(s.pg.confirmed)),
		records:     profileRecoveryMap(s.pg.records),
		uncertainty: profileRecoveryMap(s.pg.uncertain),
		markers:     profileRecoveryMap(s.pg.unknownMarkers),
	}
	for id, value := range s.settings {
		state.current[id] = cloneSettingsValue(value)
	}
	for id, value := range s.pg.confirmed {
		state.confirmed[id] = cloneSettingsValue(value)
	}
	for id, record := range state.records {
		if record.Settings != nil {
			copy := cloneSettingsValue(*record.Settings)
			record.Settings = &copy
			state.records[id] = record
		}
	}
	return state
}

func profileSettingsRecoveryUnchanged(t *testing.T, s *Service, before profileSettingsCacheObservation) {
	t.Helper()
	after := profileSettingsRecoveryCache(s)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected recovery adopted state or cleared another marker")
	}
	for id, marker := range before.markers {
		if after.markers[id] != marker {
			t.Fatal("rejected recovery replaced an uncertainty identity")
		}
	}
}

func profileSettingsRecoveryProxy(t *testing.T, f *settingsPGFixture) (*datastore.DataStore, *settingsCommitProxy) {
	t.Helper()
	uri, proxy := settingsLostCommitProxy(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := datastore.New(ctx, uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal("open own recovery COMMIT-response proxy", err)
	}
	t.Cleanup(ds.Close)
	return ds, proxy
}

func profileSettingsActualUnknown(t *testing.T, s *Service, proxy *settingsCommitProxy, ids ...string) {
	t.Helper()
	proxy.dropped.Store(false)
	proxy.drop.Store(true)
	var err error
	if len(ids) == 1 {
		err = s.UpdateSkipping(ids[0], SkipPreferences{Intro: models.BoolPtr(true)})
	} else {
		// Stage two desired values under the same lock used by the real
		// multi-target persistence path; one actual transaction loses its ACK.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		s.mu.Lock()
		for _, id := range ids {
			value := s.settings[id]
			value.Playback.IntroAutoSkip = models.BoolPtr(true)
			s.settings[id] = value
		}
		err = s.persistPGLocked(ctx, ids, nil)
		s.mu.Unlock()
	}
	if !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) || !proxy.dropped.Load() {
		t.Fatal("actual PostgreSQL COMMIT response loss did not produce uncertainty", err)
	}
	for _, id := range ids {
		value, getErr := s.Get(id)
		if value != nil || !errors.Is(getErr, datastore.ErrUserSettingsOutcomeUnknown) {
			t.Fatal("unknown target was exposed as an ordinary successful read")
		}
	}
}

func TestUserSettingsProfileRecoveryActualUnknownPreservesForeignState(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("two_targets_%t", batch), func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			for _, id := range []string{"a", "b", "empty"} {
				f.user(t, id)
			}
			mutatorsSeed(t, f, "a", mutatorsDocument)
			mutatorsSeed(t, f, "b", mutatorsOther)
			ds, proxy := profileSettingsRecoveryProxy(t, f)
			s := f.service(t, ds)
			ids := []string{"a"}
			if batch {
				ids = append(ids, "b")
			}
			profileSettingsActualUnknown(t, s, proxy, ids...)
			for _, id := range ids {
				actual, err := f.ds.UserSettings().Get(t.Context(), id)
				if err != nil || actual == nil || actual.Playback.IntroAutoSkip == nil || !*actual.Playback.IntroAutoSkip {
					t.Fatal("independent pool did not observe the actual committed value")
				}
			}
			// A separate unsaved value is intentionally not part of recovery A.
			s.mu.Lock()
			dirty := s.settings["b"]
			dirty.Metadata.PrimaryLanguage = "deu"
			s.settings["b"] = dirty
			s.mu.Unlock()
			foreign := profileSettingsRecoveryCache(s)
			db := profileSettingsRecoveryDB(t, f.ds)
			read := prepareProfileSettingsRecovery(t, t.Context(), s, ds, "a")
			status := installProfileSettingsRecovery(t, t.Context(), s, read)
			if !status.TargetPresent || status.PendingUncertainty != batch {
				t.Fatal("recovery status hid target presence or foreign uncertainty")
			}
			value, err := s.Get("a")
			if err != nil || value == nil || value.Playback.IntroAutoSkip == nil || !*value.Playback.IntroAutoSkip || value.Display.SimpleMode == nil || *value.Display.SimpleMode {
				t.Fatal("recovery did not adopt the actual A value with explicit false")
			}
			current := profileSettingsRecoveryCache(s)
			if !reflect.DeepEqual(foreign.current["b"], current.current["b"]) ||
				!reflect.DeepEqual(foreign.confirmed["b"], current.confirmed["b"]) ||
				!reflect.DeepEqual(foreign.records["b"], current.records["b"]) || foreign.markers["b"] != current.markers["b"] {
				t.Fatal("recovery A adopted or discarded foreign dirty/confirmed data")
			}
			if batch {
				if value, err := s.Get("b"); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
					t.Fatal("recovery A cleared B's real unknown marker")
				}
			}
			profileSettingsRecoveryNoWrite(t, f.ds, db)
			before := profileSettingsRecoveryCache(s)
			if _, err := s.InstallUserSettingsRecovery(t.Context(), read.ticket, read.snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("consumed recovery was replayed", err)
			}
			profileSettingsRecoveryUnchanged(t, s, before)
			profileSettingsRecoveryNoWrite(t, f.ds, db)
		})
	}
}

func TestUserSettingsProfileRecoveryOldTicketCannotClearNewUnknown(t *testing.T) {
	mutatorsNoHTTP(t)
	f := newSettingsPG(t)
	f.user(t, "target")
	ds, proxy := profileSettingsRecoveryProxy(t, f)
	s := f.service(t, ds)
	profileSettingsActualUnknown(t, s, proxy, "target")
	old := prepareProfileSettingsRecovery(t, t.Context(), s, ds, "target")
	current := prepareProfileSettingsRecovery(t, t.Context(), s, ds, "target")
	installProfileSettingsRecovery(t, t.Context(), s, current)
	if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(false)}); err != nil {
		t.Fatal("new original mutation after explicit recovery", err)
	}
	profileSettingsActualUnknown(t, s, proxy, "target")
	before := profileSettingsRecoveryCache(s)
	db := profileSettingsRecoveryDB(t, f.ds)
	if err := s.ValidateUserSettingsRecovery(t.Context(), old.ticket, old.snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("old capture validated over a successor unknown marker", err)
	}
	if _, err := s.InstallUserSettingsRecovery(t.Context(), old.ticket, old.snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("old capture cleared a successor unknown marker", err)
	}
	profileSettingsRecoveryUnchanged(t, s, before)
	profileSettingsRecoveryNoWrite(t, f.ds, db)
	if value, err := s.Get("target"); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("successor unknown target became publicly readable")
	}
}

// The second marker here is a deliberately isolated local bookkeeping event,
// using the same helper as the actual unknown-write path. The first marker and
// both quarantines are established by a real two-target lost COMMIT ACK above.
// No additional SQL-commit loss is claimed for the isolated marker replacement.
func TestUserSettingsProfileRecoveryMarkerOnlySuccessor(t *testing.T) {
	for _, stage := range []string{"before_read", "after_read"} {
		t.Run(stage, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "a")
			f.user(t, "b")
			ds, proxy := profileSettingsRecoveryProxy(t, f)
			s := f.service(t, ds)
			profileSettingsActualUnknown(t, s, proxy, "a", "b")
			plan, err := ds.PrepareProfileRecovery(t.Context(), "a")
			if err != nil {
				t.Fatal(err)
			}
			defer plan.Close()
			ticket, err := s.PrepareUserSettingsRecovery(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}
			defer ticket.Close()
			captured := profileSettingsRecoveryCache(s)
			replaceMarker := func() {
				s.mu.Lock()
				s.markUserSettingsUnknownLocked([]string{"a"})
				s.mu.Unlock()
			}
			if stage == "before_read" {
				replaceMarker()
			}
			snapshot, err := ds.ReadProfileRecovery(plan)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			if stage == "after_read" {
				replaceMarker()
			}
			before := profileSettingsRecoveryCache(s)
			if !reflect.DeepEqual(captured.current, before.current) ||
				!reflect.DeepEqual(captured.confirmed, before.confirmed) ||
				!reflect.DeepEqual(captured.records, before.records) ||
				!reflect.DeepEqual(captured.uncertainty, before.uncertainty) ||
				captured.markers["a"] == before.markers["a"] || captured.markers["b"] != before.markers["b"] {
				t.Fatal("isolated successor probe changed more than A's marker identity")
			}
			db := profileSettingsRecoveryDB(t, f.ds)
			if _, err := s.InstallUserSettingsRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("same-revision successor marker was cleared by an older capture", err)
			}
			profileSettingsRecoveryUnchanged(t, s, before)
			profileSettingsRecoveryNoWrite(t, f.ds, db)
		})
	}
}

func TestUserSettingsProfileRecoveryAbsenceRequiresExplicitReappearance(t *testing.T) {
	mutatorsNoHTTP(t)
	f := newSettingsPG(t)
	f.user(t, "target")
	s := f.service(t, f.ds)
	f.exec(t, `DELETE FROM users WHERE id='target'`)
	db := profileSettingsRecoveryDB(t, f.ds)
	absent := prepareProfileSettingsRecovery(t, t.Context(), s, f.ds, "target")
	status := installProfileSettingsRecovery(t, t.Context(), s, absent)
	if status.TargetPresent || status.PendingUncertainty {
		t.Fatal("confirmed absence was not kept as a scoped tombstone")
	}
	profileSettingsRecoveryNoWrite(t, f.ds, db)
	f.user(t, "target")
	mutatorsSeed(t, f, "target", mutatorsDocument)
	recreated := profileSettingsRecoveryDB(t, f.ds)
	if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)}); !errors.Is(err, datastore.ErrUserSettingsConflict) {
		t.Fatal("normal mutator silently acquired a reused ID after confirmed absence", err)
	}
	profileSettingsRecoveryNoWrite(t, f.ds, recreated)
	present := prepareProfileSettingsRecovery(t, t.Context(), s, f.ds, "target")
	if status = installProfileSettingsRecovery(t, t.Context(), s, present); !status.TargetPresent {
		t.Fatal("new explicit recovery did not acquire the reused target")
	}
	profileSettingsRecoveryNoWrite(t, f.ds, recreated)
	if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)}); err != nil {
		t.Fatal("explicitly recovered reused target remained unusable", err)
	}
}

func TestUserSettingsProfileRecoveryRejectsLocalSuccessorsAndDirtyTarget(t *testing.T) {
	for _, mode := range []string{"confirmed_revision", "unsaved_target"} {
		t.Run(mode, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "target")
			s := f.service(t, f.ds)
			plan, err := f.ds.PrepareProfileRecovery(t.Context(), "target")
			if err != nil {
				t.Fatal(err)
			}
			defer plan.Close()
			ticket, err := s.PrepareUserSettingsRecovery(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}
			defer ticket.Close()
			if mode == "confirmed_revision" {
				if err := s.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)}); err != nil {
					t.Fatal(err)
				}
			} else {
				s.mu.Lock()
				value := s.settings["target"]
				value.Metadata.PrimaryLanguage = "ita"
				s.settings["target"] = value
				s.mu.Unlock()
			}
			snapshot, err := f.ds.ReadProfileRecovery(plan)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			before := profileSettingsRecoveryCache(s)
			db := profileSettingsRecoveryDB(t, f.ds)
			if _, err := s.InstallUserSettingsRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("recovery overwrote a successor local revision or dirty target", err)
			}
			profileSettingsRecoveryUnchanged(t, s, before)
			profileSettingsRecoveryNoWrite(t, f.ds, db)
		})
	}
}

func TestUserSettingsProfileRecoveryRejectsWrongOrClosedCapabilities(t *testing.T) {
	for _, kind := range []string{"copied_ticket", "copied_snapshot", "other_service", "other_store", "other_read", "other_context", "cancelled", "closed_ticket", "closed_snapshot", "closed_read"} {
		t.Run(kind, func(t *testing.T) {
			mutatorsNoHTTP(t)
			f := newSettingsPG(t)
			f.user(t, "target")
			ds := f.open(t)
			defer ds.Close()
			s := f.service(t, ds)
			other := f.service(t, ds)
			otherDS := f.open(t)
			defer otherDS.Close()
			otherStore := f.service(t, otherDS)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			read := prepareProfileSettingsRecovery(t, ctx, s, ds, "target")
			consumer, ticket, snapshot, useCtx := s, read.ticket, read.snapshot, ctx
			expected := datastore.ErrProfileRecoveryChanged
			switch kind {
			case "copied_ticket":
				copy := *ticket
				ticket = &copy
			case "copied_snapshot":
				copy := *snapshot
				snapshot = &copy
			case "other_service":
				consumer = other
			case "other_store":
				consumer = otherStore
			case "other_read":
				separate := prepareProfileSettingsRecovery(t, ctx, s, ds, "target")
				snapshot = separate.snapshot
			case "other_context":
				useCtx = context.WithValue(ctx, profileSettingsRecoveryContextKey{}, "different original context")
			case "cancelled":
				cancel()
				expected = context.Canceled
			case "closed_ticket":
				ticket.Close()
			case "closed_snapshot":
				snapshot.Close()
			case "closed_read":
				read.plan.Close()
			}
			before, otherBefore, storeBefore := profileSettingsRecoveryCache(s), profileSettingsRecoveryCache(other), profileSettingsRecoveryCache(otherStore)
			db := profileSettingsRecoveryDB(t, f.ds)
			// Historical installation has no SQL phase. Local invalidity must
			// keep its precise error even with the original pool unavailable.
			ds.Close()
			if _, err := consumer.InstallUserSettingsRecovery(useCtx, ticket, snapshot); !errors.Is(err, expected) {
				t.Fatal("invalid original recovery capability was accepted or used SQL", err)
			}
			profileSettingsRecoveryUnchanged(t, s, before)
			profileSettingsRecoveryUnchanged(t, other, otherBefore)
			profileSettingsRecoveryUnchanged(t, otherStore, storeBefore)
			profileSettingsRecoveryNoWrite(t, f.ds, db)
		})
	}
}

type profileSettingsRecoveryContextKey struct{}

func TestUserSettingsProfileRecoveryHistoricalAdoptionDoesNotReplayIO(t *testing.T) {
	mutatorsNoHTTP(t)
	f := newSettingsPG(t)
	f.user(t, "target")
	ds := f.open(t)
	defer ds.Close()
	s := f.service(t, ds)
	// The stored row is deliberately added after service construction. A
	// constructor/whole-store migration would mutate this literal document.
	mutatorsSeed(t, f, "target", `{"futureRoot":{"integer":9007199254740993},"display":{"simpleMode":false},"playback":{"introAutoSkip":true}}`)
	read := prepareProfileSettingsRecovery(t, t.Context(), s, ds, "target")
	observed, err := f.ds.UserSettings().Get(t.Context(), "target")
	if err != nil || observed == nil {
		t.Fatal("read independent original snapshot", err)
	}
	// A later remote write is outside the historical ACK's freshness claim.
	// It must not be adopted by another hidden read, nor overwritten by replay.
	f.exec(t, `UPDATE user_settings SET settings=jsonb_set(settings,'{metadata}', '{"primaryLanguage":"ita"}'::jsonb) WHERE user_id='target'`)
	db := profileSettingsRecoveryDB(t, f.ds)
	ds.Close()
	status := installProfileSettingsRecovery(t, t.Context(), s, read)
	if !status.TargetPresent || status.PendingUncertainty {
		t.Fatal("historical recovery status")
	}
	value, err := s.Get("target")
	if err != nil || !reflect.DeepEqual(observed, value) {
		t.Fatal("historical ACK performed another read or default migration", err)
	}
	profileSettingsRecoveryNoWrite(t, f.ds, db)
}
