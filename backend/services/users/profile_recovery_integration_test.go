package users

import (
	"context"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/user_settings"
)

type recoveryPairFixture struct {
	f        *usersPGFixture
	users    *Service
	settings *user_settings.Service
	target   models.User
	other    models.User
}

func recoveryPair(t *testing.T) recoveryPairFixture {
	t.Helper()
	f := usersPG(t)
	u := f.service(t, f.ds)
	target := mustProfile(t, u, "Original target")
	other := mustProfile(t, u, "Original other")
	seed, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	var initial models.UserSettings
	initial.Display.SimpleMode = models.BoolPtr(false)
	initial.Metadata.PrimaryLanguage = "eng"
	for _, id := range []string{target.ID, other.ID} {
		if err := seed.Update(id, initial); err != nil {
			t.Fatal(err)
		}
	}
	s, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	return recoveryPairFixture{f: f, users: u, settings: s, target: target, other: other}
}

func recoveryFingerprint(t *testing.T, f *usersPGFixture) [3]string {
	t.Helper()
	var result [3]string
	err := f.ds.Pool().QueryRow(t.Context(), `SELECT
	 COALESCE((SELECT jsonb_agg(to_jsonb(u) || jsonb_build_object('physical',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY u.id) FROM users u),'[]'::jsonb)::text,
	 COALESCE((SELECT jsonb_agg(to_jsonb(s) || jsonb_build_object('physical',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY s.user_id) FROM user_settings s),'[]'::jsonb)::text,
	 COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.user_id) FROM user_settings_versions v),'[]'::jsonb)::text`).Scan(&result[0], &result[1], &result[2])
	if err != nil {
		t.Fatal("read fixture fingerprint")
	}
	return result
}

func recoveryCapture(t *testing.T, ctx context.Context, pair recoveryPairFixture) (*datastore.ProfileRecoveryRead, *StorageRecoveryTicket, *user_settings.UserSettingsRecoveryTicket, *datastore.ProfileRecoverySnapshot) {
	t.Helper()
	read, err := pair.f.ds.PrepareProfileRecovery(ctx, pair.target.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(read.Close)
	userTicket, err := pair.users.PrepareStorageRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(userTicket.Close)
	settingsTicket, err := pair.settings.PrepareUserSettingsRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(settingsTicket.Close)
	snapshot, err := pair.f.ds.ReadProfileRecovery(read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Close)
	return read, userTicket, settingsTicket, snapshot
}

func TestProfileRecoveryIntegrationSharedSnapshotPreservesOtherDirtyState(t *testing.T) {
	pair := recoveryPair(t)
	remote := pair.f.service(t, pair.f.ds)
	if _, err := remote.Rename(pair.target.ID, "Persisted target"); err != nil {
		t.Fatal(err)
	}
	settingsRemote, err := user_settings.NewServiceWithStore(pair.f.ds)
	if err != nil {
		t.Fatal(err)
	}
	value, err := settingsRemote.Get(pair.target.ID)
	if err != nil || value == nil {
		t.Fatal("load remote settings")
	}
	value.Metadata.PrimaryLanguage = "fra"
	if err := settingsRemote.Update(pair.target.ID, *value); err != nil {
		t.Fatal(err)
	}
	// An unrelated staged edit is deliberately not persisted by recovery.
	pair.users.mu.Lock()
	dirty := pair.users.users[pair.other.ID]
	dirty.Name = "Unrelated pending edit"
	pair.users.users[pair.other.ID] = dirty
	confirmed := cloneUser(pair.users.persistence.confirmed[pair.other.ID])
	otherRevision := pair.users.persistence.revisions[pair.other.ID]
	pair.users.mu.Unlock()
	before := recoveryFingerprint(t, pair.f)
	ctx := t.Context()
	_, userTicket, settingsTicket, snapshot := recoveryCapture(t, ctx, pair)
	if err := pair.users.ValidateStorageRecovery(ctx, userTicket, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := pair.settings.ValidateUserSettingsRecovery(ctx, settingsTicket, snapshot); err != nil {
		t.Fatal(err)
	}
	userStatus, err := pair.users.InstallStorageRecovery(ctx, userTicket, snapshot)
	if err != nil || !userStatus.TargetPresent || userStatus.PendingUncertainty {
		t.Fatal("install user")
	}
	settingsStatus, err := pair.settings.InstallUserSettingsRecovery(ctx, settingsTicket, snapshot)
	if err != nil || !settingsStatus.TargetPresent || settingsStatus.PendingUncertainty {
		t.Fatal("install settings")
	}
	pair.users.mu.RLock()
	got := pair.users.users[pair.target.ID]
	other := pair.users.users[pair.other.ID]
	confirmedAfter := pair.users.persistence.confirmed[pair.other.ID]
	revisionAfter := pair.users.persistence.revisions[pair.other.ID]
	pair.users.mu.RUnlock()
	if got.Name != "Persisted target" || !reflect.DeepEqual(other, dirty) || !reflect.DeepEqual(confirmedAfter, confirmed) || revisionAfter != otherRevision {
		t.Fatal("target recovery changed unrelated bookkeeping")
	}
	actual, err := pair.settings.Get(pair.target.ID)
	if err != nil || actual == nil || actual.Metadata.PrimaryLanguage != "fra" {
		t.Fatal("settings did not use shared snapshot")
	}
	if recoveryFingerprint(t, pair.f) != before {
		t.Fatal("recovery wrote database")
	}
}

func TestProfileRecoveryIntegrationRejectsLateTicketsAndReplay(t *testing.T) {
	pair := recoveryPair(t)
	ctx := t.Context()
	read, userTicket, settingsTicket, snapshot := recoveryCapture(t, ctx, pair)
	if ticket, err := pair.users.PrepareStorageRecovery(ctx, read); err == nil {
		ticket.Close()
		t.Fatal("user ticket captured after read")
	}
	if ticket, err := pair.settings.PrepareUserSettingsRecovery(ctx, read); err == nil {
		ticket.Close()
		t.Fatal("settings ticket captured after read")
	}
	if _, err := pair.users.InstallStorageRecovery(ctx, userTicket, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := pair.settings.InstallUserSettingsRecovery(ctx, settingsTicket, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := pair.users.InstallStorageRecovery(ctx, userTicket, snapshot); err == nil {
		t.Fatal("user ticket replay")
	}
	if _, err := pair.settings.InstallUserSettingsRecovery(ctx, settingsTicket, snapshot); err == nil {
		t.Fatal("settings ticket replay")
	}
}

func TestProfileRecoveryIntegrationCanceledBeforeInstall(t *testing.T) {
	pair := recoveryPair(t)
	settingsBefore, err := pair.settings.Get(pair.target.ID)
	if err != nil || settingsBefore == nil {
		t.Fatal("load original settings")
	}
	remote := pair.f.service(t, pair.f.ds)
	if _, err := remote.Rename(pair.target.ID, "Must not be installed"); err != nil {
		t.Fatal(err)
	}
	settingsRemote, err := user_settings.NewServiceWithStore(pair.f.ds)
	if err != nil {
		t.Fatal(err)
	}
	value, err := settingsRemote.Get(pair.target.ID)
	if err != nil || value == nil {
		t.Fatal("load remote settings")
	}
	value.Metadata.PrimaryLanguage = "spa"
	if err := settingsRemote.Update(pair.target.ID, *value); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, userTicket, settingsTicket, snapshot := recoveryCapture(t, ctx, pair)
	before := recoveryFingerprint(t, pair.f)
	cancel()
	if err := pair.users.ValidateStorageRecovery(ctx, userTicket, snapshot); err == nil {
		t.Fatal("canceled validation")
	}
	if _, err := pair.users.InstallStorageRecovery(ctx, userTicket, snapshot); err == nil {
		t.Fatal("canceled user install")
	}
	if _, err := pair.settings.InstallUserSettingsRecovery(ctx, settingsTicket, snapshot); err == nil {
		t.Fatal("canceled settings install")
	}
	if recoveryFingerprint(t, pair.f) != before {
		t.Fatal("canceled recovery wrote")
	}
	pair.users.mu.RLock()
	name := pair.users.users[pair.target.ID].Name
	pair.users.mu.RUnlock()
	settingsAfter, err := pair.settings.Get(pair.target.ID)
	if name != pair.target.Name || err != nil || !reflect.DeepEqual(settingsBefore, settingsAfter) {
		t.Fatal("canceled recovery changed a cache")
	}
}

func TestProfileRecoveryIntegrationInterveningWrite(t *testing.T) {
	pair := recoveryPair(t)
	ctx := t.Context()
	_, userTicket, _, snapshot := recoveryCapture(t, ctx, pair)
	if _, err := pair.users.Rename(pair.target.ID, "Newer local write"); err != nil {
		t.Fatal(err)
	}
	if err := pair.users.ValidateStorageRecovery(ctx, userTicket, snapshot); err == nil {
		t.Fatal("stale validation")
	}
	if _, err := pair.users.InstallStorageRecovery(ctx, userTicket, snapshot); err == nil {
		t.Fatal("stale install")
	}
	// The settings install is deliberately not called: there is no coordinator.
	pair.users.mu.RLock()
	name := pair.users.users[pair.target.ID].Name
	pair.users.mu.RUnlock()
	if name != "Newer local write" {
		t.Fatal("newer write lost")
	}
}
