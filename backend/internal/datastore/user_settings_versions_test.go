package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"novastream/models"
)

type versionsDenyHTTP struct{ calls atomic.Int32 }

func (d *versionsDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("HTTP denied in settings-version fixture")
}

type settingsVersionsFixture struct {
	ds          *DataStore
	admin       *pgxpool.Pool
	uri, schema string
}

// This fixture owns only a new NOLOGIN role/schema. Full production New runs
// every migration; no minimal table surrogate or external network is used.
func newSettingsVersionsPG(t *testing.T) *settingsVersionsFixture {
	t.Helper()
	raw := os.Getenv("USERS_PERSISTENCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("synthetic local Unix-socket fixture required")
	}
	previous := http.DefaultTransport
	denied := &versionsDenyHTTP{}
	http.DefaultTransport = denied
	t.Cleanup(func() {
		http.DefaultTransport = previous
		if denied.calls.Load() != 0 {
			t.Error("datastore persistence attempted HTTP")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect own synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_settings_versions_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own NOLOGIN role")
	}
	f := &settingsVersionsFixture{admin: admin, schema: name}
	t.Cleanup(func() {
		if f.ds != nil {
			f.ds.Close()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, e := admin.Exec(cleanup, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
			t.Error("drop own settings-version schema")
		}
		if _, e := admin.Exec(cleanup, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("drop own settings-version role")
		}
		admin.Close()
	})
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("create own settings-version schema")
	}
	q := u.Query()
	q.Set("search_path", name+",pg_catalog")
	q.Set("role", name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	f.uri = u.String()
	f.ds = f.open(t)
	f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master) VALUES('settings_owner','Synthetic settings owner','synthetic-unused-hash',false)`)
	return f
}

func (f *settingsVersionsFixture) open(t *testing.T) *DataStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := New(ctx, f.uri, WithMaxConns(4), WithMinConns(1))
	if err != nil {
		t.Fatal("full own-schema datastore constructor")
	}
	var role, schema string
	if err = ds.Pool().QueryRow(ctx, `SELECT current_user,current_schema()`).Scan(&role, &schema); err != nil || role != f.schema || schema != f.schema {
		ds.Close()
		t.Fatal("fixture role/schema mismatch")
	}
	return ds
}

func (f *settingsVersionsFixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := f.ds.Pool().Exec(t.Context(), query, args...); err != nil {
		t.Fatal("own settings-version statement")
	}
}

func (f *settingsVersionsFixture) user(t *testing.T, id string) {
	t.Helper()
	f.exec(t, `INSERT INTO users(id,name,account_id) VALUES($1,$1,'settings_owner')`, id)
}

func versionsGeneration(t *testing.T, ds *DataStore, id string) int64 {
	t.Helper()
	var value int64
	if err := ds.Pool().QueryRow(t.Context(), `SELECT generation FROM user_settings_versions WHERE user_id=$1`, id).Scan(&value); err != nil {
		t.Fatal("read own lifecycle generation")
	}
	return value
}

func versionsRaw(t *testing.T, ds *DataStore, id string) ([]byte, string) {
	t.Helper()
	var raw []byte
	var version string
	err := ds.Pool().QueryRow(t.Context(), `SELECT settings,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM user_settings WHERE user_id=$1`, id).Scan(&raw, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ""
	}
	if err != nil {
		t.Fatal("read own raw settings row")
	}
	return raw, version
}

func versionsSnapshot(t *testing.T, ds *DataStore, id string) UserSettingsRecord {
	t.Helper()
	records, err := ds.LoadUserSettingsSnapshot(t.Context())
	if err != nil {
		t.Fatal("read actual settings snapshots")
	}
	for _, record := range records {
		if record.ID == id {
			return record
		}
	}
	t.Fatal("expected existing user snapshot absent")
	return UserSettingsRecord{}
}

func versionsConflict(t *testing.T, ds *DataStore, record UserSettingsRecord) {
	t.Helper()
	receipt, err := ds.CommitUserSettingsChanges(t.Context(), []UserSettingsChange{{Kind: UserSettingsCheck, Expected: record.Revision}})
	if receipt != nil {
		receipt.Close()
	}
	if !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("old snapshot survived actual direct or lifecycle write")
	}
}

func TestUserSettingsVersionsFull079UpgradeLegacyRepositoryAndRestart(t *testing.T) {
	f := newSettingsVersionsPG(t)
	// Downgrade only this owned schema, then seed pre-079 rows. No prepared
	// revision is retained across the deliberate loss of downgrade history.
	db := stdlib.OpenDBFromPool(f.ds.Pool())
	if err := goose.DownToContext(t.Context(), db, "migrations", 78); err != nil {
		t.Fatal("own migration 079 downgrade")
	}
	if err := db.Close(); err != nil {
		t.Fatal("close own migration adapter")
	}
	f.user(t, "legacy")
	initial := models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}, Metadata: models.MetadataSettings{PrimaryLanguage: "fra"}}
	if err := f.ds.UserSettings().Upsert(t.Context(), "legacy", &initial); err != nil {
		t.Fatal("legacy repository before version migration")
	}
	f.exec(t, `UPDATE user_settings SET settings=settings||'{"future":{"integer":9007199254740993}}'::jsonb WHERE user_id='legacy'`)
	before, _ := versionsRaw(t, f.ds, "legacy")
	f.ds.Close()
	f.ds = f.open(t)
	var applied bool
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM goose_db_version WHERE version_id=79 AND is_applied)`).Scan(&applied); err != nil || !applied {
		t.Fatal("full New did not apply real migration 079")
	}
	after, _ := versionsRaw(t, f.ds, "legacy")
	if !bytes.Equal(before, after) || versionsGeneration(t, f.ds, "legacy") != 0 {
		t.Fatal("079 backfill changed legacy data or initial generation")
	}
	original := versionsSnapshot(t, f.ds, "legacy")
	if original.Settings == nil || original.Settings.Display.SimpleMode == nil || *original.Settings.Display.SimpleMode {
		t.Fatal("explicit legacy false lost on upgrade")
	}
	updated := initial
	updated.Metadata.PrimaryLanguage = "deu"
	if err := f.ds.UserSettings().Upsert(t.Context(), "legacy", &updated); err != nil {
		t.Fatal("unchanged legacy repository after migration")
	}
	if versionsGeneration(t, f.ds, "legacy") <= 0 {
		t.Fatal("legacy Upsert bypassed version observation")
	}
	versionsConflict(t, f.ds, original)
	confirmed, version := versionsRaw(t, f.ds, "legacy")
	generation := versionsGeneration(t, f.ds, "legacy")
	f.ds.Close()
	f.ds = f.open(t)
	restarted, restartedVersion := versionsRaw(t, f.ds, "legacy")
	if !bytes.Equal(confirmed, restarted) || version != restartedVersion || generation != versionsGeneration(t, f.ds, "legacy") {
		t.Fatal("constructor restart rewrote data or reset generation")
	}
}

func TestUserSettingsVersionsCopyLifecycleABAAndSchemaIsolation(t *testing.T) {
	a, b := newSettingsVersionsPG(t), newSettingsVersionsPG(t)
	a.user(t, "same-id")
	b.user(t, "same-id")
	other := models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}}
	if err := b.ds.UserSettings().Upsert(t.Context(), "same-id", &other); err != nil {
		t.Fatal("second schema control")
	}
	otherRaw, otherVersion := versionsRaw(t, b.ds, "same-id")
	otherGeneration := versionsGeneration(t, b.ds, "same-id")
	absent := versionsSnapshot(t, a.ds, "same-id")
	originalGeneration := versionsGeneration(t, a.ds, "same-id")
	if absent.Settings != nil {
		t.Fatal("expected absent-settings snapshot")
	}
	// Real COPY with triggers enabled models a direct row import. This is not
	// a claim that an entire backup/restore orchestration was executed.
	raw := []byte(`{"display":{"simpleMode":false},"future":{"integer":9007199254740993}}`)
	if count, err := a.ds.Pool().CopyFrom(t.Context(), pgx.Identifier{"user_settings"}, []string{"user_id", "settings", "updated_at"}, pgx.CopyFromRows([][]any{{"same-id", raw, time.Now().UTC()}})); err != nil || count != 1 {
		t.Fatal("actual own-schema COPY row")
	}
	if versionsGeneration(t, a.ds, "same-id") <= originalGeneration {
		t.Fatal("direct COPY bypassed generation")
	}
	versionsConflict(t, a.ds, absent)
	present := versionsSnapshot(t, a.ds, "same-id")
	a.exec(t, `UPDATE user_settings SET settings=settings WHERE user_id='same-id'`)
	versionsConflict(t, a.ds, present)
	beforeDelete := versionsGeneration(t, a.ds, "same-id")
	if err := a.ds.Users().Delete(t.Context(), "same-id"); err != nil {
		t.Fatal("actual user cascade delete")
	}
	if current, _ := versionsRaw(t, a.ds, "same-id"); current != nil {
		t.Fatal("user cascade retained settings row")
	}
	if versionsGeneration(t, a.ds, "same-id") <= beforeDelete {
		t.Fatal("cascade removed or failed to advance tombstone")
	}
	a.user(t, "same-id")
	if versionsSnapshot(t, a.ds, "same-id").Settings != nil {
		t.Fatal("recreated user inherited deleted settings")
	}
	// Both old and new settings are absent; lifecycle generation, rather than
	// only a row/xmin comparison, must reject the original absent snapshot.
	versionsConflict(t, a.ds, absent)
	if current, currentVersion := versionsRaw(t, b.ds, "same-id"); !bytes.Equal(current, otherRaw) || currentVersion != otherVersion || versionsGeneration(t, b.ds, "same-id") != otherGeneration {
		t.Fatal("trigger affected another owned fixture schema")
	}
	var allowed bool
	if err := a.ds.Pool().QueryRow(t.Context(), `SELECT has_schema_privilege(current_user,$1,'USAGE')`, b.schema).Scan(&allowed); err != nil || allowed {
		t.Fatal("NOLOGIN role escaped its own schema")
	}
}

func TestUserSettingsVersionsIdenticalLegacyWritesAndOverflowRollback(t *testing.T) {
	f := newSettingsVersionsPG(t)
	f.user(t, "target")
	settings := models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}}
	if err := f.ds.UserSettings().Upsert(t.Context(), "target", &settings); err != nil {
		t.Fatal("initial legacy Upsert")
	}
	old := versionsSnapshot(t, f.ds, "target")
	generation := versionsGeneration(t, f.ds, "target")
	if err := f.ds.UserSettings().Upsert(t.Context(), "target", &settings); err != nil {
		t.Fatal("identical legacy Upsert")
	}
	if versionsGeneration(t, f.ds, "target") <= generation {
		t.Fatal("identical direct write was invisible")
	}
	versionsConflict(t, f.ds, old)
	f.exec(t, `UPDATE user_settings_versions SET generation=9223372036854775807 WHERE user_id='target'`)
	before, version := versionsRaw(t, f.ds, "target")
	fresh := versionsSnapshot(t, f.ds, "target")
	desired := *fresh.Settings
	desired.Metadata.PrimaryLanguage = "deu"
	receipt, err := f.ds.CommitUserSettingsChanges(t.Context(), []UserSettingsChange{{Kind: UserSettingsPatch, Expected: fresh.Revision, Settings: desired}})
	if receipt != nil {
		receipt.Close()
	}
	if err == nil {
		t.Fatal("generation overflow accepted mutation")
	}
	after, currentVersion := versionsRaw(t, f.ds, "target")
	if !bytes.Equal(before, after) || version != currentVersion || versionsGeneration(t, f.ds, "target") != 9223372036854775807 {
		t.Fatal("overflow left a partially committed settings write")
	}
	encoded, err := json.Marshal(fresh.Revision)
	if err != nil || string(encoded) != "null" {
		t.Fatal("opaque revision became an exportable storage token")
	}
}
