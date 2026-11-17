package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/models"
)

// Each fixture owns its schema and NOLOGIN role. The existing production
// constructor runs real schema/data migrations, never on the public schema.
type settingsDSFixture struct {
	ds          *DataStore
	admin       *pgxpool.Pool
	uri, schema string
}

func newSettingsDS(t *testing.T) *settingsDSFixture {
	t.Helper()
	raw := os.Getenv("USERS_PERSISTENCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_uc_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatalf("own role: %v", err)
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("own schema")
	}
	q := u.Query()
	q.Set("search_path", name+",pg_catalog")
	q.Set("role", name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	f := &settingsDSFixture{admin: admin, uri: u.String(), schema: name}
	t.Cleanup(func() {
		if f.ds != nil {
			f.ds.Close()
		}
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, e := admin.Exec(c, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
			t.Error("drop own schema")
		}
		if _, e := admin.Exec(c, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	f.ds = f.open(t)
	f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master) VALUES('master','Synthetic Master','synthetic unused hash',true),('default','Synthetic Default','synthetic unused hash',false),('other','Synthetic Other','synthetic unused hash',false)`)
	return f
}
func (f *settingsDSFixture) open(t *testing.T) *DataStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := New(ctx, f.uri, WithMaxConns(4), WithMinConns(1))
	if err != nil {
		t.Fatalf("own-schema datastore: %v", err)
	}
	var current, search string
	if err = ds.Pool().QueryRow(ctx, `SELECT current_user,current_schema()`).Scan(&current, &search); err != nil || current != f.schema || search != f.schema {
		ds.Close()
		t.Fatal("fixture scope not exact")
	}
	return ds
}
func (f *settingsDSFixture) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, e := f.ds.Pool().Exec(t.Context(), q, args...); e != nil {
		t.Fatalf("synthetic statement: %v", e)
	}
}
func (f *settingsDSFixture) user(t *testing.T, id string) {
	t.Helper()
	f.exec(t, `INSERT INTO users(id,name,account_id) VALUES($1,$1,'default')`, id)
}
func (f *settingsDSFixture) record(t *testing.T, id string) UserSettingsRecord {
	t.Helper()
	r, e := f.ds.LoadUserSettingsTarget(t.Context(), id)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func (f *settingsDSFixture) raw(t *testing.T, id string) string {
	t.Helper()
	var raw string
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT settings::text FROM user_settings WHERE user_id=$1`, id).Scan(&raw); e != nil {
		t.Fatal("read own raw settings")
	}
	return raw
}
func (f *settingsDSFixture) generation(t *testing.T, id string) int64 {
	t.Helper()
	var n int64
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT generation FROM user_settings_versions WHERE user_id=$1`, id).Scan(&n); e != nil {
		t.Fatal("read own version")
	}
	return n
}
func settingsDesired(value bool) models.UserSettings {
	v := models.UserSettings{}
	v.Display.SimpleMode = models.BoolPtr(value)
	return v
}
func commitSettingsDS(t *testing.T, ds *DataStore, r UserSettingsRecord, kind UserSettingsChangeKind, v models.UserSettings) UserSettingsRecord {
	t.Helper()
	ctx := t.Context()
	receipt, e := ds.CommitUserSettingsChanges(ctx, []UserSettingsChange{{Kind: kind, Expected: r.Revision, Settings: v}})
	if e != nil {
		t.Fatal(e)
	}
	defer receipt.Close()
	records, e := ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{r.Revision})
	if e != nil || len(records) != 1 {
		t.Fatal("canonical receipt not accepted", e)
	}
	return records[0]
}

func TestUserSettingsCASDatastorePreservesUnknownAndPreciseNumbers(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "a")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"future":{"integer":9007199254740993,"fraction":0.12345678901234567890123456789},"display":{"future":{"on":true},"simpleMode":false}}')`)
	r := f.record(t, "a")
	v := *r.Settings
	v.Display.SimpleMode = models.BoolPtr(true)
	next := commitSettingsDS(t, f.ds, r, UserSettingsPatch, v)
	if next.Settings == nil || !*next.Settings.Display.SimpleMode {
		t.Fatal("canonical update missing")
	}
	var n, decimal string
	var preserved bool
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT settings#>>'{future,integer}',settings#>>'{future,fraction}',(settings#>>'{display,future,on}')::boolean FROM user_settings WHERE user_id='a'`).Scan(&n, &decimal, &preserved); e != nil || n != "9007199254740993" || decimal != "0.12345678901234567890123456789" || !preserved {
		t.Fatal("unrelated JSON value changed")
	}
}

func TestUserSettingsCASDatastoreUnknownArrayConflict(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"homeShelves":{"shelves":[{"id":"synthetic","name":"Original","future":{"n":9007199254740993}}]}}')`)
			r := f.record(t, "a")
			v := *r.Settings
			v.Display.SimpleMode = models.BoolPtr(true)
			if change {
				v.HomeShelves.Shelves[0].Name = "Changed"
			}
			receipt, e := f.ds.CommitUserSettingsChanges(t.Context(), []UserSettingsChange{{Kind: UserSettingsPatch, Expected: r.Revision, Settings: v}})
			if change {
				if !errors.Is(e, ErrUserSettingsUnsupported) || receipt != nil {
					t.Fatal("unknown array data would be dropped", e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				receipt.Close()
				if !strings.Contains(f.raw(t, "a"), "9007199254740993") {
					t.Fatal("unchanged unknown array lost")
				}
			}
		})
	}
}

func TestUserSettingsCASDatastoreRejectsUnsupportedRoot(t *testing.T) {
	for _, raw := range []string{"null", "[]", "true", `{"display":{"simpleMode":"wrong"}}`} {
		t.Run(raw, func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a',$1::jsonb)`, raw)
			if _, e := f.ds.LoadUserSettingsTarget(t.Context(), "a"); !errors.Is(e, ErrUserSettingsUnsupported) {
				t.Fatal("malformed projection was accepted", e)
			}
		})
	}
}

func TestUserSettingsCASDatastoreSnapshotAndLifecycleABA(t *testing.T) {
	for _, action := range []string{"settings-insert-delete", "settings-row-aba", "user-recreate-without-settings", "rename-control"} {
		t.Run(action, func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			if action == "settings-row-aba" {
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}')`)
			}
			r := f.record(t, "a")
			switch action {
			case "settings-insert-delete":
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}');DELETE FROM user_settings WHERE user_id='a'`)
			case "settings-row-aba":
				f.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":true}}' WHERE user_id='a';UPDATE user_settings SET settings='{}' WHERE user_id='a'`)
			case "user-recreate-without-settings":
				f.exec(t, `DELETE FROM users WHERE id='a'`)
				f.user(t, "a")
			case "rename-control":
				f.exec(t, `UPDATE users SET name='Renamed' WHERE id='a'`)
			}
			receipt, e := f.ds.CommitUserSettingsChanges(t.Context(), []UserSettingsChange{{Kind: UserSettingsPatch, Expected: r.Revision, Settings: settingsDesired(false)}})
			if action == "rename-control" {
				if e != nil {
					t.Fatal(e)
				}
				receipt.Close()
			} else if !errors.Is(e, ErrUserSettingsConflict) || receipt != nil {
				t.Fatal("old snapshot was revived", e)
			}
		})
	}
}

func TestUserSettingsCASDatastoreExplicitDeleteAndNoop(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "a")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"future":42,"display":{"simpleMode":false}}')`)
	r := f.record(t, "a")
	version, stamp := r.Revision.snapshot.version, f.generation(t, "a")
	next := commitSettingsDS(t, f.ds, r, UserSettingsCheck, models.UserSettings{})
	if next.Revision.snapshot.version != version || f.generation(t, "a") != stamp {
		t.Fatal("checked noop changed settings or stamp")
	}
	next = commitSettingsDS(t, f.ds, next, UserSettingsPatch, models.UserSettings{})
	if !strings.Contains(f.raw(t, "a"), "42") {
		t.Fatal("empty typed projection erased unknown data")
	}
	next = commitSettingsDS(t, f.ds, next, UserSettingsDelete, models.UserSettings{})
	if next.Settings != nil || next.Revision.snapshot.version != "" || f.generation(t, "a") <= stamp {
		t.Fatal("deliberate delete lost its tombstone")
	}
}

func TestUserSettingsCASDatastoreProofAndReceiptZeroIO(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "a")
	cfg := f.ds.pool.Config()
	trace := &unlockQueryTrace{}
	cfg.ConnConfig.Tracer = trace
	pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	ds := &DataStore{pool: pool}
	r, e := ds.LoadUserSettingsTarget(t.Context(), "a")
	if e != nil {
		t.Fatal(e)
	}
	before := trace.calls.Load()
	for _, change := range []UserSettingsChange{{Kind: UserSettingsPatch}, {Kind: 99, Expected: r.Revision}, {Kind: UserSettingsPatch, Expected: f.record(t, "a").Revision}} {
		if receipt, e := ds.CommitUserSettingsChanges(t.Context(), []UserSettingsChange{change}); !errors.Is(e, ErrUserSettingsConflict) || receipt != nil {
			t.Fatal("invalid revision accepted", e)
		}
	}
	if trace.calls.Load() != before {
		t.Fatal("invalid proof reached SQL")
	}
	ctx := t.Context()
	receipt, e := ds.CommitUserSettingsChanges(ctx, []UserSettingsChange{{Kind: UserSettingsPatch, Expected: r.Revision, Settings: settingsDesired(false)}})
	if e != nil {
		t.Fatal(e)
	}
	before = trace.calls.Load()
	copy := new(UserSettingsCommit)
	reflect.ValueOf(copy).Elem().Set(reflect.ValueOf(receipt).Elem())
	for _, candidate := range []*UserSettingsCommit{nil, {}, copy} {
		if _, e := ds.AcceptUserSettingsCommit(ctx, candidate, []UserSettingsRevision{r.Revision}); !errors.Is(e, ErrUserSettingsConflict) {
			t.Fatal("copied or zero receipt accepted")
		}
	}
	if _, e := f.ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{r.Revision}); !errors.Is(e, ErrUserSettingsConflict) {
		t.Fatal("wrong store adopted receipt")
	}
	if _, e := ds.AcceptUserSettingsCommit(context.Background(), receipt, []UserSettingsRevision{r.Revision}); !errors.Is(e, ErrUserSettingsConflict) {
		t.Fatal("replacement context adopted receipt")
	}
	records, e := ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{r.Revision})
	if e != nil || len(records) != 1 {
		t.Fatal(e)
	}
	if _, e := ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{r.Revision}); !errors.Is(e, ErrUserSettingsConflict) {
		t.Fatal("receipt replay")
	}
	if trace.calls.Load() != before {
		t.Fatal("receipt use reached SQL")
	}
	encoded, _ := json.Marshal(receipt)
	if string(encoded) != "null" || strings.Contains(fmt.Sprintf("%+v", r.Revision), "simpleMode") {
		t.Fatal("opaque persistence state leaked")
	}
}

func (f *settingsDSFixture) waitBlocked(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if e := f.admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%user_settings%')`, f.schema).Scan(&blocked); e != nil {
			t.Fatal("observe own blocked settings statement")
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("own settings operation did not block")
		case <-ticker.C:
		}
	}
}

func TestUserSettingsCASDatastoreConcurrentLockAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}')`)
			r := f.record(t, "a")
			lock, e := f.ds.pool.Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			defer userWriteRollback(lock)
			if _, e = lock.Exec(t.Context(), `UPDATE user_settings SET settings='{"future":1}' WHERE user_id='a'`); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				receipt, e := f.ds.CommitUserSettingsChanges(ctx, []UserSettingsChange{{Kind: UserSettingsPatch, Expected: r.Revision, Settings: settingsDesired(true)}})
				if receipt != nil {
					receipt.Close()
				}
				done <- e
			}()
			t.Cleanup(func() {
				cancel()
				userWriteRollback(lock)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("settings worker did not end")
				}
			})
			f.waitBlocked(t)
			if cancelled {
				cancel()
			} else {
				if e = lock.Commit(t.Context()); e != nil {
					t.Fatal(e)
				}
			}
			select {
			case e = <-done:
				done <- e
			case <-time.After(5 * time.Second):
				t.Fatal("settings worker exceeded bounded wait")
			}
			if cancelled {
				if !errors.Is(e, context.Canceled) {
					t.Fatal("cancel not preserved", e)
				}
			} else if !errors.Is(e, ErrUserSettingsConflict) {
				t.Fatal("concurrent write was not fenced", e)
			}
		})
	}
}
