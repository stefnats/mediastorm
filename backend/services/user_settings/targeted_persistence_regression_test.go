package user_settings

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

// This fixture uses only the established datastore and settings public APIs so
// the identical regression can run against the unmodified parent revision.
func targetedRegressionStore(t *testing.T) *datastore.DataStore {
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
		t.Fatal("open synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_settings_reg_%d", time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE ROLE "+ident+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own fixture role")
	}
	var ds *datastore.DataStore
	t.Cleanup(func() {
		if ds != nil {
			ds.Close()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, e := admin.Exec(cleanup, "DROP SCHEMA IF EXISTS "+ident+" CASCADE"); e != nil {
			t.Error("drop own fixture schema")
		}
		if _, e := admin.Exec(cleanup, "DROP ROLE "+ident); e != nil {
			t.Error("drop own fixture role")
		}
		admin.Close()
	})
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+ident+" AUTHORIZATION "+ident); err != nil {
		t.Fatal("create own fixture schema")
	}
	q := u.Query()
	q.Set("search_path", name+",pg_catalog")
	q.Set("role", name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	ds, err = datastore.New(ctx, u.String(), datastore.WithMaxConns(4), datastore.WithMinConns(1))
	if err != nil {
		t.Fatalf("migrate own fixture: %v", err)
	}
	var role, schema string
	if err = ds.Pool().QueryRow(ctx, "SELECT current_user,current_schema()").Scan(&role, &schema); err != nil || role != name || schema != name {
		t.Fatal("fixture identity mismatch")
	}
	if _, err = ds.Pool().Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('synthetic-owner','Synthetic Settings Regression','unused synthetic hash'); INSERT INTO users(id,account_id,name) VALUES('target','synthetic-owner','Target'),('other','synthetic-owner','Other')`); err != nil {
		t.Fatal("seed synthetic profiles")
	}
	return ds
}

func targetedRegressionService(t *testing.T, ds *datastore.DataStore) *Service {
	t.Helper()
	s, err := NewServiceWithStore(ds)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func targetedRegressionSettings(simple bool) models.UserSettings {
	var s models.UserSettings
	s.Display.SimpleMode = models.BoolPtr(simple)
	return s
}

func TestUserSettingsTargetedPersistenceRegression(t *testing.T) {
	t.Run("unrelated_row_inserted_by_other_service_survives", func(t *testing.T) {
		ds := targetedRegressionStore(t)
		a := targetedRegressionService(t, ds)
		b := targetedRegressionService(t, ds)
		if err := b.Update("other", targetedRegressionSettings(false)); err != nil {
			t.Fatal(err)
		}
		var before string
		if err := ds.Pool().QueryRow(t.Context(), `SELECT concat(xmin::text,':',ctid::text) FROM user_settings WHERE user_id='other'`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if err := a.Update("target", targetedRegressionSettings(true)); err != nil {
			t.Fatal(err)
		}
		var after string
		if err := ds.Pool().QueryRow(t.Context(), `SELECT concat(xmin::text,':',ctid::text) FROM user_settings WHERE user_id='other'`).Scan(&after); err != nil {
			t.Fatal("unrelated settings row was deleted")
		}
		if before != after {
			t.Fatal("unrelated settings row was rewritten")
		}
	})
	t.Run("missing_target_changed_to_explicit_false_is_not_defaulted", func(t *testing.T) {
		ds := targetedRegressionStore(t)
		a := targetedRegressionService(t, ds)
		b := targetedRegressionService(t, ds)
		if err := b.Update("target", targetedRegressionSettings(false)); err != nil {
			t.Fatal(err)
		}
		changed, err := a.ApplyKidsProfileDefaults("target")
		if err == nil || changed != 0 {
			t.Error("stale default must conflict without reporting a change")
		}
		var actual bool
		if err := ds.Pool().QueryRow(t.Context(), `SELECT (settings #>> '{display,simpleMode}')::boolean FROM user_settings WHERE user_id='target'`).Scan(&actual); err != nil || actual {
			t.Error("newer explicit false preference was overwritten")
		}
	})
	t.Run("cached_explicit_default_noop_rechecks_revision", func(t *testing.T) {
		ds := targetedRegressionStore(t)
		seed := targetedRegressionService(t, ds)
		if err := seed.Update("target", targetedRegressionSettings(false)); err != nil {
			t.Fatal(err)
		}
		a := targetedRegressionService(t, ds)
		b := targetedRegressionService(t, ds)
		if err := b.Update("target", targetedRegressionSettings(true)); err != nil {
			t.Fatal(err)
		}
		if changed, err := a.ApplyKidsProfileDefaults("target"); err == nil || changed != 0 {
			t.Fatal("cached no-op accepted a stale revision")
		}
	})
	t.Run("unknown_nested_fields_and_large_integer_survive_patch", func(t *testing.T) {
		ds := targetedRegressionStore(t)
		if _, err := ds.Pool().Exec(t.Context(), `INSERT INTO user_settings(user_id,settings) VALUES('target','{"display":{"simpleMode":false,"futureDisplay":{"precise":9007199254740993}},"futureRoot":{"precise":9007199254740993}}'::jsonb)`); err != nil {
			t.Fatal(err)
		}
		a := targetedRegressionService(t, ds)
		if err := a.UpdateSkipping("target", SkipPreferences{Intro: models.BoolPtr(true)}); err != nil {
			t.Fatal(err)
		}
		var stored []byte
		if err := ds.Pool().QueryRow(t.Context(), `SELECT settings FROM user_settings WHERE user_id='target'`).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(stored, &root); err != nil {
			t.Fatal(err)
		}
		var display map[string]json.RawMessage
		if err := json.Unmarshal(root["display"], &display); err != nil {
			t.Fatal(err)
		}
		for _, raw := range []json.RawMessage{root["futureRoot"], display["futureDisplay"]} {
			var future map[string]json.RawMessage
			if err := json.Unmarshal(raw, &future); err != nil || string(future["precise"]) != "9007199254740993" {
				t.Error("unknown object or exact large integer was lost")
			}
		}
	})
	t.Run("new_profile_after_service_construction_can_save", func(t *testing.T) {
		ds := targetedRegressionStore(t)
		a := targetedRegressionService(t, ds)
		if _, err := ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name) VALUES('new-target','synthetic-owner','New Target')`); err != nil {
			t.Fatal(err)
		}
		if err := a.UpdateSkipping("new-target", SkipPreferences{Intro: models.BoolPtr(true)}); err != nil {
			t.Fatal("newly created profile cannot save its first preference")
		}
		actual, err := ds.UserSettings().Get(t.Context(), "new-target")
		if err != nil || actual == nil || actual.Playback.IntroAutoSkip == nil || !*actual.Playback.IntroAutoSkip {
			t.Fatal("first preference was not persisted")
		}
	})
}
