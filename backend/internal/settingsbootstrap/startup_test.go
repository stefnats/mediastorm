//go:build linux || darwin || windows

package settingsbootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/settingscommit"
)

type bootstrapPGFixture struct {
	admin, observer *pgxpool.Pool
	raw, name       string
	manager         *config.Manager
}

func bootstrapPG(t *testing.T) *bootstrapPGFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("open fixture admin")
	}
	name := fmt.Sprintf("synthetic_settings_boot_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own fixture database")
	}
	f := &bootstrapPGFixture{admin: admin, name: name, manager: config.NewManager(filepath.Join(t.TempDir(), "settings.json"))}
	t.Cleanup(func() {
		if f.observer != nil {
			f.observer.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
		admin.Close()
	})
	u.Path = "/" + name
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	f.raw = u.String()
	f.observer, err = pgxpool.New(t.Context(), f.raw)
	if err != nil {
		t.Fatal(err)
	}
	if f.observer.Ping(t.Context()) != nil {
		t.Fatal("open actual independent observer")
	}
	return f
}
func TestSettingsBootstrapGuardsTrueFirstDefaultsAndSchemaWrites(t *testing.T) {
	f := bootstrapPG(t)
	tx, err := f.observer.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked) != nil || !locked {
		t.Fatal("hold actual original progress transaction")
	}
	if startup, err := Begin(t.Context(), f.manager, f.raw); !errors.Is(err, settingscommit.ErrBusy) {
		if startup != nil {
			startup.Close()
		}
		t.Fatal("first startup was not excluded before default/schema writes", err)
	}
	if _, err = os.Stat(f.manager.ConfigPath()); !os.IsNotExist(err) {
		t.Fatal("Busy first startup wrote default settings")
	}
	var table bool
	if f.observer.QueryRow(t.Context(), `SELECT to_regclass('accounts') IS NOT NULL OR to_regclass('goose_db_version') IS NOT NULL`).Scan(&table) != nil || table {
		t.Fatal("Busy first startup created schema")
	}
	tx.Rollback(t.Context())
	startup, err := Begin(t.Context(), f.manager, f.raw)
	if err != nil {
		t.Fatal("genuine first bootstrap unavailable", err)
	}
	defer startup.Close()
	if _, err = os.Stat(f.manager.ConfigPath()); !os.IsNotExist(err) {
		t.Fatal("Begin itself performed hidden Load/default writes")
	}
	settings, err := startup.Load()
	if err != nil {
		t.Fatal("true default Load under original guard", err)
	}
	store, err := startup.Migrate(t.Context())
	if err != nil {
		t.Fatal("actual guarded schema startup", err)
	}
	if err = startup.MigrateFromJSON(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err = startup.RunDataMigrations(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = startup.Finish(); err != nil {
		t.Fatal("finish actual startup resources", err)
	}
	if err = f.manager.Save(settings); err != nil {
		t.Fatal("normal managed Save stopped working after startup", err)
	}
	if store.Pool().Ping(t.Context()) != nil {
		t.Fatal("temporary startup finish closed actual runtime datastore")
	}
	if _, err = startup.Migrate(t.Context()); err == nil {
		t.Fatal("finished startup admitted more migration work")
	}
}
func TestSettingsBootstrapRechecksOriginalSelectionBeforeArming(t *testing.T) {
	for _, change := range []string{"database", "options"} {
		t.Run(change, func(t *testing.T) {
			f := bootstrapPG(t)
			settings := config.DefaultSettings()
			settings.Database.URL = f.raw
			settings.Database.MaxOpenConns = 1
			settings.Database.MaxIdleConns = 1
			if err := f.manager.Save(settings); err != nil {
				t.Fatal(err)
			}
			reservation, err := config.BeginManagedReplacement(f.manager)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			started := false
			defer func() {
				reservation.Close()
				if started {
					select {
					case <-done:
					case <-time.After(8 * time.Second):
						t.Error("bootstrap did not join original native waiter")
					}
				}
			}()
			started = true
			go func() {
				startup, err := Begin(t.Context(), f.manager, "")
				if startup != nil {
					startup.Close()
				}
				done <- err
			}()
			deadline := time.Now().Add(5 * time.Second)
			witness := false
			for time.Now().Before(deadline) {
				var n int
				_ = f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, f.name).Scan(&n)
				if n >= 2 {
					witness = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !witness {
				t.Fatal("actual preliminary read/runtime Ping did not finish before held native turn")
			}
			select {
			case err := <-done:
				done <- err
				t.Fatal("startup skipped actual pre-arming mutation turn")
			default:
			}
			if change == "database" {
				settings.Database.URL = "postgresql://synthetic_changed@/different?host=/synthetic"
			} else {
				settings.Database.MaxOpenConns = 2
			}
			if err = reservation.Save(f.manager, settings); err != nil {
				t.Fatal("genuine held unarmed Save failed", err)
			}
			reservation.Close()
			select {
			case err := <-done:
				started = false
				if !errors.Is(err, settingscommit.ErrDomain) {
					t.Fatal("pre-arm source/options drift was adopted", err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("drift check did not return")
			}
			if err = f.manager.Save(settings); err != nil {
				t.Fatal("failed source correlation incorrectly armed original root", err)
			}
		})
	}
}
func TestSettingsBootstrapRecoveryTrimAndEffectiveOverrideRemainDistinct(t *testing.T) {
	f := bootstrapPG(t)
	settings := config.DefaultSettings()
	settings.Database.URL = "postgresql://synthetic_invalid@/unused?host=/nonexistent"
	if err := f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	if startupSelection(settings, " "+f.raw+" ", false).url != " "+f.raw+" " || startupSelection(settings, " "+f.raw+" ", true).url != f.raw {
		t.Fatal("Main raw selection was silently changed to recovery trimming")
	}
	s, err := BeginRecovery(t.Context(), f.manager, " "+f.raw+" ")
	if err != nil {
		t.Fatal("existing recovery trimmed effective override failed", err)
	}
	defer s.Close()
	if _, err = s.Load(); err != nil {
		t.Fatal(err)
	}
	store, err := s.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if store.Pool().Config().ConnConfig.Database != f.name {
		t.Fatal("configured data overrode original effective environment source")
	}
	if err = s.Finish(); err != nil {
		t.Fatal(err)
	}
}
