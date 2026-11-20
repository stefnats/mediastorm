//go:build linux || darwin || windows

package datastore

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
	"novastream/internal/settingscommit"
)

type settingsStoreFixture struct {
	source          *SettingsStartupStore
	admin, observer *pgxpool.Pool
	objects         *settingscommit.LockObjects
	raw, name       string
}

func settingsStorePG(t *testing.T) *settingsStoreFixture {
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
	name := fmt.Sprintf("synthetic_settings_store_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own fixture database")
	}
	f := &settingsStoreFixture{admin: admin, name: name}
	t.Cleanup(func() {
		if f.source != nil {
			f.source.Close()
		}
		if f.objects != nil {
			f.objects.Close()
		}
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
	f.raw = u.String()
	f.observer, err = pgxpool.New(t.Context(), f.raw)
	if err != nil {
		t.Fatal(err)
	}
	f.objects, err = settingscommit.OpenLockObjects(t.Context(), filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	f.source, err = OpenSettingsStartupStore(t.Context(), f.raw, f.objects, WithMaxConns(1), WithMinConns(1))
	if err != nil {
		t.Fatal("open genuine startup store", err)
	}
	return f
}
func (f *settingsStoreFixture) guard(t *testing.T) *settingscommit.WriteGuard {
	t.Helper()
	g, err := f.source.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.source.FinishMutations(); _ = g.Close() })
	return g
}
func settingsStoreWait(t *testing.T, check func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual original SQL lifetime witness did not arrive")
}
func TestSettingsStartupSchemaJSONAndDataMigrationsUseOneOriginalSession(t *testing.T) {
	f := settingsStorePG(t)
	if _, err := f.observer.Exec(t.Context(), `CREATE TABLE settings_schema_witness(backend int,tag text,query text);CREATE FUNCTION settings_capture_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO settings_schema_witness VALUES(pg_backend_pid(),tg_tag,current_query());END$$;CREATE EVENT TRIGGER settings_observe_ddl ON ddl_command_end EXECUTE FUNCTION settings_capture_ddl();TRUNCATE settings_schema_witness`); err != nil {
		t.Fatal("create own actual DDL observer", err)
	}
	g := f.guard(t)
	runtime, err := f.source.Migrate(t.Context(), g)
	if err != nil {
		t.Fatal("run actual Goose including NO TRANSACTION migration", err)
	}
	if runtime != f.source.store || runtime.pool.Config().MaxConns != 1 || f.source.mutations.pool == runtime.pool {
		t.Fatal("startup silently replaced original runtime datastore")
	}
	var mutationPID uint32
	if f.source.mutations.pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&mutationPID) != nil {
		t.Fatal("read actual original session")
	}
	var count int
	var only uint32
	var concurrentIndex bool
	if f.observer.QueryRow(t.Context(), `SELECT count(DISTINCT backend),min(backend),bool_or(query ILIKE '%CREATE INDEX CONCURRENTLY%') FROM settings_schema_witness`).Scan(&count, &only, &concurrentIndex) != nil || count != 1 || only != mutationPID || !concurrentIndex {
		t.Fatal("actual schema/NO-TX writes escaped the one original backend")
	}
	if _, err = f.source.mutations.pool.Exec(t.Context(), `CREATE TABLE settings_json_witness(backend int);CREATE FUNCTION settings_capture_account() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO settings_json_witness VALUES(pg_backend_pid());RETURN NEW;END$$;CREATE TRIGGER settings_account_write AFTER INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION settings_capture_account()`); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	if os.WriteFile(filepath.Join(cache, "accounts.json"), []byte(`[{"id":"synthetic-one","username":"Synthetic JSON","passwordHash":"synthetic-not-a-password"}]`), 0600) != nil {
		t.Fatal("write own JSON fixture")
	}
	if err = f.source.MigrateFromJSON(t.Context(), g, cache); err != nil {
		t.Fatal("actual serial JSON import/marker rerun starved pool1", err)
	}
	if err = f.source.RunDataMigrations(t.Context(), g); err != nil {
		t.Fatal("actual post-import recheck failed", err)
	}
	if f.observer.QueryRow(t.Context(), `SELECT count(*),min(backend) FROM settings_json_witness`).Scan(&count, &only) != nil || count != 1 || only != mutationPID {
		t.Fatal("JSON transaction escaped original guarded backend")
	}
	var runtimePID uint32
	if runtime.pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&runtimePID) != nil || runtimePID == mutationPID {
		t.Fatal("normal runtime pool1 did not remain independently usable")
	}
	f.source.FinishMutations()
	g.Close()
	if runtime.pool.Ping(t.Context()) != nil {
		t.Fatal("finishing temporary mutation scope closed runtime pool")
	}
}
func TestSettingsProgressLockIsInsideActualOriginalTransaction(t *testing.T) {
	f := settingsStorePG(t)
	tx, err := f.source.store.beginSettingsProgressTx(t.Context(), f.source.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	var pid uint32
	var xid uint64
	if tx.tx.QueryRow(t.Context(), `SELECT pg_backend_pid(),txid_current()`).Scan(&pid, &xid) != nil || xid == 0 {
		t.Fatal("actual shared writer transaction missing")
	}
	var owns bool
	if f.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND classid=$2::oid AND objid=$3::oid AND objsubid=2 AND mode='ShareLock' AND granted)`, pid, settingscommit.Namespace, settingscommit.Resource).Scan(&owns) != nil || !owns {
		t.Fatal("lock was not owned by actual original TX backend")
	}
	if guard, err := f.source.coordinator.TryWrite(t.Context()); !errors.Is(err, settingscommit.ErrBusy) {
		if guard != nil {
			guard.Close()
		}
		t.Fatal("actual progress TX did not exclude settings mutation")
	}
	tx.Close()
	g := f.guard(t)
	if g.Current(f.source.coordinator) != nil {
		t.Fatal("ended actual writer prevented next mutation")
	}
	foreign := &DataStore{pool: f.observer}
	if foreign.SettingsCommitReady(f.source.coordinator) {
		t.Fatal("different concrete pool gained original coordinator")
	}
}
func TestSettingsRestoreConcurrentCloseJoinsActualRepositoryWork(t *testing.T) {
	f := settingsStorePG(t)
	startup := f.guard(t)
	store, err := f.source.Migrate(t.Context(), startup)
	if err != nil {
		t.Fatal(err)
	}
	f.source.FinishMutations()
	startup.Close()
	guard := f.guard(t)
	restore, err := store.BeginSettingsRestore(t.Context(), guard)
	if err != nil {
		t.Fatal(err)
	}
	tx, finish, err := restore.Repositories()
	if err != nil {
		restore.Close()
		t.Fatal(err)
	}
	release := make(chan struct{})
	queryDone := make(chan struct{})
	closeDone := make(chan struct{}, 2)
	released := false
	unblock := func() {
		if !released {
			released = true
			close(release)
		}
	}
	launched := false
	t.Cleanup(func() {
		unblock()
		restore.cancel()
		if launched {
			select {
			case <-queryDone:
			case <-time.After(8 * time.Second):
				t.Error("actual repository query did not join during cleanup")
				return
			}
		}
		finish()
		restore.Close()
	})
	lock, err := f.observer.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(t.Context(), `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	var pid uint32
	if restore.connection.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid) != nil {
		t.Fatal("actual restore backend absent")
	}
	launched = true
	go func() { _, _ = tx.Accounts().List(restore.Context()); close(queryDone); <-release; finish() }()
	settingsStoreWait(t, func() bool {
		var waiting bool
		_ = f.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND query ILIKE '%accounts%')`, pid).Scan(&waiting)
		return waiting
	})
	for i := 0; i < 2; i++ {
		go func() { _ = restore.Close(); closeDone <- struct{}{} }()
	}
	select {
	case <-queryDone:
	case <-time.After(8 * time.Second):
		t.Fatal("actual held query did not return after original context cancellation")
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before original repository scope finished")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	for i := 0; i < 2; i++ {
		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close did not join same end")
		}
	}
	// The lock is still held: old SQL cannot be silently executed after cleanup.
	var pending bool
	if f.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&pending) != nil || pending {
		t.Fatal("restore cleanup returned with original SQL still pending")
	}
	guard.Close()
}
func TestSettingsStartupStoreAllCloseCallersJoinActualGuard(t *testing.T) {
	f := settingsStorePG(t)
	g := f.guard(t)
	done := make(chan struct{}, 2)
	go func() { f.source.Close(); done <- struct{}{} }()
	settingsStoreWait(t, func() bool { f.source.mu.Lock(); defer f.source.mu.Unlock(); return f.source.closed })
	go func() { f.source.Close(); done <- struct{}{} }()
	select {
	case <-done:
		t.Fatal("one Close returned before actual guarded backend ended")
	case <-time.After(30 * time.Millisecond):
	}
	g.Close()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("source Close did not join same actual lifetime")
		}
	}
}
