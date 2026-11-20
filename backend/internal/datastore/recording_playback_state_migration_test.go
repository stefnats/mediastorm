package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"novastream/models"
)

type recordingStatePGFixture struct {
	ds     *DataStore
	admin  *pgxpool.Pool
	schema string
}

// Only an explicitly configured own synthetic Unix-socket fixture is accepted.
// Every case uses a fresh private role/schema and the real embedded migrations.
func recordingStatePG(t *testing.T, version int64) *recordingStatePGFixture {
	t.Helper()
	raw := os.Getenv("RECORDING_CAPTURE_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("a synthetic Unix-socket fixture is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("open fixture observer")
	}
	name := "synthetic_recording_state_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE ROLE "+quoted+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own fixture role")
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted+" AUTHORIZATION "+quoted); err != nil {
		_, _ = admin.Exec(ctx, "DROP ROLE "+quoted)
		admin.Close()
		t.Fatal("create own fixture schema")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("parse own fixture")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["search_path"] = quoted + ",pg_catalog"
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	configureQueryLifecycle(cfg.ConnConfig)
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET ROLE "+quoted)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE")
		_, _ = admin.Exec(cleanup, "DROP ROLE "+quoted)
		admin.Close()
		t.Fatal("open scoped fixture")
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error("remove own fixture schema")
		}
		if _, err := admin.Exec(cleanup, "DROP ROLE "+quoted); err != nil {
			t.Error("remove own fixture role")
		}
		admin.Close()
	})
	f := &recordingStatePGFixture{ds: &DataStore{pool: pool}, admin: admin, schema: name}
	f.migrate(t, version, false)
	return f
}

func (f *recordingStatePGFixture) migrate(t *testing.T, version int64, down bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db := stdlib.OpenDBFromPool(f.ds.pool)
	defer db.Close()
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	var err error
	if down {
		err = goose.DownToContext(ctx, db, "migrations", version)
	} else {
		err = goose.UpToContext(ctx, db, "migrations", version)
	}
	if err != nil {
		t.Fatalf("actual migration target %d failed: %v", version, err)
	}
	got, err := goose.GetDBVersionContext(ctx, db)
	if err != nil || got != version {
		t.Fatal("actual schema version differs")
	}
}

func (f *recordingStatePGFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.ds.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("synthetic statement failed: %v", err)
	}
}

func (f *recordingStatePGFixture) profiles(t *testing.T) {
	t.Helper()
	f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES('account','Synthetic DVR','synthetic');
 INSERT INTO users(id,account_id,name)VALUES('one','account','Synthetic One'),('two','account','Synthetic Two')`)
}

func (f *recordingStatePGFixture) create(t *testing.T, id string) *models.Recording {
	t.Helper()
	now := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
	end := now.Add(time.Minute)
	r := &models.Recording{ID: id, UserID: "one", Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusCompleted, Title: "Synthetic Recording", StartAt: now, EndAt: end, CreatedAt: now, UpdatedAt: now, Capture: &models.RecordingCapture{Version: 1, Phase: "completed", RunID: "run-A", Parts: []models.RecordingPart{{Index: 1, StartedAt: now, EndedAt: &end, Bytes: 32, EndReason: "completed", StreamPath: "recording:synthetic/parts/1"}}, Gaps: []models.RecordingGap{}}}
	if err := f.ds.Recordings().Create(t.Context(), r); err != nil {
		t.Fatal("generic recording create failed", err)
	}
	return r
}

func TestRecordingPlaybackStateMigrationPreservesLegacyAndUnknownOwners(t *testing.T) {
	f := recordingStatePG(t, 83)
	f.profiles(t)
	f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('missing-account-profile','absent-account','Synthetic Unassigned');
 INSERT INTO recordings(id,user_id,type,title,start_at,end_at,created_at)VALUES
 ('legacy','one','time_block','Legacy',now(),now(),now()),
 ('unassigned','missing-account-profile','time_block','Unassigned',now(),now(),now());
 ALTER TABLE recordings DROP CONSTRAINT recordings_user_id_fkey;
 INSERT INTO recordings(id,user_id,type,title,start_at,end_at,capture_state)VALUES
 ('historical-orphan','unknown-user','time_block','Historical',now(),now(),'{"runId":{"legacy":true},"unknown":4}');`)
	var before, after []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM recordings r`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	f.migrate(t, 84, false)
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM recordings r`).Scan(&after); err != nil || !bytes.Equal(before, after) {
		t.Fatal("backfill rewrote legacy recording data")
	}
	for _, id := range []string{"legacy", "unassigned", "historical-orphan"} {
		state := f.state(t, id)
		if state.incarnation == "" || state.revision != 0 || state.generation != 0 || !state.tombstone || state.operationID != nil {
			t.Fatal("legacy bookkeeping invented a writer or progress")
		}
	}
	if f.state(t, "historical-orphan").ownerID != "unknown-user" || f.state(t, "unassigned").ownerID != "missing-account-profile" {
		t.Fatal("backfill adopted or reassigned a legacy owner")
	}
}

func TestRecordingPlaybackStateMigrationKeepsOriginalForeignKey(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "known")
	r := f.create(t, "template")
	r.ID, r.UserID = "unknown", "not-present"
	var pgerr *pgconn.PgError
	if err := f.ds.Recordings().Create(t.Context(), r); !errors.As(err, &pgerr) || pgerr.Code != "23503" {
		t.Fatal("existing recording/user foreign key was relaxed")
	}
	var count int
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid='recording_playback_state'::regclass AND contype='f' AND confrelid='recordings'::regclass AND confdeltype='c'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("state does not cascade exclusively with its recording")
	}
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid='recording_playback_state'::regclass AND contype='f'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("new state introduced a legacy owner/user/account foreign key")
	}
}

func TestRecordingPlaybackStateMigrationUpgrade75DownAndReapply(t *testing.T) {
	f := recordingStatePG(t, 75)
	f.profiles(t)
	f.exec(t, `INSERT INTO recordings(id,user_id,type,title,start_at,end_at)VALUES('legacy','one','time_block','Retained',now(),now())`)
	f.migrate(t, 76, false)
	if err := runMigrations(t.Context(), f.ds.pool); err != nil {
		t.Fatal("production latest migration path", err)
	}
	old := f.state(t, "legacy")
	if old.runID != nil {
		t.Fatal("legacy NULL capture was assigned a run")
	}
	f.migrate(t, 75, true)
	var title string
	var removed bool
	if f.ds.pool.QueryRow(t.Context(), `SELECT title FROM recordings WHERE id='legacy'`).Scan(&title) != nil || title != "Retained" || f.ds.pool.QueryRow(t.Context(), `SELECT to_regclass('recording_playback_state') IS NULL AND NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='recordings' AND column_name='capture_state')`).Scan(&removed) != nil || !removed {
		t.Fatal("downgrade changed legacy row or retained new/capture dependencies")
	}
	f.migrate(t, 76, false)
	if err := runMigrations(t.Context(), f.ds.pool); err != nil {
		t.Fatal(err)
	}
	current := f.state(t, "legacy")
	if current.incarnation == old.incarnation || current.revision != 0 || current.generation != 0 || current.runID != nil {
		t.Fatal("reapply revived removed incarnation or capture metadata")
	}
	f.rejectCAS(t, old, recordingPlaybackClaim, errRecordingPlaybackChanged)
}

func TestRecordingPlaybackStateRestoreUsesFreshIncarnationAndNoImportedWriter(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "restored")
	old := f.mutate(t, f.state(t, r.ID), recordingPlaybackClaim)
	wire, err := json.Marshal(r)
	if err != nil || bytes.Contains(wire, []byte("incarnation")) || bytes.Contains(wire, []byte("writerGeneration")) || bytes.Contains(wire, []byte("lastOperation")) {
		t.Fatal("bookkeeping leaked into legacy recording backup DTO")
	}
	var imported models.Recording
	if json.Unmarshal(wire, &imported) != nil {
		t.Fatal("decode synthetic legacy backup")
	}
	models.RestoreRecordingCapture(&imported)
	// These are the original PG backup transaction operations, not a claim
	// that this package invokes the backup HTTP/service from another package.
	if err := f.ds.WithTx(t.Context(), func(tx *Tx) error {
		if err := tx.LockRemindersForRestore(t.Context()); err != nil {
			return err
		}
		if err := tx.GuardRegionalRestore(t.Context(), false); err != nil {
			return err
		}
		if err := tx.Users().Delete(t.Context(), "one"); err != nil {
			return err
		}
		if _, err := tx.tx.Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('one','account','Restored One')`); err != nil {
			return err
		}
		return tx.Recordings().Create(t.Context(), &imported)
	}); err != nil {
		t.Fatal("original restore-style transaction failed", err)
	}
	current := f.state(t, r.ID)
	if current.incarnation == old.incarnation || current.revision != 0 || current.generation != 0 || !current.tombstone || current.operationID != nil || !current.createdAt.Equal(old.createdAt) {
		t.Fatal("same-ID restore imported or revived old writer")
	}
	f.rejectCAS(t, old, recordingPlaybackTouch, errRecordingPlaybackChanged)
	f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='two'`)
	if err := f.ds.WithTx(t.Context(), func(tx *Tx) error { return tx.GuardRegionalRestore(t.Context(), false) }); !errors.Is(err, ErrProtectedFamilyRestore) {
		t.Fatal("foundation weakened protected restore guard")
	}
}

func TestRecordingPlaybackStatePrivateValuesDoNotSerializeWriterState(t *testing.T) {
	state := recordingPlaybackState{recordingID: "PRIVATE_RECORDING", ownerID: "PRIVATE_OWNER", incarnation: uuid.NewString(), revision: 8, generation: 4}
	for _, value := range []any{state, &state} {
		for _, verb := range []string{"%v", "%+v", "%#v"} {
			if text := fmt.Sprintf(verb, value); strings.Contains(text, "PRIVATE_") || strings.Contains(text, state.incarnation) {
				t.Fatal("bookkeeping formatting exposed original values")
			}
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("bookkeeping values were exported/importable JSON")
		}
	}
}
