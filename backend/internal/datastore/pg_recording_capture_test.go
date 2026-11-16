package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"novastream/models"
)

func TestRecordingCapturePostgresMigrationAndLedgerRoundTrip(t *testing.T) {
	raw := os.Getenv("RECORDING_CAPTURE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("disposable PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("a synthetic Unix-socket fixture database is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect to synthetic fixture")
	}
	defer admin.Close()
	for _, upgrade := range []bool{false, true} {
		name := "fresh"
		if upgrade {
			name = "upgrade75"
		}
		t.Run(name, func(t *testing.T) {
			dbName := fmt.Sprintf("synthetic_recording_capture_%d", time.Now().UnixNano())
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
					t.Error("remove own fixture database")
				}
			}()
			target := *u
			target.Path = "/" + dbName
			pool, err := pgxpool.New(ctx, target.String())
			if err != nil {
				t.Fatal("connect to child fixture")
			}
			defer pool.Close()
			goose.SetBaseFS(embedMigrations)
			if err := goose.SetDialect("postgres"); err != nil {
				t.Fatal(err)
			}
			migrateTo := func(version int64, down bool) {
				t.Helper()
				db := stdlib.OpenDBFromPool(pool)
				var err error
				if down {
					err = goose.DownToContext(ctx, db, "migrations", version)
				} else {
					err = goose.UpToContext(ctx, db, "migrations", version)
				}
				got, versionErr := goose.GetDBVersionContext(ctx, db)
				db.Close()
				if err != nil || versionErr != nil || got != version {
					t.Fatalf("migration target %d failed", version)
				}
			}
			if upgrade {
				migrateTo(75, false)
				if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-capture','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('one','fixture','Synthetic One'),('two','fixture','Synthetic Two'); INSERT INTO recordings(id,user_id,type,status,channel_id,channel_name,title,source_url,start_at,end_at,created_at,updated_at) VALUES('legacy','one','time_block','completed','channel','Synthetic','Legacy','',now(),now()+interval '1 hour',now(),now())`); err != nil {
					t.Fatal(err)
				}
			}
			migrateTo(76, false)
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if !upgrade {
				if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-capture','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('one','fixture','Synthetic One'),('two','fixture','Synthetic Two')`); err != nil {
					t.Fatal(err)
				}
			} else {
				var isNull bool
				if err := pool.QueryRow(ctx, `SELECT capture_state IS NULL FROM recordings WHERE id='legacy'`).Scan(&isNull); err != nil || !isNull {
					t.Fatal("upgrade did not preserve legacy SQL NULL")
				}
				old, err := (&pgRecordingRepo{pool: pool}).Get(ctx, "legacy")
				if err != nil || old == nil || old.Capture != nil || old.Title != "Legacy" {
					t.Fatal("upgrade changed legacy recording")
				}
			}
			now := time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)
			closed := now.Add(time.Minute)
			capture := &models.RecordingCapture{Version: 1, Phase: "capturing", Incomplete: true, SourceChanges: 1, StationID: "station:synthetic", RunID: "synthetic-run", Parts: []models.RecordingPart{
				{Index: 1, SourceID: "list-one", ChannelID: "channel-one", StartedAt: now, EndedAt: &closed, Bytes: 8192, EndReason: "source_ended", StreamPath: "recording:captured/parts/1"},
				{Index: 2, SourceID: "list-two", ChannelID: "channel-two", StartedAt: closed, Bytes: 4096, StreamPath: "recording:captured/parts/2"},
			}, Gaps: []models.RecordingGap{{StartedAt: closed, Reason: "capacity"}}}
			recording := &models.Recording{ID: "captured", UserID: "one", Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusRunning, ChannelID: "channel-one", ChannelName: "Synthetic", Title: "Synthetic programme", StartAt: now, EndAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now, Capture: capture}
			repo := &pgRecordingRepo{pool: pool}
			if err := repo.Create(ctx, recording); err != nil {
				t.Fatal(err)
			}
			assertLedger := func(repo *pgRecordingRepo, want *models.RecordingCapture) {
				t.Helper()
				got, err := repo.Get(ctx, recording.ID)
				if err != nil || got == nil || !reflect.DeepEqual(got.Capture, want) {
					t.Fatal("recording capture ledger did not round-trip")
				}
				list, err := repo.List(ctx, models.RecordingListFilter{UserID: "one", Statuses: []models.RecordingStatus{recording.Status}})
				if err != nil || len(list) != 1 || !reflect.DeepEqual(list[0].Capture, want) {
					t.Fatal("recording list lost capture ledger")
				}
				other, err := repo.List(ctx, models.RecordingListFilter{UserID: "two"})
				if err != nil || len(other) != 0 {
					t.Fatal("recording list crossed profile boundary")
				}
			}
			assertLedger(repo, capture)
			capture.Phase = "interrupted"
			capture.Parts[1].EndedAt = &closed
			capture.Parts[1].EndReason = "shutdown"
			capture.Gaps[0].EndedAt = &closed
			recording.Status = models.RecordingStatusFailed
			if err := repo.Update(ctx, recording); err != nil {
				t.Fatal(err)
			}
			assertLedger(repo, capture)
			reopened, err := New(ctx, target.String())
			if err != nil {
				t.Fatal(err)
			}
			assertLedger(&pgRecordingRepo{pool: reopened.pool}, capture)
			reopened.Close()
			stale := &models.Recording{ID: "stale-after-crash", UserID: "two", Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusRunning, ChannelID: "channel", ChannelName: "Synthetic", Title: "Interrupted", StartAt: now, EndAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now, Capture: &models.RecordingCapture{Version: 1, Phase: "capturing", StationID: "station:synthetic", RunID: "old-process", Parts: []models.RecordingPart{{Index: 1, StartedAt: now, Bytes: 123}}, Gaps: []models.RecordingGap{}}}
			if err := repo.Create(ctx, stale); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.MarkStaleActiveAsFailed(ctx, now.Add(10*time.Minute)); err != nil {
				t.Fatal(err)
			}
			crashed, err := repo.Get(ctx, stale.ID)
			if err != nil || crashed.Status != models.RecordingStatusFailed || crashed.Capture.Phase != "interrupted" || !crashed.Capture.Incomplete || crashed.Capture.RunID != "" || crashed.Capture.StationID != "" || crashed.Capture.Parts[0].EndedAt != nil || crashed.Capture.Parts[0].Bytes != 123 {
				t.Fatal("startup invented a complete/precisely ended capture or retained old process authority")
			}
			wire, err := json.Marshal(recording)
			if err != nil {
				t.Fatal(err)
			}
			var imported models.Recording
			if err := json.Unmarshal(wire, &imported); err != nil || !reflect.DeepEqual(imported.Capture, capture) {
				t.Fatal("internal backup JSON lost ledger")
			}
			models.RestoreRecordingCapture(&imported)
			if imported.Capture == nil || !imported.Capture.Restored || imported.Capture.StationID != "" || imported.Capture.RunID != "" || imported.Capture.Parts[0].StreamPath != "" || capture.Parts[0].StreamPath == "" {
				t.Fatal("restore retained authority or mutated original ledger")
			}
			migrateTo(75, true)
			var exists bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='recordings' AND column_name='capture_state')`).Scan(&exists); err != nil || exists {
				t.Fatal("migration76 down retained capture column")
			}
			var title string
			if err := pool.QueryRow(ctx, `SELECT title FROM recordings WHERE id='captured'`).Scan(&title); err != nil || title != recording.Title {
				t.Fatal("migration76 down removed recording")
			}
			migrateTo(76, false)
			got, err := repo.Get(ctx, recording.ID)
			if err != nil || got == nil || got.Capture != nil {
				t.Fatal("migration76 reapply invented removed capture metadata")
			}
		})
	}
}
