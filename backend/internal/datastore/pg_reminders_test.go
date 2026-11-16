package datastore

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"novastream/models"
	"novastream/services/reminders"
)

func TestReminderPostgresFreshUpgradeConcurrencyRevokeRestartAndRollback(t *testing.T) {
	raw := os.Getenv("REMINDER_TEST_DATABASE_URL")
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
			name = "upgrade74"
		}
		t.Run(name, func(t *testing.T) {
			dbName := fmt.Sprintf("synthetic_reminders_%d", time.Now().UnixNano())
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
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
			if upgrade {
				goose.SetBaseFS(embedMigrations)
				if err := goose.SetDialect("postgres"); err != nil {
					t.Fatal(err)
				}
				db := stdlib.OpenDBFromPool(pool)
				err := goose.UpToContext(ctx, db, "migrations", 74)
				db.Close()
				if err != nil {
					t.Fatal(err)
				}
				db = stdlib.OpenDBFromPool(pool)
				err = goose.UpToContext(ctx, db, "migrations", 75)
				version, versionErr := goose.GetDBVersionContext(ctx, db)
				db.Close()
				if err != nil || versionErr != nil || version != 75 {
					t.Fatal("explicit migration74 to75 upgrade failed")
				}
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			var version int64
			db := stdlib.OpenDBFromPool(pool)
			version, err = goose.GetDBVersionContext(ctx, db)
			db.Close()
			if err != nil || version < 75 {
				t.Fatal("migration75 was not applied")
			}
			if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash)VALUES('fixture','synthetic-reminders','fixture-hash');INSERT INTO users(id,account_id,name)VALUES('one','fixture','Synthetic One'),('two','fixture','Synthetic Two')`); err != nil {
				t.Fatal(err)
			}
			repo := &pgReminderRepo{pool: pool}
			service := reminders.New(repo, func(profile string) bool { return profile == "one" || profile == "two" })
			title := models.Title{ID: "tmdb:movie:7", Name: "Synthetic Film", MediaType: "movie", TMDBID: 7}
			var wg sync.WaitGroup
			ids := make(chan string, 12)
			failures := make(chan error, 12)
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					item, _, err := service.Create(ctx, "one", "phone", reminders.CreateRequest{ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, title)
					ids <- item.ID
					failures <- err
				}()
			}
			wg.Wait()
			close(ids)
			close(failures)
			id := ""
			for got := range ids {
				if id == "" {
					id = got
				}
				if id != got {
					t.Fatal("concurrent creation duplicated subscriptions")
				}
			}
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			checked, expires := time.Now().UTC().Add(time.Millisecond), time.Now().UTC().Add(time.Hour)
			value := models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires, Reason: "matching_candidates"}
			time.Sleep(2 * time.Millisecond)
			failures = make(chan error, 12)
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); failures <- service.ObserveSource(ctx, "one", "phone", title, nil, value) }()
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			events, _, err := service.Events(ctx, "one", "", 100, false)
			if err != nil || len(events) != 1 || !events[0].CandidateOnly {
				t.Fatal("atomic candidate event missing or duplicated")
			}
			event := events[0]
			yes := true
			if _, err := service.Mark(ctx, "one", event.ID, &yes, &yes); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(ctx, target.String())
			if err != nil {
				t.Fatal(err)
			}
			state, err := reopened.Reminders().Get(ctx, "one")
			reopened.Close()
			if err != nil || len(state.Events) != 1 || state.Events[0].MilestoneKey == "" || state.Events[0].DismissedAt == nil {
				t.Fatal("restart lost durable inbox receipt")
			}
			if err := service.Revoke(ctx, "one", id); err != nil {
				t.Fatal(err)
			}
			checked = time.Now().UTC()
			service.ObserveSource(ctx, "one", "phone", title, nil, value)
			state, err = repo.Get(ctx, "one")
			if err != nil || len(state.Events) != 1 || state.Subscriptions[0].RevokedAt == nil {
				t.Fatal("revocation lost receipt")
			}
			other, err := repo.Get(ctx, "two")
			if err != nil || len(other.Subscriptions) != 0 || len(other.Events) != 0 {
				t.Fatal("profile data leaked")
			}
			before := len(state.Events)
			if err := repo.Update(ctx, "one", func(s *models.ReminderProfileState) error {
				s.Events = nil
				return fmt.Errorf("synthetic mutation failure")
			}); err == nil {
				t.Fatal("failed change committed")
			}
			state, _ = repo.Get(ctx, "one")
			if len(state.Events) != before {
				t.Fatal("rollback changed receipts")
			}
			if _, err := pool.Exec(ctx, `INSERT INTO reminder_events(profile_id,id,reminder_id,event_kind,milestone_key,payload)SELECT profile_id,'duplicate',reminder_id,event_kind,milestone_key,payload FROM reminder_events LIMIT 1`); err == nil {
				t.Fatal("database allowed duplicate event milestone")
			}
			exerciseReminderHistoryAndRestore(t, ctx, pool, repo, service)
			db = stdlib.OpenDBFromPool(pool)
			err = goose.DownToContext(ctx, db, "migrations", 74)
			db.Close()
			if err != nil {
				t.Fatal(err)
			}
			var exists *string
			if err := pool.QueryRow(ctx, `SELECT to_regclass('reminder_events')::text`).Scan(&exists); err != nil || exists != nil {
				t.Fatal("migration75 down left reminder table")
			}
			db = stdlib.OpenDBFromPool(pool)
			err = goose.UpToContext(ctx, db, "migrations", 75)
			version, versionErr := goose.GetDBVersionContext(ctx, db)
			db.Close()
			if err != nil || versionErr != nil || version != 75 {
				t.Fatal("explicit migration75 reapply failed")
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			state, err = repo.Get(ctx, "one")
			if err != nil || len(state.Events) != 0 {
				t.Fatal("migration75 reapply failed")
			}
			item, _, err := service.Create(ctx, "one", "phone", reminders.CreateRequest{ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, title)
			if err != nil || item.ID == "" {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `DELETE FROM users WHERE id='one'`); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM reminder_subscriptions`).Scan(&count); err != nil || count != 0 {
				t.Fatal("profile deletion retained reminders")
			}
		})
	}
}

func exerciseReminderHistoryAndRestore(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *pgReminderRepo, service *reminders.Service) {
	t.Helper()
	const history = 1500
	if err := repo.Update(ctx, "one", func(state *models.ReminderProfileState) error {
		reminder := state.Subscriptions[0].Reminder
		for i := 0; i < history; i++ {
			key := fmt.Sprintf("historical-%d", i)
			state.Checkpoints = append(state.Checkpoints, models.ReminderCheckpoint{ReminderID: reminder.ID, MilestoneKey: key, Fired: true})
			state.Events = append(state.Events, models.ReminderEvent{ID: key, ReminderID: reminder.ID, EventKind: models.ReminderNewEpisode, ContentID: reminder.ContentID, ContentType: reminder.ContentType, Title: reminder.Title, MilestoneKey: key, OccurredAt: time.Now().UTC()})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var untouchedBefore, untouchedAfter string
	if err := pool.QueryRow(ctx, `SELECT xmin::text FROM reminder_events WHERE profile_id='one' AND id='historical-1'`).Scan(&untouchedBefore); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	yes := true
	_, err := service.Mark(bounded, "one", "historical-0", &yes, nil)
	cancel()
	if err != nil {
		t.Fatalf("single inbox update exceeded bounded history path: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT xmin::text FROM reminder_events WHERE profile_id='one' AND id='historical-1'`).Scan(&untouchedAfter); err != nil || untouchedAfter != untouchedBefore {
		t.Fatal("single mark rewrote untouched history")
	}
	state, err := repo.Get(ctx, "one")
	if err != nil || len(state.Events) != history+1 {
		t.Fatal("large history lost receipts")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- repo.Update(ctx, "one", func(state *models.ReminderProfileState) error {
			close(entered)
			<-release
			state.Subscriptions[0].ObserveAfter = time.Time{}
			return nil
		})
	}()
	<-entered
	restoring, finish := make(chan struct{}), make(chan struct{})
	restoreDone := make(chan error, 1)
	boundary := time.Now().UTC().Add(time.Hour)
	store := &DataStore{pool: pool}
	go func() {
		restoreDone <- store.WithTx(ctx, func(tx *Tx) error {
			if err := tx.LockRemindersForRestore(ctx); err != nil {
				return err
			}
			close(restoring)
			<-finish
			state.Subscriptions[0].ObserveAfter = boundary
			state.Subscriptions[0].RevokedAt = &boundary
			if _, err := tx.tx.Exec(ctx, `DELETE FROM users WHERE id='one';INSERT INTO users(id,account_id,name)VALUES('one','fixture','Restored Synthetic')`); err != nil {
				return err
			}
			return tx.RestoreReminders(ctx, "one", state)
		})
	}()
	select {
	case <-restoring:
		t.Fatal("restore bypassed shared old writer")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	<-restoring
	lateRead := make(chan struct{})
	lateDone := make(chan error, 1)
	go func() {
		lateDone <- repo.Update(ctx, "one", func(state *models.ReminderProfileState) error {
			close(lateRead)
			if !state.Subscriptions[0].ObserveAfter.Equal(boundary) || state.Subscriptions[0].RevokedAt == nil {
				return fmt.Errorf("old boundary overwrote restore")
			}
			return nil
		})
	}()
	select {
	case <-lateRead:
		t.Fatal("new update read before restore committed")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	if err := <-restoreDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lateDone; err != nil {
		t.Fatal(err)
	}
}
