package backup

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
	"novastream/services/reminders"
)

func TestReminderPostgresBackupRoundtripRetainsReceiptsAndResetsProof(t *testing.T) {
	raw := os.Getenv("REMINDER_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("disposable PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("a synthetic Unix-socket fixture database is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect to synthetic fixture")
	}
	defer admin.Close()
	name := fmt.Sprintf("synthetic_reminder_backup_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
	}()
	target := *u
	target.Path = "/" + name
	store, err := datastore.New(ctx, target.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Pool().Exec(ctx, `INSERT INTO accounts(id,username,password_hash)VALUES('fixture','synthetic-backup','fixture-hash');INSERT INTO users(id,account_id,name)VALUES('one','fixture','Synthetic')`); err != nil {
		t.Fatal(err)
	}
	service := reminders.New(store.Reminders(), func(p string) bool { return p == "one" })
	title := models.Title{ID: "tmdb:movie:7", Name: "Synthetic", MediaType: "movie"}
	sub, _, err := service.Create(ctx, "one", "phone", reminders.CreateRequest{ContentID: title.ID, ContentType: "movie", EventKind: models.ReminderMatchingSource}, title)
	if err != nil {
		t.Fatal(err)
	}
	checked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	if err := service.ObserveSource(ctx, "one", "phone", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires}); err != nil {
		t.Fatal(err)
	}
	backup, _ := setupTestService(t)
	backup.SetDataStore(store)
	data, err := backup.exportDatabaseBytes()
	if err != nil {
		t.Fatal(err)
	}
	var exported databaseExport
	if err := json.Unmarshal(data, &exported); err != nil {
		t.Fatal(err)
	}
	before := exported.Reminders["one"]
	if len(before.Events) != 1 || before.EventKeys[before.Events[0].ID] == "" {
		t.Fatal("PG export omitted hidden receipt identities")
	}
	if err := service.Revoke(ctx, "one", sub.ID); err != nil {
		t.Fatal(err)
	}
	restoredAt := time.Now().UTC()
	if err := backup.importDatabaseBytes(data); err != nil {
		t.Fatal(err)
	}
	after, err := store.Reminders().Get(ctx, "one")
	if err != nil || len(after.Events) != 1 || after.Events[0].MilestoneKey == "" || after.Subscriptions[0].RevokedAt != nil || after.Subscriptions[0].ObserveAfter.Before(restoredAt) {
		t.Fatal("PG import lost controls/receipt or retained old authority")
	}
	for _, checkpoint := range after.Checkpoints {
		if !checkpoint.Fired || checkpoint.State != "unknown" || !checkpoint.ObservedAt.IsZero() {
			t.Fatal("import manufactured observation authority")
		}
	}
	fresh := reminders.New(store.Reminders(), func(p string) bool { return p == "one" })
	fresh.ObserveSource(ctx, "one", "phone", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires})
	events, _, err := fresh.Events(ctx, "one", "", 100, false)
	if err != nil || len(events) != 1 {
		t.Fatal("restore/restart duplicated historical candidate")
	}
}
