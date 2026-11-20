//go:build linux || darwin || windows

package accountrecovery

import (
	"bytes"
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
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
)

type securityRecoveryFixture struct {
	store     *datastore.DataStore
	admin     *pgxpool.Pool
	manager   *config.Manager
	raw, name string
	oldHash   []byte
}

func securityRecoveryPG(t *testing.T) *securityRecoveryFixture {
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
	name := fmt.Sprintf("synthetic_settings_recovery_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own database")
	}
	f := &securityRecoveryFixture{admin: admin, name: name, manager: config.NewManager(filepath.Join(t.TempDir(), "settings.json"))}
	t.Cleanup(func() {
		if f.store != nil {
			f.store.Close()
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
	settings := config.DefaultSettings()
	settings.Database.URL = f.raw
	settings.Database.MaxOpenConns = 1
	settings.Database.MaxIdleConns = 1
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	f.store, err = datastore.New(t.Context(), f.raw, datastore.WithMaxConns(1), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal("actual synthetic schema", err)
	}
	f.oldHash, err = bcrypt.GenerateFromPassword([]byte("SyntheticOld123!"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"target", "other"} {
		account := models.Account{ID: id, Username: "Synthetic " + id, PasswordHash: string(f.oldHash), IsMaster: id == "target", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
		if err = f.store.Accounts().Create(t.Context(), &account); err != nil {
			t.Fatal("create own target accounts", err)
		}
		session := models.Session{Token: "synthetic-session-" + id, AccountID: id, IsMaster: id == "target", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}
		if err = f.store.Sessions().Create(t.Context(), &session); err != nil {
			t.Fatal("create own synthetic session", err)
		}
	}
	return f
}
func TestSecurityRecoveryActualRunPreservesTargetPasswordAndSessionPolicy(t *testing.T) {
	f := securityRecoveryPG(t)
	var output bytes.Buffer
	options := Options{ConfigPath: f.manager.ConfigPath(), AccountID: "target", NewPassword: "SyntheticNew123!", RevokeSessions: true}
	if err := RunWithOptions(options, &output, func(key string) string {
		if key == "DATABASE_URL" {
			return " " + f.raw + " "
		}
		return ""
	}); err != nil {
		t.Fatal("actual coordinated account recovery failed", err)
	}
	defer output.Reset()
	account, err := f.store.Accounts().Get(t.Context(), "target")
	if err != nil || account == nil || bcrypt.CompareHashAndPassword([]byte(account.PasswordHash), []byte(options.NewPassword)) != nil {
		t.Fatal("actual target password was not changed")
	}
	if bytes.Equal([]byte(account.PasswordHash), f.oldHash) {
		t.Fatal("recovery retained original hash")
	}
	target, err := f.store.Sessions().ListByAccount(t.Context(), "target")
	if err != nil || len(target) != 0 {
		t.Fatal("original target sessions not revoked")
	}
	other, err := f.store.Sessions().ListByAccount(t.Context(), "other")
	if err != nil || len(other) != 1 {
		t.Fatal("recovery changed unrelated account sessions")
	}
	if !strings.Contains(output.String(), "account_id: target") || !strings.Contains(output.String(), "revoked_sessions: 1") {
		t.Fatal("existing recovery output contract changed")
	}
	// Synthetic password output stays only in this private buffer, never logs.
}
func TestSecurityRecoveryBusyCannotDefaultMigrateOrChangeAccount(t *testing.T) {
	f := securityRecoveryPG(t)
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked) != nil || !locked {
		t.Fatal("hold actual original progress TX")
	}
	missing := filepath.Join(t.TempDir(), "settings.json")
	var output bytes.Buffer
	err = RunWithOptions(Options{ConfigPath: missing, AccountID: "target", NewPassword: "SyntheticDenied123!", RevokeSessions: true}, &output, func(key string) string {
		if key == "DATABASE_URL" {
			return f.raw
		}
		return ""
	})
	if !errors.Is(err, settingscommit.ErrBusy) || output.Len() != 0 {
		t.Fatal("Busy recovery crossed original mutation admission", err)
	}
	if _, err = os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("Busy recovery created defaults before real PG fence")
	}
	tx.Rollback(t.Context())
	account, err := f.store.Accounts().Get(t.Context(), "target")
	if err != nil || account == nil || !bytes.Equal([]byte(account.PasswordHash), f.oldHash) {
		t.Fatal("Busy recovery changed original account")
	}
	sessions, err := f.store.Sessions().ListByAccount(t.Context(), "target")
	if err != nil || len(sessions) != 1 {
		t.Fatal("Busy recovery revoked original sessions")
	}
}
