package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/sessions"
)

func TestSABManagedCopyActualSQLRoleAndSessionReads(t *testing.T) {
	raw := os.Getenv("SAB_FALLBACK_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("synthetic PostgreSQL socket fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic fixture")
	}
	defer admin.Close()
	schema := fmt.Sprintf("synthetic_sab_managed_copy_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal("create own schema")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own schema")
		}
	}()
	target := *u
	q := target.Query()
	q.Set("search_path", schema)
	target.RawQuery = q.Encode()
	store, err := datastore.New(ctx, target.String(), datastore.WithMaxConns(2))
	if err != nil {
		t.Fatal("migrate own schema")
	}
	defer store.Close()
	at := time.Now().UTC().Truncate(time.Microsecond)
	account := models.Account{ID: "synthetic-actor", Username: "synthetic-sab-master", PasswordHash: "not-a-login-fixture", IsMaster: true, CreatedAt: at, UpdatedAt: at}
	if err := store.Accounts().Create(ctx, &account); err != nil {
		t.Fatal(err)
	}
	realSessions, err := sessions.NewServiceWithStore(store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"positive cached false", "demoted before", "demoted during", "expired account", "revoked during", "session scope during", "expiry during"} {
		t.Run(mode, func(t *testing.T) {
			if _, err := store.Pool().Exec(ctx, "UPDATE accounts SET is_master=true,expires_at=NULL,updated_at=$1 WHERE id=$2", at, account.ID); err != nil {
				t.Fatal(err)
			}
			issued, err := realSessions.Create(account.ID, mode != "positive cached false", "synthetic-test", "127.0.0.1")
			if err != nil {
				t.Fatal(err)
			}
			// A normal PG-backed auth request carries the validated SQL snapshot. This
			// test injects that context, not a successful browser/password login.
			claim, err := realSessions.Validate(issued.Token)
			if err != nil {
				t.Fatal(err)
			}
			service := &managedCopyAccessService{}
			if mode == "demoted before" {
				if _, err := store.Pool().Exec(ctx, "UPDATE accounts SET is_master=false WHERE id=$1", account.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "expired account" {
				if _, err := store.Pool().Exec(ctx, "UPDATE accounts SET expires_at=$1 WHERE id=$2", at.Add(-time.Hour), account.ID); err != nil {
					t.Fatal(err)
				}
			}
			service.afterCopy = func(work context.Context) {
				var query string
				var args []any
				switch mode {
				case "demoted during":
					query = "UPDATE accounts SET is_master=false WHERE id=$1"
					args = []any{account.ID}
				case "revoked during":
					query = "DELETE FROM sessions WHERE token=$1"
					args = []any{issued.Token}
				case "session scope during":
					query = "UPDATE sessions SET scope='stream',scope_resource='private' WHERE token=$1"
					args = []any{issued.Token}
				case "expiry during":
					query = "UPDATE sessions SET expires_at=$1 WHERE token=$2"
					args = []any{at.Add(-time.Hour), issued.Token}
				default:
					return
				}
				_, err := store.Pool().Exec(work, query, args...)
				if err != nil {
					t.Error(err)
				}
			}
			handler := NewUsenetFallbackHandler(service, realSessions, store.Accounts())
			handler.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return managedCopyConfig(), ctx.Err() })
			request := httptest.NewRequest(http.MethodPost, "/admin/api/usenet/fallback/17/stage", nil)
			request = mux.SetURLVars(request, map[string]string{"queueID": "17"})
			request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeySession, claim))
			request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeyAccountID, account.ID))
			request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeyIsMaster, true))
			w := httptest.NewRecorder()
			handler.CopyFiles(w, request)
			expected := 403
			if mode == "positive cached false" {
				expected = 200
			}
			if w.Code != expected {
				t.Fatalf("SQL mode%s status%d", mode, w.Code)
			}
			if mode == "demoted before" || mode == "expired account" {
				if service.copies.Load() != 0 {
					t.Fatal("stale cached role started service")
				}
			} else if service.copies.Load() != 1 {
				t.Fatal("positive precheck did not reach counted service")
			}
			if expected == 403 && strings.Contains(w.Body.String(), "copy_recorded") {
				t.Fatal("late SQL denial emitted old observation")
			}
			if mode == "positive cached false" {
				stored, err := store.Sessions().Get(ctx, issued.Token)
				if err != nil || !stored.ExpiresAt.Equal(claim.ExpiresAt) {
					t.Fatal("status renewed SQL expiry")
				}
			}
		})
	}
}
