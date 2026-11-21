package playback

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
)

func preparedStorePG(t *testing.T) (*datastore.DataStore, *pgxpool.Pool, string) {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket database required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("synthetic_prepared_store_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	var ds *datastore.DataStore
	t.Cleanup(func() {
		if ds != nil {
			ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE"); e != nil {
			t.Error(e)
		}
		if _, e := admin.Exec(ctx, "DROP ROLE IF EXISTS "+identifier); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	if _, err = admin.Exec(t.Context(), "CREATE ROLE "+identifier+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+identifier+" AUTHORIZATION "+identifier); err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", identifier+",pg_catalog")
	q.Set("options", "-c role="+name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	ds, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(1), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal(err)
	}
	return ds, admin, name
}
func preparedStoreWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("actual store operation did not join")
	}
}

func TestPreparedPrequeueStoreLifecycle(t *testing.T) {
	ds, admin, name := preparedStorePG(t)
	fresh := func(t *testing.T) (*PrequeueStore, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		s, e := PreparePrequeueStore(ctx, time.Hour)
		if e != nil {
			t.Fatal(e)
		}
		s.SetDataStore(ds)
		t.Cleanup(func() { cancel(); s.Close() })
		return s, cancel
	}
	seed := func(t *testing.T) {
		t.Helper()
		if e := ds.Prequeue().Upsert(t.Context(), "expired", "synthetic", "profile", "ready", []byte(`{}`), time.Now().Add(-time.Hour)); e != nil {
			t.Fatal(e)
		}
	}
	exists := func(t *testing.T) bool {
		t.Helper()
		b, e := ds.Prequeue().Get(t.Context(), "expired")
		if e != nil {
			t.Fatal(e)
		}
		return b != nil
	}
	t.Run("prepare-no-cleanup-loop", func(t *testing.T) {
		s, _ := fresh(t)
		seed(t)
		s.cleanup()
		if !exists(t) {
			t.Fatal("inactive cleanup deleted a row")
		}
		done := make(chan struct{})
		go func() { s.lifetime.operations.Wait(); close(done) }()
		preparedStoreWait(t, done)
	})
	t.Run("prepared-no-expired-row-delete", func(t *testing.T) {
		s, _ := fresh(t)
		seed(t)
		s.CleanupExpiredPersisted(t.Context())
		if !exists(t) {
			t.Fatal("inactive explicit cleanup deleted a row")
		}
		if _, _, e := s.CreateReportReplacement("expired", ""); e == nil {
			t.Fatal("inactive store admitted replacement")
		}
	})
	t.Run("activate-current-original-store", func(t *testing.T) {
		s, _ := fresh(t)
		before := s.lifetime
		if e := s.Activate(); e != nil {
			t.Fatal(e)
		}
		if e := s.Activate(); e != nil {
			t.Fatal(e)
		}
		seed(t)
		s.cleanup()
		if exists(t) || s.lifetime != before {
			t.Fatal("actual original store was not activated")
		}
	})
	blocked := func(t *testing.T, cancelOnly bool) {
		t.Helper()
		s, cancel := fresh(t)
		if e := s.Activate(); e != nil {
			t.Fatal(e)
		}
		seed(t)
		holder, e := admin.Acquire(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		tx, e := holder.Begin(t.Context())
		if e != nil {
			holder.Release()
			t.Fatal(e)
		}
		operationDone := make(chan struct{})
		closeDone := make(chan struct{})
		launched := false
		closed := false
		t.Cleanup(func() {
			cancel()
			_ = tx.Rollback(context.Background())
			holder.Release()
			if launched {
				preparedStoreWait(t, operationDone)
			}
			if closed {
				preparedStoreWait(t, closeDone)
			}
		})
		if _, e = tx.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{name, "prequeue"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); e != nil {
			t.Fatal(e)
		}
		launched = true
		go func() { s.CleanupExpiredPersisted(t.Context()); close(operationDone) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waiting bool
			e = admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'DELETE FROM prequeue%' AND $2=ANY(pg_catalog.pg_blocking_pids(pid)))`, name, holder.Conn().PgConn().PID()).Scan(&waiting)
			if e != nil {
				t.Fatal(e)
			}
			if waiting {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("actual original cleanup query was not blocked by this holder")
			}
			time.Sleep(time.Millisecond)
		}
		if cancelOnly {
			cancel()
		} else {
			closed = true
			go func() { s.Close(); close(closeDone) }()
		}
		preparedStoreWait(t, operationDone)
		if closed {
			preparedStoreWait(t, closeDone)
		}
		var waiting bool
		if e = admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name=$1 AND state='active' AND query LIKE 'DELETE FROM prequeue%')`, name).Scan(&waiting); e != nil || waiting {
			t.Fatal("store returned before actual SQL ended", e)
		}
	}
	t.Run("cleanup-sql-original-context", func(t *testing.T) { blocked(t, true) })
	t.Run("held-cleanup-close-joins", func(t *testing.T) { blocked(t, false) })
	t.Run("unstarted-close-cancels", func(t *testing.T) {
		s, _ := fresh(t)
		s.Close()
		if s.runtimeContext().Err() == nil || s.Activate() == nil {
			t.Fatal("unstarted store survived Close")
		}
	})
	t.Run("no-restart-after-close", func(t *testing.T) {
		s, _ := fresh(t)
		if e := s.Activate(); e != nil {
			t.Fatal(e)
		}
		s.Close()
		if s.Activate() == nil {
			t.Fatal("closed store restarted")
		}
		if v, _ := s.Create("a", "b", "c", "movie", 2026, nil, "details"); v != nil {
			t.Fatal("closed store admitted new work")
		}
	})
	t.Run("legacy-constructor-cleanup", func(t *testing.T) {
		s := NewPrequeueStore(time.Hour)
		t.Cleanup(s.Close)
		s.SetDataStore(ds)
		seed(t)
		s.cleanup()
		if exists(t) {
			t.Fatal("legacy cleanup no longer executes")
		}
	})
}
