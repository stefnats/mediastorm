//go:build linux

package settingsbootstrap

import (
	"context"
	"fmt"
	"net/http/httptest"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/integration"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/pool"
	"novastream/models"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type familyNZBStartupFixture struct {
	boot         *bootstrapPGFixture
	startup      *Startup
	store        *datastore.DataStore
	ns           *integration.NzbSystem
	pool         *pool.PreparedManager
	metadataRoot string
}

func familyNZBStartup(t *testing.T) *familyNZBStartupFixture { return familyNZBStartupCase(t, true) }
func familyNZBStartupCase(t *testing.T, prepare bool) *familyNZBStartupFixture {
	t.Helper()
	// Actual default relative cache must resolve under this ORIGINAL cwd.
	// This fixture is deliberately nonparallel; all actors retire before restore.
	t.Chdir(t.TempDir())
	f := &familyNZBStartupFixture{boot: bootstrapPG(t)}
	settings := config.DefaultSettings()
	settings.Database.URL = f.boot.raw
	settings.Database.MaxOpenConns = 1
	settings.Database.MaxIdleConns = 1
	if err := f.boot.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	var err error
	f.startup, err = Begin(t.Context(), f.boot.manager, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.startup.Close(); err != nil {
			t.Error(err)
		}
		if f.ns != nil {
			if err := f.ns.Close(); err != nil {
				t.Error(err)
			}
		}
		if f.pool != nil {
			if err := f.pool.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	f.store, err = f.startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if prepare {
		f.metadataRoot, err = f.startup.PrepareFamilyNZBMetadataRoot()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		f.metadataRoot = filepath.Join(working, "cache", "streams")
	}
	if f.metadataRoot != filepath.Join(working, "cache", "streams") {
		t.Fatal("actual default cache was not rooted once at startup")
	}
	f.pool, err = pool.NewPreparedManager(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	getter := func() *config.AltMountConfig {
		return &config.AltMountConfig{Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	}
	f.ns, err = integration.PrepareNzbSystem(t.Context(), integration.NzbConfig{QueueDatabasePath: filepath.Join(t.TempDir(), "queue.db"), MetadataRootPath: f.metadataRoot, MaxProcessorWorkers: 1}, f.pool, getter)
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.NewMetadataService(f.metadataRoot).WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 64, ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "synthetic-unused-startup-article", SegmentSize: 64, EndOffset: 63}}}); err != nil {
		t.Fatal(err)
	}
	return f
}
func familyNZBStartupAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(15 * time.Second):
		t.Fatal("original startup work did not physically join")
	}
}
func familyNZBStartupUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(10 * time.Second)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("actual startup witness absent")
		}
		time.Sleep(time.Millisecond)
	}
}
func (f *familyNZBStartupFixture) finish(t *testing.T) *datastore.FamilyNZBSources {
	t.Helper()
	if err := f.startup.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := f.startup.NewFamilyNZBSources(f.store, f.ns)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func familyNZBStartupOriginal(t *testing.T, f *familyNZBStartupFixture) *datastore.FamilyMediaOwner {
	t.Helper()
	if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('nzb-startup-actor','Synthetic Actor','synthetic-unused-password'); INSERT INTO users(id,account_id,name)VALUES('nzb-startup-adult','nzb-startup-actor','Synthetic Adult')`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "synthetic-nzb-startup-session", AccountID: "nzb-startup-actor", ClientID: "synthetic-nzb-startup-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := f.store.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://synthetic.invalid/original-nzb", nil)
	r.Header.Set("X-Client-ID", session.ClientID)
	checked, release, err := f.store.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	o, err := f.store.CaptureFamilyMediaOwner(checked, "nzb-startup-adult", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)
	if err = o.Handoff(checked); err != nil {
		t.Fatal(err)
	}
	return o
}
func TestFamilyNZBOriginalFactory(t *testing.T) {
	t.Run("unfinished-startup-denied", func(t *testing.T) {
		f := familyNZBStartup(t)
		if r, err := f.startup.NewFamilyNZBSources(f.store, f.ns); err == nil || r != nil {
			t.Fatal("factory began an unbegun Finish")
		}
		f.startup.mu.Lock()
		started := f.startup.finishing
		f.startup.mu.Unlock()
		if started {
			t.Fatal("unbegun Finish was started by factory")
		}
		// Hold a REAL earlier admitted migration behind a real PG table lock.
		tx, err := f.boot.observer.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var holder int32
		if err = tx.QueryRow(t.Context(), `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
			_ = tx.Rollback(context.Background())
			t.Fatal(err)
		}
		if _, err = tx.Exec(t.Context(), `LOCK TABLE app_data_migrations IN ACCESS EXCLUSIVE MODE`); err != nil {
			_ = tx.Rollback(context.Background())
			t.Fatal(err)
		}
		workDone, finishDone, factoryDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		workStarted, finishStarted, factoryStarted := false, false, false
		var workErr, finishErr, factoryErr error
		var result *datastore.FamilyNZBSources
		t.Cleanup(func() {
			_ = tx.Rollback(context.Background())
			if workStarted {
				familyNZBStartupAwait(t, workDone)
			}
			if finishStarted {
				familyNZBStartupAwait(t, finishDone)
			}
			if factoryStarted {
				familyNZBStartupAwait(t, factoryDone)
				if result != nil {
					result.Close()
				}
			}
		})
		workStarted = true
		go func() { defer close(workDone); workErr = f.startup.RunDataMigrations(t.Context()) }()
		familyNZBStartupUntil(t, func() bool {
			var yes bool
			err := f.boot.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND $2::int=ANY(pg_blocking_pids(pid)))`, f.boot.name, holder).Scan(&yes)
			return err == nil && yes
		})
		finishStarted = true
		go func() { defer close(finishDone); finishErr = f.startup.Finish() }()
		familyNZBStartupUntil(t, func() bool {
			f.startup.mu.Lock()
			defer f.startup.mu.Unlock()
			return f.startup.finishing && !f.startup.finishCompleted
		})
		factoryStarted = true
		go func() { defer close(factoryDone); result, factoryErr = f.startup.NewFamilyNZBSources(f.store, f.ns) }()
		familyNZBStartupUntil(t, func() bool { f.startup.mu.Lock(); defer f.startup.mu.Unlock(); return f.startup.familyNZBBuilding })
		select {
		case <-factoryDone:
			t.Fatal("early finishing was mistaken for physical success")
		default:
		}
		if err = tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		familyNZBStartupAwait(t, workDone)
		familyNZBStartupAwait(t, finishDone)
		familyNZBStartupAwait(t, factoryDone)
		if workErr != nil || finishErr != nil || factoryErr != nil || result == nil {
			t.Fatal("actual completed original Finish/factory", workErr, finishErr, factoryErr)
		}
	})
	t.Run("exact-finished-store-and-ns-binding", func(t *testing.T) {
		f := familyNZBStartup(t)
		r := f.finish(t)
		again, err := f.startup.NewFamilyNZBSources(f.store, f.ns)
		if err != nil || again != r || !r.MatchesOriginal(f.store, f.ns) {
			t.Fatal("original exact factory binding changed", err)
		}
		f.startup.mu.Lock()
		complete := f.startup.finishCompleted
		f.startup.mu.Unlock()
		if !complete {
			t.Fatal("physical Finish completion unpublished")
		}
	})
	t.Run("foreign-ns-same-path-not-substituted", func(t *testing.T) {
		f := familyNZBStartup(t)
		r := f.finish(t)
		getter := func() *config.AltMountConfig {
			return &config.AltMountConfig{Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}}
		}
		other, err := integration.PrepareNzbSystem(t.Context(), integration.NzbConfig{QueueDatabasePath: filepath.Join(t.TempDir(), "other.db"), MetadataRootPath: f.metadataRoot}, f.pool, getter)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = other.Close() })
		if replacement, err := f.startup.NewFamilyNZBSources(f.store, other); err == nil || replacement != nil {
			t.Fatal("same path DATA substituted original NS")
		}
		if !r.MatchesOriginal(f.store, f.ns) {
			t.Fatal("foreign request retired valid original factory")
		}
		if replacement, err := f.startup.NewFamilyNZBSources(&datastore.DataStore{}, f.ns); err == nil || replacement != nil {
			t.Fatal("foreign store borrowed original factory")
		}
	})
	t.Run("missing-managed-dependencies-denied", func(t *testing.T) {
		f := familyNZBStartupCase(t, false)
		if err := f.startup.Finish(); err != nil {
			t.Fatal(err)
		}
		if value, err := f.startup.NewFamilyNZBSources(f.store, f.ns); err == nil || value != nil {
			t.Fatal("matching absolute root DATA replaced actual original preparation")
		}
		if _, err := f.startup.PrepareFamilyNZBMetadataRoot(); err == nil {
			t.Fatal("late preparation backfilled missing original root after Finish")
		}
		var zero Startup
		if value, err := zero.NewFamilyNZBSources(f.store, f.ns); err == nil || value != nil {
			t.Fatal("zero startup minted namespace registration")
		}
	})
	t.Run("close-gates-and-joins-actual-source", func(t *testing.T) {
		f := familyNZBStartup(t)
		r := f.finish(t)
		if err := f.ns.Activate(t.Context()); err != nil {
			t.Fatal(err)
		}
		o := familyNZBStartupOriginal(t, f)
		source, err := o.CaptureNZBSource(t.Context(), r, "movie.meta")
		if err != nil {
			t.Fatal(err)
		}
		held, err := f.store.Pool().Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		done, closed := make(chan struct{}), make(chan struct{})
		started, closing := false, false
		var checkErr, closeErr error
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(held.Release) }
		t.Cleanup(func() {
			release()
			if started {
				familyNZBStartupAwait(t, done)
			}
			if closing {
				familyNZBStartupAwait(t, closed)
			}
		})
		started = true
		go func() { defer close(done); checkErr = source.Check(t.Context()) }()
		familyNZBStartupUntil(t, func() bool {
			buf := make([]byte, 2<<20)
			n := runtime.Stack(buf, true)
			for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
				if strings.Contains(stack, "(*FamilyNZBSource).Check(") && strings.Contains(stack, fmt.Sprintf("%p", source)) && strings.Contains(stack, "pgxpool.(*Pool).Acquire(") {
					return true
				}
			}
			return false
		})
		closing = true
		go func() { defer close(closed); closeErr = f.startup.Close() }()
		familyNZBStartupAwait(t, done)
		release()
		familyNZBStartupAwait(t, closed)
		if checkErr == nil || closeErr != nil || source.Context().Err() == nil || r.ReadyForStore(f.store) {
			t.Fatal("actual source/native close did not join original PG cancellation", checkErr, closeErr)
		}
		if value, err := f.startup.NewFamilyNZBSources(f.store, f.ns); err == nil || value != nil {
			t.Fatal("closed factory admitted new source")
		}
	})
	t.Run("factory-does-not-activate-nzb", func(t *testing.T) {
		f := familyNZBStartup(t)
		_ = f.finish(t)
		n, err := f.ns.CaptureOriginalNamespace(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if m, err := n.CaptureMetadata(t.Context(), "movie.meta"); err == nil || m != nil {
			if m != nil {
				_ = m.Close()
			}
			t.Fatal("factory activated completed-source metadata admission")
		}
		if f.pool.HasPool() {
			t.Fatal("factory activated actual prepared NNTP manager")
		}
	})
}
