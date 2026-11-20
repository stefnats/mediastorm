//go:build linux

package settingsbootstrap

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
)

// This controlled child proves cleanup ownership only, never media authority.
type recordingProgressTestChild struct {
	closed           atomic.Int64
	entered, proceed chan struct{}
}

func (c *recordingProgressTestChild) Close() {
	if c.entered != nil {
		close(c.entered)
		<-c.proceed
	}
	c.closed.Add(1)
}

type recordingProgressStartupFixture struct {
	boot    *bootstrapPGFixture
	startup *Startup
	store   *datastore.DataStore
	service *recordings.Service
	pins    *profileunlock.PGProfilePins
}

func recordingProgressStartupPG(t *testing.T, finish bool) *recordingProgressStartupFixture {
	t.Helper()
	f := &recordingProgressStartupFixture{boot: bootstrapPG(t)}
	settings := config.DefaultSettings()
	settings.Database.URL = f.boot.raw
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
		if f.service != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.service.Stop(ctx); err != nil {
				t.Error(err)
			}
		}
	})
	f.store, err = f.startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sessionStore, err := sessions.NewServiceWithStore(f.store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.pins = profileunlock.NewPGProfilePins(f.store, sessionStore)
	f.service = recordings.NewService(f.store.Recordings(), "/usr/bin/ffmpeg", t.TempDir())
	if finish {
		if err := f.startup.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func TestRecordingProgressStartupFactoryOriginalObjects(t *testing.T) {
	for _, kind := range []string{"before-finish", "ready", "foreign-store", "foreign-service", "foreign-pins", "closed", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressStartupPG(t, kind != "before-finish")
			store, service, pins := f.store, f.service, f.pins
			if kind == "foreign-store" || kind == "foreign-service" || kind == "foreign-pins" {
				// ManagedSecurity deliberately has one process owner. A genuine
				// separate PG pool tests foreign runtime identity without trying
				// to install an unsupported second startup/security owner.
				foreignStore, err := datastore.New(t.Context(), f.boot.raw)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(foreignStore.Close)
				foreignSessions, err := sessions.NewServiceWithStore(foreignStore, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				foreignPins := profileunlock.NewPGProfilePins(foreignStore, foreignSessions)
				foreignService := recordings.NewService(foreignStore.Recordings(), "/usr/bin/ffmpeg", t.TempDir())
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := foreignService.Stop(ctx); err != nil {
						t.Error(err)
					}
				})
				if foreignStore == f.store || foreignStore.Pool() == nil || foreignStore.Pool() == f.store.Pool() || foreignService == f.service || foreignPins == f.pins || !foreignService.DerivedTimelineReady(foreignStore) || !foreignPins.FamilyMediaReady(foreignStore) {
					t.Fatal("foreign arguments are not genuine independent runtime objects")
				}
				switch kind {
				case "foreign-store":
					store = foreignStore
				case "foreign-service":
					service = foreignService
				case "foreign-pins":
					pins = foreignPins
				}
			}
			if kind == "closed" {
				if err := f.startup.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "duplicate" {
				first, err := f.startup.BeginRecordingProgressBuild(store, service, pins)
				if err != nil || first == nil {
					t.Fatal("genuine original build failed", err)
				}
				t.Cleanup(first.Close)
				if _, _, _, err := first.Runtime(); err != nil {
					t.Fatal(err)
				}
				if err := first.Attach(&recordingProgressTestChild{}); err != nil {
					t.Fatal(err)
				}
			}
			handler, err := f.startup.BeginRecordingProgressBuild(store, service, pins)
			if kind == "ready" {
				if err != nil || handler == nil {
					t.Fatal("original runtime build was not accepted", err)
				}
				t.Cleanup(handler.Close)
				manager, security, coordinator, err := handler.Runtime()
				if err != nil || manager != f.boot.manager || security != f.startup.security || coordinator != f.startup.source.Coordinator() {
					t.Fatal("build borrowed different live runtime dependencies", err)
				}
				if _, _, _, err := handler.Runtime(); err == nil {
					t.Fatal("construction dependency borrow was replayed")
				}
				child := &recordingProgressTestChild{}
				if err := handler.Attach(child); err != nil {
					t.Fatal(err)
				}
				f.startup.mu.Lock()
				owned := f.startup.recordingProgress == child && f.startup.recordingProgressBuilding == nil
				f.startup.mu.Unlock()
				if !owned {
					t.Fatal("attached cleanup has no original startup owner")
				}
				if err := f.startup.Close(); err != nil {
					t.Fatal(err)
				}
				if child.closed.Load() != 1 {
					t.Fatal("direct Close skipped attached cleanup")
				}
				handler.Close()
				// An unused captured reservation has no actual constructor to
				// join and must not deadlock direct original startup shutdown.
				idle := recordingProgressStartupPG(t, true)
				unused, err := idle.startup.BeginRecordingProgressBuild(idle.store, idle.service, idle.pins)
				if err != nil {
					t.Fatal(err)
				}
				idleClosed := make(chan error, 1)
				go func() { idleClosed <- idle.startup.Close() }()
				select {
				case err := <-idleClosed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("idle build trapped Startup.Close")
				}
				if _, _, _, err := unused.Runtime(); err == nil {
					t.Fatal("retired idle build still lends dependencies")
				}
				unused.Close()
				return
			}
			if err == nil || handler != nil {
				t.Fatal("unqualified runtime produced handler", kind)
			}
			if kind == "before-finish" {
				f.startup.mu.Lock()
				marked := f.startup.finishing
				f.startup.mu.Unlock()
				if marked {
					t.Fatal("factory started an unbegun Finish")
				}
			}
		})
	}
}

func TestRecordingProgressStartupCloseJoinsFactoryBlockedOnOriginalFinish(t *testing.T) {
	f := recordingProgressStartupPG(t, false)
	if _, err := f.store.Pool().Exec(t.Context(), `CREATE TABLE IF NOT EXISTS app_data_migrations(name TEXT PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatal(err)
	}
	var relation uint32
	if err := f.store.Pool().QueryRow(t.Context(), `SELECT 'app_data_migrations'::regclass::oid`).Scan(&relation); err != nil {
		t.Fatal(err)
	}
	holder, err := f.boot.observer.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = holder.Rollback(ctx)
	})
	if _, err := holder.Exec(t.Context(), `LOCK TABLE app_data_migrations IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	migration := make(chan error, 1)
	go func() { migration <- f.startup.RunDataMigrations(t.Context()) }()
	pending := func() int {
		var count int
		if err := f.boot.observer.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted`, relation).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	until := time.Now().Add(5 * time.Second)
	for pending() == 0 {
		if time.Now().After(until) {
			t.Fatal("actual original startup query never blocked")
		}
		time.Sleep(time.Millisecond)
	}
	finished := make(chan error, 1)
	go func() { finished <- f.startup.Finish() }()
	for {
		f.startup.mu.Lock()
		entered := f.startup.finishing
		f.startup.mu.Unlock()
		if entered {
			break
		}
		if time.Now().After(until) {
			t.Fatal("actual Finish did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	type factoryResult struct {
		handler *RecordingProgressBuild
		err     error
	}
	factory := make(chan factoryResult, 1)
	go func() {
		h, err := f.startup.BeginRecordingProgressBuild(f.store, f.service, f.pins)
		factory <- factoryResult{h, err}
	}()
	for {
		f.startup.mu.Lock()
		building := f.startup.recordingProgressBuilding != nil
		ready := f.startup.recordingProgressReady
		f.startup.mu.Unlock()
		if building && ready != nil {
			select {
			case <-ready:
				t.Fatal("builder completed before held original Finish")
			default:
			}
			break
		}
		if time.Now().After(until) {
			t.Fatal("factory did not reserve its original construction")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-factory:
		t.Fatal("factory passed uncompleted Finish")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- f.startup.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("direct Startup.Close did not join original SQL/factory")
	}
	if pending() != 0 {
		t.Fatal("original canceled backend query outlived Startup.Close")
	}
	f.startup.mu.Lock()
	ready := f.startup.recordingProgressReady
	f.startup.mu.Unlock()
	select {
	case <-ready:
	default:
		t.Fatal("Close returned before actual factory cleanup barrier")
	}
	// The completion channels are goroutine result publication, which can be
	// scheduled after the already-joined cleanup barrier. They are not IO gates.
	select {
	case result := <-factory:
		if result.handler != nil || result.err == nil {
			t.Fatal("departed startup published a handler")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("factory result not published")
	}
	select {
	case err := <-migration:
		if err == nil {
			t.Fatal("held canceled original SQL unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration result not published")
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Finish result not published")
	}
	if err := holder.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	late := recordingProgressStartupPG(t, true)
	build, err := late.startup.BeginRecordingProgressBuild(late.store, late.service, late.pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(build.Close)
	if _, _, _, err := build.Runtime(); err != nil {
		t.Fatal(err)
	}
	closing := make(chan error, 1)
	go func() { closing <- late.startup.Close() }()
	select {
	case <-late.startup.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("original Close did not signal the builder")
	}
	child := &recordingProgressTestChild{entered: make(chan struct{}), proceed: make(chan struct{})}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(child.proceed) }) }
	t.Cleanup(unblock)
	attached := make(chan error, 1)
	go func() { attached <- build.Attach(child) }()
	select {
	case <-child.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("denied attach did not close actual child")
	}
	builderClosed := make(chan struct{})
	go func() { build.Close(); close(builderClosed) }()
	select {
	case <-builderClosed:
		t.Fatal("concurrent build Close overtook actual child cleanup")
	default:
	}
	select {
	case <-closing:
		t.Fatal("Startup.Close overtook actual failed-attach cleanup")
	default:
	}
	late.startup.mu.Lock()
	done := late.startup.recordingProgressReady
	late.startup.mu.Unlock()
	select {
	case <-done:
		t.Fatal("failed-attach cleanup refunded before actual Close end")
	default:
	}
	if err := late.store.Pool().Ping(t.Context()); err != nil {
		t.Fatal("original pool was released before child cleanup", err)
	}
	unblock()
	select {
	case err := <-attached:
		if err == nil {
			t.Fatal("late attach succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed attach did not finish")
	}
	select {
	case err := <-closing:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original Close did not join released child")
	}
	select {
	case <-builderClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent builder Close did not finish after cleanup")
	}
}
