//go:build linux

package importer

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"novastream/internal/database"
	"novastream/internal/nzb/metadata"
)

func preparedImporter(t *testing.T, legacy bool) (*Service, *database.DB, context.CancelFunc) {
	t.Helper()
	db, e := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "queue.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	var s *Service
	if legacy {
		s, e = NewService(ServiceConfig{Workers: 1}, metadata.NewMetadataService(t.TempDir()), db, nil, nil)
	} else {
		s, e = PrepareService(ctx, ServiceConfig{Workers: 1}, metadata.NewMetadataService(t.TempDir()), db, nil, nil)
	}
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	t.Cleanup(func() { cancel(); _ = s.Close() })
	return s, db, cancel
}
func preparedImportWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual importer work did not end")
	}
}
func preparedImportPending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("importer returned before actual work ended")
	case <-time.After(20 * time.Millisecond):
	}
}
func preparedImportStatus(t *testing.T, db *database.DB) string {
	t.Helper()
	var status string
	if e := db.Connection().QueryRow(`SELECT status FROM import_queue WHERE nzb_path='synthetic-stale.nzb'`).Scan(&status); e != nil {
		t.Fatal(e)
	}
	return status
}
func preparedImportSeed(t *testing.T, db *database.DB) {
	t.Helper()
	if _, e := db.Connection().Exec(`INSERT INTO import_queue(nzb_path,status,priority,retry_count,max_retries)VALUES('synthetic-stale.nzb','processing',0,0,3)`); e != nil {
		t.Fatal(e)
	}
}
func preparedImportSQLWait(t *testing.T, db *sql.DB, previous int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().WaitCount <= previous {
		if time.Now().After(deadline) {
			t.Fatal("actual queue connection wait not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestPreparedImporterLifecycle(t *testing.T) {
	t.Run("prepared-no-stale-reset", func(t *testing.T) {
		s, db, _ := preparedImporter(t, false)
		preparedImportSeed(t, db)
		if preparedImportStatus(t, db) != "processing" || s.IsRunning() {
			t.Fatal("preparation reset or started original queue")
		}
	})
	t.Run("prepared-no-workers", func(t *testing.T) {
		s, db, _ := preparedImporter(t, false)
		preparedImportSeed(t, db)
		select {
		case s.workNotify <- struct{}{}:
		default:
		}
		time.Sleep(20 * time.Millisecond)
		if preparedImportStatus(t, db) != "processing" {
			t.Fatal("inactive worker touched queue")
		}
	})
	t.Run("prepared-manual-scan-denied", func(t *testing.T) {
		s, _, _ := preparedImporter(t, false)
		if !errors.Is(s.StartManualScan(t.TempDir()), ErrServiceUnavailable) {
			t.Fatal("inactive manual scan admitted")
		}
	})
	t.Run("prepared-background-item-denied", func(t *testing.T) {
		s, db, _ := preparedImporter(t, false)
		preparedImportSeed(t, db)
		s.ProcessItemInBackground(t.Context(), 1)
		if preparedImportStatus(t, db) != "processing" {
			t.Fatal("inactive background item changed queue")
		}
		s.mu.RLock()
		jobs := s.jobs
		s.mu.RUnlock()
		if jobs != 0 {
			t.Fatal("inactive background worker registered")
		}
	})
	t.Run("prepared-immediate-import-denied", func(t *testing.T) {
		s, _, _ := preparedImporter(t, false)
		if _, e := s.ProcessNZBImmediately(t.Context(), "synthetic.nzb", []byte("not an NZB")); !errors.Is(e, ErrServiceUnavailable) {
			t.Fatal("inactive immediate import reached parser", e)
		}
	})
	t.Run("cancelled-activation-no-write", func(t *testing.T) {
		s, db, cancel := preparedImporter(t, false)
		preparedImportSeed(t, db)
		cancel()
		if s.Start(t.Context()) == nil || preparedImportStatus(t, db) != "processing" {
			t.Fatal("cancelled activation reset rows")
		}
	})
	t.Run("actual-activation-stale-reset", func(t *testing.T) {
		s, db, _ := preparedImporter(t, false)
		preparedImportSeed(t, db)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		if preparedImportStatus(t, db) != "pending" {
			t.Fatal("real activation did not reset original stale item")
		}
	})
	t.Run("workers-use-original-context", func(t *testing.T) {
		s, db, cancel := preparedImporter(t, false)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		db.Connection().SetMaxOpenConns(1)
		holder, e := db.Connection().Conn(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		before := db.Connection().Stats().WaitCount
		done := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			cancel()
			_ = holder.Close()
			_ = s.Close()
			if launched {
				preparedImportWait(t, done)
			}
		})
		s.workNotify <- struct{}{}
		preparedImportSQLWait(t, db.Connection(), before)
		cancel()
		launched = true
		go func() { s.wg.Wait(); close(done) }()
		preparedImportWait(t, done)
		if s.ctx.Err() == nil {
			t.Fatal("worker outlived original runtime")
		}
	})
	manual := func(t *testing.T, originalOnly bool) {
		t.Helper()
		s, db, cancel := preparedImporter(t, false)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		dir := t.TempDir()
		if e := os.WriteFile(filepath.Join(dir, "synthetic.nzb"), []byte(`<nzb></nzb>`), 0600); e != nil {
			t.Fatal(e)
		}
		db.Connection().SetMaxOpenConns(1)
		holder, e := db.Connection().Conn(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		before := db.Connection().Stats().WaitCount
		closed := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			cancel()
			_ = holder.Close()
			_ = s.Close()
			if launched {
				preparedImportWait(t, closed)
			}
		})
		if e = s.StartManualScan(dir); e != nil {
			t.Fatal(e)
		}
		preparedImportSQLWait(t, db.Connection(), before)
		launched = true
		if originalOnly {
			cancel()
		}
		go func() {
			if originalOnly {
				s.wg.Wait()
			} else {
				_ = s.Close()
			}
			close(closed)
		}()
		preparedImportWait(t, closed)
		if s.GetScanStatus().Status != ScanStatusIdle {
			t.Fatal("manual scan was not joined")
		}
		if originalOnly {
			s.mu.RLock()
			closedBefore := s.closed
			s.mu.RUnlock()
			if closedBefore || s.ctx.Err() == nil {
				t.Fatal("original scan only ended through explicit Close")
			}
		}
	}
	t.Run("manual-scan-original-runtime", func(t *testing.T) { manual(t, true) })
	immediate := func(t *testing.T, concurrent, originalOnly bool) {
		t.Helper()
		s, _, cancel := preparedImporter(t, false)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		if e := syscall.Mkfifo(s.resolvedCache.path, 0600); e != nil {
			t.Fatal(e)
		}
		writer, e := os.OpenFile(s.resolvedCache.path, os.O_RDWR|syscall.O_NONBLOCK, 0600)
		if e != nil {
			t.Fatal(e)
		}
		heldInfo, e := writer.Stat()
		if e != nil {
			_ = writer.Close()
			t.Fatal(e)
		}
		locked := true

		done := make(chan struct{})
		closeDone := make(chan struct{})
		closing := false
		var resultErr error
		t.Cleanup(func() {
			cancel()
			if locked {
				_ = writer.Close()
			}
			_ = s.Close()
			preparedImportWait(t, done)
			if closing {
				preparedImportWait(t, closeDone)
			}
		})
		go func() {
			_, resultErr = s.ProcessNZBImmediately(t.Context(), "synthetic.nzb", []byte("synthetic invalid NZB"))
			close(done)
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			descriptors, readErr := os.ReadDir("/proc/self/fd")
			if readErr != nil {
				t.Fatal(readErr)
			}
			matching := 0
			for _, descriptor := range descriptors {
				info, statErr := os.Stat(filepath.Join("/proc/self/fd", descriptor.Name()))
				if statErr == nil && os.SameFile(heldInfo, info) {
					matching++
				}
			}
			if matching >= 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("actual immediate cache-file read was not opened")
			}
			time.Sleep(time.Millisecond)
		}

		closing = true
		if originalOnly {
			cancel()
		}
		go func() {
			if originalOnly {
				s.wg.Wait()
			} else if concurrent {
				var wg sync.WaitGroup
				wg.Add(4)
				for n := 0; n < 4; n++ {
					go func() { defer wg.Done(); _ = s.Close() }()
				}
				wg.Wait()
			} else {
				_ = s.Close()
			}
			close(closeDone)
		}()
		preparedImportPending(t, closeDone)
		locked = false
		_ = writer.Close()
		preparedImportWait(t, done)
		preparedImportWait(t, closeDone)
		if resultErr == nil {
			t.Fatal("retired import reported success")
		}
		if originalOnly {
			s.mu.RLock()
			closedBefore := s.closed
			s.mu.RUnlock()
			if closedBefore || !errors.Is(resultErr, context.Canceled) || s.ctx.Err() == nil {
				t.Fatal("original import required explicit Close or lost caller cancellation", resultErr)
			}
		}
	}
	t.Run("immediate-import-original-runtime", func(t *testing.T) { immediate(t, false, true) })
	t.Run("held-queue-worker-close-joins", func(t *testing.T) {
		s, db, cancel := preparedImporter(t, false)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		db.Connection().SetMaxOpenConns(1)
		holder, e := db.Connection().Conn(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		before := db.Connection().Stats().WaitCount
		done := make(chan struct{})
		t.Cleanup(func() { cancel(); _ = holder.Close(); _ = s.Close(); preparedImportWait(t, done) })
		s.workNotify <- struct{}{}
		go func() { s.wg.Wait(); close(done) }()
		preparedImportSQLWait(t, db.Connection(), before)
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		preparedImportWait(t, done)
		if s.IsRunning() {
			t.Fatal("late startup published workers")
		}
	})
	t.Run("held-manual-scan-close-joins", func(t *testing.T) { manual(t, false) })
	t.Run("held-immediate-import-close-joins", func(t *testing.T) { immediate(t, false, false) })
	t.Run("unstarted-close-cancels", func(t *testing.T) {
		s, _, _ := preparedImporter(t, false)
		if e := s.Close(); e != nil || s.ctx.Err() == nil {
			t.Fatal("unstarted original lifetime was not cancelled", e)
		}
	})
	t.Run("repeated-concurrent-close", func(t *testing.T) { immediate(t, true, false) })
	t.Run("no-add-after-close", func(t *testing.T) {
		s, db, _ := preparedImporter(t, false)
		_ = s.Close()
		if _, e := s.AddNZBToQueue(t.Context(), "late.nzb", []byte(`<nzb/>`)); e == nil {
			t.Fatal("closed importer wrote a queue entry")
		}
		var n int
		if e := db.Connection().QueryRow(`SELECT count(*) FROM import_queue`).Scan(&n); e != nil || n != 0 {
			t.Fatal("late queue write", n, e)
		}
	})
	t.Run("notification-close-race", func(t *testing.T) {
		s, _, _ := preparedImporter(t, false)
		if e := s.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		wg.Add(16)
		for n := 0; n < 16; n++ {
			go func() { defer wg.Done(); s.ProcessItemInBackground(t.Context(), 999) }()
		}
		_ = s.Close()
		wg.Wait()
		if e := s.Start(t.Context()); e == nil {
			t.Fatal("closed notification channel was reused")
		}
	})
	t.Run("legacy-unstarted-immediate-import", func(t *testing.T) {
		s, _, _ := preparedImporter(t, true)
		_, e := s.ProcessNZBImmediately(t.Context(), "synthetic.nzb", []byte("invalid NZB"))
		if e == nil || errors.Is(e, ErrServiceUnavailable) {
			t.Fatal("legacy unstarted import did not reach actual parser", e)
		}
	})
}
