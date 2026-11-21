//go:build linux

package importer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
)

func originalImporterNoAdmission(t *testing.T, s *Service) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.jobs != 0 || len(s.publications) != 0 {
		t.Fatal("missing caller admitted source work")
	}
}

func originalImporterCache(t *testing.T) (*Service, []byte, string) {
	t.Helper()
	m := metadata.NewMetadataService(t.TempDir())
	path := "/movies/synthetic-original.mkv"
	if err := m.WriteFileMetadata(path, &metapb.FileMetadata{FileSize: 17, SourceNzbPath: "synthetic.nzb", Status: metapb.FileStatus_FILE_STATUS_HEALTHY}); err != nil {
		t.Fatal(err)
	}
	raw := []byte("synthetic already-resolved NZB")
	c := newResolvedNZBCache(m)
	if err := c.put(t.Context(), resolvedNZBHash(raw), "synthetic.nzb", path, ResolvedNZBSource{DownloadURL: "https://synthetic.invalid/original.nzb"}); err != nil {
		t.Fatal(err)
	}
	s := &Service{resolvedCache: c, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	t.Cleanup(func() { _ = s.Close() })
	return s, raw, path
}

// The callbacks below model importer policy ports only. They are deliberately
// not represented as a PostgreSQL login or an installed HTTP authorization.
func originalImporterStageZero(t *testing.T, cancelled bool) {
	t.Helper()
	f := newPreviewFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	id, _ := f.add(t)
	var calls atomic.Int32
	settings := func(ctx context.Context) (config.SABnzbdSettings, error) { calls.Add(1); return f.settings(ctx) }
	root := t.TempDir()
	policy := func(ctx context.Context) (SABImportStagePolicy, error) {
		calls.Add(1)
		return SABImportStagePolicy{root, 1 << 20}, ctx.Err()
	}
	permit := func(ctx context.Context) error { calls.Add(1); return ctx.Err() }
	var caller context.Context
	if cancelled {
		ctx, stop := context.WithCancel(t.Context())
		stop()
		caller = ctx
	}
	before := f.statuses.Load()
	if _, err := f.service.StageSABImport(caller, id, settings, policy, permit); err == nil {
		t.Fatal("invalid original caller accepted")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 || calls.Load() != 0 || f.statuses.Load() != before || stageCharge(t, f) != 0 {
		t.Fatal("invalid caller demanded policy, status, charge, or copy work", err)
	}
	originalImporterNoAdmission(t, f.service)
}

func TestImporterSourceWorkRejectsMissingOriginalCaller(t *testing.T) {
	t.Run("nil-stage-zero-policy-demand", func(t *testing.T) { originalImporterStageZero(t, false) })
	t.Run("cancelled-stage-zero-policy-demand", func(t *testing.T) { originalImporterStageZero(t, true) })
	t.Run("nil-preview-zero-policy-demand", func(t *testing.T) {
		f := newPreviewFixture(t)
		t.Cleanup(func() { _ = f.service.Close() })
		id, _ := f.add(t)
		var calls atomic.Int32
		settings := func(ctx context.Context) (config.SABnzbdSettings, error) { calls.Add(1); return f.settings(ctx) }
		permit := func(ctx context.Context) error { calls.Add(1); return ctx.Err() }
		ctx, stop := context.WithCancel(t.Context())
		stop()
		before := f.statuses.Load()
		for _, invalid := range []context.Context{nil, ctx} {
			if _, err := f.service.PreviewSABImport(invalid, id, settings, permit); err == nil {
				t.Fatal("preview accepted invalid original caller")
			}
		}
		if calls.Load() != 0 || before != f.statuses.Load() {
			t.Fatal("preview called policy or provider without original caller")
		}
		originalImporterNoAdmission(t, f.service)
	})
	t.Run("nil-status-zero-sql-demand", func(t *testing.T) {
		f := newSABFallbackFixture(t)
		t.Cleanup(func() { _ = f.service.Close() })
		f.db.Connection().SetMaxOpenConns(1)
		holder, err := f.db.Connection().Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		before := f.db.Connection().Stats().WaitCount
		done := make(chan struct{})
		var outcome error
		t.Cleanup(func() { _ = holder.Close(); _ = f.service.Close(); preparedImportWait(t, done) })
		go func() {
			defer close(done)
			cancelled, stop := context.WithCancel(t.Context())
			stop()
			for _, ctx := range []context.Context{nil, cancelled} {
				if _, e := f.service.ListSABFallbackStatuses(ctx, "", 10); e == nil {
					outcome = errors.New("list accepted invalid caller")
					return
				}
				if _, e := f.service.GetSABFallbackStatus(ctx, f.item.ID); e == nil {
					outcome = errors.New("get accepted invalid caller")
					return
				}
				if _, e := f.service.RefreshSABFallbackStatus(ctx, f.item.ID); e == nil {
					outcome = errors.New("refresh accepted invalid caller")
					return
				}
			}
		}()
		preparedImportWait(t, done)
		if outcome != nil || before != f.db.Connection().Stats().WaitCount || f.cfgReads.Load() != 0 || f.statuses.Load() != 0 {
			t.Fatal("invalid caller reached original SQL/config/provider", outcome)
		}
	})
	t.Run("nil-publication-no-slot-or-io", func(t *testing.T) {
		s, db, _ := preparedImporter(t, true)
		if !s.SABPublicationSourceReady() {
			t.Fatal("actual legacy publication source is not ready")
		}
		db.Connection().SetMaxOpenConns(1)
		holder, err := db.Connection().Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		before := db.Connection().Stats().WaitCount
		root, target := t.TempDir(), t.TempDir()
		policy := config.SABPublicationPolicyValues{StageRoot: root, MaxBytes: 1 << 20, SourceFingerprint: strings.Repeat("a", 64)}
		done := make(chan struct{})
		var actor *SABStagedPublication
		var operationErr error
		t.Cleanup(func() {
			_ = holder.Close()
			_ = s.Close()
			preparedImportWait(t, done)
			if actor != nil {
				actor.Close()
			}
		})
		go func() {
			defer close(done)
			actor, operationErr = s.OpenSABStagedPublication(nil, 1, policy, target, []int{1})
		}()
		preparedImportWait(t, done)
		if actor != nil || operationErr == nil || before != db.Connection().Stats().WaitCount {
			t.Fatal("nil publication admitted original work", operationErr)
		}
		originalImporterNoAdmission(t, s)
	})
	t.Run("nil-cache-and-immediate-no-file-work", func(t *testing.T) {
		s, raw, path := originalImporterCache(t)
		before, err := os.ReadFile(s.resolvedCache.path)
		if err != nil {
			t.Fatal(err)
		}
		tmp := t.TempDir()
		t.Setenv("TMPDIR", tmp)
		t.Setenv("TMP", tmp)
		t.Setenv("TEMP", tmp)
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		for _, invalid := range []context.Context{nil, cancelled} {
			if _, err := s.AddNZBToQueue(invalid, "synthetic.nzb", raw); err == nil {
				t.Fatal("nil enqueue created file")
			}
			if _, err := s.ProcessNZBImmediately(invalid, "synthetic.nzb", raw); err == nil {
				t.Fatal("nil immediate import reused cache")
			}
			if _, _, err := s.FindResolvedNZBByDownloadURL(invalid, "https://synthetic.invalid/original.nzb"); err == nil {
				t.Fatal("nil cache lookup admitted")
			}
			if _, err := s.ListResolvedNZBs(invalid, "", 1, 10); err == nil {
				t.Fatal("nil cache list admitted")
			}
			if s.HasResolvedNZB(invalid, raw) || s.ForgetResolvedNZB(invalid, raw) == nil || s.DeleteResolvedNZB(invalid, resolvedNZBHash(raw)) == nil {
				t.Fatal("invalid cache mutation accepted")
			}
		}
		after, err := os.ReadFile(s.resolvedCache.path)
		entries, dirErr := os.ReadDir(tmp)
		if err != nil || dirErr != nil || string(before) != string(after) || len(entries) != 0 || !s.resolvedCache.metadataService.FileExists(path) {
			t.Fatal("invalid caller touched actual cache or import files", err, dirErr)
		}
		if s.ctx != nil || s.cancel != nil {
			t.Fatal("invalid caller inferred legacy runtime")
		}
	})
	t.Run("invalid-caller-before-legacy-life-inference", func(t *testing.T) {
		s := &Service{}
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		for _, caller := range []context.Context{nil, cancelled} {
			if _, finish, err := s.beginSourceWork(caller, true); err == nil {
				if finish != nil {
					finish()
				}
				t.Fatal("missing caller admitted")
			}
			if s.ctx != nil || s.cancel != nil || s.jobs != 0 {
				t.Fatal("invalid caller initialized original legacy lifetime")
			}
		}
	})
	t.Run("prepared-never-infers-original", func(t *testing.T) {
		s, _, cancel := preparedImporter(t, false)
		original := s.ctx
		for _, attempt := range []func() (context.Context, func(), error){func() (context.Context, func(), error) { return s.beginSourceWork(t.Context(), true) }, func() (context.Context, func(), error) { return s.beginRuntimeWork(true) }} {
			if _, finish, err := attempt(); err == nil {
				finish()
				t.Fatal("inactive prepared service admitted")
			}
		}
		cancel()
		if _, finish, err := s.beginRuntimeWork(false); err == nil {
			finish()
			t.Fatal("retired original context replaced")
		}
		if s.ctx != original || s.active {
			t.Fatal("prepared lifetime inferred or activated")
		}
		// Controlled negative fixture: retain the genuine constructor/self, but
		// remove its already-cancelled original root. Never admit replacement work.
		s.mu.Lock()
		s.ctx, s.cancel = nil, nil
		s.mu.Unlock()
		if _, finish, err := s.beginSourceWork(t.Context(), true); err == nil {
			finish()
			t.Fatal("missing prepared original inferred by explicit caller")
		}
		if _, finish, err := s.beginRuntimeWork(true); err == nil {
			finish()
			t.Fatal("missing prepared original inferred by runtime path")
		}
		if s.ctx != nil || s.cancel != nil {
			t.Fatal("missing prepared original replaced")
		}
		originalImporterNoAdmission(t, s)
	})
}

// Observe the actual operation goroutine, not a periodic queue worker.
func originalImporterOwnedSQL(t *testing.T, s *Service, background bool, expected string) string {
	operation, repository := "(*Service).GetQueueStats", "(*QueueRepository).GetQueueStatsContext"
	if background {
		operation, repository = "ProcessItemInBackground.func1", "(*QueueRepository).GetQueueItemContext"
	}
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for {
		buffer := make([]byte, 512<<10)
		n := runtime.Stack(buffer, true)
		if n == len(buffer) {
			t.Fatal("bounded actual goroutine snapshot truncated")
		}
		for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
			if !strings.Contains(stack, operation) || !strings.Contains(stack, repository) || !strings.Contains(stack, "database/sql.(*DB).conn") {
				continue
			}
			header := strings.SplitN(stack, "\n", 2)[0]
			fields := strings.Fields(header)
			if len(fields) < 3 || !strings.Contains(header, "[select]") || expected != "" && fields[1] != expected {
				continue
			}
			s.mu.RLock()
			jobs := s.jobs
			s.mu.RUnlock()
			if !background || jobs == 1 {
				return fields[1]
			}
		}
		if time.Now().After(until) {
			t.Fatal("actual accepted background SQL wait/charge not observed")
		}
		time.Sleep(time.Millisecond)
	}
}

func originalImporterHeldSQL(t *testing.T, background bool, cancelCaller bool) {
	t.Helper()
	s, db, stopOriginal := preparedImporter(t, false)
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	db.Connection().SetMaxOpenConns(1)
	holder, err := db.Connection().Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	caller, stopCaller := context.WithCancel(t.Context())
	before := db.Connection().Stats().WaitCount
	done := make(chan struct{})
	var operationErr error
	t.Cleanup(func() { stopCaller(); stopOriginal(); _ = holder.Close(); _ = s.Close(); preparedImportWait(t, done) })
	if background {
		s.ProcessItemInBackground(caller, 987654)
		go func() { s.wg.Wait(); close(done) }()
	} else {
		go func() { defer close(done); _, operationErr = s.GetQueueStats(caller) }()
	}
	preparedImportSQLWait(t, db.Connection(), before)
	if background {
		actualID := originalImporterOwnedSQL(t, s, true, "")
		stopCaller()
		originalImporterOwnedSQL(t, s, true, actualID)
		preparedImportPending(t, done)
		if s.ctx.Err() != nil {
			t.Fatal("request cancellation retired accepted runtime work")
		}
		stopOriginal()
	} else {
		originalImporterOwnedSQL(t, s, false, "")
		if cancelCaller {
			stopCaller()
		} else {
			stopOriginal()
		}
	}
	preparedImportWait(t, done)
	if !background && operationErr == nil {
		t.Fatal("cancelled real SQL work succeeded")
	}
	s.mu.RLock()
	closed := s.closed
	jobs := s.jobs
	s.mu.RUnlock()
	if closed || jobs != 0 {
		t.Fatal("work ended only through Close or leaked its charge")
	}
}

func TestImporterPrivateRuntimeWorkKeepsOriginalService(t *testing.T) {
	t.Run("legacy-manual-scan-original-lifetime", func(t *testing.T) {
		s, db, _ := preparedImporter(t, true)
		// Legacy construction is active without starting queue workers.
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "synthetic.nzb"), []byte(`<nzb></nzb>`), 0600); err != nil {
			t.Fatal(err)
		}
		db.Connection().SetMaxOpenConns(1)
		holder, err := db.Connection().Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		before := db.Connection().Stats().WaitCount
		done := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			s.cancel()
			_ = holder.Close()
			_ = s.Close()
			if launched {
				preparedImportWait(t, done)
			}
		})
		if err := s.StartManualScan(dir); err != nil {
			t.Fatal(err)
		}
		launched = true
		go func() { s.wg.Wait(); close(done) }()
		preparedImportSQLWait(t, db.Connection(), before)
		s.cancel()
		preparedImportWait(t, done)
		if s.closed || s.GetScanStatus().Status != ScanStatusIdle {
			t.Fatal("legacy scan did not follow actual original runtime")
		}
	})
	t.Run("adopted-background-work-original-lifetime", func(t *testing.T) { originalImporterHeldSQL(t, true, false) })
	t.Run("internal-fallback-original-lifetime", func(t *testing.T) {
		f := newSABFallbackFixture(t)
		entered, returned, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		f.setCallback(func(w http.ResponseWriter, r *http.Request) {
			defer close(returned)
			_, _ = io.WriteString(w, `{"status":`)
			w.(http.Flusher).Flush()
			close(entered)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		done := make(chan struct{})
		var operationErr error
		t.Cleanup(func() {
			close(release)
			if f.service.cancel != nil {
				f.service.cancel()
			}
			_ = f.service.Close()
			preparedImportWait(t, done)
		})
		go func() { defer close(done); operationErr = f.service.submitSABFallback(f.item) }()
		preparedImportWait(t, entered)
		f.service.mu.RLock()
		stop := f.service.cancel
		f.service.mu.RUnlock()
		if stop == nil {
			t.Fatal("legacy original runtime not retained")
		}
		stop()
		preparedImportWait(t, done)
		preparedImportWait(t, returned)
		if operationErr == nil || f.submits.Load() != 1 || len(f.service.fallbackSlots) != 0 {
			t.Fatal("held original HTTP work not cancelled/joined", operationErr)
		}
		_, _, receipt, err := f.service.fallbackItem(t.Context(), f.item.ID)
		if err != nil || receipt == nil || receipt.Status.State != "submission_unknown" {
			t.Fatal("lost acknowledgement falsely rolled back durable marker", err)
		}
	})
	t.Run("valid-explicit-caller-keeps-original-cancellation", func(t *testing.T) { originalImporterHeldSQL(t, false, true); originalImporterHeldSQL(t, false, false) })
	t.Run("closed-runtime-no-readmit", func(t *testing.T) {
		f := newSABFallbackFixture(t)
		if err := f.service.Close(); err != nil {
			t.Fatal(err)
		}
		before := f.cfgReads.Load()
		if f.service.StartManualScan(t.TempDir()) == nil || f.service.submitSABFallback(f.item) == nil {
			t.Fatal("closed runtime re-admitted")
		}
		f.service.ProcessItemInBackground(t.Context(), f.item.ID)
		if _, err := f.service.CalculateFileSizeOnly(f.item.NzbPath); err == nil {
			t.Fatal("closed size work re-admitted")
		}
		if _, err := f.service.AddToQueue(f.item.NzbPath, nil, nil, nil); err == nil {
			t.Fatal("closed queue work re-admitted")
		}
		if before != f.cfgReads.Load() || f.submits.Load() != 0 {
			t.Fatal("closed runtime performed config/provider work")
		}
		originalImporterNoAdmission(t, f.service)
	})
	t.Run("legacy-valid-explicit-cache-context-compatible", func(t *testing.T) {
		s, raw, path := originalImporterCache(t)
		if !s.HasResolvedNZB(t.Context(), raw) || s.ctx == nil || s.cancel == nil {
			t.Fatal("valid explicit legacy cache compatibility lost")
		}
		if got, err := s.ProcessNZBImmediately(t.Context(), "synthetic.nzb", raw); err != nil || got != path {
			t.Fatal("actual cache hit unexpectedly processed source", err)
		}
		// Both historical APIs intentionally lack a context argument. Exercise
		// real size parsing and the real queue INSERT, not a fake processor.
		q, db, _ := preparedImporter(t, true)
		file := filepath.Join(t.TempDir(), "synthetic.strm")
		if err := os.WriteFile(file, []byte("nxglnk://synthetic?file_size=1234\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if size, err := q.CalculateFileSizeOnly(file); err != nil || size != 1234 {
			t.Fatal("historical size API lost original runtime", err)
		}
		item, err := q.AddToQueue(file, nil, nil, nil)
		if err != nil || item == nil || item.FileSize == nil || *item.FileSize != 1234 {
			t.Fatal("historical queue API lost actual original work", err)
		}
		stored, err := db.Repository.GetQueueItem(item.ID)
		if err != nil || stored == nil || stored.NzbPath != file || stored.Status != database.QueueStatusPending {
			t.Fatal("actual queue insertion missing", err)
		}
		if _, err := q.addToQueue(nil, file, nil, nil, nil); err == nil {
			t.Fatal("explicit missing caller adopted runtime")
		}
		if _, err := q.calculateFileSizeContext(nil, file); err == nil {
			t.Fatal("explicit missing size caller adopted runtime")
		}
		originalImporterNoAdmission(t, q)
	})
}
