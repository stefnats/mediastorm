package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/database"
)

type sabFallbackFixture struct {
	service           *Service
	db                *database.DB
	item              *database.ImportQueueItem
	path              string
	mu                sync.RWMutex
	cfg               config.AltMountConfig
	submits, statuses atomic.Int32
	cfgReads          atomic.Int32
	callback          atomic.Pointer[sabFallbackCallback]
	status            atomic.Value
}

type sabFallbackCallback struct {
	call func(http.ResponseWriter, *http.Request)
}

func (f *sabFallbackFixture) setCallback(fn func(http.ResponseWriter, *http.Request)) {
	f.callback.Store(&sabFallbackCallback{call: fn})
}
func newSABFallbackFixture(t *testing.T) *sabFallbackFixture {
	t.Helper()
	f := &sabFallbackFixture{}
	f.status.Store("Downloading")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Error("wrong SAB API path")
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("mode") {
		case "addfile":
			f.submits.Add(1)
			if r.Method != "POST" {
				t.Error("not multipart POST")
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			} else {
				file, header, err := r.FormFile("nzbfile")
				if err != nil {
					t.Error(err)
				} else {
					b, _ := io.ReadAll(file)
					file.Close()
					if string(b) != "<nzb>synthetic fallback payload</nzb>" || header.Filename != filepath.Base(f.item.NzbPath) {
						t.Error("wrong original NZB payload")
					}
				}
			}
			if cb := f.callback.Load(); cb != nil {
				cb.call(w, r)
				return
			}
			fmt.Fprint(w, `{"status":true,"nzo_ids":["own_job_1"]}`)
		case "queue":
			if r.URL.Query().Get("nzo_ids") != "own_job_1" {
				t.Error("status targeted another job")
			}
			f.statuses.Add(1)
			if cb := f.callback.Load(); cb != nil {
				cb.call(w, r)
				return
			}
			if f.status.Load().(string) == "Completed" {
				fmt.Fprint(w, `{"queue":{"slots":[]}}`)
				return
			}
			fmt.Fprintf(w, `{"queue":{"slots":[{"nzo_id":"own_job_1","status":%q,"filename":"PRIVATE_RELEASE","fail_message":"PRIVATE_ERROR","storage":"/PRIVATE/PATH"}]}}`, f.status.Load().(string))
		case "history":
			if r.URL.Query().Get("nzo_ids") != "own_job_1" {
				t.Error("history targeted another job")
			}
			f.statuses.Add(1)
			if cb := f.callback.Load(); cb != nil {
				cb.call(w, r)
				return
			}
			fmt.Fprint(w, `{"history":{"slots":[{"nzo_id":"own_job_1","status":"Completed","storage":"/PRIVATE/COMPLETED","fail_message":"PRIVATE_PROVIDER_ERROR"}]}}`)
		default:
			t.Error("unexpected SAB operation")
		}
	}))
	t.Cleanup(server.Close)
	enabled := true
	f.cfg.SABnzbd = config.SABnzbdConfig{Enabled: &enabled, FallbackHost: server.URL, FallbackAPIKey: "synthetic_api_key"}
	f.path = filepath.Join(t.TempDir(), "queue.db")
	f.open(t)
	nzb := filepath.Join(t.TempDir(), "original.nzb")
	if err := os.WriteFile(nzb, []byte("<nzb>synthetic fallback payload</nzb>"), 0600); err != nil {
		t.Fatal(err)
	}
	metadata := `{"foreign":{"keep":[1,"value"]},"titleId":"saved-id"}`
	f.item = &database.ImportQueueItem{NzbPath: nzb, Status: database.QueueStatusFailed, Metadata: &metadata, MaxRetries: 0, Priority: database.QueuePriorityNormal}
	if err := f.db.Repository.AddToQueue(f.item); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *sabFallbackFixture) open(t *testing.T) {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: f.path})
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() { db.Close() })
	ctx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	f.service = &Service{database: db, ctx: ctx, log: slog.New(slog.NewTextHandler(io.Discard, nil)), fallbackSlots: make(chan struct{}, 2), configGetter: func() *config.AltMountConfig {
		f.mu.RLock()
		defer f.mu.RUnlock()
		copy := f.cfg
		f.cfgReads.Add(1)
		return &copy
	}}
}
func (f *sabFallbackFixture) fail() {
	f.service.handleProcessingFailure(f.item, NewNonRetryableError("synthetic final archive failure", nil), f.service.log)
}
func assertSABSafe(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"synthetic_api_key", "PRIVATE_", "http://", "https://", "outputPath", "storagePath", "stream_ready", "imported"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("private/provider/ready value leaked: %s", secret)
		}
	}
}
func TestSABFallbackActualSubmissionStatusReopenAndCompletion(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	if f.submits.Load() != 1 {
		t.Fatal("existing terminal error did not submit once")
	}
	stored, err := f.db.Repository.GetQueueItem(f.item.ID)
	if err != nil || stored.Status != database.QueueStatusFailed {
		t.Fatal("original failed import became completed")
	}
	before, _ := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
	if before.State != "submitted" || before.SubmittedAt == nil || f.statuses.Load() != 0 {
		t.Fatal(before)
	}
	f.db.Close()
	f.open(t)
	f.fail()
	if f.submits.Load() != 1 {
		t.Fatal("restart resubmitted")
	}
	status, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID)
	if err != nil || status.State != "downloading" || status.CheckedAt == nil {
		t.Fatalf("status %+v %v", status, err)
	}
	assertSABSafe(t, status)
	f.status.Store("Completed")
	completed, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID)
	if err != nil || completed.State != "download_completed" {
		t.Fatalf("completion %+v %v", completed, err)
	}
	assertSABSafe(t, completed)
	count := f.statuses.Load()
	_, err = f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
	if err != nil || f.statuses.Load() != count {
		t.Fatal("GET performed HTTP")
	}
	stored, _ = f.db.Repository.GetQueueItem(f.item.ID)
	var all map[string]json.RawMessage
	if json.Unmarshal([]byte(*stored.Metadata), &all) != nil || string(all["foreign"]) != `{"keep":[1,"value"]}` || string(all["titleId"]) != `"saved-id"` {
		t.Fatal("foreign metadata erased")
	}
	assertSABSafe(t, all)
}
func TestSABFallbackConcurrentAndLostAcknowledgementNeverResubmit(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			f := newSABFallbackFixture(t)
			if lost {
				f.setCallback(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "PRIVATE_ACK_FAILURE", 502) })
			}
			var wg sync.WaitGroup
			for n := 0; n < 16; n++ {
				wg.Add(1)
				go func() { defer wg.Done(); f.fail() }()
			}
			wg.Wait()
			if f.submits.Load() != 1 {
				t.Fatalf("submits%d", f.submits.Load())
			}
			status, err := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
			if err != nil {
				t.Fatal(err)
			}
			if lost && status.State != "submission_unknown" {
				t.Fatal(status)
			}
			f.db.Close()
			f.open(t)
			f.fail()
			if f.submits.Load() != 1 {
				t.Fatal("unknown receipt resubmitted after reopen")
			}
			if lost {
				_, err = f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID)
				if err != nil || f.statuses.Load() != 0 {
					t.Fatal("unknown acknowledgement invented job/status")
				}
			}
		})
	}
}
func TestSABFallbackAllMetadataWritersPreserveReceipt(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	for _, mode := range []string{"update", "add", "batch"} {
		t.Run(mode, func(t *testing.T) {
			meta := `{"foreign2":{"nested":true}}`
			switch mode {
			case "update":
				if err := f.db.Repository.UpdateMetadata(f.item.ID, &meta); err != nil {
					t.Fatal(err)
				}
			case "add":
				copy := *f.item
				copy.Metadata = &meta
				if err := f.db.Repository.AddToQueue(&copy); err != nil {
					t.Fatal(err)
				}
			case "batch":
				copy := *f.item
				copy.Metadata = &meta
				if err := f.db.Repository.AddBatchToQueue([]*database.ImportQueueItem{&copy}); err != nil {
					t.Fatal(err)
				}
			}
			status, err := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
			if err != nil || status.State != "submitted" {
				t.Fatalf("receipt erased %+v %v", status, err)
			}
			f.fail()
			if f.submits.Load() != 1 {
				t.Fatal("metadata replacement permitted duplicate submission")
			}
		})
	}
}
func TestSABFallbackConfigChangesBlockBeforeAndAfterHTTP(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	f.mu.Lock()
	original := f.cfg.SABnzbd
	f.cfg.SABnzbd.FallbackAPIKey = "changed"
	f.mu.Unlock()
	if _, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID); !errors.Is(err, ErrSABFallbackSourceChanged) || f.statuses.Load() != 0 {
		t.Fatal("new credentials used on old job")
	}
	f.mu.Lock()
	f.cfg.SABnzbd = original
	f.mu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	f.setCallback(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprint(w, `{"queue":{"slots":[{"nzo_id":"own_job_1","status":"Completed"}]}}`)
	})
	done := make(chan error, 1)
	go func() { _, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID); done <- err }()
	<-entered
	f.mu.Lock()
	f.cfg.SABnzbd.FallbackAPIKey = "changed-again"
	f.mu.Unlock()
	close(release)
	if err := <-done; !errors.Is(err, ErrSABFallbackSourceChanged) {
		t.Fatal("late changed-source observation published")
	}
	stored, _ := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
	if stored.State != "submitted" || stored.CheckedAt != nil {
		t.Fatal(stored)
	}
}
func TestSABFallbackExplicitRefreshBusyCancellationAndSafeError(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	entered, closed := make(chan struct{}), make(chan struct{})
	f.setCallback(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(closed) })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := f.service.RefreshSABFallbackStatus(ctx, f.item.ID); done <- err }()
	<-entered
	if _, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID); !errors.Is(err, ErrSABFallbackBusy) {
		t.Fatal("parallel same-job HTTP allowed")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("HTTP not cancelled")
	}
	status, _ := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
	if status.CheckedAt != nil || status.State != "submitted" {
		t.Fatal("cancel invented fresh observation")
	}
}
func TestSABFallbackDisabledMalformedAndFailedStatus(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.mu.Lock()
	disabled := false
	f.cfg.SABnzbd.Enabled = &disabled
	f.mu.Unlock()
	f.fail()
	if f.submits.Load() != 0 {
		t.Fatal("optout submitted")
	}
	status, _ := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
	if status.State != "not_submitted" {
		t.Fatal(status)
	}
	f.mu.Lock()
	enabled := true
	f.cfg.SABnzbd.Enabled = &enabled
	f.mu.Unlock()
	f.fail()
	f.status.Store("Failed")
	status, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID)
	if err != nil || status.State != "failed" {
		t.Fatalf("failed observation %+v %v", status, err)
	}
	assertSABSafe(t, status)
	meta := `{"_sabFallbackV1":{"version":1,"source":"bad"}}`
	if _, err := f.db.Connection().Exec("UPDATE import_queue SET metadata=? WHERE id=?", meta, f.item.ID); err != nil {
		t.Fatal(err)
	}
	f.fail()
	if f.submits.Load() != 1 {
		t.Fatal("malformed receipt caused resubmit")
	}
	if _, err := f.service.GetSABFallbackStatus(t.Context(), f.item.ID); err == nil {
		t.Fatal("malformed persisted receipt accepted")
	}
}

func TestSABFallbackRejectsNonregularOversizedAndCancelledInputs(t *testing.T) {
	for _, kind := range []string{"fifo", "symlink", "oversized", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			f := newSABFallbackFixture(t)
			switch kind {
			case "fifo":
				if err := os.Remove(f.item.NzbPath); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(f.item.NzbPath, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				if err := os.Rename(f.item.NzbPath, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, f.item.NzbPath); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.Truncate(f.item.NzbPath, fallbackNZBMaxBytes+1); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				f.service.ctx = ctx
			}
			done := make(chan struct{})
			go func() { f.fail(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("invalid input blocked")
			}
			if f.submits.Load() != 0 {
				t.Fatal("invalid input submitted")
			}
		})
	}
}
func TestSABFallbackForeignOrMalformedProviderStatusIsUnknown(t *testing.T) {
	for _, kind := range []string{"foreign", "malformed", "http_error"} {
		t.Run(kind, func(t *testing.T) {
			f := newSABFallbackFixture(t)
			f.fail()
			f.setCallback(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "foreign":
					fmt.Fprintf(w, `{%q:{"slots":[{"nzo_id":"foreign_job","status":"Completed"}]}}`, r.URL.Query().Get("mode"))
				case "malformed":
					fmt.Fprint(w, "{ malformed PRIVATE_PROVIDER")
				default:
					http.Error(w, "PRIVATE_PROVIDER_API_KEY", 500)
				}
			})
			status, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID)
			if err != nil || status.State != "unknown" || status.CheckedAt == nil {
				t.Fatalf("unknown status %+v %v", status, err)
			}
			assertSABSafe(t, status)
		})
	}
}

func TestSABFallbackNonobjectReAddsCannotEraseReceipt(t *testing.T) {
	for _, mode := range []string{"add", "batch"} {
		for _, incoming := range []string{`[]`, `null`, `123`, `"scalar"`, `true`} {
			t.Run(mode+incoming, func(t *testing.T) {
				f := newSABFallbackFixture(t)
				f.fail()
				copy := *f.item
				copy.Metadata = &incoming
				var err error
				if mode == "add" {
					err = f.db.Repository.AddToQueue(&copy)
				} else {
					err = f.db.Repository.AddBatchToQueue([]*database.ImportQueueItem{&copy})
				}
				if err != nil {
					t.Fatal(err)
				}
				status, err := f.service.GetSABFallbackStatus(t.Context(), f.item.ID)
				if err != nil || status.State != "submitted" {
					t.Fatalf("nonobject readd erased durable receipt: %+v %v", status, err)
				}
			})
		}
	}
}
func TestSABFallbackConfigChangeWhileRefreshCASBlocked(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			f := newSABFallbackFixture(t)
			f.fail()
			conn, err := f.db.Connection().Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
				t.Fatal(err)
			}
			before := f.cfgReads.Load()
			done := make(chan error, 1)
			go func() { _, err := f.service.RefreshSABFallbackStatus(t.Context(), f.item.ID); done <- err }()
			deadline := time.After(time.Second)
			for f.cfgReads.Load() == before {
				select {
				case <-deadline:
					t.Fatal("refresh did not capture initial source")
				case <-time.After(time.Millisecond):
				}
			}
			if change {
				f.mu.Lock()
				f.cfg.SABnzbd.FallbackAPIKey = "new-credential"
				f.mu.Unlock()
			}
			if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
				t.Fatal(err)
			}
			err = <-done
			if change {
				if !errors.Is(err, ErrSABFallbackSourceChanged) || f.statuses.Load() != 0 {
					t.Fatalf("changed source issued HTTP after blocked CAS: err%v calls%d", err, f.statuses.Load())
				}
			} else if err != nil || f.statuses.Load() != 1 {
				t.Fatalf("positive blocked CAS control %v %d", err, f.statuses.Load())
			}
		})
	}
}
