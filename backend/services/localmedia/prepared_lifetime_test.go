//go:build linux

package localmedia

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"novastream/internal/datastore"
	"novastream/models"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func preparedLocalPG(t *testing.T) (*datastore.DataStore, *pgxpool.Pool, string) {
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
	name := fmt.Sprintf("synthetic_prepared_local_%d", time.Now().UnixNano())
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

type preparedMetadataGate struct {
	url          string
	client       *http.Client
	entered      chan struct{}
	received     chan struct{}
	closeEntered chan struct{}
	release      chan struct{}
	closeOnce    sync.Once
	hold         bool
}
type preparedMetadataTransport struct {
	actual http.RoundTripper
	gate   *preparedMetadataGate
}

func (t preparedMetadataTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, e := t.actual.RoundTrip(r)
	if response != nil && response.Body != nil {
		response.Body = &preparedMetadataBody{ReadCloser: response.Body, gate: t.gate}
	}
	return response, e
}

type preparedMetadataBody struct {
	io.ReadCloser
	gate *preparedMetadataGate
	once sync.Once
}

func (b *preparedMetadataBody) Close() error {
	e := b.ReadCloser.Close()
	b.once.Do(func() { b.gate.closeEntered <- struct{}{} })
	if b.gate.hold {
		<-b.gate.release
	}
	return e
}
func preparedMetadata(t *testing.T, hold bool) *preparedMetadataGate {
	t.Helper()
	g := &preparedMetadataGate{entered: make(chan struct{}, 16), received: make(chan struct{}, 16), closeEntered: make(chan struct{}, 16), release: make(chan struct{}), hold: hold}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_ = http.NewResponseController(w).Flush()
		g.entered <- struct{}{}
		<-r.Context().Done()
	}))
	transport := &http.Transport{Proxy: nil}
	g.url = server.URL
	g.client = &http.Client{Transport: preparedMetadataTransport{actual: transport, gate: g}}
	t.Cleanup(func() { g.closeOnce.Do(func() { close(g.release) }); transport.CloseIdleConnections(); server.Close() })
	return g
}
func (g *preparedMetadataGate) request(ctx context.Context) error {
	r, e := http.NewRequestWithContext(ctx, http.MethodGet, g.url, nil)
	if e != nil {
		return e
	}
	response, e := g.client.Do(r)
	if e != nil {
		return e
	}
	if response == nil || response.Body == nil {
		return fmt.Errorf("actual metadata response body unavailable")
	}
	defer response.Body.Close()
	g.received <- struct{}{}
	_, e = io.ReadAll(response.Body)
	return e
}
func (g *preparedMetadataGate) Search(ctx context.Context, _, _ string) ([]models.SearchResult, error) {
	return nil, g.request(ctx)
}
func (g *preparedMetadataGate) MovieDetails(ctx context.Context, _ models.MovieDetailsQuery) (*models.Title, error) {
	return nil, g.request(ctx)
}
func (g *preparedMetadataGate) SeriesDetails(ctx context.Context, _ models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	return nil, g.request(ctx)
}
func preparedLocalWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("actual local work did not join")
	}
}
func preparedLocalSignal(t *testing.T, signals <-chan struct{}) {
	t.Helper()
	select {
	case <-signals:
	case <-time.After(8 * time.Second):
		t.Fatal("actual local I/O phase not reached")
	}
}
func TestPreparedLocalMediaLifecycle(t *testing.T) {
	ds, _, _ := preparedLocalPG(t)
	library := func(t *testing.T, id string, scanning bool) *models.LocalMediaLibrary {
		t.Helper()
		root := t.TempDir()
		if e := os.WriteFile(filepath.Join(root, "Synthetic.Movie.2026.mkv"), []byte("synthetic source"), 0600); e != nil {
			t.Fatal(e)
		}
		status := models.LocalMediaScanStatusIdle
		if scanning {
			status = models.LocalMediaScanStatusScanning
		}
		value := &models.LocalMediaLibrary{ID: id, Name: "Synthetic library", Type: models.LocalMediaLibraryTypeMovie, RootPath: root, LastScanStatus: status, CreatedAt: time.Now(), UpdatedAt: time.Now()}
		if e := ds.LocalMedia().CreateLibrary(t.Context(), value); e != nil {
			t.Fatal(e)
		}
		return value
	}
	fresh := func(t *testing.T, matcher metadataMatcher) (*Service, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		s, e := PrepareService(ctx, ds, matcher, "ffprobe")
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		t.Cleanup(func() { cancel(); _ = s.Close() })
		return s, cancel
	}
	status := func(t *testing.T, id string) models.LocalMediaScanStatus {
		t.Helper()
		value, e := ds.LocalMedia().GetLibrary(t.Context(), id)
		if e != nil || value == nil {
			t.Fatal(e)
		}
		return value.LastScanStatus
	}
	t.Run("prepare-no-reconciliation-write", func(t *testing.T) {
		value := library(t, "prepare", true)
		_, _ = fresh(t, nil)
		if status(t, value.ID) != models.LocalMediaScanStatusScanning {
			t.Fatal("preparation reconciled durable state")
		}
	})
	t.Run("prepared-scan-denied", func(t *testing.T) {
		s, _ := fresh(t, nil)
		if _, e := s.StartScan(t.Context(), "prepare"); e != ErrServiceUnavailable {
			t.Fatal("inactive scan admitted", e)
		}
	})
	t.Run("prepared-mutation-denied", func(t *testing.T) {
		s, _ := fresh(t, nil)
		if e := s.DeleteLibrary(t.Context(), "prepare"); e != ErrServiceUnavailable {
			t.Fatal("inactive mutation admitted", e)
		}
		if _, e := s.ListLibraries(t.Context()); e != ErrServiceUnavailable {
			t.Fatal("inactive catalogue queried", e)
		}
	})
	t.Run("cancelled-activation-no-write", func(t *testing.T) {
		value := library(t, "cancelled", true)
		s, cancel := fresh(t, nil)
		cancel()
		if s.Activate(t.Context()) == nil || status(t, value.ID) != models.LocalMediaScanStatusScanning {
			t.Fatal("cancelled activation reconciled state")
		}
	})
	t.Run("actual-reconciliation-after-activate", func(t *testing.T) {
		value := library(t, "activate", true)
		s, _ := fresh(t, nil)
		if e := s.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		if status(t, value.ID) != models.LocalMediaScanStatusFailed {
			t.Fatal("activation did not reconcile original stale row")
		}
	})
	t.Run("reconciliation-failure-remains-inactive", func(t *testing.T) {
		library(t, "failure", true)
		if _, e := ds.Pool().Exec(t.Context(), `ALTER TABLE local_media_libraries ADD CONSTRAINT synthetic_no_failed CHECK(last_scan_status<>'failed') NOT VALID`); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			_, e := ds.Pool().Exec(context.Background(), `ALTER TABLE local_media_libraries DROP CONSTRAINT synthetic_no_failed`)
			if e != nil {
				t.Error(e)
			}
		})
		s, _ := fresh(t, nil)
		if s.Activate(t.Context()) == nil {
			t.Fatal("failed reconciliation activated")
		}
		if _, e := s.StartScan(t.Context(), "failure"); e != ErrServiceUnavailable {
			t.Fatal("failed activation admitted scan", e)
		}
	})
	scan := func(t *testing.T, synchronous, hold bool) {
		t.Helper()
		g := preparedMetadata(t, hold)
		value := library(t, fmt.Sprintf("scan-%d", time.Now().UnixNano()), false)
		s, cancel := fresh(t, g)
		if e := s.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		caller, stopCaller := context.WithCancel(t.Context())
		done := make(chan struct{})
		closed := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			stopCaller()
			cancel()
			g.closeOnce.Do(func() { close(g.release) })
			if synchronous {
				preparedLocalWait(t, done)
			}
			if launched {
				preparedLocalWait(t, closed)
			}
		})
		if synchronous {
			go func() { _, _ = s.RunScan(caller, value.ID); close(done) }()
		} else {
			if _, e := s.StartScan(caller, value.ID); e != nil {
				t.Fatal(e)
			}
		}
		preparedLocalSignal(t, g.entered)
		preparedLocalSignal(t, g.received)
		if synchronous {
			stopCaller()
			preparedLocalSignal(t, g.closeEntered)
			preparedLocalWait(t, done)
			if s.life.Err() != nil {
				t.Fatal("caller cancellation retired the original service instead of only its scan")
			}
		} else {
			stopCaller()
			select {
			case <-g.closeEntered:
				t.Fatal("accepted background scan inherited retired setup caller")
			case <-time.After(20 * time.Millisecond):
			}
		}
		launched = true
		go func() { _ = s.Close(); close(closed) }()
		if hold {
			preparedLocalSignal(t, g.closeEntered)
			select {
			case <-closed:
				t.Fatal("Close refunded held actual metadata cleanup")
			case <-time.After(20 * time.Millisecond):
			}
			g.closeOnce.Do(func() { close(g.release) })
		}
		preparedLocalWait(t, closed)
		if synchronous {
			preparedLocalWait(t, done)
		}
	}
	t.Run("background-scan-original-runtime", func(t *testing.T) { scan(t, false, false) })
	t.Run("synchronous-scan-keeps-caller-cancel", func(t *testing.T) { scan(t, true, false) })
	t.Run("held-metadata-close-joins", func(t *testing.T) { scan(t, false, true) })
	t.Run("actual-probe-child-close-joins", func(t *testing.T) {
		tool, e := exec.LookPath("ffprobe")
		if e != nil {
			t.Fatal("actual pinned ffprobe required", e)
		}
		s, cancel := fresh(t, nil)
		s.ffprobePath = tool
		if e = s.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(t.TempDir(), "synthetic.mkv")
		if e = syscall.Mkfifo(path, 0600); e != nil {
			t.Fatal(e)
		}
		writer, e := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0600)
		if e != nil {
			t.Fatal(e)
		}
		info, e := writer.Stat()
		if e != nil {
			_ = writer.Close()
			t.Fatal(e)
		}
		done := make(chan struct{})
		t.Cleanup(func() { cancel(); _ = writer.Close(); preparedLocalWait(t, done) })
		go func() {
			_, _ = s.ProbeItemForPlayback(t.Context(), &models.LocalMediaItem{FilePath: path})
			close(done)
		}()
		child := 0
		deadline := time.Now().Add(8 * time.Second)
		for child == 0 {
			tasks, _ := os.ReadDir("/proc/self/task")
			for _, task := range tasks {
				raw, _ := os.ReadFile(filepath.Join("/proc/self/task", task.Name(), "children"))
				for _, pid := range strings.Fields(string(raw)) {
					fds, _ := os.ReadDir(filepath.Join("/proc", pid, "fd"))
					for _, fd := range fds {
						actual, e := os.Stat(filepath.Join("/proc", pid, "fd", fd.Name()))
						if e == nil && os.SameFile(info, actual) {
							child, _ = strconv.Atoi(pid)
						}
					}
				}
			}
			if child != 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("actual ffprobe input descriptor was not observed")
			}
			time.Sleep(time.Millisecond)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		preparedLocalWait(t, done)
		if _, e = os.Stat(filepath.Join("/proc", strconv.Itoa(child))); !os.IsNotExist(e) {
			t.Fatal("Close returned before actual probe child Wait", e)
		}
	})
	t.Run("bounded-concurrent-library-scans", func(t *testing.T) {
		g := preparedMetadata(t, false)
		s, cancel := fresh(t, g)
		if e := s.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(cancel)
		for n := 0; n < 4; n++ {
			value := library(t, fmt.Sprintf("bounded-%d", n), false)
			if _, e := s.StartScan(t.Context(), value.ID); e != nil {
				t.Fatal(e)
			}
			preparedLocalSignal(t, g.entered)
		}
		value := library(t, "bounded-overflow", false)
		if _, e := s.StartScan(t.Context(), value.ID); e != ErrServiceUnavailable {
			t.Fatal("fifth actual scan admitted", e)
		}
		_ = s.Close()
	})
	t.Run("unstarted-close-cancels", func(t *testing.T) {
		s, _ := fresh(t, nil)
		_ = s.Close()
		if s.life.Err() == nil || s.Activate(t.Context()) == nil {
			t.Fatal("unstarted local runtime survived Close")
		}
	})
	t.Run("closed-no-restart", func(t *testing.T) {
		s, _ := fresh(t, nil)
		if e := s.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		_ = s.Close()
		if s.Activate(t.Context()) == nil {
			t.Fatal("closed local runtime restarted")
		}
		if _, e := s.BrowseDirectories(""); e != ErrServiceUnavailable {
			t.Fatal("closed local runtime opened directory", e)
		}
	})
	t.Run("legacy-constructor-reconciles", func(t *testing.T) {
		value := library(t, "legacy", true)
		s, e := NewService(ds, nil, "ffprobe")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = s.Close() })
		if status(t, value.ID) != models.LocalMediaScanStatusFailed {
			t.Fatal("legacy constructor no longer reconciles")
		}
	})
}
