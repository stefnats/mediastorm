//go:build linux

package integration

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/javi11/nntppool/v2"
	"io"
	"log/slog"
	"net"
	"net/http"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/services/streaming"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func providerContextWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("actual provider work did not end", label)
	}
}

// The wrapper delegates actual NNTP Body. Holding its real returned worker
// isolates physical cleanup from cancellation; no cleanup acknowledgement is fake.
type providerContextPool struct {
	nntppool.UsenetConnectionPool
	release, returned chan struct{}
	seen              chan context.Context
	once              sync.Once
}

func (p *providerContextPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	select {
	case p.seen <- ctx:
	default:
	}
	n, e := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.once.Do(func() { close(p.returned) })
	<-p.release
	return n, e
}

type providerContextManager struct{ actual nntppool.UsenetConnectionPool }

func (m *providerContextManager) GetPool() (nntppool.UsenetConnectionPool, error) {
	return m.actual, nil
}
func (m *providerContextManager) SetProviders([]nntppool.UsenetProviderConfig) error {
	return errors.New("synthetic provider mutation forbidden")
}
func (m *providerContextManager) ClearPool() error {
	return errors.New("synthetic provider mutation forbidden")
}
func (m *providerContextManager) HasPool() bool             { return true }
func (m *providerContextManager) AvailableConnections() int { return 2 }

type providerContextNNTP struct {
	manager     *providerContextManager
	pool        *providerContextPool
	accepted    chan struct{}
	releaseOnce sync.Once
}

func newProviderContextNNTP(t *testing.T) *providerContextNNTP {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	f := &providerContextNNTP{accepted: make(chan struct{})}
	var mu sync.Mutex
	connections := map[net.Conn]struct{}{}
	var workers sync.WaitGroup
	var first sync.Once
	acceptedDone := make(chan struct{})
	go func() {
		defer close(acceptedDone)
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			connections[c] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(connections, c); mu.Unlock() }()
				_, _ = io.WriteString(c, "200 synthetic NNTP ready\r\n")
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					line := scanner.Text()
					switch {
					case strings.HasPrefix(line, "BODY "):
						first.Do(func() { close(f.accepted) })
						_, _ = io.WriteString(c, "222 synthetic body\r\n")
					case line == "CAPABILITIES":
						_, _ = io.WriteString(c, "101 capabilities\r\nVERSION 2\r\n.\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(c, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(c, "200 synthetic ok\r\n")
					}
				}
			}()
		}
	}()
	actual, e := nntppool.NewConnectionPool(nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 2, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 200 * time.Millisecond, HealthCheckInterval: time.Hour})
	if e != nil {
		listener.Close()
		<-acceptedDone
		workers.Wait()
		t.Fatal(e)
	}
	f.pool = &providerContextPool{UsenetConnectionPool: actual, release: make(chan struct{}), returned: make(chan struct{}), seen: make(chan context.Context, 4)}
	f.manager = &providerContextManager{actual: f.pool}
	t.Cleanup(func() {
		f.release()
		actual.Quit()
		listener.Close()
		<-acceptedDone
		mu.Lock()
		for c := range connections {
			c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return f
}
func (f *providerContextNNTP) release() { f.releaseOnce.Do(func() { close(f.pool.release) }) }

type providerContextMarker struct{}

func TestNzbProviderOriginalContext(t *testing.T) {
	for _, kind := range []string{"storage-get-original-context", "queue-get-original-context", "storage-head-original-context", "queue-head-original-context", "nil-context-denied", "missing-concrete-fs-denied", "queue-not-ready-no-reader", "missing-metadata-no-reader", "range-seek-failure-joined", "storage-caller-cancel-header-read", "queue-caller-cancel-header-read", "response-close-joins-held-worker", "cancelled-queue-sql-wait", "bounded-metadata-and-path", "late-open-context-denied"} {
		t.Run(kind, func(t *testing.T) {
			service := metadata.NewMetadataService(t.TempDir())
			meta := &metapb.FileMetadata{FileSize: 128, ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "synthetic-provider", SegmentSize: 128, EndOffset: 127}}}
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), providerContextMarker{}, "synthetic-original-provider"))
			defer cancel()
			var late atomic.Bool
			var nntp *providerContextNNTP
			var manager *providerContextManager
			if strings.Contains(kind, "header-read") || kind == "response-close-joins-held-worker" {
				nntp = newProviderContextNNTP(t)
				manager = nntp.manager
				meta.Encryption = metapb.Encryption_RCLONE
			}
			remote := nzbfilesystem.NewMetadataRemoteFile(service, nil, manager, func() *config.AltMountConfig {
				if late.Load() {
					cancel()
				}
				return &config.AltMountConfig{Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}, RClone: config.RCloneConfig{Password: "synthetic-provider-rclone"}}
			})
			fs := nzbfilesystem.NewNzbFilesystem(remote).(*nzbfilesystem.NzbFilesystem)
			system := &NzbSystem{fs: fs, nzbFs: fs}
			if e := service.WriteFileMetadata("movie.bin", meta); e != nil {
				t.Fatal(e)
			}
			request := streaming.Request{Path: "/webdav/movie.bin", Method: http.MethodGet}
			if strings.Contains(kind, "head-original-context") {
				request.Method = http.MethodHead
			}
			var queue *database.DB
			var queuePath string
			if strings.HasPrefix(kind, "queue-") || kind == "cancelled-queue-sql-wait" {
				queuePath = filepath.Join(t.TempDir(), "synthetic-queue.db")
				var e error
				queue, e = database.NewDB(database.Config{DatabasePath: queuePath})
				if e != nil {
					t.Fatal(e)
				}
				defer queue.Close()
				system.database = queue
				status := database.QueueStatusCompleted
				if kind == "queue-not-ready-no-reader" {
					status = database.QueueStatusPending
				}
				item := &database.ImportQueueItem{NzbPath: "synthetic-provider.nzb", Status: status, MaxRetries: 1}
				if e = queue.Repository.AddToQueue(item); e != nil {
					t.Fatal(e)
				}
				if e = queue.Repository.AddStoragePath(item.ID, "movie.bin"); e != nil {
					t.Fatal(e)
				}
				request.Path = fmt.Sprintf("queue/%d/movie.bin", item.ID)
			}
			switch kind {
			case "nil-context-denied":
				ctx = nil
			case "missing-concrete-fs-denied":
				system.nzbFs = nil
			case "missing-metadata-no-reader":
				request.Path = "/webdav/missing.bin"
			case "range-seek-failure-joined":
				request.RangeHeader = "bytes=2000-"
			case "bounded-metadata-and-path":
				request.Path = "/webdav/../outside.bin"
			case "late-open-context-denied":
				late.Store(true)
			case "cancelled-queue-sql-wait":
				actual, e := sql.Open("sqlite3", queuePath)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = actual.Close() })
				actual.SetMaxOpenConns(1)
				held, e := actual.Conn(t.Context())
				if e != nil {
					t.Fatal(e)
				}

				system.database = &database.DB{Repository: database.NewQueueRepository(actual)}
				done := make(chan struct{})
				var result *streaming.Response
				var failure error
				launched := false
				t.Cleanup(func() {
					cancel()
					_ = held.Close()
					if launched {
						providerContextWait(t, done, "actual queued Stream cleanup")
					}
					if result != nil {
						_ = result.Close()
					}
				})
				launched = true
				go func() { defer close(done); result, failure = system.Stream(ctx, request) }()
				until := time.Now().Add(5 * time.Second)
				for actual.Stats().WaitCount == 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if actual.Stats().WaitCount == 0 {
					t.Fatal("actual queue SQL never waited for held connection")
				}
				cancel()
				providerContextWait(t, done, "queue SQL cancellation")
				if result != nil || !errors.Is(failure, context.Canceled) {
					t.Fatal("queue lookup dropped original caller cancellation", failure)
				}
				return
			}
			response, e := system.Stream(ctx, request)
			denied := kind == "nil-context-denied" || kind == "missing-concrete-fs-denied" || kind == "missing-metadata-no-reader" || kind == "range-seek-failure-joined" || kind == "bounded-metadata-and-path" || kind == "late-open-context-denied"
			if denied {
				if e == nil || response != nil {
					if response != nil {
						response.Close()
					}
					t.Fatal("unavailable/late/unbounded source published", kind, e)
				}
				return
			}
			if e != nil || response == nil {
				t.Fatal("actual current source unavailable", kind, e)
			}
			if kind == "queue-not-ready-no-reader" {
				defer response.Close()
				if response.Status != 503 || response.Headers.Get("Retry-After") != "4" {
					t.Fatal("queue not-ready response changed")
				}
				return
			}
			if response.Status != 200 || response.ContentLength != 128 || response.Headers.Get("Accept-Ranges") != "bytes" || response.Headers.Get("Last-Modified") == "" {
				response.Close()
				t.Fatal("existing source headers changed", kind)
			}
			if request.Method == http.MethodHead {
				if e = response.Close(); e != nil {
					t.Fatal(e)
				}
				return
			}
			if e = nzbfilesystem.RequireSourceCleanup(response.Body); e != nil {
				response.Close()
				t.Fatal("provider lost actual F2 cleanup identity", e)
			}
			if nntp == nil {
				if e = response.Close(); e != nil {
					t.Fatal(e)
				}
				if e = nzbfilesystem.RequireSourceCleanup(response.Body); e == nil {
					t.Fatal("actual returned source remained live after Response.Close")
				}
				return
			}
			readDone := make(chan struct{})
			closed := make(chan struct{})
			var readErr, closeErr error
			closeStarted := false
			// Register failure cleanup before either actual child starts. Release the
			// held genuine NNTP return before joining the real read/Close paths.
			t.Cleanup(func() {
				cancel()
				nntp.release()
				providerContextWait(t, readDone, "read cleanup")
				if !closeStarted {
					closeErr = response.Close()
				} else {
					providerContextWait(t, closed, "response cleanup")
				}
				_ = closeErr
			})
			go func() { defer close(readDone); _, readErr = response.Body.Read(make([]byte, 64)) }()
			providerContextWait(t, nntp.accepted, "actual encrypted header BODY")
			select {
			case original := <-nntp.pool.seen:
				if original.Value(providerContextMarker{}) != "synthetic-original-provider" {
					t.Fatal("actual NNTP header reader lost original request context")
				}
			case <-time.After(time.Second):
				t.Fatal("actual NNTP reader context not observed")
			}
			if kind != "response-close-joins-held-worker" {
				cancel()
			}
			closeStarted = true
			go func() { defer close(closed); closeErr = response.Close() }()
			providerContextWait(t, nntp.pool.returned, "actual NNTP operation return")
			select {
			case <-closed:
				t.Fatal("response close preceded actual held source worker end")
			case <-time.After(20 * time.Millisecond):
			}
			nntp.release()
			providerContextWait(t, readDone, "original header read")
			providerContextWait(t, closed, "physical source join")
			if readErr == nil {
				t.Fatal("retired original source returned encrypted header bytes")
			}
			_ = closeErr
		})
	}
}
