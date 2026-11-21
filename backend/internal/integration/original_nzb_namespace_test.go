//go:build linux

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	"novastream/config"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/services/streaming"
)

type namespaceActualPool struct {
	nntppool.UsenetConnectionPool
	returned chan struct{}
	release  chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (p *namespaceActualPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	n, e := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.calls.Add(1)
	p.once.Do(func() { close(p.returned) })
	<-p.release
	return n, e
}

type namespacePoolManager struct{ actual *namespaceActualPool }

func (m *namespacePoolManager) GetPool() (nntppool.UsenetConnectionPool, error) { return m.actual, nil }
func (m *namespacePoolManager) SetProviders([]nntppool.UsenetProviderConfig) error {
	return errors.New("fixture provider mutation forbidden")
}
func (m *namespacePoolManager) ClearPool() error {
	return errors.New("fixture provider mutation forbidden")
}
func (m *namespacePoolManager) HasPool() bool             { return true }
func (m *namespacePoolManager) AvailableConnections() int { return 4 }

type namespaceActualFixture struct {
	ns          *NzbSystem
	pool        *namespaceActualPool
	root        string
	cancel      context.CancelFunc
	releaseOnce sync.Once
	requests    atomic.Int32
}

func (f *namespaceActualFixture) release() { f.releaseOnce.Do(func() { close(f.pool.release) }) }
func namespaceAwait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("actual original operation did not join", label)
	}
}
func namespaceWait(t *testing.T, label string, check func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !check() {
		if time.Now().After(end) {
			t.Fatal("actual original witness absent", label)
		}
		time.Sleep(time.Millisecond)
	}
}
func namespacePending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("physical original completion published while worker held")
	case <-time.After(20 * time.Millisecond):
	}
}
func namespaceYenc(data []byte) string {
	var b strings.Builder
	column := 0
	for _, v := range data {
		x := v + 42
		escape := x == 0 || x == 9 || x == 10 || x == 13 || x == 32 || x == 46 || x == 61
		width := 1
		if escape {
			width = 2
		}
		if column+width > 128 {
			b.WriteString("\r\n")
			column = 0
		}
		if escape {
			b.WriteByte('=')
			x += 64
		}
		b.WriteByte(x)
		column += width
	}
	return fmt.Sprintf("=ybegin line=128 size=%d name=original.bin\r\n%s\r\n=yend size=%d crc32=%08x\r\n.\r\n", len(data), b.String(), len(data), crc32.ChecksumIEEE(data))
}
func namespaceFixture(t *testing.T, active bool) *namespaceActualFixture {
	t.Helper()
	f := &namespaceActualFixture{}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			connections[conn] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(connections, conn); mu.Unlock() }()
				_, _ = io.WriteString(conn, "200 synthetic original ready\r\n")
				reader := bufio.NewReader(conn)
				for {
					line, e := reader.ReadString('\n')
					if e != nil {
						return
					}
					line = strings.TrimSpace(line)
					switch {
					case strings.HasPrefix(line, "BODY "):
						f.requests.Add(1)
						_, _ = io.WriteString(conn, "222 body\r\n"+namespaceYenc([]byte(strings.Repeat("n", 131072))))
					case line == "CAPABILITIES":
						_, _ = io.WriteString(conn, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(conn, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(conn, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(conn, "500 unsupported\r\n")
					}
				}
			}()
		}
	}()
	pool, e := nntppool.NewConnectionPool(nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 4, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 200 * time.Millisecond, HealthCheckInterval: time.Hour})
	if e != nil {
		_ = listener.Close()
		<-accepted
		workers.Wait()
		t.Fatal(e)
	}
	f.pool = &namespaceActualPool{UsenetConnectionPool: pool, returned: make(chan struct{}), release: make(chan struct{})}
	// Registered before any NS construction/activation/body work can fail.
	t.Cleanup(func() {
		f.release()
		if f.cancel != nil {
			f.cancel()
		}
		if f.ns != nil {
			if e := f.ns.Close(); e != nil {
				t.Error(e)
			}
		}
		pool.Quit()
		_ = listener.Close()
		<-accepted
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	root := t.TempDir()
	f.root = filepath.Join(root, "metadata")
	ctx, cancel := context.WithCancel(t.Context())
	f.cancel = cancel
	getter := func() *config.AltMountConfig {
		return &config.AltMountConfig{Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	}
	f.ns, e = PrepareNzbSystem(ctx, NzbConfig{QueueDatabasePath: filepath.Join(root, "queue.db"), MetadataRootPath: f.root, MaxProcessorWorkers: 1}, &namespacePoolManager{actual: f.pool}, getter)
	if e != nil {
		t.Fatal(e)
	}
	service := metadata.NewMetadataService(f.root)
	if e = service.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 131072, ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "original-namespace", SegmentSize: 131072, EndOffset: 131071}}}); e != nil {
		t.Fatal(e)
	}
	if active {
		if e = f.ns.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
	}
	return f
}
func namespaceCapture(t *testing.T, f *namespaceActualFixture) *OriginalNZBNamespace {
	t.Helper()
	n, e := f.ns.CaptureOriginalNamespace(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	return n
}
func namespaceSource(t *testing.T, n *OriginalNZBNamespace, ctx context.Context) *OriginalNZBMetadata {
	t.Helper()
	m, e := n.CaptureMetadata(ctx, "movie.meta")
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func namespaceSourceCount(ns *NzbSystem) int {
	ns.runtimeMu.Lock()
	defer ns.runtimeMu.Unlock()
	return len(ns.sources)
}
func namespaceHeldFD(t *testing.T, path string) {
	t.Helper()
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if e == nil && target == path {
			return
		}
	}
	t.Fatal("actual named original descriptor not held", path)
}

func TestOriginalNZBNamespace(t *testing.T) {
	t.Run("actual-prepared-original-only", func(t *testing.T) {
		f := namespaceFixture(t, false)
		n := namespaceCapture(t, f)
		if e := n.MatchesOriginal(t.Context(), f.ns, f.root); e != nil {
			t.Fatal(e)
		}
		again, e := f.ns.CaptureOriginalNamespace(t.Context())
		if e != nil || again != n {
			t.Fatal("original namespace renewed", e)
		}
		if f.ns.service.IsRunning() || f.requests.Load() != 0 || namespaceSourceCount(f.ns) != 1 {
			t.Fatal("namespace setup activated provider/importer or omitted root reservation")
		}
		namespaceHeldFD(t, f.root)
	})
	t.Run("zero-copied-and-foreign-system-denied", func(t *testing.T) {
		f := namespaceFixture(t, false)
		n := namespaceCapture(t, f)
		other := namespaceFixture(t, false)
		copyNamespace := *n
		var zero OriginalNZBNamespace
		if copyNamespace.MatchesOriginal(t.Context(), f.ns, f.root) == nil || zero.MatchesOriginal(t.Context(), f.ns, f.root) == nil || n.MatchesOriginal(t.Context(), other.ns, f.root) == nil {
			t.Fatal("copied/zero/foreign original binding accepted")
		}
		for _, actor := range []any{n, *n} {
			data, e := json.Marshal(actor)
			if e != nil || string(data) != "null" || !strings.Contains(fmt.Sprintf("%#v", actor), "opaque") {
				t.Fatal("namespace actor serialization exposed private fields", e)
			}
		}
		var imported OriginalNZBNamespace
		if json.Unmarshal([]byte("{}"), &imported) == nil {
			t.Fatal("JSON imported namespace")
		}
		if e := f.ns.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		m := namespaceSource(t, n, t.Context())
		r, e := m.Open(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		for _, actor := range []any{m, *m, r, *r} {
			data, e := json.Marshal(actor)
			if e != nil || string(data) != "null" || !strings.Contains(fmt.Sprintf("%#v", actor), "opaque") {
				t.Fatal("actual source/stream exposed private fields", e)
			}
		}
		var importedMetadata OriginalNZBMetadata
		var importedStream OriginalNZBStream
		if json.Unmarshal([]byte("{}"), &importedMetadata) == nil || json.Unmarshal([]byte("{}"), &importedStream) == nil {
			t.Fatal("JSON imported original source")
		}
		copySystem := *f.ns
		if v, e := copySystem.CaptureOriginalNamespace(t.Context()); e == nil || v != nil {
			t.Fatal("copied prepared system minted namespace")
		}
	})
	t.Run("actual-canonical-root-replacement-denied", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		m := namespaceSource(t, n, t.Context())
		defer m.Close()
		if e := os.WriteFile(filepath.Join(f.root, "unrelated-sibling"), []byte("synthetic sibling import"), 0600); e != nil {
			t.Fatal(e)
		}
		if e := m.Check(t.Context()); e != nil {
			t.Fatal("unrelated sibling directory churn retired original source", e)
		}
		if e := os.Rename(f.root, f.root+".original"); e != nil {
			t.Fatal(e)
		}
		if e := os.Mkdir(f.root, 0700); e != nil {
			t.Fatal(e)
		}
		if e := n.MatchesOriginal(t.Context(), f.ns, f.root); e == nil {
			t.Fatal("recycled canonical root adopted")
		}
		if f.requests.Load() != 0 {
			t.Fatal("root replacement opened provider")
		}
	})
	t.Run("queue-and-webdav-alias-terminal", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		for _, name := range []string{"queue/1/movie.meta", "webdav/movie.meta", "/movie.meta", "movie.meta?ARGS?origin=x"} {
			if m, e := n.CaptureMetadata(t.Context(), name); e == nil || m != nil {
				t.Fatal("mutable alias gained original source", name)
			}
		}
		if f.requests.Load() != 0 || namespaceSourceCount(f.ns) != 1 {
			t.Fatal("denied alias retained work or opened provider")
		}
	})
	t.Run("inactive-no-provider-header-work", func(t *testing.T) {
		f := namespaceFixture(t, false)
		n := namespaceCapture(t, f)
		if m, e := n.CaptureMetadata(t.Context(), "movie.meta"); e == nil || m != nil {
			t.Fatal("inactive prepared source admitted metadata/header work")
		}
		if response, e := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet}); e == nil || response != nil {
			t.Fatal("inactive ordinary path opened source")
		}
		if f.requests.Load() != 0 {
			t.Fatal("inactive source opened NNTP")
		}
	})
	t.Run("namespace-setup-close-join", func(t *testing.T) {
		f := namespaceFixture(t, false)
		entered, release, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		started, closing := false, false
		f.ns.originalNamespaceObserved = func() { close(entered); <-release }
		t.Cleanup(func() {
			f.cancel()
			unblock()
			if started {
				namespaceAwait(t, done, "namespace setup")
			}
			if closing {
				namespaceAwait(t, closed, "namespace Close")
			}
		})
		started = true
		go func() { defer close(done); _, _ = f.ns.CaptureOriginalNamespace(t.Context()) }()
		namespaceAwait(t, entered, "actual root setup return")
		namespaceHeldFD(t, f.root)
		if namespaceSourceCount(f.ns) != 1 {
			t.Fatal("actual pending root setup not charged")
		}
		closing = true
		go func() { defer close(closed); _ = f.ns.Close() }()
		namespacePending(t, closed)
		unblock()
		namespaceAwait(t, done, "original setup return after close")
		namespaceAwait(t, closed, "original namespace physical end")
		if namespaceSourceCount(f.ns) != 0 {
			t.Fatal("root reservation survived actual end")
		}
	})
	t.Run("existing-and-purpose-shared32-limit", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		var calls atomic.Int32
		started := false
		f.ns.originalMetadataObserved = func() {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
		}
		caller, cancel := context.WithCancel(t.Context())
		t.Cleanup(func() {
			cancel()
			unblock()
			f.release()
			if started {
				namespaceAwait(t, done, "actual pending capture")
			}
		})
		started = true
		go func() {
			defer close(done)
			m, e := n.CaptureMetadata(caller, "movie.meta")
			if e == nil {
				_ = m.Close()
			}
		}()
		namespaceAwait(t, entered, "actual held metadata ReadAt/capture")
		namespaceHeldFD(t, filepath.Join(f.root, "movie.meta"))
		response, e := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet})
		if e != nil || response == nil {
			t.Fatal("genuine existing source", e)
		}
		t.Cleanup(func() { _ = response.Close() })
		for i := 0; i < 29; i++ {
			m := namespaceSource(t, n, t.Context())
			t.Cleanup(func() { _ = m.Close() })
		}
		if namespaceSourceCount(f.ns) != 32 {
			t.Fatal("pending/namespace/legacy/purpose work not one actual registry", namespaceSourceCount(f.ns))
		}
		if extra, e := n.CaptureMetadata(t.Context(), "movie.meta"); e == nil || extra != nil {
			t.Fatal("purpose work bypassed shared32")
		}
		if extra, e := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet}); e == nil || extra != nil {
			t.Fatal("legacy work bypassed shared32")
		}
		cancel()
		namespacePending(t, done)
		if namespaceSourceCount(f.ns) != 32 {
			t.Fatal("pending capture refunded before actual return")
		}
		unblock()
		namespaceAwait(t, done, "held capture release")
	})
	t.Run("caller-cancel-held-actual-body", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		ctx, cancel := context.WithCancel(t.Context())
		m := namespaceSource(t, n, ctx)
		r, e := m.Open(ctx, 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		stale, stop := context.WithCancel(t.Context())
		stop()
		if m.Check(stale) == nil {
			t.Fatal("canceled metadata check succeeded")
		}
		if _, e = m.Info(stale); e == nil {
			t.Fatal("canceled metadata info succeeded")
		}
		if second, e := m.Open(ctx, 0, 32); e == nil || second != nil {
			t.Fatal("duplicate wrapper Open succeeded")
		}
		if m.Context().Err() != nil {
			t.Fatal("local wrapper denial retired original")
		}
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			cancel()
			f.release()
			if started {
				namespaceAwait(t, done, "actual caller-cancel Read")
			}
			_ = m.Close()
		})
		started = true
		go func() { defer close(done); _, _ = r.Read(make([]byte, 32)) }()
		namespaceAwait(t, f.pool.returned, "actual BODY delegate return")
		cancel()
		namespacePending(t, r.CleanupDone())
		f.release()
		namespaceAwait(t, done, "actual cancelled reader end")
		namespaceAwait(t, r.CleanupDone(), "actual caller source end")
		if namespaceSourceCount(f.ns) != 1 {
			t.Fatal("actual source reservation not refunded after physical end")
		}
	})
	t.Run("all-original-readers-signalled-before-join", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		var readers []*OriginalNZBStream
		var wg sync.WaitGroup
		done, closed := make(chan struct{}), make(chan struct{})
		closing := false
		t.Cleanup(func() {
			f.cancel()
			f.release()
			wg.Wait()
			if closing {
				namespaceAwait(t, closed, "all original source Close")
			}
		})
		for i := 0; i < 2; i++ {
			m := namespaceSource(t, n, t.Context())
			r, e := m.Open(t.Context(), 0, 32)
			if e != nil {
				t.Fatal(e)
			}
			readers = append(readers, r)
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = r.Read(make([]byte, 32)) }()
		}
		go func() { wg.Wait(); close(done) }()
		namespaceWait(t, "two real BODY delegates", func() bool { return f.pool.calls.Load() >= 2 })
		closing = true
		go func() { defer close(closed); _ = f.ns.Close() }()
		namespaceWait(t, "all original contexts signalled", func() bool { return readers[0].Context().Err() != nil && readers[1].Context().Err() != nil })
		namespacePending(t, closed)
		f.release()
		namespaceAwait(t, done, "all actual reads")
		namespaceAwait(t, closed, "all actual bodies and NS teardown")
	})
	t.Run("actual-cleanup-before-work-refund", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		m := namespaceSource(t, n, t.Context())
		r, e := m.Open(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		started, closing := false, false
		t.Cleanup(func() {
			f.cancel()
			f.release()
			if started {
				namespaceAwait(t, readDone, "actual refund Read")
			}
			if closing {
				namespaceAwait(t, closeDone, "actual refund Close")
			}
			_ = m.Close()
		})
		started = true
		go func() { defer close(readDone); _, _ = r.Read(make([]byte, 32)) }()
		namespaceAwait(t, f.pool.returned, "actual held worker")
		closing = true
		go func() { defer close(closeDone); _ = r.Close() }()
		namespacePending(t, closeDone)
		if namespaceSourceCount(f.ns) != 2 {
			t.Fatal("source work refunded while BODY worker retained")
		}
		f.release()
		namespaceAwait(t, readDone, "actual refund reader")
		namespaceAwait(t, closeDone, "actual refund source end")
		if namespaceSourceCount(f.ns) != 1 {
			t.Fatal("physical end did not refund exact source")
		}
	})
	t.Run("unsupported-body-retains-charge", func(t *testing.T) {
		f := namespaceFixture(t, true)
		_ = namespaceCapture(t, f)
		response, e := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet})
		if e != nil || response == nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = response.Close() })
		raw := response.Body
		if _, e = nzbfilesystem.SourceCleanupDone(io.NopCloser(raw)); !errors.Is(e, nzbfilesystem.ErrSourceCleanupUnsupported) {
			t.Fatal("structural wrapper fabricated actual completion", e)
		}
		if namespaceSourceCount(f.ns) != 2 {
			t.Fatal("unsupported receipt refunded original source")
		}
		actual, e := nzbfilesystem.SourceCleanupDone(raw)
		if e != nil {
			t.Fatal(e)
		}
		namespacePending(t, actual)
		if e = response.Close(); e != nil {
			t.Fatal(e)
		}
		namespaceAwait(t, actual, "actual original raw close")
		namespaceWait(t, "actual work refund", func() bool { return namespaceSourceCount(f.ns) == 1 })
	})
	t.Run("ns-close-joins-namespace-source-health", func(t *testing.T) {
		f := namespaceFixture(t, true)
		n := namespaceCapture(t, f)
		m := namespaceSource(t, n, t.Context())
		r, e := m.Open(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		started, closing := false, false
		t.Cleanup(func() {
			f.cancel()
			f.release()
			if started {
				namespaceAwait(t, readDone, "actual shutdown reader")
			}
			if closing {
				namespaceAwait(t, closeDone, "actual shutdown")
			}
		})
		started = true
		go func() { defer close(readDone); _, _ = r.Read(make([]byte, 32)) }()
		namespaceAwait(t, f.pool.returned, "actual shutdown BODY")
		closing = true
		go func() { defer close(closeDone); _ = f.ns.Close() }()
		namespacePending(t, closeDone)
		if e = f.ns.database.Connection().PingContext(t.Context()); e != nil {
			t.Fatal("SQLite closed before actual source/health lifetime ended", e)
		}
		f.release()
		namespaceAwait(t, readDone, "actual source read end")
		namespaceAwait(t, closeDone, "namespace/root/source before SQLite teardown")
		if f.ns.database.Connection().PingContext(t.Context()) == nil {
			t.Fatal("actual SQLite dependency remained open after NS.Close")
		}
	})
}
