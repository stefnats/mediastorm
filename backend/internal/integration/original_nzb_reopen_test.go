//go:build linux

package integration

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	"novastream/config"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/services/streaming"
)

// The peer really answers BODY with NNTP430. It does not inject a decoder,
// cleanup error, permission or structural body in place of the genuine actor.
func retainedMissingNZB(t *testing.T) *namespaceActualFixture {
	t.Helper()
	f := &namespaceActualFixture{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	var actual nntppool.UsenetConnectionPool
	started := false
	t.Cleanup(func() {
		if f.pool != nil {
			f.release()
		}
		if f.cancel != nil {
			f.cancel()
		}
		if f.ns != nil {
			if err := f.ns.Close(); err != nil {
				t.Error(err)
			}
		}
		if actual != nil {
			if err := nntppool.JoinConnectionPoolCleanup(actual); err != nil {
				t.Error(err)
			}
		}
		_ = listener.Close()
		if started {
			<-accepted
		}
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	started = true
	go func() {
		defer close(accepted)
		for {
			c, err := listener.Accept()
			if err != nil {
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
				_, _ = io.WriteString(c, "200 synthetic missing-article peer\r\n")
				r := bufio.NewScanner(c)
				for r.Scan() {
					switch line := r.Text(); {
					case strings.HasPrefix(line, "BODY "):
						f.requests.Add(1)
						_, _ = io.WriteString(c, "430 synthetic original article unavailable\r\n")
					case line == "CAPABILITIES":
						_, _ = io.WriteString(c, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(c, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(c, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(c, "500 unsupported synthetic command\r\n")
					}
				}
			}()
		}
	}()
	actual, err = nntppool.NewConnectionPoolWithContext(t.Context(), nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 4, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 200 * time.Millisecond, HealthCheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	f.pool = &namespaceActualPool{UsenetConnectionPool: actual, returned: make(chan struct{}), release: make(chan struct{})}
	root := t.TempDir()
	f.root = filepath.Join(root, "metadata")
	ctx, cancel := context.WithCancel(t.Context())
	f.cancel = cancel
	getter := func() *config.AltMountConfig {
		return &config.AltMountConfig{Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	}
	f.ns, err = PrepareNzbSystem(ctx, NzbConfig{QueueDatabasePath: filepath.Join(root, "queue.db"), MetadataRootPath: f.root, MaxProcessorWorkers: 1}, &namespacePoolManager{actual: f.pool}, getter)
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.NewMetadataService(f.root).WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 131072, SegmentData: []*metapb.SegmentData{{Id: "actual-missing-original", SegmentSize: 131072, EndOffset: 131071}}}); err != nil {
		t.Fatal(err)
	}
	if err = f.ns.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return f
}

func retainedNZB(t *testing.T) (*namespaceActualFixture, *OriginalNZBNamespace, *OriginalNZBMetadata) {
	t.Helper()
	f := namespaceFixture(t, true)
	n := namespaceCapture(t, f)
	m := namespaceSource(t, n, t.Context())
	t.Cleanup(func() { f.release(); _ = m.Close() })
	return f, n, m
}
func retainedNZBAttempt(t *testing.T, f *namespaceActualFixture, m *OriginalNZBMetadata, ctx context.Context, start, length int64) *OriginalNZBAttempt {
	t.Helper()
	a, err := m.OpenAttempt(ctx, start, length)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.release(); _ = a.CloseAndJoin() })
	return a
}
func retainedNZBRead(t *testing.T, f *namespaceActualFixture, m *OriginalNZBMetadata, start, length int64) {
	t.Helper()
	f.release()
	a := retainedNZBAttempt(t, f, m, t.Context(), start, length)
	data := make([]byte, int(length))
	if _, err := io.ReadFull(a, data); err != nil || !bytes.Equal(data, bytes.Repeat([]byte{'n'}, int(length))) {
		t.Fatal("real original NNTP range differs", err)
	}
	if n, err := a.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("actual attempt exceeded original range", n, err)
	}
	if err := a.CloseAndJoin(); err != nil {
		t.Fatal(err)
	}
	namespaceAwait(t, a.CleanupDone(), "actual upper and F2 attempt")
}

func retainedNZBHeld(t *testing.T, whole bool) {
	t.Helper()
	f, _, m := retainedNZB(t)
	a := retainedNZBAttempt(t, f, m, t.Context(), 13, 32)
	readDone, closed := make(chan struct{}), make(chan struct{})
	started, closing := false, false
	buffer := bytes.Repeat([]byte{0xa5}, 64)
	var readN int
	var readErr error
	t.Cleanup(func() {
		a.SignalClose()
		f.release()
		if started {
			namespaceAwait(t, readDone, "held actual upper read")
		}
		if closing {
			namespaceAwait(t, closed, "held actual original close")
		}
	})
	started = true
	go func() { defer close(readDone); readN, readErr = a.Read(buffer) }()
	namespaceAwait(t, f.pool.returned, "genuine NNTP BODY return")
	// The real BODY has already written its bytes before its return is held.
	// Establish that the caller read completed before cancellation; this phase
	// then tests the independent physical F2 worker end, not an in-flight read.
	namespaceAwait(t, readDone, "actual upper positive read before close")
	if readN != 32 || readErr != nil || !bytes.Equal(buffer[:32], bytes.Repeat([]byte{'n'}, 32)) || !bytes.Equal(buffer[32:], bytes.Repeat([]byte{0xa5}, 32)) {
		t.Fatal("completed upper read differs before physical close", readN, readErr)
	}
	closing = true
	go func() {
		defer close(closed)
		if whole {
			_ = f.ns.Close()
		} else {
			_ = a.CloseAndJoin()
		}
	}()
	namespaceAwait(t, a.Context().Done(), "actual attempt signal")
	namespacePending(t, closed)
	namespacePending(t, a.CleanupDone())
	before := f.requests.Load()
	if next, err := m.OpenAttempt(t.Context(), 29, 16); err == nil || next != nil || f.requests.Load() != before {
		t.Fatal("held original F2 admitted successor")
	}
	if namespaceSourceCount(f.ns) != 2 {
		t.Fatal("held upper read refunded original source work")
	}
	namespaceHeldFD(t, filepath.Join(f.root, "movie.meta"))
	f.release()
	namespaceAwait(t, readDone, "actual completed upper read")
	namespaceAwait(t, closed, "actual held F2/upper joins")
	if readN != 32 || readErr != nil || !bytes.Equal(buffer[:32], bytes.Repeat([]byte{'n'}, 32)) || !bytes.Equal(buffer[32:], bytes.Repeat([]byte{0xa5}, 32)) {
		t.Fatal("completed upper read changed during physical join", readN, readErr)
	}
	if whole {
		namespaceWait(t, "closed original NS work not physically joined", func() bool { return namespaceSourceCount(f.ns) == 0 })
		before := f.requests.Load()
		if next, err := m.OpenAttempt(t.Context(), 29, 16); err == nil || next != nil || f.requests.Load() != before {
			t.Fatal("closed original NS admitted another attempt or provider")
		}
	} else {
		if m.Check(t.Context()) != nil || namespaceSourceCount(f.ns) != 2 {
			t.Fatal("attempt close retired original metadata")
		}
		retainedNZBRead(t, f, m, 29, 16)
	}
}

func retainedNZBUpperReturn(t *testing.T) {
	t.Helper()
	f, _, m := retainedNZB(t)
	f.release()
	a := retainedNZBAttempt(t, f, m, t.Context(), 13, 32)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	a.afterRead = func() { close(entered); <-release }
	readDone, closeDone := make(chan struct{}), make(chan struct{})
	buffer := bytes.Repeat([]byte{0xa5}, 64)
	var readN int
	var readErr error
	started, closing := false, false
	t.Cleanup(func() {
		unblock()
		a.SignalClose()
		f.release()
		if started {
			namespaceAwait(t, readDone, "actual held upper postcheck")
		}
		if closing {
			namespaceAwait(t, closeDone, "actual held upper cleanup")
		}
	})
	started = true
	go func() { defer close(readDone); readN, readErr = a.Read(buffer) }()
	namespaceAwait(t, entered, "genuine lower return before upper postcheck")
	if !bytes.Equal(buffer[:32], bytes.Repeat([]byte{'n'}, 32)) || !bytes.Equal(buffer[32:], bytes.Repeat([]byte{0xa5}, 32)) || f.requests.Load() == 0 {
		t.Fatal("upper observer did not hold genuine exact-range bytes")
	}
	closing = true
	go func() { defer close(closeDone); _ = a.CloseAndJoin() }()
	// The lower request really ends; its acknowledgement is insufficient for
	// the still executing upper return/check/whole-buffer cleanup actor.
	namespaceAwait(t, a.lower.CleanupDone(), "independent real lower F2 end")
	namespacePending(t, a.CleanupDone())
	namespacePending(t, closeDone)
	m.mu.Lock()
	originalPending := m.attempt == a
	m.mu.Unlock()
	if !originalPending || namespaceSourceCount(f.ns) != 2 || m.Check(t.Context()) != nil {
		t.Fatal("lower done replaced upper ownership or original source")
	}
	before := f.requests.Load()
	if next, err := m.OpenAttempt(t.Context(), 29, 16); err == nil || next != nil || f.requests.Load() != before {
		t.Fatal("lower done admitted next upper attempt before actual upper end")
	}
	unblock()
	namespaceAwait(t, readDone, "upper postcheck and actual whole-buffer clear")
	namespaceAwait(t, closeDone, "actual upper cleanup acknowledgement")
	if readN != 0 || readErr == nil || !bytes.Equal(buffer, make([]byte, len(buffer))) {
		t.Fatal("upper terminal postcheck left caller bytes", readN, readErr)
	}
	retainedNZBRead(t, f, m, 29, 16)
}

func TestOriginalNZBRetainedAttempts(t *testing.T) {
	t.Run("prepared-original-runtime-only", func(t *testing.T) {
		f := namespaceFixture(t, false)
		n := namespaceCapture(t, f)
		if m, err := n.CaptureMetadata(t.Context(), "movie.meta"); err == nil || m != nil || f.requests.Load() != 0 {
			t.Fatal("inactive system acquired media")
		}
		if err := f.ns.Activate(t.Context()); err != nil {
			t.Fatal(err)
		}
		m := namespaceSource(t, n, t.Context())
		t.Cleanup(func() { f.release(); _ = m.Close() })
		a := retainedNZBAttempt(t, f, m, t.Context(), 13, 16)
		copy := *a
		for _, invalid := range []*OriginalNZBAttempt{nil, {}, &copy} {
			buffer := bytes.Repeat([]byte{1}, 16)
			if n, err := invalid.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, make([]byte, 16)) || invalid.CloseAndJoin() == nil || invalid.CleanupDone() != nil {
				t.Fatal("zero or copied upper attempt became original")
			}
		}
		if a.Check(t.Context()) != nil || f.requests.Load() != 0 {
			t.Fatal("rejected copy changed genuine inactive IO boundary")
		}
	})
	t.Run("actual-retained-metadata-not-path-recapture", func(t *testing.T) {
		f, _, m := retainedNZB(t)
		retainedNZBRead(t, f, m, 13, 16)
		originalLower, originalExpiry := m.lower, m.ExpiresAt()
		retainedNZBRead(t, f, m, 65549, 32)
		if m.lower != originalLower || m.ExpiresAt() != originalExpiry {
			t.Fatal("upper attempt recaptured metadata or renewed expiry")
		}
		path := filepath.Join(f.root, "movie.meta")
		if err := os.Rename(path, path+".original"); err != nil {
			t.Fatal(err)
		}
		service := metadata.NewMetadataService(f.root)
		if err := service.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 131072, SegmentData: []*metapb.SegmentData{{Id: "foreign-same-name", SegmentSize: 131072, EndOffset: 131071}}}); err != nil {
			t.Fatal(err)
		}
		before := f.requests.Load()
		if next, err := m.OpenAttempt(t.Context(), 0, 16); err == nil || next != nil || f.requests.Load() != before {
			t.Fatal("replacement name became a new source")
		}
	})
	t.Run("shared-permanent32-source-reservation", func(t *testing.T) {
		f, n, m := retainedNZB(t)
		f.release()
		for i := 0; i < 12; i++ {
			retainedNZBRead(t, f, m, int64(i*19), 16)
			if namespaceSourceCount(f.ns) != 2 {
				t.Fatal("attempt acquired or refunded original source slot")
			}
		}
		for i := 0; i < 30; i++ {
			extra := namespaceSource(t, n, t.Context())
			t.Cleanup(func() { _ = extra.Close() })
		}
		if namespaceSourceCount(f.ns) != 32 {
			t.Fatal("original shared source registry did not reach32")
		}
		retainedNZBRead(t, f, m, 65549, 16) // Existing source may reopen without a new slot.
		if extra, err := n.CaptureMetadata(t.Context(), "movie.meta"); err == nil || extra != nil {
			t.Fatal("new source bypassed original32limit")
		}
	})
	t.Run("ordinary-and-retained-source-budget-shared", func(t *testing.T) {
		f, n, m := retainedNZB(t)
		f.release()
		response, err := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet})
		if err != nil || response == nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = response.Close() })
		for i := 0; i < 29; i++ {
			extra := namespaceSource(t, n, t.Context())
			t.Cleanup(func() { _ = extra.Close() })
		}
		if namespaceSourceCount(f.ns) != 32 {
			t.Fatal("legacy and retained captures do not share original limit")
		}
		if extra, err := f.ns.Stream(t.Context(), streaming.Request{Path: "movie", Method: http.MethodGet}); err == nil || extra != nil {
			t.Fatal("ordinary source bypassed shared limit")
		}
		retainedNZBRead(t, f, m, 13, 16)
	})
	t.Run("sequential-range-crosses-real-nntp-window", func(t *testing.T) {
		f := namespaceFixture(t, true)
		const block = 131072
		meta := &metapb.FileMetadata{FileSize: 24 * block}
		for i := 0; i < 24; i++ {
			meta.SegmentData = append(meta.SegmentData, &metapb.SegmentData{Id: "original-window-" + strings.Repeat("n", i+1), SegmentSize: block, EndOffset: block - 1})
		}
		if err := metadata.NewMetadataService(f.root).WriteFileMetadata("movie", meta); err != nil {
			t.Fatal(err)
		}
		m := namespaceSource(t, namespaceCapture(t, f), t.Context())
		t.Cleanup(func() { f.release(); _ = m.Close() })
		retainedNZBRead(t, f, m, 13, 23*block)
		retainedNZBRead(t, f, m, 21*block+19, 64)
		if f.requests.Load() < 22 {
			t.Fatal("genuine progressive window was never reopened", f.requests.Load())
		}
	})
	t.Run("close-attempt-keeps-original-source-slot", func(t *testing.T) {
		f, _, m := retainedNZB(t)
		retainedNZBRead(t, f, m, 13, 16)
		if namespaceSourceCount(f.ns) != 2 || m.Check(t.Context()) != nil {
			t.Fatal("attempt Close refunded source work")
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		namespaceWait(t, "whole source failed to refund actual work", func() bool { return namespaceSourceCount(f.ns) == 1 })
	})
	t.Run("new-attempt-waits-real-f2-completion", func(t *testing.T) {
		retainedNZBHeld(t, false)
		retainedNZBUpperReturn(t)
	})
	t.Run("local-cancel-does-not-retire-owner-source", func(t *testing.T) {
		f, _, m := retainedNZB(t)
		f.release()
		ctx, cancel := context.WithCancel(t.Context())
		a := retainedNZBAttempt(t, f, m, ctx, 13, 16)
		cancel()
		namespaceAwait(t, a.CleanupDone(), "cancelled actual upper attempt")
		if m.Check(t.Context()) != nil || namespaceSourceCount(f.ns) != 2 {
			t.Fatal("local cancel retired original source owner")
		}
		retainedNZBRead(t, f, m, 65549, 32)
	})
	t.Run("original-ns-close-denies-next-attempt", func(t *testing.T) { retainedNZBHeld(t, true) })
	t.Run("all-original-attempts-signalled-before-join", func(t *testing.T) {
		f, n, m := retainedNZB(t)
		second := namespaceSource(t, n, t.Context())
		a := retainedNZBAttempt(t, f, m, t.Context(), 13, 16)
		b := retainedNZBAttempt(t, f, second, t.Context(), 29, 16)
		one, two, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		started, closing := false, false
		t.Cleanup(func() {
			f.release()
			if started {
				namespaceAwait(t, one, "first actual upper read")
				namespaceAwait(t, two, "second actual upper read")
			}
			if closing {
				namespaceAwait(t, closed, "original NS close")
			}
		})
		started = true
		go func() { defer close(one); _, _ = a.Read(make([]byte, 16)) }()
		go func() { defer close(two); _, _ = b.Read(make([]byte, 16)) }()
		namespaceWait(t, "two genuine BODY returns not held", func() bool { return f.pool.calls.Load() >= 2 })
		closing = true
		go func() { defer close(closed); _ = f.ns.Close() }()
		namespaceAwait(t, a.Context().Done(), "first original signal")
		namespaceAwait(t, b.Context().Done(), "second original signal")
		namespacePending(t, closed)
		if namespaceSourceCount(f.ns) != 3 {
			t.Fatal("held sources refunded before all actual ends")
		}
		f.release()
		namespaceAwait(t, one, "first actual end")
		namespaceAwait(t, two, "second actual end")
		namespaceAwait(t, closed, "NS all actual joins")
	})
	t.Run("terminal-provider-error-and-unsupported-cleanup-receipt", func(t *testing.T) {
		f := retainedMissingNZB(t)
		m := namespaceSource(t, namespaceCapture(t, f), t.Context())
		t.Cleanup(func() { f.release(); _ = m.Close() })
		f.release()
		a := retainedNZBAttempt(t, f, m, t.Context(), 13, 16)
		if _, err := nzbfilesystem.SourceCleanupDone(io.NopCloser(a)); !errors.Is(err, nzbfilesystem.ErrSourceCleanupUnsupported) {
			t.Fatal("structural wrapper became physical completion", err)
		}
		if namespaceSourceCount(f.ns) != 2 {
			t.Fatal("unsupported receipt refunded actual source work")
		}
		buffer := bytes.Repeat([]byte{0xa5}, 64)
		if n, err := a.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, make([]byte, len(buffer))) || f.requests.Load() == 0 {
			t.Fatal("genuine NNTP430 did not deny and clear actual bytes", n, err)
		}
		if err := a.CloseAndJoin(); err != nil {
			t.Fatal(err)
		}
		namespaceAwait(t, a.CleanupDone(), "actual terminal provider attempt end")
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		before := f.requests.Load()
		if next, err := m.OpenAttempt(t.Context(), 0, 16); err == nil || next != nil {
			t.Fatal("retired original source reopened")
		}
		if f.requests.Load() != before {
			t.Fatal("terminal original performed new provider work")
		}
	})
	t.Run("same-name-replacement-health-unchanged", func(t *testing.T) {
		f, _, m := retainedNZB(t)
		retainedNZBRead(t, f, m, 13, 16)
		path := filepath.Join(f.root, "movie.meta")
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		service := metadata.NewMetadataService(f.root)
		if err := service.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 131072, Status: metapb.FileStatus_FILE_STATUS_HEALTHY, SegmentData: []*metapb.SegmentData{{Id: "foreign-new-metadata", SegmentSize: 131072, EndOffset: 131071}}}); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		calls := f.requests.Load()
		if _, err = m.OpenAttempt(t.Context(), 0, 16); err == nil {
			t.Fatal("same name changed original source")
		}
		if err = m.Close(); err != nil {
			t.Fatal(err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) || f.requests.Load() != calls {
			t.Fatal("old original attempt altered foreign metadata/health", err)
		}
	})
}
