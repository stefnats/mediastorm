//go:build linux

package nzbfilesystem

import (
	"bufio"
	"bytes"
	"context"
	stdaes "crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	"novastream/config"
	"novastream/internal/encryption/rclone"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
)

// Distinct actual article bytes expose wrong segment selection as well as
// wrong offsets. The pool still reads real TCP/yEnc and owns real F2 workers.
func retainedMappedFixture(t *testing.T, articles map[string][]byte) *requestFixture {
	t.Helper()
	f := &requestFixture{accepted: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	var acceptedOnce sync.Once
	var actual nntppool.UsenetConnectionPool
	started := false
	t.Cleanup(func() {
		if f.pool != nil {
			f.releaseAll()
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
				_, _ = io.WriteString(c, "200 synthetic distinct-article peer\r\n")
				r := bufio.NewScanner(c)
				for r.Scan() {
					switch line := r.Text(); {
					case strings.HasPrefix(line, "BODY "):
						f.bodyCalls.Add(1)
						acceptedOnce.Do(func() { close(f.accepted) })
						id := strings.Trim(strings.TrimPrefix(line, "BODY "), "<>")
						payload, found := articles[id]
						if !found {
							_, _ = io.WriteString(c, "430 unknown synthetic article\r\n")
						} else {
							_, _ = io.WriteString(c, "222 original distinct article\r\n"+requestYenc(payload))
						}
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
	f.pool = &requestHeldPool{UsenetConnectionPool: actual, release: make(chan struct{}), returned: make(chan struct{})}
	f.manager = &requestPoolManager{pool: f.pool}
	f.service = metadata.NewMetadataService(t.TempDir())
	f.remote = NewMetadataRemoteFile(f.service, nil, f.manager, func() *config.AltMountConfig {
		return &config.AltMountConfig{Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	})
	f.fs = NewNzbFilesystem(f.remote).(*NzbFilesystem)
	return f
}

func retainedOriginal(t *testing.T) (*requestFixture, *OriginalMetadataSource, []byte) {
	t.Helper()
	plain := pinnedPattern(131072)
	f := requestRealFixture(t, plain, false)
	f.write(t, requestMetadata(int64(len(plain))))
	s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
	// This runs before the existing source cleanup, including failed held work.
	t.Cleanup(f.releaseAll)
	return f, s, plain
}
func retainedAttempt(t *testing.T, s *OriginalMetadataSource, ctx context.Context, start, length int64) *OriginalMetadataAttempt {
	t.Helper()
	a, err := s.OpenAttempt(ctx, start, length)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.CloseAndJoin() })
	return a
}
func retainedBytes(t *testing.T, s *OriginalMetadataSource, start, length int64) []byte {
	t.Helper()
	a := retainedAttempt(t, s, t.Context(), start, length)
	data := make([]byte, int(length))
	if _, err := io.ReadFull(a, data); err != nil {
		t.Fatal(err)
	}
	if n, err := a.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("retained range exceeded its original end", n, err)
	}
	if err := a.CloseAndJoin(); err != nil {
		t.Fatal(err)
	}
	requestWait(t, a.CleanupDone(), "actual attempt cleanup")
	return data
}
func retainedCharge(s *OriginalMetadataSource) int64 {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.owner.used
}
func retainedUntil(t *testing.T, label string, check func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !check() {
		if time.Now().After(end) {
			t.Fatal(label)
		}
		time.Sleep(time.Millisecond)
	}
}

func retainedHeldEnd(t *testing.T, postRead bool) {
	t.Helper()
	f, s, plain := retainedOriginal(t)
	a := retainedAttempt(t, s, t.Context(), 13, 32)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	if postRead {
		s.afterRead = func() { close(entered); <-release }
		f.releaseAll()
	} else {
		entered = f.pool.returned
	}
	readDone, closeDone := make(chan struct{}), make(chan struct{})
	buffer := bytes.Repeat([]byte{0xa5}, 64)
	var count int
	var readErr error
	started, closing := false, false
	t.Cleanup(func() {
		a.SignalClose()
		f.releaseAll()
		unblock()
		if started {
			requestWait(t, readDone, "held actual read")
		}
		if closing {
			requestWait(t, closeDone, "held actual close")
		}
	})
	charge := retainedCharge(s)
	started = true
	go func() { defer close(readDone); count, readErr = a.Read(buffer) }()
	requestWait(t, entered, "actual BODY or returned-byte hold")
	if postRead && !bytes.Equal(buffer[:32], plain[13:45]) {
		t.Fatal("observer did not hold genuine original bytes")
	}
	if !postRead {
		// BODY already wrote the actual segment pipe before its held return.
		// Prove a completed original read first; the remaining held actor is
		// physical provider cleanup, independently of caller cancellation.
		requestWait(t, readDone, "actual positive read before physical close")
		if count != 32 || readErr != nil || !bytes.Equal(buffer[:32], plain[13:45]) || !bytes.Equal(buffer[32:], bytes.Repeat([]byte{0xa5}, 32)) {
			t.Fatal("completed original read differs before close", count, readErr)
		}
	}
	closing = true
	go func() { defer close(closeDone); _ = a.CloseAndJoin() }()
	requestWait(t, a.Context().Done(), "actual attempt cancellation before release")
	requestPending(t, closeDone, "attempt close")
	requestPending(t, a.CleanupDone(), "attempt F2 acknowledgement")
	calls := f.bodyCalls.Load()
	if next, err := s.OpenAttempt(t.Context(), 7, 16); err == nil || next != nil || f.bodyCalls.Load() != calls {
		t.Fatal("closing real attempt admitted another provider")
	}
	if retainedCharge(s) != charge || s.meta == nil {
		t.Fatal("held actual work refunded original snapshot")
	}
	if _, err := s.file.Stat(); err != nil {
		t.Fatal("original metadata FD ended before actual attempt")
	}
	f.releaseAll()
	unblock()
	requestWait(t, readDone, "actual read/postcheck end")
	requestWait(t, closeDone, "actual request/F2/callback end")
	if postRead {
		if count != 0 || readErr == nil || !bytes.Equal(buffer, make([]byte, len(buffer))) {
			t.Fatal("cancelled held return left caller bytes", count, readErr)
		}
	} else if count != 32 || readErr != nil || !bytes.Equal(buffer[:32], plain[13:45]) || !bytes.Equal(buffer[32:], bytes.Repeat([]byte{0xa5}, 32)) {
		t.Fatal("completed original read changed during physical join", count, readErr)
	}
	s.afterRead = nil
	if s.Check(t.Context()) != nil || retainedCharge(s) != charge {
		t.Fatal("local attempt close retired original snapshot")
	}
	if got := retainedBytes(t, s, 7, 16); !bytes.Equal(got, plain[7:23]) {
		t.Fatal("successor did not read original exact range")
	}
}

func TestOriginalMetadataRetainedAttempts(t *testing.T) {
	t.Run("same-snapshot-two-numeric-ranges", func(t *testing.T) {
		f, s, plain := retainedOriginal(t)
		f.releaseAll()
		file, tree, digest, expiry, charge := s.file, s.meta, s.digest, s.ExpiresAt(), retainedCharge(s)
		for _, r := range [][2]int64{{13, 32}, {65549, 64}, {7, 17}} {
			if got := retainedBytes(t, s, r[0], r[1]); !bytes.Equal(got, plain[r[0]:r[0]+r[1]]) {
				t.Fatal("sequential original byte range differs")
			}
			if s.file != file || s.meta != tree || s.digest != digest || s.ExpiresAt() != expiry || retainedCharge(s) != charge {
				t.Fatal("attempt replaced original snapshot or expiry")
			}
		}
	})
	t.Run("actual-plain-segment-window-reopen", func(t *testing.T) {
		const blockSize = 65536
		articles := make(map[string][]byte)
		plain := make([]byte, 0, 24*blockSize)
		meta := requestMetadata(24 * blockSize)
		meta.SegmentData = nil
		for i := 0; i < 24; i++ {
			id := fmt.Sprintf("retained-varying-%02d", i)
			block := make([]byte, blockSize)
			for j := range block {
				block[j] = byte((i*37 + j*17 + j/251) % 256)
			}
			articles[id] = block
			plain = append(plain, block...)
			meta.SegmentData = append(meta.SegmentData, &metapb.SegmentData{Id: id, SegmentSize: blockSize, EndOffset: blockSize - 1})
		}
		f := retainedMappedFixture(t, articles)
		f.write(t, meta)
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		t.Cleanup(f.releaseAll)
		f.releaseAll()
		f.remote.configGetter, f.remote.poolManager = nil, nil
		for _, r := range [][2]int64{{13, 23 * blockSize}, {21*blockSize + 19, 64}, {31, 23 * blockSize}} {
			if got := retainedBytes(t, s, r[0], r[1]); !bytes.Equal(got, plain[r[0]:r[0]+r[1]]) {
				t.Fatal("real progressive-window successor used wrong offset")
			}
		}
		if f.bodyCalls.Load() < 22 {
			t.Fatal("real progressive NNTP window was not crossed")
		}
	})
	t.Run("actual-aes-header-reopen", func(t *testing.T) {
		plain := pinnedPattern(131072)
		payload := append([]byte(nil), plain...)
		key, iv := []byte("0123456789abcdef0123456789abcdef"), []byte("0123456789abcdef")
		block, err := stdaes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(payload, payload)
		meta := requestMetadata(int64(len(plain)))
		meta.Encryption = metapb.Encryption_HEADERS
		meta.Password, meta.Salt = base64.StdEncoding.EncodeToString(key), base64.StdEncoding.EncodeToString(iv)
		f := requestRealFixture(t, payload, false)
		f.write(t, meta)
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		t.Cleanup(f.releaseAll)
		f.releaseAll()
		for _, r := range [][2]int64{{65536, 32}, {13, 64}} {
			if got := retainedBytes(t, s, r[0], r[1]); !bytes.Equal(got, plain[r[0]:r[0]+r[1]]) {
				t.Fatal("real AES successor bytes differ")
			}
		}
	})
	t.Run("actual-rclone-header-and-range-reopen", func(t *testing.T) {
		plain := pinnedPattern(131072)
		c, err := rclone.NewCipher(rclone.NameEncryptionOff, "synthetic-password", "synthetic-salt", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		encrypted, err := c.EncryptData(bytes.NewReader(plain), nil)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(encrypted)
		if err != nil {
			t.Fatal(err)
		}
		meta := requestMetadata(int64(len(plain)))
		meta.Encryption, meta.Password, meta.Salt = metapb.Encryption_RCLONE, "", ""
		meta.SegmentData[0].SegmentSize, meta.SegmentData[0].EndOffset = int64(len(payload)), int64(len(payload)-1)
		f := requestRealFixture(t, payload, false)
		f.remote.configGetter = func() *config.AltMountConfig {
			return &config.AltMountConfig{RClone: config.RCloneConfig{Password: "synthetic-password", Salt: "synthetic-salt"}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
		}
		f.write(t, meta)
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		t.Cleanup(f.releaseAll)
		f.releaseAll()
		f.remote.configGetter, f.remote.poolManager, f.remote.rcloneCipher = nil, nil, nil
		for _, r := range [][2]int64{{65536, 32}, {13, 64}} {
			if got := retainedBytes(t, s, r[0], r[1]); !bytes.Equal(got, plain[r[0]:r[0]+r[1]]) {
				t.Fatal("real rclone successor bytes differ")
			}
		}
		if f.bodyCalls.Load() < 4 {
			t.Fatal("real encrypted header/range attempts not reopened")
		}
	})
	t.Run("legacy-one-open-semantics-preserved", func(t *testing.T) {
		f, s, plain := retainedOriginal(t)
		f.releaseAll()
		r, err := s.Open(t.Context(), 13, 16)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if _, err = s.Open(t.Context(), 0, 16); err == nil {
			t.Fatal("legacy duplicate admitted")
		}
		if _, err = s.OpenAttempt(t.Context(), 0, 16); err == nil {
			t.Fatal("legacy source entered retained-attempt mode")
		}
		data := make([]byte, 16)
		if _, err = io.ReadFull(r, data); err != nil || !bytes.Equal(data, plain[13:29]) {
			t.Fatal("legacy duplicate retired working source", err)
		}
		if err = r.Close(); err != nil || s.Check(t.Context()) == nil {
			t.Fatal("legacy close no longer retires whole snapshot", err)
		}
		failed := pinnedCapture(t, s.owner, t.Context())
		if _, err = failed.Open(nil, -1, 0); err == nil || !failed.opened {
			t.Fatal("legacy failed Open no longer consumes before validation")
		}
		requestWait(t, failed.done, "legacy failed Open physical retirement")
	})
	t.Run("legacy-attempt-mode-mixing-denied", func(t *testing.T) {
		f, s, plain := retainedOriginal(t)
		f.releaseAll()
		a := retainedAttempt(t, s, t.Context(), 13, 16)
		if _, err := s.Open(t.Context(), 0, 16); err == nil {
			t.Fatal("retained snapshot entered legacy mode")
		}
		data := make([]byte, 16)
		if _, err := io.ReadFull(a, data); err != nil || !bytes.Equal(data, plain[13:29]) {
			t.Fatal("mode denial retired working attempt", err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Open(t.Context(), 0, 16); err == nil {
			t.Fatal("closed retained attempt reset permanent mode")
		}
	})
	t.Run("active-or-closing-attempt-zero-new-provider", func(t *testing.T) {
		f, s, plain := retainedOriginal(t)
		f.releaseAll()
		a := retainedAttempt(t, s, t.Context(), 13, 16)
		before := f.bodyCalls.Load()
		if _, err := s.OpenAttempt(t.Context(), 0, 16); err == nil || f.bodyCalls.Load() != before {
			t.Fatal("active attempt admitted another provider")
		}
		data := make([]byte, 16)
		if _, err := io.ReadFull(a, data); err != nil || !bytes.Equal(data, plain[13:29]) {
			t.Fatal("duplicate changed original bytes", err)
		}
	})
	t.Run("held-body-close-before-next-open", func(t *testing.T) { retainedHeldEnd(t, false) })
	t.Run("held-postread-return-before-next-open", func(t *testing.T) { retainedHeldEnd(t, true) })
	t.Run("local-cancel-preserves-original-snapshot", func(t *testing.T) {
		f, s, plain := retainedOriginal(t)
		f.releaseAll()
		ctx, cancel := context.WithCancel(t.Context())
		a := retainedAttempt(t, s, ctx, 13, 16)
		cancel()
		requestWait(t, a.CleanupDone(), "local cancellation actual attempt")
		if s.Check(t.Context()) != nil {
			t.Fatal("local cancellation retired retained source")
		}
		if got := retainedBytes(t, s, 65549, 16); !bytes.Equal(got, plain[65549:65565]) {
			t.Fatal("local cancel successor differs")
		}
		bad, stop := context.WithCancel(t.Context())
		stop()
		if _, err := s.OpenAttempt(bad, 0, 16); err == nil {
			t.Fatal("cancelled operation admitted")
		}
		a.physicalFailure(context.Canceled)
		if s.Check(bad) == nil || s.Check(t.Context()) != nil {
			t.Fatal("typed local cancellation retired physical snapshot")
		}
		if _, err := s.OpenAttempt(nil, -1, 0); err == nil || s.Check(t.Context()) != nil {
			t.Fatal("invalid operation retired original")
		}
	})
	t.Run("fixed-expiry-not-renewed-by-attempt", func(t *testing.T) {
		plain := pinnedPattern(131072)
		f := requestRealFixture(t, plain, false)
		f.write(t, requestMetadata(int64(len(plain))))
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		s := pinnedCapture(t, pinnedNamespace(t, f), ctx)
		t.Cleanup(f.releaseAll)
		f.releaseAll()
		expiry := s.ExpiresAt()
		deadline, _ := ctx.Deadline()
		if !expiry.Equal(deadline) {
			t.Fatal("capture did not preserve original deadline")
		}
		for i := 0; i < 4; i++ {
			_ = retainedBytes(t, s, int64(i*17), 16)
			if s.ExpiresAt() != expiry {
				t.Fatal("attempt renewed original expiry")
			}
		}
		requestWait(t, s.done, "actual fixed original deadline retirement")
		if _, err := s.OpenAttempt(t.Context(), 0, 16); err == nil {
			t.Fatal("expired original reopened")
		}
	})
	t.Run("metadata-digest-or-path-change-terminal", func(t *testing.T) {
		f, s, _ := retainedOriginal(t)
		f.releaseAll()
		_ = retainedBytes(t, s, 13, 16)
		before := f.bodyCalls.Load()
		ctx, cancel := context.WithCancel(t.Context())
		a := retainedAttempt(t, s, ctx, 13, 16)
		path := pinnedStored(t, f)
		if err := os.Rename(path, path+".original"); err != nil {
			t.Fatal(err)
		}
		meta := requestMetadata(131072)
		meta.SegmentData[0].Id = "foreign-retained-replacement"
		f.write(t, meta)
		observed := s.checkPhysical(t.Context()) // Genuine held/named mismatch.
		if observed == nil {
			t.Fatal("replacement did not change original physical identity")
		}
		cancel() // Later cancellation cannot erase this already observed mismatch.
		a.physicalFailure(observed)
		if _, err := s.OpenAttempt(t.Context(), 0, 16); err == nil || f.bodyCalls.Load() != before {
			t.Fatal("changed physical metadata admitted provider")
		}
		requestWait(t, s.done, "changed original source retirement")
		pinnedClosed(t, s.file)
	})
	t.Run("attempt-cleanup-does-not-refund-snapshot", func(t *testing.T) {
		f, s, _ := retainedOriginal(t)
		f.releaseAll()
		charge := retainedCharge(s)
		for i := 0; i < 12; i++ {
			_ = retainedBytes(t, s, int64(i*13), 16)
			if retainedCharge(s) != charge || s.meta == nil {
				t.Fatal("attempt refunded original metadata")
			}
		}
		if err := s.CloseAndJoin(); err != nil {
			t.Fatal(err)
		}
		if retainedCharge(s) != 0 || s.meta != nil || s.decoder.manager != nil {
			t.Fatal("whole source did not release original memory charge")
		}
		pinnedClosed(t, s.file)
	})
	t.Run("closed-attempt-clears-decoder-tree-references", func(t *testing.T) {
		f, s, _ := retainedOriginal(t)
		f.releaseAll()
		a := retainedAttempt(t, s, t.Context(), 13, 16)
		if _, err := io.ReadFull(a, make([]byte, 16)); err != nil {
			t.Fatal(err)
		}
		vmf := a.raw.File.(*MetadataVirtualFile)
		if vmf.fileMeta != s.meta || vmf.poolManager == nil {
			t.Fatal("attempt did not own original decoded tree")
		}
		if err := a.CloseAndJoin(); err != nil {
			t.Fatal(err)
		}
		if vmf.fileMeta != nil || vmf.poolManager != nil || vmf.rcloneCipher != nil || vmf.aesCipher != nil || vmf.globalPassword != "" || vmf.globalSalt != "" || s.meta == nil {
			t.Fatal("closed caller handle retained charged decoder/tree")
		}
		b := retainedAttempt(t, s, t.Context(), 29, 16)
		if err := a.CloseAndJoin(); err != nil || b.Check(t.Context()) != nil {
			t.Fatal("stale close retired successor")
		}
	})
	t.Run("source-close-signals-before-attempt-join", func(t *testing.T) {
		f, s, _ := retainedOriginal(t)
		second := pinnedCapture(t, s.owner, t.Context())
		a := retainedAttempt(t, s, t.Context(), 13, 16)
		b := retainedAttempt(t, second, t.Context(), 29, 16)
		one, two, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		started, closing := false, false
		t.Cleanup(func() {
			f.releaseAll()
			s.owner.SignalClose()
			if started {
				requestWait(t, one, "first real read")
				requestWait(t, two, "second real read")
			}
			if closing {
				requestWait(t, closed, "namespace actual cleanup")
			}
		})
		started = true
		go func() { defer close(one); _, _ = a.Read(make([]byte, 16)) }()
		go func() { defer close(two); _, _ = b.Read(make([]byte, 16)) }()
		retainedUntil(t, "two genuine BODY calls were not held", func() bool { return f.pool.calls.Load() >= 2 })
		closing = true
		go func() { defer close(closed); _ = s.owner.CloseAndJoin() }()
		requestWait(t, a.Context().Done(), "first attempt signal")
		requestWait(t, b.Context().Done(), "second attempt signal")
		requestPending(t, closed, "all original attempts physical join")
		f.releaseAll()
		requestWait(t, one, "first actual read end")
		requestWait(t, two, "second actual read end")
		requestWait(t, closed, "namespace physical end")
	})
	t.Run("copied-zero-and-unsupported-attempt-denied", func(t *testing.T) {
		f, s, _ := retainedOriginal(t)
		f.releaseAll()
		a := retainedAttempt(t, s, t.Context(), 13, 16)
		copy := *a
		for _, invalid := range []*OriginalMetadataAttempt{nil, {}, &copy} {
			buffer := bytes.Repeat([]byte{1}, 16)
			if n, err := invalid.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, make([]byte, 16)) || invalid.Check(t.Context()) == nil || invalid.CloseAndJoin() == nil || invalid.CleanupDone() != nil {
				t.Fatal("copied/zero actor fabricated ownership")
			}
		}
		if _, err := SourceCleanupDone(io.NopCloser(a)); err == nil {
			t.Fatal("structural wrapper became actual F2 completion")
		}
		encoded, err := json.Marshal(a)
		if err != nil || string(encoded) != "null" || fmt.Sprintf("%+v", a) != "OriginalMetadataAttempt(opaque)" {
			t.Fatal("attempt leaked retained state", err)
		}
		if a.Check(t.Context()) != nil {
			t.Fatal("rejected copy retired original")
		}
	})
}
