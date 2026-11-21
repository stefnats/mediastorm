//go:build linux

package nzbfilesystem

import (
	"bytes"
	"context"
	stdaes "crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/encryption/rclone"
	metapb "novastream/internal/nzb/metadata/proto"
)

func pinnedNamespace(t *testing.T, f *requestFixture) *OriginalMetadataNamespace {
	t.Helper()
	n, e := f.fs.CaptureOriginalMetadataNamespace(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		f.releaseAll()
		if e := n.CloseAndJoin(); e != nil {
			t.Error(e)
		}
	})
	return n
}
func pinnedCapture(t *testing.T, n *OriginalMetadataNamespace, ctx context.Context) *OriginalMetadataSource {
	t.Helper()
	s, e := n.Capture(ctx, "movie.meta")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.CloseAndJoin(); e != nil {
			t.Error(e)
		}
	})
	return s
}
func pinnedRead(t *testing.T, f *requestFixture, s *OriginalMetadataSource, start, length int64) []byte {
	t.Helper()
	f.releaseAll()
	r, e := s.Open(t.Context(), start, length)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = r.Close() })
	data := make([]byte, int(length))
	if _, e = io.ReadFull(r, data); e != nil {
		t.Fatal(e)
	}
	if count, e := r.Read(make([]byte, 1)); count != 0 || e != io.EOF {
		t.Fatal("original numeric range exceeded", count, e)
	}
	return data
}
func pinnedStored(t *testing.T, f *requestFixture) string {
	t.Helper()
	return filepath.Join(f.service.RootPath(), "movie.meta")
}
func pinnedClosed(t *testing.T, f *os.File) {
	t.Helper()
	if _, e := f.Stat(); !errors.Is(e, os.ErrClosed) {
		t.Fatal("actual retained descriptor not closed", e)
	}
}

type pinnedArticleAudit struct {
	nntppool.UsenetConnectionPool
	mu  sync.Mutex
	ids []string
}

func (p *pinnedArticleAudit) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	p.mu.Lock()
	p.ids = append(p.ids, id)
	p.mu.Unlock()
	return p.UsenetConnectionPool.Body(ctx, id, w, groups)
}

func pinnedPattern(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*17 + i/251) % 256)
	}
	return data
}

// The real Read has already filled caller memory. Holding only its return
// exposes the physical-retarget/output boundary without substituting any bytes.
func pinnedSuccessfulRetargetZero(t *testing.T) {
	t.Helper()
	plain := pinnedPattern(131072)
	f := requestRealFixture(t, plain, false)
	f.write(t, requestMetadata(int64(len(plain))))
	n := pinnedNamespace(t, f)
	source := pinnedCapture(t, n, t.Context())
	r, e := source.Open(t.Context(), 13, 32)
	if e != nil {
		t.Fatal(e)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	started := false
	source.afterRead = func() { close(entered); <-release }
	buffer := bytes.Repeat([]byte{0xa5}, 64)
	var readN int
	var readErr error
	t.Cleanup(func() {
		source.SignalClose()
		f.releaseAll()
		unblock()
		if started {
			requestWait(t, done, "actual post-Read return")
		}
		_ = r.Close()
	})
	f.releaseAll()
	started = true
	go func() { defer close(done); readN, readErr = r.Read(buffer) }()
	requestWait(t, entered, "actual successful BODY and raw.Read return")
	if f.bodyCalls.Load() == 0 || !bytes.Equal(buffer[:32], plain[13:45]) || source.Check(t.Context()) != nil {
		t.Fatal("actual original bytes/receipt missing before held return")
	}
	if _, e = source.file.Stat(); e != nil {
		t.Fatal("actual original metadata descriptor absent", e)
	}
	n.mu.Lock()
	charged := n.used
	n.mu.Unlock()
	if charged == 0 {
		t.Fatal("original pending Read charge missing")
	}
	path := pinnedStored(t, f)
	if e = os.Rename(path, path+".original"); e != nil {
		t.Fatal(e)
	}
	foreign := requestMetadata(int64(len(plain)))
	foreign.SegmentData[0].Id = "foreign-replacement"
	f.write(t, foreign)
	requestPending(t, r.CleanupDone(), "actual pending return owner")
	n.mu.Lock()
	stillCharged := n.used
	n.mu.Unlock()
	if stillCharged != charged {
		t.Fatal("pending return refunded metadata charge")
	}
	unblock()
	requestWait(t, done, "post-check and caller-memory clear")
	if readN != 0 || readErr == nil || !bytes.Equal(buffer, make([]byte, len(buffer))) {
		t.Fatal("terminal changed Read retained caller bytes", readN, readErr)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	requestWait(t, r.CleanupDone(), "actual changed-source physical end")
	pinnedClosed(t, source.file)
}

func TestPinnedOriginalNZBMetadata(t *testing.T) {
	t.Run("regular-original-metadata-receipt", func(t *testing.T) {
		f := requestRealFixture(t, []byte("original synthetic media"), false)
		f.write(t, requestMetadata(24))
		n := pinnedNamespace(t, f)
		s := pinnedCapture(t, n, t.Context())
		info, e := s.Info(t.Context())
		if e != nil || info.Size != 24 {
			t.Fatal("actual metadata info", info, e)
		}
		copySource := *s
		if copySource.Check(t.Context()) == nil {
			t.Fatal("copied physical receipt accepted")
		}
		for _, actor := range []any{n, *n, s, *s} {
			if data, e := json.Marshal(actor); e != nil || string(data) != "null" {
				t.Fatal("original actor JSON exposed fields", e)
			}
			if printed := fmt.Sprintf("%#v", actor); strings.Contains(printed, f.service.RootPath()) || !strings.Contains(printed, "opaque") {
				t.Fatal("original actor formatting exposed source")
			}
		}
		var imported OriginalMetadataSource
		if json.Unmarshal([]byte("{}"), &imported) == nil {
			t.Fatal("JSON imported original actor")
		}
		old := s.file
		if e = s.CloseAndJoin(); e != nil {
			t.Fatal(e)
		}
		pinnedClosed(t, old)
		if s.Check(t.Context()) == nil {
			t.Fatal("closed receipt accepted")
		}
		n.mu.Lock()
		remaining := n.used
		n.mu.Unlock()
		if remaining != 0 {
			t.Fatal("ended actual metadata retained allocation charge", remaining)
		}
	})
	t.Run("strict-path-and-args-denied", func(t *testing.T) {
		f := requestRealFixture(t, nil, false)
		f.write(t, requestMetadata(32))
		n := pinnedNamespace(t, f)
		for _, name := range []string{"", "/movie.meta", "movie.meta/", "./movie.meta", "../movie.meta", "a//movie.meta", "movie.meta?ARGS?origin=x", "movie%2emeta", "queue/1/movie.meta", "webdav/movie.meta", "https://example.invalid/movie.meta", "movie.meta#x", "a\\movie.meta", "movie\x00.meta"} {
			if s, e := n.Capture(t.Context(), name); e == nil || s != nil {
				t.Fatal("noncanonical source accepted", name)
			}
		}
		if f.bodyCalls.Load() != 0 {
			t.Fatal("rejected path reached NNTP")
		}
	})
	t.Run("symlink-parent-and-leaf-denied", func(t *testing.T) {
		f := requestRealFixture(t, nil, false)
		f.write(t, requestMetadata(32))
		n := pinnedNamespace(t, f)
		root := f.service.RootPath()
		if e := os.Symlink("movie.meta", filepath.Join(root, "alias.meta")); e != nil {
			t.Fatal(e)
		}
		if e := os.Symlink(root, filepath.Join(root, "alias")); e != nil {
			t.Fatal(e)
		}
		for _, name := range []string{"alias.meta", "alias/movie.meta"} {
			if s, e := n.Capture(t.Context(), name); e == nil || s != nil {
				t.Fatal("actual symlink followed", name)
			}
		}
		if f.bodyCalls.Load() != 0 {
			t.Fatal("symlink denial opened actual article")
		}
	})
	t.Run("fifo-directory-and-oversize-before-decode", func(t *testing.T) {
		f := requestRealFixture(t, nil, false)
		f.write(t, requestMetadata(32))
		n := pinnedNamespace(t, f)
		root := f.service.RootPath()
		if e := unix.Mkfifo(filepath.Join(root, "pipe.meta"), 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.Mkdir(filepath.Join(root, "dir.meta"), 0700); e != nil {
			t.Fatal(e)
		}
		big, e := os.Create(filepath.Join(root, "big.meta"))
		if e != nil {
			t.Fatal(e)
		}
		if e = big.Truncate(originalMetadataLimit + 1); e != nil {
			_ = big.Close()
			t.Fatal(e)
		}
		_ = big.Close()
		for _, name := range []string{"pipe.meta", "dir.meta", "big.meta"} {
			if s, e := n.Capture(t.Context(), name); e == nil || s != nil {
				t.Fatal("nonregular/overbound metadata decoded", name)
			}
		}
		// A syntactically valid wire stream with too many actual segment messages
		// must fail before allocating the repeated-message tree.
		excessive := requestMetadata(32)
		excessive.SegmentData = make([]*metapb.SegmentData, originalMetadataSegments+1)
		for i := range excessive.SegmentData {
			excessive.SegmentData[i] = &metapb.SegmentData{Id: "synthetic", SegmentSize: 32, EndOffset: 31}
		}
		raw, e := proto.Marshal(excessive)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, "count.meta"), raw, 0600); e != nil {
			t.Fatal(e)
		}
		if s, e := n.Capture(t.Context(), "count.meta"); e == nil || s != nil {
			t.Fatal("actual wire segment overflow accepted")
		}
		// Unknown/deeper, duplicate and oversized strings are rejected by prewire.
		for i, raw := range [][]byte{{0x0a, 0x01, 0x00}, {0x08, 0x01, 0x08, 0x01}, {0x52, 0x01, 0x00}} {
			name := []string{"wrong.meta", "duplicate.meta", "unknown.meta"}[i]
			if e = os.WriteFile(filepath.Join(root, name), raw, 0600); e != nil {
				t.Fatal(e)
			}
			if s, e := n.Capture(t.Context(), name); e == nil || s != nil {
				t.Fatal("malformed prewire accepted", name)
			}
		}
		// Real large valid metadata charges raw+decoded objects, not just wire bytes.
		large := requestMetadata(32)
		large.SegmentData = make([]*metapb.SegmentData, 15000)
		for i := range large.SegmentData {
			large.SegmentData[i] = &metapb.SegmentData{Id: strings.Repeat("s", 1024), SegmentSize: 32, EndOffset: 31}
		}
		raw, e = proto.Marshal(large)
		if e != nil {
			t.Fatal(e)
		}
		for _, name := range []string{"large-a.meta", "large-b.meta"} {
			if e = os.WriteFile(filepath.Join(root, name), raw, 0600); e != nil {
				t.Fatal(e)
			}
		}
		first, e := n.Capture(t.Context(), "large-a.meta")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = first.CloseAndJoin() })
		if second, e := n.Capture(t.Context(), "large-b.meta"); e == nil || second != nil {
			t.Fatal("aggregate decoded allocation bound ignored")
		}
		if f.bodyCalls.Load() != 0 {
			t.Fatal("predecode denial reached provider")
		}
	})
	t.Run("original-name-and-metadata-aba-denied", func(t *testing.T) {
		f := requestRealFixture(t, nil, false)
		f.write(t, requestMetadata(32))
		n := pinnedNamespace(t, f)
		s := pinnedCapture(t, n, t.Context())
		path := pinnedStored(t, f)
		before, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.Rename(path, path+".original"); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, before, 0644); e != nil {
			t.Fatal(e)
		}
		if s.Check(t.Context()) == nil {
			t.Fatal("same-name/hash replacement adopted")
		}
		if f.bodyCalls.Load() != 0 {
			t.Fatal("retargeted source opened foreign body")
		}
	})
	t.Run("actual-plain-nntp-source", func(t *testing.T) {
		block := pinnedPattern(65536)
		whole := bytes.Repeat(block, 24)
		f := requestRealFixture(t, block, false)
		meta := requestMetadata(int64(len(whole)))
		meta.SegmentData = nil
		for i := 0; i < 24; i++ {
			meta.SegmentData = append(meta.SegmentData, &metapb.SegmentData{Id: fmt.Sprintf("synthetic-window-%d", i), SegmentSize: int64(len(block)), EndOffset: int64(len(block) - 1)})
		}
		f.write(t, meta)
		source := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		// Changing the original mutable factory after Capture must not replace the
		// retained actual decoder inputs during the original one-use Open.
		f.remote.configGetter = nil
		f.remote.poolManager = nil
		f.remote.rcloneCipher = nil
		f.remote.aesCipher = nil
		start, length := int64(13), int64(23*len(block))
		got := pinnedRead(t, f, source, start, length)
		if !bytes.Equal(got, whole[start:start+length]) {
			t.Fatal("actual progressive-window reopen used a relative offset")
		}
		if f.bodyCalls.Load() < 21 {
			t.Fatal("actual20segment window never reopened", f.bodyCalls.Load())
		}
	})
	t.Run("actual-aes-header-source", func(t *testing.T) {
		plain := pinnedPattern(131072)
		payload := append([]byte(nil), plain...)
		key := []byte("0123456789abcdef0123456789abcdef")
		iv := []byte("0123456789abcdef")
		block, e := stdaes.NewCipher(key)
		if e != nil {
			t.Fatal(e)
		}
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(payload, payload)
		meta := requestMetadata(int64(len(plain)))
		meta.Encryption = metapb.Encryption_HEADERS
		meta.Password = base64.StdEncoding.EncodeToString(key)
		meta.Salt = base64.StdEncoding.EncodeToString(iv)
		f := requestRealFixture(t, payload, false)
		f.write(t, meta)
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		if got := pinnedRead(t, f, s, 65536, 32); !bytes.Equal(got, plain[65536:65568]) {
			t.Fatal("actual original AES header/range differs")
		}
	})
	t.Run("actual-rclone-reopen-same-snapshot", func(t *testing.T) {
		plain := pinnedPattern(131072)
		c, e := rclone.NewCipher(rclone.NameEncryptionOff, "synthetic-password", "synthetic-salt", false, nil)
		if e != nil {
			t.Fatal(e)
		}
		encrypted, e := c.EncryptData(bytes.NewReader(plain), nil)
		if e != nil {
			t.Fatal(e)
		}
		payload, e := io.ReadAll(encrypted)
		if e != nil {
			t.Fatal(e)
		}
		meta := requestMetadata(int64(len(plain)))
		meta.Encryption = metapb.Encryption_RCLONE
		meta.Password = ""
		meta.Salt = ""
		meta.SegmentData[0].SegmentSize = int64(len(payload))
		meta.SegmentData[0].EndOffset = int64(len(payload) - 1)
		f := requestRealFixture(t, payload, false)
		f.remote.configGetter = func() *config.AltMountConfig {
			return &config.AltMountConfig{RClone: config.RCloneConfig{Password: "synthetic-password", Salt: "synthetic-salt"}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
		}
		f.write(t, meta)
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		f.remote.configGetter = nil
		f.remote.rcloneCipher = nil
		f.remote.poolManager = nil
		if got := pinnedRead(t, f, s, 65536, 32); !bytes.Equal(got, plain[65536:65568]) {
			t.Fatal("actual original rclone range/reopened reader differs")
		}
		if f.bodyCalls.Load() < 2 {
			t.Fatal("real header/reopen did not use original article")
		}
	})
	t.Run("retarget-before-header-no-foreign-body", func(t *testing.T) {
		pinnedSuccessfulRetargetZero(t)
		f := requestRealFixture(t, nil, true)
		f.write(t, requestMetadata(32))
		audit := &pinnedArticleAudit{UsenetConnectionPool: f.pool.UsenetConnectionPool}
		f.pool.UsenetConnectionPool = audit
		db, e := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "health.db")})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = db.Close() })
		repo := database.NewHealthRepository(db.Connection())
		f.remote.healthRepository = repo
		for _, name := range []string{"movie", "movie.meta"} {
			if e = repo.UpdateFileHealth(name, database.HealthStatusHealthy, nil, nil, nil); e != nil {
				t.Fatal(e)
			}
		}
		n := pinnedNamespace(t, f)
		s := pinnedCapture(t, n, t.Context())
		r, e := s.Open(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		readDone := make(chan struct{})
		var readN int
		var readErr error
		started := false
		t.Cleanup(func() {
			s.SignalClose()
			f.releaseAll()
			if started {
				requestWait(t, readDone, "actual missing article Read")
			}
			_ = r.Close()
		})
		started = true
		go func() { defer close(readDone); readN, readErr = r.Read(make([]byte, 32)) }()
		requestWait(t, f.pool.returned, "actual missing BODY delegate return")
		foreign := requestMetadata(32)
		foreign.SegmentData[0].Id = "foreign-same-name"
		f.write(t, foreign)
		replacement, e := os.ReadFile(pinnedStored(t, f))
		if e != nil {
			t.Fatal(e)
		}
		f.releaseAll()
		requestWait(t, readDone, "actual missing article/health callbacks")
		if readN != 0 || readErr == nil {
			t.Fatal("changed physical source delivered bytes", readN, readErr)
		}
		_ = r.Close()
		after, e := os.ReadFile(pinnedStored(t, f))
		if e != nil || !bytes.Equal(after, replacement) {
			t.Fatal("old health callback modified replacement metadata", e)
		}
		var rows, bad int
		if e = db.Connection().QueryRow("SELECT count(*),coalesce(sum(CASE WHEN status!=? THEN 1 ELSE 0 END),0) FROM file_health", database.HealthStatusHealthy).Scan(&rows, &bad); e != nil || rows != 2 || bad != 0 {
			t.Fatal("old health callback modified replacement health rows", rows, bad, e)
		}
		audit.mu.Lock()
		defer audit.mu.Unlock()
		if len(audit.ids) == 0 {
			t.Fatal("actual old article not requested")
		}
		for _, id := range audit.ids {
			if id != "synthetic-request" {
				t.Fatal("reopen adopted foreign article", id)
			}
		}
	})
	t.Run("late-opener-physically-closed", func(t *testing.T) {
		f := requestRealFixture(t, []byte("synthetic source data"), false)
		f.write(t, requestMetadata(21))
		f.manager.entered = make(chan struct{})
		f.manager.release = make(chan struct{})
		n := pinnedNamespace(t, f)
		s := pinnedCapture(t, n, t.Context())
		r, e := s.Open(t.Context(), 0, 16)
		if e != nil {
			t.Fatal(e)
		}
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		started := false
		closeStarted := false
		t.Cleanup(func() {
			s.SignalClose()
			f.releaseAll()
			if started {
				requestWait(t, readDone, "actual late Read")
			}
			if closeStarted {
				requestWait(t, closeDone, "late original source Close")
			}
			_ = r.Close()
		})
		started = true
		go func() { defer close(readDone); _, _ = r.Read(make([]byte, 16)) }()
		requestWait(t, f.manager.entered, "actual GetPool entered")
		closeStarted = true
		go func() { defer close(closeDone); _ = r.Close() }()
		requestPending(t, closeDone, "held real opener")
		f.releaseAll()
		requestWait(t, readDone, "late actual reader return")
		requestWait(t, closeDone, "late created reader physical join")
		pinnedClosed(t, s.file)
	})
	t.Run("held-nntp-worker-sourcecleanupdone", func(t *testing.T) {
		f := requestRealFixture(t, bytes.Repeat([]byte("h"), 131072), false)
		f.write(t, requestMetadata(131072))
		s := pinnedCapture(t, pinnedNamespace(t, f), t.Context())
		r, e := s.Open(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		started, closing := false, false
		t.Cleanup(func() {
			s.SignalClose()
			f.releaseAll()
			if started {
				requestWait(t, readDone, "held BODY Read")
			}
			if closing {
				requestWait(t, closeDone, "held source Close")
			}
			_ = r.Close()
		})
		started = true
		go func() { defer close(readDone); _, _ = r.Read(make([]byte, 32)) }()
		requestWait(t, f.pool.returned, "actual held BODY return")
		closing = true
		go func() { defer close(closeDone); _ = r.Close() }()
		requestPending(t, r.CleanupDone(), "original physical cleanup receipt")
		requestPending(t, closeDone, "original held worker join")
		f.releaseAll()
		requestWait(t, readDone, "actual BODY Read end")
		requestWait(t, closeDone, "actual source Close end")
		requestWait(t, r.CleanupDone(), "actual source cleanup completion")
		if vmf := r.raw.File.(*MetadataVirtualFile); vmf.fileMeta != nil || vmf.globalPassword != "" || vmf.globalSalt != "" {
			t.Fatal("closed caller-held stream retained charged metadata/credentials")
		}
	})
	t.Run("one-open-and-fixed-original-expiry", func(t *testing.T) {
		plain := pinnedPattern(131072)
		f := requestRealFixture(t, plain, false)
		f.write(t, requestMetadata(int64(len(plain))))
		n := pinnedNamespace(t, f)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		source := pinnedCapture(t, n, ctx)
		wanted, _ := ctx.Deadline()
		if source.ExpiresAt() != wanted {
			t.Fatal("original expiry renewed")
		}
		stale, stop := context.WithCancel(t.Context())
		stop()
		if source.Check(stale) == nil {
			t.Fatal("canceled Check succeeded")
		}
		if _, e := source.Info(stale); e == nil {
			t.Fatal("canceled Info succeeded")
		}
		if source.Check(nil) == nil {
			t.Fatal("nil Check succeeded")
		}
		if n.MatchesOriginal(stale, f.fs, f.service.RootPath()) == nil {
			t.Fatal("canceled namespace check succeeded")
		}
		if source.Context().Err() != nil || n.MatchesOriginal(t.Context(), f.fs, f.service.RootPath()) != nil {
			t.Fatal("operation-local cancellation retired original")
		}
		r, e := source.Open(ctx, 65536, 32)
		if e != nil {
			t.Fatal(e)
		}
		if second, e := source.Open(ctx, 65536, 32); e == nil || second != nil {
			t.Fatal("original physical receipt opened twice")
		}
		if source.Context().Err() != nil {
			t.Fatal("duplicate Open retired first stream")
		}
		f.releaseAll()
		got := make([]byte, 32)
		if _, e = io.ReadFull(r, got); e != nil || !bytes.Equal(got, plain[65536:65568]) {
			t.Fatal("original stream not readable after local denials", e)
		}
		for _, actor := range []any{r, *r} {
			data, e := json.Marshal(actor)
			if e != nil || string(data) != "null" || !strings.Contains(fmt.Sprintf("%#v", actor), "opaque") {
				t.Fatal("stream opaque data exposed", e)
			}
		}
		cancel()
		requestWait(t, r.CleanupDone(), "original fixed lifetime")
		if source.Check(t.Context()) == nil {
			t.Fatal("expired source renewed")
		}
	})
}
