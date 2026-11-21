package nzbfilesystem

import (
	"bufio"
	"bytes"
	"context"
	stdaes "crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/encryption/rclone"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzb/utils"
	"novastream/internal/usenet"
)

func requestWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("actual %s did not end", label)
	}
}
func requestPending(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s ended before owned worker", label)
	case <-time.After(20 * time.Millisecond):
	}
}
func requestYenc(payload []byte) string {
	var encoded strings.Builder
	for _, part := range payload {
		b := part + 42
		if b == 0 || b == 9 || b == 10 || b == 13 || b == 32 || b == 46 || b == 61 {
			encoded.WriteByte('=')
			b += 64
		}
		encoded.WriteByte(b)
	}
	return fmt.Sprintf("=ybegin line=1048576 size=%d name=synthetic.bin\r\n%s\r\n=yend size=%d crc32=%08x\r\n.\r\n", len(payload), encoded.String(), len(payload), crc32.ChecksumIEEE(payload))
}

type requestHeldPool struct {
	nntppool.UsenetConnectionPool
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (p *requestHeldPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	n, e := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.calls.Add(1)
	p.once.Do(func() { close(p.returned) })
	<-p.release
	return n, e
}

type requestPoolManager struct {
	pool             nntppool.UsenetConnectionPool
	entered, release chan struct{}
	once             sync.Once
}

func (m *requestPoolManager) GetPool() (nntppool.UsenetConnectionPool, error) {
	if m.entered != nil {
		m.once.Do(func() { close(m.entered) })
		<-m.release
	}
	return m.pool, nil
}
func (m *requestPoolManager) SetProviders([]nntppool.UsenetProviderConfig) error {
	return errors.New("fixture provider mutation forbidden")
}
func (m *requestPoolManager) ClearPool() error          { return errors.New("fixture pool mutation forbidden") }
func (m *requestPoolManager) HasPool() bool             { return true }
func (m *requestPoolManager) AvailableConnections() int { return 4 }

type requestFixture struct {
	fs        *NzbFilesystem
	remote    *MetadataRemoteFile
	service   *metadata.MetadataService
	pool      *requestHeldPool
	manager   *requestPoolManager
	accepted  chan struct{}
	bodyCalls atomic.Int32
}

func requestRealFixture(t *testing.T, payload []byte, missing bool) *requestFixture {
	t.Helper()
	value := &requestFixture{accepted: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})
	var wg sync.WaitGroup
	acceptDone := make(chan struct{})
	var acceptedOnce sync.Once
	go func() {
		defer close(acceptDone)
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			conns[conn] = struct{}{}
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(conns, conn); mu.Unlock() }()
				_, _ = io.WriteString(conn, "200 synthetic NNTP ready\r\n")
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					line := scanner.Text()
					switch {
					case strings.HasPrefix(line, "BODY "):
						value.bodyCalls.Add(1)
						acceptedOnce.Do(func() { close(value.accepted) })
						if missing {
							_, _ = io.WriteString(conn, "430 synthetic article missing\r\n")
						} else {
							_, _ = io.WriteString(conn, "222 synthetic body\r\n")
							if payload != nil {
								_, _ = io.WriteString(conn, requestYenc(payload))
							}
						}
					case line == "CAPABILITIES":
						_, _ = io.WriteString(conn, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(conn, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(conn, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(conn, "500 unsupported synthetic command\r\n")
					}
				}
			}()
		}
	}()
	actual, err := nntppool.NewConnectionPool(nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 4, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 200 * time.Millisecond, HealthCheckInterval: time.Hour})
	if err != nil {
		_ = listener.Close()
		<-acceptDone
		wg.Wait()
		t.Fatal(err)
	}
	value.pool = &requestHeldPool{UsenetConnectionPool: actual, release: make(chan struct{}), returned: make(chan struct{})}
	value.manager = &requestPoolManager{pool: value.pool}
	value.service = metadata.NewMetadataService(t.TempDir())
	value.remote = NewMetadataRemoteFile(value.service, nil, value.manager, func() *config.AltMountConfig {
		return &config.AltMountConfig{Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	})
	value.fs = NewNzbFilesystem(value.remote).(*NzbFilesystem)
	t.Cleanup(func() {
		select {
		case <-value.pool.release:
		default:
			close(value.pool.release)
		}
		if value.manager.release != nil {
			select {
			case <-value.manager.release:
			default:
				close(value.manager.release)
			}
		}
		actual.Quit()
		_ = listener.Close()
		<-acceptDone
		mu.Lock()
		for conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return value
}
func (f *requestFixture) write(t *testing.T, fileMeta *metapb.FileMetadata) {
	t.Helper()
	if err := f.service.WriteFileMetadata("movie", fileMeta); err != nil {
		t.Fatal(err)
	}
}
func (f *requestFixture) open(t *testing.T, ctx context.Context, args utils.PathWithArgs) *MetadataVirtualFile {
	t.Helper()
	ok, file, err := f.remote.OpenFile(ctx, "movie", args)
	if err != nil || !ok {
		t.Fatalf("actual open: %v", err)
	}
	value := file.(*MetadataVirtualFile)
	t.Cleanup(func() {
		f.releaseAll()
		if err := JoinSourceCleanup(value); err != nil {
			t.Errorf("source cleanup: %v", err)
		}
	})
	return value
}
func (f *requestFixture) releaseAll() {
	select {
	case <-f.pool.release:
	default:
		close(f.pool.release)
	}
	if f.manager.release != nil {
		select {
		case <-f.manager.release:
		default:
			close(f.manager.release)
		}
	}
}

func requestMetadata(size int64) *metapb.FileMetadata {
	return &metapb.FileMetadata{FileSize: size, ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "synthetic-request", SegmentSize: size, EndOffset: size - 1}}}
}
func requestJoin(t *testing.T, file io.ReadCloser) {
	t.Helper()
	if err := JoinSourceCleanup(file); err != nil {
		t.Fatal(err)
	}
}

type requestHeldHealthDB struct {
	*sql.DB
	entered, release, done chan struct{}
	once                   sync.Once
}

func (db *requestHeldHealthDB) Exec(query string, args ...any) (sql.Result, error) {
	db.once.Do(func() { close(db.entered) })
	<-db.release
	defer close(db.done)
	return db.DB.Exec(query, args...)
}

func TestMetadataRequestSourceLifetime(t *testing.T) {
	for _, name := range []string{"plain-blocked-read", "aes-reopened-inner-readers", "rclone-header-and-reopen-readers", "close-before-first-read", "read-close-race", "directory-cancel", "health-write-workers-joined", "late-created-reader-joined", "admission-not-refunded-early"} {
		t.Run(name, func(t *testing.T) {
			plain := bytes.Repeat([]byte("synthetic-media-"), 8192)
			payload := plain
			meta := requestMetadata(int64(len(plain)))
			if name == "aes-reopened-inner-readers" {
				key := []byte("0123456789abcdef0123456789abcdef")
				iv := []byte("0123456789abcdef")
				padded := (len(plain) + 15) / 16 * 16
				payload = make([]byte, padded)
				copy(payload, plain)
				block, e := stdaes.NewCipher(key)
				if e != nil {
					t.Fatal(e)
				}
				cipher.NewCBCEncrypter(block, iv).CryptBlocks(payload, payload)
				meta.Encryption = metapb.Encryption_HEADERS
				meta.Password = base64.StdEncoding.EncodeToString(key)
				meta.Salt = base64.StdEncoding.EncodeToString(iv)
			} else if name == "rclone-header-and-reopen-readers" {
				c, e := rclone.NewCipher(rclone.NameEncryptionOff, "synthetic-password", "synthetic-salt", false, nil)
				if e != nil {
					t.Fatal(e)
				}
				encrypted, e := c.EncryptData(bytes.NewReader(plain), nil)
				if e != nil {
					t.Fatal(e)
				}
				payload, e = io.ReadAll(encrypted)
				if e != nil {
					t.Fatal(e)
				}
				meta.Encryption = metapb.Encryption_RCLONE
				meta.Password = "synthetic-password"
				meta.Salt = "synthetic-salt"
			} else if name == "plain-blocked-read" || name == "read-close-race" || name == "admission-not-refunded-early" {
				payload = nil
			}
			if payload != nil {
				meta.SegmentData[0].SegmentSize = int64(len(payload))
				meta.SegmentData[0].EndOffset = int64(len(payload) - 1)
			}
			f := requestRealFixture(t, payload, name == "health-write-workers-joined")
			if name == "health-write-workers-joined" {
				meta.SourceNzbPath = "synthetic-source-provenance.nzb"
			}
			f.write(t, meta)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			args := utils.NewPathWithArgs("movie")
			if name == "rclone-header-and-reopen-readers" {
				args.SetRange("bytes=65536-65567")
			}
			file := f.open(t, ctx, args)
			if err := RequireSourceCleanup(file); err != nil {
				t.Fatal(err)
			}
			if err := RequireSourceCleanup(io.NopCloser(bytes.NewReader(nil))); !errors.Is(err, ErrSourceCleanupUnsupported) {
				t.Fatal("foreign cleanup imitation accepted")
			}
			switch name {
			case "close-before-first-read":
				requestJoin(t, file)
				if n, e := file.Read(make([]byte, 1)); n != 0 || !errors.Is(e, ErrRequestSourceRetired) {
					t.Fatal("retired Read accepted")
				}
				if f.bodyCalls.Load() != 0 {
					t.Fatal("close-only issued NNTP BODY")
				}
				if err := RequireSourceCleanup(file); !errors.Is(err, ErrSourceCleanupUnsupported) {
					t.Fatal("closed actor is ready")
				}
			case "directory-cancel":
				source, e := f.fs.NewRequestSource(ctx, RequestSourceBudgets{Entries: 3, Depth: 1})
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					f.releaseAll()
					if err := source.CloseAndJoin(); err != nil {
						t.Errorf("view cleanup: %v", err)
					}
				})
				for _, name := range []string{"nested/one", "nested/two"} {
					if e = f.service.WriteFileMetadata(name, meta); e != nil {
						t.Fatal(e)
					}
				}
				directory, e := source.Open("/")
				if e != nil {
					t.Fatal(e)
				}
				if entries, e := directory.Readdir(-1); e != nil || len(entries) != 2 {
					t.Fatalf("bounded root enumeration: %d,%v", len(entries), e)
				}
				nested, e := source.Open("nested")
				if e != nil {
					t.Fatal(e)
				}
				if _, e = nested.Readdir(-1); !errors.Is(e, ErrRequestSourceBudget) {
					t.Fatalf("aggregate entry budget ignored: %v", e)
				}
				if _, e = source.Open("one/two/movie"); !errors.Is(e, ErrRequestSourceBudget) {
					t.Fatal("depth ignored")
				}
				cancel()
				if _, e = directory.Readdir(-1); !errors.Is(e, ErrRequestSourceRetired) {
					t.Fatal("cancelled directory accepted")
				}
				if e = source.CloseAndJoin(); e != nil {
					t.Fatal(e)
				}
				requestJoin(t, file)

			case "late-created-reader-joined":
				f.manager.entered = make(chan struct{})
				f.manager.release = make(chan struct{})
				if e := file.lifetime.enter(); e != nil {
					t.Fatal(e)
				}
				callEnded := make(chan struct{})
				var lateErr error
				go func() {
					defer close(callEnded)
					defer file.lifetime.leave()
					_, lateErr = file.createUsenetReader(context.Background(), 0, 63)
				}()
				requestWait(t, f.manager.entered, "late actual pool acquisition")
				if e := SignalSourceClose(file); e != nil {
					t.Fatal(e)
				}
				joined := make(chan struct{})
				go func() { defer close(joined); _ = JoinSourceCleanup(file) }()
				requestPending(t, joined, "source join")
				close(f.manager.release)
				requestWait(t, callEnded, "late factory end")
				requestWait(t, joined, "source cleanup")
				requestJoin(t, file)
				if !errors.Is(lateErr, ErrRequestSourceRetired) {
					t.Fatalf("late child published: %v", lateErr)
				}
				file.lifetime.mu.Lock()
				children := len(file.lifetime.children)
				file.lifetime.mu.Unlock()
				if children != 0 {
					t.Fatal("late actual child retained")
				}
			case "admission-not-refunded-early":
				requestJoin(t, file)
				source, e := f.fs.NewRequestSource(ctx, RequestSourceBudgets{PendingReaders: 1})
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					f.releaseAll()
					if err := source.CloseAndJoin(); err != nil {
						t.Errorf("view cleanup: %v", err)
					}
				})
				owned, e := source.Open("movie")
				if e != nil {
					t.Fatal(e)
				}
				file = owned.(*requestFile).File.(*MetadataVirtualFile)
				other, e := source.Open("movie")
				if e != nil {
					t.Fatal(e)
				}
				otherFile := other.(*requestFile).File.(*MetadataVirtualFile)
				first, e := file.createUsenetReader(file.ctx, 0, 63)
				if e != nil {
					t.Fatal(e)
				}
				ack, e := usenet.ReaderCleanupDone(first)
				if e != nil {
					t.Fatal(e)
				}
				readEnded := make(chan struct{})
				go func() { defer close(readEnded); _, _ = first.Read(make([]byte, 1)) }()
				requestWait(t, f.accepted, "first actual BODY")
				_ = first.Close()
				requestWait(t, f.pool.returned, "first actual transport end")
				requestPending(t, ack, "inner physical cleanup")
				if _, e = otherFile.createUsenetReader(otherFile.ctx, 0, 63); !errors.Is(e, ErrRequestSourceBudget) {
					t.Fatalf("pending slot was refunded early: %v", e)
				}
				file.lifetime.mu.Lock()
				retained := len(file.lifetime.children)
				file.lifetime.mu.Unlock()
				if retained != 1 {
					t.Fatal("unjoined reader was pruned")
				}
				close(f.pool.release)
				requestWait(t, readEnded, "first Read")
				requestWait(t, ack, "first physical end")
				next, e := file.createUsenetReader(file.ctx, 0, 63)
				if e != nil {
					t.Fatal(e)
				}
				if next == first {
					t.Fatal("reader incarnation reused")
				}
				file.lifetime.mu.Lock()
				retained = len(file.lifetime.children)
				file.lifetime.mu.Unlock()
				if retained != 1 {
					t.Fatal("only physically completed actor may be pruned")
				}
				requestJoin(t, file)
				if e = source.CloseAndJoin(); e != nil {
					t.Fatal(e)
				}
			case "health-write-workers-joined":
				requestJoin(t, file)
				source, e := f.fs.NewRequestSource(ctx, RequestSourceBudgets{MetadataBytes: 256})
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					f.releaseAll()
					if err := source.CloseAndJoin(); err != nil {
						t.Errorf("health source view cleanup: %v", err)
					}
				})
				owned, e := source.Open("movie")
				if e != nil {
					t.Fatal(e)
				}
				file = owned.(*requestFile).File.(*MetadataVirtualFile)
				oversized, e := os.OpenFile(filepath.Join(f.service.RootPath(), "oversized.meta"), os.O_WRONLY|os.O_CREATE, 0600)
				if e != nil {
					t.Fatal(e)
				}
				if e = oversized.Truncate(257); e != nil {
					t.Fatal(e)
				}
				if e = oversized.Close(); e != nil {
					t.Fatal(e)
				}
				if e = source.updateFileStatus("oversized", metapb.FileStatus_FILE_STATUS_CORRUPTED); !errors.Is(e, ErrRequestSourceBudget) {
					t.Fatalf("request health read bypassed byte cap: %v", e)
				}
				if current, e := os.Stat(filepath.Join(f.service.RootPath(), "oversized.meta")); e != nil || current.Size() != 257 {
					t.Fatal("oversized health metadata was changed")
				}
				original, e := f.service.ReadRequestMetadata(ctx, "movie", 256)
				if e != nil {
					t.Fatal(e)
				}
				if e = source.updateFileStatus("movie", metapb.FileStatus_FILE_STATUS_PARTIAL); e != nil {
					t.Fatal(e)
				}
				current, e := f.service.ReadRequestMetadata(ctx, "movie", 256)
				if e != nil {
					t.Fatal(e)
				}
				expected := proto.Clone(original).(*metapb.FileMetadata)
				expected.Status = metapb.FileStatus_FILE_STATUS_PARTIAL
				expected.ModifiedAt = current.ModifiedAt
				if original.SourceNzbPath == "" || !proto.Equal(current, expected) {
					t.Fatal("bounded health update changed original source provenance or segment/crypto metadata")
				}

				close(f.pool.release)
				db, e := sql.Open("sqlite3", filepath.Join(t.TempDir(), "synthetic-health.db"))
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Errorf("health database cleanup: %v", err)
					}
				})
				if _, e = db.Exec(`CREATE TABLE file_health(file_path TEXT PRIMARY KEY,status TEXT,last_checked TEXT,last_error TEXT,source_nzb_path TEXT,error_details TEXT,retry_count INTEGER,repair_retry_count INTEGER,created_at TEXT,updated_at TEXT)`); e != nil {
					t.Fatal(e)
				}
				held := &requestHeldHealthDB{DB: db, entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
				file.healthRepository = database.NewHealthRepository(held)
				t.Cleanup(func() {
					select {
					case <-held.release:
					default:
						close(held.release)
					}
					f.releaseAll()
					requestJoin(t, file)
				})
				readEnded := make(chan struct{})
				go func() { defer close(readEnded); _, _ = file.Read(make([]byte, 1)) }()
				requestWait(t, held.entered, "actual health Exec entry")
				joined := make(chan struct{})
				go func() { defer close(joined); _ = JoinSourceCleanup(file) }()
				requestPending(t, joined, "health-owner join")
				close(held.release)
				requestWait(t, held.done, "actual SQLite health write")
				requestWait(t, readEnded, "missing article Read")
				requestWait(t, joined, "health owner cleanup")
				requestJoin(t, file)
				var status string
				if e = db.QueryRow("SELECT status FROM file_health WHERE file_path='movie'").Scan(&status); e != nil || status != string(database.HealthStatusCorrupted) {
					t.Fatalf("actual health row: %s,%v", status, e)
				}
			case "aes-reopened-inner-readers", "rclone-header-and-reopen-readers":
				out := make([]byte, 32)
				if _, e := io.ReadFull(file, out); e != nil {
					t.Fatal(e)
				}
				offset := 0
				if name == "rclone-header-and-reopen-readers" {
					offset = 65536
				}
				if !bytes.Equal(out, plain[offset:offset+32]) {
					t.Fatal("actual encrypted source did not decode")
				}
				if name == "aes-reopened-inner-readers" {
					file.mu.Lock()
					seeker, ok := file.reader.(io.Seeker)
					if !ok {
						file.mu.Unlock()
						t.Fatal("actual AES reader has no seek")
					}
					_, e := seeker.Seek(2048, io.SeekStart)
					file.mu.Unlock()
					if e != nil {
						t.Fatal(e)
					}
					if _, e = io.ReadFull(file, out); e != nil {
						t.Fatal(e)
					}
					if !bytes.Equal(out, plain[2048:2080]) {
						t.Fatal("reopened AES source mismatch")
					}
				}
				if f.bodyCalls.Load() < 2 {
					t.Fatal("genuine encrypted header/reopen path did not issue multiple BODY requests")
				}
				joined := make(chan struct{})
				go func() { defer close(joined); _ = JoinSourceCleanup(file) }()
				requestPending(t, joined, "all encrypted inner worker join")
				close(f.pool.release)
				requestWait(t, joined, "all encrypted cleanup")
				requestJoin(t, file)
			default:
				readEnded := make(chan struct{})
				go func() { defer close(readEnded); _, _ = file.Read(make([]byte, 64)) }()
				requestWait(t, f.accepted, "actual blocked BODY")
				signalReturned := make(chan struct{})
				go func() { defer close(signalReturned); _ = SignalSourceClose(file) }()
				requestWait(t, signalReturned, "signal before Read mutex")
				joined := make(chan struct{})
				go func() { defer close(joined); _ = JoinSourceCleanup(file) }()
				requestWait(t, f.pool.returned, "actual transport end")
				requestPending(t, joined, "actual source join")
				close(f.pool.release)
				requestWait(t, readEnded, "original file Read")
				requestWait(t, joined, "physical source cleanup")
				requestJoin(t, file)
				if _, e := file.Seek(0, io.SeekStart); !errors.Is(e, ErrRequestSourceRetired) {
					t.Fatal("retired seek accepted")
				}
			}
		})
	}
}
