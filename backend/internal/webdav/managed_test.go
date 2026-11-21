//go:build linux

package webdav

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	cryptocipher "crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/javi11/nntppool/v2"
	"github.com/spf13/afero"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/encryption/rclone"
	"novastream/internal/integration"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/settingsbootstrap"
	"novastream/internal/settingscommit"
	"novastream/services/streaming"
	"novastream/utils"
)

func managedTestAwait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("actual " + label + " did not end")
	}
}
func managedTestPending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("actual source ended before physical worker")
	case <-time.After(30 * time.Millisecond):
	}
}

type managedTestPool struct {
	nntppool.UsenetConnectionPool
	gate, returned chan struct{}
	once           sync.Once
	calls          atomic.Int64
}

func (p *managedTestPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	n, e := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.calls.Add(1)
	p.once.Do(func() { close(p.returned) })
	<-p.gate
	return n, e
}

type managedTestManager struct{ pool nntppool.UsenetConnectionPool }

func (m *managedTestManager) GetPool() (nntppool.UsenetConnectionPool, error) { return m.pool, nil }
func (*managedTestManager) SetProviders([]nntppool.UsenetProviderConfig) error {
	return errors.New("synthetic mutation unsupported")
}
func (*managedTestManager) ClearPool() error          { return errors.New("synthetic mutation unsupported") }
func (*managedTestManager) HasPool() bool             { return true }
func (*managedTestManager) AvailableConnections() int { return 4 }
func managedTestYenc(payload []byte) string {
	var b strings.Builder
	column := 0
	for _, v := range payload {
		x := v + 42
		escaped := x == 0 || x == 9 || x == 10 || x == 13 || x == 32 || x == 46 || x == 61
		width := 1
		if escaped {
			width = 2
		}
		if column+width > 128 {
			b.WriteString("\r\n")
			column = 0
		}
		if escaped {
			b.WriteByte('=')
			x += 64
		}
		b.WriteByte(x)
		column += width
	}
	return fmt.Sprintf("=ybegin line=128 size=%d name=synthetic.bin\r\n%s\r\n=yend size=%d crc32=%08x\r\n.\r\n", len(payload), b.String(), len(payload), crc32.ChecksumIEEE(payload))
}
func managedTestNNTP(t *testing.T, payload []byte, hold bool) *managedTestPool {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var mu sync.Mutex
	conns := map[net.Conn]bool{}
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			conns[c] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(conns, c); mu.Unlock() }()
				_, _ = io.WriteString(c, "200 synthetic ready\r\n")
				scan := bufio.NewScanner(c)
				for scan.Scan() {
					line := scan.Text()
					switch {
					case strings.HasPrefix(line, "BODY "):
						_, _ = io.WriteString(c, "222 synthetic body\r\n")
						_, _ = io.WriteString(c, managedTestYenc(payload))
					case line == "CAPABILITIES":
						_, _ = io.WriteString(c, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(c, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(c, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(c, "500 unsupported\r\n")
					}
				}
			}()
		}
	}()
	var networkOnce sync.Once
	closeNetwork := func() {
		networkOnce.Do(func() {
			_ = listener.Close()
			<-accepted
			mu.Lock()
			for c := range conns {
				_ = c.Close()
			}
			mu.Unlock()
			workers.Wait()
		})
	}
	t.Cleanup(closeNetwork)
	actual, e := nntppool.NewConnectionPool(nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 4, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 200 * time.Millisecond, HealthCheckInterval: time.Hour})
	if e != nil {
		closeNetwork()
		t.Fatal(e)
	}
	p := &managedTestPool{UsenetConnectionPool: actual, gate: make(chan struct{}), returned: make(chan struct{})}
	if !hold {
		close(p.gate)
	}
	t.Cleanup(func() {
		select {
		case <-p.gate:
		default:
			close(p.gate)
		}
		actual.Quit()
		closeNetwork()
	})
	return p
}

type managedTestFixture struct {
	startup    *settingsbootstrap.Startup
	store      *datastore.DataStore
	manager    *config.Manager
	settings   config.Settings
	handler    *ManagedHandler
	service    *metadata.MetadataService
	system     *integration.NzbSystem
	pool       *managedTestPool
	plain      []byte
	stopSystem func()
}

func managedTestPG(t *testing.T, hold bool, encryption string) *managedTestFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket PostgreSQL required")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal(e)
	}
	name := fmt.Sprintf("synthetic_managed_webdav_%d", time.Now().UnixNano())
	if _, e = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	u.Path = "/" + name
	f := &managedTestFixture{manager: config.NewManager(filepath.Join(t.TempDir(), "settings.json")), settings: config.DefaultSettings(), plain: bytes.Repeat([]byte("synthetic-media-"), 8192)}
	f.settings.Database.URL = u.String()
	f.settings.Database.MaxOpenConns = 1
	f.settings.Security.EnforceProfilePins = false
	f.settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if e = f.manager.Save(f.settings); e != nil {
		t.Fatal(e)
	}
	f.startup, e = settingsbootstrap.Begin(t.Context(), f.manager, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := f.startup.Close(); e != nil {
			t.Error(e)
		}
	})
	f.store, e = f.startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.startup.Finish(); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('original','Synthetic original','synthetic-unused');INSERT INTO users(id,account_id,name)VALUES('profile','original','Synthetic profile')`); e != nil {
		t.Fatal(e)
	}
	payload := f.plain
	meta := &metapb.FileMetadata{FileSize: int64(len(payload)), ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "synthetic-source", SegmentSize: int64(len(payload)), EndOffset: int64(len(payload) - 1)}}}
	if encryption == "aes" {
		key := []byte("0123456789abcdef0123456789abcdef")
		iv := []byte("0123456789abcdef")
		payload = append([]byte(nil), f.plain...)
		block, e := aes.NewCipher(key)
		if e != nil {
			t.Fatal(e)
		}
		cryptocipher.NewCBCEncrypter(block, iv).CryptBlocks(payload, payload)
		meta.Encryption = metapb.Encryption_HEADERS
		meta.Password = base64.StdEncoding.EncodeToString(key)
		meta.Salt = base64.StdEncoding.EncodeToString(iv)
	} else if encryption == "rclone" {
		cipher, e := rclone.NewCipher(rclone.NameEncryptionOff, "synthetic-password", "synthetic-salt", false, nil)
		if e != nil {
			t.Fatal(e)
		}
		reader, e := cipher.EncryptData(bytes.NewReader(f.plain), nil)
		if e != nil {
			t.Fatal(e)
		}
		payload, e = io.ReadAll(reader)
		if e != nil {
			t.Fatal(e)
		}
		meta.Encryption = metapb.Encryption_RCLONE
		meta.Password = "synthetic-password"
		meta.Salt = "synthetic-salt"
		meta.SegmentData[0].SegmentSize = int64(len(payload))
		meta.SegmentData[0].EndOffset = int64(len(payload) - 1)
	}
	f.pool = managedTestNNTP(t, payload, hold)
	root := t.TempDir()
	f.service = metadata.NewMetadataService(root)
	if e = f.service.WriteFileMetadata("movie", meta); e != nil {
		t.Fatal(e)
	}
	f.system, e = integration.NewNzbSystem(integration.NzbConfig{QueueDatabasePath: filepath.Join(t.TempDir(), "queue.db"), MetadataRootPath: root, MaxProcessorWorkers: 1, MaxDownloadWorkers: 1}, &managedTestManager{pool: f.pool}, func() *config.AltMountConfig {
		return &config.AltMountConfig{Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}, Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}}
	})
	if e != nil {
		t.Fatal(e)
	}
	var stopOnce sync.Once
	f.stopSystem = func() {
		stopOnce.Do(func() {
			select {
			case <-f.pool.gate:
			default:
				close(f.pool.gate)
			}
			if e := f.system.Close(); e != nil {
				t.Error(e)
			}
		})
	}
	t.Cleanup(f.stopSystem)
	issuer, e := f.startup.NewDeploymentWebDAVIssuer(f.store)
	if e != nil {
		t.Fatal(e)
	}
	f.handler, e = NewManagedHandler("/webdav", f.system.FileSystem().(*nzbfilesystem.NzbFilesystem), issuer)
	if e != nil {
		issuer.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		select {
		case <-f.pool.gate:
		default:
			close(f.pool.gate)
		}
		f.handler.Close()
	})
	return f
}
func managedTestRequest(method, target string) *http.Request {
	r := httptest.NewRequest(method, "http://synthetic.test"+target, nil)
	r.RequestURI = r.URL.RequestURI()
	r.RemoteAddr = "127.0.0.1:23100"
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	return r
}
func TestManagedWebDAVRegisteredBasic(t *testing.T) {
	for _, name := range []string{"actual-valid-basic-inactive", "wrong-basic-no-pg-or-source", "missing-managed-dependencies", "file-legacy-unchanged", "basepath-and-range"} {
		t.Run(name, func(t *testing.T) {
			f := managedTestPG(t, false, "")
			switch name {
			case "actual-valid-basic-inactive":
				r := managedTestRequest("PROPFIND", "/webdav/")
				r.Header.Set("Depth", "1")
				w := httptest.NewRecorder()
				f.handler.ServeHTTP(w, r)
				if w.Code != 207 || !strings.Contains(w.Body.String(), "movie") {
					t.Fatalf("actual original metadata: %d", w.Code)
				}
				server := httptest.NewServer(f.handler)
				defer server.Close()
				req, _ := http.NewRequest("GET", server.URL+"/webdav/movie", nil)
				req.SetBasicAuth("synthetic-basic", "synthetic-password")
				req.Header.Set("Range", "bytes=0-31")
				response, e := server.Client().Do(req)
				if e != nil {
					t.Fatal(e)
				}
				data, e := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if e != nil || closeErr != nil || response.StatusCode != 206 || !bytes.Equal(data, f.plain[:32]) {
					t.Fatal("healthy actual TCP final body/chunk mismatch", e, closeErr, response.StatusCode)
				}
				managedTestEarlyAdmission(t, f)
			case "wrong-basic-no-pg-or-source":
				tx, e := f.store.Pool().Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Rollback(context.Background())
				if _, e = tx.Exec(t.Context(), "LOCK TABLE users IN ACCESS EXCLUSIVE MODE"); e != nil {
					t.Fatal(e)
				}
				r := managedTestRequest("GET", "/webdav/movie")
				r.SetBasicAuth("synthetic-basic", "wrong")
				w := httptest.NewRecorder()
				f.handler.ServeHTTP(w, r)
				if w.Code != 401 || f.pool.calls.Load() != 0 {
					t.Fatal("wrong actual Basic reached blocked PG/source", w.Code)
				}
			case "missing-managed-dependencies":
				if h, e := NewManagedHandler("/webdav", nil, nil); h != nil || e == nil {
					t.Fatal("missing managed deps accepted")
				}
				var h ManagedHandler
				copied := new(ManagedHandler)
				reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(f.handler).Elem())
				if !h.Closed() || !(*ManagedHandler)(nil).Closed() || !copied.Closed() || f.handler.Closed() {
					t.Fatal("local closed-state DATA forged runtime liveness")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
				if w.Code != 503 {
					t.Fatal("zero runtime did not fail closed")
				}
				f.handler.Close()
				w = httptest.NewRecorder()
				f.handler.ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
				if w.Code != 503 {
					t.Fatal("closed runtime revived")
				}
			case "file-legacy-unchanged":
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='profile'`); e != nil {
					t.Fatal(e)
				}
				fs := afero.NewMemMapFs()
				if e := afero.WriteFile(fs, "/movie", []byte("synthetic-legacy"), 0600); e != nil {
					t.Fatal(e)
				}
				legacy, e := NewHandler(&Config{Prefix: "/webdav", User: "synthetic-basic", Pass: "synthetic-password"}, fs, nil, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
				w := httptest.NewRecorder()
				legacy.GetHTTPHandler().ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
				if w.Code != 200 || w.Body.String() != "synthetic-legacy" {
					t.Fatal("explicit legacy mode changed", w.Code)
				}
				w = httptest.NewRecorder()
				f.handler.ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
				if w.Code != 403 {
					t.Fatal("shared managed gate ignored current child", w.Code)
				}
			case "basepath-and-range":
				w := httptest.NewRecorder()
				f.handler.ServeHTTP(w, managedTestRequest("GET", "/webdav"))
				if w.Code != 301 {
					t.Fatal("authenticated base redirect missing", w.Code)
				}
				server := httptest.NewServer(utils.BasePathHandler("/app", f.handler))
				defer server.Close()
				client := server.Client()
				client.Timeout = 8 * time.Second
				redirectClient := *client
				redirectClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
				red, e := http.NewRequest("GET", server.URL+"/app/webdav?query=synthetic", nil)
				if e != nil {
					t.Fatal(e)
				}
				red.SetBasicAuth("synthetic-basic", "synthetic-password")
				redResponse, e := redirectClient.Do(red)
				if e != nil {
					t.Fatal(e)
				}
				_, readErr := io.ReadAll(redResponse.Body)
				closeErr := redResponse.Body.Close()
				if readErr != nil || closeErr != nil || redResponse.StatusCode != 301 || redResponse.Header.Get("Location") != "/app/webdav/?query=synthetic" {
					t.Fatal("actual BasePath local redirect lost presentation prefix", readErr, closeErr, redResponse.Header.Get("Location"))
				}
				request, e := http.NewRequest("GET", server.URL+"/app/webdav/movie", nil)
				if e != nil {
					t.Fatal(e)
				}
				request.SetBasicAuth("synthetic-basic", "synthetic-password")
				request.Header.Set("Range", "bytes=32-63")
				response, e := client.Do(request)
				if e != nil {
					t.Fatal(e)
				}
				data, e := io.ReadAll(response.Body)
				closeErr = response.Body.Close()
				if e != nil || closeErr != nil || response.StatusCode != 206 || !bytes.Equal(data, f.plain[32:64]) {
					t.Fatal("actual registered BasePath/range flow lost", e, closeErr, response.StatusCode)
				}

				if e := f.service.WriteFileMetadata("movie & ü", &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
					t.Fatal(e)
				}
				for _, route := range []string{"/app/webdav/", "/webdav/"} {
					request, e := http.NewRequest("PROPFIND", server.URL+route, nil)
					if e != nil {
						t.Fatal(e)
					}
					request.SetBasicAuth("synthetic-basic", "synthetic-password")
					request.Header.Set("Depth", "1")
					response, e := client.Do(request)
					if e != nil {
						t.Fatal(e)
					}
					data, e := io.ReadAll(response.Body)
					closeErr := response.Body.Close()
					expected := strings.TrimSuffix(route, "/") + "/movie%20&amp;%20%C3%BC"
					if e != nil || closeErr != nil || response.StatusCode != 207 || !strings.Contains(string(data), expected) {
						t.Fatal("actual prefixed escaped href/direct form mismatch", route, e, closeErr, string(data))
					}
				}
				f.settings.WebDAV.Prefix = "/"
				if e := f.manager.Save(f.settings); e != nil {
					t.Fatal(e)
				}
				before := f.pool.calls.Load()
				w = httptest.NewRecorder()
				f.handler.ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
				if w.Code != 403 || f.pool.calls.Load() != before {
					t.Fatal("native prefix mismatch opened substituted source", w.Code)
				}
			}
		})
	}
}
func TestManagedWebDAVCurrentSource(t *testing.T) {
	for _, name := range []string{"late-family-change-stops-body", "late-basic-config-change-stops-body", "plain-source-workers-joined", "encrypted-inner-workers-joined", "known-owner-inactive-provider-unchanged"} {
		t.Run(name, func(t *testing.T) {
			if name == "encrypted-inner-workers-joined" {
				for _, flavor := range []string{"rclone", "aes"} {
					managedTestHeldSource(t, name, flavor)
				}
				return
			}
			managedTestHeldSource(t, name, "")
		})
	}
}
func managedTestHeldSource(t *testing.T, name, flavor string) {
	f := managedTestPG(t, true, flavor)
	if name == "known-owner-inactive-provider-unchanged" {
		close(f.pool.gate)
		if _, e := f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('foreign','Synthetic foreign','synthetic-unused');INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('foreign-child','foreign','Synthetic child',true)`); e != nil {
			t.Fatal(e)
		}
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, managedTestRequest("GET", "/webdav/movie"))
		if w.Code != 403 {
			t.Fatal("shared Basic ignored unrelated child", w.Code)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		response, e := f.system.Stream(ctx, streaming.Request{Path: "movie", RangeHeader: "bytes=0-31", Method: http.MethodGet})
		if e != nil {
			t.Fatal(e)
		}
		if response == nil || response.Body == nil {
			t.Fatal("actual ordinary provider returned no source")
		}
		defer response.Close()
		if response.Status != http.StatusPartialContent || response.ContentLength != 32 || response.Headers.Get("Content-Range") != fmt.Sprintf("bytes 0-31/%d", len(f.plain)) {
			t.Fatal("ordinary declared range contract changed", response.Status, response.ContentLength, response.Headers.Get("Content-Range"))
		}
		data := make([]byte, response.ContentLength)
		n, e := io.ReadFull(response.Body, data)
		closeErr := response.Close()
		if e != nil || n != 32 || closeErr != nil || !bytes.Equal(data, f.plain[:32]) || f.pool.calls.Load() == 0 {
			t.Fatal("actual per-owner provider path gained global shared gate", e, closeErr, n)
		}
		return
	}
	r := managedTestRequest("GET", "/webdav/movie")
	offset := 0
	if name == "encrypted-inner-workers-joined" {
		offset = 65536
	}
	r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+31))
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	done, closed := make(chan struct{}), make(chan struct{})
	started, closeStarted := false, false
	recorder := httptest.NewRecorder()
	w := &managedTestOutput{ResponseWriter: recorder, wrote: make(chan struct{})}
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.pool.gate:
		default:
			close(f.pool.gate)
		}
		if started {
			managedTestAwait(t, done, "source request")
		}
		if closeStarted {
			managedTestAwait(t, closed, "runtime close")
		}
	})
	started = true
	go func() { defer close(done); f.handler.ServeHTTP(w, r) }()
	managedTestAwait(t, f.pool.returned, "actual NNTP delegate return")
	if flavor != "" {
		managedTestAwait(t, w.wrote, "actual decoded encrypted output")
	}

	f.handler.mu.Lock()
	var original *managedWebDAVRequest
	for op := range f.handler.active {
		original = op
	}
	f.handler.mu.Unlock()
	if original == nil {
		t.Fatal("original active source missing")
	}
	if time.Until(original.expiry) < 23*time.Hour {
		t.Fatal("body ingress budget shortened original playback lifetime")
	}

	switch name {
	case "late-family-change-stops-body":
		if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='profile'`); e != nil {
			t.Fatal(e)
		}
	case "late-basic-config-change-stops-body":
		f.settings.WebDAV.Password = "changed-synthetic-password"
		managedTestSave(t, f.manager, f.settings)
	default:
		cancel()
	}
	if name == "late-family-change-stops-body" || name == "late-basic-config-change-stops-body" {
		managedTestAwait(t, original.ctx.Done(), "genuine current policy retirement")
	}
	closeStarted = true
	go func() { defer close(closed); f.handler.Close() }()
	managedTestPending(t, closed)
	f.handler.mu.Lock()
	retained := len(f.handler.active)
	f.handler.mu.Unlock()
	if retained != 1 {
		t.Fatal("original admission refunded before actual NNTP/crypto join")
	}
	close(f.pool.gate)
	managedTestAwait(t, done, "actual HTTP source end")
	managedTestAwait(t, closed, "actual runtime source cleanup")
	if flavor == "rclone" && f.pool.calls.Load() < 2 {
		t.Fatal("actual rclone header/reopen workers not exercised")
	}
	if flavor != "" && !bytes.Equal(recorder.Body.Bytes(), f.plain[offset:offset+32]) {
		t.Fatal("actual encrypted range plaintext mismatch")
	}
	f.stopSystem()
	if e := f.startup.Close(); e != nil {
		t.Fatal(e)
	}

}

type managedTestOutput struct {
	http.ResponseWriter
	once  sync.Once
	wrote chan struct{}
}

func (w *managedTestOutput) Write(p []byte) (int, error) {
	n, e := w.ResponseWriter.Write(p)
	if n > 0 {
		w.once.Do(func() { close(w.wrote) })
	}
	return n, e
}

// This wrapper holds the return of a delegated REAL TCP deadline setter. It
// cannot supply auth or source bytes. The outer owner has admitted the request,
// while native policy/PG Capture has not started yet.
type managedTestPreCaptureWriter struct {
	*httptest.ResponseRecorder
	connection net.Conn
	entered    chan struct{}
	released   <-chan struct{}
	delegated  chan error
	once       sync.Once
}

func (w *managedTestPreCaptureWriter) SetReadDeadline(at time.Time) error {
	e := w.connection.SetReadDeadline(at)
	w.once.Do(func() { w.delegated <- e; close(w.entered); <-w.released })
	return e
}
func (w *managedTestPreCaptureWriter) SetWriteDeadline(at time.Time) error {
	return w.connection.SetWriteDeadline(at)
}
func managedTestEarlyAdmission(t *testing.T, f *managedTestFixture) {
	t.Helper()
	// The concrete coordinator has a distinct native-hold capacity8. Exercise
	// those genuine original PG waits/cancellations separately from outer64.
	held, e := f.store.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	var releasePGOnce sync.Once
	releasePG := func() { releasePGOnce.Do(held.Release) }
	var cancels []context.CancelFunc
	var ended []chan struct{}
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		releasePG()
		t.Fatal(e)
	}
	var sockets []net.Conn
	released := make(chan struct{})
	var releaseOnce sync.Once
	releaseTransport := func() { releaseOnce.Do(func() { close(released) }) }
	closeDone := make(chan struct{})
	closeStarted, closeJoined := false, false
	// Register before launching either actual PG waits or transport producers.
	t.Cleanup(func() {
		for _, cancel := range cancels {
			cancel()
		}
		releasePG()
		releaseTransport()
		for _, c := range sockets {
			_ = c.Close()
		}
		_ = listener.Close()
		for _, done := range ended {
			managedTestAwait(t, done, "original admission producer")
		}
		f.handler.Close()
		if closeStarted && !closeJoined {
			managedTestAwait(t, closeDone, "early runtime Close")
		}
	})
	beforeWait, beforeCancelled := f.store.Pool().Stat().WaitCount(), f.store.Pool().Stat().CanceledAcquireCount()
	for n := 0; n < 8; n++ {
		ctx, cancel := context.WithCancel(t.Context())
		cancels = append(cancels, cancel)
		r := managedTestRequest("PROPFIND", "/webdav/").WithContext(ctx)
		r.RemoteAddr = fmt.Sprintf("127.0.0.1:%d", 25000+n)
		done := make(chan struct{})
		ended = append(ended, done)
		go func() { defer close(done); f.handler.ServeHTTP(httptest.NewRecorder(), r) }()
	}
	until := time.Now().Add(4 * time.Second)
	for f.store.Pool().Stat().WaitCount() < beforeWait+8 {
		if time.Now().After(until) {
			t.Fatal("eight actual native/PG admission waits missing")
		}
		time.Sleep(time.Millisecond)
	}
	mapped := managedTestRequest("PROPFIND", "/webdav/")
	mapped.RemoteAddr = "[::ffff:127.0.0.1]:26000"
	mapped.Header.Set("X-Forwarded-For", "203.0.113.99")
	denied := httptest.NewRecorder()
	f.handler.ServeHTTP(denied, mapped)
	if denied.Code != 503 {
		t.Fatal("canonical actual-peer cap bypassed", denied.Code)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for _, done := range ended {
		managedTestAwait(t, done, "actual PG acquire cancellation")
	}
	if f.store.Pool().Stat().CanceledAcquireCount() < beforeCancelled+8 {
		t.Fatal("actual original PG wait cancellation was not joined")
	}
	releasePG()
	cancels = nil
	ended = nil
	waitCount := func(want int) {
		until := time.Now().Add(4 * time.Second)
		for {
			f.handler.mu.Lock()
			n := len(f.handler.active)
			f.handler.mu.Unlock()
			if n == want {
				return
			}
			if time.Now().After(until) {
				t.Fatal("early original admission missing", want, n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitCount(0)
	pair := func(host string) (net.Conn, net.Conn) {
		t.Helper()
		dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(host)}}
		client, e := dialer.DialContext(t.Context(), "tcp4", listener.Addr().String())
		if e != nil {
			t.Fatal(e)
		}
		sockets = append(sockets, client)
		accepted, e := listener.Accept()
		if e != nil {
			t.Fatal(e)
		}
		sockets = append(sockets, accepted)
		actual, _, e := net.SplitHostPort(accepted.RemoteAddr().String())
		if e != nil || actual != host {
			t.Fatal("actual TCP peer differs", actual, e)
		}
		return client, accepted
	}
	noNewPG := f.store.Pool().Stat().AcquireCount()
	noNewSource := f.pool.calls.Load()
	for host := 1; host <= 8; host++ {
		for n := 0; n < 8; n++ {
			_, accepted := pair(fmt.Sprintf("127.0.0.%d", host))
			ctx, cancel := context.WithCancel(t.Context())
			cancels = append(cancels, cancel)
			r := managedTestRequest("PROPFIND", "/webdav/").WithContext(ctx)
			r.RemoteAddr = accepted.RemoteAddr().String()
			writer := &managedTestPreCaptureWriter{ResponseRecorder: httptest.NewRecorder(), connection: accepted, entered: make(chan struct{}), released: released, delegated: make(chan error, 1)}
			done := make(chan struct{})
			ended = append(ended, done)
			go func() { defer close(done); f.handler.ServeHTTP(writer, r) }()
			managedTestAwait(t, writer.entered, "actual pre-Capture delegated deadline")
			if e := <-writer.delegated; e != nil {
				t.Fatal("actual TCP deadline failed", e)
			}
		}
		waitCount(host * 8)
		if host == 1 {
			_, accepted := pair("127.0.0.1")
			r := managedTestRequest("PROPFIND", "/webdav/")
			r.RemoteAddr = accepted.RemoteAddr().String()
			out := httptest.NewRecorder()
			f.handler.ServeHTTP(out, r)
			if out.Code != 503 {
				t.Fatal("actual ninth peer connection bypassed early8", out.Code)
			}
		}
	}
	_, accepted := pair("127.0.0.99")
	r := managedTestRequest("PROPFIND", "/webdav/")
	r.RemoteAddr = accepted.RemoteAddr().String()
	out := httptest.NewRecorder()
	f.handler.ServeHTTP(out, r)
	if out.Code != 503 {
		t.Fatal("actual65th connection bypassed early64", out.Code)
	}
	if f.store.Pool().Stat().AcquireCount() != noNewPG || f.pool.calls.Load() != noNewSource {
		t.Fatal("held pre-Capture transport unexpectedly reached PG/source")
	}
	closeStarted = true
	go func() { f.handler.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		closeJoined = true
		t.Fatal("early ownership refunded before actual deadline wrappers returned")
	case <-time.After(40 * time.Millisecond):
	}
	for _, cancel := range cancels {
		cancel()
	}
	releaseTransport()
	for _, done := range ended {
		managedTestAwait(t, done, "actual early transport return")
	}
	managedTestAwait(t, closeDone, "actual64 early ownership shutdown")
	closeJoined = true
	if f.store.Pool().Stat().AcquireCount() != noNewPG || f.pool.calls.Load() != noNewSource {
		t.Fatal("cancelled pre-Capture ingress reached PG/source")
	}
	waitCount(0)
}

func managedTestSave(t *testing.T, manager *config.Manager, settings config.Settings) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e := manager.Save(settings)
		if e == nil {
			return
		}
		if !errors.Is(e, settingscommit.ErrBusy) || time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(time.Millisecond)
	}
}
func (w *managedTestOutput) Unwrap() http.ResponseWriter { return w.ResponseWriter }
