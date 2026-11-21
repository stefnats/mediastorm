//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/settingsbootstrap"
	"novastream/internal/settingscommit"
	"novastream/internal/webdav"
	"novastream/models"
	"novastream/utils"
)

type mainWebDAVFixture struct {
	startup  *settingsbootstrap.Startup
	store    *datastore.DataStore
	manager  *config.Manager
	settings config.Settings
	runtime  *webdav.ManagedHandler
	source   *metadata.MetadataService
	router   *mux.Router
	entry    http.Handler
}

func mainWebDAVPG(t *testing.T) *mainWebDAVFixture {
	t.Helper()
	old := newPublicPinWebFixture(t)
	// The reused profile fixture bootstraps a Primary Profile owned by master.
	// Supply its real account before asking the complete managed deployment to
	// attest an inactive household; an orphan profile must remain unavailable.
	now := time.Now().UTC()
	bootstrapOwner := &models.Account{ID: models.MasterAccountID, Username: "Synthetic DAV bootstrap owner", PasswordHash: "synthetic-unused-hash", IsMaster: true, CreatedAt: now, UpdatedAt: now}
	if e := old.store.Accounts().Create(t.Context(), bootstrapOwner); e != nil {
		t.Fatal("actual bootstrap account", e)
	}
	var profileCount, orphanCount int
	if e := old.store.Pool().QueryRow(t.Context(), `SELECT count(*),count(*) FILTER(WHERE a.id IS NULL) FROM users u LEFT JOIN accounts a ON a.id=u.account_id`).Scan(&profileCount, &orphanCount); e != nil || profileCount != 2 || orphanCount != 0 {
		t.Fatal("synthetic deployment must retain both profiles with real current account owners", e, profileCount, orphanCount)
	}
	f := &mainWebDAVFixture{manager: config.NewManager(filepath.Join(t.TempDir(), "settings.json")), settings: config.DefaultSettings()}
	f.settings.Database.URL = old.store.Pool().Config().ConnConfig.ConnString()
	f.settings.Database.MaxOpenConns = 1
	f.settings.Security.EnforceProfilePins = false
	f.settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if e := f.manager.Save(f.settings); e != nil {
		t.Fatal(e)
	}
	var e error
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
	f.source = metadata.NewMetadataService(t.TempDir())
	if e = f.source.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
		t.Fatal(e)
	}
	fs := nzbfilesystem.NewNzbFilesystem(nzbfilesystem.NewMetadataRemoteFile(f.source, nil, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} }))
	f.runtime, e = newManagedWebDAVRuntime(f.startup, f.store, fs, "/webdav")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.runtime.Close)
	f.router = utils.NewRouter()
	f.router.HandleFunc("/unrelated-cors", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }).Methods(http.MethodOptions, http.MethodPost)
	mountManagedWebDAV(f.router, "/webdav", f.runtime)
	f.entry = utils.BasePathHandler("/app", managedWebDAVOptions("/webdav", f.runtime, f.router))
	return f
}
func mainWebDAVWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("actual " + label + " not joined")
	}
}

type mainWebDAVBody struct {
	io.ReadCloser
	entered, ended, closed   chan struct{}
	once, endOnce, closeOnce sync.Once
}

func (b *mainWebDAVBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	n, e := b.ReadCloser.Read(p)
	if e != nil {
		b.endOnce.Do(func() { close(b.ended) })
	}
	return n, e
}
func (b *mainWebDAVBody) Close() error {
	e := b.ReadCloser.Close()
	b.closeOnce.Do(func() { close(b.closed) })
	return e
}

type mainWebDAVConn struct {
	net.Conn
	inWrite                atomic.Bool
	writeFailed, deadline  chan struct{}
	failOnce, deadlineOnce sync.Once
}

func (c *mainWebDAVConn) Write(p []byte) (int, error) {
	c.inWrite.Store(true)
	defer c.inWrite.Store(false)
	n, e := c.Conn.Write(p)
	if e != nil {
		c.failOnce.Do(func() { close(c.writeFailed) })
	}
	return n, e
}
func (c *mainWebDAVConn) SetWriteDeadline(at time.Time) error {
	if !at.IsZero() && at.Before(time.Now().Add(time.Second)) {
		c.deadlineOnce.Do(func() { close(c.deadline) })
	}
	return c.Conn.SetWriteDeadline(at)
}

type mainWebDAVListener struct {
	net.Listener
	connections chan *mainWebDAVConn
}

func (l *mainWebDAVListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(1024)
	}
	owned := &mainWebDAVConn{Conn: c, writeFailed: make(chan struct{}), deadline: make(chan struct{})}
	l.connections <- owned
	return owned, nil
}

func TestManagedWebDAVRealTransport(t *testing.T) {
	for _, name := range []string{"held-propfind-body-cancel", "held-output-write-cancel", "earliest-fixed-expiry", "startup-stop-joins-requests", "no-second-response-after-write"} {
		t.Run(name, func(t *testing.T) {
			f := mainWebDAVPG(t)
			mainWebDAVActualOptionsAndHealthy(t, f)
			// This is the exact Main mount fallback, not a copied handler approximation.
			fallback := mux.NewRouter()
			mountManagedWebDAV(fallback, "/webdav", nil)
			for _, target := range []string{"/webdav", "/webdav/movie"} {
				w := httptest.NewRecorder()
				fallback.ServeHTTP(w, httptest.NewRequest("GET", target, nil))
				if w.Code != 503 {
					t.Fatal("Main missing-dependency route did not fail closed", w.Code)
				}
			}
			entered, ended, bodyClosed, handlerDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var actualRequest atomic.Pointer[http.Request]
			var lifetime time.Time
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				if name == "earliest-fixed-expiry" {
					ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
					defer cancel()
					r = r.WithContext(ctx)
					lifetime, _ = ctx.Deadline()
				}
				r.Body = &mainWebDAVBody{ReadCloser: r.Body, entered: entered, ended: ended, closed: bodyClosed}
				actualRequest.Store(r)
				f.entry.ServeHTTP(w, r)
			})
			output := name == "held-output-write-cancel" || name == "no-second-response-after-write"
			if output {
				for i := 0; i < 600; i++ {
					n := fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 180))
					if e := f.source.WriteFileMetadata(n, &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
						t.Fatal(e)
					}
				}
			}
			server := httptest.NewUnstartedServer(handler)
			listener := &mainWebDAVListener{Listener: server.Listener, connections: make(chan *mainWebDAVConn, 1)}
			server.Listener = listener
			server.Start()
			// Explicit cleanup is registered before any request producer or held acquire.
			var connection net.Conn
			var wireResponse *http.Response
			var wireReader *bufio.Reader
			var wirePrefix []byte
			var heldRelease func()
			var requestStarted bool
			t.Cleanup(func() {
				if connection != nil {
					_ = connection.Close()
				}
				if heldRelease != nil {
					heldRelease()
				}
				f.runtime.Close()
				if requestStarted {
					mainWebDAVWait(t, handlerDone, "outer original HTTP handler")
				}
				if wireResponse != nil && wireResponse.Body != nil {
					_ = wireResponse.Body.Close()
				}
				server.Close()
			})
			beforeCancel := f.store.Pool().Stat().CanceledAcquireCount()
			if name == "startup-stop-joins-requests" {
				held, e := f.store.Pool().Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				var once sync.Once
				heldRelease = func() { once.Do(held.Release) }
			}
			var e error
			connection, e = net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if e != nil {
				t.Fatal(e)
			}
			_ = connection.SetDeadline(time.Now().Add(20 * time.Second))
			if tcp, ok := connection.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(1024)
			}
			requestStarted = true
			body, declared := "<D:propfind xmlns:D=\"DAV:\"><D:allprop/>", 200
			if output {
				body = ""
				declared = 0
			}
			_, e = fmt.Fprintf(connection, "PROPFIND /app/webdav/ HTTP/1.1\r\nHost: synthetic.test\r\nAuthorization: Basic c3ludGhldGljLWJhc2ljOnN5bnRoZXRpYy1wYXNzd29yZA==\r\nDepth: 1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", declared, body)
			if e != nil {
				t.Fatal(e)
			}
			actualConn := <-listener.connections
			if name == "no-second-response-after-write" {
				wireReader = bufio.NewReader(connection)
				wireResponse, e = http.ReadResponse(wireReader, &http.Request{Method: "PROPFIND"})
				if e != nil {
					t.Fatal("actual first response headers", e)
				}
				if wireResponse.StatusCode != 207 || wireResponse.ContentLength < 256 || !strings.Contains(wireResponse.Header.Get("Content-Type"), "xml") {
					t.Fatal("mid-write canary requires actual207 XML already started", wireResponse.StatusCode, wireResponse.ContentLength)
				}
				wirePrefix = make([]byte, 256)
				if _, e = io.ReadFull(wireResponse.Body, wirePrefix); e != nil || !bytes.Contains(wirePrefix, []byte("multistatus")) {
					t.Fatal("actual first multistatus body not observed", e)
				}
			}
			if output {
				deadline := time.Now().Add(10 * time.Second)
				stable := time.Time{}
				for {
					if actualConn.inWrite.Load() {
						if stable.IsZero() {
							stable = time.Now()
						}
						if time.Since(stable) > 30*time.Millisecond {
							break
						}
					} else {
						stable = time.Time{}
					}
					if time.Now().After(deadline) {
						t.Fatal("real TCP Write did not remain held")
					}
					select {
					case <-handlerDone:
						t.Fatal("unread real output ended before cancellation")
					case <-time.After(time.Millisecond):
					}
				}
			} else if name == "startup-stop-joins-requests" {
				// Wait for a genuine current native SH while Capture is still waiting for
				// its only actual PG connection; no successful proof/body/source exists yet.
				deadline := time.Now().Add(5 * time.Second)
				for {
					if actualRequest.Load() != nil && mainWebDAVNativeHeld(t, f.manager.ConfigPath()) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("actual pre-Capture pool wait missing")
					}
					time.Sleep(time.Millisecond)
				}
			} else {
				mainWebDAVWait(t, entered, "real incomplete PROPFIND Body.Read")
			}
			if name == "earliest-fixed-expiry" {
				mainWebDAVWait(t, handlerDone, "fixed original expiry")
				if time.Now().Before(lifetime) || time.Since(lifetime) > 5*time.Second {
					t.Fatal("original earlier expiry was renewed or ignored")
				}
				mainWebDAVWait(t, ended, "actual expired body read")
			} else {
				if name == "no-second-response-after-write" {
					f.settings.WebDAV.Password = "changed-synthetic-password"
					mainWebDAVSave(t, f.manager, f.settings)
					mainWebDAVWait(t, actualConn.deadline, "real native revocation write interrupt")
				}
				closed := make(chan struct{})
				go func() { defer close(closed); f.runtime.Close() }()
				mainWebDAVWait(t, closed, "Main owned runtime Close")
				mainWebDAVWait(t, handlerDone, "original HTTP return")
			}
			mainWebDAVWait(t, bodyClosed, "actual original body Close")
			if output {
				mainWebDAVWait(t, actualConn.deadline, "real held output deadline")
				mainWebDAVWait(t, actualConn.writeFailed, "actual held Write return")
			}
			if name == "startup-stop-joins-requests" {
				if f.store.Pool().Stat().CanceledAcquireCount() <= beforeCancel {
					t.Fatal("early owner did not cancel/join actual pre-Capture acquire")
				}
				heldRelease()
				heldRelease = nil
				if e = f.startup.Close(); e != nil {
					t.Fatal(e)
				}
			}
			if name == "no-second-response-after-write" {
				tail, _ := io.ReadAll(wireResponse.Body)
				_ = wireResponse.Body.Close()
				extra, _ := io.ReadAll(wireReader)
				body := append(append(wirePrefix, tail...), extra...)
				// DAV propstat XML legitimately contains HTTP/1.1 text. Only a new wire
				// status line or bytes following the declared response are a second response.
				if bytes.Contains(body, []byte("\r\nHTTP/1.1 ")) || bytes.HasPrefix(body, []byte("HTTP/1.1 ")) || len(extra) != 0 {
					t.Fatal("midresponse failure emitted another HTTP status line")
				}

			}
			f.runtime.Close()
			preflightBody := &mainWebDAVPreflightBody{reader: strings.NewReader("ignored")}
			preflight := httptest.NewRequest("OPTIONS", "/app/webdav/", nil)
			preflight.Body = preflightBody
			preflight.Header.Set("Origin", "http://localhost:3000")
			preflight.Header.Set("Access-Control-Request-Method", "GET")
			out := httptest.NewRecorder()
			f.entry.ServeHTTP(out, preflight)
			if out.Code != 503 || preflightBody.reads.Load() != 0 || preflightBody.closes.Load() != 0 {
				t.Fatal("closed Main dependency revived preflight/source work", out.Code)
			}
			if _, e := os.Stat(f.source.RootPath()); e != nil {
				t.Fatal("cleanup destroyed source metadata", e)
			}
		})
	}
}

func mainWebDAVNativeHeld(t *testing.T, configPath string) bool {
	t.Helper()
	info, e := os.Stat(filepath.Join(filepath.Dir(configPath), ".mediastorm-settings-commit", "commit-barrier.lock"))
	if e != nil {
		t.Fatal(e)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("actual lock identity unavailable")
	}
	identity := fmt.Sprintf("%02x:%02x:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)), stat.Ino)
	raw, e := os.ReadFile("/proc/locks")
	if e != nil {
		t.Fatal(e)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 8 && f[1] == "FLOCK" && f[3] == "READ" && f[4] == strconv.Itoa(os.Getpid()) && f[5] == identity {
			return true
		}
	}
	return false
}

func mainWebDAVSave(t *testing.T, manager *config.Manager, settings config.Settings) {
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

type mainWebDAVPreflightBody struct {
	reader        io.Reader
	reads, closes atomic.Int64
}

func (b *mainWebDAVPreflightBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	return b.reader.Read(p)
}
func (b *mainWebDAVPreflightBody) Close() error { b.closes.Add(1); return nil }
func mainWebDAVActualOptionsAndHealthy(t *testing.T, f *mainWebDAVFixture) {
	t.Helper()
	if handler, e := newManagedWebDAVRuntime(nil, nil, nil, "/webdav"); e == nil || handler != nil {
		t.Fatal("real Main constructor fabricated missing dependencies")
	}
	held, e := f.store.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	var once sync.Once
	release := func() { once.Do(held.Release) }
	t.Cleanup(release)
	for _, method := range []string{"PROPFIND", "LOCK"} {
		body := &mainWebDAVPreflightBody{reader: strings.NewReader("ignored")}
		r := httptest.NewRequest("OPTIONS", "/app/webdav/", nil)
		r.Body = body
		r.ContentLength = 7
		r.Header.Set("Origin", "http://localhost:3000")
		r.Header.Set("Access-Control-Request-Method", method)
		r.Header.Set("Access-Control-Request-Headers", "Authorization, Depth, Destination, Range, X-Arbitrary")
		w := httptest.NewRecorder()
		f.entry.ServeHTTP(w, r)
		want := 200
		if method == "LOCK" {
			want = 405
		}
		if w.Code != want || body.reads.Load() != 0 || body.closes.Load() != 0 || w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" || strings.Contains(w.Header().Get("Access-Control-Allow-Methods"), "LOCK") || strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), "X-Arbitrary") {
			t.Fatal("real Main preflight bypass/method/body contract failed", w.Code)
		}
		for _, header := range []string{"Depth", "Destination", "Range", "Authorization", "Content-Type"} {
			if !strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), header) {
				t.Fatal("real browser WebDAV header missing", header)
			}
		}
	}
	forbidden := httptest.NewRequest("OPTIONS", "/app/webdav/", nil)
	forbidden.Header.Set("Origin", "https://public.example.invalid")
	forbidden.Header.Set("Access-Control-Request-Method", "GET")
	forbiddenOut := httptest.NewRecorder()
	f.entry.ServeHTTP(forbiddenOut, forbidden)
	if forbiddenOut.Code != 403 || forbiddenOut.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unapproved origin received preflight capability metadata")
	}
	r := httptest.NewRequest("OPTIONS", "/app/webdav/", nil)
	w := httptest.NewRecorder()
	f.entry.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("nonbrowser OPTIONS fell through global shortcut", w.Code)
	}
	fallbackRouter := utils.NewRouter()
	mountManagedWebDAV(fallbackRouter, "/webdav", nil)
	fallback := utils.BasePathHandler("/app", managedWebDAVOptions("/webdav", nil, fallbackRouter))
	r = httptest.NewRequest("OPTIONS", "/app/webdav/", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	r.Header.Set("Access-Control-Request-Method", "GET")
	w = httptest.NewRecorder()
	fallback.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatal("missing managed browser preflight fell back to global200")
	}
	r = httptest.NewRequest("OPTIONS", "/app/unrelated-cors", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	w = httptest.NewRecorder()
	f.entry.ServeHTTP(w, r)
	if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("unrelated actual CORS route changed")
	}
	release()
	mainWebDAVRootCoexistence(t, f)
	server := httptest.NewServer(f.entry)
	client := server.Client()
	client.Timeout = 8 * time.Second
	t.Cleanup(func() { client.CloseIdleConnections(); server.Close() })
	request, e := http.NewRequest("PROPFIND", server.URL+"/app/webdav/", nil)
	if e != nil {
		t.Fatal(e)
	}
	request.SetBasicAuth("synthetic-basic", "synthetic-password")
	request.Header.Set("Origin", "http://localhost:3000")
	request.Header.Set("Depth", "1")
	response, e := client.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != 207 || !bytes.Contains(data, []byte("movie")) || response.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatal("actual Main BasePath/CORS healthy response final chunk truncated", readErr, closeErr, response.StatusCode)
	}
}

func mainWebDAVRootCoexistence(t *testing.T, f *mainWebDAVFixture) {
	t.Helper()
	f.settings.WebDAV.Prefix = "/"
	mainWebDAVSave(t, f.manager, f.settings)
	fs := nzbfilesystem.NewNzbFilesystem(nzbfilesystem.NewMetadataRemoteFile(f.source, nil, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} }))
	root, e := newManagedWebDAVRuntime(f.startup, f.store, fs, "/")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(root.Close)
	router := utils.NewRouter()
	var actualPosts atomic.Int64
	router.HandleFunc("/api/unrelated", func(w http.ResponseWriter, r *http.Request) { actualPosts.Add(1); w.WriteHeader(http.StatusNoContent) }).Methods(http.MethodPost, http.MethodOptions)
	mountManagedWebDAV(router, "/", root)
	entry := utils.BasePathHandler("/app", managedWebDAVOptions("/", root, router))
	held, e := f.store.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	var once sync.Once
	release := func() { once.Do(held.Release) }
	t.Cleanup(release)
	preflight := func(entry http.Handler, target, method string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("OPTIONS", target, nil)
		r.Header.Set("Origin", "http://localhost:3000")
		r.Header.Set("Access-Control-Request-Method", method)
		w := httptest.NewRecorder()
		entry.ServeHTTP(w, r)
		return w
	}
	w := preflight(entry, "/app/api/unrelated", "POST")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Access-Control-Allow-Methods"), "POST") || actualPosts.Load() != 0 {
		t.Fatal("root DAV captured unrelated API browser route", w.Code)
	}
	w = preflight(entry, "/app/movie", "GET")
	if w.Code != 200 || strings.Contains(w.Header().Get("Access-Control-Allow-Methods"), "POST") {
		t.Fatal("actual root DAV route lost accurate metadata", w.Code)
	}
	w = httptest.NewRecorder()
	entry.ServeHTTP(w, httptest.NewRequest("POST", "/app/api/unrelated", nil))
	if w.Code != 204 || actualPosts.Load() != 1 {
		t.Fatal("root DAV changed actual prior API POST", w.Code)
	}
	missingRouter := utils.NewRouter()
	missingRouter.HandleFunc("/api/unrelated", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }).Methods(http.MethodPost, http.MethodOptions)
	mountManagedWebDAV(missingRouter, "/", nil)
	missing := utils.BasePathHandler("/app", managedWebDAVOptions("/", nil, missingRouter))
	if w = preflight(missing, "/app/movie", "GET"); w.Code != 503 {
		t.Fatal("missing root DAV fell through unrelated CORS shortcut", w.Code)
	}
	if w = preflight(missing, "/app/api/unrelated", "POST"); w.Code != 200 {
		t.Fatal("missing root DAV captured independent prior API route", w.Code)
	}
	release()
	r := httptest.NewRequest("PROPFIND", "/app/", nil)
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	r.RemoteAddr = "127.0.0.1:23456"
	r.Header.Set("Depth", "1")
	w = httptest.NewRecorder()
	entry.ServeHTTP(w, r)
	if w.Code != 207 || !bytes.Contains(w.Body.Bytes(), []byte("/app/movie")) {
		t.Fatal("actual root DAV source route failed", w.Code, w.Body.String())
	}
	root.Close()
	f.settings.WebDAV.Prefix = "/webdav"
	mainWebDAVSave(t, f.manager, f.settings)
}
