//go:build linux

package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	managedHTTPTestUser    = "aaaaaaaa111122223333444444444444"
	managedHTTPTestPrimary = "bbbbbbbb111122223333444444444444"
	managedHTTPTestBacking = "cccccccc111122223333444444444444"
	managedHTTPTestLibrary = "dddddddd111122223333444444444444"
	managedHTTPTestToken   = "SYNTHETIC_HTTP_TOKEN_NOT_A_REAL_CREDENTIAL"
)

type managedHTTPFixture struct {
	server    *httptest.Server
	manager   *Manager
	settings  Settings
	lease     *ManagedJellyfinLease
	transport *ManagedJellyfinTransport
	data      []byte
	users     atomic.Int32
	media     atomic.Int32
	metadata  atomic.Int32
	bad       atomic.Bool
	mu        sync.Mutex
	user      func(int) (int, []byte)
	response  func(http.ResponseWriter, *http.Request)
	item      func(http.ResponseWriter, *http.Request)
}

func (fixture *managedHTTPFixture) setUser(value func(int) (int, []byte)) {
	fixture.mu.Lock()
	fixture.user = value
	fixture.mu.Unlock()
}

func (fixture *managedHTTPFixture) setResponse(value func(http.ResponseWriter, *http.Request)) {
	fixture.mu.Lock()
	fixture.response = value
	fixture.mu.Unlock()
}

func (fixture *managedHTTPFixture) setItem(value func(http.ResponseWriter, *http.Request)) {
	fixture.mu.Lock()
	fixture.item = value
	fixture.mu.Unlock()
}

func (fixture *managedHTTPFixture) serve(w http.ResponseWriter, r *http.Request) {
	fixture.mu.Lock()
	userResponse, downloadResponse, itemResponse := fixture.user, fixture.response, fixture.item
	fixture.mu.Unlock()
	if !strings.Contains(r.Header.Get("Authorization"), `Client="mediastorm"`) || !strings.Contains(r.Header.Get("Authorization"), `Token="`+managedHTTPTestToken+`"`) || r.Header.Get("User-Agent") != "mediastorm/1.0" || r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" || r.Method != http.MethodGet || r.ProtoMajor != 1 || strings.Contains(r.URL.RawQuery, "Token") || strings.Contains(r.URL.RawQuery, managedHTTPTestToken) {
		fixture.bad.Store(true)
	}
	user := r.URL.Query().Get("userId")
	switch r.URL.Path {
	case "/jf/Users/Me":
		if user != "" || r.URL.RawQuery != "" {
			fixture.bad.Store(true)
		}
		count := fixture.users.Add(1)
		status, raw := http.StatusOK, []byte(`{"Id":"`+managedHTTPTestUser+`","Policy":{"IsDisabled":false}}`)
		if userResponse != nil {
			status, raw = userResponse(int(count))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(raw)
	case "/jf/Items/" + managedHTTPTestPrimary, "/jf/Items/" + managedHTTPTestBacking:
		fixture.metadata.Add(1)
		if user != managedHTTPTestUser || len(r.URL.Query()) != 1 {
			fixture.bad.Store(true)
		}
		if itemResponse != nil {
			itemResponse(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"Id":%q,"UnverifiedMetadata":true}`, strings.TrimPrefix(r.URL.Path, "/jf/Items/"))
	case "/jf/Items/" + managedHTTPTestBacking + "/Ancestors":
		fixture.metadata.Add(1)
		if user != managedHTTPTestUser || len(r.URL.Query()) != 1 {
			fixture.bad.Store(true)
		}
		if itemResponse != nil {
			itemResponse(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"Id":%q}]`, managedHTTPTestLibrary)
	case "/jf/Items/" + managedHTTPTestBacking + "/Download":
		fixture.media.Add(1)
		if r.URL.RawQuery != "" {
			fixture.bad.Store(true)
		}
		if downloadResponse != nil {
			downloadResponse(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(fixture.data)))
		_, _ = w.Write(fixture.data)
	default:
		fixture.bad.Store(true)
		http.NotFound(w, r)
	}
}

func managedHTTPScope() ManagedJellyfinScope {
	return ManagedJellyfinScope{managedHTTPTestPrimary, managedHTTPTestBacking, managedHTTPTestLibrary}
}

func managedHTTPBoundedTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func newManagedHTTPFixture(t *testing.T, tlsServer bool) *managedHTTPFixture {
	t.Helper()
	fixture := &managedHTTPFixture{data: bytes.Repeat([]byte("Synthetic original HTTP bytes. "), 4096)}
	fixture.server = httptest.NewUnstartedServer(http.HandlerFunc(fixture.serve))
	if tlsServer {
		fixture.server.EnableHTTP2 = true
		fixture.server.StartTLS()
	} else {
		fixture.server.Start()
	}
	t.Cleanup(fixture.server.Close)
	address, ok := fixture.server.Listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		t.Fatal("own literal-loopback fixture required")
	}
	fixture.settings = DefaultSettings()
	fixture.settings.Jellyfin.Accounts = []JellyfinAccount{{ID: "integration", ServerURL: fixture.server.URL + "/jf", Token: managedHTTPTestToken, UserID: managedHTTPTestUser}}
	raw, err := json.Marshal(fixture.settings)
	if err != nil {
		t.Fatal("encode synthetic configuration")
	}
	fixture.manager = NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if os.WriteFile(fixture.manager.ConfigPath(), raw, 0o600) != nil {
		t.Fatal("write own synthetic settings")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	fixture.lease, err = fixture.manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		t.Fatal("capture genuine original synthetic settings")
	}
	t.Cleanup(func() { _ = fixture.lease.Close() })
	fixture.transport, err = fixture.lease.BindJellyfinScope(managedHTTPScope())
	if err != nil {
		t.Fatal("bind original exact scope")
	}
	if tlsServer {
		// This changes only this private native fixture transport's in-memory
		// trust roots, never a system CA or real configuration.
		roots := x509.NewCertPool()
		roots.AddCert(fixture.server.Certificate())
		fixture.transport.state.native.TLSClientConfig.RootCAs = roots
	}
	return fixture
}

func openManagedHTTPDownload(t *testing.T, fixture *managedHTTPFixture) *ManagedJellyfinDownload {
	t.Helper()
	download, err := fixture.transport.OpenDownload(t.Context())
	if err != nil || download == nil {
		t.Fatal("open actual original loopback Download")
	}
	t.Cleanup(func() { _ = download.Close() })
	return download
}

func TestManagedJellyfinHTTPExactScopeIdentityAndFiniteResponse(t *testing.T) {
	for _, tlsServer := range []bool{false, true} {
		t.Run(fmt.Sprint(tlsServer), func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, tlsServer)
			download := openManagedHTTPDownload(t, fixture)
			observation, err := fixture.transport.ObserveScope(t.Context())
			if err != nil || !managedHTTPJSON(observation.PrimaryJSON, false) || !managedHTTPJSON(observation.BackingJSON, false) || !managedHTTPJSON(observation.AncestorsJSON, true) || observation.StartedAt.IsZero() {
				t.Fatal("exact bounded metadata operation failed")
			}
			raw, err := io.ReadAll(download)
			if err != nil || sha256.Sum256(raw) != sha256.Sum256(fixture.data) || fixture.bad.Load() || fixture.media.Load() != 1 || fixture.users.Load() != 4 || fixture.metadata.Load() != 3 {
				t.Fatal("original finite response, identity or exact selector sequence changed")
			}
			info := download.Info()
			if info.Length != int64(len(fixture.data)) || info.ContentType != "application/octet-stream" {
				t.Fatal("safe finite response metadata changed")
			}
			deadline, _ := download.Context().Deadline()
			original, _ := fixture.lease.Context().Deadline()
			if !deadline.Equal(original) {
				t.Fatal("Download minted a different original deadline")
			}
		})
	}
}

func TestManagedJellyfinHTTPDirectDownloadRequiresCurrentOriginalTokenUser(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		raw  string
	}{
		{"no user API key", 400, `{}`},
		{"different user", 200, `{"Id":"eeeeeeee111122223333444444444444","Policy":{"IsDisabled":false}}`},
		{"disabled", 200, `{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":true}}`},
		{"unknown policy", 200, `{"Id":"` + managedHTTPTestUser + `"}`},
		{"duplicate user", 200, `{"Id":"` + managedHTTPTestUser + `","Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`},
		{"zero user", 200, `{"Id":"00000000000000000000000000000000","Policy":{"IsDisabled":false}}`},
		{"null disabled", 200, `{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":null}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			fixture.setUser(func(int) (int, []byte) { return tc.code, []byte(tc.raw) })
			if download, err := fixture.transport.OpenDownload(t.Context()); download != nil || err == nil || fixture.media.Load() != 0 || fixture.metadata.Load() != 0 {
				t.Fatal("direct Download bypassed original concrete token-user binding")
			}
			if download, err := fixture.transport.OpenDownload(t.Context()); download != nil || err == nil || fixture.media.Load() != 0 {
				t.Fatal("failed Download/user proof replayed or healed its claim")
			}
		})
	}
}

func TestManagedJellyfinHTTPPostHeaderUserLossClosesUnreadOriginal(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	fixture.setUser(func(count int) (int, []byte) {
		if count == 1 {
			return 200, []byte(`{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`)
		}
		return 400, []byte(`{}`)
	})
	if download, err := fixture.transport.OpenDownload(t.Context()); download != nil || err == nil || fixture.media.Load() != 1 {
		t.Fatal("unread body was published after post-header user loss")
	}
	owned := fixture.transport.state.download
	if owned == nil {
		t.Fatal("original rejected response ownership was not tracked")
	}
	select {
	case <-owned.state.closeDone:
	default:
		t.Fatal("post-header rejection did not join actual raw-body Close")
	}
	if fixture.bad.Load() {
		t.Fatal("rejected user attempted another endpoint or identity")
	}
}

func TestManagedJellyfinHTTPHeldDownloadPermitsSerialMetadataOnSecondConnection(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	fixture.setResponse(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(fixture.data)))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(fixture.data)
		case <-r.Context().Done():
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	download, err := fixture.transport.OpenDownload(ctx)
	if err != nil {
		t.Fatal("post-header original-user lookup could not use second connection")
	}
	defer download.Close()
	if _, err := fixture.transport.ObserveScope(ctx); err != nil || fixture.metadata.Load() != 3 {
		t.Fatal("held original response blocked the actual metadata connection")
	}
	unblock()
	if raw, err := io.ReadAll(download); err != nil || !bytes.Equal(raw, fixture.data) {
		t.Fatal("held original response changed after concurrent metadata")
	}
}

func TestManagedJellyfinHTTPRedirectAndFreshDisconnectNeverReplay(t *testing.T) {
	for _, kind := range []string{"http redirect", "https redirect", "same origin redirect", "fresh disconnect", "download denial"} {
		t.Run(kind, func(t *testing.T) {
			var foreignRequests, foreignConnections atomic.Int32
			foreign := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignRequests.Add(1); w.WriteHeader(500) }))
			foreign.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					foreignConnections.Add(1)
				}
			}
			if kind == "https redirect" {
				foreign.StartTLS()
			} else {
				foreign.Start()
			}
			defer foreign.Close()
			fixture := newManagedHTTPFixture(t, false)
			fixture.setResponse(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "fresh disconnect":
					connection, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = connection.Close()
					}
				case "download denial":
					w.WriteHeader(http.StatusForbidden)
				default:
					location := foreign.URL + "/forbidden"
					if kind == "same origin redirect" {
						location = fixture.server.URL + "/forbidden"
					}
					http.Redirect(w, r, location, http.StatusFound)
				}
			})
			if download, err := fixture.transport.OpenDownload(t.Context()); download != nil || err == nil {
				t.Fatal("redirect/disconnect/CanDownload denial became media")
			}
			if fixture.media.Load() != 1 || foreignRequests.Load() != 0 || foreignConnections.Load() != 0 || fixture.bad.Load() {
				t.Fatal("redirect or transparent reconnect replay escaped original request")
			}
		})
	}
}

func TestManagedJellyfinHTTPProxyEnvironmentAndArbitraryInputsAreUnused(t *testing.T) {
	var proxied atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { proxied.Add(1); w.WriteHeader(500) }))
	defer proxy.Close()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"} {
		t.Setenv(name, proxy.URL)
	}
	fixture := newManagedHTTPFixture(t, false)
	// A named original origin prevents the usual automatic loopback proxy
	// bypass from making the poison-proxy canary vacuous. The private native
	// dialer still accepts only this exact name and connects to our own socket.
	if fixture.lease.Close() != nil {
		t.Fatal("close initial original before named-origin capture")
	}
	fixture.settings.Jellyfin.Accounts[0].ServerURL = "http://owned-fixture.invalid/jf"
	raw, _ := json.Marshal(fixture.settings)
	if os.WriteFile(fixture.manager.ConfigPath(), raw, 0o600) != nil {
		t.Fatal("write own named-origin fixture")
	}
	var err error
	fixture.lease, err = fixture.manager.CaptureManagedJellyfin(managedHTTPBoundedTestContext(t), "integration")
	if err != nil {
		t.Fatal("capture own named original")
	}
	fixture.transport, err = fixture.lease.BindJellyfinScope(managedHTTPScope())
	if err != nil {
		t.Fatal("bind own named original")
	}
	dial := fixture.transport.state.dial
	address := managedHTTPLoopbackAddress(t, fixture.server.URL)
	fixture.transport.state.dial = func(ctx context.Context, network, target string) (net.Conn, error) {
		if network != "tcp" || target != "owned-fixture.invalid:80" {
			return nil, errors.New("unowned synthetic dial denied")
		}
		return dial(ctx, network, address)
	}
	if fixture.transport.state.native.Proxy != nil || fixture.transport.state.client.Jar != nil || fixture.transport.state.native.MaxConnsPerHost != 2 {
		t.Fatal("private native transport inherited proxy/cookie or connection state")
	}
	download := openManagedHTTPDownload(t, fixture)
	if _, err := io.Copy(io.Discard, download); err != nil || proxied.Load() != 0 {
		t.Fatal("poisoned proxy environment affected original scope")
	}
	for _, value := range []any{fixture.lease, *fixture.lease, fixture.transport, *fixture.transport, download, *download} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			text := fmt.Sprintf(format, value)
			if strings.Contains(text, managedHTTPTestToken) || strings.Contains(text, fixture.server.URL) {
				t.Fatal("capability formatting exposed credentials or private origin")
			}
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(ManagedJellyfinTransport{}), reflect.TypeOf(ManagedJellyfinDownload{})} {
		for index := 0; index < typ.NumField(); index++ {
			if typ.Field(index).PkgPath == "" {
				t.Fatal("HTTP capability exported its request/response/header state")
			}
		}
	}
	copy := *fixture.transport
	if copy.Close() == nil || fixture.transport.Context().Err() != nil {
		t.Fatal("copied transport gained or retired original authority")
	}
	if next, err := fixture.lease.BindJellyfinScope(managedHTTPScope()); next != nil || err == nil {
		t.Fatal("original lease acquired a second scope")
	}
}

func TestManagedJellyfinHTTPRejectsUnsupportedDownloadFraming(t *testing.T) {
	for _, kind := range []string{"zero", "unknown", "partial", "range", "encoding", "large", "json", "short"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			fixture.setResponse(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", "8")
				switch kind {
				case "zero":
					w.Header().Set("Content-Length", "0")
				case "unknown":
					w.Header().Del("Content-Length")
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				case "partial":
					w.WriteHeader(206)
				case "range":
					w.Header().Set("Content-Range", "bytes 0-7/8")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "large":
					w.Header().Set("Content-Length", "8589934593")
				case "json":
					w.Header().Set("Content-Type", "application/json")
				}
				_, _ = w.Write([]byte("short"))
			})
			download, err := fixture.transport.OpenDownload(t.Context())
			if kind == "short" {
				if err != nil || download == nil {
					t.Fatal("finite declared header was rejected before body evidence")
				}
				defer download.Close()
				if raw, err := io.ReadAll(download); err == nil || len(raw) > 8 {
					t.Fatal("short response was accepted as complete or exceeded framing")
				}
			} else if download != nil || err == nil {
				t.Fatal("unsupported finite response shape was accepted")
			}
			if fixture.media.Load() != 1 {
				t.Fatal("bad response caused another Download request")
			}
		})
	}
}

type managedHTTPObservedConn struct {
	net.Conn
	download atomic.Bool
	reads    atomic.Int32
	armRead  atomic.Bool
	armClose atomic.Bool
	readOnce sync.Once
	endOnce  sync.Once
	readIn   chan struct{}
	closeIn  chan struct{}
	release  chan struct{}
}

func (connection *managedHTTPObservedConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("/Items/"+managedHTTPTestBacking+"/Download")) {
		connection.download.Store(true)
	}
	return connection.Conn.Write(p)
}
func (connection *managedHTTPObservedConn) Read(p []byte) (int, error) {
	if connection.download.Load() {
		connection.reads.Add(1)
		if connection.armRead.Load() {
			connection.readOnce.Do(func() { close(connection.readIn) })
		}
	}
	return connection.Conn.Read(p)
}
func (connection *managedHTTPObservedConn) Close() error {
	if connection.download.Load() && connection.armClose.Load() {
		connection.endOnce.Do(func() { close(connection.closeIn); <-connection.release })
	}
	return connection.Conn.Close()
}

func observeManagedHTTPConnections(fixture *managedHTTPFixture) *sync.Map {
	connections := &sync.Map{}
	dial := fixture.transport.state.dial
	fixture.transport.state.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		actual, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		observed := &managedHTTPObservedConn{Conn: actual, readIn: make(chan struct{}), closeIn: make(chan struct{}), release: make(chan struct{})}
		connections.Store(observed, true)
		return observed, nil
	}
	return connections
}

func managedHTTPDownloadConnection(t *testing.T, connections *sync.Map) *managedHTTPObservedConn {
	t.Helper()
	var selected *managedHTTPObservedConn
	connections.Range(func(key, _ any) bool {
		value := key.(*managedHTTPObservedConn)
		if value.download.Load() {
			selected = value
			return false
		}
		return true
	})
	if selected == nil {
		t.Fatal("actual own Download connection was not observed")
	}
	return selected
}

func TestManagedJellyfinHTTPBufferedDrainChecksOriginalManagedEpoch(t *testing.T) {
	for _, mode := range []string{"same-value Save", "failed Save", "actual replacement"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			connections := observeManagedHTTPConnections(fixture)
			download := openManagedHTTPDownload(t, fixture)
			if n, err := download.Read(make([]byte, 1)); n != 1 || err != nil {
				t.Fatal("actual initial read did not prefill native HTTP buffering")
			}
			connection := managedHTTPDownloadConnection(t, connections)
			before := connection.reads.Load()
			switch mode {
			case "same-value Save":
				if fixture.manager.Save(fixture.settings) != nil {
					t.Fatal("actual same-value managed Save")
				}
			case "failed Save":
				settings := fixture.settings
				settings.Server.ExternalBackendURL = "file:///unsupported"
				if fixture.manager.Save(settings) == nil {
					t.Fatal("invalid managed Save unexpectedly succeeded")
				}
			case "actual replacement":
				staged := fixture.manager.ConfigPath() + ".staged"
				raw, _ := json.Marshal(fixture.settings)
				if os.WriteFile(staged, raw, 0o600) != nil || ReplaceManagedSettingsFile(staged, fixture.manager.ConfigPath()) != nil {
					t.Fatal("actual managed replacement failed")
				}
			}
			buffer := bytes.Repeat([]byte{0xa7}, 64)
			if n, err := download.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, bytes.Repeat([]byte{0xa7}, 64)) || connection.reads.Load() != before {
				t.Fatal("native buffered bytes escaped the current consumer boundary")
			}
		})
	}
}

func TestManagedJellyfinHTTPConcurrentLeaseCloseJoinsRealReadAndBodyClose(t *testing.T) {
	for _, stage := range []string{"active read", "body Close"} {
		t.Run(stage, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			connections := observeManagedHTTPConnections(fixture)
			serverRelease := make(chan struct{})
			var serverOnce sync.Once
			releaseServer := func() { serverOnce.Do(func() { close(serverRelease) }) }
			defer releaseServer()
			fixture.setResponse(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", strconv.Itoa(len(fixture.data)))
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				select {
				case <-serverRelease:
					_, _ = w.Write(fixture.data)
				case <-r.Context().Done():
				}
			})
			download := openManagedHTTPDownload(t, fixture)
			connection := managedHTTPDownloadConnection(t, connections)
			connection.armClose.Store(true)
			var once sync.Once
			unblock := func() { once.Do(func() { close(connection.release) }) }
			defer unblock()
			readDone := make(chan error, 1)
			buffer := bytes.Repeat([]byte{0xa9}, 64)
			if stage == "active read" {
				connection.armRead.Store(true)
				go func() { _, err := download.Read(buffer); readDone <- err }()
				select {
				case <-connection.readIn:
				case <-time.After(2 * time.Second):
					t.Fatal("actual consumer read did not enter held own socket")
				}
			} else {
				go func() { readDone <- download.Close() }()
			}
			closed := make(chan error, 2)
			go func() { closed <- fixture.lease.Close() }()
			go func() { closed <- fixture.lease.Close() }()
			select {
			case <-connection.closeIn:
			case <-time.After(2 * time.Second):
				t.Fatal("actual native connection/body Close did not enter cleanup barrier")
			}
			select {
			case <-closed:
				t.Fatal("concurrent lease.Close returned before actual HTTP cleanup")
			default:
			}
			managedConfigAdmission.Lock()
			_, charged := managedConfigAdmission.owners[fixture.lease.state]
			managedConfigAdmission.Unlock()
			managedHTTPAdmission.Lock()
			httpCharged := managedHTTPAdmission.keys[fixture.transport.state.key]
			managedHTTPAdmission.Unlock()
			if !charged || httpCharged != 1 {
				t.Fatal("actual lease/transport admission refunded before cleanup")
			}
			written := make(chan error, 1)
			go func() { written <- fixture.manager.Save(fixture.settings) }()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal("managed write failed while actual Close was held")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("writer joined HTTP cleanup or held a close-cycle lock")
			}
			unblock()
			var results [2]error
			for index := range results {
				select {
				case results[index] = <-closed:
				case <-time.After(2 * time.Second):
					t.Fatal("concurrent lease close did not join actual cleanup")
				}
			}
			if results[0] != results[1] {
				t.Fatal("concurrent original closes returned different completion results")
			}
			select {
			case err := <-readDone:
				if stage == "active read" && (err == nil || !bytes.Equal(buffer, bytes.Repeat([]byte{0xa9}, 64))) {
					t.Fatal("cancelled held read published stale bytes")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("actual read/body Close did not join")
			}
			managedHTTPAdmission.Lock()
			remaining := managedHTTPAdmission.keys[fixture.transport.state.key]
			managedHTTPAdmission.Unlock()
			if remaining != 0 {
				t.Fatal("actual cleanup did not refund transport admission")
			}
		})
	}
}

func TestManagedJellyfinHTTPLateNativeDialRemainsChargedUntilActualClose(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	dial := fixture.transport.state.dial
	fixture.transport.state.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		actual, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		close(started)
		<-release // An actual acquired socket returns after caller cancellation.
		return actual, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := make(chan error, 1)
	go func() { _, err := fixture.transport.ObserveScope(ctx); observed <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("actual private dial did not acquire its loopback socket")
	}
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- fixture.lease.Close() }()
	select {
	case <-closed:
		t.Fatal("late native acquisition was not charged through lease.Close")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-observed:
		if err == nil {
			t.Fatal("cancelled late metadata operation succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled actual Do did not finish")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("late actual socket cleanup did not join")
	}
}

func TestManagedJellyfinHTTPSetupHandoffDoesNotShortenOriginalBodyLifetime(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	setup, cancel := context.WithTimeout(t.Context(), time.Second)
	download, err := fixture.transport.OpenDownload(setup)
	if err != nil {
		cancel()
		t.Fatal("actual setup handoff failed")
	}
	defer download.Close()
	cancel()
	if raw, err := io.ReadAll(download); err != nil || !bytes.Equal(raw, fixture.data) || fixture.lease.Check(t.Context()) != nil {
		t.Fatal("successful one-way handoff remained attached to setup cancellation")
	}
	if next, err := fixture.transport.OpenDownload(t.Context()); next != nil || err == nil || fixture.media.Load() != 1 {
		t.Fatal("one held response was reopened")
	}
}

func TestManagedJellyfinHTTPSharedObservationBudgetAndFreshCallerCancellation(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	download := openManagedHTTPDownload(t, fixture)
	var slow atomic.Bool
	slow.Store(true)
	fixture.setUser(func(int) (int, []byte) {
		if slow.Load() {
			time.Sleep(150 * time.Millisecond)
		}
		return 200, []byte(`{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := fixture.transport.ObserveScope(ctx); err == nil {
		t.Fatal("fresh caller cancellation did not abort its own operation")
	}
	slow.Store(false)
	if fixture.transport.Context().Err() != nil || fixture.lease.Check(t.Context()) != nil {
		t.Fatal("fresh cancelled observation retired a healthy original")
	}
	if n, err := download.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatal("fresh caller cancellation retired the original held body")
	}
	// A separate genuine operation spends the fixed shared3s budget across
	// both user checks, rather than allocating a new budget to every stage.
	fixture.setUser(func(int) (int, []byte) {
		time.Sleep(1600 * time.Millisecond)
		return 200, []byte(`{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`)
	})
	started := time.Now()
	if _, err := fixture.transport.ObserveScope(t.Context()); err == nil || time.Since(started) > 5*time.Second {
		t.Fatal("bounded observation restarted its budget per request")
	}
}

func TestManagedJellyfinHTTPJoinedCancelLinkAccountsTrueStopAndRunningCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var called atomic.Bool
	stop := managedHTTPAfterFunc(ctx, func() { called.Store(true) })
	if !stop() {
		t.Fatal("live exact callback could not be stopped")
	}
	cancel()
	if !stop() || called.Load() {
		t.Fatal("true-stop then cancellation stranded or executed the old callback")
	}
	ctx, cancel = context.WithCancel(t.Context())
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	stop = managedHTTPAfterFunc(ctx, func() { close(entered); <-release })
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual cancel callback did not enter")
	}
	joined := make(chan bool, 1)
	go func() { joined <- stop() }()
	select {
	case <-joined:
		t.Fatal("running cancellation callback was not joined")
	default:
	}
	unblock()
	select {
	case prevented := <-joined:
		if prevented {
			t.Fatal("running callback was mislabeled stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("completed callback did not join")
	}
}

func TestManagedJellyfinHTTPBoundedScopeAdmissionAndOpaqueCopies(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	var imported ManagedJellyfinTransport
	if json.Unmarshal([]byte("{}"), &imported) == nil || imported.Close() == nil {
		t.Fatal("imported transport acquired original ownership")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	second, err := fixture.manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		t.Fatal("capture second genuine original")
	}
	defer second.Close()
	bound, err := second.BindJellyfinScope(managedHTTPScope())
	if err != nil {
		t.Fatal("second original binding failed before capacity")
	}
	defer bound.Close()
	third, err := fixture.manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		t.Fatal("capture third genuine configuration")
	}
	defer third.Close()
	if next, err := third.BindJellyfinScope(managedHTTPScope()); next != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("integration transport admission exceeded its bound")
	}
	if bound.Close() != nil {
		t.Fatal("actual unused transport cleanup failed")
	}
	if next, err := third.BindJellyfinScope(managedHTTPScope()); next != nil || err == nil {
		t.Fatal("failed admitted binding retried its consumed original claim")
	}
	var download ManagedJellyfinDownload
	if json.Unmarshal([]byte("{}"), &download) == nil {
		t.Fatal("imported Download acquired ownership")
	}
	if n, err := download.Read(make([]byte, 1)); n != 0 || err == nil || download.Close() == nil {
		t.Fatal("unowned Download operated on original bytes")
	}
	for _, bad := range []string{"", "http://foreign.invalid/", "00000000000000000000000000000000", "urn:uuid:" + managedHTTPTestPrimary} {
		fresh, err := fixture.manager.CaptureManagedJellyfin(ctx, "integration")
		if err != nil {
			t.Fatal("capture selector validation original")
		}
		if got, err := fresh.BindJellyfinScope(ManagedJellyfinScope{bad, managedHTTPTestBacking, managedHTTPTestLibrary}); got != nil || err == nil {
			_ = fresh.Close()
			t.Fatal("free URL or nonconcrete selector was accepted")
		}
		_ = fresh.Close()
	}
}

func TestManagedJellyfinHTTPGlobalAdmissionIsChargedUntilExplicitCleanup(t *testing.T) {
	var fixtures []*managedHTTPFixture
	for range 8 {
		fixtures = append(fixtures, newManagedHTTPFixture(t, false))
	}
	manager := fixtures[0].manager
	denied, err := manager.CaptureManagedJellyfin(managedHTTPBoundedTestContext(t), "integration")
	if err != nil {
		t.Fatal("capture global-bound canary")
	}
	defer denied.Close()
	if bound, err := denied.BindJellyfinScope(managedHTTPScope()); bound != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("global bound admitted a ninth live transport")
	}
	if fixtures[7].transport.Close() != nil {
		t.Fatal("explicit actual transport cleanup failed")
	}
	if bound, err := denied.BindJellyfinScope(managedHTTPScope()); bound != nil || err == nil {
		t.Fatal("released capacity healed an already consumed claim")
	}
	fresh, err := manager.CaptureManagedJellyfin(managedHTTPBoundedTestContext(t), "integration")
	if err != nil {
		t.Fatal("capture post-cleanup original")
	}
	defer fresh.Close()
	bound, err := fresh.BindJellyfinScope(managedHTTPScope())
	if err != nil {
		t.Fatal("joined cleanup did not reclaim global capacity")
	}
	defer bound.Close()
}

func TestManagedJellyfinHTTPMetadataIsWholeBoundedAndUnverified(t *testing.T) {
	for _, kind := range []string{"duplicate", "trailing", "array", "utf8", "deep", "oversize chunked", "many ancestors"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			fixture.setItem(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if kind == "many ancestors" {
					if strings.HasSuffix(r.URL.Path, "/Ancestors") {
						_, _ = w.Write([]byte("[" + strings.Repeat(`{},`, 64) + "{}]"))
					} else {
						_, _ = w.Write([]byte(`{"Id":"unverified"}`))
					}
					return
				}
				switch kind {
				case "duplicate":
					_, _ = w.Write([]byte(`{"Id":"one","Id":"two"}`))
				case "trailing":
					_, _ = w.Write([]byte(`{} {}`))
				case "array":
					_, _ = w.Write([]byte(`[]`))
				case "utf8":
					_, _ = w.Write([]byte{'{', '"', 'a', '"', ':', '"', 0xff, '"', '}'})
				case "deep":
					_, _ = w.Write([]byte(`{"a":` + strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65) + "}"))
				case "oversize chunked":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					_, _ = w.Write(bytes.Repeat([]byte(" "), (1<<20)+1))
				}
			})
			observation, err := fixture.transport.ObserveScope(t.Context())
			if err == nil || len(observation.PrimaryJSON)+len(observation.BackingJSON)+len(observation.AncestorsJSON) != 0 || !observation.StartedAt.IsZero() || fixture.media.Load() != 0 {
				t.Fatal("invalid metadata became a bounded observation or media permission")
			}
		})
	}
}

func TestManagedJellyfinHTTPMetadataAdmissionDoesNotQueueOrResetDownloadClaim(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	fixture.setUser(func(count int) (int, []byte) {
		if count == 1 {
			close(entered)
			<-release
		}
		return 200, []byte(`{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`)
	})
	completed := make(chan error, 1)
	go func() { _, err := fixture.transport.ObserveScope(t.Context()); completed <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual original metadata request did not enter")
	}
	if _, err := fixture.transport.ObserveScope(t.Context()); !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("concurrent metadata queued instead of denying at admission")
	}
	if body, err := fixture.transport.OpenDownload(t.Context()); body != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("Download setup exceeded the metadata reservation")
	}
	unblock()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal("admission denial invalidated the admitted original")
		}
	case <-time.After(time.Second):
		t.Fatal("released original metadata did not complete")
	}
	if body, err := fixture.transport.OpenDownload(t.Context()); body != nil || err == nil || fixture.media.Load() != 0 {
		t.Fatal("busy setup healed its consumed Download claim")
	}
}

func TestManagedJellyfinHTTPOriginalExpiryCannotPublishBufferedBody(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	if fixture.lease.Close() != nil {
		t.Fatal("close initial synthetic lease")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var err error
	fixture.lease, err = fixture.manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		t.Fatal("capture original short deadline")
	}
	fixture.transport, err = fixture.lease.BindJellyfinScope(managedHTTPScope())
	if err != nil {
		t.Fatal("bind short original deadline")
	}
	body := openManagedHTTPDownload(t, fixture)
	deadline, _ := body.Context().Deadline()
	original, _ := ctx.Deadline()
	if !deadline.Equal(original) {
		t.Fatal("body minted a later expiry")
	}
	<-ctx.Done()
	buffer := bytes.Repeat([]byte{0xb1}, 64)
	if n, err := body.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, bytes.Repeat([]byte{0xb1}, 64)) {
		t.Fatal("original expiry admitted already buffered bytes")
	}
}

// Preserve the actual caller Done/Err signal while deliberately delaying the
// delivery of its cancel-only link into the owned operation budget.
type managedHTTPDelayedCaller struct {
	context.Context
	entered, release, done, stopRequested chan struct{}
}

func (*managedHTTPDelayedCaller) Value(any) any { return nil }
func (caller *managedHTTPDelayedCaller) AfterFunc(callback func()) func() bool {
	stop := context.AfterFunc(caller.Context, func() {
		defer close(caller.done)
		close(caller.entered)
		<-caller.release
		callback()
	})
	var once sync.Once
	return func() bool {
		once.Do(func() { close(caller.stopRequested) })
		return stop()
	}
}

func TestManagedJellyfinHTTPFinalPublicationChecksCallerBeforeDelayedLink(t *testing.T) {
	for _, operation := range []string{"Download", "observation"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			caller := &managedHTTPDelayedCaller{Context: ctx, entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}), stopRequested: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(caller.release) }) }
			defer unblock()
			fixture.setUser(func(count int) (int, []byte) {
				if count == 2 {
					cancel()
					<-caller.entered
				}
				return 200, []byte(`{"Id":"` + managedHTTPTestUser + `","Policy":{"IsDisabled":false}}`)
			})
			completed := make(chan error, 1)
			go func() {
				if operation == "Download" {
					body, err := fixture.transport.OpenDownload(caller)
					if body != nil {
						_ = body.Close()
					}
					completed <- err
				} else {
					_, err := fixture.transport.ObserveScope(caller)
					completed <- err
				}
			}()
			select {
			case <-caller.entered:
			case <-time.After(time.Second):
				t.Fatal("final real user response did not cancel original caller")
			}
			// Wait until final result selection stops the still-unstarted owned
			// callback. Its custom parent's delayed delivery is a separate test
			// resource, joined explicitly below; the budget is still healthy.
			select {
			case <-caller.stopRequested:
			case <-time.After(time.Second):
				t.Fatal("operation did not reach final callback join")
			}
			select {
			case err := <-completed:
				if err == nil {
					t.Fatal("delayed callback concealed an already canceled caller at publication")
				}
			case <-time.After(time.Second):
				t.Fatal("stopped owned callback stranded operation completion")
			}
			unblock()
			select {
			case <-caller.done:
			case <-time.After(time.Second):
				t.Fatal("custom parent delivery did not join")
			}
			if fixture.transport.Context().Err() != nil || fixture.lease.Check(t.Context()) != nil {
				t.Fatal("caller-only cancellation invalidated healthy original authority")
			}
		})
	}
}

func TestManagedJellyfinHTTPAmbiguousOriginalCredentialsCannotReachNetwork(t *testing.T) {
	for _, kind := range []string{"token quote", "token comma", "token whitespace", "user zero", "userinfo", "query", "fragment", "traversal", "escaped separator"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newManagedHTTPFixture(t, false)
			if fixture.lease.Close() != nil {
				t.Fatal("close initial synthetic configuration")
			}
			account := &fixture.settings.Jellyfin.Accounts[0]
			switch kind {
			case "token quote":
				account.Token += `"`
			case "token comma":
				account.Token += ","
			case "token whitespace":
				account.Token += " "
			case "user zero":
				account.UserID = "00000000000000000000000000000000"
			case "userinfo":
				account.ServerURL = strings.Replace(fixture.server.URL, "://", "://synthetic@", 1)
			case "query":
				account.ServerURL += "?other=selector"
			case "fragment":
				account.ServerURL += "#other"
			case "traversal":
				account.ServerURL += "/../other"
			case "escaped separator":
				account.ServerURL += "/%2fother"
			}
			raw, _ := json.Marshal(fixture.settings)
			if os.WriteFile(fixture.manager.ConfigPath(), raw, 0o600) != nil {
				t.Fatal("write own ambiguous synthetic configuration")
			}
			ctx := managedHTTPBoundedTestContext(t)
			original, err := fixture.manager.CaptureManagedJellyfin(ctx, "integration")
			if err == nil {
				defer original.Close()
				if bound, err := original.BindJellyfinScope(managedHTTPScope()); bound != nil || err == nil {
					t.Fatal("ambiguous original credential or endpoint gained a transport")
				}
			}
			if ctx.Err() != nil {
				t.Fatal("configuration negative control lost its genuine bounded context")
			}
			if fixture.users.Load()+fixture.media.Load()+fixture.metadata.Load() != 0 {
				t.Fatal("invalid original configuration performed network IO")
			}
		})
	}
}

// Keep the fixture dialer explicit if tests ever need a named origin: no test
// is allowed to resolve or contact an address outside its own listener.
func managedHTTPLoopbackAddress(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() {
		t.Fatal("literal loopback fixture address required")
	}
	return u.Host
}
