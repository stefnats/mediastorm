//go:build linux

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
	"novastream/services/remotemedia"
	"novastream/utils"
)

type registeredFamilyParent struct {
	*localParentFixture
	installed    *handlers.FamilyMediaInstallation
	local        *localmedia.Service
	media        *handlers.LocalMediaHandler
	usersHandler *handlers.UsersHandler
	pins         *profileunlock.PGProfilePins
	accounts     *accounts.Service
	ctx          context.Context
	cancel       context.CancelFunc
}

func registeredOriginalParent(t *testing.T) *registeredFamilyParent {
	t.Helper()
	base := localParentPG(t)
	ctx, cancel := context.WithCancel(t.Context())
	f := &registeredFamilyParent{localParentFixture: base, ctx: ctx, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		if f.installed != nil {
			f.installed.Close()
		}
		if f.local != nil {
			_ = f.local.Close()
		}
	})
	var err error
	f.local, err = localmedia.PrepareService(ctx, f.ds, nil, "/synthetic-probe-must-not-run")
	if err != nil {
		t.Fatal(err)
	}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	raw, err := json.Marshal(config.DefaultSettings())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manager.ConfigPath(), raw, 0600); err != nil {
		t.Fatal(err)
	}
	remote, err := remotemedia.NewService(f.ds, manager, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.accounts, err = accounts.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	f.pins = profileunlock.NewPGProfilePins(f.ds, f.sessions)
	f.usersHandler = handlers.NewUsersHandler(f.users, f.accounts)
	f.usersHandler.SetPGProfilePins(f.pins)
	f.installed, err = f.usersHandler.PrepareFamilyMediaConsumers(ctx, f.ds, f.pins, f.local, manager, remote)
	if err != nil {
		t.Fatal("actual original managed family constructor", err)
	}
	f.files = f.installed.ParentFiles
	f.media = handlers.NewLocalMediaHandler(f.local, f.users, true)
	if err = f.local.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	f.register(t, f.installed, f.usersHandler, f.pins, "")
	return f
}
func (f *registeredFamilyParent) register(t *testing.T, installed *handlers.FamilyMediaInstallation, users *handlers.UsersHandler, pins *profileunlock.PGProfilePins, base string) {
	t.Helper()
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	list.SetMetadataService(f.facts)
	list.SetMetadataHandler(meta)
	f.router = mux.NewRouter()
	Register(f.router, nil, meta,
		nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{},
		users, nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, f.media,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		f.accounts, f.sessions, f.users, nil, nil, nil, "", nil,
		WithPGProfilePins(pins), WithProfileUnlocks(profileunlock.NewService(f.ds.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: false} }), WithFamilyMediaInstallation(installed, base))
}

type originalParentAPIRecorder struct{ *localDeadlineRecorder }

func (*originalParentAPIRecorder) SetReadDeadline(time.Time) error { return nil }
func (f *registeredFamilyParent) call(r *http.Request) *originalParentAPIRecorder {
	w := &originalParentAPIRecorder{&localDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}}
	f.router.ServeHTTP(w, r)
	return w
}
func (f *registeredFamilyParent) request(method, path, body, intent, password string) *http.Request {
	r := f.localParentFixture.request(method, path, body, intent, password)
	// httptest wraps an empty strings.Reader even for a zero-length request;
	// actual no-body wire requests use NoBody. Preserve real malformed TCP
	// body cases separately instead of bypassing the production body guard.
	if body == "" {
		r.Body = http.NoBody
	}
	return r
}
func (f *registeredFamilyParent) challenge(t *testing.T) string {
	t.Helper()
	w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), "", ""))
	var value struct {
		Intent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || !strings.HasPrefix(value.Intent, "pfa_") {
		t.Fatal("registered managed challenge unavailable", w.Code, w.Body.String())
	}
	return value.Intent
}
func (f *registeredFamilyParent) approve(t *testing.T) string {
	t.Helper()
	intent := f.challenge(t)
	w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), intent, "synthetic-api-password"))
	var value struct {
		URL string `json:"streamUrl"`
		ID  string `json:"approvalId"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || !strings.HasPrefix(value.ID, "lfa_") {
		t.Fatal("registered fresh managed approval unavailable", w.Code, w.Body.String())
	}
	return value.URL
}
func (f *registeredFamilyParent) wire(t *testing.T, r *http.Request) (*http.Response, []byte) {
	t.Helper()
	server := httptest.NewServer(f.router)
	defer server.Close()
	request, err := http.NewRequest(r.Method, server.URL+r.URL.RequestURI(), r.Body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = r.Header.Clone()
	request.ContentLength = r.ContentLength
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal("actual registered publication transport", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.ContentLength != int64(len(body)) || len(response.TransferEncoding) != 0 {
		t.Fatal("original publication did not complete exact physical framing", err, response.ContentLength, len(body))
	}
	return response, body
}
func (f *registeredFamilyParent) approveWire(t *testing.T) string {
	t.Helper()
	intent := f.challenge(t)
	response, body := f.wire(t, f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), intent, "synthetic-api-password"))
	var value struct {
		URL string `json:"streamUrl"`
		ID  string `json:"approvalId"`
	}
	if response.StatusCode != 200 || json.Unmarshal(body, &value) != nil || value.URL == "" || !strings.HasPrefix(value.ID, "lfa_") || originalParentAPIFDs(t, f.path) == 0 {
		t.Fatal("actual original approval source/publication not retained", response.StatusCode)
	}
	return value.URL
}

func (f *registeredFamilyParent) mediaInput(t *testing.T) {
	t.Helper()
	args := []string{"-nostdin", "-v", "error", "-threads", "1", "-filter_threads", "1", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "3", "-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-threads:v", "1", "-preset", "ultrafast", "-g", "25", "-c:a", "aac", "-threads:a", "1", "-f", "matroska", "-y", f.path}
	if err := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).Run(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE local_media_items SET size_bytes=$1 WHERE id=$2`, info.Size(), f.item)
}
func originalParentAPIWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("actual managed parent operation did not join")
	}
}
func originalParentAPIUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(15 * time.Second)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("actual managed parent witness absent")
		}
		time.Sleep(time.Millisecond)
	}
}

type originalParentAPIConn struct {
	net.Conn
	reading     atomic.Int32
	writing     atomic.Int32
	interrupted atomic.Bool
}

func (c *originalParentAPIConn) Read(p []byte) (int, error) {
	c.reading.Add(1)
	defer c.reading.Add(-1)
	return c.Conn.Read(p)
}
func (c *originalParentAPIConn) Write(p []byte) (int, error) {
	c.writing.Add(1)
	defer c.writing.Add(-1)
	return c.Conn.Write(p)
}
func (c *originalParentAPIConn) SetReadDeadline(at time.Time) error {
	if !at.IsZero() && !at.After(time.Now()) {
		c.interrupted.Store(true)
	}
	return c.Conn.SetReadDeadline(at)
}

type originalParentAPIListener struct {
	net.Listener
	accepted chan *originalParentAPIConn
}

func (l *originalParentAPIListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		if err = tcp.SetWriteBuffer(1024); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	actual := &originalParentAPIConn{Conn: c}
	l.accepted <- actual
	return actual, nil
}
func originalParentAPITCP(t *testing.T, f *registeredFamilyParent, handler http.Handler, request string, read func(net.Conn, *originalParentAPIConn, <-chan struct{})) {
	t.Helper()
	done := make(chan struct{})
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer once.Do(func() { close(done) })
		handler.ServeHTTP(w, r)
	}))
	listener := &originalParentAPIListener{Listener: server.Listener, accepted: make(chan *originalParentAPIConn, 2)}
	server.Listener = listener
	server.Start()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	// Register basic physical ownership before any fallible buffer/setup step.
	t.Cleanup(func() { f.cancel(); _ = connection.Close(); server.Close() })
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err = tcp.SetReadBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}
	if err = connection.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(connection, request); err != nil {
		t.Fatal(err)
	}
	var actual *originalParentAPIConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("actual connection not accepted")
	}
	read(connection, actual, done)
}
func originalParentAPIFDs(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err == nil && (target == path || target == path+" (deleted)") {
			count++
		}
	}
	return count
}
func originalParentAPIWriteBlocked() bool {
	buffer := make([]byte, 2<<20)
	n := runtime.Stack(buffer, true)
	for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
		if strings.Contains(stack, "(*originalParentAPIConn).Write(") && strings.Contains(stack, "internal/poll.(*FD).Write(") && strings.Contains(stack, "runtime_pollWait(") {
			return true
		}
	}
	return false
}
func originalParentHeldRegisteredWrite(t *testing.T, originalCancel bool) {
	f := registeredOriginalParent(t)
	payload := bytes.Repeat([]byte("original parent bytes "), 1<<18)
	if err := os.WriteFile(f.path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE local_media_items SET size_bytes=$1 WHERE id=$2`, len(payload), f.item)
	path := f.approve(t)
	request := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: synthetic\r\nAuthorization: Bearer %s\r\nX-Client-ID: synthetic-pg-api-client\r\n\r\n", path, f.token)
	originalParentAPITCP(t, f, f.router, request, func(connection net.Conn, actual *originalParentAPIConn, done <-chan struct{}) {
		originalParentAPIUntil(t, func() bool { return originalParentAPIWriteBlocked() })
		if originalParentAPIFDs(t, f.path) < 2 {
			t.Fatal("actual registered source+duplicate not retained")
		}
		if originalCancel {
			f.cancel()
		}
		f.installed.Close()
		originalParentAPIWait(t, done)
		_ = connection.Close()
		if actual.writing.Load() != 0 || originalParentAPIFDs(t, f.path) != 0 {
			t.Fatal("actual transport/source work survived Close")
		}
		if !originalCancel && f.ctx.Err() != nil {
			t.Fatal("installation Close replaced independent source-service root")
		}
	})
}

func originalParentUnread(t *testing.T, method string, authReturn bool) {
	f := registeredOriginalParent(t)
	path := "/api/users/" + f.open + "/library/items/" + f.item + "/approved-file?approvalId=lfa_" + strings.Repeat("a", 64)
	if authReturn {
		path = "/api/users/" + f.open + "/kids/local-file-approvals"
		f.exec(`UPDATE sessions SET scope=$1 WHERE token=$2`, models.SessionScopeWatchParty, f.token)
	}
	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: synthetic\r\nAuthorization: Bearer %s\r\nX-Client-ID: synthetic-pg-api-client\r\nContent-Length: 100\r\n\r\n{", method, path, f.token)
	originalParentAPITCP(t, f, f.router, req, func(connection net.Conn, actual *originalParentAPIConn, done <-chan struct{}) {
		response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: method})
		if err != nil {
			t.Fatal("actual unread-body denial was not delivered", err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode < 400 || bytes.Contains(body, []byte(parentLocalSyntheticBytes)) {
			t.Fatal("unread body lost complete denial framing", err, response.StatusCode)
		}
		originalParentAPIWait(t, done)
		if !actual.interrupted.Load() || actual.reading.Load() != 0 {
			t.Fatal("original raw body Close/read not physically joined before handler end")
		}
		if authReturn && response.StatusCode != 403 {
			t.Fatal("genuine scoped auth denial not reached", response.StatusCode)
		}
	})
}

func TestRegisteredParentFileOriginalInstallation(t *testing.T) {
	t.Run("actual-registered-full-parent-challenge", func(t *testing.T) { f := registeredOriginalParent(t); _ = f.challenge(t) })
	t.Run("actual-registered-fresh-parent-confirmation", func(t *testing.T) {
		f := registeredOriginalParent(t)
		path := f.approveWire(t)
		if !strings.Contains(path, "/approved-file?") || strings.Contains(path, "/video/") {
			t.Fatal("actual approval promoted generic video")
		}
	})
	t.Run("actual-registered-approved-raw", func(t *testing.T) {
		f := registeredOriginalParent(t)
		path := f.approve(t)
		w := f.call(f.request("GET", path, "", "", ""))
		if w.Code != 200 || w.Body.String() != parentLocalSyntheticBytes {
			t.Fatal("registered actual original bytes not delivered", w.Code)
		}
	})
	t.Run("actual-registered-approved-hls", func(t *testing.T) {
		f := registeredOriginalParent(t)
		f.mediaInput(t)
		raw := f.approve(t)
		approval := strings.Split(raw, "approvalId=")[1]
		options := handlers.ParentFileHLSOptions{Directory: t.TempDir(), FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe"}
		if err := f.files.ConfigureHLS(f.ctx, options, func(ctx context.Context) (handlers.ParentFileHLSOptions, error) { return options, ctx.Err() }); err != nil {
			t.Fatal(err)
		}
		base := "/api/users/" + f.open + "/library/items/" + f.item + "/approved-hls"
		response, body := f.wire(t, f.request("POST", base, fmt.Sprintf(`{"approvalId":%q}`, approval), "", ""))
		var out struct {
			URL string `json:"playlistUrl"`
		}
		if response.StatusCode != 200 || json.Unmarshal(body, &out) != nil || out.URL == "" || originalParentAPIFDs(t, f.path) == 0 {
			t.Fatal("actual registered parent HLS original source/publication unavailable", response.StatusCode)
		}
		playlist := f.call(f.request("GET", out.URL, "", "", ""))
		if playlist.Code != 200 || !strings.HasPrefix(playlist.Body.String(), "#EXTM3U") {
			t.Fatal("original HLS asset absent", playlist.Code)
		}
	})
	t.Run("early-entry-before-account-auth-and-current-body", func(t *testing.T) {
		// Real valid PG scoped session makes AccountAuth return without reading
		// the original POST body. Entry must own its actual Close before Flush.
		originalParentUnread(t, "POST", true)
	})
	t.Run("missing-selected-installation-unavailable", func(t *testing.T) {
		f := registeredOriginalParent(t)
		f.register(t, nil, f.usersHandler, f.pins, "")
		w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", `{`, "", ""))
		if w.Code != 503 {
			t.Fatal("selected nil installation chose fallback", w.Code)
		}
	})
	t.Run("foreign-users-guard-denied", func(t *testing.T) {
		f := registeredOriginalParent(t)
		foreign := handlers.NewUsersHandler(f.users, f.accounts)
		foreign.SetPGProfilePins(f.pins)
		f.register(t, f.installed, foreign, f.pins, "")
		w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", `{}`, "", ""))
		if w.Code != 503 {
			t.Fatal("new distinct parent guard became original", w.Code)
		}
	})
	t.Run("foreign-pins-or-store-denied", func(t *testing.T) {
		f := registeredOriginalParent(t)
		f.register(t, f.installed, f.usersHandler, profileunlock.NewPGProfilePins(f.ds, f.sessions), "")
		w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", `{}`, "", ""))
		if w.Code != 503 {
			t.Fatal("foreign concrete pins selected original source", w.Code)
		}
	})
	t.Run("closed-selected-installation-unavailable", func(t *testing.T) {
		f := registeredOriginalParent(t)
		f.installed.Close()
		w := f.call(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", `{}`, "", ""))
		if w.Code != 503 {
			t.Fatal("closed actual installation chose legacy path", w.Code)
		}
	})
	t.Run("actual-basepath-prefix-and-stripped", func(t *testing.T) {
		f := registeredOriginalParent(t)
		f.register(t, f.installed, f.usersHandler, f.pins, "/mediastorm")
		server := httptest.NewServer(utils.BasePathHandler("/mediastorm", f.router))
		t.Cleanup(server.Close)
		for _, prefix := range []string{"/mediastorm", ""} {
			request, err := http.NewRequest("POST", server.URL+prefix+"/api/users/"+f.open+"/kids/local-file-approvals", strings.NewReader(fmt.Sprintf(`{"itemId":%q}`, f.item)))
			if err != nil {
				t.Fatal(err)
			}
			request.Header = f.request("POST", "/", "", "", "").Header.Clone()
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != 403 || !bytes.Contains(body, []byte(`"parentIntent"`)) {
				t.Fatal("actual configured BasePath original entry missing", response.StatusCode, readErr)
			}
		}
	})
	t.Run("ordinary-api-and-long-stream-unaffected", func(t *testing.T) {
		f := registeredOriginalParent(t)
		for _, method := range []string{"HEAD", "OPTIONS"} {
			r := f.request(method, "/api/library/items/unknown/raw-jellyfin", "", "", "")
			w := f.call(r)
			want := 405
			if method == "OPTIONS" {
				want = 200
			}
			if w.Code != want {
				t.Fatal("actual Jellyfin method compatibility changed", method, w.Code)
			}
		}
		// Two full healthy responses on one real connection prove a noBody GET
		// is not force-retired by the unread-body cleanup branch.
		path := f.approve(t)
		server := httptest.NewServer(f.router)
		t.Cleanup(server.Close)
		transport := &http.Transport{MaxConnsPerHost: 1}
		t.Cleanup(transport.CloseIdleConnections)
		client := &http.Client{Transport: transport}
		var first net.Conn
		for n := 0; n < 2; n++ {
			request, err := http.NewRequest("GET", server.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header = f.request("GET", path, "", "", "").Header.Clone()
			reused := false
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
				if first == nil {
					first = info.Conn
				} else {
					reused = info.Reused && info.Conn == first
				}
			}}
			request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != 200 || string(body) != parentLocalSyntheticBytes || response.Close || n == 1 && !reused {
				t.Fatal("healthy original noBody response was truncated or forced closed", readErr)
			}
		}
	})
	t.Run("legacy-isolated-registration-preserved", func(t *testing.T) {
		f := localParentPG(t)
		_, path := f.approve(t)
		w := f.serve(f.request("GET", path, "", "", ""))
		if w.Code != 200 || w.Body.String() != parentLocalSyntheticBytes {
			t.Fatal("isolated legacy parent registration changed")
		}
	})
	t.Run("no-full-or-share-session-promotion", func(t *testing.T) {
		f := registeredOriginalParent(t)
		path := f.approve(t)
		f.exec(`UPDATE sessions SET scope=$1,scope_resource=$2 WHERE token=$3`, models.SessionScopeStream, "synthetic-unrelated", f.token)
		w := f.call(f.request("GET", path, "", "", ""))
		if w.Code == 200 || w.Code == 206 || w.Body.String() == parentLocalSyntheticBytes {
			t.Fatal("stream scoped token became parent grant")
		}
	})
	t.Run("no-unknown-child-catalogue-promotion", func(t *testing.T) {
		f := registeredOriginalParent(t)
		w := f.call(f.request("GET", "/api/library/items/"+f.item+"/raw-local?profileId="+f.open, "", "", ""))
		if w.Code == 200 || w.Code == 206 || w.Body.String() == parentLocalSyntheticBytes {
			t.Fatal("unclassified child entered ordinary full source")
		}
	})
	t.Run("no-generic-video-url-fallback", func(t *testing.T) {
		f := registeredOriginalParent(t)
		path := f.approve(t)
		if strings.Contains(path, "/video/stream") || strings.Contains(path, "path=") {
			t.Fatal("parent exception published generic source locator")
		}
		w := f.call(f.request("GET", path+"&path=local:synthetic", "", "", ""))
		if w.Code == 200 || w.Body.String() == parentLocalSyntheticBytes {
			t.Fatal("extra source selector escaped exact approval")
		}
	})
	t.Run("actual-router-runtime-cancel-held-tcp-body", func(t *testing.T) {
		f := registeredOriginalParent(t)
		request := fmt.Sprintf("POST /api/users/%s/kids/local-file-approvals HTTP/1.1\r\nHost: synthetic\r\nAuthorization: Bearer %s\r\nX-Client-ID: synthetic-pg-api-client\r\nContent-Length: 200\r\n\r\n{", f.open, f.token)
		originalParentAPITCP(t, f, f.router, request, func(_ net.Conn, actual *originalParentAPIConn, done <-chan struct{}) {
			originalParentAPIUntil(t, func() bool { return actual.reading.Load() > 0 })
			f.cancel()
			originalParentAPIWait(t, done)
			if actual.reading.Load() != 0 || !actual.interrupted.Load() {
				t.Fatal("registered actual read outlived original runtime")
			}
		})
	})
	t.Run("actual-router-runtime-close-held-write", func(t *testing.T) { originalParentHeldRegisteredWrite(t, true) })
	t.Run("actual-router-close-held-source-physical-join", func(t *testing.T) {
		// Both authenticated GET and HEAD include unread original transport
		// bodies. They must fail before source open and finish actual Close,
		// rather than leaving net/http to drain after the owner refunded.
		originalParentUnread(t, "GET", false)
		originalParentUnread(t, "HEAD", false)
		originalParentHeldRegisteredWrite(t, false)
	})
}
