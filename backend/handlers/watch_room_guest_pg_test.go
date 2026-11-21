//go:build linux

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
	"novastream/utils"
)

type partyHTTPFixture struct {
	native   *watchRoomHTTPFixture
	pins     *profileunlock.PGProfilePins
	video    *VideoHandler
	router   *mux.Router
	room     *models.WatchRoom
	invite   *models.WatchRoomExternalInvite
	cookie   *http.Cookie
	token    string
	resource string
}

func newPartyHTTP(t *testing.T, life context.Context, provider streaming.Provider, basePath string, short bool) *partyHTTPFixture {
	t.Helper()
	native := newWatchRoomHTTP(t, life, basePath)
	if short {
		if _, e := native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id=$2`, time.Now().UTC().Add(5*time.Second).Truncate(time.Microsecond), native.base.base.pin.session.AccountID); e != nil {
			t.Fatal(e)
		}
	}
	pins := profileunlock.NewPGProfilePins(native.base.base.pin.store, native.base.base.pin.sessions)
	if e := pins.SetManagedWatchPartyPlayback(native.authority); e != nil {
		t.Fatal(e)
	}
	native.party.SetManagedPublicParty(native.authority)
	if provider == nil {
		provider = &shareStaticProvider{data: shareWAV()}
	}
	video := NewVideoHandlerWithProvider(false, "/usr/bin/ffmpeg", "/usr/bin/ffprobe", t.TempDir(), provider)
	if e := video.SetManagedWatchPartyPlayback(native.authority, pins); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(video.Shutdown)
	room := native.create(t)
	invite, e := native.authority.ReplaceExternalInvite(native.request(t, "POST", "/api/users/"+native.profile+"/watch-rooms/"+room.ID+"/external-invite", nil, t.Context()), native.profile, room.ID)
	if e != nil {
		t.Fatal(e)
	}
	f := &partyHTTPFixture{native: native, pins: pins, video: video, room: room, invite: invite, resource: "synthetic:original-HTTP-party.wav"}
	f.router = mux.NewRouter()
	public := func(next http.HandlerFunc) http.Handler { return native.party.BoundedManagedPartyEntry(next) }
	f.router.Handle("/watch-party/code", public(native.party.ResolveCode)).Methods("POST")
	f.router.Handle("/watch-party/code/{code}", public(native.party.OpenCode)).Methods("GET")
	f.router.Handle("/watch-party/code/{code}", public(native.party.JoinCode)).Methods("POST")
	f.router.Handle("/watch-party/room/{roomID}", public(native.party.RoomPage)).Methods("GET")
	f.router.Handle("/watch-party/{token}", public(native.party.OpenToken)).Methods("GET")
	f.router.Handle("/watch-party/{token}", public(native.party.JoinToken)).Methods("POST")
	f.router.Handle("/api/watch-party/room/{roomID}", public(native.party.RoomSnapshot)).Methods("GET")
	f.router.Handle("/api/watch-party/room/{roomID}/ready", public(native.party.Ready)).Methods("POST")
	f.router.Handle("/api/watch-party/room/{roomID}/heartbeat", public(native.party.Heartbeat)).Methods("POST")
	f.router.Handle("/api/watch-party/room/{roomID}/playback-grant", public(native.party.PlaybackGrant)).Methods("POST")
	return f
}
func (f *partyHTTPFixture) call(t *testing.T, method, path, body string, cookie bool) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.test"+path, strings.NewReader(body))
	if cookie && f.cookie != nil {
		r.AddCookie(f.cookie)
	}
	if strings.HasPrefix(path, "/watch-party") {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}
func (f *partyHTTPFixture) join(t *testing.T) {
	t.Helper()
	w := f.call(t, "POST", "/watch-party/"+f.invite.Token, url.Values{"name": {"Synthetic HTTP guest"}}.Encode(), false)
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == watchPartyCookieName {
			f.cookie = cookie
		}
	}
	if w.Code != 303 || f.cookie == nil || !f.cookie.HttpOnly {
		t.Fatal("actual managed public guest cookie", w.Code)
	}
}
func (f *partyHTTPFixture) bind(t *testing.T) {
	t.Helper()
	if _, e := f.native.authority.BindExternalSource(f.native.request(t, "POST", "/api/users/"+f.native.profile+"/watch-rooms/"+f.room.ID+"/external-source", nil, t.Context()), f.native.profile, f.room.ID, f.resource, map[string]string{"profileId": f.native.profile, "titleId": "synthetic-party"}); e != nil {
		t.Fatal(e)
	}
	if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, f.room.ID); e != nil {
		t.Fatal(e)
	}
}
func (f *partyHTTPFixture) grant(t *testing.T) {
	t.Helper()
	w := f.call(t, "POST", "/api/watch-party/room/"+f.room.ID+"/playback-grant", "", true)
	var result struct {
		URL string `json:"url"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("actual managed public source grant", w.Code, w.Body.String())
	}
	parsed, e := url.Parse(result.URL)
	if e != nil || parsed.Query().Get("profileId") != f.native.profile || parsed.Query().Get("token") == "" {
		t.Fatal("actual original grant source wire")
	}
	f.token = parsed.Query().Get("token")
}
func (f *partyHTTPFixture) serveVideo(t *testing.T, w http.ResponseWriter, r *http.Request, next http.Handler) {
	t.Helper()
	token := r.URL.Query().Get("token")
	if token == "" {
		token = f.token
	}
	actual, release, e := f.pins.Authenticate(r, token, "")
	if e != nil {
		writeJSONError(w, "source unavailable", 401)
		return
	}
	defer release()
	f.video.ManagedSharePlaybackEntry(next).ServeHTTP(w, actual)
}
func TestWatchPartyPublicManagedEntry(t *testing.T) {
	for _, kind := range []string{"open-token-current", "open-code-current", "join-cookie-known-ack", "code-form-bounded", "returning-guest-no-renewal", "rotate-keeps-room-guest", "managed-missing-503", "legacy-constructor", "ready-same-original", "heartbeat-same-original"} {
		t.Run(kind, func(t *testing.T) {
			f := newPartyHTTP(t, t.Context(), nil, "", false)
			if kind == "open-token-current" || kind == "open-code-current" {
				path := "/watch-party/" + f.invite.Token
				if kind == "open-code-current" {
					path = "/watch-party/code/" + f.invite.ShortCode
				}
				w := f.call(t, "GET", path, "", false)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "Synthetic HTTP room") {
					t.Fatal("actual original invitation page", w.Code)
				}
				return
			}
			if kind == "code-form-bounded" {
				body := url.Values{"code": {strings.ToLower(f.invite.ShortCode)}}.Encode()
				w := f.call(t, "POST", "/watch-party/code", body, false)
				if w.Code != 303 || !strings.Contains(w.Header().Get("Location"), f.invite.ShortCode) {
					t.Fatal("current original code resolution", w.Code)
				}
				w = f.call(t, "POST", "/watch-party/code", strings.Repeat("x", (32<<10)+1), false)
				if w.Code == 303 || len(w.Result().Cookies()) != 0 {
					t.Fatal("unbounded code form admitted")
				}
				return
			}
			if kind == "managed-missing-503" {
				f.native.party.SetManagedPublicParty(nil)
				w := f.call(t, "POST", "/watch-party/"+f.invite.Token, "name=Synthetic", false)
				if w.Code != 503 || len(w.Result().Cookies()) != 0 {
					t.Fatal("missing original runtime fell back to legacy", w.Code)
				}
				return
			}
			if kind == "legacy-constructor" {
				legacy := NewWatchPartyHandler(f.native.party.service, f.native.base.base.pin.sessions, "")
				if legacy.publicPartyEntry != nil {
					t.Fatal("isolated legacy constructor selected managed runtime")
				}
				r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test/watch-party/"+f.invite.Token, nil), map[string]string{"token": f.invite.Token})
				w := httptest.NewRecorder()
				legacy.OpenToken(w, r)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "Join ") {
					t.Fatal("isolated legacy page changed", w.Code)
				}
				return
			}
			f.join(t)
			original := f.cookie.Value
			var expiry time.Time
			if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT expires_at FROM sessions WHERE token=$1`, original).Scan(&expiry); e != nil {
				t.Fatal(e)
			}
			if kind == "returning-guest-no-renewal" {
				f.joinWithCookie(t)
				if f.cookie.Value != original {
					t.Fatal("returning guest replaced original token")
				}
			}
			if kind == "rotate-keeps-room-guest" {
				if _, e := f.native.authority.ReplaceExternalInvite(f.native.request(t, "POST", "/api/users/"+f.native.profile+"/watch-rooms/"+f.room.ID+"/external-invite", nil, t.Context()), f.native.profile, f.room.ID); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "ready-same-original" {
				w := f.call(t, "POST", "/api/watch-party/room/"+f.room.ID+"/ready", `{"ready":true}`, true)
				if w.Code != 204 {
					t.Fatal("current original ready mutation", w.Code)
				}
				var ready bool
				if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT ready FROM watch_room_guest_members WHERE room_id=$1 AND guest_id=$2`, f.room.ID, watchPartyGuestID(original)).Scan(&ready); e != nil || !ready {
					t.Fatal("actual original member not updated", e)
				}
			}
			if kind == "heartbeat-same-original" {
				w := f.call(t, "POST", "/api/watch-party/room/"+f.room.ID+"/heartbeat?buffering=true", "", true)
				if w.Code != 204 {
					t.Fatal("original guest heartbeat", w.Code)
				}
				var buffering bool
				if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT buffering FROM watch_room_guest_members WHERE room_id=$1 AND guest_id=$2`, f.room.ID, watchPartyGuestID(original)).Scan(&buffering); e != nil || !buffering {
					t.Fatal("actual original heartbeat row", e)
				}
			}
			w := f.call(t, "GET", "/api/watch-party/room/"+f.room.ID, "", true)
			if w.Code != 200 {
				t.Fatal("original joined identity after admission/data mutation", w.Code)
			}
			var after time.Time
			if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT expires_at FROM sessions WHERE token=$1`, original).Scan(&after); e != nil || !after.Equal(expiry) {
				t.Fatal("guest liveness/request renewed fixed expiry", e)
			}
		})
	}
}
func (f *partyHTTPFixture) joinWithCookie(t *testing.T) {
	t.Helper()
	w := f.call(t, "POST", "/watch-party/"+f.invite.Token, url.Values{"name": {"Synthetic returning HTTP guest"}}.Encode(), true)
	if w.Code != 303 {
		t.Fatal("actual rejoin", w.Code)
	}
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == watchPartyCookieName {
			f.cookie = cookie
		}
	}
}

func partyHeldPublicBody(t *testing.T, f *partyHTTPFixture, path string, guest, caller bool, retire context.CancelFunc) {
	t.Helper()
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	var started atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Store(true)
		defer close(done)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		f.router.ServeHTTP(w, r.WithContext(ctx))
	}))
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	var cancel context.CancelFunc
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
		retire()
		_ = conn.Close()
		server.Close()
		if started.Load() {
			progressHTTPAwait(t, done)
		}
	})
	kind, body, cookie := "application/x-www-form-urlencoded", "name=", ""
	if guest {
		kind = "application/json"
		body = `{"ready":`
		cookie = "Cookie: " + watchPartyCookieName + "=" + f.cookie.Value + "\r\n"
	}
	if _, e = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: synthetic.test\r\nContent-Type: %s\r\n%sContent-Length: 200\r\nConnection: close\r\n\r\n%s", path, kind, cookie, body); e != nil {
		t.Fatal(e)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no real public incoming connection")
	}
	select {
	case cancel = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no original public request context")
	}
	until := time.Now().Add(5 * time.Second)
	for actual.reading.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.reading.Load() == 0 {
		t.Fatal("actual original public form/JSON read not held")
	}
	select {
	case <-done:
		t.Fatal("incomplete public request ended before cancellation")
	default:
	}
	if caller {
		cancel()
	} else {
		retire()
	}
	progressHTTPAwait(t, done)
	if !actual.demanded.Load() {
		t.Fatal("public original cancellation did not interrupt actual transport read")
	}
	server.Close()
	if actual.reading.Load() != 0 {
		t.Fatal("public actual read survived entry cleanup")
	}
}

type partyCookieCancelWriter struct {
	http.ResponseWriter
	cancel       context.CancelFunc
	once         atomic.Bool
	beforeCancel func()
}

func (w *partyCookieCancelWriter) Header() http.Header {
	header := w.ResponseWriter.Header()
	if header.Get("Set-Cookie") != "" && w.once.CompareAndSwap(false, true) {
		w.beforeCancel()
		w.cancel()
	}
	return header
}
func (*partyCookieCancelWriter) SetReadDeadline(time.Time) error  { return nil }
func (*partyCookieCancelWriter) SetWriteDeadline(time.Time) error { return nil }
func TestWatchPartyPublicTransportLifetime(t *testing.T) {
	for _, kind := range []string{"incomplete-form-caller-cancel", "incomplete-form-runtime-cancel", "guest-body-before-limiter", "base-path-prefixed-direct", "held-output-caller-cancel", "held-output-runtime-cancel", "known-ack-preheader-cancel", "healthy-tcp-complete-body"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			basePath := ""
			if kind == "base-path-prefixed-direct" {
				basePath = "/mediastorm"
			}
			f := newPartyHTTP(t, life, nil, basePath, false)
			if strings.HasPrefix(kind, "incomplete-form-") {
				partyHeldPublicBody(t, f, "/watch-party/"+f.invite.Token, false, strings.Contains(kind, "caller"), retire)
				return
			}
			if kind == "guest-body-before-limiter" {
				f.join(t)
				partyHeldPublicBody(t, f, "/api/watch-party/room/"+f.room.ID+"/ready", true, true, retire)
				return
			}
			if kind == "base-path-prefixed-direct" {
				actualMain := utils.BasePathHandler(basePath, f.router)
				for _, prefix := range []string{basePath, ""} {
					w := httptest.NewRecorder()
					actualMain.ServeHTTP(w, httptest.NewRequest("GET", "http://synthetic.test"+prefix+"/watch-party/"+f.invite.Token, nil))
					if w.Code != 200 || !strings.Contains(w.Body.String(), basePath+"/watch-party/") {
						t.Fatal("actual prefixed/direct public Main path", w.Code)
					}
				}
				return
			}
			if kind == "known-ack-preheader-cancel" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				r := httptest.NewRequest("POST", "http://synthetic.test/watch-party/"+f.invite.Token, strings.NewReader("name=Synthetic+queued+cookie")).WithContext(ctx)
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				recorder := httptest.NewRecorder()
				observer := &partyCookieCancelWriter{ResponseWriter: recorder, cancel: cancel, beforeCancel: func() {
					var count int
					if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_party_guest_bindings`).Scan(&count); e != nil || count != 1 {
						t.Error("queued actual cookie was not after knownACK/cleanup", count, e)
					}
				}}
				f.router.ServeHTTP(observer, r)
				if !observer.once.Load() || recorder.Code != 503 || recorder.Header().Get("Set-Cookie") != "" || recorder.Header().Get("Location") != "" {
					t.Fatal("preheader cancellation delivered guest cookie or false success", recorder.Code)
				}
				var count int
				if e := f.native.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_party_guest_bindings`).Scan(&count); e != nil || count != 1 {
					t.Fatal("lost publication falsely rolled back/retried join", count, e)
				}
				return
			}
			f.join(t)
			if kind == "healthy-tcp-complete-body" {
				server := httptest.NewServer(f.router)
				t.Cleanup(server.Close)
				transport := &http.Transport{}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
				request, e := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/api/watch-party/room/"+f.room.ID, nil)
				if e != nil {
					t.Fatal(e)
				}
				request.AddCookie(f.cookie)
				response, e := client.Do(request)
				if e != nil {
					t.Fatal(e)
				}
				raw, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				var view watchPartyRoomView
				if response.StatusCode != 200 || readErr != nil || closeErr != nil || json.Unmarshal(raw, &view) != nil || view.ID != f.room.ID {
					t.Fatal("healthy public final HTTP body did not finish", response.StatusCode, readErr, closeErr)
				}
				return
			}
			// Many bounded descriptive members hold a REAL socket JSON write. These rows
			// grant no session/private source authority to those synthetic display names.
			if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `INSERT INTO watch_room_guest_members(room_id,guest_id,name)SELECT $1,'synthetic-output-'||n,repeat('x',120) FROM generate_series(1,200)n`, f.room.ID); e != nil {
				t.Fatal(e)
			}
			done := make(chan struct{})
			ready := make(chan context.CancelFunc, 1)
			var started atomic.Bool
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				started.Store(true)
				defer close(done)
				ctx, cancel := context.WithCancel(r.Context())
				defer cancel()
				ready <- cancel
				f.router.ServeHTTP(w, r.WithContext(ctx))
			}))
			listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
			server.Listener = listener
			server.Start()
			conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if e != nil {
				server.Close()
				t.Fatal(e)
			}
			var cancel context.CancelFunc
			t.Cleanup(func() {
				if cancel != nil {
					cancel()
				}
				retire()
				_ = conn.Close()
				server.Close()
				if started.Load() {
					progressHTTPAwait(t, done)
				}
			})
			tcp, ok := conn.(*net.TCPConn)
			if !ok {
				t.Fatal("actual public client TCP missing")
			}
			if e = tcp.SetReadBuffer(1024); e != nil {
				t.Fatal(e)
			}
			if _, e = fmt.Fprintf(conn, "GET /api/watch-party/room/%s HTTP/1.1\r\nHost: synthetic.test\r\nCookie: %s=%s\r\nConnection: close\r\n\r\n", f.room.ID, watchPartyCookieName, f.cookie.Value); e != nil {
				t.Fatal(e)
			}
			var actual *shareEntryConn
			select {
			case actual = <-listener.accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("no real public output socket")
			}
			select {
			case cancel = <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("no actual public output context")
			}
			until := time.Now().Add(5 * time.Second)
			for actual.writing.Load() == 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if actual.writing.Load() == 0 {
				t.Fatal("actual public JSON output never held")
			}
			closed := make(chan struct{})
			closeStarted := false
			t.Cleanup(func() {
				retire()
				if closeStarted {
					progressHTTPAwait(t, closed)
				}
			})
			if kind == "held-output-caller-cancel" {
				cancel()
			} else {
				closeStarted = true
				go func() { defer close(closed); f.native.authority.Close() }()
			}
			progressHTTPAwait(t, done)
			if closeStarted {
				progressHTTPAwait(t, closed)
			}
			if actual.writing.Load() != 0 || !actual.writeDemanded.Load() {
				t.Fatal("actual public output/callback outlived original entry")
			}
		})
	}
}
