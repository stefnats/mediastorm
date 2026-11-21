//go:build linux

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/services/accounts"
)

type shareCookieFixture struct {
	fixture *progressHTTPFixture
	router  *mux.Router
	handler *ShareHandler
	admin   *AdminUIHandler
}

func newShareCookieFixture(t *testing.T, cachedDenial, absent bool, lifetimes ...context.Context) *shareCookieFixture {
	t.Helper()
	f := progressHTTPPG(t, false)
	base := f.base.pin
	// The already-loaded profile cache intentionally predates this grant.
	if _, err := base.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, base.profile.ID); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-share-cookie")
	actualAccounts, err := accounts.NewServiceWithStore(base.store)
	if err != nil {
		t.Fatal(err)
	}
	var profiles ShareProfileService = base.users
	if cachedDenial {
		profiles = fakeShareProfiles{deny: true, notOwned: true, notFound: true}
	}
	h := NewShareHandler(NewShareStore(base.store.ShareLinks()), base.sessions, profiles, "")
	lifetime := t.Context()
	if len(lifetimes) != 0 {
		lifetime = lifetimes[0]
	}
	creator, err := datastore.NewShareLinkCreator(lifetime, base.store, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if absent {
		creator = nil
	}
	h.SetManagedShareCreation(creator)
	admin := NewAdminUIHandler("", "", nil, base.users, nil, nil)
	admin.SetAccountsService(actualAccounts)
	admin.SetSessionsService(base.sessions)
	admin.SetPGProfilePins(base.pins)
	router := mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/api/share/create", admin.RequireShareCreationAuth(h)).Methods("POST")
	}
	return &shareCookieFixture{f, router, h, admin}
}
func (f *shareCookieFixture) call(t *testing.T, prefix, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "http://synthetic.test"+prefix+"/api/share/create", strings.NewReader(body))
	r.Header.Set("Origin", "http://synthetic.test")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.fixture.base.pin.session.Token})
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}
func shareCookieBody(profile string) string {
	b, _ := json.Marshal(map[string]any{"params": map[string]string{"profileId": profile, "sourcePath": "synthetic:share-source"}})
	return string(b)
}
func (f *shareCookieFixture) provenance(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("share status %d: %s", w.Code, w.Body.String())
	}
	var v struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil || v.Token == "" {
		t.Fatal("missing share result", err)
	}
	var version int
	var source, owner, actor string
	err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT provenance_version,source_profile_id,source_account_id,account_id FROM share_links WHERE token=$1`, v.Token).Scan(&version, &source, &owner, &actor)
	if err != nil || version != 1 || source != f.fixture.base.pin.profile.ID || owner != "actor" || actor != "actor" {
		t.Fatal("wrong actual cookie provenance", version, source, owner, actor, err)
	}
}
func TestShareLinkProvenanceCookieCreation(t *testing.T) {
	for _, kind := range []string{"admin-cookie", "account-cookie", "stale-users-cache", "missing-managed-creator", "file-legacy"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "file-legacy" {
				h, _ := newTestShareHandler()
				r := httptest.NewRequest("POST", "/api/share/create", strings.NewReader(shareCookieBody("p1")))
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "legacy-actor"))
				w := httptest.NewRecorder()
				h.Create(w, r)
				if w.Code != 200 {
					t.Fatal("explicit legacy creation rejected", w.Code)
				}
				return
			}
			f := newShareCookieFixture(t, kind == "stale-users-cache", kind == "missing-managed-creator")
			prefix := "/admin"
			if kind == "account-cookie" {
				prefix = "/account"
			}
			w := f.call(t, prefix, shareCookieBody(f.fixture.base.pin.profile.ID))
			if kind == "missing-managed-creator" {
				if w.Code != 503 {
					t.Fatal("unavailable managed creator fell back", w.Code)
				}
				var n int
				if err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 0 {
					t.Fatal("unready managed route wrote legacy row", n, err)
				}
				return
			}
			f.provenance(t, w)
		})
	}
}
func TestShareLinkProvenanceBodyAndSelectors(t *testing.T) {
	for _, kind := range []string{"forged-data-ignored", "body-bounded", "allow-share-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := newShareCookieFixture(t, false, false)
			body := shareCookieBody(f.fixture.base.pin.profile.ID)
			switch kind {
			case "forged-data-ignored":
				b, _ := json.Marshal(map[string]any{"accountId": "forged-actor", "isMaster": true, "source_account_id": "forged-owner", "provenance_version": 99, "params": map[string]string{"profileId": f.fixture.base.pin.profile.ID, "sourcePath": "synthetic:share-source", "accountId": "forged-actor", "source_account_id": "forged-owner"}})
				body = string(b)
			case "body-bounded":
				body = strings.Repeat("x", (1<<20)+1)
			case "allow-share-denied":
				if _, err := f.fixture.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.fixture.base.pin.profile.ID); err != nil {
					t.Fatal(err)
				}
			}
			w := f.call(t, "/admin", body)
			if kind == "forged-data-ignored" {
				f.provenance(t, w)
				return
			}
			want := 400
			if kind == "allow-share-denied" {
				want = 403
			}
			if w.Code != want {
				t.Fatal("unexpected denial status", w.Code, w.Body.String())
			}
			var n int
			if err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 0 {
				t.Fatal("denial persisted share", n, err)
			}
		})
	}
}

// This observes the genuine accepted TCP transport. Read and deadline methods
// always delegate to that socket; no body/auth/SQL result is substituted.
type shareEntryConn struct {
	net.Conn
	reading       atomic.Int32
	writing       atomic.Int32
	writeDemanded atomic.Bool
	writeDeadline atomic.Int64
	demanded      atomic.Bool
}

func (c *shareEntryConn) Read(p []byte) (int, error) {
	c.reading.Add(1)
	defer c.reading.Add(-1)
	return c.Conn.Read(p)
}
func (c *shareEntryConn) Write(p []byte) (int, error) {
	c.writing.Add(1)
	defer c.writing.Add(-1)
	return c.Conn.Write(p)
}
func (c *shareEntryConn) SetWriteDeadline(at time.Time) error {
	if !at.IsZero() {
		c.writeDeadline.Store(at.UnixNano())
	}
	if !at.IsZero() && !at.After(time.Now()) {
		c.writeDemanded.Store(true)
	}
	return c.Conn.SetWriteDeadline(at)
}
func (c *shareEntryConn) SetReadDeadline(at time.Time) error {
	if !at.IsZero() && !at.After(time.Now()) {
		c.demanded.Store(true)
	}
	return c.Conn.SetReadDeadline(at)
}

type shareEntryListener struct {
	net.Listener
	accepted chan *shareEntryConn
}

func (l *shareEntryListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		if e = tcp.SetWriteBuffer(1024); e != nil {
			_ = c.Close()
			return nil, e
		}
	}
	v := &shareEntryConn{Conn: c}
	l.accepted <- v
	return v, nil
}
func shareEntryHeldTCP(t *testing.T, handler http.Handler, headers string, runtimeCancel context.CancelFunc, caller bool) {
	t.Helper()
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		handler.ServeHTTP(w, r.WithContext(ctx))
		once.Do(func() { close(done) })
	}))
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	var cancel context.CancelFunc
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
		runtimeCancel()
		_ = connection.Close()
		server.Close()
	})
	if _, err = fmt.Fprintf(connection, "POST /account/api/share/create HTTP/1.1\r\nHost: synthetic.test\r\nOrigin: http://synthetic.test\r\nSec-Fetch-Site: same-origin\r\nContent-Type: application/json\r\nContent-Length: 200\r\n%sConnection: close\r\n\r\n{", headers); err != nil {
		t.Fatal(err)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no actual accepted socket")
	}
	select {
	case cancel = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no original incoming request")
	}
	until := time.Now().Add(5 * time.Second)
	for actual.reading.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.reading.Load() == 0 {
		t.Fatal("incomplete body did not block original transport")
	}
	select {
	case <-done:
		t.Fatal("incomplete body finished before cancellation")
	default:
	}
	if caller {
		cancel()
	} else {
		runtimeCancel()
	}
	progressHTTPAwait(t, done)
	if !actual.demanded.Load() {
		t.Fatal("original cancellation failed to demand actual read interruption")
	}
	server.Close()
	if actual.reading.Load() != 0 {
		t.Fatal("actual incoming read survived handler/server cleanup")
	}
}
func TestShareLinkProvenanceBodyEntryLifetime(t *testing.T) {
	for _, kind := range []string{"cookie-caller-cancel", "cookie-runtime-cancel", "non-share-unchanged", "legacy-body-guard"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			f := newShareCookieFixture(t, false, false, life)
			if kind == "non-share-unchanged" {
				reached := false
				handler := f.handler.BoundedCreateEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reached = true
					if r.Context().Value(shareCreateEntryKey{}) != nil {
						t.Error("unrelated route acquired share entry")
					}
					w.WriteHeader(204)
				}))
				retire()
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest("POST", "/account/api/other", strings.NewReader("{}")))
				if !reached || w.Code != 204 {
					t.Fatal("closed share runtime changed unrelated route")
				}
				return
			}
			if kind == "legacy-body-guard" {
				// An isolated PG caller can deliberately retain the old constructor. Its
				// upstream current-ownership body check must still run before that handler.
				legacy := NewShareHandler(NewShareStore(f.fixture.base.pin.store.ShareLinks()), f.fixture.base.pin.sessions, fakeShareProfiles{}, "")
				router := mux.NewRouter()
				router.HandleFunc("/account/api/share/create", f.admin.RequireShareCreationAuth(legacy)).Methods("POST")
				body, _ := json.Marshal(map[string]any{"profileId": "synthetic-missing-profile", "params": map[string]string{"profileId": f.fixture.base.pin.profile.ID, "sourcePath": "synthetic:legacy"}})
				oldRouter := f.router
				f.router = router
				w := f.call(t, "/account", string(body))
				f.router = oldRouter
				if w.Code != 404 {
					t.Fatal("legacy PG constructor lost original upstream body denial", w.Code, w.Body.String())
				}
				return
			}

			headers := "Cookie: " + adminSessionCookieName + "=" + f.fixture.base.pin.session.Token + "\r\n"
			shareEntryHeldTCP(t, f.router, headers, retire, kind == "cookie-caller-cancel")
			var n int
			if err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 0 {
				t.Fatal("incomplete body created share", n, err)
			}
		})
	}
}

func TestShareLinkProvenancePublicationLifetime(t *testing.T) {
	for _, kind := range []string{"runtime-cancel", "original-expiry", "source-expiry", "before-output-cancel"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			f := newShareCookieFixture(t, false, false, life)
			if kind == "before-output-cancel" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				r := httptest.NewRequestWithContext(ctx, "POST", "http://synthetic.test/account/api/share/create", strings.NewReader(shareCookieBody(f.fixture.base.pin.profile.ID)))
				r.Header.Set("Origin", "http://synthetic.test")
				r.Header.Set("Sec-Fetch-Site", "same-origin")
				r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.fixture.base.pin.session.Token})
				recorder := httptest.NewRecorder()
				writer := &sharePublicationCancelWriter{ResponseWriter: recorder, cancel: cancel}
				writer.beforeCancel = func() {
					var n int
					if err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 1 {
						t.Error("pre-output signal lacked actual knownACK link", n, err)
					}
				}
				f.router.ServeHTTP(writer, r)
				if !writer.signaled.Load() || recorder.Code != 503 || strings.Contains(recorder.Body.String(), `"token"`) {
					t.Fatal("pre-header retirement implied success or exposed token", recorder.Code)
				}
				var n int
				if err := f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 1 {
					t.Fatal("pre-output cancellation falsely rolled back/retried knownACK row", n, err)
				}
				return
			}
			selected := f.fixture.base.pin.profile.ID
			if kind == "source-expiry" {
				// Keep the actual master actor/session long-lived. Only the explicitly
				// selected foreign source owner expires, independently of original HTTP.
				if _, err := f.fixture.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor';INSERT INTO users(id,account_id,name,allow_share_links) VALUES('synthetic-foreign-output','other','Synthetic foreign output',true)`); err != nil {
					t.Fatal(err)
				}
				selected = "synthetic-foreign-output"
			}
			body, _ := json.Marshal(map[string]any{"params": map[string]string{"profileId": selected, "sourcePath": "synthetic:held-output", "title": strings.Repeat("x", 768*1024)}})
			done := make(chan struct{})
			var entered sync.Once
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if kind == "original-expiry" {
					ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
					defer cancel()
					r = r.WithContext(ctx)
				}
				defer entered.Do(func() { close(done) })
				f.router.ServeHTTP(w, r)
			}))
			listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
			server.Listener = listener
			server.Start()
			connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if err != nil {
				server.Close()
				t.Fatal(err)
			}
			uploadDone := make(chan struct{})
			uploadStarted := false
			t.Cleanup(func() {
				retire()
				_ = connection.Close()
				if uploadStarted {
					progressHTTPAwait(t, uploadDone)
				}
				server.Close()
			})
			tcp, ok := connection.(*net.TCPConn)
			if !ok {
				t.Fatal("actual client TCP socket unavailable")
			}
			if err = tcp.SetReadBuffer(1024); err != nil {
				t.Fatal(err)
			}
			if kind == "source-expiry" {
				at := time.Now().UTC().Add(5 * time.Second).Truncate(time.Microsecond)
				if _, err = f.fixture.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id='other'`, at); err != nil {
					t.Fatal(err)
				}
			}
			written := make(chan error, 1)
			uploadStarted = true
			go func() {
				defer close(uploadDone)
				_, err := fmt.Fprintf(connection, "POST /account/api/share/create HTTP/1.1\r\nHost: synthetic.test\r\nCookie: %s=%s\r\nContent-Type: application/json\r\nOrigin: http://synthetic.test\r\nSec-Fetch-Site: same-origin\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", adminSessionCookieName, f.fixture.base.pin.session.Token, len(body), body)
				written <- err
			}()
			var actual *shareEntryConn
			select {
			case actual = <-listener.accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("no actual accepted output socket")
			}
			serverTCP, ok := actual.Conn.(*net.TCPConn)
			if !ok {
				t.Fatal("actual server TCP socket unavailable")
			}
			if err = serverTCP.SetWriteBuffer(1024); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("original request upload failed to complete")
			}
			// The client deliberately reads no response bytes. The original accepted
			// socket's small genuine send window must hold the actual JSON Write.
			until := time.Now().Add(2 * time.Second)
			for actual.writing.Load() == 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if actual.writing.Load() == 0 {
				t.Fatal("actual JSON socket write was not held")
			}
			select {
			case <-done:
				t.Fatal("held output already returned")
			default:
			}
			var count int
			if err = f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&count); err != nil || count != 1 {
				t.Fatal("held output lacked original knownACK persisted link", count, err)
			}
			if kind == "runtime-cancel" {
				retire()
			}
			progressHTTPAwait(t, done)
			if actual.writing.Load() != 0 {
				t.Fatal("actual output write survived handler end")
			}
			// An elapsed original/source deadline interrupts the genuine transport even
			// when no runtime cancellation is requested; callbacks must also join.
			if kind != "runtime-cancel" && time.Now().UnixNano() < actual.writeDeadline.Load() {
				t.Fatal("actual held write returned before original/source expiry")
			}
			if kind == "runtime-cancel" && !actual.writeDemanded.Load() {
				t.Fatal("runtime retirement did not demand write interruption")
			}
			if err = f.fixture.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&count); err != nil || count != 1 {
				t.Fatal("lost output falsely rolled back/retried committed link", count, err)
			}
			server.Close()
		})
	}
}

// The observer supplies no authorization or synthetic SQL result. Its second
// output-deadline call is the real helper after ACK/physical cleanup; it only
// cancels that original request before any success headers are written.
type sharePublicationCancelWriter struct {
	http.ResponseWriter
	calls        atomic.Int32
	signaled     atomic.Bool
	cancel       context.CancelFunc
	beforeCancel func()
}

func (w *sharePublicationCancelWriter) SetReadDeadline(time.Time) error { return nil }
func (w *sharePublicationCancelWriter) SetWriteDeadline(time.Time) error {
	if w.calls.Add(1) == 2 {
		w.beforeCancel()
		w.signaled.Store(true)
		w.cancel()
	}
	return nil
}
