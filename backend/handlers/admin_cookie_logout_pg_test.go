package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
)

func cookieLogoutRequest(f *publicPinCarrierFixture, ctx context.Context, method, path, body string) *http.Request {
	r := httptest.NewRequestWithContext(ctx, method, "http://synthetic.test"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Client-ID", "synthetic-client")
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
	return r
}

func cookieLogoutCount(ctx context.Context, f *publicPinCarrierFixture, token string) (int, error) {
	var count int
	err := f.store.Pool().QueryRow(ctx, "SELECT count(*) FROM sessions WHERE token=$1", token).Scan(&count)
	return count, err
}

func cookieLogoutRestore(t *testing.T, f *publicPinCarrierFixture, from, to string) {
	t.Helper()
	var schema string
	if err := f.store.Pool().QueryRow(t.Context(), "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := f.store.Pool().Exec(ctx, "ALTER TABLE "+pgx.Identifier{schema, from}.Sanitize()+" RENAME TO "+pgx.Identifier{to}.Sanitize()); err != nil {
			t.Error("restore own cookie fixture table", err)
		}
	})
}

func cookieLogoutNoSuccess(t *testing.T, status int, header http.Header, body []byte, want int) {
	t.Helper()
	if status != want || header.Get("Location") != "" || len(header.Values("Set-Cookie")) != 0 || bytes.Contains(body, []byte(`"status":"logged out"`)) || bytes.Contains(body, []byte(`"token"`)) || bytes.Contains(body, []byte(`"count"`)) {
		t.Fatalf("unconfirmed cookie logout emitted success headers/body: status=%d body=%s", status, body)
	}
}

func cookieLogoutConfirmed(t *testing.T, status int, header http.Header) {
	t.Helper()
	cleared := false
	for _, c := range (&http.Response{Header: header}).Cookies() {
		if c.Name == adminSessionCookieName && c.Value == "" && c.MaxAge < 0 && c.Path == "/" && c.HttpOnly {
			cleared = true
		}
	}
	if status != 303 || header.Get("Location") != "/admin/login" || !cleared {
		t.Fatal("confirmed cookie logout lost its original redirect/clear contract", status)
	}
}

func TestCookieLogoutPGInitialOutcomeClassification(t *testing.T) {
	for _, kind := range []string{"missing-cookie", "invalid-cookie", "expired-cookie", "unavailable", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			h := f.admin()
			if kind == "expired-cookie" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unavailable" {
				cookieLogoutRestore(t, f, "unavailable_cookie_sessions", "sessions")
				if _, err := f.store.Pool().Exec(t.Context(), "ALTER TABLE sessions RENAME TO unavailable_cookie_sessions"); err != nil {
					t.Fatal(err)
				}
			}
			for _, entry := range []string{"admin", "account"} {
				for _, method := range []string{"GET", "POST"} {
					ctx, cancel := context.WithCancel(t.Context())
					if kind == "canceled" {
						cancel()
					}
					r := cookieLogoutRequest(f, ctx, method, "/"+entry+"/logout", "")
					if kind == "missing-cookie" {
						r.Header.Del("Cookie")
					}
					if kind == "invalid-cookie" {
						r.Header.Set("Cookie", adminSessionCookieName+"=synthetic-invalid-token")
					}
					w := httptest.NewRecorder()
					if entry == "account" {
						h.RequireLogoutAuth(h.Logout)(w, r)
					} else {
						h.Logout(w, r)
					}
					cancel()
					switch kind {
					case "unavailable", "canceled":
						cookieLogoutNoSuccess(t, w.Code, w.Header(), w.Body.Bytes(), 503)
					case "missing-cookie":
						if w.Code != 303 || w.Header().Get("Location") != "/admin/login" {
							t.Fatal("missing-cookie redirect changed", entry, w.Code)
						}
					default:
						if entry == "admin" {
							cookieLogoutNoSuccess(t, w.Code, w.Header(), w.Body.Bytes(), 401)
						} else if w.Code != 303 || w.Header().Get("Location") != "/admin/login" || len(w.Header().Values("Set-Cookie")) != 0 {
							t.Fatal("initial invalid account-cookie compatibility changed", w.Code)
						}
					}
				}
			}
			if kind == "canceled" {
				if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != 1 {
					t.Fatal("canceled entry touched original row", err)
				}
			}
		})
	}
}

// One real TCP connection per call: handler/frame cleanup and actual connection
// closure both finish before a result is delivered. Redirects remain disabled.
type cookieLogoutReply struct {
	status int
	header http.Header
	body   []byte
	err    error
}
type cookieLogoutLoopback struct {
	server                        *httptest.Server
	handlerDone, connectionClosed chan struct{}
	once                          sync.Once
}

func newCookieLogoutLoopback(t *testing.T, ctx context.Context, handler http.Handler, wrap func(http.ResponseWriter) *cookieLogoutPublicationWriter) *cookieLogoutLoopback {
	t.Helper()
	s := &cookieLogoutLoopback{handlerDone: make(chan struct{}), connectionClosed: make(chan struct{})}
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(s.handlerDone)
		if wrap != nil {
			w = wrap(w)
		}
		handler.ServeHTTP(w, r)
	}))
	s.server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	s.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			s.once.Do(func() { close(s.connectionClosed) })
		}
	}
	s.server.Start()
	t.Cleanup(s.server.Close)
	return s
}
func (s *cookieLogoutLoopback) call(ctx context.Context, method, path, token, body string) (out cookieLogoutReply) {
	r, err := http.NewRequestWithContext(ctx, method, s.server.URL+path, strings.NewReader(body))
	if err != nil {
		out.err = err
		return
	}
	r.Header.Set("X-Client-ID", "synthetic-client")
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
	}
	tr := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	response, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(r)
	if err != nil {
		out.err = err
		return
	}
	out.status = response.StatusCode
	out.header = response.Header.Clone()
	out.body, err = io.ReadAll(io.LimitReader(response.Body, 64<<10))
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil {
		out.err = fmt.Errorf("actual response read/close: %v %v", err, closeErr)
		return
	}
	for _, done := range []chan struct{}{s.handlerDone, s.connectionClosed} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			out.err = errors.New("actual cookie HTTP/frame/connection cleanup not finished")
			return
		}
	}
	return
}

type cookieLogoutPublicationWriter struct {
	http.ResponseWriter
	f                *publicPinCarrierFixture
	entered, proceed chan struct{}
	once             sync.Once
	deadline         func(time.Time)
	physicalHeaders  *atomic.Int64
}

func (w *cookieLogoutPublicationWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *cookieLogoutPublicationWriter) SetReadDeadline(at time.Time) error {
	if w.deadline != nil {
		w.deadline(at)
	}
	return http.NewResponseController(w.ResponseWriter).SetReadDeadline(at)
}
func (w *cookieLogoutPublicationWriter) Header() http.Header {
	if w.physicalHeaders != nil {
		w.physicalHeaders.Add(1)
	}
	header := w.ResponseWriter.Header()
	if w.entered != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if n, err := cookieLogoutCount(ctx, w.f, w.f.session.Token); err == nil && n == 0 {
			w.once.Do(func() { close(w.entered); <-w.proceed })
		}
	}
	return header
}

func TestCookieLogoutPGPreservesScopeOwnershipAndOriginalTTL(t *testing.T) {
	for _, kind := range []string{"scoped", "foreign-query", "foreign-body", "malformed-body", "original-deadline", "onboarding-exempt", "already-guarded"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			h := f.admin()
			path, body := "/account/logout", "{}"
			method := http.MethodPost
			want := 303
			switch kind {
			case "scoped":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET scope='jellyfin',scope_resource=$1 WHERE token=$2", f.profile.ID, f.session.Token); err != nil {
					t.Fatal(err)
				}
				want = 403
			case "foreign-query", "foreign-body":
				foreign, err := f.users.CreateForAccount("other", "Synthetic foreign")
				if err != nil {
					t.Fatal(err)
				}
				if kind == "foreign-query" {
					path += "?profileId=" + url.QueryEscape(foreign.ID)
				} else {
					body = `{"profileId":"` + foreign.ID + `"}`
				}
				want = 404
			case "malformed-body":
				body = `{"profileId":`
				want = 400
			case "onboarding-exempt":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true WHERE id='actor'"); err != nil {
					t.Fatal(err)
				}
				path, method = "/admin/logout", http.MethodGet
				h.settingsPath = filepath.Join(t.TempDir(), "settings.json")
				settings := config.DefaultSettings()
				settings.UI.OnboardingCompleted, settings.UI.OnboardingSkipped = false, false
				settings.Streaming.DebridProviders = nil
				settings.Usenet, settings.TorrentScrapers, settings.Indexers = nil, nil, nil
				settings.Metadata.TMDBAPIKey = ""
				if err := config.NewManager(h.settingsPath).Save(settings); err != nil {
					t.Fatal(err)
				}
				status, err := h.buildOnboardingStatus()
				if err != nil || !status.NeedsOnboarding {
					t.Fatal("genuine onboarding-needed control unavailable", err)
				}
				control := httptest.NewRecorder()
				called := false
				h.RequireAuth(func(http.ResponseWriter, *http.Request) { called = true })(control, cookieLogoutRequest(f, t.Context(), http.MethodGet, "/admin/settings", ""))
				if called || control.Code != http.StatusSeeOther || control.Header().Get("Location") != "/admin/onboarding" {
					t.Fatal("ordinary genuine master GET did not require onboarding", control.Code)
				}
				if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != 1 {
					t.Fatal("onboarding control changed original session", err)
				}
			}
			if kind == "already-guarded" {
				original, release := f.request(t, "POST", "/api/auth/refresh")
				defer release()
				var successor string
				w := httptest.NewRecorder()
				r := cookieLogoutRequest(f, t.Context(), "GET", path, "")
				h.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
					next, err := f.sessions.RefreshProfilePinSession(original)
					if err != nil {
						t.Error(err)
						return
					}
					successor = next.Token
					h.Logout(w, r)
				})(w, r)
				cookieLogoutConfirmed(t, w.Code, w.Header())
				if n, err := cookieLogoutCount(t.Context(), f, successor); err != nil || n != 0 {
					t.Fatal("guarded A recapture lost B", err)
				}
				return
			}
			if kind == "original-deadline" {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				var observed time.Time
				var actualDeadline atomic.Bool
				handler := h.RequireLogoutAuth(func(w http.ResponseWriter, r *http.Request) {
					original, bound := auth.GetSession(r)
					if !bound || original.Token != f.session.Token || f.pins.CheckOriginal(r) != nil {
						http.Error(w, "genuine original frame unavailable", 500)
						return
					}
					deadline, ok := r.Context().Deadline()
					observed = deadline
					if !ok || deadline.After(f.session.ExpiresAt) {
						http.Error(w, "original deadline lost", 500)
						return
					}
					data, err := io.ReadAll(r.Body)
					if err != nil || string(data) != body {
						http.Error(w, "body changed", 500)
						return
					}
					w.WriteHeader(204)
				})
				server := newCookieLogoutLoopback(t, ctx, http.HandlerFunc(handler), func(w http.ResponseWriter) *cookieLogoutPublicationWriter {
					return &cookieLogoutPublicationWriter{ResponseWriter: w, deadline: func(at time.Time) {
						if !at.IsZero() {
							actualDeadline.Store(true)
						}
					}}
				})
				out := server.call(t.Context(), "POST", path, f.session.Token, body)
				if out.err != nil || out.status != 204 || !actualDeadline.Load() || observed.IsZero() || time.Until(observed) > 2*time.Second {
					t.Fatal("actual original socket deadline/body/cleanup changed", out.err, out.status)
				}
				if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != 1 {
					t.Fatal("guard-only test revoked session", err)
				}
				return
			}
			r := cookieLogoutRequest(f, t.Context(), method, path, body)
			w := httptest.NewRecorder()
			if kind == "onboarding-exempt" {
				h.Logout(w, r)
			} else {
				h.RequireLogoutAuth(h.Logout)(w, r)
			}
			if want == 303 {
				cookieLogoutConfirmed(t, w.Code, w.Header())
			} else {
				cookieLogoutNoSuccess(t, w.Code, w.Header(), w.Body.Bytes(), want)
				if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != 1 {
					t.Fatal("guard denial revoked original session", err)
				}
			}
		})
	}
}

// The proxy owns both directions and joins every accepted connection. It
// observes the server's actual COMMIT command tag before discarding that ACK.
type cookieLogoutProxy struct {
	listener net.Listener
	upstream string
	dir      string
	done     chan struct{}
	closed   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	stopped  bool
	conns    map[net.Conn]struct{}
	workers  sync.WaitGroup
	armed    atomic.Bool
	dropped  atomic.Bool
}

func newCookieLogoutProxy(t *testing.T) *cookieLogoutProxy {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL cookie fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") {
		t.Fatal("own synthetic proxy fixture required")
	}
	tmp := os.Getenv("TMPDIR")
	if !strings.HasPrefix(tmp, "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("persistent private fixture temp directory required")
	}
	dir, err := os.MkdirTemp(tmp, "clgo-")
	if err != nil {
		t.Fatal(err)
	}
	port := u.Query().Get("port")
	if port == "" {
		port = "5432"
	}
	p := &cookieLogoutProxy{dir: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port), done: make(chan struct{}), closed: make(chan struct{}), conns: map[net.Conn]struct{}{}}
	t.Cleanup(p.close)
	p.listener, err = net.Listen("unix", filepath.Join(dir, ".s.PGSQL."+port))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(p.done)
		for {
			client, err := p.listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.stopped {
				p.mu.Unlock()
				_ = client.Close()
				return
			}
			p.conns[client] = struct{}{}
			p.workers.Add(1)
			p.mu.Unlock()
			go p.forward(client)
		}
	}()
	q := u.Query()
	q.Set("host", dir)
	u.RawQuery = q.Encode()
	t.Setenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL", u.String())
	return p
}

func (p *cookieLogoutProxy) forward(client net.Conn) {
	defer p.workers.Done()
	upstream, err := net.Dial("unix", p.upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()
	defer func() {
		_ = client.Close()
		_ = upstream.Close()
		p.mu.Lock()
		delete(p.conns, client)
		delete(p.conns, upstream)
		p.mu.Unlock()
	}()
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
		_ = client.Close()
	}()
	defer func() { _ = client.Close(); _ = upstream.Close(); <-inputDone }()
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(upstream, header); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[1:])
		if n < 4 || n > 4<<20 {
			return
		}
		body := make([]byte, n-4)
		if _, err := io.ReadFull(upstream, body); err != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && p.armed.CompareAndSwap(true, false) {
			p.dropped.Store(true)
			return
		}
		packet := append(header, body...)
		for len(packet) != 0 {
			n, err := client.Write(packet)
			if err != nil || n == 0 {
				return
			}
			packet = packet[n:]
		}
	}
}

func (p *cookieLogoutProxy) close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.stopped = true
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		if p.listener != nil {
			_ = p.listener.Close()
			<-p.done
		}
		p.workers.Wait()
		_ = os.RemoveAll(p.dir)
		close(p.closed)
	})
	<-p.closed
}

func TestCookieLogoutPGMutationOutcomesHaveNoSuccessHeaders(t *testing.T) {
	for _, kind := range []string{"actor-missing", "actor-changed", "actor-unavailable", "unknown-commit"} {
		t.Run(kind, func(t *testing.T) {
			var proxy *cookieLogoutProxy
			if kind == "unknown-commit" {
				proxy = newCookieLogoutProxy(t)
			}
			f := newPublicPinCarrierFixture(t)
			h := f.admin()
			_, release := f.request(t, "POST", "/account/logout")
			defer release()
			if kind == "actor-unavailable" {
				cookieLogoutRestore(t, f, "unavailable_cookie_accounts", "accounts")
			}
			r := cookieLogoutRequest(f, t.Context(), "POST", "/account/logout", "{}")
			w := httptest.NewRecorder()
			reached := false
			h.RequireLogoutAuth(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				query := "UPDATE accounts SET username=username WHERE id='actor'"
				switch kind {
				case "actor-missing":
					query = "DELETE FROM accounts WHERE id='actor'"
				case "actor-unavailable":
					query = "ALTER TABLE accounts RENAME TO unavailable_cookie_accounts"
				case "unknown-commit":
					proxy.armed.Store(true)
					query = ""
				}
				if query != "" {
					if _, err := f.store.Pool().Exec(t.Context(), query); err != nil {
						t.Error(err)
						http.Error(w, "fixture unavailable", 500)
						return
					}
				}
				h.Logout(w, r)
			})(w, r)
			want := 401
			if kind == "actor-unavailable" || kind == "unknown-commit" {
				want = 503
			}
			cookieLogoutNoSuccess(t, w.Code, w.Header(), w.Body.Bytes(), want)
			if !reached {
				t.Fatal("case did not reach genuine guarded original mutation")
			}
			if kind == "unknown-commit" {
				if !proxy.dropped.Load() {
					t.Fatal("fixture never observed actual COMMIT tag")
				}
				if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != 0 {
					t.Fatal("unknown ACK was misrepresented as rollback", err)
				}
			}
		})
	}
}

func TestCookieLogoutPGActualCleanupPrecedesCookiePublication(t *testing.T) {
	for _, kind := range []string{"blocked-query-cancel", "late-cancel", "late-deadline"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			h := f.admin()
			ctx, cancel := context.WithCancel(t.Context())
			if kind == "late-deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
			}
			defer cancel()
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			var holder pgx.Tx
			var relation uint32
			handler := h.RequireLogoutAuth(h.Logout)
			if kind == "blocked-query-cancel" {
				// The holder is created before the request but acquires its accounts lock
				// only after the genuine original cookie capture and ordinary guard.
				var err error
				holder, err = f.store.Pool().Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_ = holder.Rollback(cleanup)
				})
				if err := holder.QueryRow(t.Context(), "SELECT 'accounts'::regclass::oid").Scan(&relation); err != nil {
					t.Fatal(err)
				}
				handler = h.RequireLogoutAuth(func(w http.ResponseWriter, r *http.Request) {
					if _, err := holder.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
						http.Error(w, "fixture lock unavailable", 500)
						return
					}
					close(entered)
					h.Logout(w, r)
				})
			}
			var physicalHeaders atomic.Int64
			wrap := func(w http.ResponseWriter) *cookieLogoutPublicationWriter {
				writer := &cookieLogoutPublicationWriter{ResponseWriter: w, f: f, physicalHeaders: &physicalHeaders}
				if kind != "blocked-query-cancel" {
					writer.entered = entered
					writer.proceed = proceed
				}
				return writer
			}
			server := newCookieLogoutLoopback(t, ctx, http.HandlerFunc(handler), wrap)
			t.Cleanup(unblock)
			done := make(chan cookieLogoutReply, 1)
			go func() { done <- server.call(t.Context(), "POST", "/account/logout", f.session.Token, "{}") }()
			select {
			case <-entered:
			case out := <-done:
				t.Fatal("actual guarded query/publication never held", out.err, out.status)
			case <-time.After(5 * time.Second):
				t.Fatal("actual query/publication hold not reached")
			}
			pending := func() int {
				var n int
				if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			if kind == "blocked-query-cancel" {
				until := time.Now().Add(time.Second)
				for pending() == 0 {
					if time.Now().After(until) {
						t.Fatal("original core query did not actually block")
					}
					time.Sleep(time.Millisecond)
				}
			}
			if kind != "late-deadline" {
				cancel()
			}
			select {
			case <-ctx.Done():
			case <-time.After(4 * time.Second):
				t.Fatal("original lifetime did not expire")
			}
			unblock()
			select {
			case out := <-done:
				if out.err != nil {
					t.Fatal(out.err)
				}
				cookieLogoutNoSuccess(t, out.status, out.header, out.body, 503)
			case <-time.After(5 * time.Second):
				t.Fatal("actual cookie request/frame cleanup not joined")
			}
			if physicalHeaders.Load() != 1 {
				t.Fatal("PG cookie publication reacquired physical Header after its final context check", physicalHeaders.Load())
			}
			want := 0
			if kind == "blocked-query-cancel" {
				want = 1
				if pending() != 0 {
					t.Fatal("canceled backend SQL survived actual frame return")
				}
			}
			if n, err := cookieLogoutCount(t.Context(), f, f.session.Token); err != nil || n != want {
				t.Fatal("commit effect/cancellation was misrepresented", err)
			}
		})
	}
}
