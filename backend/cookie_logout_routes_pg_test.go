package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/handlers"
	"novastream/services/sessions"
)

type cookieRouteReply struct {
	status int
	header http.Header
	body   []byte
	err    error
}
type cookieRouteServer struct {
	server       *httptest.Server
	done, closed chan struct{}
	once         sync.Once
}

func newCookieRouteServer(t *testing.T, ctx context.Context, router http.Handler, bodyWrap func(*http.Request)) *cookieRouteServer {
	t.Helper()
	s := &cookieRouteServer{done: make(chan struct{}), closed: make(chan struct{})}
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bodyWrap != nil {
			bodyWrap(r)
		}
		defer func() {
			if r.Body != nil {
				_ = r.Body.Close()
			}
			close(s.done)
		}()
		router.ServeHTTP(w, r)
	}))
	s.server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	s.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			s.once.Do(func() { close(s.closed) })
		}
	}
	s.server.Start()
	t.Cleanup(s.server.Close)
	return s
}
func (s *cookieRouteServer) call(ctx context.Context, method, path, token string) (out cookieRouteReply) {
	r, err := http.NewRequestWithContext(ctx, method, s.server.URL+path, strings.NewReader("{}"))
	if err != nil {
		out.err = err
		return
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Client-ID", "synthetic-client")
	r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: token})
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
		out.err = errors.New("actual cookie alias response could not be read/closed")
		return
	}
	for _, done := range []chan struct{}{s.done, s.closed} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			out.err = errors.New("actual alias body/frame/HTTP cleanup not finished")
			return
		}
	}
	return
}
func cookieRouteRouter(t *testing.T, f *publicPinWebFixture) *mux.Router {
	t.Helper()
	router := mux.NewRouter()
	admin := f.admin(t)
	registerAdminLogoutRoute(router, admin)
	account := handlers.NewAccountUIHandler(nil, f.sessions, f.users, nil, nil, nil, nil)
	registerAccountLogoutRoute(router, account, admin, f.pins)
	return router
}
func cookieRouteSuccess(t *testing.T, out cookieRouteReply) {
	t.Helper()
	if out.err != nil {
		t.Fatal(out.err)
	}
	clear := false
	for _, c := range (&http.Response{Header: out.header}).Cookies() {
		if c.Name == "strmr_admin_session" && c.Value == "" && c.MaxAge < 0 && c.Path == "/" && c.HttpOnly {
			clear = true
		}
	}
	if out.status != 303 || out.header.Get("Location") != "/admin/login" || !clear {
		t.Fatal("real registered alias did not confirm original cookie logout", out.status)
	}
}

func TestCookieLogoutRegisteredAliasesActualLoopback(t *testing.T) {
	for _, kind := range []string{"admin-get", "admin-post", "account-get", "account-post", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicPinWebFixture(t)
			router := cookieRouteRouter(t, f)
			if kind == "unavailable" {
				if _, err := f.store.Pool().Exec(t.Context(), "ALTER TABLE sessions RENAME TO unavailable_route_cookie_sessions"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, err := f.store.Pool().Exec(ctx, "ALTER TABLE unavailable_route_cookie_sessions RENAME TO sessions"); err != nil {
						t.Error("restore own alias fixture", err)
					}
				})
				for _, path := range []string{"/admin/logout", "/account/logout"} {
					server := newCookieRouteServer(t, t.Context(), router, nil)
					out := server.call(t.Context(), "POST", path, f.session.Token)
					if out.err != nil || out.status != 503 || out.header.Get("Location") != "" || len(out.header.Values("Set-Cookie")) != 0 || bytes.Contains(out.body, []byte(`"status":"logged out"`)) {
						t.Fatal("actual unavailable alias falsely redirected/cleared", out.err, out.status)
					}
					server.server.Close()
				}
				return
			}
			parts := strings.Split(kind, "-")
			server := newCookieRouteServer(t, t.Context(), router, nil)
			out := server.call(t.Context(), strings.ToUpper(parts[1]), "/"+parts[0]+"/logout", f.session.Token)
			cookieRouteSuccess(t, out)
			var left int
			if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1", f.session.Token).Scan(&left); err != nil || left != 0 {
				t.Fatal("actual alias retained original session", err)
			}
			// Method registration is the real Main helper, not a copied fake route.
			denied := httptest.NewRecorder()
			router.ServeHTTP(denied, httptest.NewRequest("HEAD", "/"+parts[0]+"/logout", nil))
			if denied.Code != 405 {
				t.Fatal("alias method restriction changed", denied.Code)
			}
		})
	}
}

type cookieRouteHeldBody struct {
	io.ReadCloser
	entered, proceed, closed chan struct{}
	readOnce, closeOnce      sync.Once
	ctx                      context.Context
}

func (b *cookieRouteHeldBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() { close(b.entered) })
	select {
	case <-b.proceed:
		return b.ReadCloser.Read(p)
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}
func (b *cookieRouteHeldBody) Close() error {
	err := b.ReadCloser.Close()
	b.closeOnce.Do(func() { close(b.closed) })
	return err
}

func TestCookieLogoutRegisteredAliasesRetainOriginalRefreshLineage(t *testing.T) {
	for _, order := range []string{"refresh-before-logout", "logout-before-refresh"} {
		t.Run(order, func(t *testing.T) {
			for _, path := range []string{"/admin/logout", "/account/logout"} {
				f := newPublicPinWebFixture(t)
				if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
					t.Fatal(err)
				}
				original, release, err := f.pins.Authenticate(httptest.NewRequestWithContext(t.Context(), "POST", "/api/auth/refresh", nil), f.session.Token, "synthetic-client")
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := f.pins.Verify(original, f.profile.ID, "1357", time.Hour, f.users); err != nil {
					release()
					t.Fatal(err)
				}
				fresh, err := f.sessions.Create("actor", false, "synthetic independent login", "127.0.0.1")
				if err != nil {
					release()
					t.Fatal(err)
				}
				router := cookieRouteRouter(t, f)
				entered, proceed, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(proceed) }) }
				server := newCookieRouteServer(t, t.Context(), router, func(r *http.Request) {
					r.Body = &cookieRouteHeldBody{ReadCloser: r.Body, entered: entered, proceed: proceed, closed: closed, ctx: r.Context()}
				})
				t.Cleanup(unblock)
				t.Cleanup(release)
				done := make(chan cookieRouteReply, 1)
				go func() { done <- server.call(t.Context(), "POST", path, f.session.Token) }()
				select {
				case <-entered:
				case value := <-done:
					t.Fatal("real original alias never reached post-capture body", path, value.err, value.status)
				case <-time.After(5 * time.Second):
					t.Fatal("original alias did not capture before body")
				}
				replacement := ""
				if order == "refresh-before-logout" {
					next, err := f.sessions.RefreshProfilePinSession(original)
					if err != nil {
						t.Fatal(err)
					}
					replacement = next.Token
					var count int
					if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1", replacement).Scan(&count); err != nil || count != 1 {
						t.Fatal("actual B missing before original request release", err)
					}
				}
				unblock()
				select {
				case out := <-done:
					cookieRouteSuccess(t, out)
				case <-time.After(5 * time.Second):
					t.Fatal("real alias did not join original request")
				}
				select {
				case <-closed:
				default:
					t.Fatal("actual request body remained open after alias cleanup")
				}
				if order == "logout-before-refresh" {
					if _, err := f.sessions.RefreshProfilePinSession(original); err == nil {
						t.Fatal("logout allowed original lineage to revive")
					}
				}
				for token, want := range map[string]int{f.session.Token: 0, fresh.Token: 1} {
					var count int
					if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1", token).Scan(&count); err != nil || count != want {
						t.Fatal("alias adopted independent C or retained A", err)
					}
				}
				if replacement != "" {
					var count int
					if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1", replacement).Scan(&count); err != nil || count != 0 {
						t.Fatal("A recapture lost actual B", err)
					}
				}
				var grants int
				if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM profile_unlocks").Scan(&grants); err != nil || grants != 0 {
					t.Fatal("alias retained original lineage grants", err)
				}
				server.server.Close()
				release()
			}
		})
	}
}

func TestCookieLogoutLegacyAccountJSONCompatibility(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		service, err := sessions.NewService("", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		session, err := service.Create("synthetic-cookie-json", false, "synthetic cookie legacy", "127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		router := mux.NewRouter()
		account := handlers.NewAccountUIHandler(nil, service, nil, nil, nil, nil, nil)
		// A nil admin also proves the actual JSON branch cannot enter new PG code.
		registerAccountLogoutRoute(router, account, nil, nil)
		server := newCookieRouteServer(t, t.Context(), router, nil)
		out := server.call(t.Context(), method, "/account/logout", session.Token)
		cookieRouteSuccess(t, out)
		if _, err := service.Validate(session.Token); err == nil {
			t.Fatal("JSON compatibility left session active")
		}
		server.server.Close()
	}
}
