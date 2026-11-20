package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/profileunlock"
)

type registeredLogoutCall struct {
	done chan struct{}
	err  error
}

type registeredLogoutResponse struct {
	status int
	body   []byte
	err    error
}

type registeredLogoutFixture struct {
	*publicPGPinFixture
	token, schema string
	pins          *profileunlock.PGProfilePins
	server        *httptest.Server
	hook          func(*http.Request) error
	wrap          func(http.ResponseWriter, *http.Request) http.ResponseWriter
	requestID     atomic.Uint64
	mu            sync.Mutex
	pending       map[string]*registeredLogoutCall
}

func registeredLogoutToken(label string) string {
	sum := sha256.Sum256([]byte("synthetic-registered-logout-" + label))
	return base64.URLEncoding.EncodeToString(sum[:])
}

func registeredLogoutPG(t *testing.T) *registeredLogoutFixture {
	t.Helper()
	f := &registeredLogoutFixture{publicPGPinFixture: newPublicPGPinFixture(t), pending: map[string]*registeredLogoutCall{}}
	f.pins = profileunlock.NewPGProfilePins(f.ds, f.sessions)
	if err := f.ds.Pool().QueryRow(t.Context(), "SELECT current_schema()").Scan(&f.schema); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.token = registeredLogoutToken("original")
	session := models.Session{Token: f.token, AccountID: f.account, ClientID: "synthetic-pg-api-client", UserAgent: "synthetic logout", IPAddress: "127.0.0.1", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	// Stored after the existing service loaded: this target is genuinely cold
	// in the session cache, but its original PG frame is still authoritative.
	if err := f.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://synthetic.test/api/auth/logout", nil)
	r.Header.Set("X-Client-ID", session.ClientID)
	_, release, err := f.pins.Authenticate(r, f.token, session.ClientID)
	if err != nil {
		t.Fatal("bind genuine original session before arming faults", err)
	}
	release()
	found := 0
	if err := f.router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		path, _ := route.GetPathTemplate()
		if path != "/api/auth/logout" {
			return nil
		}
		methods, _ := route.GetMethods()
		if len(methods) != 1 || methods[0] != http.MethodPost {
			return nil
		}
		original := route.GetHandler()
		if original == nil {
			return errors.New("registered logout handler missing")
		}
		found++
		// This test-only seam is after the real registered middleware captured
		// the private PG frame. It can mutate only this fixture or hold IO; it
		// never replaces the original concrete AuthHandler or authorizes access.
		route.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if f.hook != nil {
				if err := f.pins.CheckOriginal(r); err != nil {
					f.recordHookError(r, err)
					http.Error(w, "original fixture frame unavailable", 500)
					return
				}
				if err := f.hook(r); err != nil {
					f.recordHookError(r, err)
					http.Error(w, "fixture mutation unavailable", 500)
					return
				}
			}
			original.ServeHTTP(w, r)
		}))
		return nil
	}); err != nil || found != 1 {
		t.Fatal("actual registered logout route not found once", err, found)
	}
	return f
}

func (f *registeredLogoutFixture) start(t *testing.T, ctx context.Context) {
	t.Helper()
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		call := f.pending[r.Header.Get("X-Synthetic-Logout-Request")]
		f.mu.Unlock()
		if call == nil {
			http.Error(w, "fixture request not registered", 500)
			return
		}
		defer close(call.done)
		if f.wrap != nil {
			w = f.wrap(w, r)
		}
		f.router.ServeHTTP(w, r)
	}))
	f.server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	f.server.Start()
	t.Cleanup(f.server.Close)
}

func (f *registeredLogoutFixture) recordHookError(r *http.Request, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if call := f.pending[r.Header.Get("X-Synthetic-Logout-Request")]; call != nil {
		call.err = err
	}
}

// Each concurrent HTTP exchange has its own completion signal. A refresh reply
// cannot stand in for completion of the held original logout/frame cleanup.
func (f *registeredLogoutFixture) call(ctx context.Context, method, path, token string) (out registeredLogoutResponse) {
	key := fmt.Sprint(f.requestID.Add(1))
	call := &registeredLogoutCall{done: make(chan struct{})}
	f.mu.Lock()
	f.pending[key] = call
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.pending, key); f.mu.Unlock() }()
	r, err := http.NewRequestWithContext(ctx, method, f.server.URL+path, nil)
	if err != nil {
		out.err = err
		return
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	r.Header.Set("X-Synthetic-Logout-Request", key)
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Do(r)
	if err != nil {
		out.err = fmt.Errorf("actual loopback request: %w", err)
		return
	}
	out.status = response.StatusCode
	out.body, err = io.ReadAll(io.LimitReader(response.Body, 64<<10))
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil {
		out.err = fmt.Errorf("actual loopback response: %v %v", err, closeErr)
		return
	}
	select {
	case <-call.done:
	case <-time.After(5 * time.Second):
		out.err = errors.New("actual registered handler/frame cleanup not finished")
		return
	}
	f.mu.Lock()
	hookErr := call.err
	f.mu.Unlock()
	if hookErr != nil {
		out.err = fmt.Errorf("genuine original fixture failed: %w", hookErr)
		return
	}
	if method == http.MethodPost && path == "/api/auth/logout" {
		validator, err := openapi.NewValidator(ctx)
		if err != nil {
			out.err = err
			return
		}
		if err := validator.ValidateRequest(r, nil); err != nil {
			out.err = fmt.Errorf("actual logout request contract: %w", err)
			return
		}
		if err := validator.ValidateResponse(r, out.status, response.Header, out.body); err != nil {
			out.err = fmt.Errorf("actual logout response contract: %w", err)
			return
		}
	}
	return
}

func (f *registeredLogoutFixture) request(t *testing.T, method, path, token string) (int, []byte) {
	t.Helper()
	out := f.call(t.Context(), method, path, token)
	if out.err != nil {
		t.Fatal(out.err)
	}
	return out.status, out.body
}

func (f *registeredLogoutFixture) count(ctx context.Context, token string) (int, error) {
	var count int
	err := f.ds.Pool().QueryRow(ctx, "SELECT count(*) FROM sessions WHERE token=$1", token).Scan(&count)
	return count, err
}

// testing.Context is already canceled when cleanup starts. Restore only this
// fixture's exact table through a separate bounded cleanup context.
func (f *registeredLogoutFixture) restoreTable(t *testing.T, from, to string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		query := "ALTER TABLE " + pgx.Identifier{f.schema, from}.Sanitize() + " RENAME TO " + pgx.Identifier{to}.Sanitize()
		if _, err := f.ds.Pool().Exec(ctx, query); err != nil {
			t.Error("restore own logout fixture table", err)
		}
	})
}

func assertLogoutFailure(t *testing.T, status int, body []byte, want int) {
	t.Helper()
	if status != want || bytes.Contains(body, []byte(`"status":"logged out"`)) || bytes.Contains(body, []byte(`"token"`)) || bytes.Contains(body, []byte(`"count"`)) {
		t.Fatalf("logout failure became success/token/count: %d %s", status, body)
	}
}

func TestPGLogoutRegisteredUnavailableAndInvalid(t *testing.T) {
	for _, kind := range []string{"initial-unavailable", "missing", "expired"} {
		t.Run(kind, func(t *testing.T) {
			f := registeredLogoutPG(t)
			token, want := f.token, 401
			switch kind {
			case "initial-unavailable":
				f.exec("ALTER TABLE sessions RENAME TO unavailable_logout_sessions")
				f.restoreTable(t, "unavailable_logout_sessions", "sessions")
				want = 503
			case "missing":
				token = registeredLogoutToken("missing")
			case "expired":
				f.exec("UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1", token)
			}
			f.start(t, t.Context())
			status, body := f.request(t, http.MethodPost, "/api/auth/logout", token)
			assertLogoutFailure(t, status, body, want)
		})
	}
}

func TestPGLogoutRegisteredOriginalActorFailureAfterCapture(t *testing.T) {
	for _, kind := range []string{"unavailable", "missing", "changed"} {
		t.Run(kind, func(t *testing.T) {
			f := registeredLogoutPG(t)
			var captured atomic.Bool
			f.hook = func(r *http.Request) error {
				captured.Store(true)
				query := "UPDATE accounts SET username=username WHERE id=$1"
				args := []any{f.account}
				switch kind {
				case "unavailable":
					query, args = "ALTER TABLE accounts RENAME TO unavailable_logout_accounts", nil
				case "missing":
					query = "DELETE FROM accounts WHERE id=$1"
				}
				_, err := f.ds.Pool().Exec(r.Context(), query, args...)
				return err
			}
			if kind == "unavailable" {
				f.restoreTable(t, "unavailable_logout_accounts", "accounts")
			}
			f.start(t, t.Context())
			status, body := f.request(t, http.MethodPost, "/api/auth/logout", f.token)
			want := 401
			if kind == "unavailable" {
				want = 503
			}
			assertLogoutFailure(t, status, body, want)
			if !captured.Load() {
				t.Fatal("case never reached original real PG logout")
			}
			if kind == "unavailable" {
				if count, err := f.count(t.Context(), f.token); err != nil || count != 1 {
					t.Fatal("outage changed the genuine original row", err)
				}
			}
		})
	}
}

// The proxy owns both directions and joins every accepted connection. It
// observes the server's actual COMMIT command tag before discarding that ACK.
type logoutOutcomeProxy struct {
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

func logoutProxyPG(t *testing.T) *logoutOutcomeProxy {
	t.Helper()
	u, err := url.Parse(os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL"))
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") {
		t.Fatal("own synthetic proxy fixture required")
	}
	tmp := os.Getenv("TMPDIR")
	if !strings.HasPrefix(tmp, "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("persistent private fixture temp directory required")
	}
	dir, err := os.MkdirTemp(tmp, "lgo-")
	if err != nil {
		t.Fatal(err)
	}
	port := u.Query().Get("port")
	if port == "" {
		port = "5432"
	}
	p := &logoutOutcomeProxy{dir: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port), done: make(chan struct{}), closed: make(chan struct{}), conns: map[net.Conn]struct{}{}}
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

func (p *logoutOutcomeProxy) forward(client net.Conn) {
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

func (p *logoutOutcomeProxy) close() {
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

func TestPGLogoutRegisteredKnownAndUnknownCommit(t *testing.T) {
	for _, kind := range []string{"known", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			var proxy *logoutOutcomeProxy
			if kind == "unknown" {
				proxy = logoutProxyPG(t)
			}
			f := registeredLogoutPG(t)
			f.start(t, t.Context())
			if proxy != nil {
				proxy.armed.Store(true)
			}
			status, body := f.request(t, http.MethodPost, "/api/auth/logout", f.token)
			if kind == "known" {
				if status != 200 || !bytes.Contains(body, []byte(`"status":"logged out"`)) {
					t.Fatal("known logout did not complete", status)
				}
			} else {
				assertLogoutFailure(t, status, body, 503)
				if !proxy.dropped.Load() {
					t.Fatal("unknown fixture did not observe a real COMMIT ACK")
				}
			}
			if count, err := f.count(t.Context(), f.token); err != nil || count != 0 {
				t.Fatal("actual acknowledged/lost-ACK commit was called rollback", err)
			}
		})
	}
}

type logoutPublicationWriter struct {
	http.ResponseWriter
	f       *registeredLogoutFixture
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (w *logoutPublicationWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *logoutPublicationWriter) Header() http.Header {
	header := w.ResponseWriter.Header()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count, err := w.f.count(ctx, w.f.token)
	if err == nil && count == 0 {
		w.once.Do(func() { close(w.entered); <-w.proceed })
	}
	return header
}

func TestPGLogoutRegisteredLatePublicationCannotSucceed(t *testing.T) {
	for _, kind := range []string{"canceled", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			f := registeredLogoutPG(t)
			ctx, cancel := context.WithCancel(t.Context())
			if kind == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
			}
			defer cancel()
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			f.wrap = func(w http.ResponseWriter, _ *http.Request) http.ResponseWriter {
				return &logoutPublicationWriter{ResponseWriter: w, f: f, entered: entered, proceed: proceed}
			}
			f.start(t, ctx)
			t.Cleanup(unblock)
			done := make(chan registeredLogoutResponse, 1)
			go func() { done <- f.call(t.Context(), http.MethodPost, "/api/auth/logout", f.token) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real post-COMMIT publication was not held")
			}
			if kind == "canceled" {
				cancel()
			}
			select {
			case <-ctx.Done():
			case <-time.After(4 * time.Second):
				t.Fatal("original server request lifetime did not end")
			}
			unblock()
			select {
			case value := <-done:
				if value.err != nil {
					t.Fatal(value.err)
				}
				assertLogoutFailure(t, value.status, value.body, 503)
			case <-time.After(5 * time.Second):
				t.Fatal("late canceled publication did not join")
			}
			if count, err := f.count(t.Context(), f.token); err != nil || count != 0 {
				t.Fatal("lost response changed the known committed logout", err)
			}
		})
	}
}

func TestPGLogoutRegisteredCanceledQueryJoins(t *testing.T) {
	f := registeredLogoutPG(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var holder pgx.Tx
	var holderMu sync.Mutex
	holderClosed := false
	entered := make(chan uint32, 1)
	f.hook = func(r *http.Request) error {
		actual, err := f.ds.Pool().Begin(t.Context())
		if err != nil {
			return err
		}
		holderMu.Lock()
		if holderClosed {
			holderMu.Unlock()
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = actual.Rollback(cleanup)
			return errors.New("fixture holder retired")
		}
		holder = actual
		holderMu.Unlock()
		if _, err := actual.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
			return err
		}
		var relation uint32
		if err := actual.QueryRow(t.Context(), "SELECT 'accounts'::regclass::oid").Scan(&relation); err != nil {
			return err
		}
		entered <- relation
		return r.Context().Err()
	}
	f.start(t, ctx)
	t.Cleanup(func() {
		cancel()
		holderMu.Lock()
		holderClosed = true
		actual := holder
		holderMu.Unlock()
		if actual != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = actual.Rollback(cleanup)
		}
	})
	done := make(chan registeredLogoutResponse, 1)
	go func() { done <- f.call(t.Context(), http.MethodPost, "/api/auth/logout", f.token) }()
	var relation uint32
	select {
	case relation = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("genuine PG frame did not enter actual core wait")
	}
	pending := func() int {
		var count int
		if err := f.ds.Pool().QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	until := time.Now().Add(time.Second)
	for pending() == 0 {
		if time.Now().After(until) {
			t.Fatal("actual core account query never blocked")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("logout finished while its query was still held")
	default:
	}
	cancel()
	select {
	case value := <-done:
		if value.err != nil {
			t.Fatal(value.err)
		}
		assertLogoutFailure(t, value.status, value.body, 503)
	case <-time.After(5 * time.Second):
		t.Fatal("registered cancellation did not join query/frame cleanup")
	}
	if pending() != 0 {
		t.Fatal("backend query outlived actual handler/frame return")
	}
	if count, err := f.count(t.Context(), f.token); err != nil || count != 1 {
		t.Fatal("canceled uncommitted logout changed original row", err)
	}
}

func TestPGLogoutRegisteredRefreshLineageKeepsFreshLogin(t *testing.T) {
	f := registeredLogoutPG(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(proceed) }) }
	f.hook = func(*http.Request) error { close(entered); <-proceed; return nil }
	f.start(t, t.Context())
	t.Cleanup(unblock)
	done := make(chan registeredLogoutResponse, 1)
	go func() { done <- f.call(t.Context(), http.MethodPost, "/api/auth/logout", f.token) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual registered logout did not retain original A frame")
	}
	status, body := f.request(t, http.MethodPost, "/api/auth/refresh", f.token)
	var rotated struct {
		Token string `json:"token"`
	}
	if status != 200 || json.Unmarshal(body, &rotated) != nil || rotated.Token == "" || rotated.Token == f.token {
		t.Fatal("actual public refresh did not create B", status)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	fresh := models.Session{Token: registeredLogoutToken("independent"), AccountID: f.account, ClientID: "synthetic-pg-api-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := f.ds.Sessions().Create(t.Context(), &fresh); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.request(t, http.MethodGet, "/api/auth/me", fresh.Token); status != 200 {
		t.Fatal("independent actual C login was not current", status)
	}
	unblock()
	select {
	case value := <-done:
		if value.err != nil {
			t.Fatal(value.err)
		}
		if value.status != 200 {
			t.Fatal("original A lineage logout failed", value.status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original logout did not complete")
	}
	for token, want := range map[string]int{f.token: 0, rotated.Token: 0, fresh.Token: 1} {
		if count, err := f.count(t.Context(), token); err != nil || count != want {
			t.Fatal("logout adopted another login or lost B lineage", err)
		}
	}
}

func TestPGLogoutOpenAPIUnavailableContract(t *testing.T) {
	f := registeredLogoutPG(t)
	f.exec("ALTER TABLE sessions RENAME TO unavailable_logout_sessions")
	f.restoreTable(t, "unavailable_logout_sessions", "sessions")
	f.start(t, t.Context())
	status, body := f.request(t, http.MethodPost, "/api/auth/logout", f.token)
	assertLogoutFailure(t, status, body, 503)
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	operation := validator.Doc.Paths.Value("/api/auth/logout").Post
	if operation.OperationID != "logout" || operation.Responses.Status(503) == nil {
		t.Fatal("existing logout operation lacks its actual503 response")
	}
}
