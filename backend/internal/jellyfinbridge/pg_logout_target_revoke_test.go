//go:build linux

package jellyfinbridge

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/sessions"
	"novastream/services/users"
)

type bridgeLogoutChecked struct {
	*sessions.Service
	before func(context.Context)
}

func (s *bridgeLogoutChecked) RevokeContext(ctx context.Context, token string) error {
	if s.before != nil {
		s.before(ctx)
	}
	return s.Service.RevokeContext(ctx, token)
}

type bridgeLogoutFixture struct {
	ds      *datastore.DataStore
	admin   *pgxpool.Pool
	server  *Server
	checked *bridgeLogoutChecked
	schema  string
}

func bridgeLogoutPG(t *testing.T, proxied bool) (*bridgeLogoutFixture, *bridgeLogoutProxy) {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic Unix PG required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("only own synthetic Unix PG allowed")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	f := &bridgeLogoutFixture{admin: admin, schema: fmt.Sprintf("synthetic_bridge_logout_%d", time.Now().UnixNano())}
	owned := false
	t.Cleanup(func() {
		if f.server != nil {
			_ = f.server.Shutdown(context.Background())
		}
		if f.ds != nil {
			f.ds.Close()
		}
		ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if owned {
			if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE"); err != nil {
				t.Error(err)
			}
		}
		admin.Close()
	})
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	owned = true
	q := u.Query()
	q.Set("search_path", pgx.Identifier{f.schema}.Sanitize()+",pg_catalog")
	u.RawQuery = q.Encode()
	var proxy *bridgeLogoutProxy
	if proxied {
		proxy = bridgeLogoutProxyStart(t, u)
		q = u.Query()
		q.Set("host", proxy.directory)
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
	}
	f.ds, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(4))
	if err != nil {
		t.Fatal("migrate own actual schema", err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash,is_master)VALUES('actor','Synthetic actor','synthetic-unused',true);INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic profile')`); err != nil {
		t.Fatal(err)
	}
	service, err := sessions.NewServiceWithStore(f.ds, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.checked = &bridgeLogoutChecked{Service: service}
	account, err := accounts.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewServiceWithStore(f.ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.server = New(Deps{Accounts: account, Users: profiles, Sessions: f.checked}, config.JellyfinBridgeSettings{ServerID: "synthetic-logout-server"})
	at := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "synthetic-bridge-logout", AccountID: "actor", ClientID: "synthetic-logout-client", Scope: models.SessionScopeJellyfin, ScopeResource: "target", CreatedAt: at, ExpiresAt: at.Add(time.Hour)}
	if err = f.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	if service.Count() != 0 {
		t.Fatal("target session cache warmed before actual PG lookup")
	}
	return f, proxy
}
func (f *bridgeLogoutFixture) call(t *testing.T, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/Sessions/Logout", nil)
	r.Header.Set("Authorization", `MediaBrowser DeviceId="synthetic-logout-client", Token="synthetic-bridge-logout"`)
	w := httptest.NewRecorder()
	f.server.Handler().ServeHTTP(w, r)
	return w
}
func bridgeLogoutCount(t *testing.T, f *bridgeLogoutFixture) int {
	t.Helper()
	var n int
	if f.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.schema, "sessions"}.Sanitize()+` WHERE token='synthetic-bridge-logout'`).Scan(&n) != nil {
		t.Fatal("actual stored session count unavailable")
	}
	return n
}

// This own Unix proxy discards an actually received server COMMIT command tag.
// Both real socket pumps are closed and joined; no fabricated SQL result exists.
type bridgeLogoutProxy struct {
	listener            net.Listener
	upstream, directory string
	mu                  sync.Mutex
	closing             bool
	connections         []net.Conn
	wg                  sync.WaitGroup
	once                sync.Once
	armed, dropped      atomic.Bool
}

func bridgeLogoutProxyStart(t *testing.T, u *url.URL) *bridgeLogoutProxy {
	t.Helper()
	temp := os.Getenv("TMPDIR")
	if !strings.HasPrefix(temp, "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("own private persistent TMP required")
	}
	dir, e := os.MkdirTemp(temp, "sr-")
	if e != nil {
		t.Fatal(e)
	}
	port := u.Query().Get("port")
	if port == "" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/home/stefan/ops-runbook/tmp/") {
		os.RemoveAll(dir)
		t.Fatal("only dedicated own Unix PG is allowed")
	}
	listener, e := net.Listen("unix", filepath.Join(dir, ".s.PGSQL."+port))
	if e != nil {
		os.RemoveAll(dir)
		t.Fatal(e)
	}
	p := &bridgeLogoutProxy{listener: listener, directory: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port)}
	t.Cleanup(p.close)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			p.mu.Lock()
			if p.closing {
				p.mu.Unlock()
				c.Close()
				continue
			}
			p.connections = append(p.connections, c)
			p.wg.Add(1)
			p.mu.Unlock()
			go p.forward(c)
		}
	}()
	return p
}
func (p *bridgeLogoutProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	server, e := (&net.Dialer{}).DialContext(ctx, "unix", p.upstream)
	cancel()
	if e != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.connections = append(p.connections, server)
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { _, _ = io.Copy(server, client); server.Close(); close(done) }()
	defer func() { client.Close(); server.Close(); <-done }()
	for {
		var header [5]byte
		if _, e = io.ReadFull(server, header[:]); e != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[1:])
		if size < 4 || size > 1<<20 {
			return
		}
		body := make([]byte, int(size)-4)
		if _, e = io.ReadFull(server, body); e != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && p.armed.CompareAndSwap(true, false) {
			p.dropped.Store(true)
			return
		}
		if _, e = client.Write(header[:]); e != nil {
			return
		}
		if _, e = client.Write(body); e != nil {
			return
		}
	}
}
func (p *bridgeLogoutProxy) close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closing = true
		connections := append([]net.Conn(nil), p.connections...)
		p.mu.Unlock()
		p.listener.Close()
		for _, c := range connections {
			c.Close()
		}
		p.wg.Wait()
		os.RemoveAll(p.directory)
	})
}

func TestBridgeTargetLogoutActualPGKnown(t *testing.T) {
	for _, kind := range []string{"cold", "already-missing"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := bridgeLogoutPG(t, false)
			if kind == "already-missing" {
				f.checked.before = func(ctx context.Context) {
					if _, err := f.ds.Pool().Exec(ctx, `DELETE FROM sessions WHERE token='synthetic-bridge-logout'`); err != nil {
						t.Error(err)
					}
				}
			}
			w := f.call(t, t.Context())
			if w.Code != http.StatusNoContent || w.Body.Len() != 0 || bridgeLogoutCount(t, f) != 0 {
				t.Fatal("known successful/absent logout was not idempotent", w.Code)
			}
		})
	}
}
func TestBridgeTargetLogoutActualPGFailure(t *testing.T) {
	for _, kind := range []string{"unavailable", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			f, proxy := bridgeLogoutPG(t, kind == "unknown")
			if kind == "unknown" {
				f.checked.before = func(context.Context) { proxy.armed.Store(true) }
			} else {
				f.checked.before = func(ctx context.Context) {
					if _, err := f.ds.Pool().Exec(ctx, `ALTER TABLE sessions RENAME TO unavailable_public_logout_sessions`); err != nil {
						t.Error(err)
					}
				}
				t.Cleanup(func() {
					ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
					defer c()
					if _, err := f.ds.Pool().Exec(ctx, `ALTER TABLE unavailable_public_logout_sessions RENAME TO sessions`); err != nil {
						t.Error(err)
					}
				})
			}
			w := f.call(t, t.Context())
			var got struct {
				Error   string `json:"error"`
				Unknown bool   `json:"outcomeUnknown"`
			}
			if w.Code != http.StatusServiceUnavailable || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.Error == "" || got.Unknown != (kind == "unknown") {
				t.Fatal("failed/unknown logout was reported as success", w.Code)
			}
			if strings.Contains(w.Body.String(), "synthetic-bridge-logout") {
				t.Fatal("response exposed session token")
			}
			if kind == "unknown" && (!proxy.dropped.Load() || bridgeLogoutCount(t, f) != 0) {
				t.Fatal("actual received COMMIT ACK did not demonstrate uncertain deletion")
			}
		})
	}
}
func TestBridgeTargetLogoutActualPGCancelJoins(t *testing.T) {
	f, _ := bridgeLogoutPG(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lock, err := f.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = lock.Rollback(ctx)
	})
	var relation uint32
	if f.ds.Pool().QueryRow(t.Context(), `SELECT 'sessions'::regclass::oid`).Scan(&relation) != nil {
		t.Fatal("actual relation absent")
	}
	f.checked.before = func(ctx context.Context) {
		if _, err := lock.Exec(ctx, `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Error(err)
		}
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	actualDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lock.Rollback(cleanup)
		stop()
		select {
		case <-actualDone:
		case <-time.After(8 * time.Second):
			t.Error("actual canceled logout handler did not join during cleanup")
		}
	})
	go func() { defer close(actualDone); done <- f.call(t, ctx) }()
	until := time.Now().Add(3 * time.Second)
	pending := func() int {
		var n int
		if f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.relation=$1 AND NOT l.granted AND a.query='DELETE FROM sessions WHERE token=$1'`, relation).Scan(&n) != nil {
			t.Fatal("actual wait unavailable")
		}
		return n
	}
	for pending() != 1 {
		if time.Now().After(until) {
			t.Fatal("actual target revoke did not wait")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case w := <-done:
		if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 {
			t.Fatal("canceled logout published success")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("actual handler/query did not join")
	}
	if pending() != 0 {
		t.Fatal("return preceded actual backend query end")
	}
	if lock.Rollback(t.Context()) != nil || bridgeLogoutCount(t, f) != 1 {
		t.Fatal("canceled uncommitted revoke persisted")
	}
}
func TestBridgeTargetLogoutExplicitMemoryMode(t *testing.T) {
	h := newHarness(t, config.JellyfinBridgeSettings{})
	if h.sessions.UsesPostgres() {
		t.Fatal("legacy fixture unexpectedly PG")
	}
	token := h.login("alice/Living Room", "alice-pass")
	w := h.do(http.MethodPost, "/Sessions/Logout", token, nil)
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatal("actual explicit memory logout regressed", w.Code)
	}
	if _, err := h.sessions.ValidateScoped(token, models.SessionScopeJellyfin); err == nil {
		t.Fatal("memory token remained valid")
	}
}
