//go:build linux

package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/remoteaccess"
	"novastream/services/sessions"
)

// No external host, network, process or rendezvous publisher is installed.
type pairedTargetHost struct{}

func (pairedTargetHost) Ensure(context.Context) (string, error) {
	return "synthetic-existing-invite", nil
}
func (pairedTargetHost) Stop(context.Context) error { return nil }
func (pairedTargetHost) Status(context.Context) models.RemoteAccessStatus {
	return models.RemoteAccessStatus{Running: false}
}

type pairedTargetFixture struct {
	*publicPinCarrierFixture
	handler *AdminUIHandler
	route   http.Handler
	before  func(*http.Request)
}

func pairedTargetPG(t *testing.T) *pairedTargetFixture {
	t.Helper()
	f := &pairedTargetFixture{publicPinCarrierFixture: newPublicPinCarrierFixture(t)}
	if _, err := f.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); err != nil {
		t.Fatal(err)
	}
	f.handler = f.admin()
	f.handler.remoteAccessService = remoteaccess.NewService(f.store.RemoteAccessInvites(), pairedTargetHost{})
	at := time.Now().UTC().Truncate(time.Microsecond)
	for i, id := range []string{"synthetic-invite-one", "synthetic-invite-two"} {
		inv := models.RemoteAccessInvite{ID: id, TokenHash: strings.Repeat(string(rune('a'+i)), 64), ConnectionCode: "synthetic-code-" + id, IrohInvite: "synthetic-existing-invite", CreatedBy: "actor", UsedByPeerID: "synthetic-target", UsedAt: &at, CreatedAt: at.Add(-time.Duration(i) * time.Minute), ExpiresAt: at.Add(time.Hour)}
		if err := f.store.RemoteAccessInvites().Create(t.Context(), &inv); err != nil {
			t.Fatal(err)
		}
	}
	for _, token := range []string{"synthetic-target-one", "synthetic-target-two"} {
		value := models.Session{Token: token, AccountID: "actor", ClientID: "synthetic-target", Scope: models.SessionScopeJellyfin, ScopeResource: f.profile.ID, CreatedAt: at, ExpiresAt: at.Add(time.Hour)}
		if err := f.store.Sessions().Create(t.Context(), &value); err != nil {
			t.Fatal(err)
		}
	}
	router := mux.NewRouter()
	router.HandleFunc("/admin/api/remote-access/paired-devices/{inviteID}", f.handler.RequireMasterAuth(func(w http.ResponseWriter, r *http.Request) {
		if f.before != nil {
			f.before(r)
		}
		f.handler.RevokePairedDevice(w, r)
	})).Methods(http.MethodDelete)
	f.route = router
	return f
}
func (f *pairedTargetFixture) call(t *testing.T, ctx context.Context, id string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequestWithContext(ctx, http.MethodDelete, "http://synthetic.invalid/admin/api/remote-access/paired-devices/"+id, nil)
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
	w := httptest.NewRecorder()
	f.route.ServeHTTP(w, r)
	return w
}
func pairedTargetCount(t *testing.T, f *pairedTargetFixture) int {
	t.Helper()
	var count int
	if f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE client_id='synthetic-target'`).Scan(&count) != nil {
		t.Fatal("read actual target rows")
	}
	return count
}
func pairedTargetResult(t *testing.T, w *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	var result map[string]json.RawMessage
	if json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("response is not actual result JSON", w.Code)
	}
	return result
}
func TestPairedTargetRevokeActualPGKnownCounts(t *testing.T) {
	for _, kind := range []string{"cold", "already-revoked"} {
		t.Run(kind, func(t *testing.T) {
			f := pairedTargetPG(t)
			if len(f.sessions.GetSessionsForClient("synthetic-target")) != 0 {
				t.Fatal("target cache was warmed")
			}
			if kind == "already-revoked" {
				if _, err := f.store.RevokeClientSessions(t.Context(), "synthetic-target"); err != nil {
					t.Fatal(err)
				}
			}
			w := f.call(t, t.Context(), "synthetic-invite-one")
			got := pairedTargetResult(t, w)
			want := "2"
			if kind == "already-revoked" {
				want = "0"
			}
			if w.Code != http.StatusOK || string(got["ok"]) != "true" || string(got["revokedSessions"]) != want || string(got["revokedInvites"]) != "2" || pairedTargetCount(t, f) != 0 {
				t.Fatal("known PG count was replaced by cache or false result", w.Code)
			}
			for _, id := range []string{"synthetic-invite-one", "synthetic-invite-two"} {
				v, err := f.store.RemoteAccessInvites().Get(t.Context(), id)
				if err != nil || v == nil || v.RevokedAt == nil {
					t.Fatal("actual invite not revoked", err)
				}
			}
		})
	}
}

// This own Unix proxy discards an actually received server COMMIT command tag.
// Both real socket pumps are closed and joined; no fabricated SQL result exists.
type publicTargetProxy struct {
	listener            net.Listener
	upstream, directory string
	mu                  sync.Mutex
	closing             bool
	connections         []net.Conn
	wg                  sync.WaitGroup
	once                sync.Once
	armed, dropped      atomic.Bool
}

func publicTargetProxyStart(t *testing.T, u *url.URL) *publicTargetProxy {
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
	p := &publicTargetProxy{listener: listener, directory: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port)}
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
func (p *publicTargetProxy) forward(client net.Conn) {
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
func (p *publicTargetProxy) close() {
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

func TestPairedTargetRevokeActualPGPartialEffects(t *testing.T) {
	for _, kind := range []string{"sessions-unavailable", "sessions-unknown", "invite-error"} {
		t.Run(kind, func(t *testing.T) {
			f := pairedTargetPG(t)
			var proxy *publicTargetProxy
			if kind == "sessions-unknown" {
				u, err := url.Parse(f.store.Pool().Config().ConnString())
				if err != nil {
					t.Fatal(err)
				}
				proxy = publicTargetProxyStart(t, u)
				q := u.Query()
				q.Set("host", proxy.directory)
				q.Set("sslmode", "disable")
				u.RawQuery = q.Encode()
				ds, err := datastore.New(t.Context(), u.String())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(ds.Close)
				service, err := sessions.NewServiceWithStore(ds, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				f.handler.sessionsService = service
				f.before = func(*http.Request) { proxy.armed.Store(true) }
			} else if kind == "sessions-unavailable" {
				f.before = func(r *http.Request) {
					if _, err := f.store.Pool().Exec(r.Context(), `ALTER TABLE sessions RENAME TO unavailable_public_target_sessions`); err != nil {
						t.Fatal(err)
					}
				}
				t.Cleanup(func() {
					ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
					defer c()
					if _, err := f.store.Pool().Exec(ctx, `ALTER TABLE unavailable_public_target_sessions RENAME TO sessions`); err != nil {
						t.Error(err)
					}
				})
			} else {
				if _, err := f.store.Pool().Exec(t.Context(), `CREATE FUNCTION public_target_invite_error() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.id='synthetic-invite-two' THEN RAISE EXCEPTION 'synthetic invite update refused';END IF;RETURN NEW;END$$;CREATE TRIGGER public_target_invite_error BEFORE UPDATE ON remote_access_invites FOR EACH ROW EXECUTE FUNCTION public_target_invite_error()`); err != nil {
					t.Fatal(err)
				}
			}
			w := f.call(t, t.Context(), "synthetic-invite-one")
			got := pairedTargetResult(t, w)
			if w.Code != http.StatusServiceUnavailable || string(got["ok"]) != "false" {
				t.Fatal("partial/uncertain revocation claimed success", w.Code)
			}
			switch kind {
			case "sessions-unavailable":
				if string(got["sessionOutcome"]) != `"unavailable"` || got["revokedSessions"] != nil || string(got["revokedInvites"]) != "2" {
					t.Fatal("unavailable became invented zero or lost known invite updates")
				}
			case "sessions-unknown":
				if !proxy.dropped.Load() || string(got["sessionOutcome"]) != `"unknown"` || string(got["outcomeUnknown"]) != "true" || got["revokedSessions"] != nil || pairedTargetCount(t, f) != 0 {
					t.Fatal("lost received COMMIT ACK was not actual uncertain deletion")
				}
			case "invite-error":
				one, e := f.store.RemoteAccessInvites().Get(t.Context(), "synthetic-invite-one")
				two, e2 := f.store.RemoteAccessInvites().Get(t.Context(), "synthetic-invite-two")
				if e != nil || e2 != nil || one.RevokedAt == nil || two.RevokedAt != nil || string(got["inviteOutcome"]) != `"failed"` || string(got["revokedInvites"]) != "1" || string(got["revokedSessions"]) != "2" || pairedTargetCount(t, f) != 0 {
					t.Fatal("real durable partial invite/session effects were hidden")
				}
			}
		})
	}
}
func TestPairedTargetRevokeActualPGCanceledSQLJoins(t *testing.T) {
	f := pairedTargetPG(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lock, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = lock.Rollback(cleanup)
	})
	var relation uint32
	if f.store.Pool().QueryRow(t.Context(), `SELECT 'sessions'::regclass::oid`).Scan(&relation) != nil {
		t.Fatal("actual relation missing")
	}
	f.before = func(r *http.Request) {
		if _, err := lock.Exec(r.Context(), `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
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
			t.Error("actual canceled handler did not join during cleanup")
		}
	})
	go func() { defer close(actualDone); done <- f.call(t, ctx, "synthetic-invite-one") }()
	until := time.Now().Add(3 * time.Second)
	pending := func() int {
		var n int
		err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.relation=$1 AND NOT l.granted AND a.query='DELETE FROM sessions WHERE client_id=$1'`, relation).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for pending() != 1 {
		if time.Now().After(until) {
			t.Fatal("actual target DELETE did not block")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case w := <-done:
		if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 {
			t.Fatal("canceled request published success")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("actual handler/query did not join")
	}
	if pending() != 0 {
		t.Fatal("handler returned before actual query end")
	}
	if lock.Rollback(t.Context()) != nil || pairedTargetCount(t, f) != 2 {
		t.Fatal("canceled DELETE persisted")
	}
}
func TestPairedTargetRevokeActualPeerSelection(t *testing.T) {
	for _, kind := range []string{"missing", "unclaimed", "foreign-peer"} {
		t.Run(kind, func(t *testing.T) {
			f := pairedTargetPG(t)
			id := "missing"
			if kind != "missing" {
				at := time.Now().UTC()
				inv := models.RemoteAccessInvite{ID: "synthetic-other-invite", TokenHash: strings.Repeat("c", 64), ConnectionCode: "synthetic-other-code", IrohInvite: "synthetic-existing-invite", CreatedBy: "actor", CreatedAt: at, ExpiresAt: at.Add(time.Hour)}
				if kind == "foreign-peer" {
					inv.UsedAt = &at
					inv.UsedByPeerID = "synthetic-other-peer"
				}
				if err := f.store.RemoteAccessInvites().Create(t.Context(), &inv); err != nil {
					t.Fatal(err)
				}
				id = inv.ID
			}
			w := f.call(t, t.Context(), id)
			if kind == "foreign-peer" {
				got := pairedTargetResult(t, w)
				if w.Code != http.StatusOK || string(got["revokedSessions"]) != "0" {
					t.Fatal("selected peer did not remain exact", w.Code)
				}
			} else if w.Code != http.StatusNotFound {
				t.Fatal("unknown/unclaimed invite selected sessions", w.Code)
			}
			if pairedTargetCount(t, f) != 2 {
				t.Fatal("unrelated target sessions deleted")
			}
		})
	}
}
func TestPairedTargetRevokeExplicitMemoryMode(t *testing.T) {
	f := pairedTargetPG(t)
	memory, err := sessions.NewService(t.TempDir(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if memory.UsesPostgres() || !f.sessions.UsesPostgres() {
		t.Fatal("configured storage mode is not explicit")
	}
	if _, err = memory.CreateForClient("actor", false, "synthetic", "127.0.0.1", "synthetic-target"); err != nil {
		t.Fatal(err)
	}
	f.handler.sessionsService = memory
	w := f.call(t, t.Context(), "synthetic-invite-one")
	got := pairedTargetResult(t, w)
	if w.Code != http.StatusOK || string(got["revokedSessions"]) != "1" || memory.Count() != 0 || pairedTargetCount(t, f) != 2 {
		t.Fatal("explicit memory mode adopted PG or lost compatibility")
	}
	var nilService *sessions.Service
	if nilService.UsesPostgres() {
		t.Fatal("nil claims PG mode")
	}
}
