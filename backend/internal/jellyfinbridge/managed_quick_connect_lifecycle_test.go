//go:build linux

package jellyfinbridge

import (
	"bytes"
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
)

// This own Unix proxy counts actually received server COMMIT command tags.
// Dropping one does not fabricate a result; gating one retains the actual ACK.
type publicQCProxy struct {
	listener                    net.Listener
	upstream, directory         string
	mu                          sync.Mutex
	closing                     bool
	connections                 []net.Conn
	wg                          sync.WaitGroup
	once, pauseOnce, resumeOnce sync.Once
	dropAfter, pauseAfter       atomic.Int32
	dropped                     atomic.Bool
	paused, resume              chan struct{}
}

func publicQCProxyStart(t *testing.T, u *url.URL) *publicQCProxy {
	t.Helper()
	temp := os.Getenv("TMPDIR")
	host := u.Query().Get("host")
	if !strings.HasPrefix(temp, "/home/stefan/ops-runbook/tmp/") || !strings.HasPrefix(host, "/home/stefan/ops-runbook/tmp/") || u.Host != "" {
		t.Fatal("own persistent synthetic Unix PG only")
	}
	directory, err := os.MkdirTemp(temp, "q-")
	if err != nil {
		t.Fatal(err)
	}
	port := u.Query().Get("port")
	if port == "" {
		port = "5432"
	}
	listener, err := net.Listen("unix", filepath.Join(directory, ".s.PGSQL."+port))
	if err != nil {
		os.RemoveAll(directory)
		t.Fatal(err)
	}
	p := &publicQCProxy{listener: listener, upstream: filepath.Join(host, ".s.PGSQL."+port), directory: directory, paused: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(p.close)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closing {
				p.mu.Unlock()
				client.Close()
				return
			}
			p.connections = append(p.connections, client)
			p.wg.Add(1)
			p.mu.Unlock()
			go p.forward(client)
		}
	}()
	return p
}
func (p *publicQCProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	server, err := (&net.Dialer{}).DialContext(ctx, "unix", p.upstream)
	cancel()
	if err != nil {
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
		if _, err = io.ReadFull(server, header[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header[1:])
		if length < 4 || length > 1<<20 {
			return
		}
		body := make([]byte, int(length)-4)
		if _, err = io.ReadFull(server, body); err != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" {
			if p.dropAfter.Load() > 0 && p.dropAfter.Add(-1) == 0 {
				p.dropped.Store(true)
				return
			}
			if p.pauseAfter.Load() > 0 && p.pauseAfter.Add(-1) == 0 {
				p.pauseOnce.Do(func() { close(p.paused) })
				<-p.resume
			}
		}
		if _, err = client.Write(header[:]); err != nil {
			return
		}
		if _, err = client.Write(body); err != nil {
			return
		}
	}
}
func (p *publicQCProxy) proceed() { p.resumeOnce.Do(func() { close(p.resume) }) }
func (p *publicQCProxy) close() {
	p.once.Do(func() {
		p.proceed()
		p.mu.Lock()
		p.closing = true
		connections := append([]net.Conn(nil), p.connections...)
		p.mu.Unlock()
		p.listener.Close()
		for _, connection := range connections {
			connection.Close()
		}
		p.wg.Wait()
		os.RemoveAll(p.directory)
	})
}
func publicQCAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("owned QC operation did not complete")
	}
}

type publicQCWriteListener struct {
	net.Listener
	requested chan struct{}
	once      sync.Once
}
type publicQCWriteConn struct {
	net.Conn
	owner *publicQCWriteListener
}

func (l *publicQCWriteListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &publicQCWriteConn{Conn: connection, owner: l}, nil
}
func (c *publicQCWriteConn) SetWriteDeadline(deadline time.Time) error {
	err := c.Conn.SetWriteDeadline(deadline)
	if err == nil && !deadline.IsZero() && !deadline.After(time.Now()) {
		c.owner.once.Do(func() { close(c.owner.requested) })
	}
	return err
}

func TestManagedQuickConnectHeldBodyCancellation(t *testing.T) {
	for _, kind := range []string{"caller", "startup", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			f := publicQCPG(t, true, 0, life)
			result := f.initiate(t)
			if f.authorize(t, result).Code != 200 {
				t.Fatal("actual prior approval")
			}
			caller, cancelCaller := context.WithCancel(t.Context())
			defer cancelCaller()
			done := make(chan struct{})
			var reads *managedPasswordBodyListener
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.armed.Store(true)
				f.server.Handler().ServeHTTP(w, r)
				close(done)
			}))
			reads = &managedPasswordBodyListener{Listener: server.Listener, entered: make(chan struct{}), returned: make(chan struct{}), interrupted: make(chan struct{})}
			writes := &publicQCWriteListener{Listener: reads, requested: make(chan struct{})}
			server.Listener = writes
			server.Config.BaseContext = func(net.Listener) context.Context { return caller }
			// This is the actual HTTP server the bridge shutdown must own/join.
			f.server.mu.Lock()
			f.server.httpSrv = server.Config
			f.server.mu.Unlock()
			server.Start()
			t.Cleanup(server.Close)
			client, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			defer func() { cancelCaller(); retire(); client.Close(); publicQCAwait(t, done) }()
			if _, err = fmt.Fprintf(client, "POST /Users/AuthenticateWithQuickConnect HTTP/1.1\r\nHost: synthetic.invalid\r\nAuthorization: MediaBrowser DeviceId=\"%s\"\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{\"Secret\":\"%s", publicQCTargetClient, result.Secret); err != nil {
				t.Fatal(err)
			}
			publicQCAwait(t, reads.entered)
			select {
			case <-reads.returned:
				t.Fatal("actual held body completed before cancellation")
			default:
			}
			shutdown := make(chan struct{})
			if kind == "caller" {
				cancelCaller()
			} else if kind == "startup" {
				retire()
			} else {
				go func() { _ = f.server.Shutdown(context.Background()); close(shutdown) }()
			}
			publicQCAwait(t, reads.interrupted)
			publicQCAwait(t, writes.requested)
			publicQCAwait(t, reads.returned)
			publicQCAwait(t, done)
			if kind == "shutdown" {
				publicQCAwait(t, shutdown)
			}
			if f.count(t) != 0 || f.ds.Pool().Stat().AcquiredConns() != 0 {
				t.Fatal("held body cancellation entered mint or left actual work")
			}
		})
	}
}

func TestManagedQuickConnectLateMetadata(t *testing.T) {
	for _, kind := range []string{"earlier-session", "earlier-account", "cancelled-metadata"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, true, -1)
			if kind != "cancelled-metadata" {
				until := time.Now().Add(5 * time.Second).UTC().Truncate(time.Microsecond)
				if kind == "earlier-account" {
					if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, until); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=$1 WHERE token=$2`, until, f.actorToken); err != nil {
						t.Fatal(err)
					}
				}
			}
			result := f.initiate(t)
			if f.authorize(t, result).Code != 200 {
				t.Fatal("actual bounded original approval")
			}
			connection, err := f.ds.Pool().Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			pid := connection.Conn().PgConn().PID()
			connection.Release()
			f.proxy.pauseAfter.Store(2)
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			body, _ := json.Marshal(map[string]string{"Secret": result.Secret})
			r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/Users/AuthenticateWithQuickConnect", bytes.NewReader(body))
			r.RemoteAddr = "198.51.100.9:1234"
			r.Header.Set("Authorization", `MediaBrowser DeviceId="target-public-device"`)
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { f.server.Handler().ServeHTTP(w, r); close(done) }()
			defer func() { cancel(); f.proxy.proceed(); publicQCAwait(t, done) }()
			publicQCAwait(t, f.proxy.paused)
			// The actual Release COMMIT is complete, but its genuine ACK is held.
			// Lock only now, so this blocks the subsequent DATA query, not mint.
			tx, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err = tx.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "users"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			f.proxy.proceed()
			waiting := false
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND query LIKE '%a.id IS NOT NULL,u.is_kids_profile%')`, pid).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !waiting {
				t.Fatal("actual post-cleanup DATA query never blocked")
			}
			if kind == "cancelled-metadata" {
				cancel()
			}
			publicQCAwait(t, done)
			assertPublicQCError(t, w, 503)
			if f.count(t) != 1 || f.ds.Pool().Stat().AcquiredConns() != 0 {
				t.Fatal("late metadata delivered or retained connection instead of withholding known token")
			}
			_ = tx.Rollback(context.Background())
			guard, err := f.coordinator.TryWrite(t.Context())
			if err != nil {
				t.Fatal("outer original native hold refunded before/after wrong lifecycle", err)
			}
			_ = guard.Close()
		})
	}
}

func publicQCFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}
func TestManagedQuickConnectServerLifecycle(t *testing.T) {
	for _, kind := range []string{"stop-restart", "bind-failure", "late-init-attach", "late-approval-attach"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, false, 0)
			m := f.server.managedQC
			if kind == "stop-restart" {
				f.wire.Close()
				_ = f.server.Shutdown(context.Background())
				controller := NewController()
				controller.Configure(f.server.deps, nil)
				cfg := f.server.cfg
				cfg.Enabled = true
				cfg.Host = "127.0.0.1"
				cfg.Port = publicQCFreePort(t)
				if err := controller.Apply(cfg); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { controller.Shutdown(context.Background()) })
				f.server = controller.server
				f.endpoint = fmt.Sprintf("http://127.0.0.1:%d", cfg.Port)
				old := f.server.managedQC
				result := f.initiate(t)
				cfg.TrustedProxies = []string{"127.0.0.1/32"}
				if err := controller.Apply(cfg); err != nil {
					t.Fatal(err)
				}
				f.server = controller.server
				publicQCAwait(t, old.ctx.Done())
				if f.server.managedQC == old || !f.runtime.ready() {
					t.Fatal("restart reused closed issuer or closed original Main runtime")
				}
				assertPublicQCError(t, f.collect(t, result), 404)
				_ = f.initiate(t)
				return
			}
			if kind == "bind-failure" {
				result := f.initiate(t)
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				f.server.cfg.Host = "127.0.0.1"
				f.server.cfg.Port = listener.Addr().(*net.TCPAddr).Port
				if err = f.server.Start(); err == nil {
					t.Fatal("occupied actual listener unexpectedly bound")
				}
				publicQCAwait(t, m.ctx.Done())
				assertPublicQCError(t, f.collect(t, result), 503)
				return
			}
			if kind == "late-init-attach" {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/QuickConnect/Initiate", nil)
				r.RemoteAddr = "198.51.100.7:1234"
				challenge, err := m.issuer.Initiate(r, publicQCTargetClient)
				if err != nil {
					t.Fatal(err)
				}
				details, err := challenge.Details()
				if err != nil {
					t.Fatal(err)
				}
				entry := &managedQuickConnectEntry{challenge: challenge, client: clientInfo{DeviceID: publicQCTargetClient}, details: details}
				m.close()
				if m.attach(entry) {
					t.Fatal("late original challenge attached after shutdown")
				}
				if _, err = challenge.Details(); err == nil {
					t.Fatal("late attach did not join original challenge cleanup")
				}
				return
			}
			result := f.initiate(t)
			entry := m.lookup(result.Secret, "")
			if entry == nil || !m.claimApproval(entry) {
				t.Fatal("actual wrapper ownership claim")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			input := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/QuickConnect/Authorize", nil)
			input.RemoteAddr = "198.51.100.7:1234"
			input.Header.Set("X-Client-ID", publicQCApproverClient)
			checked, release, err := f.runtime.pins.Authenticate(input, f.actorToken, publicQCApproverClient)
			if err != nil {
				t.Fatal(err)
			}
			approval, err := m.issuer.ApproveAuthenticated(checked, entry.challenge)
			release()
			if err != nil {
				t.Fatal(err)
			}
			m.close()
			if m.attachApproval(entry, approval) {
				t.Fatal("late original approval attached after shutdown")
			}
			if _, err = entry.challenge.Details(); err == nil {
				t.Fatal("late approval cleanup retained authority")
			}
		})
	}
}
func TestManagedQuickConnectActualPeerPortCapacity(t *testing.T) {
	f := publicQCPG(t, false, 0)
	// The real HTTP transport disables keep-alive, so every request has a
	// different actual TCP source port while the peer host remains unchanged.
	for n := 0; n < 5; n++ {
		if f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Initiate", publicQCTargetClient, "", nil).Code != 200 {
			t.Fatal("genuine within-capacity TCP initiation", n)
		}
	}
	assertPublicQCError(t, f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Initiate", publicQCTargetClient, "", nil), 429)
}
