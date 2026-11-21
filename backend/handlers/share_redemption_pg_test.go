//go:build linux

package handlers

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
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This real Unix transport drops only an actually received COMMIT ACK. It
// never substitutes a query/auth result, and Close joins both actual pumps.
type shareRedemptionProxy struct {
	listener       net.Listener
	upstream, dir  string
	mu             sync.Mutex
	closed         bool
	connections    []net.Conn
	wg             sync.WaitGroup
	armed, dropped atomic.Bool
	once           sync.Once
}

func newShareRedemptionProxy(t *testing.T, u *url.URL) *shareRedemptionProxy {
	t.Helper()
	root := os.Getenv("TMPDIR")
	if !strings.HasPrefix(root, "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("persistent private test temporary root required")
	}
	dir, e := os.MkdirTemp(root, "sh-")
	if e != nil {
		t.Fatal(e)
	}
	port := u.Query().Get("port")
	if port == "" {
		port = u.Port()
	}
	if port == "" {
		t.Fatal("explicit own fixture port required")
	}
	listener, e := net.Listen("unix", filepath.Join(dir, ".s.PGSQL."+port))
	if e != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(e)
	}
	p := &shareRedemptionProxy{listener: listener, dir: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port)}
	t.Cleanup(p.Close)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			p.mu.Lock()
			if p.closed {
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
func (p *shareRedemptionProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	server, e := net.DialTimeout("unix", p.upstream, 5*time.Second)
	if e != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.connections = append(p.connections, server)
	p.mu.Unlock()
	copied := make(chan struct{})
	go func() { defer close(copied); _, _ = io.Copy(server, client); server.Close() }()
	defer func() { client.Close(); server.Close(); <-copied }()
	for {
		header := make([]byte, 5)
		if _, e = io.ReadFull(server, header); e != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[1:])
		if n < 4 || n > 64<<20 {
			return
		}
		body := make([]byte, n-4)
		if _, e = io.ReadFull(server, body); e != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && p.armed.CompareAndSwap(true, false) {
			p.dropped.Store(true)
			return
		}
		if _, e = client.Write(header); e != nil {
			return
		}
		if _, e = client.Write(body); e != nil {
			return
		}
	}
}
func (p *shareRedemptionProxy) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		connections := append([]net.Conn(nil), p.connections...)
		p.mu.Unlock()
		p.listener.Close()
		for _, c := range connections {
			c.Close()
		}
		p.wg.Wait()
		_ = os.RemoveAll(p.dir)
	})
}
func shareRedemptionThroughProxy(t *testing.T, f *sharePlaybackHTTPFixture) *shareRedemptionProxy {
	t.Helper()
	u, e := url.Parse(f.store.Pool().Config().ConnString())
	if e != nil {
		t.Fatal(e)
	}
	p := newShareRedemptionProxy(t, u)
	// Recompose real security/coordinator after retiring the old managed owner;
	// the same original synthetic schema/data is reachable through actual proxy.
	f.runtime.Close()
	f.base.h.Close()
	if e = f.security.Close(); e != nil {
		t.Fatal(e)
	}
	if e = f.coordinator.Close(); e != nil {
		t.Fatal(e)
	}
	query := u.Query()
	query.Set("host", p.dir)
	query.Set("sslmode", "disable")
	u.RawQuery = query.Encode()
	settings, e := f.manager.Load()
	if e != nil {
		t.Fatal(e)
	}
	settings.Database.URL = u.String()
	if e = f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	store, e := datastore.New(t.Context(), u.String(), datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(store.Close)
	objects, e := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = objects.Close() })
	coordinator, e := settingscommit.NewForPool(t.Context(), store.Pool(), objects)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	guard, e := coordinator.TryWrite(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = coordinator.Arm(guard); e != nil {
		t.Fatal(e)
	}
	security, e := config.InstallManagedSecurity(f.manager, coordinator, "")
	_ = guard.Close()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = security.Close() })
	runtime, e := datastore.NewShareLinkRedeemer(t.Context(), store, f.manager, security, coordinator)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(runtime.Close)
	f.share.SetManagedShareRedemption(runtime)
	return p
}
func TestSharePlaybackPublicOpen(t *testing.T) {
	for _, kind := range []string{"known-ack-redirect", "null-legacy-recreate", "ids-only-recreate", "wrong-token", "unknown-ack-no-output", "pre-header-cancel-503", "held-write-runtime-cancel", "held-write-source-expiry", "owned-display-params-no-authority"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharePlaybackHTTPFixture(t, "synthetic:public.wav", nil)
			if kind == "held-write-runtime-cancel" || kind == "held-write-source-expiry" {
				sharePublicHeldOutput(t, f, kind)
				return
			}
			token := f.linkToken
			switch kind {
			case "null-legacy-recreate":
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE share_links SET provenance_version=NULL,source_profile_id=NULL,source_account_id=NULL,delegation_version=NULL,creator_incarnation=NULL,source_owner_incarnation=NULL,source_profile_incarnation=NULL WHERE token=$1`, token); e != nil {
					t.Fatal(e)
				}
			case "ids-only-recreate":
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE share_links SET delegation_version=NULL,creator_incarnation=NULL,source_owner_incarnation=NULL,source_profile_incarnation=NULL WHERE token=$1`, token); e != nil {
					t.Fatal(e)
				}
			case "wrong-token":
				token = strings.Repeat("a", 43)
			case "owned-display-params-no-authority":
				params, _ := json.Marshal(map[string]string{"sourcePath": f.resource, "profileId": f.profileID, "userId": "forged-other", "title": "Synthetic title", "accountId": "forged-master", "isMaster": "true"})
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE share_links SET params=$1 WHERE token=$2`, params, token); e != nil {
					t.Fatal(e)
				}
			}
			var proxy *shareRedemptionProxy
			if kind == "unknown-ack-no-output" {
				proxy = shareRedemptionThroughProxy(t, f)
				proxy.armed.Store(true)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request := httptest.NewRequest("GET", "http://synthetic.test/share/"+token, nil).WithContext(ctx)
			record := httptest.NewRecorder()
			var output http.ResponseWriter = record
			if kind == "pre-header-cancel-503" {
				output = &sharePublicationCancelWriter{ResponseWriter: record, cancel: cancel, beforeCancel: func() {
					var n int
					if e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&n); e != nil || n != 2 {
						t.Error("pre-output signal preceded actual committed recipient", n, e)
					}
				}}
			}
			f.share.Open(output, request)
			var n int
			if e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&n); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "null-legacy-recreate", "ids-only-recreate", "wrong-token":
				if record.Code != 410 || record.Header().Get("Location") != "" || n != 1 {
					t.Fatal("legacy/unknown share did not require recreation", record.Code, n)
				}
			case "unknown-ack-no-output":
				if !proxy.dropped.Load() || record.Code != 503 || record.Header().Get("Location") != "" || n != 2 {
					t.Fatal("actual unknownACK leaked output/retried/claimed rollback", record.Code, n)
				}
			case "pre-header-cancel-503":
				if record.Code != 503 || record.Header().Get("Location") != "" || strings.Contains(record.Body.String(), "token=") || n != 2 {
					t.Fatal("pre-header cancel emitted success or lost knownACK", record.Code, n)
				}
			default:
				u, e := url.Parse(record.Header().Get("Location"))
				if e != nil || record.Code != 303 || u.Query().Get("token") == "" || u.Query().Get("profileId") != f.profileID || u.Query().Get("sourcePath") != "" || u.Query().Get("accountId") != "" || u.Query().Get("isMaster") != "" || u.Query().Get("userId") != "" || n != 2 {
					t.Fatal("actual committed redirect contained wrong authority/output", record.Code, n, e)
				}
				recipient := u.Query().Get("token")
				framed, release, e := f.pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), recipient, "")
				if e != nil {
					t.Fatal("redirect did not carry genuine recipient", e)
				}
				release()
				_ = framed
			}
		})
	}
}
func sharePublicHeldOutput(t *testing.T, f *sharePlaybackHTTPFixture, kind string) {
	t.Helper()
	params := map[string]string{"sourcePath": f.resource, "profileId": f.profileID}
	for k := range shareAllowedParams {
		if k != "profileId" && !shareRedirectSensitiveParams[k] && k != "userId" {
			params[k] = strings.Repeat("x", 4096)
		}
	}
	raw, _ := json.Marshal(params)
	if _, e := f.store.Pool().Exec(t.Context(), `UPDATE share_links SET params=$1 WHERE token=$2`, raw, f.linkToken); e != nil {
		t.Fatal(e)
	}
	life, retire := context.WithCancel(t.Context())
	defer retire()
	runtime, e := datastore.NewShareLinkRedeemer(life, f.store, f.manager, f.security, f.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	defer runtime.Close()
	f.share.SetManagedShareRedemption(runtime)
	if kind == "held-write-source-expiry" {
		// Existing actor/source UUID cannot be changed after creation. Create a new
		// original link only after genuine shortened source expiry is established.
		if _, e = f.store.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=clock_timestamp()+interval '4 seconds' WHERE id=$1`, f.base.base.pin.session.AccountID); e != nil {
			t.Fatal(e)
		}
		creator, e := datastore.NewShareLinkCreator(life, f.store, f.manager, f.security, f.coordinator)
		if e != nil {
			t.Fatal(e)
		}
		r, release, e := f.base.base.pin.pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), f.base.base.pin.session.Token, "")
		if e != nil {
			t.Fatal(e)
		}
		link, e := creator.Create(r, f.profileID, params, time.Hour, 4, "")
		release()
		if e != nil {
			t.Fatal(e)
		}
		f.linkToken = link.Token
	}
	done := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); f.share.Open(w, r) }))
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	c, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { retire(); _ = c.Close(); server.Close() })
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	if _, e = fmt.Fprintf(c, "GET /share/%s HTTP/1.1\r\nHost: synthetic.test\r\nConnection: close\r\n\r\n", f.linkToken); e != nil {
		t.Fatal(e)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no original output socket")
	}
	until := time.Now().Add(2 * time.Second)
	for actual.writing.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.writing.Load() == 0 {
		t.Fatal("actual redirect write not held")
	}
	var rows int
	if e = f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&rows); e != nil || rows != 2 {
		t.Fatal("held public write lacked knownACK", rows, e)
	}
	if kind == "held-write-runtime-cancel" {
		retire()
	}
	progressHTTPAwait(t, done)
	if actual.writing.Load() != 0 || kind == "held-write-runtime-cancel" && !actual.writeDemanded.Load() {
		t.Fatal("actual public output/callback survived lifetime")
	}
	if e = f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&rows); e != nil || rows != 2 {
		t.Fatal("late public output lost committed data", rows, e)
	}
}
