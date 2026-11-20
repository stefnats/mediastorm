//go:build linux

package sessions

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

func targetServiceSeed(t *testing.T, f *accountRevokePGFixture, token, client string) models.Session {
	t.Helper()
	value := f.seed(t, token, "actor")
	value.ClientID = client
	if err := f.ds.Sessions().Update(t.Context(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestPGTargetRevokeColdCacheCheckedAndLegacy(t *testing.T) {
	for _, kind := range []string{"token", "empty-token", "legacy-token", "client", "legacy-client"} {
		t.Run(kind, func(t *testing.T) {
			f := sessionAccountRevokePG(t)
			token := "synthetic-cold"
			if kind == "empty-token" {
				token = ""
			}
			targetServiceSeed(t, f, token, "synthetic-target")
			other := targetServiceSeed(t, f, "synthetic-other", "other-device")
			if f.svc.Count() != 0 {
				t.Fatal("test warmed cache before checked PG operation")
			}
			var count int64
			var err error
			switch kind {
			case "token", "empty-token":
				err = f.svc.RevokeContext(t.Context(), token)
			case "legacy-token":
				err = f.svc.Revoke(token)
			case "client":
				count, err = f.svc.RevokeAllForClientContext(t.Context(), " synthetic-target ")
			case "legacy-client":
				count = int64(f.svc.RevokeAllForClient(" synthetic-target "))
			}
			if err != nil || strings.Contains(kind, "client") && count != 1 || f.count(t, "actor") != 1 {
				t.Fatal("cold cache replaced actual target DELETE", err, count)
			}
			row, e := f.ds.Sessions().Get(t.Context(), other.Token)
			if e != nil || row == nil {
				t.Fatal("foreign client was revoked", e)
			}
		})
	}
}

func TestPGTargetRevokeStaleProjectionCannotCreateCounts(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := sessionAccountRevokePG(t)
			value := targetServiceSeed(t, f, "synthetic-live", "other-device")
			stale := value
			stale.Token = "synthetic-stale"
			stale.ClientID = "synthetic-target"
			f.svc.mu.Lock()
			f.svc.sessions[stale.Token] = stale
			f.svc.sessions[value.Token] = value
			f.svc.mu.Unlock()
			if kind == "token" {
				if !errors.Is(f.svc.RevokeContext(t.Context(), stale.Token), ErrSessionNotFound) {
					t.Fatal("known missing token was not reported")
				}
			} else {
				n, e := f.svc.RevokeAllForClientContext(t.Context(), stale.ClientID)
				if e != nil || n != 0 {
					t.Fatal("stale cache fabricated affected rows", e, n)
				}
			}
			if f.svc.Count() != 1 || f.count(t, "actor") != 1 {
				t.Fatal("known stale projection cleanup changed actual row")
			}
		})
	}
}

func TestPGTargetRevokeCheckedKeepsMemoryAndJSONUnchanged(t *testing.T) {
	dir := t.TempDir()
	svc, e := NewService(dir, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	value, e := svc.CreateForClient("actor", false, "synthetic", "127.0.0.1", "synthetic-target")
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "sessions.json"))
	if e != nil {
		t.Fatal(e)
	}
	var zero *Service
	for _, call := range []func() error{func() error { return svc.RevokeContext(t.Context(), value.Token) }, func() error { _, e := svc.RevokeAllForClientContext(t.Context(), value.ClientID); return e }, func() error { return zero.RevokeContext(t.Context(), "") }, func() error { _, e := zero.RevokeAllForClientContext(t.Context(), ""); return e }} {
		if !errors.Is(call(), datastore.ErrSessionTargetRevokeUnavailable) {
			t.Fatal("PG-only method adopted non-PG state")
		}
	}
	after, e := os.ReadFile(filepath.Join(dir, "sessions.json"))
	if e != nil || !bytes.Equal(before, after) || svc.Count() != 1 {
		t.Fatal("checked API mutated memory/JSON", e)
	}
	if svc.RevokeAllForClient(" ") != 0 || svc.RevokeAllForClient(" synthetic-target ") != 1 || svc.Revoke(value.Token) != ErrSessionNotFound {
		t.Fatal("ordinary memory behavior changed")
	}
}

func TestPGTargetRevokeFailuresRetainProjection(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := sessionAccountRevokePG(t)
			value := targetServiceSeed(t, f, "synthetic-cached", "synthetic-target")
			f.svc.mu.Lock()
			f.svc.sessions[value.Token] = value
			f.svc.mu.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			call := func(ctx context.Context) error {
				if kind == "token" {
					return f.svc.RevokeContext(ctx, value.Token)
				}
				_, e := f.svc.RevokeAllForClientContext(ctx, value.ClientID)
				return e
			}
			if !errors.Is(call(ctx), context.Canceled) {
				t.Fatal("already canceled operation succeeded")
			}
			if _, e := f.ds.Pool().Exec(t.Context(), `ALTER TABLE sessions RENAME TO unavailable_target_sessions`); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				if _, e := f.ds.Pool().Exec(context.Background(), `ALTER TABLE unavailable_target_sessions RENAME TO sessions`); e != nil {
					t.Error(e)
				}
			})
			if e := call(t.Context()); !errors.Is(e, datastore.ErrSessionTargetRevokeUnavailable) {
				t.Fatal("unavailable store was reported as target success", e)
			}
			if f.svc.RevokeAllForClient(value.ClientID) != 0 || f.svc.Revoke(value.Token) == nil || f.svc.Count() != 1 {
				t.Fatal("failed legacy/checked calls claimed cache success")
			}
		})
	}
}

func TestPGTargetRevokeNoMutexDuringActualSQL(t *testing.T) {
	for _, kind := range []string{"token", "client"} {
		t.Run(kind, func(t *testing.T) {
			f := sessionAccountRevokePG(t)
			value := targetServiceSeed(t, f, "synthetic-cached", "synthetic-target")
			f.svc.mu.Lock()
			f.svc.sessions[value.Token] = value
			f.svc.mu.Unlock()
			var relation uint32
			if f.ds.Pool().QueryRow(t.Context(), `SELECT 'sessions'::regclass::oid`).Scan(&relation) != nil {
				t.Fatal("relation missing")
			}
			lock, e := f.ds.Pool().Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_ = lock.Rollback(ctx)
			})
			if _, e = lock.Exec(t.Context(), `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if kind == "token" {
					done <- f.svc.RevokeContext(ctx, value.Token)
				} else {
					_, e := f.svc.RevokeAllForClientContext(ctx, value.ClientID)
					done <- e
				}
			}()
			until := time.Now().Add(3 * time.Second)
			for {
				var n int
				if e = f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted`, relation).Scan(&n); e != nil {
					t.Fatal(e)
				}
				if n > 0 {
					break
				}
				if time.Now().After(until) {
					t.Fatal("actual target query never blocked")
				}
				time.Sleep(time.Millisecond)
			}
			if !f.svc.mu.TryLock() {
				t.Fatal("service mutex held during actual PG wait")
			}
			f.svc.mu.Unlock()
			cancel()
			select {
			case e = <-done:
				if !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("canceled SQL did not join")
			}
			var pending int
			if f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted`, relation).Scan(&pending) != nil || pending != 0 {
				t.Fatal("return preceded actual SQL end")
			}
			if f.svc.Count() != 1 {
				t.Fatal("failed operation changed projection")
			}
			if lock.Rollback(t.Context()) != nil || f.count(t, "actor") != 1 {
				t.Fatal("canceled operation persisted")
			}
		})
	}
}

func TestPGTargetRevokeConcurrentProjectionCounts(t *testing.T) {
	f := sessionAccountRevokePG(t)
	targetServiceSeed(t, f, "synthetic-one", "synthetic-target")
	targetServiceSeed(t, f, "synthetic-two", "synthetic-target")
	var count atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, e := f.svc.RevokeAllForClientContext(t.Context(), "synthetic-target")
			if e != nil {
				t.Error(e)
			}
			count.Add(n)
		}()
	}
	wg.Wait()
	if count.Load() != 2 || f.count(t, "actor") != 0 {
		t.Fatal("parallel projection duplicated committed count")
	}
}

// This own Unix proxy discards an actually received server COMMIT command tag.
// Both real socket pumps are closed and joined; no fabricated SQL result exists.
type targetRevokeProxy struct {
	listener            net.Listener
	upstream, directory string
	mu                  sync.Mutex
	closing             bool
	connections         []net.Conn
	wg                  sync.WaitGroup
	once                sync.Once
	armed, dropped      atomic.Bool
}

func targetRevokeProxyStart(t *testing.T, u *url.URL) *targetRevokeProxy {
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
	p := &targetRevokeProxy{listener: listener, directory: dir, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port)}
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
func (p *targetRevokeProxy) forward(client net.Conn) {
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
func (p *targetRevokeProxy) close() {
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

func TestPGTargetRevokeUnknownDoesNotProjectSuccess(t *testing.T) {
	f := sessionAccountRevokePG(t)
	u, e := url.Parse(f.ds.Pool().Config().ConnString())
	if e != nil {
		t.Fatal(e)
	}
	proxy := targetRevokeProxyStart(t, u)
	q := u.Query()
	q.Set("host", proxy.directory)
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	ds, e := datastore.New(t.Context(), u.String())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(ds.Close)
	svc, e := NewServiceWithStore(ds, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	value := targetServiceSeed(t, f, "synthetic-unknown", "synthetic-target")
	svc.mu.Lock()
	svc.sessions[value.Token] = value
	svc.mu.Unlock()
	proxy.armed.Store(true)
	n, e := svc.RevokeAllForClientContext(t.Context(), value.ClientID)
	if n != 0 || !errors.Is(e, datastore.ErrSessionTargetRevokeUnknown) || !proxy.dropped.Load() {
		t.Fatal("lost actual ACK claimed a successful count", e, n)
	}
	if svc.Count() != 1 || f.count(t, "actor") != 0 {
		t.Fatal("unknown ACK projected success or failed to prove actual commit")
	}
	if _, e = svc.ValidateScoped(value.Token, models.SessionScopeJellyfin); e == nil || svc.Count() != 0 {
		t.Fatal("stale projection replaced actual PG validation", e)
	}
}
