//go:build linux || darwin || windows

package settingscommit

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type settingsPGFixture struct {
	pool, admin *pgxpool.Pool
	objects     *LockObjects
	coordinator *Coordinator
	raw, name   string
}

func settingsPG(t *testing.T, fault *settingsLostACK) *settingsPGFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("a synthetic Unix-socket database is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("open fixture observer")
	}
	name := fmt.Sprintf("synthetic_settings_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own fixture database")
	}
	var pool *pgxpool.Pool
	var objects *LockObjects
	var c *Coordinator
	t.Cleanup(func() {
		if fault != nil {
			fault.releaseAll()
		}
		if c != nil {
			_ = c.Close()
		}
		if objects != nil {
			_ = objects.Close()
		}
		if pool != nil {
			pool.Close()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
		admin.Close()
	})
	u.Path = "/" + name
	target := u.String()
	cfg, err := pgxpool.ParseConfig(target)
	if err != nil {
		t.Fatal("parse own fixture")
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	if fault != nil {
		cfg.ConnConfig.DialFunc = fault.dial
	}
	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("open own fixture pool")
	}
	objects, err = OpenLockObjects(ctx, filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatal("open original native objects", err)
	}
	c, err = NewForPool(ctx, pool, objects)
	if err != nil {
		t.Fatal("bind original store", err)
	}
	return &settingsPGFixture{pool, admin, objects, c, target, name}
}
func settingsEventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual synthetic lifetime witness did not arrive")
}
func settingsGuard(t *testing.T, f *settingsPGFixture) *WriteGuard {
	t.Helper()
	g, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal("acquire actual mutation guard", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestSettingsCoordinatorOriginalSessionAndRuntimePoolRemainDistinct(t *testing.T) {
	f := settingsPG(t, nil)
	g := settingsGuard(t, f)
	pool, finish, err := g.MutationPool(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if pool == f.pool || pool.Config().MaxConns != 1 || f.pool.Config().MaxConns != 1 {
		t.Fatal("guard replaced or enlarged original runtime pool")
	}
	var original, again, runtime uint32
	if pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&original) != nil || pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&again) != nil || f.pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&runtime) != nil {
		t.Fatal("original pool1 was starved by its independent guard")
	}
	if original != again || original == runtime {
		t.Fatal("mutation backend was replaced or borrowed from runtime")
	}
	var locked bool
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, Namespace, Resource).Scan(&locked) != nil || locked {
		t.Fatal("actual mutation backend did not retain conflicting session key")
	}
	if _, _, err = g.MutationPool(f.pool); err == nil {
		t.Fatal("same source scope was claimed twice")
	}
}
func TestSettingsCoordinatorSharedTransactionMakesMutationBusy(t *testing.T) {
	f := settingsPG(t, nil)
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, Namespace, Resource).Scan(&locked) != nil || !locked {
		t.Fatal("hold actual original shared TX")
	}
	if g, err := f.coordinator.TryWrite(t.Context()); !errors.Is(err, ErrBusy) {
		if g != nil {
			g.Close()
		}
		t.Fatal("actual shared TX did not exclude mutation")
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	g := settingsGuard(t, f)
	if g.Current(f.coordinator) != nil {
		t.Fatal("original ended TX prevented fresh mutation")
	}
}
func TestSettingsCoordinatorCopyForeignZeroAndOpaqueValues(t *testing.T) {
	f := settingsPG(t, nil)
	g := settingsGuard(t, f)
	copied := *g
	copyCoordinator := *f.coordinator
	if copied.Current(f.coordinator) == nil || copied.Close() == nil || copyCoordinator.MatchesPool(f.pool) || (&WriteGuard{}).Current(f.coordinator) == nil || (&Coordinator{}).MatchesPool(f.pool) {
		t.Fatal("copied/zero owner gained original lifetime")
	}
	foreign, err := pgxpool.New(t.Context(), f.raw)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if _, _, err = g.MutationPool(foreign); err == nil {
		t.Fatal("different concrete store gained original pool")
	}
	for _, v := range []any{g, *g, f.coordinator, *f.coordinator, f.objects, *f.objects} {
		raw, _ := json.Marshal(v)
		if string(raw) != "null" || !strings.Contains(fmt.Sprintf("%#v", v), "opaque") {
			t.Fatal("native owner representation was not opaque")
		}
	}
	if g.Current(f.coordinator) != nil {
		t.Fatal("rejected copy retired original")
	}
}
func TestSettingsCoordinatorOriginalBackendLossCannotReconnect(t *testing.T) {
	f := settingsPG(t, nil)
	g := settingsGuard(t, f)
	pool, finish, err := g.MutationPool(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	var pid uint32
	if pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid) != nil {
		t.Fatal("read own actual backend")
	}
	if _, err = f.admin.Exec(t.Context(), "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal("terminate own fixture backend")
	}
	if _, err = pool.Exec(t.Context(), "SELECT 1"); err == nil {
		t.Fatal("terminated original backend remained usable")
	}
	if _, err = pool.Exec(t.Context(), "SELECT 1"); err == nil {
		t.Fatal("guard reconnected after original backend loss")
	}
	if g.Current(f.coordinator) == nil {
		t.Fatal("lost original backend kept mutation ready")
	}
}

// The fault drops only the actual COMMIT read/ACK. The real server connection
// remains open until this fixture explicitly releases it. No provider is used.
type settingsLostACK struct {
	mu          sync.Mutex
	connections []*settingsACKConn
	armed       atomic.Bool
	dropped     atomic.Bool
}
type settingsACKConn struct {
	net.Conn
	owner *settingsLostACK
	hold  atomic.Bool
}

func (f *settingsLostACK) dial(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	wrapped := &settingsACKConn{Conn: c, owner: f}
	f.mu.Lock()
	f.connections = append(f.connections, wrapped)
	f.mu.Unlock()
	return wrapped, nil
}
func (c *settingsACKConn) Write(p []byte) (int, error) {
	if len(p) >= 8 && binary.BigEndian.Uint32(p[4:8]) == 80877102 && c.owner.dropped.Load() {
		return 0, io.ErrClosedPipe
	}
	if c.hold.Load() {
		return len(p), nil
	}
	if len(p) > 5 && p[0] == 'Q' && strings.EqualFold(strings.TrimRight(string(p[5:]), "\x00"), "commit") && c.owner.armed.CompareAndSwap(true, false) {
		c.hold.Store(true)
	}
	return c.Conn.Write(p)
}
func (c *settingsACKConn) Read(p []byte) (int, error) {
	if c.hold.Load() {
		c.owner.dropped.Store(true)
		return 0, io.ErrUnexpectedEOF
	}
	return c.Conn.Read(p)
}
func (c *settingsACKConn) Close() error {
	if c.hold.Load() {
		return nil
	}
	return c.Conn.Close()
}
func (f *settingsLostACK) releaseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.connections {
		_ = c.Conn.Close()
	}
}
func TestSettingsCoordinatorLostCommitACKStillExcludesRestartedWriter(t *testing.T) {
	fault := &settingsLostACK{}
	f := settingsPG(t, fault)
	if _, err := f.pool.Exec(t.Context(), `CREATE TABLE synthetic_ack(id int);CREATE FUNCTION synthetic_hold_commit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(7159218);RETURN NEW;END$$;CREATE CONSTRAINT TRIGGER synthetic_ack_hold AFTER INSERT ON synthetic_ack DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION synthetic_hold_commit()`); err != nil {
		t.Fatal("create actual deferred COMMIT fixture", err)
	}
	blocker, err := f.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	if _, err = blocker.Exec(t.Context(), `SELECT pg_advisory_lock(7159218)`); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_, _ = blocker.Exec(context.Background(), `SELECT pg_advisory_unlock(7159218)`)
		}
	}
	defer release()
	t.Cleanup(release)
	g := settingsGuard(t, f)
	pool, finish, err := g.MutationPool(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	doneScope := false
	finishScope := func() {
		if !doneScope {
			doneScope = true
			finish()
		}
	}
	defer finishScope()
	t.Cleanup(finishScope)
	connection, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var pid uint32
	if connection.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid) != nil {
		t.Fatal("read exact mutation backend")
	}
	tx, err := connection.Begin(t.Context())
	if err != nil {
		connection.Release()
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `INSERT INTO synthetic_ack VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	fault.armed.Store(true)
	if err = tx.Commit(t.Context()); err == nil || !fault.dropped.Load() {
		t.Fatal("actual COMMIT ACK was not lost")
	}
	connection.Release()
	finishScope()
	settingsEventually(t, func() bool {
		var pending bool
		_ = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=$2 AND lower(query)='commit' AND wait_event='advisory')`, pid, f.name).Scan(&pending)
		return pending
	})
	if err = g.Close(); err != nil {
		t.Fatal("join actual failed client transport", err)
	}
	// A fresh coordinator/process scope uses immutable native objects plus the
	// same fixed PG key, not the previous client's local CleanupDone or PID.
	fresh, err := NewForPool(t.Context(), f.pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if next, err := fresh.TryWrite(t.Context()); !errors.Is(err, ErrBusy) {
		if next != nil {
			next.Close()
		}
		t.Fatal("lost ACK was treated as server transaction end")
	}
	release()
	settingsEventually(t, func() bool {
		var pending bool
		_ = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=$2 AND state='idle')`, pid, f.name).Scan(&pending)
		return pending
	})
	fault.releaseAll()
	settingsEventually(t, func() bool {
		next, err := fresh.TryWrite(t.Context())
		if err != nil {
			return false
		}
		_ = next.Close()
		return true
	})
	// No SELECT of the restored row is used to recast the unknown business ACK.
}

func TestSettingsCoordinatorShutdownJoinsMutationTurnWaiter(t *testing.T) {
	f := settingsPG(t, nil)
	first := settingsGuard(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(entered)
		g, err := f.coordinator.TryWrite(ctx)
		if g != nil {
			_ = g.Close()
		}
		waiterDone <- err
	}()
	<-entered
	settingsEventually(t, func() bool {
		f.coordinator.mu.Lock()
		defer f.coordinator.mu.Unlock()
		return f.coordinator.pendingWrites == 1
	})
	closeDone := make(chan error, 1)
	go func() { closeDone <- f.coordinator.Close() }()
	cancel()
	select {
	case <-closeDone:
		t.Fatal("coordinator closed witnesses while native waiter/owner remained")
	case <-time.After(30 * time.Millisecond):
	}
	_ = first.Close()
	select {
	case err := <-waiterDone:
		if err == nil {
			t.Fatal("canceled pending writer became current")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native waiter did not actually return")
	}
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not join actual waiter")
	}
}

func TestSettingsNativeMetadataUnknownCannotBecomeLegacy(t *testing.T) {
	for _, mode := range []string{"missing-state", "unknown-state", "wrong-family", "replaced-object"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "settings.json")
			o, err := OpenLockObjects(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			state := filepath.Join(o.directory, "state.json")
			switch mode {
			case "missing-state":
				err = os.Remove(state)
			case "unknown-state":
				err = os.WriteFile(state, []byte(`{"version":1,"family":"unknown","mode":"unarmed"}`), 0600)
			case "wrong-family":
				err = os.WriteFile(state, []byte(`{"version":1,"family":"other","mode":"armed","source":"x"}`), 0600)
			case "replaced-object":
				err = os.Rename(filepath.Join(o.directory, "commit-barrier.lock"), filepath.Join(o.directory, "retired.lock"))
				if runtime.GOOS == "windows" && err != nil {
					// Windows nativeOpen intentionally omits FILE_SHARE_DELETE. The
					// expected sharing/access denial preserves this exact original object.
					if !errors.Is(err, syscall.Errno(32)) && !errors.Is(err, syscall.Errno(5)) {
						t.Fatal("unexpected Windows rename error")
					}
					if o.check() != nil {
						t.Fatal("denied Windows replacement changed original witnesses")
					}
					if _, statErr := os.Stat(filepath.Join(o.directory, "retired.lock")); !os.IsNotExist(statErr) {
						t.Fatal("denied Windows replacement published another name")
					}
					hold, holdErr := o.acquire(t.Context(), false)
					if holdErr != nil {
						t.Fatal(holdErr)
					}
					defer hold.close()
					if writer, writeErr := TryUnarmedWrite(t.Context(), o); !errors.Is(writeErr, ErrBusy) {
						if writer != nil {
							writer.Close()
						}
						t.Fatal("unchanged Windows lock lost actual shared/exclusive denial")
					}
					return
				}
				if err == nil {
					err = os.WriteFile(filepath.Join(o.directory, "commit-barrier.lock"), nil, 0600)
				}
			}
			if err != nil {
				t.Fatal("alter own synthetic metadata")
			}
			if g, err := TryUnarmedWrite(t.Context(), o); err == nil {
				g.Close()
				t.Fatal("unknown/renamed original root became free legacy")
			}
		})
	}
}
func TestSettingsNativeIndependentHoldsAndSuccessfulWriterSerialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	o, err := OpenLockObjects(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	one, err := o.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer one.close()
	two, err := o.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer two.close()
	_ = one.close()
	if w, err := o.acquire(t.Context(), true); !errors.Is(err, ErrBusy) {
		if w != nil {
			w.close()
		}
		t.Fatal("closing one independent reader released another")
	}
	_ = two.close()
	writer, err := TryUnarmedWrite(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	done := make(chan error, 1)
	go func() {
		next, err := TryUnarmedWrite(t.Context(), o)
		if next != nil {
			_ = next.Close()
		}
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("second writer skipped the original mutation turn")
	case <-time.After(30 * time.Millisecond):
	}
	_ = writer.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("ordinary writer serialization became Busy", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second native writer never completed")
	}
}

type settingsConnectClose struct {
	net.Conn
	mode      string
	cancel    context.CancelFunc
	pending   []byte
	triggered bool
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (c *settingsConnectClose) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.triggered || n == 0 {
		return n, err
	}
	if c.mode == "handshake" {
		c.triggered = true
		c.cancel()
		return 0, io.ErrUnexpectedEOF
	}
	c.pending = append(c.pending, p[:n]...)
	for len(c.pending) >= 5 {
		size := int(binary.BigEndian.Uint32(c.pending[1:5]))
		if size < 4 || size > 65536 {
			c.pending = nil
			break
		}
		if len(c.pending) < size+1 {
			break
		}
		ready := c.pending[0] == 'Z'
		c.pending = c.pending[size+1:]
		if ready {
			c.triggered = true
			c.cancel()
			break
		}
	}
	return n, err
}
func (c *settingsConnectClose) Close() error {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.Conn.Close()
}
func TestSettingsCoordinatorFailedConnectJoinsOriginalTransport(t *testing.T) {
	for _, mode := range []string{"handshake", "after-connect"} {
		t.Run(mode, func(t *testing.T) {
			f := settingsPG(t, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			cfg := f.pool.Config().Copy()
			dial := cfg.ConnConfig.DialFunc
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				actual, err := dial(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &settingsConnectClose{Conn: actual, mode: mode, cancel: cancel, entered: entered, release: release}, nil
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			c, err := NewForPool(t.Context(), pool, f.objects)
			if err != nil {
				t.Fatal(err)
			}
			// All release/done cleanup is installed before the actual connect starts.
			done := make(chan error, 1)
			defer func() {
				unblock()
				select {
				case err := <-done:
					if err == nil {
						t.Error("canceled original connect became current")
					}
				case <-time.After(5 * time.Second):
					t.Error("failed connect did not join")
				}
				_ = c.Close()
			}()
			go func() {
				g, err := c.TryWrite(ctx)
				if g != nil {
					g.Close()
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real connected transport Close was not entered")
			}
			select {
			case err := <-done:
				done <- err
				t.Fatal("failed constructor refunded before actual socket Close ended")
			default:
			}
			c.mu.Lock()
			var owner *WriteGuard
			for current := range c.owners {
				owner = current
			}
			c.mu.Unlock()
			if owner == nil {
				t.Fatal("late connect lost original charged owner")
			}
			if mode == "after-connect" {
				settingsEventually(t, func() bool { owner.mu.Lock(); defer owner.mu.Unlock(); return owner.original != nil })
			}
			owner.mu.Lock()
			original := owner.original
			retained := len(owner.transports)
			owner.mu.Unlock()
			if retained != 1 || mode == "after-connect" && original == nil || mode == "handshake" && original != nil {
				t.Fatal("real handshake/AfterConnect cleanup ownership was not preserved")
			}
			if hold, err := f.objects.acquire(t.Context(), false); err == nil {
				hold.close()
				t.Fatal("native admission was refunded while real failed Close remained held")
			}
			unblock()
		})
	}
}

func TestSettingsRuntimeConstructorErrorJoinsActualSocketAndAllCloseCallers(t *testing.T) {
	for _, mode := range []string{"handshake", "after-connect"} {
		t.Run(mode, func(t *testing.T) {
			f := settingsPG(t, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			cfg := f.pool.Config().Copy()
			dial := cfg.ConnConfig.DialFunc
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				connection, err := dial(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &settingsConnectClose{Conn: connection, mode: mode, cancel: cancel, entered: entered, release: release}, nil
			}
			owner, err := OpenRuntimePool(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 2)
			defer func() {
				unblock()
				owner.Close()
				for i := 0; i < 2; i++ {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("runtime constructor cleanup did not join")
					}
				}
			}()
			go func() { err := owner.StoragePool().Ping(ctx); owner.Close(); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("actual startup socket Close did not enter")
			}
			go func() { done <- owner.Close() }()
			select {
			case err := <-done:
				done <- err
				t.Fatal("runtime constructor/concurrent Close returned before actual socket Close")
			case <-time.After(30 * time.Millisecond):
			}
			if mode == "after-connect" {
				settingsEventually(t, func() bool { owner.mu.Lock(); defer owner.mu.Unlock(); return len(owner.connections) == 1 })
			}
			owner.mu.Lock()
			retained := len(owner.transports)
			owner.mu.Unlock()
			if retained != 1 {
				t.Fatal("failed startup transport lost charged ownership")
			}
			unblock()
		})
	}
}

func TestSettingsCoordinatorDefaultTLSPlainFallbackAndNoOriginalReplay(t *testing.T) {
	f := settingsPG(t, nil)
	upstream, err := pgxpool.ParseConfig(f.raw)
	if err != nil {
		t.Fatal("parse own upstream")
	}
	socket := filepath.Join(upstream.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", upstream.ConnConfig.Port))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("open own literal loopback proxy")
	}
	if address, ok := listener.Addr().(*net.TCPAddr); !ok || !address.IP.IsLoopback() {
		listener.Close()
		t.Fatal("proxy must be literal loopback")
	}
	var refusals, authentications atomic.Int32
	var workers sync.WaitGroup
	var connections sync.Map
	acceptDone := make(chan struct{})
	t.Cleanup(func() {
		listener.Close()
		<-acceptDone
		connections.Range(func(key, value any) bool { _ = key.(net.Conn).Close(); return true })
		workers.Wait()
	})
	go func() {
		defer close(acceptDone)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(connection, struct{}{})
			workers.Add(1)
			go func(client net.Conn) {
				defer workers.Done()
				defer connections.Delete(client)
				defer client.Close()
				header := make([]byte, 8)
				if _, err := io.ReadFull(client, header); err != nil {
					return
				}
				if binary.BigEndian.Uint32(header[:4]) == 8 && binary.BigEndian.Uint32(header[4:8]) == 80877103 {
					refusals.Add(1)
					_, _ = client.Write([]byte{'N'})
					return
				}
				if binary.BigEndian.Uint32(header[4:8]) == 196608 {
					authentications.Add(1)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				var dialer net.Dialer
				server, err := dialer.DialContext(ctx, "unix", socket)
				cancel()
				if err != nil {
					return
				}
				connections.Store(server, struct{}{})
				defer connections.Delete(server)
				defer server.Close()
				if _, err = server.Write(header); err != nil {
					return
				}
				copied := make(chan struct{}, 1)
				go func() { _, _ = io.Copy(server, client); server.Close(); copied <- struct{}{} }()
				_, _ = io.Copy(client, server)
				client.Close()
				server.Close()
				<-copied
			}(connection)
		}
	}()
	u, err := url.Parse(f.raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = listener.Addr().String()
	q := u.Query()
	q.Del("host")
	q.Del("port")
	q.Set("sslmode", "prefer")
	u.RawQuery = q.Encode()
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal("parse own qualified TLS fallback")
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	c, err := NewForPool(t.Context(), pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	guard, err := c.TryWrite(t.Context())
	if err != nil {
		t.Fatal("ordinary sslmode=prefer could not negotiate local server without TLS", err)
	}
	t.Cleanup(func() { guard.Close() })
	mutation, finish, err := guard.MutationPool(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	var pid uint32
	if mutation.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid) != nil {
		t.Fatal("qualified fallback never produced actual original SQL backend")
	}
	if refusals.Load() != 1 || authentications.Load() != 1 {
		t.Fatal("pre-auth fallback did not remain one TLS refusal plus one real SQL handshake")
	}
	guard.mu.Lock()
	attempts := guard.dialAttempts
	guard.mu.Unlock()
	if attempts != 2 {
		t.Fatal("bounded TLS/plain handshake lineage changed")
	}
	if _, err = f.admin.Exec(t.Context(), "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal("terminate own original qualified backend")
	}
	if _, err = mutation.Exec(t.Context(), "SELECT 1"); err == nil {
		t.Fatal("lost original backend remained usable")
	}
	if _, err = mutation.Exec(t.Context(), "SELECT 1"); err == nil {
		t.Fatal("qualified fallback became post-auth reconnect")
	}
	if authentications.Load() != 1 || refusals.Load() != 1 {
		t.Fatal("post-auth backend loss opened another SQL handshake")
	}
}

func TestSettingsNativeAtomicInitializationAcrossConcurrentFirstUsers(t *testing.T) {
	for round := 0; round < 32; round++ {
		directory := t.TempDir()
		path := filepath.Join(directory, "settings.json")
		start := make(chan struct{})
		type initialized struct {
			objects *LockObjects
			err     error
		}
		result := make(chan initialized, 2)
		for worker := 0; worker < 2; worker++ {
			go func() {
				<-start
				objects, err := OpenLockObjects(t.Context(), path)
				if err == nil {
					var guard *UnarmedWriteGuard
					guard, err = TryUnarmedWrite(t.Context(), objects)
					if guard != nil {
						if currentErr := guard.Current(); err == nil {
							err = currentErr
						}
						if closeErr := guard.Close(); err == nil {
							err = closeErr
						}
					}
				}
				result <- initialized{objects, err}
			}()
		}
		close(start)
		values := make([]initialized, 0, 2)
		// Join both real first users before inspecting outcomes or failing, so a
		// regression cannot strand another charged native caller during cleanup.
		for worker := 0; worker < 2; worker++ {
			values = append(values, <-result)
		}
		var first os.FileInfo
		var failed bool
		for _, value := range values {
			if value.err != nil {
				failed = true
			}
			if value.objects == nil {
				failed = true
				continue
			}
			info, err := os.Lstat(value.objects.directory)
			if err != nil || value.objects.check() != nil {
				failed = true
			} else if first == nil {
				first = info
			} else if !os.SameFile(first, info) {
				failed = true
			}
			if record, err := value.objects.readRecord(); err != nil || record.Mode != "unarmed" {
				failed = true
			}
			if err = value.objects.Close(); err != nil {
				failed = true
			}
		}
		if failed {
			t.Fatal("concurrent first users observed an incomplete or replaced guard root")
		}
		entries, err := os.ReadDir(directory)
		if err != nil || len(entries) != 1 || entries[0].Name() != directoryName {
			t.Fatal("completed initializers leaked private staging or changed final root")
		}
	}
}
