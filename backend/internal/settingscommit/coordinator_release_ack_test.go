//go:build linux

package settingscommit

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This wrapper delegates the actual original Unix-socket PostgreSQL protocol.
// It can withhold/drop only an actual unlock response, never invent a SQL ACK.
type releaseACKWire struct {
	mu          sync.Mutex
	connections []*releaseACKConn
	startups    atomic.Int64
	unlocks     atomic.Int64
	hold        atomic.Bool
	drop        atomic.Bool
	response    chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

type releaseACKConn struct {
	net.Conn
	owner     *releaseACKWire
	startup   atomic.Bool
	pending   atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (w *releaseACKWire) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "unix" {
		return nil, ErrCoordinatorRequired
	}
	var dialer net.Dialer
	raw, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn := &releaseACKConn{Conn: raw, owner: w, closed: make(chan struct{})}
	w.mu.Lock()
	w.connections = append(w.connections, conn)
	w.mu.Unlock()
	return conn, nil
}
func (c *releaseACKConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n == len(p) && err == nil {
		if len(p) >= 8 && binary.BigEndian.Uint32(p[4:8]) == 196608 && c.startup.CompareAndSwap(false, true) {
			c.owner.startups.Add(1)
		}
		if len(p) > 5 && p[0] == 'Q' && strings.Contains(strings.ToLower(string(p[5:])), "pg_advisory_unlock(") {
			c.owner.unlocks.Add(1)
			c.pending.Store(true)
		}
	}
	return n, err
}
func (c *releaseACKConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.pending.CompareAndSwap(true, false) {
		c.owner.response <- struct{}{}
		if c.owner.hold.Load() {
			select {
			case <-c.owner.release:
			case <-c.closed:
				return 0, net.ErrClosed
			}
		}
		if c.owner.drop.Load() {
			return 0, io.ErrUnexpectedEOF
		}
	}
	return n, err
}
func (c *releaseACKConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.Conn.Close(); close(c.closed) })
	return c.closeErr
}
func (w *releaseACKWire) unblock() { w.releaseOnce.Do(func() { close(w.release) }) }
func (w *releaseACKWire) shutdown() {
	w.unblock()
	w.mu.Lock()
	all := append([]*releaseACKConn(nil), w.connections...)
	w.mu.Unlock()
	for _, conn := range all {
		_ = conn.Close()
	}
}
func (w *releaseACKWire) originalSocketEnded(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	all := append([]*releaseACKConn(nil), w.connections...)
	w.mu.Unlock()
	seen := 0
	for _, conn := range all {
		if conn.startup.Load() {
			seen++
			select {
			case <-conn.closed:
			default:
				t.Fatal("original SQL socket did not physically close")
			}
		}
	}
	if seen != 1 {
		t.Fatal("original SQL backend was replaced", seen)
	}
}

type releaseACKFixture struct {
	base        *settingsPGFixture
	pool        *pgxpool.Pool
	coordinator *Coordinator
	wire        *releaseACKWire
}

func newReleaseACKFixture(t *testing.T) *releaseACKFixture {
	t.Helper()
	base := settingsPG(t, nil)
	// No original writer exists yet. Bind a genuine source pool with the same
	// synthetic database and a protocol-observing transport before TryWrite.
	if err := base.coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	wire := &releaseACKWire{response: make(chan struct{}, 8), release: make(chan struct{})}
	configuration := base.pool.Config()
	configuration.ConnConfig.DialFunc = wire.dial
	pool, err := pgxpool.NewWithConfig(t.Context(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wire.shutdown(); pool.Close() })
	coordinator, err := NewForPool(t.Context(), pool, base.objects)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wire.unblock(); _ = coordinator.Close() })
	return &releaseACKFixture{base: base, pool: pool, coordinator: coordinator, wire: wire}
}
func (f *releaseACKFixture) guard(t *testing.T, parent context.Context) (*WriteGuard, uint32) {
	t.Helper()
	guard, err := f.coordinator.TryWrite(parent)
	if err != nil {
		t.Fatal("own original writer", err)
	}
	t.Cleanup(func() { f.wire.unblock(); _ = guard.Close() })
	original, finish, err := guard.MutationPool(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	var pid uint32
	if err := original.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if pid == 0 || guard.original == nil || pid != guard.original.PgConn().PID() {
		t.Fatal("original backend identity changed")
	}
	return guard, pid
}
func (f *releaseACKFixture) keyCount(t *testing.T, pid uint32) int {
	t.Helper()
	var count int
	if err := f.base.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=$1 AND classid=$2::oid AND objid=$3::oid AND objsubid=2 AND granted`, pid, Namespace, Resource).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func releaseACKWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual release work did not join")
	}
}
func (f *releaseACKFixture) readyNow(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	hold, err := f.coordinator.TryProgressFileHold(ctx)
	if err != nil {
		t.Fatal("native barrier not immediately ready", err)
	}
	defer hold.Close()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var shared bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock_shared($1::integer,$2::integer)`, Namespace, Resource).Scan(&shared); err != nil || !shared {
		t.Fatal("native release preceded actual fixed-key release", err)
	}
}

func TestSettingsCoordinatorHealthyReleaseACK(t *testing.T) {
	for _, mode := range []string{"idle-original-unlock-before-native-release", "held-unlock-ack-retains-native-barrier", "lost-unlock-ack-stays-unknown", "canceled-parent-owned-cleanup-budget", "concurrent-close-one-original-release", "retired-original-never-reconnects"} {
		t.Run(mode, func(t *testing.T) {
			f := newReleaseACKFixture(t)
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			guard, pid := f.guard(t, parent)
			if f.keyCount(t, pid) != 1 {
				t.Fatal("genuine original did not hold the exact session advisory key")
			}
			switch mode {
			case "idle-original-unlock-before-native-release":
				if err := guard.Close(); err != nil {
					t.Fatal("healthy release", err)
				}
				if f.wire.unlocks.Load() != 1 || f.wire.startups.Load() != 1 || f.keyCount(t, pid) != 0 {
					t.Fatal("healthy original unlock ACK missing or replayed")
				}
				select {
				case <-guard.original.PgConn().CleanupDone():
				default:
					t.Fatal("client protocol cleanup still pending")
				}
				f.wire.originalSocketEnded(t)
				f.readyNow(t)
			case "held-unlock-ack-retains-native-barrier":
				f.wire.hold.Store(true)
				done := make(chan struct{})
				var closeErr error
				t.Cleanup(func() { f.wire.unblock(); releaseACKWait(t, done) })
				go func() { closeErr = guard.Close(); close(done) }()
				releaseACKWait(t, f.wire.response)
				settingsEventually(t, func() bool { return f.keyCount(t, pid) == 0 })
				hold, err := f.coordinator.TryProgressFileHold(t.Context())
				if hold != nil {
					_ = hold.Close()
				}
				if !errors.Is(err, ErrBusy) || hold != nil {
					t.Fatal("native barrier refunded before real unlock ACK", err)
				}
				select {
				case <-done:
					t.Fatal("withheld actual protocol response counted as completion")
				case <-time.After(25 * time.Millisecond):
				}
				f.wire.unblock()
				releaseACKWait(t, done)
				if closeErr != nil || f.wire.unlocks.Load() != 1 {
					t.Fatal("original held ACK not joined", closeErr)
				}
				f.wire.originalSocketEnded(t)
				f.readyNow(t)
			case "lost-unlock-ack-stays-unknown":
				f.wire.drop.Store(true)
				if err := guard.Close(); !errors.Is(err, ErrUnknown) {
					t.Fatal("lost original unlock ACK became known success", err)
				}
				if f.wire.unlocks.Load() != 1 || f.wire.startups.Load() != 1 {
					t.Fatal("unknown original release was replayed")
				}
				f.wire.originalSocketEnded(t)
				settingsEventually(t, func() bool { return f.keyCount(t, pid) == 0 })
				// Observer data cannot change the unknown client outcome.
				if err := guard.Close(); !errors.Is(err, ErrUnknown) {
					t.Fatal("observer data rewrote unknown close result", err)
				}
			case "canceled-parent-owned-cleanup-budget":
				cancel()
				if guard.Context().Err() == nil || guard.Current(f.coordinator) == nil {
					t.Fatal("original canceled authority remained current")
				}
				if _, finish, err := guard.MutationPool(f.pool); err == nil {
					if finish != nil {
						finish()
					}
					t.Fatal("cleanup context renewed normal mutation")
				}
				if err := guard.Close(); err != nil {
					t.Fatal("owned terminal cleanup ignored original healthy backend", err)
				}
				if guard.Context().Err() == nil || f.wire.unlocks.Load() != 1 || f.wire.startups.Load() != 1 {
					t.Fatal("terminal cleanup renewed/replaced original")
				}
				f.wire.originalSocketEnded(t)
				f.readyNow(t)
			case "concurrent-close-one-original-release":
				done := make(chan struct{})
				results := make(chan error, 4)
				var workers sync.WaitGroup
				workers.Add(4)
				t.Cleanup(func() { f.wire.unblock(); releaseACKWait(t, done) })
				for i := 0; i < 4; i++ {
					go func() { defer workers.Done(); results <- guard.Close() }()
				}
				go func() { workers.Wait(); close(results); close(done) }()
				releaseACKWait(t, done)
				for err := range results {
					if err != nil {
						t.Fatal("concurrent original close", err)
					}
				}
				if f.wire.unlocks.Load() != 1 || f.wire.startups.Load() != 1 {
					t.Fatal("concurrent release duplicated original SQL/backend")
				}
				f.wire.originalSocketEnded(t)
				f.readyNow(t)
			case "retired-original-never-reconnects":
				var terminated bool
				if err := f.base.admin.QueryRow(t.Context(), "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
					t.Fatal("terminate own actual original backend", err)
				}
				settingsEventually(t, func() bool {
					var gone bool
					err := f.base.admin.QueryRow(t.Context(), `SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=$2)`, pid, f.base.name).Scan(&gone)
					return err == nil && gone
				})
				if err := guard.Close(); err != nil && !errors.Is(err, ErrUnknown) {
					t.Fatal("retired original cleanup classification", err)
				}
				if f.wire.startups.Load() != 1 || f.wire.unlocks.Load() > 1 {
					t.Fatal("retired original was replaced or replayed")
				}
				f.wire.originalSocketEnded(t)
			}
		})
	}
}
