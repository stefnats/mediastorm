// Package settingscommit owns only native operation locks and a fixed PG lock
// protocol. It has no account, profile, PIN, settings-schema or model authority.
package settingscommit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Namespace int32 = 0x4d535343
const Resource int32 = 1
const directoryName = ".mediastorm-settings-commit"

var objectsAdmission = make(chan struct{}, 64)
var fileAdmission = make(chan struct{}, 64)

var (
	ErrBusy                = errors.New("settings commit is busy")
	ErrCapacity            = errors.New("settings coordination capacity exhausted")
	ErrMetadata            = errors.New("settings guard metadata unavailable")
	ErrUnknown             = errors.New("settings guard state unknown")
	ErrCoordinatorRequired = errors.New("settings coordinator required")
	ErrDomain              = errors.New("settings domain transition requires quiescence")
	ErrUnsupported         = errors.New("settings native coordination unsupported")
)

type record struct {
	Version int    `json:"version"`
	Family  string `json:"family"`
	Mode    string `json:"mode"`
	Source  string `json:"source,omitempty"`
}

// LockObjects retains the identity of the two immutable files. Normal close
// never removes them. Every operation opens a separate native description.
type LockObjects struct{ *lockObjectsState }
type lockObjectsState struct {
	self          *LockObjects
	directory     string
	info          os.FileInfo
	turn, barrier *os.File
	mu            sync.Mutex
	closed        bool
}

func (LockObjects) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("LockObjects(opaque)")) }
func (LockObjects) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func OpenLockObjects(ctx context.Context, configPath string) (*LockObjects, error) {
	if ctx == nil || ctx.Err() != nil || configPath == "" || len(configPath) > 4096 {
		return nil, ErrMetadata
	}
	select {
	case objectsAdmission <- struct{}{}:
	default:
		return nil, ErrCapacity
	}
	published := false
	defer func() {
		if !published {
			<-objectsAdmission
		}
	}()
	parent := filepath.Dir(configPath)
	absolute, err := filepath.Abs(parent)
	if err != nil {
		return nil, ErrMetadata
	}
	// Resolve only the existing parent for aliases. Never rewrite the actual
	// Manager write path: its raw trailing slash/dot and EnsureDir semantics stay.
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, ErrMetadata
	}
	path := filepath.Join(absolute, directoryName)
	if _, statErr := os.Lstat(path); os.IsNotExist(statErr) {
		if err = initializeLockDirectory(ctx, absolute, path); err != nil {
			return nil, err
		}
	} else if statErr != nil {
		return nil, ErrMetadata
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnknown
	}
	out := &LockObjects{lockObjectsState: &lockObjectsState{directory: path, info: info}}
	out.self = out
	fail := func(e error) (*LockObjects, error) {
		if out.turn != nil {
			_ = out.turn.Close()
		}
		if out.barrier != nil {
			_ = out.barrier.Close()
		}
		return nil, e
	}
	out.turn, err = nativeOpen(filepath.Join(path, "mutation-turn.lock"), false)
	if err != nil {
		return fail(ErrUnknown)
	}
	out.barrier, err = nativeOpen(filepath.Join(path, "commit-barrier.lock"), false)
	if err != nil {
		return fail(ErrUnknown)
	}
	if _, err = out.readRecord(); err != nil {
		return fail(err)
	}
	published = true
	return out, nil
}

// initializeLockDirectory exposes only a completely initialized positive
// root. Concurrent first callers can never observe our private partial files.
// Existing incomplete/foreign roots are not repaired or replaced.
func initializeLockDirectory(ctx context.Context, parent, destination string) error {
	stage, err := os.MkdirTemp(parent, directoryName+".initial-")
	if err != nil {
		return ErrMetadata
	}
	info, err := os.Lstat(stage)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnknown
	}
	owned := true
	defer func() {
		if owned {
			named, statErr := os.Lstat(stage)
			if statErr == nil && os.SameFile(info, named) {
				_ = os.RemoveAll(stage)
			}
		}
	}()
	objects := &LockObjects{lockObjectsState: &lockObjectsState{directory: stage, info: info}}
	objects.self = objects
	closeFiles := func() error {
		var result error
		if objects.turn != nil {
			result = objects.turn.Close()
			objects.turn = nil
		}
		if objects.barrier != nil {
			if err := objects.barrier.Close(); result == nil {
				result = err
			}
			objects.barrier = nil
		}
		return result
	}
	defer closeFiles()
	objects.turn, err = nativeOpen(filepath.Join(stage, "mutation-turn.lock"), true)
	if err != nil {
		return ErrMetadata
	}
	objects.barrier, err = nativeOpen(filepath.Join(stage, "commit-barrier.lock"), true)
	if err != nil {
		return ErrMetadata
	}
	if err = objects.writeRecord(record{Version: 1, Family: nativeFamily, Mode: "unarmed"}); err != nil {
		return err
	}
	if err = objects.turn.Sync(); err != nil {
		return ErrMetadata
	}
	if err = objects.barrier.Sync(); err != nil {
		return ErrMetadata
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Windows deliberately denies deletion/rename with these construction
	// handles open. Close every real handle before publishing the directory.
	if err = closeFiles(); err != nil {
		return ErrMetadata
	}
	named, err := os.Lstat(stage)
	if err != nil || !os.SameFile(info, named) {
		return ErrUnknown
	}
	if err = nativePublishDirectory(stage, destination); err != nil {
		winner, statErr := os.Lstat(destination)
		if statErr != nil || !winner.IsDir() || winner.Mode()&os.ModeSymlink != 0 {
			return ErrMetadata
		}
		// The caller opens and fully validates this winner. No partial marker,
		// unknown directory, or losing initializer is promoted into readiness.
		return nil
	}
	owned = false
	// Keep the historic best-effort directory flush contract. In particular,
	// Windows may reject directory Sync while its write-through move and all
	// individually synced/closed construction files remain valid basic writes.
	directory, err := os.Open(parent)
	if err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}

	return nil
}

func (o *LockObjects) valid() bool {
	return o != nil && o.lockObjectsState != nil && o.self == o && o.turn != nil && o.barrier != nil
}
func (o *LockObjects) check() error {
	if !o.valid() {
		return ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrUnknown
	}
	info, err := os.Lstat(o.directory)
	if err != nil || !info.IsDir() || !os.SameFile(info, o.info) {
		return ErrUnknown
	}
	for _, pair := range []struct {
		name string
		file *os.File
	}{{"mutation-turn.lock", o.turn}, {"commit-barrier.lock", o.barrier}} {
		held, e := pair.file.Stat()
		named, n := os.Lstat(filepath.Join(o.directory, pair.name))
		if e != nil || n != nil || !held.Mode().IsRegular() || !named.Mode().IsRegular() || !os.SameFile(held, named) {
			return ErrUnknown
		}
	}
	return nil
}
func (o *LockObjects) readRecord() (record, error) {
	var value record
	if err := o.check(); err != nil {
		return value, err
	}
	file, err := OpenRegularGuardData(filepath.Join(o.directory, "state.json"))
	if err != nil {
		return value, ErrUnknown
	}
	defer file.Close()
	info, err := file.Stat()
	named, e := os.Lstat(file.Name())
	if err != nil || e != nil || !info.Mode().IsRegular() || !os.SameFile(info, named) || info.Size() > 2048 {
		return value, ErrUnknown
	}
	decoder := json.NewDecoder(io.LimitReader(file, 2049))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil {
		return value, ErrUnknown
	}
	var tail any
	if decoder.Decode(&tail) != io.EOF || value.Version != 1 || value.Family != nativeFamily || (value.Mode != "unarmed" && value.Mode != "armed") || value.Mode == "armed" && len(value.Source) != 64 {
		return value, ErrUnknown
	}
	return value, nil
}
func (o *LockObjects) writeRecord(value record) error {
	if err := o.check(); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return ErrMetadata
	}
	name := filepath.Join(o.directory, "state.pending")
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrMetadata
	}
	defer os.Remove(name)
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return ErrMetadata
	}
	if os.Rename(name, filepath.Join(o.directory, "state.json")) != nil {
		return ErrMetadata
	}
	parent, e := os.Open(o.directory)
	if e == nil {
		_ = parent.Sync()
		_ = parent.Close()
	}
	return nil
}
func (o *LockObjects) Protected(path string) bool {
	if !o.valid() {
		return true
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return true
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return false
	}
	full := filepath.Join(parent, filepath.Base(absolute))
	return full == o.directory || strings.HasPrefix(full, o.directory+string(filepath.Separator))
}
func (o *LockObjects) Close() error {
	if !o.valid() {
		return ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	defer func() { <-objectsAdmission }()
	_ = o.turn.Close()
	return o.barrier.Close()
}

type fileHold struct {
	objects       *LockObjects
	turn, barrier *os.File
	once          sync.Once
	err           error
	charged       bool
}

func (o *LockObjects) acquire(ctx context.Context, writer bool) (*fileHold, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrBusy
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	select {
	case fileAdmission <- struct{}{}:
	default:
		return nil, ErrCapacity
	}
	hold := &fileHold{objects: o, charged: true}
	fail := func(e error) (*fileHold, error) { _ = hold.close(); return nil, e }
	var err error
	if writer {
		hold.turn, err = nativeOpen(filepath.Join(o.directory, "mutation-turn.lock"), false)
		if err != nil {
			return fail(ErrMetadata)
		}
		if err = nativeLock(hold.turn, false, false); err != nil {
			return fail(err)
		}
	}
	hold.barrier, err = nativeOpen(filepath.Join(o.directory, "commit-barrier.lock"), false)
	if err != nil {
		return fail(ErrMetadata)
	}
	if err = nativeLock(hold.barrier, !writer, true); err != nil {
		if nativeConflict(err) {
			return fail(ErrBusy)
		}
		return fail(err)
	}
	for _, pair := range []struct{ held, witness *os.File }{{hold.barrier, o.barrier}, {hold.turn, o.turn}} {
		if pair.held != nil {
			a, e := pair.held.Stat()
			b, n := pair.witness.Stat()
			if e != nil || n != nil || !os.SameFile(a, b) {
				return fail(ErrUnknown)
			}
		}
	}
	if err = o.check(); err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		return fail(err)
	}
	return hold, nil
}
func (h *fileHold) close() error {
	if h == nil {
		return nil
	}
	h.once.Do(func() {
		if h.charged {
			defer func() { <-fileAdmission }()
		}
		if h.barrier != nil {
			_ = nativeUnlock(h.barrier)
			h.err = h.barrier.Close()
		}
		if h.turn != nil {
			_ = nativeUnlock(h.turn)
			if err := h.turn.Close(); h.err == nil {
				h.err = err
			}
		}
	})
	return h.err
}

// ProgressFileHold is a file-side lifetime only, never SQL/auth permission.
type ProgressFileHold struct {
	self   *ProgressFileHold
	owner  *Coordinator
	file   *fileHold
	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (h *ProgressFileHold) Context() context.Context {
	if h == nil || h.self != h {
		return nil
	}
	return h.ctx
}
func (h *ProgressFileHold) Current(c *Coordinator) error {
	if h == nil || h.self != h || h.owner != c {
		return ErrUnknown
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.ctx.Err() != nil {
		return ErrUnknown
	}
	return h.file.objects.check()
}
func (h *ProgressFileHold) Close() error {
	if h == nil || h.self != h {
		return ErrUnknown
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	h.cancel()
	err := h.file.close()
	h.owner.mu.Lock()
	delete(h.owner.holds, h)
	h.owner.mu.Unlock()
	close(h.done)
	return err
}

// Coordinator binds a dedicated bounded guard pool to the original real pool.
// A source digest detects replacement; only the actual fixed-key PG lock proves
// end of original transactions in the supported unchanged deployment.
type Coordinator struct{ *coordinatorState }
type coordinatorState struct {
	self          *Coordinator
	pool          *pgxpool.Pool
	configuration *pgxpool.Config
	objects       *LockObjects
	source        string
	mu            sync.Mutex
	closed        bool
	owners        map[*WriteGuard]struct{}
	holds         map[*ProgressFileHold]struct{}
	operations    sync.WaitGroup
	pendingHolds  int
	pendingWrites int
	closeDone     chan struct{}
}

func (Coordinator) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("SettingsCoordinator(opaque)")) }
func (Coordinator) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func NewForPool(ctx context.Context, pool *pgxpool.Pool, objects *LockObjects) (*Coordinator, error) {
	if pool == nil || !objects.valid() || ctx == nil || ctx.Err() != nil {
		return nil, ErrCoordinatorRequired
	}
	configuration := pool.Config().Copy()
	digest := sha256.Sum256([]byte(configuration.ConnConfig.ConnString()))
	source := hex.EncodeToString(digest[:])
	value, err := objects.readRecord()
	if err != nil {
		return nil, err
	}
	if value.Mode == "armed" && value.Source != source {
		return nil, ErrDomain
	}
	c := &Coordinator{coordinatorState: &coordinatorState{pool: pool, configuration: configuration.Copy(), objects: objects, source: source, owners: map[*WriteGuard]struct{}{}, holds: map[*ProgressFileHold]struct{}{}, closeDone: make(chan struct{})}}
	c.self = c
	return c, nil
}
func (c *Coordinator) MatchesPool(pool *pgxpool.Pool) bool {
	if c == nil || c.coordinatorState == nil || c.self != c || pool == nil || c.pool != pool {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}
func (c *Coordinator) MatchesSourceURL(raw string) bool {
	if c == nil || c.coordinatorState == nil || c.self != c {
		return false
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(cfg.ConnConfig.ConnString()))
	return hex.EncodeToString(sum[:]) == c.source
}
func (c *Coordinator) Protected(path string) bool {
	return c == nil || c.coordinatorState == nil || c.self != c || c.objects.Protected(path)
}
func (c *Coordinator) TryProgressFileHold(ctx context.Context) (*ProgressFileHold, error) {
	if c == nil || c.coordinatorState == nil || c.self != c || ctx == nil {
		return nil, ErrCoordinatorRequired
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCoordinatorRequired
	}
	if len(c.holds)+c.pendingHolds >= 8 {
		c.mu.Unlock()
		return nil, ErrCapacity
	}
	c.pendingHolds++
	c.operations.Add(1)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.pendingHolds--; c.mu.Unlock(); c.operations.Done() }()
	hold, err := c.objects.acquire(ctx, false)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	value := &ProgressFileHold{owner: c, file: hold, ctx: life, cancel: cancel, done: make(chan struct{})}
	value.self = value
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		_ = hold.close()
		return nil, ErrCoordinatorRequired
	}
	c.holds[value] = struct{}{}
	c.mu.Unlock()
	return value, nil
}

// WriteGuard owns the native barrier until the caller's actual file work ends.
// Context cancellation interrupts work but never invokes Close/refunds itself.
type WriteGuard struct{ *writeGuardState }
type writeGuardState struct {
	self                     *WriteGuard
	owner                    *Coordinator
	file                     *fileHold
	ctx                      context.Context
	cancel                   context.CancelFunc
	pool                     *pgxpool.Pool
	original                 *pgx.Conn
	attempted                bool
	dialAttempts             int
	dialNetwork, dialAddress string
	controlAttempted         bool
	dialsClosed              bool
	transports               []*ownedGuardConnection
	dialWork                 sync.WaitGroup
	locked                   bool
	failed                   bool
	claimed                  bool
	work                     sync.WaitGroup
	mu                       sync.Mutex
	closing                  bool
	done                     chan struct{}
	err                      error
	cleanupContext           context.Context
	cleanupClaimed           bool
}

func (WriteGuard) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("SettingsWriteGuard(opaque)")) }
func (WriteGuard) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (c *Coordinator) TryWrite(ctx context.Context) (*WriteGuard, error) {
	if c == nil || c.coordinatorState == nil || c.self != c || ctx == nil || ctx.Err() != nil {
		return nil, ErrCoordinatorRequired
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCoordinatorRequired
	}
	if len(c.owners)+c.pendingWrites >= 8 {
		c.mu.Unlock()
		return nil, ErrCapacity
	}
	c.pendingWrites++
	c.operations.Add(1)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.pendingWrites--; c.mu.Unlock(); c.operations.Done() }()
	file, err := c.objects.acquire(ctx, true)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	guard := &WriteGuard{writeGuardState: &writeGuardState{owner: c, file: file, ctx: life, cancel: cancel, done: make(chan struct{})}}
	guard.self = guard
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		_ = file.close()
		return nil, ErrCoordinatorRequired
	}
	c.owners[guard] = struct{}{}
	c.mu.Unlock()
	fail := func(e error) (*WriteGuard, error) { _ = guard.Close(); return nil, e }
	setup, stop := context.WithTimeout(life, 2*time.Second)
	defer stop()
	configuration := c.configuration.Copy()
	// Lifecycle hooks supplied by an unrelated pool cannot silently become part
	// of this original-session protocol. The normal runtime pool is unchanged.
	if configuration.BeforeConnect != nil || configuration.AfterConnect != nil || configuration.BeforeAcquire != nil || configuration.PrepareConn != nil || configuration.AfterRelease != nil || configuration.BeforeClose != nil {
		return fail(ErrCoordinatorRequired)
	}
	configuration.MaxConns = 1
	configuration.MinConns = 0
	configuration.MinIdleConns = 0
	// Pinned pgxpool treats zero as immediate expiry, not disabled expiry.
	// This operation remains bounded by its real context and explicit Close;
	// any actual original backend retirement is terminal, never reconnectable.
	configuration.MaxConnLifetime = time.Duration(1<<63 - 1)
	configuration.MaxConnLifetimeJitter = 0
	configuration.MaxConnIdleTime = time.Duration(1<<63 - 1)
	configuration.BeforeConnect = func(ctx context.Context, _ *pgx.ConnConfig) error {
		guard.mu.Lock()
		defer guard.mu.Unlock()
		if guard.attempted || guard.closing || guard.ctx.Err() != nil || ctx.Err() != nil {
			return ErrCoordinatorRequired
		}
		guard.attempted = true
		return nil
	}
	configuration.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
		guard.mu.Lock()
		defer guard.mu.Unlock()
		if guard.original == nil {
			guard.original = connection
		}
		if guard.original != connection || guard.closing || guard.ctx.Err() != nil || ctx.Err() != nil {
			return ErrCoordinatorRequired
		}
		return nil
	}
	configuration.PrepareConn = func(ctx context.Context, connection *pgx.Conn) (bool, error) {
		guard.mu.Lock()
		defer guard.mu.Unlock()
		if guard.closing {
			// Terminal cleanup can claim only this actual original resource once.
			// This context is private and never becomes the mutation context.
			if guard.cleanupContext != nil && ctx == guard.cleanupContext && !guard.cleanupClaimed && !guard.failed && ctx.Err() == nil && guard.original == connection && !connection.IsClosed() {
				guard.cleanupClaimed = true
				return true, nil
			}
			return false, ErrCoordinatorRequired
		}
		if guard.failed || guard.ctx.Err() != nil || ctx.Err() != nil || guard.original != connection || connection.IsClosed() {
			return false, ErrCoordinatorRequired
		}
		return true, nil
	}
	configuration.BeforeClose = func(_ *pgx.Conn) {
		guard.mu.Lock()
		guard.failed = true
		guard.cancel()
		guard.mu.Unlock()
	}
	configuration.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	originalDial := configuration.ConnConfig.DialFunc
	configuration.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		guard.mu.Lock()
		if guard.dialsClosed {
			guard.mu.Unlock()
			return nil, ErrCoordinatorRequired
		}
		control := guard.original != nil
		var prior []*ownedGuardConnection
		if guard.dialAttempts > 0 && (network != guard.dialNetwork || address != guard.dialAddress) {
			guard.mu.Unlock()
			return nil, ErrCoordinatorRequired
		}
		if control {
			if guard.controlAttempted {
				guard.mu.Unlock()
				return nil, ErrCoordinatorRequired
			}
			guard.controlAttempted = true
		} else {
			if guard.dialAttempts >= 2 || guard.closing || guard.ctx.Err() != nil {
				guard.mu.Unlock()
				return nil, ErrCoordinatorRequired
			}
			if guard.dialAttempts == 0 {
				guard.dialNetwork = network
				guard.dialAddress = address
			} else {
				prior = append(prior, guard.transports...)
			}
			guard.dialAttempts++
		}
		guard.dialWork.Add(1)
		guard.mu.Unlock()
		defer guard.dialWork.Done()
		// sslmode=prefer may negotiate TLS/plain before an authenticated SQL
		// backend exists. Join the previous exact-target transport before the one
		// bounded fallback; never adopt another address or replace an original.
		for _, connection := range prior {
			_ = connection.Close()
		}
		// Existing CancelRequest uses a distinct bounded control connection. It
		// can carry only its fixed protocol packet, never another SQL handshake.
		dialContext := ctx
		finish := func() {}
		if !control {
			var cancel context.CancelFunc
			dialContext, cancel = context.WithCancel(ctx)
			done := make(chan struct{})
			stop := context.AfterFunc(guard.ctx, func() { cancel(); close(done) })
			if guard.ctx.Err() != nil {
				cancel()
			}
			finish = func() {
				if !stop() {
					<-done
				}
				cancel()
			}
		}
		defer finish()
		actual, err := originalDial(dialContext, network, address)
		if actual == nil {
			return nil, err
		}
		owned := &ownedGuardConnection{Conn: actual, control: control, done: make(chan struct{})}
		guard.mu.Lock()
		guard.transports = append(guard.transports, owned)
		guard.mu.Unlock()
		if err != nil {
			return owned, err
		}
		if !control && guard.ctx.Err() != nil {
			return owned, guard.ctx.Err()
		}
		return owned, nil
	}
	guard.pool, err = pgxpool.NewWithConfig(life, configuration)
	if err != nil {
		return fail(ErrCoordinatorRequired)
	}
	connection, err := guard.pool.Acquire(setup)
	if err != nil {
		return fail(ErrCoordinatorRequired)
	}
	var locked bool
	err = connection.QueryRow(setup, `SELECT pg_try_advisory_lock($1::integer,$2::integer)`, Namespace, Resource).Scan(&locked)
	guard.mu.Lock()
	guard.locked = err == nil && locked
	guard.mu.Unlock()
	// Release returns the borrower, not the SESSION lock. Every actual startup
	// or restore SQL writer below reuses this exact single physical backend.
	connection.Release()
	if err != nil {
		return fail(ErrCoordinatorRequired)
	}
	if !locked {
		return fail(ErrBusy)
	}
	value, err := c.objects.readRecord()
	if err != nil {
		return fail(err)
	}
	if value.Mode == "armed" && value.Source != c.source {
		return fail(ErrDomain)
	}
	if life.Err() != nil {
		return fail(life.Err())
	}
	return guard, nil
}
func (c *Coordinator) Arm(guard *WriteGuard) error {
	if guard.Current(c) != nil {
		return ErrCoordinatorRequired
	}
	return c.objects.writeRecord(record{Version: 1, Family: nativeFamily, Mode: "armed", Source: c.source})
}
func (g *WriteGuard) Current(c *Coordinator) error {
	if g == nil || g.writeGuardState == nil || g.self != g || g.owner != c {
		return ErrCoordinatorRequired
	}
	g.mu.Lock()
	current := !g.closing && !g.failed && g.locked && g.original != nil && g.ctx.Err() == nil
	g.mu.Unlock()
	if !current {
		return ErrCoordinatorRequired
	}
	return g.file.objects.check()
}

// MutationPool is an internal storage port, not account or SQL authorization.
// Only the concrete original datastore may claim the one original-session pool.
// Its actual work must end before finish; Close cannot refund that work early.
func (g *WriteGuard) MutationPool(original *pgxpool.Pool) (*pgxpool.Pool, func(), error) {
	if g == nil || g.writeGuardState == nil || g.self != g || !g.owner.MatchesPool(original) {
		return nil, nil, ErrCoordinatorRequired
	}
	if g.Current(g.owner) != nil {
		return nil, nil, ErrCoordinatorRequired
	}
	g.mu.Lock()
	if g.closing || g.failed || !g.locked || g.claimed || g.ctx.Err() != nil {
		g.mu.Unlock()
		return nil, nil, ErrCoordinatorRequired
	}
	g.claimed = true
	g.work.Add(1)
	pool := g.pool
	g.mu.Unlock()
	var once sync.Once
	return pool, func() { once.Do(g.work.Done) }, nil
}
func (g *WriteGuard) Context() context.Context {
	if g == nil || g.writeGuardState == nil || g.self != g {
		return nil
	}
	return g.ctx
}

// acknowledgeHealthyRelease owns the exact original pool resource before any
// cleanup SQL. Failed/closed originals retain the existing fixed-key fencing
// contract: their local Close is not a claim that the server transaction ended.
func (g *WriteGuard) acknowledgeHealthyRelease() (*pgx.Conn, error) {
	g.mu.Lock()
	if !g.locked || g.failed || g.original == nil || g.pool == nil || g.original.IsClosed() {
		g.mu.Unlock()
		return nil, nil
	}
	original, pool := g.original, g.pool
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	g.cleanupContext = ctx
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.cleanupContext = nil
		g.mu.Unlock()
		cancel()
	}()

	resource, err := pool.Acquire(ctx)
	if err != nil {
		return nil, ErrUnknown
	}
	if resource.Conn() != original {
		resource.Release()
		return nil, ErrUnknown
	}
	actual := resource.Hijack()
	// These protocol state reads occur only after actual resource ownership has
	// transferred. Never recover or replay a busy/uncertain business transaction.
	if actual.PgConn().IsBusy() || actual.PgConn().TxStatus() != 'I' {
		return actual, nil
	}
	var pid uint32
	var released bool
	err = actual.QueryRow(ctx, `SELECT pg_backend_pid(), pg_advisory_unlock($1::integer,$2::integer)`, pgx.QueryExecModeSimpleProtocol, Namespace, Resource).Scan(&pid, &released)
	if err != nil || !released || pid != actual.PgConn().PID() || ctx.Err() != nil {
		return actual, ErrUnknown
	}
	// A true unlock decrements one acquisition. Confirm the exact two-key lock
	// is absent as a separate, acknowledged statement, without a polling loop.
	var absent bool
	err = actual.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=$1::oid AND objid=$2::oid AND objsubid=2 AND granted)`, pgx.QueryExecModeSimpleProtocol, Namespace, Resource).Scan(&absent)
	if err != nil || !absent || ctx.Err() != nil || actual.PgConn().IsBusy() || actual.PgConn().TxStatus() != 'I' {
		return actual, ErrUnknown
	}
	return actual, nil
}

func (g *WriteGuard) Close() error {
	if g == nil || g.writeGuardState == nil || g.self != g {
		return ErrCoordinatorRequired
	}
	g.mu.Lock()
	if g.closing {
		done := g.done
		g.mu.Unlock()
		<-done
		return g.err
	}
	g.closing = true
	g.cancel()
	g.mu.Unlock()
	g.work.Wait()
	// The sole cleanup SQL operation retains the existing bounded control port
	// until its actual query/context callbacks end. No SQL reconnect is allowed.
	hijacked, releaseErr := g.acknowledgeHealthyRelease()
	if releaseErr != nil {
		g.err = releaseErr
	}
	if hijacked != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := hijacked.Close(closeContext); err != nil && g.err == nil {
			g.err = ErrUnknown
		}
		cancel()
	}
	g.mu.Lock()
	g.dialsClosed = true
	g.mu.Unlock()
	g.dialWork.Wait()
	g.mu.Lock()
	transports := append([]*ownedGuardConnection(nil), g.transports...)
	g.mu.Unlock()
	// A Dial or failed handshake need not yield a pgx.Conn/resource. Its real
	// transport is nevertheless ours and must close/join before native refund.
	for _, connection := range transports {
		_ = connection.Close()
	}
	if g.pool != nil {
		g.pool.Close()
	}
	// Pool destruction has a bounded internal wait in pinned pgx. It is not
	// physical cleanup proof. Join the exact original backend unconditionally.
	g.mu.Lock()
	original := g.original
	g.mu.Unlock()
	if original != nil {
		<-original.PgConn().CleanupDone()
	}
	if err := g.file.close(); g.err == nil {
		g.err = err
	}
	g.owner.mu.Lock()
	delete(g.owner.owners, g)
	g.owner.mu.Unlock()
	close(g.done)
	return g.err
}
func TryUnarmedWrite(ctx context.Context, objects *LockObjects) (*UnarmedWriteGuard, error) {
	file, err := objects.acquire(ctx, true)
	if err != nil {
		return nil, err
	}
	value, err := objects.readRecord()
	if err != nil {
		_ = file.close()
		return nil, err
	}
	if value.Mode != "unarmed" {
		_ = file.close()
		return nil, ErrCoordinatorRequired
	}
	guard := &UnarmedWriteGuard{file: file}
	guard.self = guard
	return guard, nil
}

type UnarmedWriteGuard struct {
	self   *UnarmedWriteGuard
	file   *fileHold
	mu     sync.Mutex
	closed bool
}

func (g *UnarmedWriteGuard) Close() error {
	if g == nil || g.self != g {
		return ErrUnknown
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	return g.file.close()
}
func (c *Coordinator) Close() error {
	if c == nil || c.coordinatorState == nil || c.self != c {
		return ErrCoordinatorRequired
	}
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		return nil
	}
	c.closed = true
	owners := make([]*WriteGuard, 0, len(c.owners))
	for owner := range c.owners {
		owners = append(owners, owner)
	}
	holds := make([]*ProgressFileHold, 0, len(c.holds))
	for hold := range c.holds {
		holds = append(holds, hold)
	}
	c.mu.Unlock()
	for _, owner := range owners {
		owner.cancel()
	}
	for _, hold := range holds {
		hold.cancel()
	}
	c.operations.Wait()
	for _, owner := range owners {
		<-owner.done
	}
	for _, hold := range holds {
		<-hold.done
	}
	close(c.closeDone)
	return nil
}

func (c *Coordinator) MatchesConfigPath(path string) bool {
	if c == nil || c.coordinatorState == nil || c.self != c {
		return false
	}
	absolute, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return false
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	return err == nil && filepath.Join(absolute, directoryName) == c.objects.directory && c.objects.check() == nil
}
func (c *Coordinator) Armed() bool {
	if c == nil || c.coordinatorState == nil || c.self != c {
		return false
	}
	value, err := c.objects.readRecord()
	return err == nil && value.Mode == "armed" && value.Source == c.source
}
func (g *UnarmedWriteGuard) Current() error {
	if g == nil || g.self != g || g.file == nil {
		return ErrUnknown
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrUnknown
	}
	value, err := g.file.objects.readRecord()
	if err != nil {
		return err
	}
	if value.Mode != "unarmed" {
		return ErrCoordinatorRequired
	}
	return nil
}

// ownedGuardConnection retains actual handshake and driver IO even when
// ConnectConfig/AfterConnect fails before a pool resource is published.
type ownedGuardConnection struct {
	net.Conn
	control    bool
	sent       bool
	mu         sync.Mutex
	closing    bool
	operations sync.WaitGroup
	done       chan struct{}
	err        error
	finish     func()
}

func (c *ownedGuardConnection) begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return false
	}
	c.operations.Add(1)
	return true
}
func (c *ownedGuardConnection) Read(p []byte) (int, error) {
	if !c.begin() {
		return 0, net.ErrClosed
	}
	defer c.operations.Done()
	return c.Conn.Read(p)
}
func (c *ownedGuardConnection) Write(p []byte) (int, error) {
	if !c.begin() {
		return 0, net.ErrClosed
	}
	defer c.operations.Done()
	if c.control {
		c.mu.Lock()
		valid := !c.sent && len(p) >= 16 && len(p) <= 512 && binary.BigEndian.Uint32(p[:4]) == uint32(len(p)) && binary.BigEndian.Uint32(p[4:8]) == 80877102
		if valid {
			c.sent = true
		}
		c.mu.Unlock()
		if !valid {
			return 0, ErrCoordinatorRequired
		}
	}
	return c.Conn.Write(p)
}
func (c *ownedGuardConnection) SetDeadline(t time.Time) error {
	if !c.begin() {
		return net.ErrClosed
	}
	defer c.operations.Done()
	return c.Conn.SetDeadline(t)
}
func (c *ownedGuardConnection) SetReadDeadline(t time.Time) error {
	if !c.begin() {
		return net.ErrClosed
	}
	defer c.operations.Done()
	return c.Conn.SetReadDeadline(t)
}
func (c *ownedGuardConnection) SetWriteDeadline(t time.Time) error {
	if !c.begin() {
		return net.ErrClosed
	}
	defer c.operations.Done()
	return c.Conn.SetWriteDeadline(t)
}
func (c *ownedGuardConnection) Close() error {
	c.mu.Lock()
	if c.closing {
		done := c.done
		c.mu.Unlock()
		<-done
		return c.err
	}
	c.closing = true
	c.mu.Unlock()
	c.err = c.Conn.Close()
	c.operations.Wait()
	close(c.done)
	if c.finish != nil {
		c.finish()
	}
	return c.err
}

// OpenRegularData preserves the kernel meaning of the original raw path,
// including ordinary final symlinks. It does not load or normalize settings.
func OpenRegularData(path string) (*os.File, error) { return openRegularData(path, false) }

// OpenRegularGuardData retains the stricter no-follow guard/policy contract.
func OpenRegularGuardData(path string) (*os.File, error) { return openRegularData(path, true) }
func openRegularData(path string, noFollow bool) (*os.File, error) {
	file, err := nativeReadRegular(path, noFollow)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrMetadata
	}
	var named os.FileInfo
	if noFollow {
		named, err = os.Lstat(path)
	} else {
		named, err = os.Stat(path)
	}
	if err != nil || !named.Mode().IsRegular() || !os.SameFile(info, named) {
		_ = file.Close()
		return nil, ErrMetadata
	}
	return file, nil
}

// RuntimePool owns actual constructor transports for the new bootstrap port.
// It does not change historical DataStore.New or grant account/SQL authority.
type RuntimePool struct{ *runtimePoolState }
type runtimePoolState struct {
	self           *RuntimePool
	pool           *pgxpool.Pool
	configuration  *pgxpool.Config
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	closing        bool
	dials          sync.WaitGroup
	pendingDials   int
	transportWork  sync.WaitGroup
	connectionWork sync.WaitGroup
	transports     map[*ownedGuardConnection]struct{}
	connections    map[*pgx.Conn]struct{}
	done           chan struct{}
}

func (RuntimePool) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("SettingsRuntimePool(opaque)")) }
func (RuntimePool) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func OpenRuntimePool(ctx context.Context, configuration *pgxpool.Config) (*RuntimePool, error) {
	if ctx == nil || ctx.Err() != nil || configuration == nil {
		return nil, ErrCoordinatorRequired
	}
	life, cancel := context.WithCancel(ctx)
	owner := &RuntimePool{runtimePoolState: &runtimePoolState{configuration: configuration.Copy(), ctx: life, cancel: cancel, transports: map[*ownedGuardConnection]struct{}{}, connections: map[*pgx.Conn]struct{}{}, done: make(chan struct{})}}
	owner.self = owner
	cfg := configuration.Copy()
	dial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		owner.mu.Lock()
		if owner.closing || owner.ctx.Err() != nil || int64(len(owner.transports)+len(owner.connections))+int64(owner.pendingDials)*2 >= int64(cfg.MaxConns)*4 {
			owner.mu.Unlock()
			return nil, ErrCoordinatorRequired
		}
		owner.dials.Add(1)
		owner.pendingDials++
		owner.mu.Unlock()
		defer func() { owner.mu.Lock(); owner.pendingDials--; owner.mu.Unlock(); owner.dials.Done() }()
		work, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		stop := context.AfterFunc(owner.ctx, func() { cancel(); close(done) })
		defer func() {
			if !stop() {
				<-done
			}
			cancel()
		}()
		if owner.ctx.Err() != nil {
			cancel()
		}
		actual, err := dial(work, network, address)
		if actual == nil {
			return nil, err
		}
		connection := &ownedGuardConnection{Conn: actual, done: make(chan struct{})}
		owner.mu.Lock()
		owner.transportWork.Add(1)
		owner.transports[connection] = struct{}{}
		owner.mu.Unlock()
		connection.finish = func() {
			owner.mu.Lock()
			delete(owner.transports, connection)
			owner.mu.Unlock()
			owner.transportWork.Done()
		}
		if err != nil {
			return connection, err
		}
		if owner.ctx.Err() != nil {
			return connection, owner.ctx.Err()
		}
		return connection, nil
	}
	after := cfg.AfterConnect
	cfg.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
		// Capture a real connected backend even if cancellation won this race.
		owner.mu.Lock()
		owner.connections[connection] = struct{}{}
		owner.connectionWork.Add(1)
		denied := owner.closing || owner.ctx.Err() != nil || ctx.Err() != nil
		owner.mu.Unlock()
		go func() {
			<-connection.PgConn().CleanupDone()
			owner.mu.Lock()
			delete(owner.connections, connection)
			owner.mu.Unlock()
			owner.connectionWork.Done()
		}()
		if denied {
			return ErrCoordinatorRequired
		}
		if after != nil {
			return after(ctx, connection)
		}
		return nil
	}
	var err error
	owner.pool, err = pgxpool.NewWithConfig(life, cfg)
	if err != nil {
		_ = owner.Close()
		return nil, ErrCoordinatorRequired
	}
	return owner, nil
}
func (o *RuntimePool) StoragePool() *pgxpool.Pool {
	if o == nil || o.runtimePoolState == nil || o.self != o {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing {
		return nil
	}
	return o.pool
}
func NewForRuntimePool(ctx context.Context, owner *RuntimePool, objects *LockObjects) (*Coordinator, error) {
	pool := owner.StoragePool()
	if pool == nil {
		return nil, ErrCoordinatorRequired
	}
	coordinator, err := NewForPool(ctx, pool, objects)
	if err != nil {
		return nil, err
	}
	coordinator.configuration = owner.configuration.Copy()
	return coordinator, nil
}
func (o *RuntimePool) Close() error {
	if o == nil || o.runtimePoolState == nil || o.self != o {
		return ErrCoordinatorRequired
	}
	o.mu.Lock()
	if o.closing {
		done := o.done
		o.mu.Unlock()
		<-done
		return nil
	}
	o.closing = true
	o.cancel()
	o.mu.Unlock()
	o.dials.Wait()
	o.mu.Lock()
	connections := make([]*ownedGuardConnection, 0, len(o.transports))
	for connection := range o.transports {
		connections = append(connections, connection)
	}
	o.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	if o.pool != nil {
		o.pool.Close()
	}
	// Pool.Close joins constructors, so no later AfterConnect can Add here.
	o.transportWork.Wait()
	o.connectionWork.Wait()
	close(o.done)
	return nil
}
