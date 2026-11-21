package datastore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/settingscommit"
)

// SettingsStartupStore owns the original concrete pool before migrations. It
// cannot turn a caller transaction or a role boolean into guarded readiness.
type SettingsStartupStore struct{ *settingsStartupState }
type settingsStartupState struct {
	self              *SettingsStartupStore
	store             *DataStore
	poolOwner         *settingscommit.RuntimePool
	coordinator       *settingscommit.Coordinator
	mu                sync.Mutex
	migrated          bool
	closed            bool
	closeDone         chan struct{}
	closeErr          error
	mutations         *DataStore
	mutationGuard     *settingscommit.WriteGuard
	finishMutation    func()
	mutationsFinished bool
	ctx               context.Context
	cancel            context.CancelFunc
}

func (SettingsStartupStore) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SettingsStartupStore(opaque)"))
}
func (SettingsStartupStore) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func OpenSettingsStartupStore(ctx context.Context, url string, objects *settingscommit.LockObjects, opts ...Option) (*SettingsStartupStore, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	configuration, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	options := defaultOptions()
	for _, option := range opts {
		option(&options)
	}
	if options.maxConns > 0 {
		configuration.MaxConns = int32(options.maxConns)
	}
	if options.minConns > 0 {
		configuration.MinConns = int32(options.minConns)
	}
	if options.maxConnLifetime > 0 {
		configuration.MaxConnLifetime = options.maxConnLifetime
	}
	configureQueryLifecycle(configuration.ConnConfig)
	poolOwner, err := settingscommit.OpenRuntimePool(ctx, configuration)
	if err != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	pool := poolOwner.StoragePool()
	if pool.Ping(ctx) != nil {
		_ = poolOwner.Close()
		return nil, settingscommit.ErrCoordinatorRequired
	}
	coordinator, err := settingscommit.NewForRuntimePool(ctx, poolOwner, objects)
	if err != nil {
		_ = poolOwner.Close()
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	value := &SettingsStartupStore{settingsStartupState: &settingsStartupState{store: &DataStore{pool: pool}, poolOwner: poolOwner, coordinator: coordinator, ctx: life, cancel: cancel, closeDone: make(chan struct{})}}
	value.self = value
	return value, nil
}
func (s *SettingsStartupStore) Coordinator() *settingscommit.Coordinator {
	if s == nil || s.settingsStartupState == nil || s.self != s {
		return nil
	}
	return s.coordinator
}
func (s *SettingsStartupStore) mutationStore(guard *settingscommit.WriteGuard) (*DataStore, error) {
	if s.closed || s.mutationsFinished || guard == nil || guard.Current(s.coordinator) != nil || !s.store.SettingsCommitReady(s.coordinator) {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	if s.mutations != nil {
		if s.mutationGuard != guard {
			return nil, settingscommit.ErrCoordinatorRequired
		}
		return s.mutations, nil
	}
	pool, finish, err := guard.MutationPool(s.store.pool)
	if err != nil {
		return nil, err
	}
	s.mutations = &DataStore{pool: pool}
	s.mutationGuard = guard
	s.finishMutation = finish
	return s.mutations, nil
}
func (s *SettingsStartupStore) Migrate(ctx context.Context, guard *settingscommit.WriteGuard) (*DataStore, error) {
	if s == nil || s.settingsStartupState == nil || s.self != s || ctx == nil || ctx.Err() != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mutation, err := s.mutationStore(guard)
	if err != nil {
		return nil, err
	}
	work, finish := settingsWorkContext(ctx, s.ctx, guard.Context())
	defer finish()
	if work.Err() != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	if !s.migrated {
		if err = runMigrations(work, mutation.pool); err != nil {
			return nil, err
		}
		s.migrated = true
	}
	if work.Err() != nil || guard.Current(s.coordinator) != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	return s.store, nil
}
func (s *SettingsStartupStore) MigrateFromJSON(ctx context.Context, guard *settingscommit.WriteGuard, cacheDir string) error {
	if s == nil || s.settingsStartupState == nil || s.self != s || ctx == nil {
		return settingscommit.ErrCoordinatorRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mutation, err := s.mutationStore(guard)
	if err != nil {
		return err
	}
	work, finish := settingsWorkContext(ctx, s.ctx, guard.Context())
	defer finish()
	if work.Err() != nil {
		return settingscommit.ErrCoordinatorRequired
	}
	if err = MigrateFromJSON(work, mutation, cacheDir); err != nil {
		return err
	}
	if work.Err() != nil || guard.Current(s.coordinator) != nil {
		return settingscommit.ErrCoordinatorRequired
	}
	return nil
}
func (s *SettingsStartupStore) RunDataMigrations(ctx context.Context, guard *settingscommit.WriteGuard) error {
	if s == nil || s.settingsStartupState == nil || s.self != s || ctx == nil {
		return settingscommit.ErrCoordinatorRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mutation, err := s.mutationStore(guard)
	if err != nil {
		return err
	}
	work, finish := settingsWorkContext(ctx, s.ctx, guard.Context())
	defer finish()
	if work.Err() != nil {
		return settingscommit.ErrCoordinatorRequired
	}
	if err = RunDataMigrations(work, mutation); err != nil {
		return err
	}
	if work.Err() != nil || guard.Current(s.coordinator) != nil {
		return settingscommit.ErrCoordinatorRequired
	}
	return nil
}

// FinishMutations joins the serial startup SQL scope before its native guard
// is closed. The runtime datastore and its original pool remain unchanged.
func (s *SettingsStartupStore) FinishMutations() error {
	if s == nil || s.settingsStartupState == nil || s.self != s {
		return settingscommit.ErrCoordinatorRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mutationsFinished = true
	if s.finishMutation != nil {
		s.finishMutation()
		s.finishMutation = nil
	}
	return nil
}
func settingsWorkContext(ctx context.Context, originals ...context.Context) (context.Context, func()) {
	work, cancel := context.WithCancel(ctx)
	type joinedCancel struct {
		stop func() bool
		done chan struct{}
	}
	callbacks := make([]joinedCancel, 0, len(originals))
	for _, original := range originals {
		done := make(chan struct{})
		stop := context.AfterFunc(original, func() { cancel(); close(done) })
		callbacks = append(callbacks, joinedCancel{stop: stop, done: done})
		if original.Err() != nil {
			cancel()
		}
	}
	return work, func() {
		for _, callback := range callbacks {
			if !callback.stop() {
				<-callback.done
			}
		}
		cancel()
	}
}
func (s *SettingsStartupStore) Close() error {
	if s == nil || s.settingsStartupState == nil || s.self != s {
		return settingscommit.ErrCoordinatorRequired
	}
	s.cancel()
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	if s.finishMutation != nil {
		s.finishMutation()
		s.finishMutation = nil
	}
	s.mu.Unlock()
	s.closeErr = s.coordinator.Close()
	_ = s.poolOwner.Close()
	close(s.closeDone)
	return s.closeErr
}
func (ds *DataStore) SettingsCommitReady(c *settingscommit.Coordinator) bool {
	return ds != nil && ds.pool != nil && c != nil && c.MatchesPool(ds.pool)
}

// settingsProgressTx is deliberately private. The future original-authority
// writer executes its fixed SQL through this SAME transaction. A file-side hold
// alone does not claim that PostgreSQL still owns the shared lock.
type settingsProgressTx struct {
	tx           pgx.Tx
	connection   *pgxpool.Conn
	hold         *settingscommit.ProgressFileHold
	ctx          context.Context
	cancel       context.CancelFunc
	stop         func() bool
	callbackDone chan struct{}
	once         sync.Once
	err          error
}

func (ds *DataStore) beginSettingsProgressTx(ctx context.Context, c *settingscommit.Coordinator) (*settingsProgressTx, error) {
	if !ds.SettingsCommitReady(c) || ctx == nil || ctx.Err() != nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	hold, err := c.TryProgressFileHold(ctx)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(hold.Context(), func() { cancel(); close(done) })
	value := &settingsProgressTx{hold: hold, ctx: life, cancel: cancel, stop: stop, callbackDone: done}
	fail := func(err error) (*settingsProgressTx, error) { _ = value.Close(); return nil, err }
	value.connection, err = ds.pool.Acquire(life)
	if err != nil {
		return fail(err)
	}
	value.tx, err = value.connection.BeginTx(life, pgx.TxOptions{AccessMode: pgx.ReadWrite})
	if err != nil {
		return fail(err)
	}
	var locked bool
	if err = value.tx.QueryRow(life, `SELECT pg_try_advisory_xact_lock_shared($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked); err != nil {
		return fail(err)
	}
	if !locked {
		return fail(settingscommit.ErrBusy)
	}
	if hold.Current(c) != nil || ctx.Err() != nil || life.Err() != nil {
		return fail(settingscommit.ErrCoordinatorRequired)
	}
	return value, nil
}
func (t *settingsProgressTx) Close() error {
	if t == nil {
		return nil
	}
	t.once.Do(func() {
		t.cancel()
		if t.tx != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := t.tx.Rollback(ctx)
			cancel()
			if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
				t.err = err
			}
		}
		if t.connection != nil {
			physical := t.connection.Conn().PgConn()
			if physical.IsClosed() {
				<-physical.CleanupDone()
			}
			t.connection.Release()
		}
		if t.stop != nil && !t.stop() {
			<-t.callbackDone
		}
		if err := t.hold.Close(); t.err == nil {
			t.err = err
		}
	})
	return t.err
}

// SettingsRestoreTransaction is the actual original-store restore transaction.
// Its repositories use the one session-locked backend, never a supplied TX.
type SettingsRestoreTransaction struct{ *settingsRestoreState }
type settingsRestoreState struct {
	self             *SettingsRestoreTransaction
	guard            *settingscommit.WriteGuard
	connection       *pgxpool.Conn
	tx               pgx.Tx
	ctx              context.Context
	cancel           context.CancelFunc
	finishContext    func()
	finishGuard      func()
	mu               sync.Mutex
	claimed          bool
	bodyFinishing    bool
	positionsClaimed bool
	historyClaimed   bool
	repositoryBody   *Tx
	closing          bool
	committed        bool
	bodyDone         chan struct{}
	operations       sync.WaitGroup
	done             chan struct{}
	err              error
}

func (SettingsRestoreTransaction) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SettingsRestoreTransaction(opaque)"))
}
func (SettingsRestoreTransaction) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (ds *DataStore) BeginSettingsRestore(ctx context.Context, guard *settingscommit.WriteGuard) (*SettingsRestoreTransaction, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || guard == nil {
		return nil, settingscommit.ErrCoordinatorRequired
	}
	pool, finish, err := guard.MutationPool(ds.pool)
	if err != nil {
		return nil, err
	}
	parentWork, finishContext := settingsWorkContext(ctx, guard.Context())
	work, cancel := context.WithCancel(parentWork)
	value := &SettingsRestoreTransaction{settingsRestoreState: &settingsRestoreState{guard: guard, ctx: work, cancel: cancel, finishContext: finishContext, finishGuard: finish, bodyDone: make(chan struct{}), done: make(chan struct{})}}
	value.self = value
	fail := func(e error) (*SettingsRestoreTransaction, error) { _ = value.Close(); return nil, e }
	value.connection, err = pool.Acquire(work)
	if err != nil {
		return fail(err)
	}
	value.tx, err = value.connection.Begin(work)
	if err != nil {
		return fail(err)
	}
	if work.Err() != nil {
		return fail(settingscommit.ErrCoordinatorRequired)
	}
	return value, nil
}
func (t *SettingsRestoreTransaction) Context() context.Context {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return nil
	}
	return t.ctx
}

// Repositories grants no new authority. finish is only an actual-work join;
// the caller invokes it after every original repository operation has ended.
func (t *SettingsRestoreTransaction) Repositories() (*Tx, func(), error) {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return nil, nil, settingscommit.ErrCoordinatorRequired
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing || t.claimed || t.ctx.Err() != nil || t.tx == nil {
		return nil, nil, settingscommit.ErrCoordinatorRequired
	}
	t.claimed = true
	t.repositoryBody = &Tx{tx: t.tx}
	var once sync.Once
	return t.repositoryBody, func() {
		once.Do(func() {
			t.mu.Lock()
			t.bodyFinishing = true
			t.mu.Unlock()
			// Commit cannot enter before bodyDone. No new fixed restore-body
			// operation can enter after bodyFinishing, so this joins only real
			// admitted body work, outside the state mutex and before SQL release.
			t.operations.Wait()
			close(t.bodyDone)
		})
	}, nil
}
func (t *SettingsRestoreTransaction) Commit() error {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return settingscommit.ErrCoordinatorRequired
	}
	t.mu.Lock()
	if t.closing || !t.claimed || t.committed || t.ctx.Err() != nil {
		t.mu.Unlock()
		return settingscommit.ErrCoordinatorRequired
	}
	select {
	case <-t.bodyDone:
	default:
		t.mu.Unlock()
		return settingscommit.ErrCoordinatorRequired
	}
	t.operations.Add(1)
	t.committed = true
	t.mu.Unlock()
	defer t.operations.Done()
	// A failed ACK is returned unchanged. It never proves rollback and never
	// permits a settings replacement or replay on another backend.
	return t.tx.Commit(t.ctx)
}
func (t *SettingsRestoreTransaction) Close() error {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return settingscommit.ErrCoordinatorRequired
	}
	t.mu.Lock()
	if t.closing {
		done := t.done
		t.mu.Unlock()
		<-done
		return t.err
	}
	t.closing = true
	t.cancel()
	claimed := t.claimed
	t.mu.Unlock()
	// Guard cancellation interrupts the real context. Do not stop/join that
	// link before actual admitted repository/commit work has ended.
	if claimed {
		<-t.bodyDone
	}
	t.operations.Wait()
	if t.tx != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := t.tx.Rollback(ctx)
		cancel()
		if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.err = err
		}
	}
	if t.connection != nil {
		connection := t.connection.Conn()
		physical := connection.PgConn()
		unhealthy := physical.IsClosed() || physical.IsBusy() || physical.TxStatus() != 'I'
		if unhealthy && t.err == nil {
			t.err = errors.New("settings restore connection unavailable during cleanup")
		}
		if !physical.IsClosed() && (unhealthy || t.err != nil) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			closeErr := connection.Close(ctx)
			cancel()
			if t.err == nil {
				t.err = closeErr
			}
		}
		// Pool Release may start an asynchronous destructor. The original
		// physical connection must actually finish before its guard is refunded.
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		t.connection.Release()
	}
	if t.finishContext != nil {
		t.finishContext()
	}
	if t.finishGuard != nil {
		t.finishGuard()
	}
	close(t.done)
	return t.err
}
