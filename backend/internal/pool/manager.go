package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/javi11/nntppool/v2"
)

// Manager provides centralized NNTP connection pool management
type Manager interface {
	// GetPool returns the current connection pool, recreating it from the last
	// configured providers if it was cleared (e.g. by a cold-test flush) so a
	// cleared pool is not permanently broken.
	GetPool() (nntppool.UsenetConnectionPool, error)

	// SetProviders creates/recreates the pool with new providers
	SetProviders(providers []nntppool.UsenetProviderConfig) error

	// ClearPool shuts down and drops the live pool. The provider configuration
	// is retained, so the next GetPool reconstitutes it with fresh connections
	// (a "cold pool" rather than a disabled one).
	ClearPool() error

	// HasPool returns true if a pool is currently available
	HasPool() bool

	// AvailableConnections returns the number of connection slots currently free
	// across all providers (the configured per-provider budget minus connections
	// in use right now). It returns 0 when no live pool is available. Callers use
	// this to size their own parallelism fairly so several concurrent requests
	// can each take a fraction of the remaining pool without one saturating it.
	AvailableConnections() int
}

var ErrPreparedManagerUnavailable = errors.New("prepared NNTP manager unavailable")

// PreparedManager owns only configuration and physical pool lifetime. It does
// not authenticate a request or grant article/source access.
type PreparedManager struct {
	self         *PreparedManager
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	active       bool
	closed       bool
	providers    []nntppool.UsenetProviderConfig
	pool         nntppool.UsenetConnectionPool
	retiring     nntppool.UsenetConnectionPool
	poolCancel   context.CancelFunc
	retireCancel context.CancelFunc
	buildCancel  context.CancelFunc
	generation   uint64
	build        bool
	building     bool
	lastErr      error
	wake         chan struct{}
	changed      chan struct{}
	workers      sync.WaitGroup
	closeOnce    sync.Once
}

func NewPreparedManager(original context.Context) (*PreparedManager, error) {
	if original == nil || original.Err() != nil {
		return nil, ErrPreparedManagerUnavailable
	}
	ctx, cancel := context.WithCancel(original)
	m := &PreparedManager{ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), changed: make(chan struct{})}
	m.self = m
	return m, nil
}

func (m *PreparedManager) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *PreparedManager) Activate() error {
	if m == nil || m.self != m {
		return ErrPreparedManagerUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.ctx.Err() != nil || m.active {
		return ErrPreparedManagerUnavailable
	}
	m.active = true
	m.build = len(m.providers) != 0
	m.workers.Add(1)
	go m.run()
	m.notifyLocked()
	return nil
}

func (m *PreparedManager) SetProviders(providers []nntppool.UsenetProviderConfig) error {
	if m == nil || m.self != m {
		return ErrPreparedManagerUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.ctx.Err() != nil {
		return ErrPreparedManagerUnavailable
	}
	m.generation++
	if m.buildCancel != nil {
		m.buildCancel()
	}
	m.providers = append([]nntppool.UsenetProviderConfig(nil), providers...)
	for i := range m.providers {
		m.providers[i].VerifyCapabilities = append([]string(nil), providers[i].VerifyCapabilities...)
	}
	if m.pool != nil {
		m.retiring, m.pool = m.pool, nil
		m.retireCancel, m.poolCancel = m.poolCancel, nil
		if m.retireCancel != nil {
			m.retireCancel()
		}
	}
	m.build = m.active && len(providers) != 0
	m.lastErr = nil
	m.notifyLocked()
	return nil
}

func (m *PreparedManager) GetPool() (nntppool.UsenetConnectionPool, error) {
	if m == nil || m.self != m {
		return nil, ErrPreparedManagerUnavailable
	}
	for {
		m.mu.Lock()
		if m.closed || !m.active || m.ctx.Err() != nil || len(m.providers) == 0 {
			m.mu.Unlock()
			return nil, ErrPreparedManagerUnavailable
		}
		if m.pool != nil {
			current := m.pool
			m.mu.Unlock()
			return current, nil
		}
		if m.lastErr != nil {
			err := m.lastErr
			m.mu.Unlock()
			return nil, err
		}
		if !m.build && !m.building {
			m.build = true
			m.notifyLocked()
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-m.ctx.Done():
			return nil, ErrPreparedManagerUnavailable
		case <-changed:
		}
	}
}

func (m *PreparedManager) ClearPool() error {
	if m == nil || m.self != m {
		return ErrPreparedManagerUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.ctx.Err() != nil {
		return ErrPreparedManagerUnavailable
	}
	m.generation++
	if m.buildCancel != nil {
		m.buildCancel()
	}
	if m.pool != nil {
		m.retiring, m.pool = m.pool, nil
		m.retireCancel, m.poolCancel = m.poolCancel, nil
		if m.retireCancel != nil {
			m.retireCancel()
		}
	}
	m.build, m.lastErr = false, nil
	m.notifyLocked()
	return nil
}

func (m *PreparedManager) HasPool() bool {
	if m == nil || m.self != m {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active && !m.closed && m.ctx.Err() == nil && m.pool != nil
}

func (m *PreparedManager) AvailableConnections() int {
	if m == nil || m.self != m {
		return 0
	}
	m.mu.Lock()
	current := m.pool
	usable := m.active && !m.closed && m.ctx.Err() == nil
	m.mu.Unlock()
	if !usable || current == nil {
		return 0
	}
	free := 0
	for _, provider := range current.GetMetricsSnapshot().ProviderMetrics {
		if available := int(provider.MaxConnections - provider.ActiveConnections); available > 0 {
			free += available
		}
	}
	return free
}

func (m *PreparedManager) run() {
	defer m.workers.Done()
	defer func() {
		m.mu.Lock()
		current, retiring := m.pool, m.retiring
		cancelCurrent, cancelRetiring := m.poolCancel, m.retireCancel
		m.pool, m.retiring = nil, nil
		m.poolCancel, m.retireCancel = nil, nil
		m.notifyLocked()
		m.mu.Unlock()
		if cancelCurrent != nil {
			cancelCurrent()
		}
		if cancelRetiring != nil {
			cancelRetiring()
		}
		if current != nil {
			_ = nntppool.JoinConnectionPoolCleanup(current)
		}
		if retiring != nil {
			_ = nntppool.JoinConnectionPoolCleanup(retiring)
		}
	}()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
		}
		for {
			m.mu.Lock()
			retiring := m.retiring
			cancelRetiring := m.retireCancel
			m.retiring = nil
			m.retireCancel = nil
			build := m.build
			m.build = false
			m.building = build && len(m.providers) != 0
			generation := m.generation
			providers := append([]nntppool.UsenetProviderConfig(nil), m.providers...)
			m.mu.Unlock()
			if cancelRetiring != nil {
				cancelRetiring()
			}
			if retiring != nil {
				_ = nntppool.JoinConnectionPoolCleanup(retiring)
			}
			if m.ctx.Err() != nil {
				return
			}
			if !build || len(providers) == 0 {
				break
			}
			buildContext, cancelBuild := context.WithCancel(m.ctx)
			m.mu.Lock()
			m.buildCancel = cancelBuild
			if m.generation != generation || m.closed {
				cancelBuild()
			}
			m.mu.Unlock()
			current, err := nntppool.NewConnectionPoolWithContext(buildContext, nntppool.Config{Providers: providers, Logger: slog.Default(), DelayType: nntppool.DelayTypeFixed, RetryDelay: 10 * time.Millisecond, MinConnections: 2})
			m.mu.Lock()
			m.building = false
			m.buildCancel = nil
			publish := !m.closed && m.ctx.Err() == nil && buildContext.Err() == nil && generation == m.generation && err == nil
			if publish {
				m.pool = current
				m.poolCancel = cancelBuild
			} else if generation == m.generation {
				m.lastErr = err
			}
			m.notifyLocked()
			pending := m.build || m.retiring != nil
			m.mu.Unlock()
			if !publish {
				cancelBuild()
			}
			if !publish && current != nil {
				_ = nntppool.JoinConnectionPoolCleanup(current)
			}
			if !pending {
				break
			}
		}
	}
}

func (m *PreparedManager) Close() error {
	if m == nil || m.self != m {
		return ErrPreparedManagerUnavailable
	}
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.cancel()
		m.notifyLocked()
		m.mu.Unlock()
		m.workers.Wait()
	})
	return nil
}

// manager implements the Manager interface
type manager struct {
	mu         sync.RWMutex
	pool       nntppool.UsenetConnectionPool
	generation uint64
	newPool    func(nntppool.Config) (nntppool.UsenetConnectionPool, error)
	// providers is the last configured set, retained so a cleared pool can be
	// rebuilt lazily by GetPool — the latency cold-flush clears the pool and
	// resolution must be able to reconstitute it without a settings save.
	providers []nntppool.UsenetProviderConfig
	// rebuildDone signals completion of the in-flight async build launched by
	// SetProviders, so concurrent GetPool callers wait for it instead of
	// starting a second build. Nil when no build is in flight.
	rebuildDone chan struct{}
}

// NewManager creates a new pool manager
func NewManager() Manager {
	return &manager{newPool: newConnectionPool}
}

func newConnectionPool(config nntppool.Config) (nntppool.UsenetConnectionPool, error) {
	return nntppool.NewConnectionPool(config)
}

// GetPool returns the current connection pool, rebuilding it from the retained
// provider config if it was cleared (the latency cold-flush clears the pool and
// a settings save is not required to reconstitute it). If an async build from
// SetProviders is in flight, GetPool waits for it; otherwise it rebuilds
// synchronously — callers asking for the pool need it now.
func (m *manager) GetPool() (nntppool.UsenetConnectionPool, error) {
	for {
		m.mu.RLock()
		pool := m.pool
		hasProviders := len(m.providers) > 0
		m.mu.RUnlock()

		if pool != nil {
			return pool, nil
		}
		if !hasProviders {
			return nil, fmt.Errorf("NNTP connection pool not available - no providers configured")
		}

		m.mu.Lock()
		if m.pool != nil {
			m.mu.Unlock()
			return m.pool, nil
		}
		if m.rebuildDone == nil {
			// No async build in flight: rebuild synchronously (double-checked
			// under the write lock). Bump the generation so an async build
			// racing this caller (e.g. from a concurrent SetProviders) discards
			// itself instead of replacing this pool.
			m.generation++
			err := m.buildPoolLocked()
			pool := m.pool
			m.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return pool, nil
		}
		done := m.rebuildDone
		m.mu.Unlock()

		<-done
		// The waited build may have installed the pool, failed, or been
		// superseded (ClearPool). Clear the stale handle — but only if it is
		// still ours — and loop to re-check and possibly rebuild.
		m.mu.Lock()
		if m.rebuildDone == done {
			m.rebuildDone = nil
		}
		m.mu.Unlock()
	}
}

// SetProviders creates/recreates the pool with new providers
func (m *manager) SetProviders(providers []nntppool.UsenetProviderConfig) error {
	m.mu.Lock()
	m.generation++
	generation := m.generation
	oldPool := m.pool
	m.pool = nil
	// Retain the provider config so a cleared pool can be rebuilt lazily by
	// GetPool (cold-flush path). Copy because the caller retains ownership.
	providerSnapshot := append([]nntppool.UsenetProviderConfig(nil), providers...)
	m.providers = providerSnapshot
	// Publish the completion signal before launching so GetPool callers either
	// see the live pool or wait on this build.
	rebuildDone := make(chan struct{})
	m.rebuildDone = rebuildDone
	m.mu.Unlock()

	// Pool shutdown can itself wait on upstream background work, so do not make
	// settings saves wait for it.
	if oldPool != nil {
		slog.Info("Shutting down existing NNTP connection pool")
		go oldPool.Quit()
	}

	// Return early if no providers (clear pool scenario)
	if len(providers) == 0 {
		slog.Info("No NNTP providers configured - pool cleared")
		return nil
	}

	// nntppool verifies providers synchronously with a hardcoded 60-second
	// timeout. Build in the background so startup and settings saves return
	// immediately.
	go func() {
		defer close(rebuildDone)
		m.buildPool(generation, providerSnapshot)
	}()
	return nil
}

func (m *manager) buildPool(generation uint64, providers []nntppool.UsenetProviderConfig) {
	slog.Info("Creating NNTP connection pool in background", "provider_count", len(providers))
	pool, err := m.newPool(nntppool.Config{
		Providers:      providers,
		Logger:         slog.Default(),
		DelayType:      nntppool.DelayTypeFixed,
		RetryDelay:     10 * time.Millisecond,
		MinConnections: 2, // Keep 2 warm connections per provider for faster STAT commands
	})
	if err != nil {
		slog.Error("Failed to create NNTP connection pool", "error", err)
		return
	}

	m.mu.Lock()
	if generation != m.generation {
		m.mu.Unlock()
		slog.Info("Discarding superseded NNTP connection pool")
		pool.Quit()
		return
	}
	m.pool = pool
	m.mu.Unlock()
	slog.Info("NNTP connection pool created successfully")
}

// ClearPool shuts down and drops the live pool. Provider configuration is kept
// so the next GetPool recreates fresh connections — a cold pool, not a dead one.
func (m *manager) ClearPool() error {
	m.mu.Lock()
	m.generation++
	oldPool := m.pool
	m.pool = nil
	// An in-flight async build is now superseded (generation bump); drop its
	// wait handle so the next GetPool rebuilds instead of waiting on it.
	m.rebuildDone = nil
	m.mu.Unlock()

	if oldPool != nil {
		slog.Info("Clearing NNTP connection pool")
		go oldPool.Quit()
	}

	return nil
}

// HasPool returns true if a pool is currently available
func (m *manager) HasPool() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.pool != nil
}

// AvailableConnections reports how many connection slots are currently free
// across all providers (configured budget minus in-use). It reads the live
// pool's metrics snapshot; with no live pool it returns 0 so callers fall back
// to their conservative floor. The snapshot is taken once per call and is only
// an instantaneous view — callers should treat it as a hint, not a lease.
func (m *manager) AvailableConnections() int {
	m.mu.RLock()
	pool := m.pool
	m.mu.RUnlock()
	if pool == nil {
		return 0
	}

	snap := pool.GetMetricsSnapshot()
	var free int
	for _, p := range snap.ProviderMetrics {
		if n := int(p.MaxConnections - p.ActiveConnections); n > 0 {
			free += n
		}
	}
	return free
}

// buildPoolLocked builds the pool synchronously under the write lock using the
// retained provider config (the lazy-rebuild path from GetPool). SetProviders
// uses the async buildPool instead.
func (m *manager) buildPoolLocked() error {
	// Create new pool with providers
	// Keep MinConnections > 0 to maintain warm connections for faster health checks
	// MaxConnections is set per-provider from user config (UsenetSettings.Connections)
	slog.Info("Creating NNTP connection pool", "provider_count", len(m.providers))
	pool, err := m.newPool(nntppool.Config{
		Providers:      m.providers,
		Logger:         slog.Default(),
		DelayType:      nntppool.DelayTypeFixed,
		RetryDelay:     10 * time.Millisecond,
		MinConnections: 2, // Keep 2 warm connections per provider for faster STAT commands
	})
	if err != nil {
		return fmt.Errorf("failed to create NNTP connection pool: %w", err)
	}

	m.pool = pool
	slog.Info("NNTP connection pool created successfully")
	return nil
}
