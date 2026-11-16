// Package livecapacity arbitrates provider connections across Live TV, probes,
// and recordings in one server process. A lease is retained until its input has
// closed, including while a live session is reconnecting.
package livecapacity

import (
	"errors"
	"sync"
	"time"
)

var ErrLimit = errors.New("live provider connection limit reached")

type automaticLimitError struct{}

func (*automaticLimitError) Error() string { return ErrLimit.Error() }
func (*automaticLimitError) Unwrap() error { return ErrLimit }

var errAutomaticLimit error = &automaticLimitError{}

var Default = NewManager()

// AutomaticLimit is an ephemeral, server-observed extra cap, not a persisted
// manual limit or an estimate of connections used outside this process.
type AutomaticLimit struct {
	Limit     int
	ExpiresAt time.Time
}

type Manager struct {
	mu                   sync.Mutex
	buckets              map[string]*bucket
	next                 uint64
	owners               map[*Lease]Owner
	claims               map[string]*handoff
	confirmations        map[string]confirmation
	draining             map[uint64]bool
	revoked              map[string]time.Time
	admitted             map[string]time.Time
	changed              chan struct{}
	automaticLimitReader func(string) AutomaticLimit
}

type bucket struct {
	limit       int
	connections map[uint64]int
	shared      map[string]uint64
}

type Lease struct {
	manager    *Manager
	bucket     string
	connection uint64
	released   bool
}

func NewManager() *Manager {
	return &Manager{buckets: make(map[string]*bucket), owners: make(map[*Lease]Owner),
		claims: make(map[string]*handoff), confirmations: make(map[string]confirmation), draining: make(map[uint64]bool), revoked: make(map[string]time.Time), admitted: make(map[string]time.Time), changed: make(chan struct{})}
}

// SetAutomaticLimitReader installs a cache-only server policy reader. It runs
// under the manager lock and must not call the manager, a provider, or callbacks
// that acquire this lock. Nil preserves the existing manual-only policy.
func (m *Manager) SetAutomaticLimitReader(reader func(string) AutomaticLimit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.automaticLimitReader = reader
}

func (m *Manager) effectiveLimitLocked(key string, manual int) (int, AutomaticLimit) {
	if m.automaticLimitReader != nil {
		fact := m.automaticLimitReader(key)
		now := time.Now()
		if fact.Limit > 0 && fact.Limit <= 1_000_000 && fact.ExpiresAt.After(now) && !fact.ExpiresAt.After(now.Add(time.Minute)) && (manual <= 0 || fact.Limit < manual) {
			return fact.Limit, fact
		}
	}
	return manual, AutomaticLimit{}
}

func (m *Manager) automaticBlocksTakeoverLocked(key string) bool {
	if b := m.buckets[key]; b != nil {
		_, automatic := m.effectiveLimitLocked(key, b.limit)
		return automatic.Limit > 0
	}
	return false
}

// Acquire atomically reserves a connection. Only consumers actually using the
// shared upstream hub may supply a share key; private inputs use an empty key.
// Zero preserves the existing unconfigured-limit policy. A stricter limit from
// an alias applies to the whole bucket until it becomes idle.
func (m *Manager) Acquire(key, shareKey string, limit int) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireLocked(key, shareKey, limit, Owner{})
}

func (m *Manager) acquireLocked(key, shareKey string, limit int, owner Owner) (*Lease, error) {
	if owner.RevocationKey != "" && m.revokedLocked(owner.RevocationKey, time.Now()) {
		return nil, ErrTakenOver
	}
	b := m.buckets[key]
	if b == nil {
		b = &bucket{connections: make(map[uint64]int), shared: make(map[string]uint64)}
		m.buckets[key] = b
	}
	if limit > 0 && (b.limit == 0 || limit < b.limit) {
		b.limit = limit
	}
	if id, ok := b.shared[shareKey]; shareKey != "" && ok {
		if m.draining[id] {
			return nil, ErrLimit
		}
		b.connections[id]++
		return m.leaseLocked(key, id, owner), nil
	}
	reserved := m.reservedLocked(key, b)
	effective, automatic := m.effectiveLimitLocked(key, b.limit)
	if effective > 0 && len(b.connections)+reserved >= effective {
		if automatic.Limit > 0 {
			return nil, errAutomaticLimit
		}
		return nil, ErrLimit
	}
	m.next++
	id := m.next
	b.connections[id] = 1
	if shareKey != "" {
		b.shared[shareKey] = id
	}
	return m.leaseLocked(key, id, owner), nil
}

// MakePrivate corrects an optimistic TS share when the provider unexpectedly
// returns HLS. Each FFmpeg then opens its own input and needs a separate slot.
// No provider input may open if this conversion fails.
func (l *Lease) MakePrivate() error {
	if l == nil {
		return nil
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released {
		return ErrLimit
	}
	b := m.buckets[l.bucket]
	if m.draining[l.connection] {
		return ErrLimit
	}
	if b.connections[l.connection] > 1 {
		effective, _ := m.effectiveLimitLocked(l.bucket, b.limit)
		if effective > 0 && len(b.connections)+m.reservedLocked(l.bucket, b) >= effective {
			return ErrLimit
		}
		b.connections[l.connection]--
		m.next++
		l.connection = m.next
		b.connections[l.connection] = 1
	}
	for key, id := range b.shared {
		if id == l.connection {
			delete(b.shared, key)
		}
	}
	return nil
}

func (l *Lease) Release() {
	if l == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	owner := m.owners[l]
	m.markAdmittedLocked(owner.RevocationKey, time.Now())
	delete(m.owners, l)
	b := m.buckets[l.bucket]
	b.connections[l.connection]--
	if b.connections[l.connection] == 0 {
		delete(b.connections, l.connection)
		delete(m.draining, l.connection)
		for key, id := range b.shared {
			if id == l.connection {
				delete(b.shared, key)
			}
		}
	}
	if len(b.connections) == 0 && m.claims[l.bucket] == nil {
		delete(m.buckets, l.bucket)
	}
	m.notifyLocked()
}

func (m *Manager) Current(key string) int {
	current, _ := m.Usage(key, 0)
	return current
}

// Usage includes reservations still opening or shutting down. Report the same
// effective limit that admission enforces, including a stricter active alias.
func (m *Manager) Usage(key string, limit int) (int, int) {
	current, effective, _ := m.UsageWithAutomaticLimit(key, limit)
	return current, effective
}

// UsageWithAutomaticLimit returns the actual applied policy under one lock.
// A positive checked report tied with a sticky manual cap is still manual.
func (m *Manager) UsageWithAutomaticLimit(key string, limit int) (int, int, AutomaticLimit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := 0
	if b := m.buckets[key]; b != nil {
		current = len(b.connections)
		if b.limit > 0 && (limit <= 0 || b.limit < limit) {
			limit = b.limit
		}
	}
	effective, automatic := m.effectiveLimitLocked(key, limit)
	return current, effective, automatic
}
