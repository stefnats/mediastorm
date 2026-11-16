package livecapacity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"time"
)

var (
	ErrTakenOver           = errors.New("This IPTV playback was taken over by another permitted viewer. Select Play to request a new slot.")
	ErrPermission          = errors.New("live takeover permission is no longer available")
	ErrConfirmationExpired = errors.New("recording interruption confirmation expired; try again")
)

const (
	KindLive         = "live"
	KindRecording    = "recording"
	confirmationTTL  = 30 * time.Second
	maxConfirmations = 1024
)

// Owner comes from server-verified profile/account data. Stop only requests
// shutdown; the slot stays occupied until every actual input releases its lease.
type Owner struct {
	AccountID   string
	ProfileID   string
	Kind        string
	RecordingID string
	// RevocationKey identifies one playback attempt without provider URLs.
	RevocationKey string
	Stop          func(context.Context) error
}

// TakeoverPolicy is constructed by the server, never decoded from client JSON.
// Authorized must recheck current permissions; Intent binds confirmation to the
// exact requested tune. Empty identities and unregistered inputs are protected.
type TakeoverPolicy struct {
	AccountID      string
	ProfileID      string
	Intent         string
	AllowLive      bool
	AllowRecording bool
	Confirmation   string
	Authorized     func() bool
}

// ShutdownError keeps provider/database details out of client-visible errors.
type ShutdownError struct{ cause error }

func (e *ShutdownError) Error() string { return "The selected IPTV input could not be stopped safely" }
func (e *ShutdownError) Unwrap() error { return e.cause }

type ConfirmationRequired struct {
	Token          string
	RecordingCount int
}

func (e *ConfirmationRequired) Error() string {
	return "A recording must be interrupted; explicit confirmation is required"
}
func (e *ConfirmationRequired) Unwrap() error { return ErrLimit }

type handoff struct {
	victim uint64
	begun  bool
}
type confirmation struct {
	bucket  string
	account string
	profile string
	intent  string
	victim  uint64
	members map[*Lease]string
	expires time.Time
}

func (m *Manager) reservedLocked(key string, b *bucket) int {
	if claim := m.claims[key]; claim != nil && b.connections[claim.victim] == 0 {
		return 1
	}
	return 0
}

func (m *Manager) notifyLocked() { close(m.changed); m.changed = make(chan struct{}) }
func (m *Manager) leaseLocked(key string, id uint64, owner Owner) *Lease {
	lease := &Lease{manager: m, bucket: key, connection: id}
	m.owners[lease] = owner
	m.markAdmittedLocked(owner.RevocationKey, time.Now())
	m.notifyLocked()
	return lease
}

// SetStop enables an already reserved, owned input once its shutdown callback
// is ready. Anonymous Acquire leases remain protected; callers cannot relabel
// a connection or overwrite a previously registered callback.
func (l *Lease) SetStop(stop func(context.Context) error) bool {
	if l == nil || stop == nil {
		return false
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	owner, ok := m.owners[l]
	if !ok || l.released || owner.Stop != nil || owner.AccountID == "" || owner.ProfileID == "" {
		return false
	}
	owner.Stop = stop
	m.owners[l] = owner
	m.notifyLocked()
	return true
}

// AcquireOwned registers identity without granting preemption to scheduled jobs.
func (m *Manager) AcquireOwned(key, shareKey string, limit int, owner Owner) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireLocked(key, shareKey, limit, owner)
}

func (p TakeoverPolicy) permitted(owner Owner) bool {
	return p.AccountID != "" && p.ProfileID != "" && p.AccountID == owner.AccountID && p.ProfileID == owner.ProfileID && p.Intent != "" && owner.RevocationKey != "" && p.Authorized != nil && p.Authorized()
}

// AcquireWithTakeover prefers the oldest wholly owned live connection. A
// shared input can be selected only when every consumer is stoppable and in
// the same account. Recordings require a separate right and a short-lived,
// single-use confirmation tied to the exact actor, intent and consumers.
func (m *Manager) AcquireWithTakeover(ctx context.Context, key, shareKey string, limit int, owner Owner, policy TakeoverPolicy) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	previouslyAdmitted := m.wasAdmittedLocked(owner.RevocationKey, time.Now())
	lease, err := m.acquireLocked(key, shareKey, limit, owner)
	if err == nil {
		delete(m.confirmations, policy.Confirmation)
		m.mu.Unlock()
		return lease, nil
	}
	if !errors.Is(err, ErrLimit) || m.claims[key] != nil || previouslyAdmitted || errors.Is(err, errAutomaticLimit) || m.automaticBlocksTakeoverLocked(key) {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	if !policy.permitted(owner) {
		return nil, ErrLimit
	}

	m.mu.Lock()
	// The input may have closed or a peer admitted this attempt while
	// authorization was being checked. Automatic retries never preempt.
	previouslyAdmitted = m.wasAdmittedLocked(owner.RevocationKey, time.Now())
	lease, err = m.acquireLocked(key, shareKey, limit, owner)
	if err == nil {
		delete(m.confirmations, policy.Confirmation)
		m.mu.Unlock()
		return lease, nil
	}
	if !errors.Is(err, ErrLimit) {
		m.mu.Unlock()
		return nil, err
	}
	if m.claims[key] != nil || previouslyAdmitted || errors.Is(err, errAutomaticLimit) || m.automaticBlocksTakeoverLocked(key) {
		m.mu.Unlock()
		return nil, ErrLimit
	}
	// A newly lowered limit may already be exceeded. Stopping one input
	// would then still not make room; do not disrupt it without a usable slot.
	if b := m.buckets[key]; b != nil && b.limit > 0 && len(b.connections) > b.limit {
		m.mu.Unlock()
		return nil, ErrLimit
	}
	victim, members, recordings, selectErr := m.selectVictimLocked(key, policy, time.Now())
	if selectErr != nil {
		m.mu.Unlock()
		return nil, selectErr
	}
	if victim == 0 {
		m.mu.Unlock()
		return nil, ErrLimit
	}
	if recordings > 0 && policy.Confirmation == "" {
		token, tokenErr := m.confirmLocked(key, victim, members, policy, time.Now())
		m.mu.Unlock()
		if tokenErr != nil {
			return nil, tokenErr
		}
		return nil, &ConfirmationRequired{Token: token, RecordingCount: recordings}
	}
	claim := &handoff{victim: victim}
	m.claims[key] = claim
	m.draining[victim] = true
	stops := make([]func(context.Context) error, 0, len(members))
	for lease := range members {
		stops = append(stops, m.owners[lease].Stop)
	}
	m.mu.Unlock()
	defer m.clearClaim(key, claim)

	for _, stop := range stops {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !policy.permitted(owner) {
			return nil, ErrPermission
		}
		// The policy callback may block or change the checked report/settings.
		// A new extra cap must be checked before revocation or any Stop call.
		m.mu.Lock()
		if m.automaticBlocksTakeoverLocked(key) {
			m.mu.Unlock()
			return nil, ErrLimit
		}
		if !claim.begun {
			claim.begun = true
			for lease := range members {
				m.revokeLocked(m.owners[lease].RevocationKey, time.Now())
			}
		}
		m.mu.Unlock()
		if err := stop(ctx); err != nil {
			return nil, &ShutdownError{cause: err}
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !policy.permitted(owner) {
			return nil, ErrPermission
		}
		m.mu.Lock()
		b := m.buckets[key]
		if b == nil || b.connections[victim] == 0 {
			delete(m.claims, key)
			lease, err := m.acquireLocked(key, shareKey, limit, owner)
			m.notifyLocked()
			m.mu.Unlock()
			return lease, err
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (m *Manager) clearClaim(key string, claim *handoff) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claims[key] != claim {
		return
	}
	delete(m.claims, key)
	if !claim.begun {
		delete(m.draining, claim.victim)
	}
	if b := m.buckets[key]; b != nil && len(b.connections) == 0 {
		delete(m.buckets, key)
	}
	m.notifyLocked()
}

func (m *Manager) membersLocked(key string, id uint64, p TakeoverPolicy) (map[*Lease]string, int, bool) {
	b := m.buckets[key]
	if b == nil {
		return nil, 0, false
	}
	members := make(map[*Lease]string)
	recordings := make(map[string]bool)
	for lease, owner := range m.owners {
		if lease.bucket != key || lease.connection != id || lease.released {
			continue
		}
		if owner.AccountID == "" || owner.AccountID != p.AccountID || owner.ProfileID == "" || owner.Stop == nil {
			return nil, 0, false
		}
		switch owner.Kind {
		case KindLive:
			if !p.AllowLive {
				return nil, 0, false
			}
		case KindRecording:
			if !p.AllowRecording || owner.RecordingID == "" {
				return nil, 0, false
			}
			recordings[owner.RecordingID] = true
		default:
			return nil, 0, false
		}
		members[lease] = owner.RecordingID
	}
	return members, len(recordings), len(members) > 0 && len(members) == b.connections[id]
}

func sameMembers(a, b map[*Lease]string) bool {
	if len(a) != len(b) {
		return false
	}
	for lease, value := range a {
		if other, ok := b[lease]; !ok || other != value {
			return false
		}
	}
	return true
}

func (m *Manager) selectVictimLocked(key string, p TakeoverPolicy, now time.Time) (uint64, map[*Lease]string, int, error) {
	if p.Confirmation != "" {
		c, ok := m.confirmations[p.Confirmation]
		delete(m.confirmations, p.Confirmation)
		if !ok || !now.Before(c.expires) || c.bucket != key || c.account != p.AccountID || c.profile != p.ProfileID || c.intent != p.Intent {
			return 0, nil, 0, ErrConfirmationExpired
		}
		members, count, allowed := m.membersLocked(key, c.victim, p)
		if !allowed || !sameMembers(members, c.members) {
			return 0, nil, 0, ErrConfirmationExpired
		}
		return c.victim, members, count, nil
	}
	b := m.buckets[key]
	if b == nil {
		return 0, nil, 0, nil
	}
	ids := make([]uint64, 0, len(b.connections))
	for id := range b.connections {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var recorded uint64
	var recordedMembers map[*Lease]string
	var recordedCount int
	for _, id := range ids {
		if m.draining[id] {
			continue
		}
		members, count, allowed := m.membersLocked(key, id, p)
		if !allowed {
			continue
		}
		if count == 0 {
			return id, members, 0, nil
		}
		if recorded == 0 {
			recorded = id
			recordedMembers = members
			recordedCount = count
		}
	}
	return recorded, recordedMembers, recordedCount, nil
}

func (m *Manager) confirmLocked(key string, id uint64, members map[*Lease]string, p TakeoverPolicy, now time.Time) (string, error) {
	for token, c := range m.confirmations {
		if !now.Before(c.expires) {
			delete(m.confirmations, token)
		}
	}
	if len(m.confirmations) >= maxConfirmations {
		var oldest string
		var expiry time.Time
		for token, c := range m.confirmations {
			if oldest == "" || c.expires.Before(expiry) {
				oldest = token
				expiry = c.expires
			}
		}
		delete(m.confirmations, oldest)
	}
	var random [24]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", errors.New("recording confirmation unavailable")
	}
	token := hex.EncodeToString(random[:])
	m.confirmations[token] = confirmation{bucket: key, account: p.AccountID, profile: p.ProfileID, intent: p.Intent, victim: id, members: members, expires: now.Add(confirmationTTL)}
	return token, nil
}

const maxRevocations = 4096
const revocationTTL = 30 * time.Minute

func (m *Manager) revokedLocked(key string, now time.Time) bool {
	for value, expires := range m.revoked {
		if !now.Before(expires) {
			delete(m.revoked, value)
		}
	}
	expires, ok := m.revoked[key]
	if ok && now.Before(expires) {
		m.revoked[key] = now.Add(revocationTTL)
		return true
	}
	return false
}

func (m *Manager) revokeLocked(key string, now time.Time) {
	if key == "" {
		return
	}
	m.revokedLocked(key, now)
	if len(m.revoked) >= maxRevocations {
		var oldest string
		var expiry time.Time
		for value, expires := range m.revoked {
			if oldest == "" || expires.Before(expiry) {
				oldest = value
				expiry = expires
			}
		}
		delete(m.revoked, oldest)
	}
	m.revoked[key] = now.Add(revocationTTL)
}

// One explicit Play may preempt at most once. Retunes can reuse a free/shared
// input but cannot displace a viewer or recording that acquired it meanwhile.
func (m *Manager) wasAdmittedLocked(key string, now time.Time) bool {
	if key == "" {
		return false
	}
	if expires, ok := m.admitted[key]; ok && now.Before(expires) {
		m.admitted[key] = now.Add(revocationTTL)
		return true
	}
	for lease, owner := range m.owners {
		if !lease.released && owner.RevocationKey == key {
			m.markAdmittedLocked(key, now)
			return true
		}
	}
	delete(m.admitted, key)
	return false
}

func (m *Manager) markAdmittedLocked(key string, now time.Time) {
	if key == "" {
		return
	}
	for value, expires := range m.admitted {
		if !now.Before(expires) {
			delete(m.admitted, value)
		}
	}
	if _, exists := m.admitted[key]; !exists && len(m.admitted) >= maxRevocations {
		var oldest string
		var expiry time.Time
		for value, expires := range m.admitted {
			if oldest == "" || expires.Before(expiry) {
				oldest = value
				expiry = expires
			}
		}
		delete(m.admitted, oldest)
	}
	m.admitted[key] = now.Add(revocationTTL)
}
