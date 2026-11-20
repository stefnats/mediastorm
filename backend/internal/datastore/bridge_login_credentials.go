package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrBridgeCredentialUnavailable = errors.New("current bridge credentials unavailable")
	ErrBridgeCredentialChanged     = errors.New("original bridge credential observation changed")
	ErrBridgeCredentialRejected    = errors.New("bridge credential comparison rejected")
	ErrBridgeCredentialBusy        = errors.New("bridge credential work capacity unavailable")
	ErrBridgeCredentialUsed        = errors.New("bridge credential intent already used")
)

type bridgeCredentialKind uint8

const (
	bridgeCredentialAccount bridgeCredentialKind = iota + 1
	bridgeCredentialCombined
	bridgeCredentialPIN
)

type bridgeCredentialPhase uint8

const (
	bridgeCredentialPrepared bridgeCredentialPhase = iota + 1
	bridgeCredentialVerifying
	bridgeCredentialVerified
	bridgeCredentialRetired
)

// These server-owned values contain credential evidence only. They cannot mint
// a session, select configured login policy, delegate, or open a media source.
type BridgeLoginCredentialIntent struct{ state *bridgeCredentialState }
type BridgeLoginCredentialProof struct{ state *bridgeCredentialState }
type bridgeCredentialSnapshot struct {
	accountHash, pinHash, accountVersion, profileVersion, household string
	pinLength                                                       int
	settingsGeneration, profileGeneration                           int64
	accountExpiry                                                   time.Time
	hasAccountExpiry                                                bool
}
type bridgeCredentialState struct {
	mu                       sync.Mutex
	self                     *BridgeLoginCredentialIntent
	proof                    *BridgeLoginCredentialProof
	store                    *DataStore
	request                  *http.Request
	requestContext           context.Context
	account, profile, client string
	binding                  [32]byte
	snapshot                 bridgeCredentialSnapshot
	ctx                      context.Context
	cancel                   context.CancelFunc
	expiry                   time.Time
	setupStop                func() bool
	setupDone                chan struct{}
	phase                    bridgeCredentialPhase
	kind                     bridgeCredentialKind
	closed                   bool
	operations               int
	waitingCPU               bool
	pending                  sync.WaitGroup
	once                     sync.Once
	// Always the real bcrypt verifier; never an argument supplied by a caller.
	compare func([]byte, []byte) error
}

var bridgeCredentialAdmission = struct {
	sync.Mutex
	intents    map[*BridgeLoginCredentialIntent]string
	accounts   map[string]int
	operations int
}{intents: map[*BridgeLoginCredentialIntent]string{}, accounts: map[string]int{}}
var bridgeCredentialCPU = make(chan struct{}, 2)

func (BridgeLoginCredentialIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginCredentialIntent) UnmarshalJSON([]byte) error  { return ErrBridgeCredentialChanged }
func (BridgeLoginCredentialIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginCredentialIntent(opaque)"))
}
func (BridgeLoginCredentialProof) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginCredentialProof) UnmarshalJSON([]byte) error  { return ErrBridgeCredentialChanged }
func (BridgeLoginCredentialProof) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginCredentialProof(opaque)"))
}
func (i *BridgeLoginCredentialIntent) valid() bool {
	return i != nil && i.state != nil && i.state.self == i
}
func (p *BridgeLoginCredentialProof) valid() bool {
	return p != nil && p.state != nil && p.state.proof == p && p.state.self != nil
}
func (i *BridgeLoginCredentialIntent) Context() context.Context {
	if !i.valid() {
		return familyMediaClosedContext()
	}
	return i.state.ctx
}
func (i *BridgeLoginCredentialIntent) ExpiresAt() time.Time {
	if !i.valid() {
		return time.Time{}
	}
	return i.state.expiry
}
func (p *BridgeLoginCredentialProof) Context() context.Context {
	if !p.valid() {
		return familyMediaClosedContext()
	}
	return p.state.ctx
}
func (p *BridgeLoginCredentialProof) ExpiresAt() time.Time {
	if !p.valid() {
		return time.Time{}
	}
	return p.state.expiry
}

func bridgeCredentialRequestBinding(r *http.Request, client string) ([32]byte, error) {
	var zero [32]byte
	if r == nil || r.URL == nil || r.Context().Err() != nil || len(client) > 256 || !utf8.ValidString(client) || strings.TrimSpace(client) != client || len(r.Method) > 16 || len(r.Host) > 256 || len(r.RemoteAddr) > 256 || len(r.URL.EscapedPath())+len(r.URL.RawPath) > 4096 || len(r.URL.RawQuery) > 8192 {
		return zero, ErrBridgeCredentialChanged
	}
	names := []string{"Authorization", "X-Emby-Authorization", "X-Emby-Token", "X-MediaBrowser-Token", "X-Client-ID", "User-Agent", "X-Emby-Client", "X-Emby-Device-Id", "X-Emby-Device-Name", "X-Emby-Client-Version", "Origin"}
	selected := map[string][][]byte{}
	total := 0
	for _, name := range names {
		for key, values := range r.Header {
			if !strings.EqualFold(key, name) {
				continue
			}
			if _, exists := selected[name]; exists || len(values) != 1 {
				return zero, ErrBridgeCredentialChanged
			}
			total += len(name) + len(values[0])
			if total > 4096 {
				return zero, ErrBridgeCredentialChanged
			}
			selected[name] = [][]byte{[]byte(values[0])}
		}
	}
	vars := mux.Vars(r)
	if len(vars) > 32 {
		return zero, ErrBridgeCredentialChanged
	}
	keys := make([]string, 0, len(vars))
	for key, value := range vars {
		total += len(key) + len(value)
		if len(key) > 256 || len(value) > 256 || total > 4096 {
			return zero, ErrBridgeCredentialChanged
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([][2][]byte, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, [2][]byte{[]byte(key), []byte(vars[key])})
	}
	// Byte slices preserve arbitrary original carrier bytes. JSON strings would
	// replace invalid UTF-8 with the same replacement rune and hide drift.
	raw, err := json.Marshal([]any{[]byte(r.Method), []byte(r.Host), []byte(r.RemoteAddr), []byte(r.URL.EscapedPath()), []byte(r.URL.RawPath), []byte(r.URL.RawQuery), []byte(client), selected, ordered})
	if err != nil {
		return zero, ErrBridgeCredentialChanged
	}
	return sha256.Sum256(raw), nil
}
func bridgeCredentialHashSupported(hash string) bool {
	// Explicitly support Go's legacy $2$ and current $2a$/$2b$/$2y$
	// layouts. The library parser otherwise ignores trailing checksum bytes
	// and accepts unreviewed version, delimiter, and base64 forms.
	costStart := 0
	switch {
	case len(hash) == 59 && strings.HasPrefix(hash, "$2$"):
		costStart = 3
	case len(hash) == 60 && (strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2b$") || strings.HasPrefix(hash, "$2y$")):
		costStart = 4
	default:
		return false
	}
	if hash[costStart] < '0' || hash[costStart] > '9' || hash[costStart+1] < '0' || hash[costStart+1] > '9' || hash[costStart+2] != '$' {
		return false
	}
	const alphabet = "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	encoded := hash[costStart+3:]
	for n := range len(encoded) {
		if strings.IndexByte(alphabet, encoded[n]) < 0 {
			return false
		}
	}
	cost, err := bcrypt.Cost([]byte(hash))
	return err == nil && cost >= 4 && cost <= 14
}
func sameBridgeCredentialSnapshot(a, b bridgeCredentialSnapshot) bool {
	return a.accountHash == b.accountHash && a.pinHash == b.pinHash && a.accountVersion == b.accountVersion && a.profileVersion == b.profileVersion && a.household == b.household && a.pinLength == b.pinLength && a.settingsGeneration == b.settingsGeneration && a.profileGeneration == b.profileGeneration && a.hasAccountExpiry == b.hasAccountExpiry && a.accountExpiry.Equal(b.accountExpiry)
}
func (ds *DataStore) readBridgeCredentialSnapshot(ctx context.Context, account, profile string) (out bridgeCredentialSnapshot, err error) {
	phase, finish := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer finish()
	household, err := ds.readFamilyScopedHousehold(phase, account)
	if err != nil || household.HasChildren {
		return out, ErrBridgeCredentialUnavailable
	}
	read := func() (bridgeCredentialSnapshot, error) {
		var value bridgeCredentialSnapshot
		var expiry *time.Time
		var child bool
		err := ds.pool.QueryRow(phase, `SELECT
 CASE WHEN octet_length(a.password_hash)<=128 THEN a.password_hash ELSE NULL END,
 CASE WHEN octet_length(u.pin_hash)<=128 THEN u.pin_hash ELSE NULL END,u.pin_length,
 concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),
 a.expires_at,u.is_kids_profile,v.generation,
 COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key=u.id),0)
 FROM accounts a JOIN users u ON u.account_id=a.id JOIN user_settings_versions v ON v.user_id=u.id
 WHERE a.id=$1 AND u.id=$2`, account, profile).Scan(&value.accountHash, &value.pinHash, &value.pinLength, &value.accountVersion, &value.profileVersion, &expiry, &child, &value.settingsGeneration, &value.profileGeneration)
		if err != nil || child || value.settingsGeneration < 0 || value.profileGeneration < 0 || expiry != nil && !expiry.After(time.Now()) || !bridgeCredentialHashSupported(value.accountHash) || value.pinHash != "" && !bridgeCredentialHashSupported(value.pinHash) || phase.Err() != nil {
			return value, ErrBridgeCredentialUnavailable
		}
		if expiry != nil {
			value.accountExpiry = *expiry
			value.hasAccountExpiry = true
		}
		return value, nil
	}
	first, err := read()
	if err != nil {
		return out, err
	}
	after, err := ds.readFamilyScopedHousehold(phase, account)
	if err != nil || household != after {
		return out, ErrBridgeCredentialChanged
	}
	last, err := read()
	if err != nil || !sameBridgeCredentialSnapshot(first, last) || phase.Err() != nil || first.hasAccountExpiry && !first.accountExpiry.After(time.Now()) {
		return out, ErrBridgeCredentialChanged
	}
	first.household = household.Revision
	return first, nil
}

// No authenticated Session is assumed or constructed. IDs and client metadata
// are original selectors/claims; only current PG hashes establish evidence.
func (ds *DataStore) PrepareBridgeLoginCredentials(r *http.Request, account, profile, client string) (intent *BridgeLoginCredentialIntent, err error) {
	if ds == nil || ds.pool == nil || !parentMutationID(account) || !parentMutationID(profile) {
		return nil, ErrBridgeCredentialUnavailable
	}
	binding, err := bridgeCredentialRequestBinding(r, client)
	if err != nil {
		return nil, err
	}
	expiry := time.Now().Add(30 * time.Second)
	if deadline, ok := r.Context().Deadline(); ok && deadline.Before(expiry) {
		expiry = deadline
	}
	life, cancel := context.WithDeadline(context.Background(), expiry)
	i := &BridgeLoginCredentialIntent{state: &bridgeCredentialState{store: ds, request: r, requestContext: r.Context(), account: account, profile: profile, client: client, binding: binding, ctx: life, cancel: cancel, expiry: expiry, phase: bridgeCredentialPrepared, setupDone: make(chan struct{}), compare: bcrypt.CompareHashAndPassword}}
	i.state.self = i
	bridgeCredentialAdmission.Lock()
	if len(bridgeCredentialAdmission.intents) >= 64 || bridgeCredentialAdmission.accounts[account] >= 8 {
		bridgeCredentialAdmission.Unlock()
		cancel()
		return nil, ErrBridgeCredentialBusy
	}
	bridgeCredentialAdmission.intents[i] = account
	bridgeCredentialAdmission.accounts[account]++
	bridgeCredentialAdmission.Unlock()
	i.state.setupStop = context.AfterFunc(r.Context(), func() { defer close(i.state.setupDone); cancel() })
	defer func() {
		if intent == nil {
			i.Close()
		}
	}()
	ctx, done, err := i.state.begin(r.Context())
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer done()
		snapshot, err := ds.readBridgeCredentialSnapshot(ctx, account, profile)
		if err != nil {
			return err
		}
		if snapshot.hasAccountExpiry && snapshot.accountExpiry.Before(expiry) {
			expiry = snapshot.accountExpiry
		}
		bounded, finish := context.WithDeadline(life, expiry)
		if ctx.Err() != nil || bounded.Err() != nil || r.Context().Err() != nil || !expiry.After(time.Now()) {
			finish()
			return ErrBridgeCredentialChanged
		}
		current, err := bridgeCredentialRequestBinding(r, client)
		if err != nil || current != binding {
			finish()
			return ErrBridgeCredentialChanged
		}
		i.state.snapshot = snapshot
		i.state.ctx = bounded
		i.state.expiry = expiry
		i.state.cancel = func() { finish(); cancel() }
		return nil
	}()
	if err != nil {
		return nil, err
	}
	return i, nil
}
func (s *bridgeCredentialState) begin(caller context.Context) (context.Context, func(), error) {
	if caller == nil || caller.Err() != nil {
		return nil, nil, ErrBridgeCredentialChanged
	}
	s.mu.Lock()
	if s.closed || s.phase == bridgeCredentialRetired || s.ctx.Err() != nil || s.requestContext.Err() != nil {
		s.mu.Unlock()
		return nil, nil, ErrBridgeCredentialChanged
	}
	if s.operations >= 2 {
		s.mu.Unlock()
		return nil, nil, ErrBridgeCredentialBusy
	}
	bridgeCredentialAdmission.Lock()
	if bridgeCredentialAdmission.operations >= 128 {
		bridgeCredentialAdmission.Unlock()
		s.mu.Unlock()
		return nil, nil, ErrBridgeCredentialBusy
	}
	bridgeCredentialAdmission.operations++
	bridgeCredentialAdmission.Unlock()
	s.operations++
	s.pending.Add(1)
	life := s.ctx
	s.mu.Unlock()
	ctx, cancel := context.WithDeadline(caller, s.expiry)
	stop := familyMediaAfterFunc(life, cancel)
	stopRequest := familyMediaAfterFunc(s.requestContext, cancel)
	var once sync.Once
	done := func() {
		once.Do(func() {
			cancel()
			stopRequest()
			stop()
			bridgeCredentialAdmission.Lock()
			bridgeCredentialAdmission.operations--
			bridgeCredentialAdmission.Unlock()
			s.mu.Lock()
			s.operations--
			s.mu.Unlock()
			s.pending.Done()
		})
	}
	return ctx, done, nil
}
func (s *bridgeCredentialState) matchesRequest(r *http.Request) bool {
	if r == nil || r != s.request || r.Context() != s.requestContext || s.requestContext.Err() != nil {
		return false
	}
	binding, err := bridgeCredentialRequestBinding(r, s.client)
	return err == nil && binding == s.binding
}
func (s *bridgeCredentialState) retire() {
	s.mu.Lock()
	s.phase = bridgeCredentialRetired
	s.cancel()
	s.mu.Unlock()
}
func (s *bridgeCredentialState) check(ctx context.Context) error {
	if !s.matchesRequest(s.request) || s.ctx.Err() != nil || !s.expiry.After(time.Now()) {
		return ErrBridgeCredentialChanged
	}
	current, err := s.store.readBridgeCredentialSnapshot(ctx, s.account, s.profile)
	if err != nil || !sameBridgeCredentialSnapshot(current, s.snapshot) || ctx.Err() != nil || s.ctx.Err() != nil || !s.matchesRequest(s.request) || !s.expiry.After(time.Now()) {
		return ErrBridgeCredentialChanged
	}
	return nil
}
func (i *BridgeLoginCredentialIntent) Check(caller context.Context) error {
	if !i.valid() {
		return ErrBridgeCredentialChanged
	}
	ctx, done, err := i.state.begin(caller)
	if err != nil {
		return err
	}
	defer done()
	err = i.state.check(ctx)
	if err != nil && caller.Err() == nil {
		i.state.retire()
	}
	return err
}
func (p *BridgeLoginCredentialProof) Check(caller context.Context) error {
	if !p.valid() {
		return ErrBridgeCredentialChanged
	}
	ctx, done, err := p.state.begin(caller)
	if err != nil {
		return err
	}
	defer done()
	p.state.mu.Lock()
	verified := p.state.phase == bridgeCredentialVerified
	p.state.mu.Unlock()
	if !verified {
		return ErrBridgeCredentialUsed
	}
	err = p.state.check(ctx)
	if err != nil && caller.Err() == nil {
		p.state.retire()
	}
	return err
}
func (ds *DataStore) VerifyBridgeAccountPassword(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	return ds.verifyBridgeCredential(r, i, raw, bridgeCredentialAccount)
}
func (ds *DataStore) VerifyBridgePasswordWithPIN(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	return ds.verifyBridgeCredential(r, i, raw, bridgeCredentialCombined)
}
func (ds *DataStore) VerifyBridgeProfilePIN(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	return ds.verifyBridgeCredential(r, i, raw, bridgeCredentialPIN)
}
func (ds *DataStore) verifyBridgeCredential(r *http.Request, i *BridgeLoginCredentialIntent, raw string, kind bridgeCredentialKind) (proof *BridgeLoginCredentialProof, err error) {
	if !i.valid() || ds == nil || i.state.store != ds || !i.state.matchesRequest(r) {
		return nil, ErrBridgeCredentialChanged
	}
	s := i.state
	ctx, done, err := s.begin(r.Context())
	if err != nil {
		return nil, err
	}
	defer done()
	s.mu.Lock()
	if s.phase != bridgeCredentialPrepared {
		s.mu.Unlock()
		return nil, ErrBridgeCredentialUsed
	}
	s.phase = bridgeCredentialVerifying
	s.mu.Unlock()
	defer func() {
		if proof == nil {
			s.retire()
		}
	}()
	if len(raw) == 0 || len(raw) > 1024 {
		return nil, ErrBridgeCredentialRejected
	}
	if err = s.check(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.waitingCPU = true
	s.mu.Unlock()
	select {
	case bridgeCredentialCPU <- struct{}{}:
	case <-ctx.Done():
		s.mu.Lock()
		s.waitingCPU = false
		s.mu.Unlock()
		return nil, ErrBridgeCredentialChanged
	}
	s.mu.Lock()
	s.waitingCPU = false
	s.mu.Unlock()
	err = func() error {
		defer func() { <-bridgeCredentialCPU }()
		compare := func(hash, password string) bool {
			if ctx.Err() != nil || s.ctx.Err() != nil || s.requestContext.Err() != nil {
				return false
			}
			bytes := []byte(password)
			defer clear(bytes)
			result := s.compare([]byte(hash), bytes)
			return result == nil && ctx.Err() == nil && s.ctx.Err() == nil && s.requestContext.Err() == nil
		}
		switch kind {
		case bridgeCredentialAccount:
			password := strings.TrimSpace(raw)
			if password != "" && compare(s.snapshot.accountHash, password) {
				return nil
			}
		case bridgeCredentialPIN:
			if s.snapshot.pinHash != "" && compare(s.snapshot.pinHash, raw) {
				return nil
			}
		case bridgeCredentialCombined:
			if s.snapshot.pinHash == "" {
				break
			}
			widths := []int{s.snapshot.pinLength}
			if s.snapshot.pinLength <= 0 {
				widths = []int{4, 5, 6, 7, 8}
			}
			for _, width := range widths {
				if width <= 0 || len(raw) <= width {
					continue
				}
				// Split exact raw bytes before trimming only the account-password prefix.
				password, pin := strings.TrimSpace(raw[:len(raw)-width]), raw[len(raw)-width:]
				if password != "" && compare(s.snapshot.pinHash, pin) && compare(s.snapshot.accountHash, password) {
					return nil
				}
			}
		}
		return ErrBridgeCredentialRejected
	}()
	if err != nil {
		return nil, err
	}
	if err = s.check(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.phase != bridgeCredentialVerifying || ctx.Err() != nil || s.ctx.Err() != nil || !s.matchesRequest(r) || !s.expiry.After(time.Now()) {
		return nil, ErrBridgeCredentialChanged
	}
	proof = &BridgeLoginCredentialProof{state: s}
	s.proof = proof
	s.kind = kind
	s.phase = bridgeCredentialVerified
	return proof, nil
}
func (i *BridgeLoginCredentialIntent) Close() {
	if i.valid() {
		i.state.close()
	}
}
func (p *BridgeLoginCredentialProof) Close() {
	if p.valid() {
		p.state.close()
	}
}
func (s *bridgeCredentialState) close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		stop := s.setupStop
		s.setupStop = nil
		s.mu.Unlock()
		if stop != nil && stop() {
			close(s.setupDone)
		}
		<-s.setupDone
		s.pending.Wait()
		s.mu.Lock()
		s.snapshot.accountHash = ""
		s.snapshot.pinHash = ""
		s.phase = bridgeCredentialRetired
		s.mu.Unlock()
		bridgeCredentialAdmission.Lock()
		if account, ok := bridgeCredentialAdmission.intents[s.self]; ok {
			delete(bridgeCredentialAdmission.intents, s.self)
			bridgeCredentialAdmission.accounts[account]--
			if bridgeCredentialAdmission.accounts[account] == 0 {
				delete(bridgeCredentialAdmission.accounts, account)
			}
		}
		bridgeCredentialAdmission.Unlock()
	})
}
