package datastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

// Preserve the existing wire header without depending on the HTTP guard package.
const profilePinParentPasswordHeader = "X-Account-Password"

var (
	ErrPGPinUnavailable    = errors.New("profile PIN issuance unavailable")
	ErrPGPinChanged        = errors.New("profile PIN issuance context changed")
	ErrPGPinCredential     = errors.New("profile PIN verification failed")
	ErrPGPinUsed           = errors.New("profile PIN attempt already retired")
	ErrPGPinOutcomeUnknown = errors.New("profile PIN issuance commit outcome unknown")
)

type pgPinPhase uint8

const (
	pgPinPrepared pgPinPhase = iota
	pgPinVerifying
	pgPinVerified
	pgPinCommitting
	pgPinRetired
)

// PGPinIssuer is a separate generation-bound PostgreSQL-only protocol. It never
// adopts a grant into the legacy cache or exposes a usable token. Its identity
// cannot be copied or exchanged for another issuer backed by the same store.
type PGPinIssuer struct {
	store   *DataStore
	self    *PGPinIssuer
	compare func([]byte, []byte) error
}

func NewPGPinIssuer(store *DataStore) *PGPinIssuer {
	v := &PGPinIssuer{store: store, compare: bcrypt.CompareHashAndPassword}
	v.self = v
	return v
}
func (v *PGPinIssuer) available() bool {
	return v != nil && v.self == v && v.store != nil && v.store.pool != nil
}

type pgPinAttempt struct {
	requestCtx                           context.Context
	mu                                   sync.Mutex
	issuer                               *PGPinIssuer
	intent                               *PGPinIntent
	proof                                *PGPinProof
	ctx                                  context.Context
	cancel                               context.CancelFunc
	deadline                             time.Time
	original                             models.Session
	snapshot                             parentMutationSnapshot
	profileGeneration, sessionGeneration int64
	pinHash                              string
	pinLength, verifiedLength            int
	ttl                                  time.Duration
	phase                                pgPinPhase
}

type PGPinIntent struct{ state *pgPinAttempt }
type PGPinProof struct{ state *pgPinAttempt }
type pgPinReceipt struct {
	requestCtx                           context.Context
	deadline                             time.Time
	beforeTargetVersion                  string
	releaseUsed                          bool
	cacheAckUsed                         bool
	mu                                   sync.Mutex
	issuer                               *PGPinIssuer
	self                                 *PGPinCommitReceipt
	original                             models.Session
	snapshot                             parentMutationSnapshot
	profileGeneration, sessionGeneration int64
	grant                                models.ProfileUnlock
	rawToken                             string
	closed                               bool
}

// PGPinCommitReceipt is historical commit evidence, not current permission.
// The receipt has no token, grant, expiry, adoption or publication accessor.
// The issuer can release once under the original request after current storage
// checks. The receipt is never authority to publish a legacy cache entry.
type PGPinCommitReceipt struct{ state *pgPinReceipt }

func (*PGPinIssuer) MarshalJSON() ([]byte, error)        { return []byte("null"), nil }
func (*PGPinIntent) MarshalJSON() ([]byte, error)        { return []byte("null"), nil }
func (*PGPinProof) MarshalJSON() ([]byte, error)         { return []byte("null"), nil }
func (*PGPinCommitReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGPinIssuer) Format(f fmt.State, _ rune)          { _, _ = f.Write([]byte("PGPinIssuer(opaque)")) }
func (*PGPinIntent) Format(f fmt.State, _ rune)          { _, _ = f.Write([]byte("PGPinIntent(opaque)")) }
func (*PGPinProof) Format(f fmt.State, _ rune)           { _, _ = f.Write([]byte("PGPinProof(opaque)")) }
func (*PGPinCommitReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("PGPinCommitReceipt(opaque)"))
}
func (v *PGPinIntent) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.intent == v {
		if s.phase != pgPinCommitting {
			s.phase = pgPinRetired
		}
		s.cancel()
	}
}
func (v *PGPinProof) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proof == v {
		if s.phase != pgPinCommitting {
			s.phase = pgPinRetired
		}
		s.cancel()
	}
}
func (v *PGPinCommitReceipt) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self == v {
		s.closed = true
		s.rawToken = ""
	}
}

// Prepare captures the actual server-authenticated original session, four
// authoritative rows and both revocation generations in one read-only MVCC
// snapshot. No SQL transaction or row lock survives the return to the caller.
func (v *PGPinIssuer) Prepare(r *http.Request, targetID string, configuredTTL time.Duration) (*PGPinIntent, error) {
	if !v.available() || r == nil || !parentMutationID(targetID) {
		return nil, ErrPGPinUnavailable
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	original, ok := auth.GetSession(r)
	if !ok || !parentMutationID(original.Token) || !parentMutationID(original.AccountID) || auth.GetAccountID(r) != original.AccountID || original.IsMaster != auth.IsMaster(r) || original.Scope != "" || original.ScopeResource != "" || !original.ExpiresAt.After(time.Now()) {
		return nil, ErrPGPinChanged
	}
	deadline := parentMutationMin(time.Now().Add(profileUnlockStateBudget), &original.ExpiresAt)
	if at, ok := r.Context().Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	tx, err := v.store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		reason := pinContextError(ctx, ErrPGPinUnavailable)
		cancel()
		return nil, reason
	}
	defer rollbackPinTx(tx, ctx)
	snapshot, dbNow, err := readParentMutationSnapshot(ctx, tx, original.Token, targetID)
	if err != nil || !parentMutationOriginal(snapshot, original) {
		reason := pinContextError(ctx, ErrPGPinChanged)
		cancel()
		return nil, reason
	}
	if frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame); frame != nil {
		if frame.store != v.store || frame.self != frame || frame.ctx.Err() != nil || !sameParentMutationRow(frame.sessionRow, snapshot.session) || !sameParentMutationRow(frame.actorRow, snapshot.parent) {
			cancel()
			return nil, ErrPGPinChanged
		}
	}
	// The stored session role is a cache. Its whole row still belongs to the
	// snapshot, while parentMutationOriginal checks the current actual actor.
	var target struct {
		Hash   string `json:"pin_hash"`
		Length int    `json:"pin_length"`
	}
	if json.Unmarshal(snapshot.target.raw, &target) != nil {
		cancel()
		return nil, ErrPGPinUnavailable
	}
	if _, err = bcrypt.Cost([]byte(target.Hash)); err != nil {
		cancel()
		return nil, ErrPGPinCredential
	}
	profileGeneration, sessionGeneration, err := readPinGenerations(ctx, tx, targetID, pinSessionLineage(snapshot.session.raw, original.Token))
	if err != nil {
		reason := pinContextError(ctx, ErrPGPinUnavailable)
		cancel()
		return nil, reason
	}
	deadline = parentMutationMin(deadline, &snapshot.sessionExpiry, snapshot.actorExpiry, snapshot.ownerExpiry)
	if !deadline.After(dbNow) || !deadline.After(time.Now()) {
		cancel()
		return nil, ErrPGPinChanged
	}
	if err = ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	// Release the read-only snapshot now, rather than holding it over bcrypt.
	if err = tx.Rollback(ctx); err != nil {
		reason := pinContextError(ctx, ErrPGPinUnavailable)
		cancel()
		return nil, reason
	}
	bounded, stop := context.WithDeadline(ctx, deadline)
	if err = bounded.Err(); err != nil {
		stop()
		cancel()
		return nil, err
	}
	state := &pgPinAttempt{requestCtx: r.Context(), issuer: v, ctx: bounded, cancel: func() { stop(); cancel() }, deadline: deadline, original: original, snapshot: snapshot, profileGeneration: profileGeneration, sessionGeneration: sessionGeneration, pinHash: target.Hash, pinLength: target.Length, ttl: configuredTTL, phase: pgPinPrepared}
	intent := &PGPinIntent{state: state}
	state.intent = intent
	return intent, nil
}

func rollbackPinTx(tx pgx.Tx, ctx context.Context) {
	rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(rollback)
}
func pinContextError(ctx context.Context, fallback error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fallback
}
func readPinGenerations(ctx context.Context, q parentMutationQuerier, profileID, sessionHash string) (int64, int64, error) {
	var profile, session int64
	err := q.QueryRow(ctx, `SELECT
        COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key=$1),0),
        COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='session' AND scope_key=$2),0)`, profileID, sessionHash).Scan(&profile, &session)
	if err == nil && (profile < 0 || session < 0) {
		err = ErrPGPinChanged
	}
	return profile, session, err
}

// Verify uses exactly the raw byte string accepted by legacy VerifyPin: no
// TrimSpace, numeric restriction or locale conversion. PinLength is byte count.
// Go bcrypt itself is not interruptible; cancellation/expiry rejects its result.
func (v *PGPinIssuer) Verify(intent *PGPinIntent, rawPIN string) (*PGPinProof, error) {
	if !v.available() || intent == nil || intent.state == nil {
		return nil, ErrPGPinUsed
	}
	s := intent.state
	s.mu.Lock()
	if s.issuer != v || s.intent != intent || s.phase != pgPinPrepared {
		s.mu.Unlock()
		return nil, ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		s.phase = pgPinRetired
		s.cancel()
		s.mu.Unlock()
		return nil, err
	}
	s.phase = pgPinVerifying
	hash := s.pinHash
	s.mu.Unlock()
	compared := v.compare([]byte(hash), []byte(rawPIN))
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		s.phase = pgPinRetired
		s.cancel()
		return nil, err
	}
	if s.phase != pgPinVerifying {
		s.cancel()
		return nil, ErrPGPinUsed
	}
	if compared != nil {
		s.phase = pgPinRetired
		s.cancel()
		return nil, ErrPGPinCredential
	}
	s.verifiedLength = len(rawPIN)
	proof := &PGPinProof{state: s}
	s.proof = proof
	s.phase = pgPinVerified
	return proof, nil
}

// Commit consumes one proof before IO. Scope generations are locked first in
// profile/session order, then sorted accounts, original session and target.
// Revokers take only their scope row followed by grants; no service lock joins
// this order. An empty revoke before Commit changes the expected generation.
func (v *PGPinIssuer) Commit(intent *PGPinIntent, proof *PGPinProof) (*PGPinCommitReceipt, error) {
	if !v.available() || intent == nil || intent.state == nil || proof == nil {
		return nil, ErrPGPinUsed
	}
	s := intent.state
	s.mu.Lock()
	if s.issuer != v || s.intent != intent || s.proof != proof || proof.state != s || s.phase != pgPinVerified {
		s.mu.Unlock()
		return nil, ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		s.phase = pgPinRetired
		s.cancel()
		s.mu.Unlock()
		return nil, err
	}
	s.phase = pgPinCommitting
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.phase = pgPinRetired; s.cancel(); s.mu.Unlock() }()
	tx, err := v.store.pool.Begin(s.ctx)
	if err != nil {
		return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	defer rollbackPinTx(tx, s.ctx)
	for _, scope := range []struct {
		name, key string
		expected  int64
	}{
		{"profile", s.snapshot.targetID, s.profileGeneration},
		{"session", pinSessionLineage(s.snapshot.session.raw, s.original.Token), s.sessionGeneration},
	} {
		if _, err = tx.Exec(s.ctx, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES($1,$2,0) ON CONFLICT(scope,scope_key) DO NOTHING`, scope.name, scope.key); err != nil {
			return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
		}
		var current int64
		if err = tx.QueryRow(s.ctx, `SELECT generation FROM profile_unlock_revocations WHERE scope=$1 AND scope_key=$2 FOR UPDATE`, scope.name, scope.key).Scan(&current); err != nil {
			return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
		}
		if current != scope.expected {
			return nil, ErrPGPinChanged
		}
	}
	ids := []string{s.snapshot.actorID}
	if s.snapshot.actorID != s.snapshot.ownerID {
		ids = append(ids, s.snapshot.ownerID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(s.ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil || found != id {
			return nil, pinContextError(s.ctx, ErrPGPinChanged)
		}
	}
	var found string
	if err = tx.QueryRow(s.ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil || found != s.original.Token {
		return nil, pinContextError(s.ctx, ErrPGPinChanged)
	}
	// Match targeted user persistence's table-before-user-row order. A topology
	// writer may hold SHARE ROW EXCLUSIVE while waiting for this same row;
	// acquiring ROW EXCLUSIVE only after taking the row would deadlock it.
	if s.pinLength != s.verifiedLength {
		if _, err = tx.Exec(s.ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
			return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
		}
	}
	if err = tx.QueryRow(s.ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.snapshot.targetID).Scan(&found); err != nil || found != s.snapshot.targetID {
		return nil, pinContextError(s.ctx, ErrPGPinChanged)
	}
	current, dbNow, err := readParentMutationSnapshot(s.ctx, tx, s.original.Token, s.snapshot.targetID)
	if err != nil || !sameParentMutationSnapshot(s.snapshot, current) || !parentMutationOriginal(current, s.original) {
		return nil, pinContextError(s.ctx, ErrPGPinChanged)
	}
	if !dbNow.Before(s.deadline) {
		return nil, ErrPGPinChanged
	}
	if s.pinLength != s.verifiedLength {
		err = tx.QueryRow(s.ctx, `UPDATE users AS u SET pin_length=$1,updated_at=clock_timestamp()
            WHERE id=$2 AND pin_hash=$3 AND concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)=$4
            RETURNING to_jsonb(u),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)`, s.verifiedLength, s.snapshot.targetID, s.pinHash, current.target.version).Scan(&current.target.raw, &current.target.version)
		if err != nil {
			return nil, pinContextError(s.ctx, ErrPGPinChanged)
		}
	}
	if err = tx.QueryRow(s.ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	if err = s.ctx.Err(); err != nil {
		return nil, err
	}
	if !dbNow.Before(s.deadline) {
		return nil, ErrPGPinChanged
	}
	expires := parentMutationMin(s.original.ExpiresAt, &s.snapshot.sessionExpiry, s.snapshot.actorExpiry, s.snapshot.ownerExpiry)
	if s.ttl > 0 {
		candidate := dbNow.Add(s.ttl)
		expires = parentMutationMin(expires, &candidate)
	}
	// PostgreSQL stores microseconds; rounding down never extends the promise.
	expires = expires.Truncate(time.Microsecond)
	if !expires.After(dbNow) {
		return nil, ErrPGPinChanged
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return nil, ErrPGPinUnavailable
	}
	token := "pu_" + base64.RawURLEncoding.EncodeToString(raw)
	fingerprint := sha256.Sum256([]byte(s.pinHash))
	grant := models.ProfileUnlock{TokenHash: profileUnlockDigest(token), SessionHash: profileUnlockDigest(s.original.Token), AccountID: s.original.AccountID, ProfileID: s.snapshot.targetID, PinFingerprint: hex.EncodeToString(fingerprint[:8]), SessionBound: s.ttl <= 0, ExpiresAt: expires, CreatedAt: dbNow}
	// No UPSERT: collision or constraint failure must not replace another grant.
	var inserted string
	verifiedHash := profileUnlockDigest(s.pinHash)
	binding := pinBindingDigest(grant, s.profileGeneration, s.sessionGeneration, verifiedHash, pinSessionLineage(s.snapshot.session.raw, s.original.Token))
	err = tx.QueryRow(s.ctx, `INSERT INTO profile_unlocks(`+profileUnlockColumns+`,`+pinBindingColumns+`)
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,1,$10,$11,$12,$13,$14 WHERE clock_timestamp()<$7 AND clock_timestamp()<$9
        RETURNING token_hash`, grant.TokenHash, grant.SessionHash, grant.AccountID, grant.ProfileID, grant.PinFingerprint, grant.SessionBound, grant.ExpiresAt, grant.CreatedAt, s.deadline, s.profileGeneration, s.sessionGeneration, verifiedHash, binding, pinSessionLineage(s.snapshot.session.raw, s.original.Token)).Scan(&inserted)
	if err != nil || inserted != grant.TokenHash {
		return nil, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	if err = s.ctx.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(s.ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return nil, ErrPGPinUnavailable
		}
		return nil, ErrPGPinOutcomeUnknown
	}
	state := &pgPinReceipt{requestCtx: s.requestCtx, deadline: s.deadline, beforeTargetVersion: s.snapshot.target.version, issuer: v, original: s.original, snapshot: current, profileGeneration: s.profileGeneration, sessionGeneration: s.sessionGeneration, grant: grant, rawToken: token}
	receipt := &PGPinCommitReceipt{state: state}
	state.self = receipt
	return receipt, nil
}

func samePinRequestContext(a, b context.Context) bool {
	left, _ := a.Value(pgRequestFrameKey{}).(*pgRequestFrame)
	right, _ := b.Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if left != nil || right != nil {
		return left != nil && left == right && left.self == left && left.ctx.Err() == nil
	}
	return a == b
}
func pinReceiptMatches(current profilePinAccessTuple, receipt *pgPinReceipt) bool {
	return current.profileGeneration == receipt.profileGeneration && current.sessionGeneration == receipt.sessionGeneration &&
		sameParentMutationRow(current.session, receipt.snapshot.session) && sameParentMutationRow(current.actor, receipt.snapshot.parent) &&
		sameParentMutationRow(current.profile, receipt.snapshot.target) && sameParentMutationRow(current.owner, receipt.snapshot.owner)
}

// Release is the only bounded token-output path. The receipt itself still has
// no token accessor. A point-in-time release is not durable permission: every
// public PG at-use gate must read current storage independently of this reply.
func (v *PGPinIssuer) Release(r *http.Request, value *PGPinCommitReceipt) (string, time.Time, error) {
	if !v.available() || r == nil || value == nil || value.state == nil {
		return "", time.Time{}, ErrPGPinUsed
	}
	s := value.state
	s.mu.Lock()
	claimed, ok := auth.GetSession(r)
	if s.issuer != v || s.self != value || s.closed || s.releaseUsed || !ok || claimed != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster || !samePinRequestContext(r.Context(), s.requestCtx) {
		s.mu.Unlock()
		return "", time.Time{}, ErrPGPinUsed
	}
	s.releaseUsed = true
	s.mu.Unlock()
	if err := s.requestCtx.Err(); err != nil {
		return "", time.Time{}, err
	}
	ctx, cancel := context.WithDeadline(r.Context(), s.deadline)
	defer cancel()
	current, outcome, _, err := readProfilePinAccess(ctx, v.store.pool, s.original, s.snapshot.targetID, []string{s.grant.TokenHash}, true)
	if err != nil {
		return "", time.Time{}, err
	}
	if err = ctx.Err(); err != nil {
		return "", time.Time{}, err
	}
	if outcome != ProfilePinUnlocked || !pinReceiptMatches(current, s) {
		return "", time.Time{}, ErrPGPinChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.rawToken == "" {
		return "", time.Time{}, ErrPGPinUsed
	}
	v.rememberPinPost(r, s.snapshot.targetID, current, outcome, []string{s.grant.TokenHash}, s.deadline)
	return s.rawToken, s.grant.ExpiresAt, nil
}

// PinIssuanceUserRecord acknowledges only this issuer's own narrow PinLength
// migration. It is a last-confirmed persistence projection, never a grant-cache
// publication. The caller must hold its local users mutex and match its exact
// pre-write revision; other profiles or newer local writes cannot be adopted.
func (ds *DataStore) PinIssuanceUserRecord(ctx context.Context, profileID string, expected UserRevision, value *PGPinCommitReceipt) (UserWriteRecord, bool, error) {
	var empty UserWriteRecord
	if ds == nil || ds.pool == nil || value == nil || value.state == nil {
		return empty, false, ErrPGPinUsed
	}
	s := value.state
	s.mu.Lock()
	if s.issuer == nil || s.issuer.store != ds || s.issuer.self != s.issuer || s.self != value || s.closed || s.cacheAckUsed || s.releaseUsed || s.snapshot.targetID != profileID || expected.owner != ds || expected.id != profileID || expected.version != s.beforeTargetVersion || !samePinRequestContext(ctx, s.requestCtx) {
		s.mu.Unlock()
		return empty, false, ErrPGPinUsed
	}
	s.cacheAckUsed = true
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return empty, false, err
	}
	if s.beforeTargetVersion == s.snapshot.target.version {
		return empty, false, nil
	}
	bounded, cancel := context.WithDeadline(ctx, s.deadline)
	defer cancel()
	current, outcome, _, err := readProfilePinAccess(bounded, ds.pool, s.original, profileID, []string{s.grant.TokenHash}, true)
	if err != nil {
		return empty, false, err
	}
	if outcome != ProfilePinUnlocked || !pinReceiptMatches(current, s) {
		return empty, false, ErrPGPinChanged
	}
	record, err := ds.scanUserWrite(ds.pool.QueryRow(bounded, `SELECT `+userColumns+`,`+userWriteVersion+` FROM users u WHERE id=$1`, profileID))
	if err != nil || record.Revision.version != s.snapshot.target.version {
		return empty, false, pinContextError(bounded, ErrPGPinChanged)
	}
	if err = bounded.Err(); err != nil {
		return empty, false, err
	}
	return record, true, nil
}

var (
	ErrPGPinRequired       = errors.New("PIN is required")
	ErrPGPinTooShort       = errors.New("PIN must be at least 4 characters")
	ErrPGPinParentRequired = errors.New("confirm the account password to manage parental controls")
)

type pgPinChange struct {
	mu                                      sync.Mutex
	store                                   *DataStore
	self                                    *PGPinChangeIntent
	ctx                                     context.Context
	cancel                                  context.CancelFunc
	original                                models.Session
	tuple                                   profilePinAccessTuple
	targetID                                string
	hashes                                  []string
	clearing                                bool
	needsParent                             bool
	master                                  bool
	passwordHash, password, newPIN, newHash string
	newLength                               int
	phase                                   pgPinPhase
}
type PGPinChangeIntent struct{ state *pgPinChange }

func (*PGPinChangeIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGPinChangeIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("PGPinChangeIntent(opaque)"))
}
func (v *PGPinChangeIntent) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self == v {
		s.phase = pgPinRetired
		s.cancel()
	}
}
func (v *PGPinChangeIntent) NeedsParent() (bool, error) {
	if v == nil || v.state == nil {
		return false, ErrPGPinUsed
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != v || s.phase != pgPinPrepared {
		return false, ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	return s.needsParent, nil
}
func (ds *DataStore) PrepareOwnedPinChange(r *http.Request, targetID, newPIN string, clearing bool, tokens []string) (*PGPinChangeIntent, error) {
	return ds.preparePinChange(r, targetID, newPIN, clearing, tokens, false)
}
func (ds *DataStore) PrepareAdministrativePinChange(r *http.Request, targetID, newPIN string, clearing bool, tokens []string) (*PGPinChangeIntent, error) {
	return ds.preparePinChange(r, targetID, newPIN, clearing, tokens, true)
}
func (ds *DataStore) preparePinChange(r *http.Request, targetID, newPIN string, clearing bool, tokens []string, administrative bool) (*PGPinChangeIntent, error) {
	if ds == nil || ds.pool == nil || r == nil {
		return nil, ErrPGPinUnavailable
	}
	if err := ds.CheckProfilePinRequestFrame(r); err != nil {
		return nil, err
	}
	// Match SetPin exactly: trim only at creation, then count bytes, not runes.
	if clearing {
		newPIN = ""
	} else {
		newPIN = strings.TrimSpace(newPIN)
		if newPIN == "" {
			return nil, ErrPGPinRequired
		}
		if len(newPIN) < 4 {
			return nil, ErrPGPinTooShort
		}
	}
	reader := NewBoundProfilePinAccessReader(ds)
	value, err := reader.Read(r, targetID, tokens)
	if err != nil {
		return nil, err
	}
	defer value.Close()
	outcome, err := reader.Check(value)
	if err != nil {
		return nil, err
	}
	if outcome == ProfilePinNotFound {
		return nil, ErrProfilePinAccessNotFound
	}
	if outcome == ProfilePinUnavailable {
		return nil, ErrPGPinChanged
	}
	kids, _, err := reader.ProfilePinAccessFacts(value)
	if err != nil {
		return nil, err
	}
	original, _ := auth.GetSession(r)
	var profile struct {
		AccountID string `json:"account_id"`
	}
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if json.Unmarshal(value.state.tuple.profile.raw, &profile) != nil || json.Unmarshal(value.state.tuple.actor.raw, &actor) != nil || actor.Hash == "" {
		return nil, ErrPGPinChanged
	}
	if profile.AccountID != original.AccountID && (!administrative || !original.IsMaster) {
		return nil, ErrProfilePinAccessNotFound
	}
	needsParent := administrative || kids || clearing && outcome != ProfilePinUnlocked || outcome == ProfilePinLocked
	if !clearing && !kids && outcome == ProfilePinNoPin && !administrative {
		needsParent = false
	}
	deadline := parentMutationMin(time.Now().Add(profileUnlockStateBudget), &original.ExpiresAt)
	if current, ok := value.state.ctx.Deadline(); ok {
		deadline = parentMutationMin(deadline, &current)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	if err = ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	password := r.Header.Get(profilePinParentPasswordHeader)
	if strings.HasPrefix(password, "base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(password, "base64:"))
		if err != nil {
			cancel()
			return nil, ErrPGPinParentRequired
		}
		password = string(decoded)
	}
	s := &pgPinChange{store: ds, ctx: ctx, cancel: cancel, original: original, tuple: value.state.tuple, targetID: targetID, hashes: append([]string(nil), value.state.tokenHashes...), clearing: clearing, needsParent: needsParent, master: original.IsMaster, passwordHash: actor.Hash, password: password, newPIN: newPIN, newLength: len(newPIN), phase: pgPinPrepared}
	intent := &PGPinChangeIntent{state: s}
	s.self = intent
	return intent, nil
}

// Verify performs the exact existing parent predicate under the frozen tuple.
// The caller still invokes the existing Guard for rate budget/wire errors;
// its boolean is never accepted as the database proof. Genuine masters retain
// Require's current exception; RequireFresh and first-403 intents are separate.
func (v *PGPinChangeIntent) Verify() error {
	if v == nil || v.state == nil {
		return ErrPGPinUsed
	}
	s := v.state
	s.mu.Lock()
	if s.self != v || s.phase != pgPinPrepared {
		s.mu.Unlock()
		return ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		s.cancel()
		s.phase = pgPinRetired
		s.mu.Unlock()
		return err
	}
	s.phase = pgPinVerifying
	s.mu.Unlock()
	var err error
	if s.needsParent && !s.master {
		err = bcrypt.CompareHashAndPassword([]byte(s.passwordHash), []byte(s.password))
		if err != nil {
			err = ErrPGPinParentRequired
		}
	}
	var hash []byte
	if err == nil && !s.clearing {
		hash, err = bcrypt.GenerateFromPassword([]byte(s.newPIN), bcrypt.DefaultCost)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.password = ""
	s.newPIN = ""
	if cancelled := s.ctx.Err(); cancelled != nil {
		s.phase = pgPinRetired
		s.cancel()
		return cancelled
	}
	if s.phase != pgPinVerifying {
		s.cancel()
		return ErrPGPinUsed
	}
	if err != nil {
		s.phase = pgPinRetired
		s.cancel()
		return err
	}
	s.newHash = string(hash)
	s.phase = pgPinVerified
	return nil
}

// CommitPinChange applies only PIN fields and revocation in the same original
// request transaction. It does not install fresh-parent mutation semantics.
func (ds *DataStore) CommitPinChange(value *PGPinChangeIntent) (UserWriteRecord, error) {
	var empty UserWriteRecord
	if ds == nil || ds.pool == nil || value == nil || value.state == nil {
		return empty, ErrPGPinUsed
	}
	s := value.state
	s.mu.Lock()
	if s.store != ds || s.self != value || s.phase != pgPinVerified {
		s.mu.Unlock()
		return empty, ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		s.phase = pgPinRetired
		s.cancel()
		s.mu.Unlock()
		return empty, err
	}
	s.phase = pgPinCommitting
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.phase = pgPinRetired; s.cancel(); s.mu.Unlock() }()
	tx, err := ds.pool.Begin(s.ctx)
	if err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	defer rollbackPinTx(tx, s.ctx)
	for _, scope := range []struct {
		name, key string
		expected  int64
	}{{"profile", s.targetID, s.tuple.profileGeneration}, {"session", pinSessionLineage(s.tuple.session.raw, s.original.Token), s.tuple.sessionGeneration}} {
		if _, err = tx.Exec(s.ctx, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES($1,$2,0) ON CONFLICT(scope,scope_key) DO NOTHING`, scope.name, scope.key); err != nil {
			return empty, pinContextError(s.ctx, ErrPGPinUnavailable)
		}
		var actual int64
		if err = tx.QueryRow(s.ctx, `SELECT generation FROM profile_unlock_revocations WHERE scope=$1 AND scope_key=$2 FOR UPDATE`, scope.name, scope.key).Scan(&actual); err != nil || actual != scope.expected {
			return empty, pinContextError(s.ctx, ErrPGPinChanged)
		}
	}
	var profile struct {
		AccountID string `json:"account_id"`
	}
	_ = json.Unmarshal(s.tuple.profile.raw, &profile)
	ids := []string{s.original.AccountID}
	if profile.AccountID != s.original.AccountID {
		ids = append(ids, profile.AccountID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(s.ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(s.ctx, ErrPGPinChanged)
		}
	}
	var found string
	if err = tx.QueryRow(s.ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinChanged)
	}
	if _, err = tx.Exec(s.ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	if err = tx.QueryRow(s.ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.targetID).Scan(&found); err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinChanged)
	}
	current, _, _, err := readProfilePinAccess(s.ctx, tx, s.original, s.targetID, s.hashes, true)
	if err != nil || !sameParentMutationRow(current.session, s.tuple.session) || !sameParentMutationRow(current.actor, s.tuple.actor) || !sameParentMutationRow(current.profile, s.tuple.profile) || !sameParentMutationRow(current.owner, s.tuple.owner) || string(current.grants) != string(s.tuple.grants) || current.profileGeneration != s.tuple.profileGeneration || current.sessionGeneration != s.tuple.sessionGeneration {
		return empty, pinContextError(s.ctx, ErrPGPinChanged)
	}
	record, err := ds.scanUserWrite(tx.QueryRow(s.ctx, `UPDATE users AS u SET pin_hash=$1,pin_length=$2,updated_at=clock_timestamp() WHERE id=$3 AND `+userWriteVersion+`=$4 RETURNING `+userColumns+`,`+userWriteVersion, s.newHash, s.newLength, s.targetID, s.tuple.profile.version))
	if err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinChanged)
	}
	if _, err = tx.Exec(s.ctx, `UPDATE profile_unlock_revocations SET generation=generation+1 WHERE scope='profile' AND scope_key=$1`, s.targetID); err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	if _, err = tx.Exec(s.ctx, `DELETE FROM profile_unlocks WHERE profile_id=$1`, s.targetID); err != nil {
		return empty, pinContextError(s.ctx, ErrPGPinUnavailable)
	}
	if err = s.ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(s.ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrPGPinUnavailable
		}
		return empty, ErrPGPinOutcomeUnknown
	}
	return record, nil
}

// A cookie bridge may check only the exact post-state established by this
// verification, including its own PinLength migration. This private stamp is
// not exposed in JSON and cannot be substituted by a fresh unrelated Current.
type profilePinVerifiedPost struct {
	issuer   *PGPinIssuer
	target   string
	tuple    profilePinAccessTuple
	outcome  ProfilePinAccessOutcome
	hashes   []string
	deadline time.Time
}

func (v *PGPinIssuer) rememberPinPost(r *http.Request, target string, tuple profilePinAccessTuple, outcome ProfilePinAccessOutcome, hashes []string, deadline time.Time) {
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != v.store {
		return
	}
	frame.verifiedMu.Lock()
	defer frame.verifiedMu.Unlock()
	frame.verified = &profilePinVerifiedPost{issuer: v, target: target, tuple: tuple, outcome: outcome, hashes: append([]string(nil), hashes...), deadline: deadline}
}
func (v *PGPinIssuer) RecordNoPinVerification(r *http.Request, target string) error {
	if !v.available() || r == nil {
		return ErrPGPinUnavailable
	}
	reader := NewBoundProfilePinAccessReader(v.store)
	state, err := reader.Read(r, target, nil)
	if err != nil {
		return err
	}
	defer state.Close()
	outcome, err := reader.Check(state)
	if err != nil {
		return err
	}
	if outcome != ProfilePinNoPin {
		return ErrPGPinChanged
	}
	deadline, _ := state.state.ctx.Deadline()
	v.rememberPinPost(r, target, state.state.tuple, outcome, nil, deadline)
	return nil
}
func (v *PGPinIssuer) CheckVerifiedProfilePinRequest(r *http.Request, target string) error {
	if !v.available() || r == nil {
		return ErrPGPinUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != v.store {
		return ErrPGPinChanged
	}
	frame.verifiedMu.Lock()
	post := frame.verified
	frame.verified = nil
	frame.verifiedMu.Unlock()
	if post == nil || post.issuer != v || post.target != target {
		return ErrPGPinUsed
	}
	if err := frame.ctx.Err(); err != nil {
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != frame.session || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster {
		return ErrPGPinChanged
	}
	ctx, cancel := context.WithDeadline(r.Context(), post.deadline)
	defer cancel()
	current, outcome, _, err := readProfilePinAccess(ctx, v.store.pool, original, target, post.hashes, true)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if outcome != post.outcome || !sameParentMutationRow(current.session, post.tuple.session) || !sameParentMutationRow(current.actor, post.tuple.actor) || !sameParentMutationRow(current.profile, post.tuple.profile) || !sameParentMutationRow(current.owner, post.tuple.owner) || string(current.grants) != string(post.tuple.grants) || current.profileGeneration != post.tuple.profileGeneration || current.sessionGeneration != post.tuple.sessionGeneration {
		return ErrPGPinChanged
	}
	return nil
}

// CheckPinChangeRevision prevents a successful targeted PIN commit from
// silently adopting a different local pending users edit or stale revision.
func (ds *DataStore) CheckPinChangeRevision(value *PGPinChangeIntent, expected UserRevision) (string, error) {
	if ds == nil || value == nil || value.state == nil {
		return "", ErrPGPinUsed
	}
	s := value.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != ds || s.self != value || s.phase != pgPinVerified {
		return "", ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		return "", err
	}
	if expected.owner != ds || expected.id != s.targetID || expected.version != s.tuple.profile.version {
		return "", ErrUserWriteConflict
	}
	return s.targetID, nil
}

// PinChangeTarget identifies the already selected public profile, never proof
// or credential material. The opaque intent still controls every write stage.
func (ds *DataStore) PinChangeTarget(value *PGPinChangeIntent) (string, error) {
	if ds == nil || value == nil || value.state == nil {
		return "", ErrPGPinUsed
	}
	s := value.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != ds || s.self != value || s.phase != pgPinVerified {
		return "", ErrPGPinUsed
	}
	if err := s.ctx.Err(); err != nil {
		return "", err
	}
	return s.targetID, nil
}
