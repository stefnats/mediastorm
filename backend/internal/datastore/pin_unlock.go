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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

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

// PGPinIssuer is a separate, uninstalled PostgreSQL-only protocol. It never
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
// There is deliberately no token, grant, expiry, adoption or publication API.
// A future adoption protocol must recheck current storage without reviving a
// grant revoked between this commit and any prospective cache publication.
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
	profileGeneration, sessionGeneration, err := readPinGenerations(ctx, tx, targetID, profileUnlockDigest(original.Token))
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
	state := &pgPinAttempt{issuer: v, ctx: bounded, cancel: func() { stop(); cancel() }, deadline: deadline, original: original, snapshot: snapshot, profileGeneration: profileGeneration, sessionGeneration: sessionGeneration, pinHash: target.Hash, pinLength: target.Length, ttl: configuredTTL, phase: pgPinPrepared}
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
		{"session", profileUnlockDigest(s.original.Token), s.sessionGeneration},
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
	err = tx.QueryRow(s.ctx, `INSERT INTO profile_unlocks(`+profileUnlockColumns+`)
        SELECT $1,$2,$3,$4,$5,$6,$7,$8 WHERE clock_timestamp()<$7 AND clock_timestamp()<$9
        RETURNING token_hash`, grant.TokenHash, grant.SessionHash, grant.AccountID, grant.ProfileID, grant.PinFingerprint, grant.SessionBound, grant.ExpiresAt, grant.CreatedAt, s.deadline).Scan(&inserted)
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
	state := &pgPinReceipt{issuer: v, original: s.original, snapshot: current, profileGeneration: s.profileGeneration, sessionGeneration: s.sessionGeneration, grant: grant, rawToken: token}
	receipt := &PGPinCommitReceipt{state: state}
	state.self = receipt
	return receipt, nil
}
