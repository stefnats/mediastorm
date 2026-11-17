package datastore

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

const profileUnlockStateBudget = 30 * time.Second

var (
	ErrProfileUnlockStateUnavailable = errors.New("profile unlock state unavailable")
	ErrProfileUnlockStateDenied      = errors.New("profile unlock state changed or denied")
	ErrProfileUnlockStateUsed        = errors.New("profile unlock state already retired")
	ErrProfileUnlockRevokeUnknown    = errors.New("profile unlock revocation commit outcome unknown")
)

type profileUnlockBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// A scope row is the serialization point, including when DELETE affects zero
// grants. PostgreSQL overflow aborts the transaction instead of resetting it.
// Nested pgx transactions use savepoints: failure rolls back this operation,
// while successful release still belongs to the caller's outer transaction.
func revokeProfileUnlocks(ctx context.Context, db DB, scope, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope != "profile" && scope != "session" || scope == "profile" && !parentMutationID(key) || scope == "session" && !profileUnlockHash(key) {
		return ErrProfileUnlockStateDenied
	}
	starter, ok := db.(profileUnlockBeginner)
	if !ok {
		return ErrProfileUnlockStateUnavailable
	}
	tx, err := starter.Begin(ctx)
	if err != nil {
		if cancelled := ctx.Err(); cancelled != nil {
			return cancelled
		}
		return ErrProfileUnlockStateUnavailable
	}
	defer func() {
		rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollback)
	}()
	_, err = tx.Exec(ctx, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)
        VALUES($1,$2,1) ON CONFLICT(scope,scope_key) DO UPDATE
        SET generation=profile_unlock_revocations.generation+1`, scope, key)
	if err != nil {
		if cancelled := ctx.Err(); cancelled != nil {
			return cancelled
		}
		return ErrProfileUnlockStateUnavailable
	}
	// The SQL column is selected only from these constants, never request data.
	column := "profile_id"
	if scope == "session" {
		column = "session_hash"
	}
	if _, err = tx.Exec(ctx, `DELETE FROM profile_unlocks WHERE `+column+`=$1`, key); err != nil {
		if cancelled := ctx.Err(); cancelled != nil {
			return cancelled
		}
		return ErrProfileUnlockStateUnavailable
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return ErrProfileUnlockStateUnavailable
		}
		// No retry or assertion that the database rolled back an unknown ACK.
		return ErrProfileUnlockRevokeUnknown
	}
	return nil
}

func profileUnlockHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}
func profileUnlockDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
func profileUnlockCredential(value string) bool {
	if !strings.HasPrefix(value, "pu_") {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "pu_"))
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == value[3:]
}

type profileUnlockTuple struct {
	session, actor, profile, owner, grant parentMutationRow
	profileGeneration, sessionGeneration  int64
}

type profileUnlockRead struct {
	mu                  sync.Mutex
	store               *DataStore
	self                *ProfileUnlockState
	ctx                 context.Context
	cancel              context.CancelFunc
	original            models.Session
	targetID, tokenHash string
	tuple               profileUnlockTuple
	used                bool
}

// ProfileUnlockState is one bounded observation, not a PIN proof or permission.
// No route, issuer or legacy grant cache adopts it. In particular a fresh read
// cannot prove that a legacy Create happened before a preceding revocation.
// All tuple and credential data is private; copies are not usable receipts.
type ProfileUnlockState struct{ state *profileUnlockRead }

func (*ProfileUnlockState) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ProfileUnlockState) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileUnlockState(opaque)"))
}
func (v *ProfileUnlockState) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self == v {
		s.used = true
		s.cancel()
	}
}

// ReadProfileUnlockState reads a single PostgreSQL snapshot under the original
// authenticated full request. It never uses startup caches or JSON fallback,
// verifies a PIN, issues a token, renews expiry, or changes a stored grant.
func (ds *DataStore) ReadProfileUnlockState(r *http.Request, targetID, rawUnlockToken string) (*ProfileUnlockState, error) {
	if ds == nil || ds.pool == nil || r == nil || !parentMutationID(targetID) || !profileUnlockCredential(rawUnlockToken) {
		return nil, ErrProfileUnlockStateDenied
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	original, ok := auth.GetSession(r)
	if !ok || !parentMutationID(original.Token) || !parentMutationID(original.AccountID) || auth.GetAccountID(r) != original.AccountID || original.IsMaster != auth.IsMaster(r) || original.Scope != "" || original.ScopeResource != "" || !original.ExpiresAt.After(time.Now()) {
		return nil, ErrProfileUnlockStateDenied
	}
	ctx, cancel := context.WithDeadline(r.Context(), parentMutationMin(time.Now().Add(profileUnlockStateBudget), &original.ExpiresAt))
	tokenHash := profileUnlockDigest(rawUnlockToken)
	tuple, deadline, err := readProfileUnlockTuple(ctx, ds.pool, original, targetID, tokenHash)
	if err != nil {
		cancel()
		return nil, err
	}
	bounded, stop := context.WithDeadline(ctx, deadline)
	if err = bounded.Err(); err != nil {
		stop()
		cancel()
		return nil, err
	}
	s := &profileUnlockRead{store: ds, ctx: bounded, cancel: func() { stop(); cancel() }, original: original, targetID: targetID, tokenHash: tokenHash, tuple: tuple}
	value := &ProfileUnlockState{state: s}
	s.self = value
	return value, nil
}

// CheckProfileUnlockState consumes the exact observation before any IO. True
// means only that the complete stored tuple still matches at this query's
// snapshot; callers cannot carry it forward as a durable authorization grant.
func (ds *DataStore) CheckProfileUnlockState(value *ProfileUnlockState) (bool, error) {
	if ds == nil || ds.pool == nil || value == nil || value.state == nil {
		return false, ErrProfileUnlockStateUsed
	}
	s := value.state
	s.mu.Lock()
	if s.store != ds || s.self != value || s.used {
		s.mu.Unlock()
		return false, ErrProfileUnlockStateUsed
	}
	s.used = true
	s.mu.Unlock()
	defer s.cancel()
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	current, _, err := readProfileUnlockTuple(s.ctx, ds.pool, s.original, s.targetID, s.tokenHash)
	if err != nil {
		return false, err
	}
	if err = s.ctx.Err(); err != nil {
		return false, err
	}
	if !sameProfileUnlockTuple(s.tuple, current) {
		return false, ErrProfileUnlockStateDenied
	}
	return true, nil
}

func sameProfileUnlockTuple(a, b profileUnlockTuple) bool {
	return a.profileGeneration == b.profileGeneration && a.sessionGeneration == b.sessionGeneration &&
		sameParentMutationRow(a.session, b.session) && sameParentMutationRow(a.actor, b.actor) &&
		sameParentMutationRow(a.profile, b.profile) && sameParentMutationRow(a.owner, b.owner) && sameParentMutationRow(a.grant, b.grant)
}

// One SQL statement avoids stitching together unrelated MVCC snapshots. Raw
// row data plus xmin/ctid detects equal-value UPDATE or DELETE/INSERT ABA.
func readProfileUnlockTuple(ctx context.Context, q parentMutationQuerier, original models.Session, targetID, tokenHash string) (profileUnlockTuple, time.Time, error) {
	var v profileUnlockTuple
	var now time.Time
	if err := ctx.Err(); err != nil {
		return v, now, err
	}
	err := q.QueryRow(ctx, `SELECT
        to_jsonb(s),concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),
        to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),
        to_jsonb(u),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),
        to_jsonb(o),concat(o.tableoid::oid,':',o.xmin::text,':',o.ctid::text),
        to_jsonb(g),concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text),
        COALESCE(p.generation,0),COALESCE(x.generation,0),clock_timestamp()
        FROM profile_unlocks g JOIN sessions s ON s.token=$1
        JOIN accounts a ON a.id=s.account_id JOIN users u ON u.id=$2
        JOIN accounts o ON o.id=u.account_id
        LEFT JOIN profile_unlock_revocations p ON p.scope='profile' AND p.scope_key=u.id
        LEFT JOIN profile_unlock_revocations x ON x.scope='session' AND x.scope_key=g.session_hash
        WHERE g.token_hash=$3`, original.Token, targetID, tokenHash).Scan(
		&v.session.raw, &v.session.version, &v.actor.raw, &v.actor.version,
		&v.profile.raw, &v.profile.version, &v.owner.raw, &v.owner.version, &v.grant.raw, &v.grant.version,
		&v.profileGeneration, &v.sessionGeneration, &now)
	if err := ctx.Err(); err != nil {
		return v, now, err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return v, now, ErrProfileUnlockStateDenied
	}
	if err != nil {
		return v, now, ErrProfileUnlockStateUnavailable
	}
	var session struct {
		Token     string    `json:"token"`
		AccountID string    `json:"account_id"`
		ClientID  string    `json:"client_id"`
		Scope     string    `json:"scope"`
		Resource  string    `json:"scope_resource"`
		Master    bool      `json:"is_master"`
		Created   time.Time `json:"created_at"`
		Expiry    time.Time `json:"expires_at"`
	}
	var actor struct {
		ID     string     `json:"id"`
		Master bool       `json:"is_master"`
		Expiry *time.Time `json:"expires_at"`
	}
	var profile struct {
		ID        string `json:"id"`
		AccountID string `json:"account_id"`
		PinHash   string `json:"pin_hash"`
	}
	var owner struct {
		ID     string     `json:"id"`
		Expiry *time.Time `json:"expires_at"`
	}
	var grant struct {
		TokenHash      string    `json:"token_hash"`
		SessionHash    string    `json:"session_hash"`
		AccountID      string    `json:"account_id"`
		ProfileID      string    `json:"profile_id"`
		PinFingerprint string    `json:"pin_fingerprint"`
		Created        time.Time `json:"created_at"`
		Expiry         time.Time `json:"expires_at"`
	}
	if json.Unmarshal(v.session.raw, &session) != nil || json.Unmarshal(v.actor.raw, &actor) != nil || json.Unmarshal(v.profile.raw, &profile) != nil || json.Unmarshal(v.owner.raw, &owner) != nil || json.Unmarshal(v.grant.raw, &grant) != nil {
		return v, now, ErrProfileUnlockStateUnavailable
	}
	if session.Token != original.Token || session.AccountID != original.AccountID || actor.ID != original.AccountID || session.ClientID != original.ClientID || actor.Master != original.IsMaster || session.Scope != "" || session.Resource != "" || !session.Created.Equal(original.CreatedAt) || !session.Expiry.Equal(original.ExpiresAt) || !session.Expiry.After(now) || actor.Expiry != nil && !actor.Expiry.After(now) || owner.Expiry != nil && !owner.Expiry.After(now) || profile.ID != targetID || profile.AccountID != owner.ID || (profile.AccountID != actor.ID && !actor.Master) || grant.AccountID != actor.ID || grant.ProfileID != targetID || grant.TokenHash != tokenHash || grant.SessionHash != profileUnlockDigest(original.Token) || !grant.Expiry.After(now) || grant.Created.After(now) || !grant.Expiry.After(grant.Created) || v.profileGeneration < 0 || v.sessionGeneration < 0 {
		return v, now, ErrProfileUnlockStateDenied
	}
	if _, err = bcrypt.Cost([]byte(profile.PinHash)); err != nil {
		return v, now, ErrProfileUnlockStateDenied
	}
	pinDigest := sha256.Sum256([]byte(profile.PinHash))
	fingerprint := hex.EncodeToString(pinDigest[:8])
	if subtle.ConstantTimeCompare([]byte(grant.PinFingerprint), []byte(fingerprint)) != 1 {
		return v, now, ErrProfileUnlockStateDenied
	}
	deadline := parentMutationMin(original.ExpiresAt, &session.Expiry, actor.Expiry, owner.Expiry, &grant.Expiry)
	return v, deadline, nil
}

// ProfilePinAccessOutcome describes one checked storage observation. These are
// not wire statuses or lasting permissions; unknown/zero never means allowed.
type ProfilePinAccessOutcome uint8

const (
	ProfilePinUnavailable ProfilePinAccessOutcome = iota
	ProfilePinNotFound
	ProfilePinLocked
	ProfilePinNoPin
	ProfilePinUnlocked
)

var (
	ErrProfilePinAccessUnavailable = errors.New("current profile PIN access unavailable")
	ErrProfilePinAccessChanged     = errors.New("current profile PIN access changed")
	ErrProfilePinAccessUsed        = errors.New("current profile PIN access already retired")
)

const maxProfilePinAccessTokens = 32
const profilePinAccessTokenBytes = 46
const maxProfilePinAccessCredentialBytes = maxProfilePinAccessTokens * profilePinAccessTokenBytes

// ProfilePinAccessReader is separate from the existing token-only reader. It
// also observes a currently owned profile with no PIN, without trusting cached
// HasPin=false. It is not installed in public middleware or mutation handlers.
type ProfilePinAccessReader struct {
	self  *ProfilePinAccessReader
	store *DataStore
}

func NewProfilePinAccessReader(store *DataStore) *ProfilePinAccessReader {
	reader := &ProfilePinAccessReader{store: store}
	reader.self = reader
	return reader
}
func (r *ProfilePinAccessReader) available() bool {
	return r != nil && r.self == r && r.store != nil && r.store.pool != nil
}

type profilePinAccessTuple struct {
	frameCurrent                         bool
	session, actor, profile, owner       parentMutationRow
	grants                               json.RawMessage
	profileGeneration, sessionGeneration int64
}
type profilePinAccessRead struct {
	mu          sync.Mutex
	reader      *ProfilePinAccessReader
	self        *ProfilePinAccessState
	ctx         context.Context
	cancel      context.CancelFunc
	original    models.Session
	targetID    string
	tokenHashes []string
	tuple       profilePinAccessTuple
	outcome     ProfilePinAccessOutcome
	used        bool
}

// ProfilePinAccessState carries private current rows and generations. It has no
// raw credential, owner, PIN hash, child-policy fact or usable grant accessor.
type ProfilePinAccessState struct{ state *profilePinAccessRead }

func (*ProfilePinAccessReader) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ProfilePinAccessState) MarshalJSON() ([]byte, error)  { return []byte("null"), nil }
func (*ProfilePinAccessReader) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfilePinAccessReader(opaque)"))
}
func (*ProfilePinAccessState) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfilePinAccessState(opaque)"))
}
func (v *ProfilePinAccessState) Close() {
	if v == nil || v.state == nil {
		return
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self == v {
		s.used = true
		s.cancel()
	}
}

// Read binds the original full authenticated frame before any deferred work.
// Caller input is only the target and presented credentials, never a role,
// fingerprint, verified boolean or policy. Client/role mismatches are not fixed.
func (r *ProfilePinAccessReader) Read(request *http.Request, targetID string, rawTokens []string) (*ProfilePinAccessState, error) {
	if !r.available() || request == nil || !parentMutationID(targetID) || len(rawTokens) > maxProfilePinAccessTokens {
		return nil, ErrProfilePinAccessUnavailable
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	original, ok := auth.GetSession(request)
	if !ok || !parentMutationID(original.Token) || !parentMutationID(original.AccountID) || auth.GetAccountID(request) != original.AccountID || auth.IsMaster(request) != original.IsMaster || original.Scope != "" || original.ScopeResource != "" || !original.ExpiresAt.After(time.Now()) {
		return nil, ErrProfilePinAccessChanged
	}
	hashes := make([]string, 0, len(rawTokens))
	seen := map[string]bool{}
	credentialBytes := 0
	for _, raw := range rawTokens {
		if len(raw) > maxProfilePinAccessCredentialBytes-credentialBytes {
			return nil, ErrProfilePinAccessUnavailable
		}
		credentialBytes += len(raw)
		if len(raw) != profilePinAccessTokenBytes || !profileUnlockCredential(raw) {
			continue
		}
		hash := profileUnlockDigest(raw)
		if !seen[hash] {
			hashes = append(hashes, hash)
			seen[hash] = true
		}
	}
	ctx, cancel := context.WithDeadline(request.Context(), parentMutationMin(time.Now().Add(profileUnlockStateBudget), &original.ExpiresAt))
	tuple, outcome, deadline, err := readProfilePinAccess(ctx, r.store.pool, original, targetID, hashes)
	if err != nil {
		cancel()
		return nil, err
	}
	bounded, stop := context.WithDeadline(ctx, deadline)
	if err = bounded.Err(); err != nil {
		stop()
		cancel()
		return nil, err
	}
	state := &profilePinAccessRead{reader: r, ctx: bounded, cancel: func() { stop(); cancel() }, original: original, targetID: targetID, tokenHashes: hashes, tuple: tuple, outcome: outcome}
	value := &ProfilePinAccessState{state: state}
	state.self = value
	return value, nil
}

// Check consumes exactly one snapshot before IO. An originally unowned/missing
// target remains the same NotFound denial even if it is later reassigned; no
// changed target can turn an earlier denial into an allowance. Owned snapshots
// require unchanged rows, generations and classification at the final query.
func (r *ProfilePinAccessReader) Check(value *ProfilePinAccessState) (ProfilePinAccessOutcome, error) {
	if !r.available() || value == nil || value.state == nil {
		return ProfilePinUnavailable, ErrProfilePinAccessUsed
	}
	s := value.state
	s.mu.Lock()
	if s.reader != r || s.self != value || s.used {
		s.mu.Unlock()
		return ProfilePinUnavailable, ErrProfilePinAccessUsed
	}
	s.used = true
	s.mu.Unlock()
	defer s.cancel()
	if err := s.ctx.Err(); err != nil {
		return ProfilePinUnavailable, err
	}
	current, outcome, _, err := readProfilePinAccess(s.ctx, r.store.pool, s.original, s.targetID, s.tokenHashes)
	if cancelled := s.ctx.Err(); cancelled != nil {
		return ProfilePinUnavailable, cancelled
	}
	// A successful current auth frame is distinct from target-local errors.
	// No new target/PIN/owner state may disclose or promote an original denial.
	if s.outcome == ProfilePinNotFound && current.frameCurrent {
		if !sameParentMutationRow(s.tuple.session, current.session) || !sameParentMutationRow(s.tuple.actor, current.actor) {
			return ProfilePinUnavailable, ErrProfilePinAccessChanged
		}
		return ProfilePinNotFound, nil
	}
	if err != nil {
		return ProfilePinUnavailable, err
	}
	if !sameParentMutationRow(s.tuple.session, current.session) || !sameParentMutationRow(s.tuple.actor, current.actor) {
		return ProfilePinUnavailable, ErrProfilePinAccessChanged
	}
	if s.outcome != outcome || !sameParentMutationRow(s.tuple.profile, current.profile) || !sameParentMutationRow(s.tuple.owner, current.owner) || string(s.tuple.grants) != string(current.grants) || s.tuple.profileGeneration != current.profileGeneration || s.tuple.sessionGeneration != current.sessionGeneration {
		return ProfilePinUnavailable, ErrProfilePinAccessChanged
	}
	return outcome, nil
}

// A single query includes even a missing target and its absence of a PIN.
// Candidate grants are bounded and limited to this exact account/session/target;
// unrelated credential rows never determine profile ownership or PIN presence.
func readProfilePinAccess(ctx context.Context, q parentMutationQuerier, original models.Session, targetID string, hashes []string) (profilePinAccessTuple, ProfilePinAccessOutcome, time.Time, error) {
	var v profilePinAccessTuple
	var now time.Time
	fail := func(err error) (profilePinAccessTuple, ProfilePinAccessOutcome, time.Time, error) {
		return v, ProfilePinUnavailable, now, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	err := q.QueryRow(ctx, `SELECT
        to_jsonb(s),concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),
        to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),
        COALESCE(to_jsonb(u),'null'::jsonb),CASE WHEN u.id IS NULL THEN '' ELSE concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) END,
        COALESCE(to_jsonb(o),'null'::jsonb),CASE WHEN o.id IS NULL THEN '' ELSE concat(o.tableoid::oid,':',o.xmin::text,':',o.ctid::text) END,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(g),'version',concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text)) ORDER BY g.token_hash)
            FROM profile_unlocks g WHERE g.token_hash=ANY($3::text[]) AND g.profile_id=$2 AND g.account_id=s.account_id AND g.session_hash=$4),'[]'::jsonb),
        COALESCE(p.generation,0),COALESCE(x.generation,0),clock_timestamp()
        FROM sessions s JOIN accounts a ON a.id=s.account_id
        LEFT JOIN users u ON u.id=$2 LEFT JOIN accounts o ON o.id=u.account_id
        LEFT JOIN profile_unlock_revocations p ON p.scope='profile' AND p.scope_key=$2
        LEFT JOIN profile_unlock_revocations x ON x.scope='session' AND x.scope_key=$4
        WHERE s.token=$1`, original.Token, targetID, hashes, profileUnlockDigest(original.Token)).Scan(
		&v.session.raw, &v.session.version, &v.actor.raw, &v.actor.version,
		&v.profile.raw, &v.profile.version, &v.owner.raw, &v.owner.version, &v.grants,
		&v.profileGeneration, &v.sessionGeneration, &now)
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err != nil {
		return fail(ErrProfilePinAccessUnavailable)
	}
	var session struct {
		Token     string    `json:"token"`
		AccountID string    `json:"account_id"`
		ClientID  string    `json:"client_id"`
		Scope     string    `json:"scope"`
		Resource  string    `json:"scope_resource"`
		Created   time.Time `json:"created_at"`
		Expiry    time.Time `json:"expires_at"`
	}
	var actor struct {
		ID     string     `json:"id"`
		Master bool       `json:"is_master"`
		Expiry *time.Time `json:"expires_at"`
	}
	var profile *struct {
		ID        string `json:"id"`
		AccountID string `json:"account_id"`
		PinHash   string `json:"pin_hash"`
	}
	var owner *struct {
		ID     string     `json:"id"`
		Expiry *time.Time `json:"expires_at"`
	}
	if !profilePinRequiredFields(v.session.raw, []string{"token", "account_id", "client_id", "scope", "scope_resource", "created_at", "expires_at"}, false) ||
		!profilePinRequiredFields(v.actor.raw, []string{"id", "is_master", "expires_at"}, true) ||
		json.Unmarshal(v.session.raw, &session) != nil || json.Unmarshal(v.actor.raw, &actor) != nil {
		return fail(ErrProfilePinAccessUnavailable)
	}
	if session.Token != original.Token || session.AccountID != original.AccountID || actor.ID != original.AccountID || session.ClientID != original.ClientID || actor.Master != original.IsMaster || session.Scope != "" || session.Resource != "" || !session.Created.Equal(original.CreatedAt) || !session.Expiry.Equal(original.ExpiresAt) || !session.Expiry.After(now) || actor.Expiry != nil && !actor.Expiry.After(now) || v.profileGeneration < 0 || v.sessionGeneration < 0 {
		return fail(ErrProfilePinAccessChanged)
	}
	v.frameCurrent = true
	deadline := parentMutationMin(original.ExpiresAt, &session.Expiry, actor.Expiry)
	if json.Unmarshal(v.profile.raw, &profile) != nil || json.Unmarshal(v.owner.raw, &owner) != nil {
		return fail(ErrProfilePinAccessUnavailable)
	}
	if profile == nil || profile.ID != targetID || (profile.AccountID != actor.ID && !actor.Master) {
		return v, ProfilePinNotFound, deadline, nil
	}
	if !profilePinRequiredFields(v.profile.raw, []string{"id", "account_id", "pin_hash"}, false) ||
		!profilePinRequiredFields(v.owner.raw, []string{"id", "expires_at"}, true) ||
		profile.AccountID == "" || owner == nil || owner.ID != profile.AccountID || owner.Expiry != nil && !owner.Expiry.After(now) {
		return fail(ErrProfilePinAccessUnavailable)
	}
	deadline = parentMutationMin(deadline, owner.Expiry)
	if profile.PinHash == "" {
		return v, ProfilePinNoPin, deadline, nil
	}
	if _, err = bcrypt.Cost([]byte(profile.PinHash)); err != nil {
		return fail(ErrProfilePinAccessUnavailable)
	}
	var grants []struct {
		Row     json.RawMessage `json:"row"`
		Version string          `json:"version"`
	}
	if json.Unmarshal(v.grants, &grants) != nil {
		return fail(ErrProfilePinAccessUnavailable)
	}
	digest := sha256.Sum256([]byte(profile.PinHash))
	fingerprint := hex.EncodeToString(digest[:8])
	var latestValid time.Time
	for _, entry := range grants {
		var grant struct {
			TokenHash   string    `json:"token_hash"`
			SessionHash string    `json:"session_hash"`
			AccountID   string    `json:"account_id"`
			ProfileID   string    `json:"profile_id"`
			Fingerprint string    `json:"pin_fingerprint"`
			Created     time.Time `json:"created_at"`
			Expiry      time.Time `json:"expires_at"`
		}
		if !profilePinRequiredFields(entry.Row, []string{"token_hash", "session_hash", "account_id", "profile_id", "pin_fingerprint", "created_at", "expires_at"}, false) ||
			json.Unmarshal(entry.Row, &grant) != nil || entry.Version == "" {
			return fail(ErrProfilePinAccessUnavailable)
		}
		if grant.AccountID != actor.ID || grant.ProfileID != targetID || grant.SessionHash != profileUnlockDigest(original.Token) || !profileUnlockHash(grant.TokenHash) {
			return fail(ErrProfilePinAccessUnavailable)
		}
		if grant.Created.After(now) || !grant.Expiry.After(grant.Created) || !grant.Expiry.After(now) || subtle.ConstantTimeCompare([]byte(grant.Fingerprint), []byte(fingerprint)) != 1 {
			continue
		}
		if grant.Expiry.After(latestValid) {
			latestValid = grant.Expiry
		}
	}
	if latestValid.IsZero() {
		return v, ProfilePinLocked, deadline, nil
	}
	return v, ProfilePinUnlocked, parentMutationMin(deadline, &latestValid), nil
}

// Missing fields must never turn into empty-PIN/full-scope/unlimited-expiry
// defaults. Only account expires_at may be explicitly null in a current row.
func profilePinRequiredFields(raw []byte, required []string, nullableAccountExpiry bool) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	for _, name := range required {
		value, present := fields[name]
		if !present || string(value) == "null" && !(nullableAccountExpiry && name == "expires_at") {
			return false
		}
	}
	return true
}
