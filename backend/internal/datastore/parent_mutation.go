package datastore

import (
	"bytes"
	"context"
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

// This internal flag-only seam is deliberately not installed by any service or
// route. It is NOT a replacement for users.Service.SetKidsProfile (defaults and
// resets), a public shared password-attempt budget, or durable family policy.
// Legacy full-cache writes can still overwrite a later committed flag.
const parentMutationBudget = 30 * time.Second

var (
	ErrParentMutationUnavailable    = errors.New("parent flag mutation unavailable")
	ErrParentMutationChanged        = errors.New("parent flag mutation context changed")
	ErrParentMutationExpired        = errors.New("parent flag mutation expired or cancelled")
	ErrParentMutationCredential     = errors.New("parent credential confirmation failed")
	ErrParentMutationUsed           = errors.New("parent flag mutation intent unavailable")
	ErrParentMutationOutcomeUnknown = errors.New("parent flag mutation commit outcome unknown")
)

type parentMutationPhase uint8

const (
	parentPrepared parentMutationPhase = iota
	parentVerifying
	parentVerified
	parentCommitting
	parentRetired
)

type parentMutationRow struct {
	raw     []byte
	version string
}
type parentMutationSnapshot struct {
	session, parent, target, owner                    parentMutationRow
	token, actorID, targetID, ownerID, clientID, hash string
	isMaster                                          bool
	sessionExpiry                                     time.Time
	actorExpiry, ownerExpiry                          *time.Time
	sessionCreated                                    time.Time
}
type parentMutationState struct {
	mu       sync.Mutex
	store    *DataStore
	intent   *ParentKidsIntent
	proof    *ParentKidsProof
	ctx      context.Context
	cancel   context.CancelFunc
	deadline time.Time
	original models.Session
	snapshot parentMutationSnapshot
	enabled  bool
	phase    parentMutationPhase
}

// ParentKidsIntent binds one original request to one exact flag-only change.
// Copies, imported/zero values, other DataStore instances and replays fail closed.
// Passwords are never retained. All formatting deliberately hides credentials.
type ParentKidsIntent struct{ state *parentMutationState }
type ParentKidsProof struct{ state *parentMutationState }

func (*ParentKidsIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsProof) MarshalJSON() ([]byte, error)  { return []byte("null"), nil }
func (*ParentKidsIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsIntent(opaque)"))
}
func (*ParentKidsProof) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsProof(opaque)"))
}
func (i *ParentKidsIntent) Close() {
	if i == nil || i.state == nil {
		return
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.intent == i {
		if s.phase != parentCommitting {
			s.phase = parentRetired
		}
		s.cancel()
	}
}
func parentMutationID(id string) bool {
	return id != "" && len(id) <= 256 && id == strings.TrimSpace(id)
}
func parentMutationLive(s *parentMutationState) bool {
	return s != nil && s.ctx.Err() == nil && time.Now().Before(s.deadline)
}
func parentMutationMin(at time.Time, candidates ...*time.Time) time.Time {
	for _, candidate := range candidates {
		if candidate != nil && candidate.Before(at) {
			at = *candidate
		}
	}
	return at
}

// PrepareKidsProfileChange reads one consistent authoritative PostgreSQL tuple.
// The original full session is taken only from server authentication context,
// never request JSON or a caller-supplied role/verified boolean. No row is locked
// while the parent subsequently confirms their actual original password.
func (ds *DataStore) PrepareKidsProfileChange(r *http.Request, targetID string, enabled bool) (*ParentKidsIntent, error) {
	if ds == nil || ds.pool == nil || r == nil || r.Context().Err() != nil || !parentMutationID(targetID) {
		return nil, ErrParentMutationUnavailable
	}
	original, ok := auth.GetSession(r)
	if !ok || !parentMutationID(original.Token) || !parentMutationID(original.AccountID) || auth.GetAccountID(r) != original.AccountID || original.Scope != "" || original.ScopeResource != "" || original.IsMaster != auth.IsMaster(r) || !original.ExpiresAt.After(time.Now()) {
		return nil, ErrParentMutationChanged
	}
	deadline := parentMutationMin(time.Now().Add(parentMutationBudget), &original.ExpiresAt)
	if at, ok := r.Context().Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	snapshot, dbNow, err := readParentMutationSnapshot(ctx, ds.pool, original.Token, targetID)
	if err != nil {
		cancel()
		return nil, ErrParentMutationUnavailable
	}
	deadline = parentMutationMin(deadline, &snapshot.sessionExpiry, snapshot.actorExpiry, snapshot.ownerExpiry)
	if !parentMutationOriginal(snapshot, original) || !deadline.After(time.Now()) || !deadline.After(dbNow) || ctx.Err() != nil {
		cancel()
		return nil, ErrParentMutationChanged
	}
	// Preserve the earlier original bound; never recapture a renewed session.
	bounded, stop := context.WithDeadline(ctx, deadline)
	state := &parentMutationState{store: ds, ctx: bounded, cancel: func() { stop(); cancel() }, deadline: deadline, original: original, snapshot: snapshot, enabled: enabled, phase: parentPrepared}
	intent := &ParentKidsIntent{state: state}
	state.intent = intent
	return intent, nil
}
func parentMutationOriginal(v parentMutationSnapshot, original models.Session) bool {
	return v.token == original.Token && v.actorID == original.AccountID && v.clientID == original.ClientID && v.sessionCreated.Equal(original.CreatedAt) && v.isMaster == original.IsMaster && v.hash != "" && (v.ownerID == v.actorID || v.isMaster)
}

// VerifyCredential always performs bcrypt, including genuine masters. The hash
// came from this intent's original actor row. Commit must still find that exact
// row version/hash. An external verified=true/callback is never accepted here.
// This uninstalled primitive does not replace the public shared attempt budget.
func (i *ParentKidsIntent) VerifyCredential(password string) (*ParentKidsProof, error) {
	if i == nil || i.state == nil {
		return nil, ErrParentMutationUsed
	}
	s := i.state
	s.mu.Lock()
	if s.intent != i || s.phase != parentPrepared {
		s.mu.Unlock()
		return nil, ErrParentMutationUsed
	}
	if !parentMutationLive(s) {
		s.phase = parentRetired
		s.cancel()
		s.mu.Unlock()
		return nil, ErrParentMutationExpired
	}
	s.phase = parentVerifying
	hash := s.snapshot.hash
	s.mu.Unlock()
	var compared error
	if len(password) == 0 || len(password) > 4096 {
		compared = ErrParentMutationCredential
	} else {
		compared = bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || !parentMutationLive(s) {
		s.phase = parentRetired
		s.cancel()
		return nil, ErrParentMutationExpired
	}
	if compared != nil {
		s.phase = parentRetired
		s.cancel()
		return nil, ErrParentMutationCredential
	}
	proof := &ParentKidsProof{state: s}
	s.proof = proof
	s.phase = parentVerified
	return proof, nil
}

type parentMutationQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Raw JSONB includes private credential/PIN fields solely inside this opaque
// tuple. xmin plus physical row identity detects updates/ABA, including changes
// which restore every public field and updated_at to their previous values.
func readParentMutationSnapshot(ctx context.Context, q parentMutationQuerier, token, target string) (parentMutationSnapshot, time.Time, error) {
	var v parentMutationSnapshot
	var now time.Time
	err := q.QueryRow(ctx, `SELECT to_jsonb(s),concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),
 to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),
 to_jsonb(u),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),
 to_jsonb(o),concat(o.tableoid::oid,':',o.xmin::text,':',o.ctid::text),clock_timestamp()
 FROM sessions s JOIN accounts a ON a.id=s.account_id
 JOIN users u ON u.id=$2 JOIN accounts o ON o.id=u.account_id
 WHERE s.token=$1`, token, target).Scan(&v.session.raw, &v.session.version, &v.parent.raw, &v.parent.version, &v.target.raw, &v.target.version, &v.owner.raw, &v.owner.version, &now)
	if err != nil {
		return v, now, err
	}
	var session struct {
		Token     string    `json:"token"`
		AccountID string    `json:"account_id"`
		ClientID  string    `json:"client_id"`
		Scope     string    `json:"scope"`
		Resource  string    `json:"scope_resource"`
		Expiry    time.Time `json:"expires_at"`
		Created   time.Time `json:"created_at"`
	}
	var actor struct {
		ID     string     `json:"id"`
		Hash   string     `json:"password_hash"`
		Master bool       `json:"is_master"`
		Expiry *time.Time `json:"expires_at"`
	}
	var user struct {
		ID        string `json:"id"`
		AccountID string `json:"account_id"`
	}
	var owner struct {
		ID     string     `json:"id"`
		Expiry *time.Time `json:"expires_at"`
	}
	if json.Unmarshal(v.session.raw, &session) != nil || json.Unmarshal(v.parent.raw, &actor) != nil || json.Unmarshal(v.target.raw, &user) != nil || json.Unmarshal(v.owner.raw, &owner) != nil {
		return v, now, ErrParentMutationUnavailable
	}
	if session.Token != token || session.AccountID != actor.ID || user.ID != target || user.AccountID != owner.ID || !parentMutationID(actor.ID) || !parentMutationID(owner.ID) || session.Scope != "" || session.Resource != "" || !session.Expiry.After(now) || actor.Expiry != nil && !actor.Expiry.After(now) || owner.Expiry != nil && !owner.Expiry.After(now) {
		return v, now, ErrParentMutationChanged
	}
	v.token = token
	v.actorID = actor.ID
	v.targetID = target
	v.ownerID = owner.ID
	v.clientID = session.ClientID
	v.hash = actor.Hash
	v.isMaster = actor.Master
	v.sessionExpiry = session.Expiry
	v.sessionCreated = session.Created
	v.actorExpiry = actor.Expiry
	v.ownerExpiry = owner.Expiry
	return v, now, nil
}
func sameParentMutationRow(a, b parentMutationRow) bool {
	return a.version == b.version && bytes.Equal(a.raw, b.raw)
}
func sameParentMutationSnapshot(a, b parentMutationSnapshot) bool {
	return sameParentMutationRow(a.session, b.session) && sameParentMutationRow(a.parent, b.parent) && sameParentMutationRow(a.target, b.target) && sameParentMutationRow(a.owner, b.owner)
}

// CommitKidsProfileChange atomically applies ONLY is_kids_profile and updated_at.
// It does not apply legacy defaults/reset lists, update caches or install family
// policy. Proofs are consumed before any transaction and are never retried.
// Cancellation before commit rolls back; an uncertain commit acknowledgement
// is OutcomeUnknown, never a claim that no write occurred.
func (ds *DataStore) CommitKidsProfileChange(i *ParentKidsIntent, p *ParentKidsProof) (*models.User, error) {
	if ds == nil || ds.pool == nil || i == nil || i.state == nil || p == nil {
		return nil, ErrParentMutationUsed
	}
	s := i.state
	s.mu.Lock()
	if s.store != ds || s.intent != i || s.proof != p || p.state != s || s.phase != parentVerified {
		s.mu.Unlock()
		return nil, ErrParentMutationUsed
	}
	if !parentMutationLive(s) {
		s.phase = parentRetired
		s.cancel()
		s.mu.Unlock()
		return nil, ErrParentMutationExpired
	}
	s.phase = parentCommitting
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.phase = parentRetired; s.cancel(); s.mu.Unlock() }()
	tx, err := ds.pool.Begin(s.ctx)
	if err != nil {
		return nil, ErrParentMutationUnavailable
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(ctx)
	}()
	// Every transaction uses the same table order; account IDs are sorted and
	// deduplicated when actor and owner coincide. No account/profile service lock.
	ids := []string{s.snapshot.actorID}
	if s.snapshot.ownerID != s.snapshot.actorID {
		ids = append(ids, s.snapshot.ownerID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(s.ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil || found != id {
			return nil, ErrParentMutationChanged
		}
	}
	var found string
	if err = tx.QueryRow(s.ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.snapshot.token).Scan(&found); err != nil || found != s.snapshot.token {
		return nil, ErrParentMutationChanged
	}
	if err = tx.QueryRow(s.ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.snapshot.targetID).Scan(&found); err != nil || found != s.snapshot.targetID {
		return nil, ErrParentMutationChanged
	}
	current, dbNow, err := readParentMutationSnapshot(s.ctx, tx, s.snapshot.token, s.snapshot.targetID)
	if err != nil || !sameParentMutationSnapshot(current, s.snapshot) || !parentMutationOriginal(current, s.original) {
		return nil, ErrParentMutationChanged
	}
	if !parentMutationLive(s) || !s.deadline.After(dbNow) {
		return nil, ErrParentMutationExpired
	}
	updated, err := scanUser(tx.QueryRow(s.ctx, `UPDATE users AS u SET is_kids_profile=$1,updated_at=clock_timestamp()
 WHERE id=$2 AND concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)=$3 AND clock_timestamp()<$4 RETURNING `+userColumns, s.enabled, s.snapshot.targetID, s.snapshot.target.version, s.deadline))
	if err != nil {
		return nil, ErrParentMutationUnavailable
	}
	if updated == nil {
		return nil, ErrParentMutationChanged
	}
	if !parentMutationLive(s) {
		return nil, ErrParentMutationExpired
	}
	if err = tx.Commit(s.ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return nil, ErrParentMutationUnavailable
		}
		return nil, ErrParentMutationOutcomeUnknown
	}
	return updated, nil
}
