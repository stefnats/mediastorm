package datastore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/models"
)

const ParentProfileCreationBodyLimit = 16 << 10

var (
	ErrParentProfileCreationChanged      = errors.New("profile creation confirmation changed")
	ErrParentProfileCreationUnavailable  = errors.New("profile creation confirmation unavailable")
	ErrParentProfileCreationCredential   = errors.New("profile creation credential rejected")
	ErrParentProfileCreationUnknown      = errors.New("profile creation outcome unknown")
	ErrParentProfileCreationPayload      = errors.New("invalid profile creation request")
	ErrParentProfileCreationBodyTooLarge = errors.New("profile creation request exceeds sixteen kibibytes")
)

type parentCreationDesired struct {
	Name      string `json:"name"`
	Color     string `json:"color"`
	AccountID string `json:"accountId"`
	Kids      bool   `json:"isKidsProfile"`
}
type parentCreationFacts struct {
	session, actor, owner parentMutationRow
	sessionEpoch          int64
	grants                json.RawMessage
	profileKeys           []string
}

type parentCreationState struct {
	mu                                   sync.Mutex
	self                                 *ParentProfileCreationIntent
	store                                *DataStore
	original                             models.Session
	desired                              parentCreationDesired
	facts                                parentCreationFacts
	hashes                               []string
	body                                 []byte
	method, path, rawPath, query, client string
	deadline                             time.Time
	life                                 context.Context
	stop                                 context.CancelFunc
	retry, run                           context.Context
	cancel                               context.CancelFunc
	stopRetry, stopLife                  func() bool
	reference                            *creationOperationReference
	phase                                parentMutationPhase
	bound, committed, taken              bool
	bytes                                int
	compare                              func([]byte, []byte) error
	users, settings                      *ParentProfileCreationCacheLease
}

// ParentProfileCreationIntent is original authenticated management authority,
// distinct from the private ledger's correlation-only metadata and references.
type ParentProfileCreationIntent struct{ state *parentCreationState }

func (*ParentProfileCreationIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentProfileCreationIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentProfileCreationIntent(opaque)"))
}
func (i *ParentProfileCreationIntent) Close() {
	if i == nil || i.state == nil {
		return
	}
	s := i.state
	// self/stop are immutable after publication. A copied handle cannot cancel
	// the original. Cancel BEFORE joining SQL work that owns the state mutex.
	if s.self != i {
		return
	}
	s.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = parentRetired
	if s.cancel != nil {
		s.cancel()
	}
	if s.stopRetry != nil {
		s.stopRetry()
	}
	if s.stopLife != nil {
		s.stopLife()
	}
	if s.reference != nil {
		s.reference.close()
	}
}
func parentCreationDecode(body []byte) (parentCreationDesired, error) {
	var d parentCreationDesired
	if len(body) > ParentProfileCreationBodyLimit {
		return d, ErrParentProfileCreationBodyTooLarge
	}
	// Preserve the existing management single Decode, unknown fields, casefold,
	// duplicate/null and trailing semantics. The cookie middleware is stricter.
	if json.NewDecoder(bytes.NewReader(body)).Decode(&d) != nil {
		return d, ErrParentProfileCreationPayload
	}
	d.Name = strings.TrimSpace(d.Name)
	d.Color = strings.TrimSpace(d.Color)
	d.AccountID = strings.TrimSpace(d.AccountID)
	if d.Name == "" {
		return d, ErrParentProfileCreationPayload
	}
	if !d.Kids {
		return d, ErrParentProfileCreationChanged
	}
	return d, nil
}
func readParentCreationFacts(ctx context.Context, q parentMutationQuerier, original models.Session, owner string, hashes []string) (parentCreationFacts, time.Time, *time.Time, error) {
	var out parentCreationFacts
	frame, now, err := readPGRequestFrame(ctx, q, original.Token)
	if err != nil {
		return out, now, nil, err
	}
	if frame.session != original || original.Scope != "" || original.ScopeResource != "" || owner != original.AccountID && !original.IsMaster {
		return out, now, nil, ErrParentProfileCreationChanged
	}
	out.session = frame.sessionRow
	out.actor = frame.actorRow
	err = q.QueryRow(ctx, `SELECT to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text) FROM accounts a WHERE id=$1`, owner).Scan(&out.owner.raw, &out.owner.version)
	if err != nil {
		return out, now, nil, pinContextError(ctx, ErrParentProfileCreationChanged)
	}
	var own struct {
		ID     string     `json:"id"`
		Expiry *time.Time `json:"expires_at"`
	}
	if !profilePinRequiredFields(out.owner.raw, []string{"id", "expires_at"}, true) || json.Unmarshal(out.owner.raw, &own) != nil || own.ID != owner || own.Expiry != nil && !own.Expiry.After(now) {
		return out, now, nil, ErrParentProfileCreationChanged
	}
	if !profilePinRequiredFields(out.actor.raw, []string{"password_hash"}, false) {
		return out, now, nil, ErrParentProfileCreationUnavailable
	}
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if json.Unmarshal(out.actor.raw, &actor) != nil {
		return out, now, nil, ErrParentProfileCreationUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return out, now, nil, ErrParentProfileCreationUnavailable
	}
	lineage := pinSessionLineage(out.session.raw, original.Token)
	err = q.QueryRow(ctx, `SELECT COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='session' AND scope_key=$1),0),
 COALESCE((SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(g),'version',concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text),'epoch',COALESCE(p.generation,0)) ORDER BY g.token_hash)
 FROM profile_unlocks g LEFT JOIN profile_unlock_revocations p ON p.scope='profile' AND p.scope_key=g.profile_id WHERE g.token_hash=ANY($2::text[])),'[]'::jsonb)`, lineage, hashes).Scan(&out.sessionEpoch, &out.grants)
	if err != nil {
		return out, now, nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	var grants []struct {
		Row struct {
			ProfileID string `json:"profile_id"`
		} `json:"row"`
	}
	if json.Unmarshal(out.grants, &grants) != nil {
		return out, now, nil, ErrParentProfileCreationUnavailable
	}
	for _, g := range grants {
		if !parentMutationID(g.Row.ProfileID) {
			return out, now, nil, ErrParentProfileCreationUnavailable
		}
		out.profileKeys = append(out.profileKeys, g.Row.ProfileID)
	}
	sort.Strings(out.profileKeys)
	return out, now, own.Expiry, nil
}
func sameParentCreationFacts(a, b parentCreationFacts) bool {
	return sameParentMutationRow(a.session, b.session) && sameParentMutationRow(a.actor, b.actor) && sameParentMutationRow(a.owner, b.owner) && a.sessionEpoch == b.sessionEpoch && bytes.Equal(a.grants, b.grants)
}
func (ds *DataStore) PrepareParentProfileCreationManagement(r *http.Request, body []byte, tokens []string) (*ParentProfileCreationIntent, error) {
	if ds == nil || ds.pool == nil || r == nil {
		return nil, ErrParentProfileCreationUnavailable
	}
	desired, err := parentCreationDecode(body)
	if err != nil {
		return nil, err
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, http.MethodPost)
	if err != nil {
		return nil, ErrParentProfileCreationChanged
	}
	if err = ds.CheckProfilePinRequestFrame(r); err != nil {
		return nil, err
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentProfileCreationChanged
	}
	if desired.AccountID == "" {
		if original.IsMaster {
			return nil, ErrParentProfileCreationPayload
		}
		desired.AccountID = original.AccountID
	}
	if !parentMutationID(desired.AccountID) || desired.AccountID != original.AccountID && !original.IsMaster {
		return nil, ErrParentProfileCreationChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil {
		return nil, err
	}
	deadline := parentMutationMin(time.Now().Add(parentModeLifetime), &original.ExpiresAt)
	if at, yes := r.Context().Deadline(); yes {
		deadline = parentMutationMin(deadline, &at)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	facts, _, ownerExpiry, err := readParentCreationFacts(ctx, tx, original, desired.AccountID, hashes)
	if err != nil {
		return nil, err
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || !sameParentMutationRow(frame.sessionRow, facts.session) || !sameParentMutationRow(frame.actorRow, facts.actor) {
		return nil, ErrParentProfileCreationChanged
	}
	deadline = parentMutationMin(deadline, frame.actorExpiry, ownerExpiry)
	size := 2*len(body) + len(desired.Name) + len(desired.Color) + len(desired.AccountID) + len(facts.session.raw) + len(facts.actor.raw) + len(facts.owner.raw) + len(facts.grants) + len(path) + len(query) + len(r.URL.RawPath) + len(client) + len(hashes)*64 + 2048
	if size > parentModeSnapshotLimit {
		return nil, ErrParentProfileCreationUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	// Bounded pre-admission bookkeeping owns no cache/operation-row lock.
	// Its ambiguous ACK belongs to OLD records, never to a new reservation.
	maintenance, endMaintenance := context.WithDeadline(r.Context(), deadline)
	defer endMaintenance()
	if _, err = ds.retireExpiredParentProfileReservations(maintenance); err != nil {
		return nil, pinContextError(maintenance, ErrParentProfileCreationUnavailable)
	}
	if _, err = ds.pruneProfileCreationOperations(maintenance, 64); err != nil {
		return nil, pinContextError(maintenance, ErrParentProfileCreationUnavailable)
	}
	// Deliberate HTTP continuation: capture/admission remains cancelable by the
	// first request until successful transfer. It is never a reusable permit.
	life, stop := context.WithDeadline(context.Background(), deadline)
	cancelCapture := context.AfterFunc(r.Context(), stop)
	transfer := false
	defer func() {
		cancelCapture()
		if !transfer {
			stop()
		}
	}()
	requestRaw, _ := json.Marshal([]string{r.Method, path, r.URL.RawPath, query, client})
	metadata := creationOperationMetadata{ActorID: original.AccountID, OwnerID: desired.AccountID, Entry: creationOperationManagement, SessionDigest: creationOperationDigest([]byte(original.Token)), ClientDigest: creationOperationDigest([]byte(original.ClientID)), RequestDigest: creationOperationDigest(requestRaw), BodyDigest: creationOperationDigest(body), BodyBytes: int64(len(body)), SessionRowDigest: creationOperationDigest(facts.session.raw), ActorRowDigest: creationOperationDigest(facts.actor.raw), OwnerRowDigest: creationOperationDigest(facts.owner.raw), SessionVersion: facts.session.version, ActorVersion: facts.actor.version, OwnerVersion: facts.owner.version, SessionExpiry: creationOperationUTC(original.ExpiresAt)}
	if frame.actorExpiry != nil {
		v := creationOperationUTC(*frame.actorExpiry)
		metadata.ActorExpiry = &v
	}
	if ownerExpiry != nil {
		v := creationOperationUTC(*ownerExpiry)
		metadata.OwnerExpiry = &v
	}
	reference, err := ds.reserveProfileCreationOperationQualified(life, metadata, true)
	if err != nil {
		return nil, parentCreationError(err)
	}
	if err = r.Context().Err(); err != nil {
		reference.close()
		return nil, err
	}
	s := &parentCreationState{store: ds, original: original, desired: desired, facts: facts, hashes: hashes, body: bytes.Clone(body), method: r.Method, path: path, rawPath: r.URL.RawPath, query: query, client: client, deadline: deadline, life: life, stop: stop, reference: reference, bytes: size, compare: bcrypt.CompareHashAndPassword, phase: parentPrepared}
	i := &ParentProfileCreationIntent{state: s}
	s.self = i
	// Cancel raced with transfer still fails closed via the life context.
	if !cancelCapture() && life.Err() != nil {
		i.Close()
		return nil, ErrParentProfileCreationChanged
	}
	if life.Err() != nil {
		i.Close()
		return nil, ErrParentProfileCreationChanged
	}
	transfer = true
	return i, nil
}
func parentCreationError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, errCreationOperationUnknown):
		return ErrParentProfileCreationUnknown
	case errors.Is(err, errCreationOperationChanged):
		return ErrParentProfileCreationChanged
	default:
		return ErrParentProfileCreationUnavailable
	}
}
func (ds *DataStore) ParentProfileCreationBudget(i *ParentProfileCreationIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentProfileCreationChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}
func (ds *DataStore) BindParentProfileCreationManagementRetry(r *http.Request, i *ParentProfileCreationIntent, body []byte, tokens []string) error {
	if i == nil || i.state == nil || r == nil {
		return ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return ErrParentProfileCreationChanged
	}
	echoes := r.Header.Values(ProfileCreationOperationHeader)
	if len(echoes) != 1 || echoes[0] != parentProfileCreationLocator(s.reference) {
		return ErrParentProfileCreationChanged
	}
	s.phase = parentVerifying
	path, query, client, err := parentConfirmationRequestBoundary(r, http.MethodPost)
	if err != nil {
		return ErrParentProfileCreationChanged
	}
	if !bytes.Equal(body, s.body) || r.Method != s.method || path != s.path || r.URL.RawPath != s.rawPath || query != s.query || client != s.client {
		return ErrParentProfileCreationChanged
	}
	original, ok := auth.GetSession(r)
	hashes, err := parentModeHashes(tokens)
	if err != nil || !ok || original != s.original || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		return ErrParentProfileCreationChanged
	}
	s.run, s.cancel = context.WithDeadline(r.Context(), s.deadline)
	s.retry = r.Context()
	s.stopRetry = context.AfterFunc(s.run, s.stop)
	// Close/expiry must interrupt retry SQL even while Bind owns s.mu.
	s.stopLife = context.AfterFunc(s.life, s.cancel)
	if err = ds.CheckProfilePinRequestFrame(r.WithContext(s.run)); err != nil {
		return err
	}
	tx, err := ds.pool.BeginTx(s.run, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return pinContextError(s.run, ErrParentProfileCreationUnavailable)
	}
	defer rollbackPinTx(tx, s.run)
	facts, _, _, err := readParentCreationFacts(s.run, tx, s.original, s.desired.AccountID, s.hashes)
	if err != nil {
		return err
	}
	if !sameParentCreationFacts(facts, s.facts) {
		return ErrParentProfileCreationChanged
	}
	if err = tx.Commit(s.run); err != nil {
		return pinContextError(s.run, ErrParentProfileCreationUnavailable)
	}
	if s.run.Err() != nil {
		return s.run.Err()
	}
	s.phase = parentVerified
	return nil
}
func (ds *DataStore) BoundParentProfileCreationRequest(r *http.Request, i *ParentProfileCreationIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.bound || s.retry != r.Context() || s.run == nil || s.run.Err() != nil {
		return nil, ErrParentProfileCreationChanged
	}
	s.bound = true
	s.retry = s.run
	return r.WithContext(s.run), nil
}
func (ds *DataStore) VerifyParentProfileCreationPassword(r *http.Request, i *ParentProfileCreationIntent) error {
	if i == nil || i.state == nil || r == nil {
		return ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || !s.bound || s.retry != r.Context() || s.run.Err() != nil || s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentProfileCreationChanged
	}
	s.phase = parentVerifying
	var actor struct {
		Hash string `json:"password_hash"`
	}
	err := json.Unmarshal(s.facts.actor.raw, &actor)
	password := r.Header.Get(profilePinParentPasswordHeader)
	compare := s.compare
	s.mu.Unlock()
	if strings.HasPrefix(password, "base64:") {
		raw, e := base64.StdEncoding.DecodeString(strings.TrimPrefix(password, "base64:"))
		if e != nil {
			return ErrParentProfileCreationCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentProfileCreationCredential
	}
	err = compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run.Err() != nil {
		return s.run.Err()
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentProfileCreationChanged
	}
	if err != nil {
		s.phase = parentRetired
		return ErrParentProfileCreationCredential
	}
	s.phase = parentCommitting
	return nil
}

// Cache leases are held by actual service locks in Users -> Settings order.
// They do not certify absence in SQL or authorize another create.
type ParentProfileCreationCacheLease struct {
	self                     *ParentProfileCreationCacheLease
	intent                   *ParentProfileCreationIntent
	settings, closed, staged bool
}

func (*ParentProfileCreationCacheLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentProfileCreationCacheLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentProfileCreationCacheLease(opaque)"))
}
func (l *ParentProfileCreationCacheLease) Taken() bool {
	if l == nil || l.self != l || l.intent == nil {
		return false
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.taken
}
func (l *ParentProfileCreationCacheLease) Retire() bool {
	if l == nil || l.self != l || l.intent == nil {
		return false
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	l.closed = true
	return s.taken
}
func (ds *DataStore) ParentProfileCreationTarget(ctx context.Context, i *ParentProfileCreationIntent) (string, string, error) {
	if i == nil || i.state == nil {
		return "", "", ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || ctx == nil || s.run != ctx || ctx.Err() != nil || s.life.Err() != nil || s.reference == nil {
		return "", "", ErrParentProfileCreationChanged
	}
	return s.reference.record.target, s.reference.record.id, nil
}
func (ds *DataStore) BeginParentProfileCreationUsersLease(ctx context.Context, i *ParentProfileCreationIntent) (*ParentProfileCreationCacheLease, error) {
	if _, _, err := ds.ParentProfileCreationTarget(ctx, i); err != nil {
		return nil, err
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.users != nil || s.phase != parentCommitting {
		return nil, ErrParentProfileCreationChanged
	}
	l := &ParentProfileCreationCacheLease{intent: i}
	l.self = l
	s.users = l
	return l, nil
}
func (ds *DataStore) BeginParentProfileCreationSettingsLease(ctx context.Context, i *ParentProfileCreationIntent, u *ParentProfileCreationCacheLease) (*ParentProfileCreationCacheLease, error) {
	if _, _, err := ds.ParentProfileCreationTarget(ctx, i); err != nil {
		return nil, err
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if u == nil || u.self != u || u != s.users || u.closed || s.settings != nil {
		return nil, ErrParentProfileCreationChanged
	}
	l := &ParentProfileCreationCacheLease{intent: i, settings: true}
	l.self = l
	s.settings = l
	return l, nil
}

type ParentProfileCreationReceipt struct {
	self     *ParentProfileCreationReceipt
	store    *DataStore
	intent   *ParentProfileCreationIntent
	user     UserWriteRecord
	settings UserSettingsRecord
	used     bool
}

func (*ParentProfileCreationReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentProfileCreationReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentProfileCreationReceipt(opaque)"))
}
func (ds *DataStore) StageParentProfileCreationUsers(l *ParentProfileCreationCacheLease, r *ParentProfileCreationReceipt) (UserWriteRecord, error) {
	if l == nil || l.self != l || l.intent == nil {
		return UserWriteRecord{}, ErrParentProfileCreationChanged
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != ds || r == nil || r.self != r || r.store != ds || r.intent != l.intent || r.used || !s.committed || l != s.users || l.closed || l.staged {
		return UserWriteRecord{}, ErrParentProfileCreationChanged
	}
	l.staged = true
	return r.user, nil
}
func (ds *DataStore) StageParentProfileCreationSettings(l *ParentProfileCreationCacheLease, r *ParentProfileCreationReceipt) (UserSettingsRecord, error) {
	if l == nil || l.self != l || l.intent == nil {
		return UserSettingsRecord{}, ErrParentProfileCreationChanged
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != ds || r == nil || r.self != r || r.store != ds || r.intent != l.intent || r.used || !s.committed || l != s.settings || l.closed || l.staged {
		return UserSettingsRecord{}, ErrParentProfileCreationChanged
	}
	l.staged = true
	return r.settings, nil
}
func (ds *DataStore) TakeParentProfileCreationReceipt(i *ParentProfileCreationIntent, r *ParentProfileCreationReceipt, u, v *ParentProfileCreationCacheLease) error {
	if i == nil || i.state == nil {
		return ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || r == nil || r.self != r || r.store != ds || r.intent != i || r.used || !s.committed || s.taken || u == nil || v == nil || u.self != u || v.self != v || u != s.users || v != s.settings || u.closed || v.closed || !u.staged || !v.staged {
		return ErrParentProfileCreationChanged
	}
	r.used = true
	s.taken = true
	return nil
}
func (ds *DataStore) CommitParentProfileCreation(i *ParentProfileCreationIntent) (*ParentProfileCreationReceipt, error) {
	if i == nil || i.state == nil {
		return nil, ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil || s.users == nil || s.settings == nil || s.users.closed || s.settings.closed {
		s.mu.Unlock()
		return nil, ErrParentProfileCreationChanged
	}
	s.phase = parentRetired
	s.mu.Unlock()
	if s.run.Err() != nil {
		return nil, s.run.Err()
	}
	if s.life.Err() != nil {
		return nil, ErrParentProfileCreationChanged
	}
	attempt, err := ds.claimProfileCreationAttempt(s.life, s.reference)
	if err != nil {
		return nil, parentCreationError(err)
	}
	defer attempt.close()
	fence, err := ds.beginProfileCreationOperation(s.life, attempt)
	if err != nil {
		return nil, parentCreationError(err)
	}
	defer fence.close()
	ctx, tx := fence.run, fence.tx
	// Operation fence comes first. No admission budget is taken while it is held.

	for _, id := range s.facts.profileKeys {
		if _, err = lockPinEpoch(ctx, tx, "profile", id); err != nil {
			return nil, err
		}
	}
	epoch, err := lockPinEpoch(ctx, tx, "session", pinSessionLineage(s.facts.session.raw, s.original.Token))
	if err != nil {
		return nil, err
	}
	if epoch != s.facts.sessionEpoch {
		return nil, ErrParentProfileCreationChanged
	}
	ids := []string{s.original.AccountID}
	if s.desired.AccountID != s.original.AccountID {
		ids = append(ids, s.desired.AccountID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return nil, pinContextError(ctx, ErrParentProfileCreationChanged)
		}
	}
	var token string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&token); err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	current, now, _, err := readParentCreationFacts(ctx, tx, s.original, s.desired.AccountID, s.hashes)
	if err != nil {
		return nil, err
	}
	if !sameParentCreationFacts(current, s.facts) {
		return nil, ErrParentProfileCreationChanged
	}
	// Relation topology is acquired before any target row. 079's retained
	// tombstone forbids reusing an ID even when the current User is absent.

	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id=$1) OR EXISTS(SELECT 1 FROM user_settings WHERE user_id=$1) OR EXISTS(SELECT 1 FROM user_settings_versions WHERE user_id=$1)`, fence.record.target).Scan(&exists); err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	if exists {
		return nil, ErrParentProfileCreationChanged
	}
	if !now.Before(s.deadline) || s.run.Err() != nil {
		return nil, ErrParentProfileCreationChanged
	}
	user := models.User{ID: fence.record.target, AccountID: s.desired.AccountID, Name: s.desired.Name, Color: s.desired.Color, ActivityPrivacy: models.ActivityPrivacySharedAnonymous, IsKidsProfile: true, KidsMode: "rating", KidsMaxRating: "G", CreatedAt: now, UpdatedAt: now}
	record, err := ds.insertUserWrite(ctx, tx, user)
	if err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationChanged)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO user_settings(user_id,settings,updated_at) VALUES($1,'{"display":{"simpleMode":true}}'::jsonb,clock_timestamp())`, user.ID); err != nil {
		return nil, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	settings, err := ds.scanSettings(tx.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, user.ID))
	if err != nil {
		return nil, err
	}
	if s.run.Err() != nil {
		return nil, s.run.Err()
	}
	if err = commitQualifiedProfileCreationResult(ctx, tx, fence.record, record, settings); err != nil {
		return nil, err
	}
	// Terminal means ledger bookkeeping, never public restart/recovery evidence.
	// The actual User+Settings and terminal state commit in this SAME transaction.
	if err = fence.finish(s.life); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) || errors.Is(err, pgx.ErrTxCommitRollback) {
			return nil, ErrParentProfileCreationUnavailable
		}
		return nil, parentCreationError(err)
	}
	s.mu.Lock()
	s.committed = true
	s.mu.Unlock()
	r := &ParentProfileCreationReceipt{store: ds, intent: i, user: record, settings: settings}
	r.self = r
	return r, nil
}

// This bounded maintenance operation retires only expired, never-attempted
// reservations. It returns no ref, execution authority, User fact or refund.
func (ds *DataStore) retireExpiredParentProfileReservations(ctx context.Context) (int, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return 0, ErrParentProfileCreationUnavailable
	}
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	if err = creationOperationBudgetLock(ctx, tx); err != nil {
		return 0, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	tag, err := tx.Exec(ctx, `WITH candidates AS (SELECT operation_id FROM profile_creation_operations WHERE state='reserved' AND write_until<=clock_timestamp() ORDER BY operation_id LIMIT 32 FOR UPDATE SKIP LOCKED) UPDATE profile_creation_operations o SET state='retired_unattempted',version=o.version+1,settled_at=clock_timestamp() FROM candidates c WHERE o.operation_id=c.operation_id AND o.state='reserved' AND o.write_until<=clock_timestamp()`)
	if err != nil {
		return 0, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, ErrParentProfileCreationUnknown
	}
	return int(tag.RowsAffected()), nil
}
