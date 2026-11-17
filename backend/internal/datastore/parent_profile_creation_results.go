package datastore

import (
	"bytes"
	"context"
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
	"novastream/internal/auth"
	"novastream/models"
)

const ProfileCreationOperationHeader = "X-Profile-Creation-Operation"

var ErrProfileCreationResultMissing = errors.New("profile creation result unqualified or unavailable to this actor")

func parentProfileCreationLocator(ref *creationOperationReference) string {
	if ref == nil || ref.self != ref {
		return ""
	}
	return "pcr_" + hex.EncodeToString(ref.raw[:])
}
func (ds *DataStore) ParentProfileCreationLocator(i *ParentProfileCreationIntent) (string, error) {
	if i == nil || i.state == nil {
		return "", ErrParentProfileCreationChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", ErrParentProfileCreationChanged
	}
	return parentProfileCreationLocator(s.reference), nil
}
func enrollProfileCreationResult(ctx context.Context, tx pgx.Tx, r creationOperationRecord) error {
	_, err := tx.Exec(ctx, `INSERT INTO profile_creation_results(operation_id,target_id,marker_id,reference_hash,owner_id,protocol_version,outcome,captured_at,write_until,read_until)
        VALUES($1,$2,$3,$4,$5,1,'prepared',$6,$7,$8)`, r.id, r.target, r.marker, r.referenceHash, r.metadata.OwnerID, r.captured, r.writeUntil, r.readUntil)
	if err != nil {
		return pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	return nil
}
func commitQualifiedProfileCreationResult(ctx context.Context, tx pgx.Tx, r creationOperationRecord, u UserWriteRecord, s UserSettingsRecord) error {
	if s.Revision.snapshot == nil || u.User.ID != r.target || s.ID != r.target {
		return ErrParentProfileCreationChanged
	}
	raw, err := json.Marshal(u.User)
	if err != nil {
		return ErrParentProfileCreationUnavailable
	}
	tag, err := tx.Exec(ctx, `UPDATE profile_creation_results SET outcome='created',result_version=result_version+1,initial_user_digest=$3,initial_settings_digest=$4,initial_settings_generation=$5
      WHERE operation_id=$1 AND reference_hash=$2 AND protocol_version=1 AND outcome='prepared' AND attempted AND NOT birth_invalidated`, r.id, r.referenceHash, creationOperationDigest(raw), creationOperationDigest(s.Revision.snapshot.raw), s.Revision.snapshot.generation)
	if err != nil || tag.RowsAffected() != 1 {
		return pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	return nil
}

// Only bounded maintenance under the existing budget calls this helper. It
// never waits on a result held by a User writer and never observes User absence.
func pruneUnattemptedCreationResult(ctx context.Context, tx pgx.Tx, r creationOperationRecord) (bool, error) {
	result, err := scanCreationResult(tx.QueryRow(ctx, `SELECT `+creationResultColumns+` FROM profile_creation_results WHERE operation_id=$1 FOR UPDATE SKIP LOCKED`, r.id))
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM profile_creation_results WHERE operation_id=$1)`, r.id).Scan(&exists); err != nil {
			return false, pinContextError(ctx, ErrParentProfileCreationUnavailable)
		}
		return !exists, nil
	}
	if err != nil {
		return false, pinContextError(ctx, ErrParentProfileCreationUnavailable)
	}
	if result.hash != r.referenceHash || result.target != r.target || result.marker != r.marker || result.owner != r.metadata.OwnerID || !result.captured.Equal(r.captured) || !result.writeUntil.Equal(r.writeUntil) || !result.readUntil.Equal(r.readUntil) {
		return false, ErrParentProfileCreationChanged
	}
	if result.outcome == "created" || result.outcome == "not_created" && result.attempted {
		return true, nil
	}
	if result.outcome == "legacy_unqualified" || !result.attempted && r.state == "retired_unattempted" && (result.outcome == "prepared" || result.outcome == "not_created") {
		tag, err := tx.Exec(ctx, `DELETE FROM profile_creation_results WHERE operation_id=$1 AND reference_hash=$2 AND result_version=$3`, r.id, r.referenceHash, result.version)
		if err != nil || tag.RowsAffected() != 1 {
			return false, pinContextError(ctx, ErrParentProfileCreationUnavailable)
		}
		return true, nil
	}
	return false, nil
}
func pinContextErrorIf(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return pinContextError(ctx, ErrParentProfileCreationUnavailable)
}

type creationResultRecord struct {
	operation, target, marker, hash, owner, outcome string
	protocol                                        int
	attempted, invalidated                          bool
	version                                         int64
	captured, writeUntil, readUntil                 time.Time
}

const creationResultColumns = `operation_id::text,target_id::text,marker_id::text,reference_hash,owner_id,protocol_version,outcome,attempted,birth_invalidated,result_version,captured_at,write_until,read_until`

func scanCreationResult(row pgx.Row) (creationResultRecord, error) {
	var v creationResultRecord
	err := row.Scan(&v.operation, &v.target, &v.marker, &v.hash, &v.owner, &v.protocol, &v.outcome, &v.attempted, &v.invalidated, &v.version, &v.captured, &v.writeUntil, &v.readUntil)
	if err != nil {
		return v, err
	}
	if !creationOperationHex(v.hash) || !parentMutationID(v.owner) || v.version <= 0 {
		return v, ErrParentProfileCreationUnavailable
	}
	return v, nil
}

// The locator is correlation only. This read captures a NEW verified current
// management frame; it cannot reconstruct, claim or extend the old write intent.
type ProfileCreationRead struct {
	self         *ProfileCreationRead
	store        *DataStore
	mu           sync.Mutex
	request      *http.Request
	original     models.Session
	facts        parentCreationFacts
	hashes       []string
	ctx, run     context.Context
	cancel       context.CancelFunc
	record       creationResultRecord
	recovery     *ProfileRecoveryRead
	snapshot     *ProfileCreationResultSnapshot
	read, closed bool
}
type ProfileCreationResultSnapshot struct {
	self     *ProfileCreationResultSnapshot
	read     *ProfileCreationRead
	outcome  string
	recovery *ProfileRecoverySnapshot
}

func (*ProfileCreationRead) MarshalJSON() ([]byte, error)           { return []byte("null"), nil }
func (*ProfileCreationResultSnapshot) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ProfileCreationRead) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileCreationRead(opaque)"))
}
func (*ProfileCreationResultSnapshot) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileCreationResultSnapshot(opaque)"))
}
func (r *ProfileCreationRead) Close() {
	if r == nil || r.self != r {
		return
	}
	r.cancel() // Interrupt SQL before joining the read mutex.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.recovery != nil {
		r.recovery.Close()
	}
}
func (s *ProfileCreationResultSnapshot) Outcome() string {
	if s == nil || s.self != s {
		return "unqualified_or_missing"
	}
	return s.outcome
}
func (ds *DataStore) PrepareProfileCreationReadManagement(request *http.Request, locator string, tokens []string) (*ProfileCreationRead, error) {
	if ds == nil || ds.pool == nil || request == nil {
		return nil, ErrParentProfileCreationUnavailable
	}
	if len(locator) != 68 || !strings.HasPrefix(locator, "pcr_") || !creationOperationHex(locator[4:]) {
		return nil, ErrProfileCreationResultMissing
	}
	run, cancel := context.WithTimeout(request.Context(), creationOperationReadBudget)
	transferred := false
	defer func() {
		if !transferred {
			cancel()
		}
	}()
	if err := ds.CheckProfilePinRequestFrame(request.WithContext(run)); err != nil {
		return nil, err
	}
	original, ok := authSessionCreationRead(request)
	if !ok {
		return nil, ErrProfileCreationResultMissing
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil {
		return nil, err
	}
	raw, _ := hex.DecodeString(locator[4:])
	// This implicit transaction completes before the operation/auth/User locks.
	// A result relation ACCESS SHARE lock must not precede the User relation.
	record, err := scanCreationResult(ds.pool.QueryRow(run, `SELECT `+creationResultColumns+` FROM profile_creation_results WHERE reference_hash=$1`, creationOperationDigest(raw)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrProfileCreationResultMissing
	}
	if err != nil {
		return nil, pinContextError(request.Context(), ErrParentProfileCreationUnavailable)
	}
	if record.protocol != 1 || record.outcome == "legacy_unqualified" || record.owner != original.AccountID && !original.IsMaster {
		return nil, ErrProfileCreationResultMissing
	}
	facts, _, ownerExpiry, err := readParentCreationFacts(run, ds.pool, original, record.owner, hashes)
	if err != nil {
		cancel()
		return nil, err
	}
	initialDeadline, _ := run.Deadline()
	deadline := parentMutationMin(initialDeadline, &original.ExpiresAt, ownerExpiry)
	frame, _ := request.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || !sameParentMutationRow(frame.actorRow, facts.actor) || !sameParentMutationRow(frame.sessionRow, facts.session) {
		cancel()
		return nil, ErrParentProfileCreationChanged
	}
	deadline = parentMutationMin(deadline, frame.actorExpiry)
	cancel()
	run, cancel = context.WithDeadline(request.Context(), deadline)
	recovery, err := ds.PrepareProfileRecovery(request.Context(), record.target)
	if err != nil {
		cancel()
		return nil, err
	}
	// Preserve original ctx identity for service CAS while imposing the SAME
	// actor/owner/session deadline at the final generic private pair gate.
	recovery.state.cancel()
	recovery.state.run, recovery.state.cancel = context.WithDeadline(request.Context(), deadline)
	r := &ProfileCreationRead{store: ds, request: request, original: original, facts: facts, hashes: hashes, ctx: request.Context(), run: run, cancel: cancel, record: record, recovery: recovery}
	r.self = r
	recovery.state.creation = r
	transferred = true
	return r, nil
}

func authSessionCreationRead(r *http.Request) (models.Session, bool) {
	s, ok := auth.GetSession(r)
	return s, ok && s.Scope == "" && s.ScopeResource == "" && auth.GetAccountID(r) == s.AccountID && auth.IsMaster(r) == s.IsMaster
}
func (ds *DataStore) ProfileCreationRecoveryRead(ctx context.Context, r *ProfileCreationRead) (*ProfileRecoveryRead, string, error) {
	if r == nil || r.self != r || r.store != ds || r.ctx != ctx {
		return nil, "", ErrParentProfileCreationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.read || r.run.Err() != nil {
		return nil, "", ErrParentProfileCreationChanged
	}
	return r.recovery, r.record.operation, nil
}
func (ds *DataStore) ProfileCreationRecoverySnapshot(ctx context.Context, r *ProfileCreationRead, s *ProfileCreationResultSnapshot) (*ProfileRecoverySnapshot, error) {
	if r == nil || r.self != r || r.store != ds || r.ctx != ctx {
		return nil, ErrParentProfileCreationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.run.Err() != nil || s == nil || s.self != s || s.read != r || r.snapshot != s || s.recovery == nil {
		return nil, ErrParentProfileCreationChanged
	}
	return s.recovery, nil
}
func (ds *DataStore) creationReadCurrent(r *ProfileCreationRead, q parentMutationQuerier) error {
	if r.closed || r.run.Err() != nil {
		return pinContextError(r.run, ErrParentProfileCreationChanged)
	}
	if err := ds.CheckProfilePinRequestFrame(r.request.WithContext(r.run)); err != nil {
		return err
	}
	current, _, _, err := readParentCreationFacts(r.run, q, r.original, r.record.owner, r.hashes)
	if err != nil {
		return err
	}
	if !sameParentCreationFacts(current, r.facts) {
		return ErrParentProfileCreationChanged
	}
	return nil
}
func (ds *DataStore) ReadProfileCreationResult(r *ProfileCreationRead) (*ProfileCreationResultSnapshot, error) {
	if r == nil || r.self != r || r.store != ds {
		return nil, ErrParentProfileCreationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.read || r.closed || r.run.Err() != nil {
		return nil, pinContextError(r.run, ErrParentProfileCreationChanged)
	}
	r.read = true
	rs := r.recovery.state
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.closed || rs.read || rs.slots[0] == nil || rs.slots[1] == nil || rs.slots[0].closed || rs.slots[1].closed {
		return nil, ErrParentProfileCreationChanged
	}
	tx, err := ds.pool.BeginTx(r.run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	defer rollbackPinTx(tx, r.run)
	// Actual080 fence FIRST; read a NEW RC statement after any wait.
	var opID string
	err = tx.QueryRow(r.run, `SELECT operation_id::text FROM profile_creation_operations WHERE operation_id=$1 AND reference_hash=$2 FOR UPDATE`, r.record.operation, r.record.hash).Scan(&opID)
	hasOperation := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	var operation creationOperationRecord
	if hasOperation {
		operation, err = creationOperationRead(r.run, tx, opID)
		if err != nil {
			return nil, err
		}
	}
	if err = ds.stabilizeProfileCreationRead(r, tx); err != nil {
		return nil, err
	}
	current, err := scanCreationResult(tx.QueryRow(r.run, `SELECT `+creationResultColumns+` FROM profile_creation_results WHERE operation_id=$1 FOR UPDATE`, r.record.operation))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrProfileCreationResultMissing
	}
	if err != nil {
		return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	if current.hash != r.record.hash || current.target != r.record.target || current.owner != r.record.owner || current.marker != r.record.marker || current.protocol != 1 {
		return nil, ErrProfileCreationResultMissing
	}
	var now time.Time
	if err = tx.QueryRow(r.run, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, ErrParentProfileCreationUnavailable
	}
	if current.outcome == "prepared" {
		if !hasOperation || operation.target != current.target || operation.referenceHash != current.hash {
			return nil, ErrProfileCreationResultMissing
		}
		if now.Before(operation.writeUntil) {
			if err = tx.Commit(r.run); err != nil {
				return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
			}
			value := &ProfileCreationResultSnapshot{read: r, outcome: "pending"}
			value.self = value
			r.snapshot = value
			return value, nil
		}
		// Under the real fence after original expiry, a prepared081 proves that
		// no qualified Create committed. Close the old execution irreversibly.
		next := "terminal"
		if operation.state == "reserved" {
			next = "retired_unattempted"
		}
		if operation.state == "reserved" || operation.state == "attempting" || operation.state == "unknown" {
			tag, e := tx.Exec(r.run, `UPDATE profile_creation_operations SET state=$3,version=version+1,settled_at=clock_timestamp() WHERE operation_id=$1 AND version=$2`, operation.id, operation.version, next)
			if e != nil || tag.RowsAffected() != 1 {
				return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
			}
		} else if operation.state != "retired_unattempted" {
			return nil, ErrParentProfileCreationChanged
		}
		if current.attempted != (operation.state != "reserved" && operation.state != "retired_unattempted") {
			return nil, ErrParentProfileCreationChanged
		}
		tag, e := tx.Exec(r.run, `UPDATE profile_creation_results SET outcome='not_created',result_version=result_version+1 WHERE operation_id=$1 AND result_version=$2 AND outcome='prepared'`, current.operation, current.version)
		if e != nil || tag.RowsAffected() != 1 {
			return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
		}
		current.version++
		current.outcome = "not_created"
	}
	projection, err := ds.creationReadProjection(r, tx)
	if err != nil {
		return nil, err
	}
	outcome := current.outcome
	adopt := false
	if current.outcome == "created" {
		if current.invalidated {
			if projection.present {
				outcome = "changed_or_replaced"
			} else {
				adopt = true
			}
		} else if projection.present {
			adopt = true
		} else {
			outcome = "changed_or_replaced"
		}
	} else if current.outcome == "not_created" {
		if projection.present {
			outcome = "changed_or_replaced"
		} else {
			adopt = true
		}
	} else {
		return nil, ErrProfileCreationResultMissing
	}
	if projection.present {
		tuple, observed, _, e := readProfilePinAccess(r.run, tx, r.original, current.target, r.hashes, true)
		_ = tuple
		if e != nil {
			return nil, e
		}
		if observed == ProfilePinNotFound || observed == ProfilePinUnavailable {
			return nil, ErrProfileCreationResultMissing
		}
		// Management recovery intentionally permits current PINLocked, matching
		// existing management recovery policy. It still verifies the current PIN.
	}
	if err = r.run.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(r.run); err != nil {
		return nil, pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	value := &ProfileCreationResultSnapshot{read: r, outcome: outcome}
	value.self = value
	if adopt {
		rs.read = true
		rs.snapshot = projection
		value.recovery = projection
	}
	r.record = current
	r.snapshot = value
	return value, nil
}

func (ds *DataStore) creationReadProjection(r *ProfileCreationRead, tx pgx.Tx) (*ProfileRecoverySnapshot, error) {
	var present bool
	var userRaw, settingsRaw []byte
	var userVersion, settingsVersion string
	var generation *int64
	err := tx.QueryRow(r.run, `SELECT u.id IS NOT NULL,to_jsonb(u),
 CASE WHEN u.id IS NULL THEN '' ELSE `+userWriteVersion+` END,s.settings,
 CASE WHEN s.user_id IS NULL THEN '' ELSE `+userSettingsVersion+` END,v.generation
 FROM (SELECT $1::text AS id) selected
 LEFT JOIN users u ON u.id=selected.id
 LEFT JOIN user_settings s ON s.user_id=selected.id
 LEFT JOIN user_settings_versions v ON v.user_id=selected.id`, r.record.target).Scan(&present, &userRaw, &userVersion, &settingsRaw, &settingsVersion, &generation)
	if e := recoveryContextError(r.recovery.state); e != nil {
		return nil, e
	}
	if err != nil {
		return nil, ErrProfileRecoveryUnavailable
	}
	if generation != nil && *generation < 0 || present && (generation == nil || userVersion == "") || !present && (userVersion != "" || settingsVersion != "" || len(settingsRaw) > 0) {
		return nil, ErrProfileRecoveryUnavailable
	}
	stamp := int64(0)
	if generation != nil {
		stamp = *generation
	}
	value := &ProfileRecoverySnapshot{state: r.recovery.state, present: present}
	value.self = value
	if !present {
		value.user = UserWriteRecord{User: models.User{ID: r.record.target}, Revision: UserRevision{owner: ds, id: r.record.target, version: fmt.Sprintf("absent:%d", stamp)}}
	}
	if present {
		value.user, err = ds.recoveryUserRecord(userRaw, userVersion)
		if err != nil || value.user.User.ID != r.record.target {
			return nil, ErrProfileRecoveryUnavailable
		}
	}
	settings := &userSettingsSnapshot{ds: ds, id: r.record.target, version: settingsVersion, generation: stamp, raw: append(json.RawMessage(nil), settingsRaw...), userPresent: present}
	value.settings = UserSettingsRecord{ID: r.record.target, Revision: UserSettingsRevision{snapshot: settings}}
	if settingsVersion != "" {
		var object map[string]json.RawMessage
		if json.Unmarshal(settingsRaw, &object) != nil || object == nil || json.Unmarshal(settingsRaw, &settings.settings) != nil {
			return nil, ErrProfileRecoveryUnavailable
		}
		copy, e := cloneSettings(settings.settings)
		if e != nil {
			return nil, ErrProfileRecoveryUnavailable
		}
		value.settings.Settings = &copy
	}
	if e := recoveryContextError(r.recovery.state); e != nil {
		return nil, e
	}
	return value, nil
}

// Take is the sole paired consumption point. Both cache owners have already
// staged isolated current records under their original Users -> Settings locks.
func (ds *DataStore) TakeProfileCreationRecoveryPair(ctx context.Context, r *ProfileCreationRead, s *ProfileCreationResultSnapshot, u, v *ProfileCreationRecoveryLease) error {
	if r == nil || r.self != r || r.store != ds || r.ctx != ctx {
		return ErrParentProfileCreationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || s == nil || s.self != s || s.read != r || r.snapshot != s || s.recovery == nil {
		return ErrParentProfileCreationChanged
	}
	tx, err := ds.pool.BeginTx(r.run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	// Locks survive through the in-memory PairTake. Rollback below releases
	// read/epoch locks only; it cannot undo the historical local consumption.
	defer rollbackPinTx(tx, r.run)
	var operation string
	err = tx.QueryRow(r.run, `SELECT operation_id::text FROM profile_creation_operations WHERE operation_id=$1 AND reference_hash=$2 FOR UPDATE`, r.record.operation, r.record.hash).Scan(&operation)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	if err = ds.stabilizeProfileCreationRead(r, tx); err != nil {
		return err
	}
	current, err := scanCreationResult(tx.QueryRow(r.run, `SELECT `+creationResultColumns+` FROM profile_creation_results WHERE operation_id=$1 FOR UPDATE`, r.record.operation))
	if err != nil {
		return pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	if current != r.record {
		return ErrParentProfileCreationChanged
	}
	if s.recovery.present {
		_, outcome, _, err := readProfilePinAccess(r.run, tx, r.original, current.target, r.hashes, true)
		if err != nil {
			return err
		}
		if outcome == ProfilePinNotFound || outcome == ProfilePinUnavailable {
			return ErrProfileCreationResultMissing
		}
	}
	latest, err := ds.creationReadProjection(r, tx)
	if err != nil {
		return err
	}
	previous := s.recovery
	oldSettings, newSettings := previous.settings.Revision.snapshot, latest.settings.Revision.snapshot
	if latest.present != previous.present || latest.user.Revision != previous.user.Revision || oldSettings == nil || newSettings == nil || oldSettings.generation != newSettings.generation || oldSettings.version != newSettings.version || oldSettings.userPresent != newSettings.userPresent || !bytes.Equal(oldSettings.raw, newSettings.raw) {
		return ErrParentProfileCreationChanged
	}
	if u == nil || v == nil || u.self != u || v.self != v || u.read != r || v.read != r {
		return ErrParentProfileCreationChanged
	}
	return ds.takeProfileRecoveryPair(ctx, u.proof, v.proof, s.recovery)
}

// The generic proof stays private inside this operation-bound wrapper. Generic
// PairTake never receives the creation cache owners' reserved proof objects.
type ProfileCreationRecoveryLease struct {
	self  *ProfileCreationRecoveryLease
	read  *ProfileCreationRead
	proof *ProfileRecoveryLease
}

func (*ProfileCreationRecoveryLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ProfileCreationRecoveryLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileCreationRecoveryLease(opaque)"))
}

func (ds *DataStore) stabilizeProfileCreationRead(r *ProfileCreationRead, tx pgx.Tx) error {
	var err error
	for _, id := range r.facts.profileKeys {
		if _, err = lockPinEpoch(r.run, tx, "profile", id); err != nil {
			return err
		}
	}
	epoch, err := lockPinEpoch(r.run, tx, "session", pinSessionLineage(r.facts.session.raw, r.original.Token))
	if err != nil {
		return err
	}
	if epoch != r.facts.sessionEpoch {
		return ErrParentProfileCreationChanged
	}
	ids := []string{r.original.AccountID}
	if r.record.owner != r.original.AccountID {
		ids = append(ids, r.record.owner)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(r.run, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return pinContextError(r.run, ErrParentProfileCreationChanged)
		}
	}
	var session string
	if err = tx.QueryRow(r.run, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, r.original.Token).Scan(&session); err != nil {
		return pinContextError(r.run, ErrParentProfileCreationChanged)
	}
	// Stabilizes present AND absent User topology before touching081. This is
	// intentionally the existing Create relation mode, with the same budget.
	if _, err = tx.Exec(r.run, `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return pinContextError(r.run, ErrParentProfileCreationUnavailable)
	}
	if err = ds.creationReadCurrent(r, tx); err != nil {
		return err
	}
	return nil
}

func (ds *DataStore) ReserveProfileCreationUsersRecovery(ctx context.Context, r *ProfileCreationRead) (*ProfileRecoverySlot, error) {
	inner, _, err := ds.ProfileCreationRecoveryRead(ctx, r)
	if err != nil {
		return nil, err
	}
	return ds.reserveRecovery(ctx, inner, 0)
}
func (ds *DataStore) ReserveProfileCreationSettingsRecovery(ctx context.Context, r *ProfileCreationRead) (*ProfileRecoverySlot, error) {
	inner, _, err := ds.ProfileCreationRecoveryRead(ctx, r)
	if err != nil {
		return nil, err
	}
	return ds.reserveRecovery(ctx, inner, 1)
}
func (ds *DataStore) ValidateProfileCreationRecoverySlot(ctx context.Context, r *ProfileCreationRead, slot *ProfileRecoverySlot, snapshot *ProfileRecoverySnapshot) error {
	if r == nil || r.self != r || r.store != ds || r.ctx != ctx {
		return ErrParentProfileCreationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.run.Err() != nil || slot == nil || slot.state != r.recovery.state || slot.state.creation != r || r.snapshot == nil || r.snapshot.recovery != snapshot {
		return ErrParentProfileCreationChanged
	}
	slot.state.mu.Lock()
	defer slot.state.mu.Unlock()
	return checkRecoverySlotLocked(ds, ctx, slot, snapshot)
}

func (ds *DataStore) BeginProfileCreationUsersRecoveryLease(ctx context.Context, r *ProfileCreationRead, slot *ProfileRecoverySlot, snapshot *ProfileRecoverySnapshot) (*ProfileCreationRecoveryLease, UserWriteRecord, bool, error) {
	if err := ds.ValidateProfileCreationRecoverySlot(ctx, r, slot, snapshot); err != nil {
		return nil, UserWriteRecord{}, false, err
	}
	p, record, present, err := ds.beginUsersRecoveryLease(ctx, slot, snapshot)
	if err != nil {
		return nil, record, false, err
	}
	lease := &ProfileCreationRecoveryLease{read: r, proof: p}
	lease.self = lease
	return lease, record, present, nil
}
func (ds *DataStore) BeginProfileCreationSettingsRecoveryLease(ctx context.Context, r *ProfileCreationRead, slot *ProfileRecoverySlot, snapshot *ProfileRecoverySnapshot) (*ProfileCreationRecoveryLease, UserSettingsRecord, bool, error) {
	if err := ds.ValidateProfileCreationRecoverySlot(ctx, r, slot, snapshot); err != nil {
		return nil, UserSettingsRecord{}, false, err
	}
	p, record, present, err := ds.beginUserSettingsRecoveryLease(ctx, slot, snapshot)
	if err != nil {
		return nil, record, false, err
	}
	lease := &ProfileCreationRecoveryLease{read: r, proof: p}
	lease.self = lease
	return lease, record, present, nil
}
func (l *ProfileCreationRecoveryLease) Paired() bool {
	return l != nil && l.self == l && l.proof.Paired()
}
func (l *ProfileCreationRecoveryLease) Retire() bool {
	return l != nil && l.self == l && l.proof.Retire()
}
