package datastore

// This unconsumed foundation stores private operation correlations only. None
// of its metadata/digests proves authentication, authorization, a User's birth,
// absence, or a successful Create. There is no HTTP/Guard/cache consumer here.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	creationOperationMaximumRows        = 512
	creationOperationMaximumActorActive = 8
	creationOperationMaximumBytes       = 4 << 20
	creationOperationMaximumRecord      = 64 << 10
	creationOperationReadBudget         = 10 * time.Second
	creationOperationWriteWindow        = 30 * time.Second
	creationOperationReadWindow         = 5 * time.Minute
	// Transaction coordination namespace, not a secret or a signing key.
	// The resolved ledger relation OID is its low32 bits. Different search
	// paths resolving the same relation MUST share one admission lock.
	creationOperationBudgetNamespace int64 = 0x43524c31
	creationOperationFixedCharge           = 512
)

var (
	errCreationOperationChanged     = errors.New("creation operation changed or retired")
	errCreationOperationUnavailable = errors.New("creation operation ledger unavailable")
	errCreationOperationUnknown     = errors.New("creation operation commit outcome unknown")
	errCreationOperationCapacity    = errors.New("creation operation capacity exhausted")
	errCreationOperationMissing     = errors.New("creation operation not retained")
	creationOperationIDPattern      = regexp.MustCompile(`^[A-Za-z0-9_.:@-]{1,256}$`)
	creationOperationVersionPattern = regexp.MustCompile(`^[0-9]+:[0-9]+:\([0-9]+,[0-9]+\)$`)
)

type creationOperationEntry uint8

const (
	creationOperationAPI creationOperationEntry = iota + 1
	creationOperationManagement
)

// A future authenticated capture must supply these facts. Their presence here
// is storage validation, NOT verification of the actual session/actor/owner.
// Store only digests of token-bearing request/row data, never passwords, PINs,
// bcrypt hashes, raw session/unlock tokens, URLs or a raw request body.
type creationOperationMetadata struct {
	ActorID          string                 `json:"actorId"`
	OwnerID          string                 `json:"ownerId"`
	Entry            creationOperationEntry `json:"entry"`
	SessionDigest    string                 `json:"sessionDigest"`
	ClientDigest     string                 `json:"clientDigest"`
	RequestDigest    string                 `json:"requestDigest"`
	BodyDigest       string                 `json:"bodyDigest"`
	BodyBytes        int64                  `json:"bodyBytes"`
	SessionRowDigest string                 `json:"sessionRowDigest"`
	ActorRowDigest   string                 `json:"actorRowDigest"`
	OwnerRowDigest   string                 `json:"ownerRowDigest"`
	SessionVersion   string                 `json:"sessionVersion"`
	ActorVersion     string                 `json:"actorVersion"`
	OwnerVersion     string                 `json:"ownerVersion"`
	SessionExpiry    time.Time              `json:"sessionExpiry"`
	ActorExpiry      *time.Time             `json:"actorExpiry"`
	OwnerExpiry      *time.Time             `json:"ownerExpiry"`
}

type creationOperationRecord struct {
	id, target, marker, referenceHash, actor string
	metadata                                 creationOperationMetadata
	charged                                  int
	captured, writeUntil, readUntil          time.Time
	state                                    string
	version                                  int64
	settled                                  *time.Time
}

type creationOperationReference struct {
	mu           sync.Mutex
	self         *creationOperationReference
	store        *DataStore
	ctx          context.Context
	life         context.Context
	stop         context.CancelFunc
	raw          [32]byte
	record       creationOperationRecord
	used, closed bool
}
type creationOperationAttempt struct {
	mu           sync.Mutex
	self         *creationOperationAttempt
	store        *DataStore
	ctx          context.Context
	record       creationOperationRecord
	life         context.Context
	stop         context.CancelFunc
	used, closed bool
}
type creationOperationSnapshot struct {
	mu           sync.Mutex
	self         *creationOperationSnapshot
	store        *DataStore
	ctx          context.Context
	record       creationOperationRecord
	life         context.Context
	stop         context.CancelFunc
	used, closed bool
}
type creationOperationFence struct {
	mu             sync.Mutex
	self           *creationOperationFence
	store          *DataStore
	ctx, run       context.Context
	stop           context.CancelFunc
	tx             pgx.Tx
	record         creationOperationRecord
	finished       bool
	settlementOnly bool
}
type creationOperationSettlement struct {
	self  *creationOperationSettlement
	fence *creationOperationFence
}

func (*creationOperationSettlement) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*creationOperationSettlement) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, "creationOperationSettlement(opaque)")
}

func (*creationOperationReference) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*creationOperationAttempt) MarshalJSON() ([]byte, error)   { return []byte("null"), nil }
func (*creationOperationSnapshot) MarshalJSON() ([]byte, error)  { return []byte("null"), nil }
func (*creationOperationFence) MarshalJSON() ([]byte, error)     { return []byte("null"), nil }
func (*creationOperationReference) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, "creationOperationReference(opaque)")
}
func (*creationOperationAttempt) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, "creationOperationAttempt(opaque)")
}
func (*creationOperationSnapshot) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, "creationOperationSnapshot(opaque)")
}
func (*creationOperationFence) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, "creationOperationFence(opaque)")
}
func (r *creationOperationReference) close() {
	if r == nil || r.self != r {
		return
	}
	r.stop()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}
func (r *creationOperationAttempt) close() {
	if r == nil || r.self != r {
		return
	}
	r.stop()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}
func (r *creationOperationSnapshot) close() {
	if r == nil || r.self != r {
		return
	}
	r.stop()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}
func (f *creationOperationFence) close() {
	if f == nil || f.self != f {
		return
	}
	f.stop() // Interrupt an actual blocked SQL/COMMIT before joining its mutex.
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.finished {
		f.finished = true
		creationOperationRollback(f.tx, f.ctx)
	}
}

func creationOperationDigest(raw []byte) string {
	value := sha256.Sum256(raw)
	return hex.EncodeToString(value[:])
}
func creationOperationHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == value
}
func creationOperationUTC(value time.Time) time.Time { return value.UTC().Truncate(time.Microsecond) }
func creationOperationCloneMetadata(value creationOperationMetadata) (creationOperationMetadata, []byte, error) {
	if !creationOperationIDPattern.MatchString(value.ActorID) || !creationOperationIDPattern.MatchString(value.OwnerID) ||
		value.Entry != creationOperationAPI && value.Entry != creationOperationManagement || value.BodyBytes < 0 || value.BodyBytes > creationOperationMaximumRecord || value.SessionExpiry.IsZero() {
		return creationOperationMetadata{}, nil, errCreationOperationChanged
	}
	for _, digest := range []string{value.SessionDigest, value.ClientDigest, value.RequestDigest, value.BodyDigest, value.SessionRowDigest, value.ActorRowDigest, value.OwnerRowDigest} {
		if !creationOperationHex(digest) {
			return creationOperationMetadata{}, nil, errCreationOperationChanged
		}
	}
	for _, version := range []string{value.SessionVersion, value.ActorVersion, value.OwnerVersion} {
		if len(version) > 8192 || !creationOperationVersionPattern.MatchString(version) {
			return creationOperationMetadata{}, nil, errCreationOperationChanged
		}
	}
	value.SessionExpiry = creationOperationUTC(value.SessionExpiry)
	if value.ActorExpiry != nil {
		v := creationOperationUTC(*value.ActorExpiry)
		value.ActorExpiry = &v
	}
	if value.OwnerExpiry != nil {
		v := creationOperationUTC(*value.OwnerExpiry)
		value.OwnerExpiry = &v
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw)+len(value.ActorID)+creationOperationFixedCharge > creationOperationMaximumRecord {
		return creationOperationMetadata{}, nil, errCreationOperationChanged
	}
	return value, raw, nil
}
func creationOperationCharge(raw []byte, actor string) int {
	return len(raw) + len(actor) + creationOperationFixedCharge
}
func creationOperationContext(ds *DataStore, ctx context.Context) error {
	if ds == nil || ds.pool == nil || ctx == nil {
		return errCreationOperationUnavailable
	}
	return ctx.Err()
}
func creationOperationError(ctx context.Context, fallback error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fallback
}
func creationOperationRollback(tx pgx.Tx, parent context.Context) {
	if tx == nil {
		return
	}
	// Cleanup only. This bounded context cannot admit another mutation.
	cleanup, stop := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer stop()
	_ = tx.Rollback(cleanup)
}
func creationOperationBudgetLock(ctx context.Context, tx pgx.Tx) error {
	// Stabilize the actual resolved relation against Down/drop/recreate BEFORE
	// waiting for its budget. Prune uses this exact table->budget order too.
	if _, err := tx.Exec(ctx, `LOCK TABLE profile_creation_operations IN ACCESS SHARE MODE`); err != nil {
		return err
	}
	var oid int64
	var kind, persistence string
	if err := tx.QueryRow(ctx, `SELECT c.oid::bigint,c.relkind::text,c.relpersistence::text
        FROM pg_class c WHERE c.oid=to_regclass('profile_creation_operations')`).Scan(&oid, &kind, &persistence); err != nil {
		return err
	}
	if kind != "r" || persistence != "p" || oid <= 0 {
		return errCreationOperationUnavailable
	}
	var ignored string
	return tx.QueryRow(ctx, `SELECT pg_advisory_xact_lock(($1::bigint << 32) | $2::bigint)::text`, creationOperationBudgetNamespace, oid).Scan(&ignored)
}

const creationOperationColumns = `operation_id::text,target_id::text,marker_id::text,reference_hash,actor_id,metadata,charged_bytes,captured_at,write_until,read_until,state,version,settled_at`

func creationOperationRead(ctx context.Context, q parentMutationQuerier, id string) (creationOperationRecord, error) {
	var v creationOperationRecord
	var raw []byte
	err := q.QueryRow(ctx, `SELECT `+creationOperationColumns+` FROM profile_creation_operations WHERE operation_id=$1`, id).Scan(&v.id, &v.target, &v.marker, &v.referenceHash, &v.actor, &raw, &v.charged, &v.captured, &v.writeUntil, &v.readUntil, &v.state, &v.version, &v.settled)
	if err != nil {
		return v, creationOperationError(ctx, errCreationOperationUnavailable)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != reflect.TypeOf(v.metadata).NumField() {
		return v, errCreationOperationUnavailable
	}
	shape := reflect.TypeOf(v.metadata)
	for n := 0; n < shape.NumField(); n++ {
		key := shape.Field(n).Tag.Get("json")
		field, ok := fields[key]
		if !ok || bytes.Equal(field, []byte("null")) && key != "actorExpiry" && key != "ownerExpiry" {
			return v, errCreationOperationUnavailable
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&v.metadata) != nil {
		return v, errCreationOperationUnavailable
	}
	v.captured = creationOperationUTC(v.captured)
	v.writeUntil = creationOperationUTC(v.writeUntil)
	v.readUntil = creationOperationUTC(v.readUntil)
	if v.settled != nil {
		copy := creationOperationUTC(*v.settled)
		v.settled = &copy
	}
	normalized, canonical, e := creationOperationCloneMetadata(v.metadata)
	if e != nil || !reflect.DeepEqual(normalized, v.metadata) || v.actor != v.metadata.ActorID || v.charged != creationOperationCharge(canonical, v.actor) ||
		!creationOperationHex(v.referenceHash) || v.version <= 0 || v.captured.IsZero() || !v.writeUntil.After(v.captured) || v.writeUntil.After(v.captured.Add(creationOperationWriteWindow)) ||
		v.readUntil.Before(v.writeUntil) || v.readUntil.After(v.captured.Add(creationOperationReadWindow)) || v.readUntil.After(v.metadata.SessionExpiry) ||
		v.metadata.ActorExpiry != nil && v.readUntil.After(*v.metadata.ActorExpiry) || v.metadata.OwnerExpiry != nil && v.readUntil.After(*v.metadata.OwnerExpiry) {
		return v, errCreationOperationUnavailable
	}
	for _, id := range []string{v.id, v.target, v.marker} {
		if parsed, e := uuid.Parse(id); e != nil || parsed.String() != id {
			return v, errCreationOperationUnavailable
		}
	}
	terminal := v.state == "terminal" || v.state == "retired_unattempted"
	if !terminal && v.state != "reserved" && v.state != "attempting" && v.state != "unknown" || terminal != (v.settled != nil) {
		return v, errCreationOperationUnavailable
	}
	return v, nil
}
func sameCreationOperation(a, b creationOperationRecord) bool { return reflect.DeepEqual(a, b) }
func creationOperationLockRow(ctx context.Context, tx pgx.Tx, id string) error {
	var found string
	if err := tx.QueryRow(ctx, `SELECT operation_id::text FROM profile_creation_operations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errCreationOperationChanged
		}
		return creationOperationError(ctx, errCreationOperationUnavailable)
	}
	return nil
}

func (ds *DataStore) reserveProfileCreationOperation(ctx context.Context, metadata creationOperationMetadata) (*creationOperationReference, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return nil, e
	}
	value, raw, e := creationOperationCloneMetadata(metadata)
	if e != nil {
		return nil, e
	}
	var token [32]byte
	if _, e = rand.Read(token[:]); e != nil {
		return nil, errCreationOperationUnavailable
	}
	operation, e := uuid.NewRandom()
	if e != nil {
		return nil, errCreationOperationUnavailable
	}
	target, e := uuid.NewRandom()
	if e != nil {
		return nil, errCreationOperationUnavailable
	}
	marker, e := uuid.NewRandom()
	if e != nil {
		return nil, errCreationOperationUnavailable
	}
	record := creationOperationRecord{id: operation.String(), target: target.String(), marker: marker.String(), referenceHash: creationOperationDigest(token[:]), actor: value.ActorID, metadata: value, charged: creationOperationCharge(raw, value.ActorID), state: "reserved", version: 1}
	run, stop := context.WithTimeout(ctx, creationOperationReadBudget)
	defer stop()
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	defer creationOperationRollback(tx, ctx)
	if e = tx.QueryRow(run, `SELECT clock_timestamp()`).Scan(&record.captured); e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	record.captured = creationOperationUTC(record.captured)
	record.writeUntil = record.captured.Add(creationOperationWriteWindow)
	record.readUntil = record.captured.Add(creationOperationReadWindow)
	ends := []time.Time{value.SessionExpiry}
	if value.ActorExpiry != nil {
		ends = append(ends, *value.ActorExpiry)
	}
	if value.OwnerExpiry != nil {
		ends = append(ends, *value.OwnerExpiry)
	}
	if deadline, ok := ctx.Deadline(); ok {
		ends = append(ends, creationOperationUTC(deadline))
	}
	for _, end := range ends {
		if end.Before(record.writeUntil) {
			record.writeUntil = end
		}
		if end.Before(record.readUntil) {
			record.readUntil = end
		}
	}
	if !record.writeUntil.After(record.captured) {
		return nil, errCreationOperationChanged
	}
	if e = creationOperationBudgetLock(run, tx); e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	// This NEW statement is intentionally after the serialized wait. An RR or
	// pre-wait statement snapshot would undercount committed reservations.
	var count, active int
	var size int64
	var now time.Time
	e = tx.QueryRow(run, `SELECT count(*),COALESCE(sum(charged_bytes),0),
        count(*) FILTER (WHERE actor_id=$1 AND state IN ('reserved','attempting','unknown')),clock_timestamp()
        FROM profile_creation_operations`, value.ActorID).Scan(&count, &size, &active, &now)
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	if !now.Before(record.writeUntil) {
		return nil, errCreationOperationChanged
	}
	if count >= creationOperationMaximumRows || active >= creationOperationMaximumActorActive || size > int64(creationOperationMaximumBytes-record.charged) {
		return nil, errCreationOperationCapacity
	}
	_, e = tx.Exec(run, `INSERT INTO profile_creation_operations(operation_id,target_id,marker_id,reference_hash,actor_id,metadata,charged_bytes,captured_at,write_until,read_until)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, record.id, record.target, record.marker, record.referenceHash, record.actor, raw, record.charged, record.captured, record.writeUntil, record.readUntil)
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	if e = run.Err(); e != nil {
		return nil, e
	}
	if e = tx.Commit(run); e != nil {
		return nil, errCreationOperationUnknown
	}
	life, cancel := context.WithCancel(ctx)
	reference := &creationOperationReference{store: ds, ctx: ctx, life: life, stop: cancel, raw: token, record: record}
	reference.self = reference
	return reference, nil
}

func (ds *DataStore) claimProfileCreationAttempt(ctx context.Context, r *creationOperationReference) (*creationOperationAttempt, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return nil, e
	}
	if r == nil {
		return nil, errCreationOperationChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.self != r || r.store != ds || r.ctx != ctx || r.closed || r.used {
		return nil, errCreationOperationChanged
	}
	if e := r.life.Err(); e != nil {
		return nil, e
	}
	r.used = true
	run, stop := context.WithDeadline(r.life, r.record.writeUntil)
	defer stop()
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	defer creationOperationRollback(tx, ctx)
	if e = creationOperationLockRow(run, tx, r.record.id); e != nil {
		return nil, e
	}
	current, e := creationOperationRead(run, tx, r.record.id)
	if e != nil {
		return nil, e
	}
	if !sameCreationOperation(current, r.record) || current.state != "reserved" {
		return nil, errCreationOperationChanged
	}
	var now time.Time
	if e = tx.QueryRow(run, `SELECT clock_timestamp()`).Scan(&now); e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	if !now.Before(current.writeUntil) {
		return nil, errCreationOperationChanged
	}
	tag, e := tx.Exec(run, `UPDATE profile_creation_operations SET state='attempting',version=version+1 WHERE operation_id=$1 AND version=$2 AND state='reserved'`, current.id, current.version)
	if e != nil || tag.RowsAffected() != 1 {
		return nil, creationOperationError(run, errCreationOperationChanged)
	}
	current, e = creationOperationRead(run, tx, current.id)
	if e != nil {
		return nil, e
	}
	if e = run.Err(); e != nil {
		return nil, e
	}
	if e = tx.Commit(run); e != nil {
		return nil, errCreationOperationUnknown
	}
	life, cancel := context.WithCancel(ctx)
	a := &creationOperationAttempt{store: ds, ctx: ctx, life: life, stop: cancel, record: current}
	a.self = a
	return a, nil
}
func (ds *DataStore) beginProfileCreationOperation(ctx context.Context, a *creationOperationAttempt) (*creationOperationFence, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return nil, e
	}
	if a == nil {
		return nil, errCreationOperationChanged
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.self != a || a.store != ds || a.ctx != ctx || a.used || a.closed {
		return nil, errCreationOperationChanged
	}
	a.used = true
	run, stop := context.WithDeadline(a.life, a.record.writeUntil)
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		failure := creationOperationError(run, errCreationOperationUnavailable)
		stop()
		return nil, failure
	}
	fail := func(e error) (*creationOperationFence, error) {
		creationOperationRollback(tx, ctx)
		stop()
		return nil, e
	}
	if e = creationOperationLockRow(run, tx, a.record.id); e != nil {
		return fail(e)
	}
	// A new RC statement observes the version AFTER any row-fence wait.
	current, e := creationOperationRead(run, tx, a.record.id)
	if e != nil {
		return fail(e)
	}
	if !sameCreationOperation(current, a.record) || current.state != "attempting" {
		return fail(errCreationOperationChanged)
	}
	var now time.Time
	if e = tx.QueryRow(run, `SELECT clock_timestamp()`).Scan(&now); e != nil {
		return fail(creationOperationError(run, errCreationOperationUnavailable))
	}
	if !now.Before(current.writeUntil) {
		return fail(errCreationOperationChanged)
	}
	f := &creationOperationFence{store: ds, ctx: ctx, run: run, stop: stop, tx: tx, record: current}
	f.self = f
	return f, nil
}

// finish and recordUnknown are distinct private ledger bookkeeping operations,
// not free bools asserting User creation/absence or an authentication result.
// A future actual Create needs its own sealed transaction/result binding.
func (f *creationOperationFence) finish(ctx context.Context) error {
	if f == nil || f.settlementOnly {
		return errCreationOperationChanged
	}
	return f.transition(ctx, "terminal")
}
func (f *creationOperationFence) recordUnknown(ctx context.Context) error {
	if f == nil || f.settlementOnly {
		return errCreationOperationChanged
	}
	return f.transition(ctx, "unknown")
}
func (s *creationOperationSettlement) close() {
	if s != nil && s.self == s {
		s.fence.close()
	}
}
func (s *creationOperationSettlement) finish(ctx context.Context) error {
	if s == nil || s.self != s || s.fence == nil || !s.fence.settlementOnly {
		return errCreationOperationChanged
	}
	return s.fence.transition(ctx, "terminal")
}
func (f *creationOperationFence) transition(ctx context.Context, next string) error {
	if f == nil || ctx == nil {
		return errCreationOperationChanged
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.self != f || f.ctx != ctx || f.finished || f.tx == nil {
		return errCreationOperationChanged
	}
	f.finished = true
	defer f.stop()
	defer creationOperationRollback(f.tx, ctx)
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := f.run.Err(); e != nil {
		return e
	}
	tag, e := f.tx.Exec(f.run, `UPDATE profile_creation_operations SET state=$3,version=version+1,
        settled_at=CASE WHEN $3='terminal' THEN clock_timestamp() ELSE NULL END
        WHERE operation_id=$1 AND version=$2 AND state=$4`, f.record.id, f.record.version, next, f.record.state)
	if e != nil || tag.RowsAffected() != 1 {
		return creationOperationError(f.run, errCreationOperationChanged)
	}
	if e = f.run.Err(); e != nil {
		return e
	}
	if e = f.tx.Commit(f.run); e != nil {
		return errCreationOperationUnknown
	}
	return nil
}

// raw is a private locator, never an auth token. A fresh storage lookup can
// observe retained historical metadata after old read_until; it cannot claim
// or reconstruct execution authority. Future public reads need current auth.
func (ds *DataStore) readFencedProfileCreationOperation(ctx context.Context, raw []byte) (*creationOperationSnapshot, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return nil, e
	}
	if len(raw) != 32 {
		return nil, errCreationOperationChanged
	}
	digest := creationOperationDigest(append([]byte(nil), raw...))
	run, stop := context.WithTimeout(ctx, creationOperationReadBudget)
	defer stop()
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	defer creationOperationRollback(tx, ctx)
	var id string
	e = tx.QueryRow(run, `SELECT operation_id::text FROM profile_creation_operations WHERE reference_hash=$1 FOR UPDATE`, digest).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, errCreationOperationMissing
	}
	if e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	value, e := creationOperationRead(run, tx, id)
	if e != nil {
		return nil, e
	}
	if value.referenceHash != digest {
		return nil, errCreationOperationChanged
	}
	if e = run.Err(); e != nil {
		return nil, e
	}
	if e = tx.Commit(run); e != nil {
		return nil, creationOperationError(run, errCreationOperationUnavailable)
	}
	life, cancel := context.WithCancel(ctx)
	snapshot := &creationOperationSnapshot{store: ds, ctx: ctx, life: life, stop: cancel, record: value}
	snapshot.self = snapshot
	return snapshot, nil
}

// This separately typed private lookup/settlement capability may outlive the
// original write window. It cannot reconstruct or renew an execution ticket.
// It records ledger-only closure, not a User-present/absent or auth proof.
func (ds *DataStore) beginProfileCreationSettlement(ctx context.Context, s *creationOperationSnapshot) (*creationOperationSettlement, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return nil, e
	}
	if s == nil {
		return nil, errCreationOperationChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != s || s.store != ds || s.ctx != ctx || s.used || s.closed {
		return nil, errCreationOperationChanged
	}
	s.used = true
	run, stop := context.WithTimeout(s.life, creationOperationReadBudget)
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		failure := creationOperationError(run, errCreationOperationUnavailable)
		stop()
		return nil, failure
	}
	fail := func(e error) (*creationOperationSettlement, error) {
		creationOperationRollback(tx, ctx)
		stop()
		return nil, e
	}
	if e = creationOperationLockRow(run, tx, s.record.id); e != nil {
		return fail(e)
	}
	current, e := creationOperationRead(run, tx, s.record.id)
	if e != nil {
		return fail(e)
	}
	if !sameCreationOperation(current, s.record) || current.state != "attempting" && current.state != "unknown" {
		return fail(errCreationOperationChanged)
	}
	fence := &creationOperationFence{store: ds, ctx: ctx, run: run, stop: stop, tx: tx, record: current, settlementOnly: true}
	fence.self = fence
	settlement := &creationOperationSettlement{fence: fence}
	settlement.self = settlement
	return settlement, nil
}

func (ds *DataStore) retireUnusedProfileCreationOperation(ctx context.Context, s *creationOperationSnapshot) error {
	if e := creationOperationContext(ds, ctx); e != nil {
		return e
	}
	if s == nil {
		return errCreationOperationChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != s || s.store != ds || s.ctx != ctx || s.closed || s.used {
		return errCreationOperationChanged
	}
	s.used = true
	run, stop := context.WithTimeout(s.life, creationOperationReadBudget)
	defer stop()
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return creationOperationError(run, errCreationOperationUnavailable)
	}
	defer creationOperationRollback(tx, ctx)
	if e = creationOperationLockRow(run, tx, s.record.id); e != nil {
		return e
	}
	current, e := creationOperationRead(run, tx, s.record.id)
	if e != nil {
		return e
	}
	if !sameCreationOperation(current, s.record) || current.state != "reserved" {
		return errCreationOperationChanged
	}
	tag, e := tx.Exec(run, `UPDATE profile_creation_operations SET state='retired_unattempted',version=version+1,settled_at=clock_timestamp()
        WHERE operation_id=$1 AND version=$2 AND state='reserved' AND write_until<=clock_timestamp()`, current.id, current.version)
	if e != nil || tag.RowsAffected() != 1 {
		return creationOperationError(run, errCreationOperationChanged)
	}
	if e = run.Err(); e != nil {
		return e
	}
	if e = tx.Commit(run); e != nil {
		return errCreationOperationUnknown
	}
	return nil
}
func (ds *DataStore) pruneProfileCreationOperations(ctx context.Context, limit int) (int64, error) {
	if e := creationOperationContext(ds, ctx); e != nil {
		return 0, e
	}
	if limit < 1 || limit > 64 {
		return 0, errCreationOperationChanged
	}
	run, stop := context.WithTimeout(ctx, creationOperationReadBudget)
	defer stop()
	tx, e := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return 0, creationOperationError(run, errCreationOperationUnavailable)
	}
	defer creationOperationRollback(tx, ctx)
	if e = creationOperationBudgetLock(run, tx); e != nil {
		return 0, creationOperationError(run, errCreationOperationUnavailable)
	}
	// Only this budget->operation path exists, and it NEVER waits for a held
	// operation row. All unresolved rows remain charged regardless of expiry.
	tag, e := tx.Exec(run, `WITH eligible AS (
        SELECT operation_id FROM profile_creation_operations
        WHERE state IN ('terminal','retired_unattempted') AND settled_at IS NOT NULL
          AND write_until<=clock_timestamp() AND read_until<=clock_timestamp()
        ORDER BY read_until,operation_id LIMIT $1 FOR UPDATE SKIP LOCKED)
        DELETE FROM profile_creation_operations p USING eligible e WHERE p.operation_id=e.operation_id`, limit)
	if e != nil {
		return 0, creationOperationError(run, errCreationOperationUnavailable)
	}
	if e = run.Err(); e != nil {
		return 0, e
	}
	if e = tx.Commit(run); e != nil {
		return 0, errCreationOperationUnknown
	}
	return tag.RowsAffected(), nil
}
