package database

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"time"
)

var (
	ErrSABPublicationUnavailable    = errors.New("publication unavailable")
	ErrSABPublicationConflict       = errors.New("publication conflicts with durable operation")
	ErrSABPublicationQuota          = errors.New("publication byte budget exceeded")
	ErrSABPublicationOutcomeUnknown = errors.New("publication acknowledgement unknown")
)

// Saved stage and journal projections are DATA only, never file or actor grants.
type SABPublicationStage struct {
	OperationID                                           string
	QueueID                                               int64
	Namespace, SourceHash, ReceiptAttempt, ManifestDigest string
	Bytes                                                 int64
	Files                                                 []SABStageFile
}
type SABPublicationRecord struct {
	OperationID, StageOperationID, StageNamespace, ManifestDigest string
	QueueID                                                       int64
	Ordinals                                                      []int
	LibraryID, TargetBinding, State                               string
	Bytes, Revision                                               int64
}
type SABLibraryPublicationRepository struct {
	db          *sql.DB
	afterCommit func() error
	// Test-only ports observe actual SQL byte projection and can replace the
	// saved fixture between its real initial read and transaction. No port can
	// supply an actor, file completion, permission or catalogue evidence.
	afterStageRead    func()
	afterManifestScan func(string, int, bool)
}

func NewSABLibraryPublicationRepository(db *sql.DB) *SABLibraryPublicationRepository {
	return &SABLibraryPublicationRepository{db: db}
}
func (r *SABLibraryPublicationRepository) Ready() bool { return r != nil && r.db != nil }
func (r *SABLibraryPublicationRepository) LoadCopied(ctx context.Context, id int64, attempt, source, namespace string) (SABPublicationStage, error) {
	if ctx == nil {
		return SABPublicationStage{}, ErrSABPublicationUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var out SABPublicationStage
	if !r.Ready() || ctx == nil || ctx.Err() != nil || id <= 0 || !stageHex(attempt, 32) || !stageHex(source, 64) || !stageHex(namespace, 64) {
		return out, ErrSABPublicationUnavailable
	}
	var raw *string
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT operation_id,queue_id,namespace,source_hash,receipt_attempt,expected_bytes,file_count,CASE WHEN length(CAST(file_manifest AS BLOB))<=65536 THEN file_manifest END FROM sab_import_stages WHERE queue_id=? AND receipt_attempt=? AND source_hash=? AND namespace=? AND state='copied'`, id, attempt, source, namespace).Scan(&out.OperationID, &out.QueueID, &out.Namespace, &out.SourceHash, &out.ReceiptAttempt, &out.Bytes, &count, &raw)
	if r.afterManifestScan != nil && err == nil {
		size := 0
		if raw != nil {
			size = len(*raw)
		}
		r.afterManifestScan("loaded", size, raw != nil)
	}
	if err != nil || raw == nil || len(*raw) > 65536 || !stageHex(out.OperationID, 32) || !stageHex(out.ReceiptAttempt, 32) || out.Bytes < 0 || out.Bytes > 1<<40 || count < 1 || count > 128 {
		return SABPublicationStage{}, ErrSABPublicationUnavailable
	}
	decoder := json.NewDecoder(bytes.NewBufferString(*raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&out.Files) != nil || decoder.Decode(new(any)) != io.EOF || len(out.Files) != count {
		return SABPublicationStage{}, ErrSABPublicationUnavailable
	}
	var total int64
	for n, f := range out.Files {
		if f.Ordinal != n+1 || f.Bytes < 0 || f.Bytes > (1<<40)-total || !stageHex(f.SHA256, 64) {
			return SABPublicationStage{}, ErrSABPublicationUnavailable
		}
		total += f.Bytes
	}
	canonical, e := json.Marshal(out.Files)
	if e != nil || string(canonical) != *raw || total != out.Bytes || ctx.Err() != nil {
		return SABPublicationStage{}, ErrSABPublicationUnavailable
	}
	digest := sha256.Sum256(canonical)
	out.ManifestDigest = hex.EncodeToString(digest[:])
	return out, nil
}
func publicationSelection(stage SABPublicationStage, ordinals []int) (int64, error) {
	if len(ordinals) < 1 || len(ordinals) > 128 {
		return 0, ErrSABPublicationUnavailable
	}
	var total int64
	prior := 0
	for _, ordinal := range ordinals {
		if ordinal <= prior || ordinal > len(stage.Files) {
			return 0, ErrSABPublicationUnavailable
		}
		file := stage.Files[ordinal-1]
		if file.Bytes <= 0 || file.Bytes > 8<<30 || file.Bytes > (64<<30)-total {
			return 0, ErrSABPublicationUnavailable
		}
		total += file.Bytes
		prior = ordinal
	}
	return total, nil
}

// fullConnection upgrades only this owned connection before every journal
// write. SQLite's normal queue setting is not a durable publication barrier.
func (r *SABLibraryPublicationRepository) fullConnection(ctx context.Context) (*sql.Conn, error) {
	if !r.Ready() || ctx == nil || ctx.Err() != nil {
		return nil, ErrSABPublicationUnavailable
	}
	conn, e := r.db.Conn(ctx)
	if e != nil {
		return nil, ErrSABPublicationUnavailable
	}
	if _, e = conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); e != nil {
		conn.Close()
		return nil, ErrSABPublicationUnavailable
	}
	var synchronous int
	if e = conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); e != nil || synchronous != 2 {
		conn.Close()
		return nil, ErrSABPublicationUnavailable
	}
	return conn, nil
}
func (r *SABLibraryPublicationRepository) commit(ctx context.Context, tx *sql.Tx) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if tx.Commit() != nil {
		return ErrSABPublicationOutcomeUnknown
	}
	if r.afterCommit != nil && r.afterCommit() != nil {
		return ErrSABPublicationOutcomeUnknown
	}
	if ctx.Err() != nil {
		return ErrSABPublicationOutcomeUnknown
	}
	return nil
}
func scanSABPublication(row interface{ Scan(...any) error }) (SABPublicationRecord, error) {
	var out SABPublicationRecord
	var raw string
	e := row.Scan(&out.OperationID, &out.StageOperationID, &out.QueueID, &out.StageNamespace, &out.ManifestDigest, &raw, &out.LibraryID, &out.TargetBinding, &out.Bytes, &out.State, &out.Revision)
	if e != nil {
		return out, e
	}
	if !stageHex(out.OperationID, 32) || !stageHex(out.StageOperationID, 32) || !stageHex(out.StageNamespace, 64) || !stageHex(out.ManifestDigest, 64) || !stageHex(out.TargetBinding, 64) || len(raw) > 1024 || json.Unmarshal([]byte(raw), &out.Ordinals) != nil || out.Revision <= 0 {
		return SABPublicationRecord{}, ErrSABPublicationUnavailable
	}
	return out, nil
}

const sabPublicationProjection = `operation_id,stage_operation_id,queue_id,stage_namespace,manifest_digest,selected_ordinals,target_library_id,target_binding,expected_bytes,state,revision`

func (r *SABLibraryPublicationRepository) Find(ctx context.Context, stage string) (*SABPublicationRecord, error) {
	if ctx == nil {
		return nil, ErrSABPublicationUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !r.Ready() || ctx == nil || ctx.Err() != nil || !stageHex(stage, 32) {
		return nil, ErrSABPublicationUnavailable
	}
	out, e := scanSABPublication(r.db.QueryRowContext(ctx, `SELECT `+sabPublicationProjection+` FROM sab_library_publications WHERE stage_operation_id=?`, stage))
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, ErrSABPublicationUnavailable
	}
	return &out, nil
}

// Claim is unique per immutable staged operation. Existing different choices do
// not create another reservation. No method deletes or releases a byte charge.
func (r *SABLibraryPublicationRepository) Claim(ctx context.Context, stage SABPublicationStage, ordinals []int, library, binding string, quota int64) (SABPublicationRecord, error) {
	if ctx == nil {
		return SABPublicationRecord{}, ErrSABPublicationUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var out SABPublicationRecord
	size, e := publicationSelection(stage, ordinals)
	if e != nil || len(library) < 1 || len(library) > 256 || !stageHex(binding, 64) || quota <= 0 || quota > 1<<40 || size > quota {
		return out, ErrSABPublicationUnavailable
	}
	current, e := r.LoadCopied(ctx, stage.QueueID, stage.ReceiptAttempt, stage.SourceHash, stage.Namespace)
	if e != nil || !reflect.DeepEqual(current, stage) {
		return out, ErrSABPublicationUnavailable
	}
	if r.afterStageRead != nil {
		r.afterStageRead()
	}
	conn, e := r.fullConnection(ctx)
	if e != nil {
		return out, e
	}
	defer conn.Close()
	tx, e := conn.BeginTx(ctx, nil)
	if e != nil {
		return out, ErrSABPublicationUnavailable
	}
	defer tx.Rollback()
	// Establish the actual write lock before either quota or existing-intent read.
	if _, e = tx.ExecContext(ctx, `UPDATE sab_import_stages SET state=state WHERE operation_id=? AND state='copied'`, stage.OperationID); e != nil {
		return out, ErrSABPublicationUnavailable
	}
	var manifest *string
	var attempt, source, namespace string
	var queueID, sizeBefore int64
	var countBefore int
	e = tx.QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(file_manifest AS BLOB))<=65536 THEN file_manifest END,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count FROM sab_import_stages WHERE operation_id=? AND state='copied'`, stage.OperationID).Scan(&manifest, &queueID, &attempt, &source, &namespace, &sizeBefore, &countBefore)
	if r.afterManifestScan != nil && e == nil {
		size := 0
		if manifest != nil {
			size = len(*manifest)
		}
		r.afterManifestScan("transaction", size, manifest != nil)
	}
	if e != nil || manifest == nil || len(*manifest) > 65536 || queueID != stage.QueueID || attempt != stage.ReceiptAttempt || source != stage.SourceHash || namespace != stage.Namespace || sizeBefore != stage.Bytes || countBefore != len(stage.Files) {
		return out, ErrSABPublicationUnavailable
	}
	digest := sha256.Sum256([]byte(*manifest))
	if hex.EncodeToString(digest[:]) != stage.ManifestDigest {
		return out, ErrSABPublicationUnavailable
	}
	prior, e := scanSABPublication(tx.QueryRowContext(ctx, `SELECT `+sabPublicationProjection+` FROM sab_library_publications WHERE stage_operation_id=?`, stage.OperationID))
	if e == nil {
		if prior.QueueID != stage.QueueID || prior.StageNamespace != stage.Namespace || prior.TargetBinding != binding || prior.LibraryID != library || prior.ManifestDigest != stage.ManifestDigest || !reflect.DeepEqual(prior.Ordinals, ordinals) || prior.Bytes != size {
			return out, ErrSABPublicationConflict
		}
		return prior, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return out, ErrSABPublicationUnavailable
	}
	var used int64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(expected_bytes),0) FROM sab_library_publications WHERE target_binding=?`, binding).Scan(&used); e != nil || used < 0 {
		return out, ErrSABPublicationUnavailable
	}
	if used > quota || size > quota-used {
		return out, ErrSABPublicationQuota
	}
	var nonce [16]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return out, ErrSABPublicationUnavailable
	}
	selection, _ := json.Marshal(ordinals)
	at := time.Now().UTC().Format(time.RFC3339Nano)
	out = SABPublicationRecord{OperationID: hex.EncodeToString(nonce[:]), StageOperationID: stage.OperationID, QueueID: stage.QueueID, StageNamespace: stage.Namespace, ManifestDigest: stage.ManifestDigest, Ordinals: append([]int(nil), ordinals...), LibraryID: library, TargetBinding: binding, Bytes: size, State: "claimed", Revision: 1}
	_, e = tx.ExecContext(ctx, `INSERT INTO sab_library_publications(operation_id,stage_operation_id,queue_id,stage_namespace,manifest_digest,selected_ordinals,target_library_id,target_binding,expected_bytes,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'claimed',?,?)`, out.OperationID, out.StageOperationID, out.QueueID, out.StageNamespace, out.ManifestDigest, string(selection), library, binding, size, at, at)
	if e != nil {
		return SABPublicationRecord{}, ErrSABPublicationUnavailable
	}
	if e = r.commit(ctx, tx); e != nil {
		return out, e
	}
	return out, nil
}

// Advance records only saved operational state; it cannot authorize a file or
// catalogue mutation. The actual copier/registrar must still supply evidence.
func (r *SABLibraryPublicationRepository) Advance(ctx context.Context, operation string, revision int64, from, to string) error {
	if ctx == nil {
		return ErrSABPublicationUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	allowed := from == "claimed" && to == "files_durable" || from == "files_durable" && (to == "registration_unknown" || to == "registered") || from == "registration_unknown" && to == "registered" || to == "reconciliation_required" && (from == "claimed" || from == "files_durable" || from == "registration_unknown")
	if !allowed || !stageHex(operation, 32) || revision <= 0 {
		return ErrSABPublicationUnavailable
	}
	conn, e := r.fullConnection(ctx)
	if e != nil {
		return e
	}
	defer conn.Close()
	tx, e := conn.BeginTx(ctx, nil)
	if e != nil {
		return ErrSABPublicationUnavailable
	}
	defer tx.Rollback()
	result, e := tx.ExecContext(ctx, `UPDATE sab_library_publications SET state=?,revision=revision+1,updated_at=? WHERE operation_id=? AND state=? AND revision=?`, to, time.Now().UTC().Format(time.RFC3339Nano), operation, from, revision)
	if e != nil {
		return ErrSABPublicationUnavailable
	}
	n, e := result.RowsAffected()
	if e != nil || n != 1 {
		return ErrSABPublicationConflict
	}
	return r.commit(ctx, tx)
}
