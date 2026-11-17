package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrSABStageUnavailable    = errors.New("staging unavailable")
	ErrSABStageExists         = errors.New("staging attempt already exists")
	ErrSABStageQuota          = errors.New("staging byte budget exceeded")
	ErrSABStageOutcomeUnknown = errors.New("staging commit outcome unknown")
)

type SABStageClaim struct {
	OperationID                                     string
	QueueID                                         int64
	ReceiptAttempt, SourceHash, Namespace, PathHash string
	ExpectedBytes                                   int64
	FileCount                                       int
	ExpectedMetadata                                string
}
type SABStageRecord struct {
	OperationID   string
	QueueID       int64
	ExpectedBytes int64
	FileCount     int
	State         string
	UpdatedAt     string
}
type SABStageFile struct {
	Ordinal int    `json:"ordinal"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
}

// SABStageRepository retains every charge without automatic expiry or retry.
type SABStageRepository struct {
	db *sql.DB
	// Tests may inject an error only after the real SQL COMMIT returned success.
	// This models an unacknowledged result, not a PostgreSQL/network protocol fault.
	afterCommit func() error
}

func NewSABStageRepository(db *sql.DB) *SABStageRepository { return &SABStageRepository{db: db} }
func stageHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}
func (r *SABStageRepository) commit(ctx context.Context, tx *sql.Tx) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := tx.Commit(); e != nil {
		return ErrSABStageOutcomeUnknown
	}
	if r.afterCommit != nil {
		if e := r.afterCommit(); e != nil {
			return ErrSABStageOutcomeUnknown
		}
	}
	if ctx.Err() != nil {
		return ErrSABStageOutcomeUnknown
	}
	return nil
}
func (r *SABStageRepository) Find(ctx context.Context, queueID int64, attempt, source, namespace string) (*SABStageRecord, error) {
	if r == nil || r.db == nil || ctx.Err() != nil || queueID <= 0 || !stageHex(attempt, 32) || !stageHex(source, 64) || !stageHex(namespace, 64) {
		return nil, ErrSABStageUnavailable
	}
	var value SABStageRecord
	e := r.db.QueryRowContext(ctx, `SELECT operation_id,queue_id,expected_bytes,file_count,state,updated_at FROM sab_import_stages WHERE queue_id=? AND receipt_attempt=? AND source_hash=? AND namespace=?`, queueID, attempt, source, namespace).Scan(&value.OperationID, &value.QueueID, &value.ExpectedBytes, &value.FileCount, &value.State, &value.UpdatedAt)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, ErrSABStageUnavailable
	}
	return &value, nil
}
func (r *SABStageRepository) Claim(ctx context.Context, c SABStageClaim, quota int64) (SABStageRecord, error) {
	value := SABStageRecord{OperationID: c.OperationID, QueueID: c.QueueID, ExpectedBytes: c.ExpectedBytes, FileCount: c.FileCount, State: "pending"}
	if r == nil || r.db == nil || ctx.Err() != nil || quota <= 0 || c.ExpectedBytes < 0 || c.ExpectedBytes > quota || c.FileCount < 1 || c.FileCount > 128 || !stageHex(c.OperationID, 32) || !stageHex(c.ReceiptAttempt, 32) || !stageHex(c.SourceHash, 64) || !stageHex(c.Namespace, 64) || !stageHex(c.PathHash, 64) || c.QueueID <= 0 || c.ExpectedMetadata == "" {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	tx, e := r.db.BeginTx(ctx, nil)
	if e != nil {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	defer tx.Rollback()
	// A write is first, before reads: acquire SQLite's write lock before quota and
	// receipt snapshots, so two connections cannot both admit against the old sum.
	_, e = tx.ExecContext(ctx, `INSERT INTO sab_import_stage_roots(path_hash,namespace) VALUES (?,?) ON CONFLICT(path_hash) DO NOTHING`, c.PathHash, c.Namespace)
	if e != nil {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	var bound string
	if e = tx.QueryRowContext(ctx, `SELECT namespace FROM sab_import_stage_roots WHERE path_hash=?`, c.PathHash).Scan(&bound); e != nil || bound != c.Namespace {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	var metadata string
	if e = tx.QueryRowContext(ctx, `SELECT metadata FROM import_queue WHERE id=?`, c.QueueID).Scan(&metadata); e != nil || metadata != c.ExpectedMetadata {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	var envelope map[string]json.RawMessage
	var receipt struct {
		Version int    `json:"version"`
		Attempt string `json:"attempt"`
		Source  string `json:"source"`
		JobID   string `json:"jobId"`
		Status  struct {
			QueueID int64  `json:"queueId"`
			State   string `json:"state"`
		} `json:"status"`
	}
	if json.Unmarshal([]byte(metadata), &envelope) != nil || json.Unmarshal(envelope[SABFallbackMetadataKey], &receipt) != nil || receipt.Version != 1 || receipt.Attempt != c.ReceiptAttempt || receipt.Source != c.SourceHash || receipt.JobID == "" || receipt.Status.QueueID != c.QueueID || receipt.Status.State != "download_completed" {
		return SABStageRecord{}, ErrSABStageUnavailable
	}

	var exists int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM sab_import_stages WHERE queue_id=? AND receipt_attempt=? AND source_hash=? AND namespace=?`, c.QueueID, c.ReceiptAttempt, c.SourceHash, c.Namespace).Scan(&exists); e != nil {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	if exists != 0 {
		return SABStageRecord{}, ErrSABStageExists
	}
	var used int64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(expected_bytes),0) FROM sab_import_stages WHERE namespace=?`, c.Namespace).Scan(&used); e != nil || used < 0 {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	if used > quota || c.ExpectedBytes > quota-used {
		return SABStageRecord{}, ErrSABStageQuota
	}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	_, e = tx.ExecContext(ctx, `INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'pending',?,?)`, c.OperationID, c.QueueID, c.ReceiptAttempt, c.SourceHash, c.Namespace, c.ExpectedBytes, c.FileCount, at, at)
	if e != nil {
		return SABStageRecord{}, ErrSABStageUnavailable
	}
	if e = r.commit(ctx, tx); e != nil {
		return value, e
	}
	return value, nil
}
func (r *SABStageRepository) Finish(ctx context.Context, operationID, state string, files []SABStageFile) error {
	if r == nil || r.db == nil || ctx.Err() != nil || !stageHex(operationID, 32) || state != "copied" && state != "failed_partial" {
		return ErrSABStageUnavailable
	}
	var total int64
	for i, f := range files {
		if f.Ordinal != i+1 || f.Bytes < 0 || f.Bytes > (1<<40)-total || !stageHex(f.SHA256, 64) {
			return ErrSABStageUnavailable
		}
		total += f.Bytes
	}
	if state == "failed_partial" && len(files) != 0 || len(files) > 128 {
		return ErrSABStageUnavailable
	}
	raw, e := json.Marshal(files)
	if e != nil {
		return ErrSABStageUnavailable
	}
	tx, e := r.db.BeginTx(ctx, nil)
	if e != nil {
		return ErrSABStageUnavailable
	}
	defer tx.Rollback()
	if state == "copied" {
		var size int64
		var count int
		if e := tx.QueryRowContext(ctx, `SELECT expected_bytes,file_count FROM sab_import_stages WHERE operation_id=? AND state='pending'`, operationID).Scan(&size, &count); e != nil || size != total || count != len(files) {
			return ErrSABStageUnavailable
		}
	}
	result, e := tx.ExecContext(ctx, `UPDATE sab_import_stages SET state=?,file_manifest=?,updated_at=? WHERE operation_id=? AND state='pending'`, state, string(raw), time.Now().UTC().Format(time.RFC3339Nano), operationID)
	if e != nil {
		return ErrSABStageUnavailable
	}
	n, e := result.RowsAffected()
	if e != nil || n != 1 {
		return ErrSABStageUnavailable
	}
	return r.commit(ctx, tx)
}

// FindAtConfiguredPath reads only durable path binding and operation projection.
// An unobserved path is not proof of a new physical namespace or zero charges.
func (r *SABStageRepository) FindAtConfiguredPath(ctx context.Context, pathHash string, queueID int64, attempt, source string) (*SABStageRecord, bool, error) {
	if r == nil || r.db == nil || ctx.Err() != nil || !stageHex(pathHash, 64) {
		return nil, false, ErrSABStageUnavailable
	}
	var namespace string
	e := r.db.QueryRowContext(ctx, `SELECT namespace FROM sab_import_stage_roots WHERE path_hash=?`, pathHash).Scan(&namespace)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, false, nil
	}
	if e != nil || !stageHex(namespace, 64) {
		return nil, false, ErrSABStageUnavailable
	}
	value, e := r.Find(ctx, queueID, attempt, source, namespace)
	if e != nil {
		return nil, false, e
	}
	return value, true, nil
}

// BindObservedPath is used only after an explicit operation observed a held
// physical root. It never replaces an old binding or changes any charge.
func (r *SABStageRepository) BindObservedPath(ctx context.Context, pathHash, namespace string) error {
	if r == nil || r.db == nil || ctx.Err() != nil || !stageHex(pathHash, 64) || !stageHex(namespace, 64) {
		return ErrSABStageUnavailable
	}
	tx, e := r.db.BeginTx(ctx, nil)
	if e != nil {
		return ErrSABStageUnavailable
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, `INSERT INTO sab_import_stage_roots(path_hash,namespace) VALUES (?,?) ON CONFLICT(path_hash) DO NOTHING`, pathHash, namespace); e != nil {
		return ErrSABStageUnavailable
	}
	var current string
	if e = tx.QueryRowContext(ctx, `SELECT namespace FROM sab_import_stage_roots WHERE path_hash=?`, pathHash).Scan(&current); e != nil || current != namespace {
		return ErrSABStageUnavailable
	}
	return r.commit(ctx, tx)
}
