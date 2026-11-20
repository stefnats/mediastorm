package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrRecordingTimelineIdentityUnavailable = errors.New("recording timeline identity unavailable")

// RecordingTimelineIdentity is observed bookkeeping DATA, never permission.
// Its original observer receipt stays private and is not an importable grant.
type RecordingTimelineIdentity struct {
	recordingID, owner string
	createdAt          time.Time
	incarnation        uuid.UUID
	run, receipt       json.RawMessage
}

func (RecordingTimelineIdentity) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (RecordingTimelineIdentity) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("RecordingTimelineIdentity(data)"))
}
func (v RecordingTimelineIdentity) RecordingID() string    { return v.recordingID }
func (v RecordingTimelineIdentity) OwnerProfileID() string { return v.owner }
func (v RecordingTimelineIdentity) CreatedAt() time.Time   { return v.createdAt }
func (v RecordingTimelineIdentity) Incarnation() uuid.UUID { return v.incarnation }
func (v RecordingTimelineIdentity) Same(other RecordingTimelineIdentity) bool {
	return v.incarnation != uuid.Nil && v.recordingID == other.recordingID && v.owner == other.owner && v.createdAt.Equal(other.createdAt) && v.incarnation == other.incarnation && bytes.Equal(v.run, other.run) && bytes.Equal(v.receipt, other.receipt)
}

// RecordingRepositoryReady correlates concrete storage; it authenticates no user.
func (ds *DataStore) RecordingRepositoryReady(repo RecordingRepository) bool {
	if ds == nil || ds.pool == nil {
		return false
	}
	r, ok := repo.(*pgRecordingRepo)
	if !ok || r == nil {
		return false
	}
	pool, ok := r.pool.(*pgxpool.Pool)
	return ok && pool == ds.pool
}

// ReadRecordingTimelineIdentity performs no media IO, repairs or writer claims.
// Each statement sees current committed data. There is no lock spanning this
// observation, HTTP authorization and the recording service's filesystem checks.
func (ds *DataStore) ReadRecordingTimelineIdentity(ctx context.Context, id string) (RecordingTimelineIdentity, error) {
	var zero RecordingTimelineIdentity
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || id == "" || len(id) > 128 || strings.TrimSpace(id) != id {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	caller := ctx
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	connection, err := ds.pool.Acquire(ctx)
	if err != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	defer func() {
		physical := connection.Conn().PgConn()
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
	}()
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = tx.Rollback(cleanup)
	}()
	catalog, err := readRecordingPlaybackCatalog(ctx, tx)
	if err != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	query := `SELECT r.id,r.user_id,r.created_at,r.capture_state->'runId',
 s.incarnation::text,s.owner_user_id,s.recording_created_at,s.capture_run_id,s.observer_receipt
 FROM ` + pgx.Identifier{catalog.schema, "recordings"}.Sanitize() + ` r JOIN ` + pgx.Identifier{catalog.schema, "recording_playback_state"}.Sanitize() + ` s ON s.recording_id=r.id
 WHERE r.id=$1 AND octet_length(r.id)<=128 AND octet_length(r.user_id) BETWEEN 1 AND 256
 AND octet_length(s.owner_user_id) BETWEEN 1 AND 256
 AND (r.capture_state->'runId' IS NULL OR octet_length((r.capture_state->'runId')::text)<=4096)
 AND (s.capture_run_id IS NULL OR octet_length(s.capture_run_id::text)<=4096)
 AND octet_length(s.observer_receipt::text)<=16384`
	value := RecordingTimelineIdentity{}
	var stateOwner, incarnation string
	var stateCreated time.Time
	var stateRun json.RawMessage
	if tx.QueryRow(ctx, query, id).Scan(&value.recordingID, &value.owner, &value.createdAt, &value.run, &incarnation, &stateOwner, &stateCreated, &stateRun, &value.receipt) != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	value.incarnation, err = uuid.Parse(incarnation)
	if err != nil || value.incarnation == uuid.Nil || value.recordingID != id || value.owner != stateOwner || !value.createdAt.Equal(stateCreated) || !bytes.Equal(value.run, stateRun) || !bytes.Equal(value.receipt, catalog.receipt) {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	latest, err := readRecordingPlaybackCatalog(ctx, tx)
	if err != nil || latest.schema != catalog.schema || latest.relation != catalog.relation || !bytes.Equal(latest.receipt, catalog.receipt) || ctx.Err() != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	if tx.Commit(ctx) != nil || ctx.Err() != nil || caller.Err() != nil {
		return zero, ErrRecordingTimelineIdentityUnavailable
	}
	return value, nil
}
