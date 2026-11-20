package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"novastream/internal/settingscommit"
)

// Match the existing backup JSON-import body budget (handlers/backup.go).
// This bounds only the new 085 subset, not the whole existing ZIP inflater.
const RecordingPlaybackPositionBackupByteLimit = 50 << 20

// Even empty scalar values require these keys and delimiters. This is a
// byte-budget-derived allocation bound, not an independent product row cap.
const positionBackupMinimumRowBytes = len(`{"recordingId":"","incarnation":"","artifactId":"","mappingSha256":"","durationMs":0,"positionMs":0,"atDerivedEnd":true,"descriptorObservedAt":"","descriptorExpiresAt":"","revision":0,"writerGeneration":0,"operationId":"","updatedAt":""}`)

var ErrRecordingPositionBackup = errors.New("invalid or unavailable recording position backup data")

// All fields are historical DATA. In particular the counters, incarnation and
// operation are never imported into 084 and cannot create a playback writer.
type RecordingPlaybackPositionBackup struct {
	RecordingID          string    `json:"recordingId"`
	Incarnation          string    `json:"incarnation"`
	ArtifactID           string    `json:"artifactId"`
	MappingSHA256        string    `json:"mappingSha256"`
	DurationMS           int64     `json:"durationMs"`
	PositionMS           int64     `json:"positionMs"`
	AtDerivedEnd         bool      `json:"atDerivedEnd"`
	DescriptorObservedAt time.Time `json:"descriptorObservedAt"`
	DescriptorExpiresAt  time.Time `json:"descriptorExpiresAt"`
	Revision             int64     `json:"revision"`
	WriterGeneration     int64     `json:"writerGeneration"`
	OperationID          string    `json:"operationId"`
	UpdatedAt            time.Time `json:"updatedAt"`
}

type RecordingPlaybackPositionBackupRows []RecordingPlaybackPositionBackup

var recordingPositionFields = map[string]bool{
	"recordingId": true, "incarnation": true, "artifactId": true, "mappingSha256": true,
	"durationMs": true, "positionMs": true, "atDerivedEnd": true,
	"descriptorObservedAt": true, "descriptorExpiresAt": true, "revision": true,
	"writerGeneration": true, "operationId": true, "updatedAt": true,
}

func positionBackupFieldBytes(name string) int {
	switch name {
	case "recordingId":
		return 6*128 + 2
	case "incarnation", "operationId":
		return 6*36 + 2
	case "artifactId":
		return 6*32 + 2
	case "mappingSha256", "descriptorObservedAt", "descriptorExpiresAt", "updatedAt":
		return 6*64 + 2
	case "atDerivedEnd":
		return 5
	default:
		return 32 // More than the complete signed int64 representation.
	}
}

func positionBackupValid(p RecordingPlaybackPositionBackup) bool {
	incarnation, a := uuid.Parse(p.Incarnation)
	operation, b := uuid.Parse(p.OperationID)
	validTime := func(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }
	return a == nil && b == nil && incarnation != uuid.Nil && operation != uuid.Nil && incarnation.String() == p.Incarnation && operation.String() == p.OperationID &&
		p.RecordingID != "" && len(p.RecordingID) <= 128 && utf8.ValidString(p.RecordingID) && !strings.ContainsRune(p.RecordingID, 0) && strings.TrimSpace(p.RecordingID) == p.RecordingID &&
		progressHex(p.ArtifactID, 32) && progressHex(p.MappingSHA256, 64) && p.DurationMS > 0 && p.DurationMS <= 43200000 &&
		p.PositionMS >= 0 && p.PositionMS <= p.DurationMS && p.AtDerivedEnd == (p.PositionMS == p.DurationMS) && p.Revision >= 0 && p.WriterGeneration >= 0 &&
		validTime(p.DescriptorObservedAt) && validTime(p.DescriptorExpiresAt) && validTime(p.UpdatedAt) &&
		p.DescriptorExpiresAt.After(p.DescriptorObservedAt) && !p.DescriptorExpiresAt.After(p.DescriptorObservedAt.Add(24*time.Hour))
}

func positionBackupBudget(rows RecordingPlaybackPositionBackupRows) error {
	bytes, seen := 2, make(map[string]bool)
	for index, row := range rows {
		if !positionBackupValid(row) || seen[row.RecordingID] {
			return ErrRecordingPositionBackup
		}
		seen[row.RecordingID] = true
		raw, err := json.Marshal(row)
		if err != nil {
			return ErrRecordingPositionBackup
		}
		if index != 0 {
			bytes++
		}
		if len(raw) > RecordingPlaybackPositionBackupByteLimit-bytes {
			return ErrRecordingPositionBackup
		}
		bytes += len(raw)
	}
	return nil
}

// Decode the new section iteratively after bounding its actual original JSON
// bytes. Existing database.json ingestion is unchanged and is not a ZIP bound.
func (rows *RecordingPlaybackPositionBackupRows) UnmarshalJSON(raw []byte) error {
	if rows == nil || len(raw) > RecordingPlaybackPositionBackupByteLimit || !utf8.Valid(raw) {
		return ErrRecordingPositionBackup
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('[') {
		return ErrRecordingPositionBackup
	}
	result := make(RecordingPlaybackPositionBackupRows, 0)
	seen := make(map[string]bool)
	for decoder.More() {
		start, err = decoder.Token()
		if err != nil || start != json.Delim('{') {
			return ErrRecordingPositionBackup
		}
		fields := make(map[string]json.RawMessage)
		for decoder.More() {
			key, err := decoder.Token()
			name, ok := key.(string)
			if err != nil || !ok || !recordingPositionFields[name] || fields[name] != nil {
				return ErrRecordingPositionBackup
			}
			var value json.RawMessage
			if decoder.Decode(&value) != nil || len(value) > positionBackupFieldBytes(name) || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return ErrRecordingPositionBackup
			}
			fields[name] = value
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') || len(fields) != len(recordingPositionFields) {
			return ErrRecordingPositionBackup
		}
		encoded, err := json.Marshal(fields)
		if err != nil {
			return ErrRecordingPositionBackup
		}
		var row RecordingPlaybackPositionBackup
		if json.Unmarshal(encoded, &row) != nil || !positionBackupValid(row) || seen[row.RecordingID] {
			return ErrRecordingPositionBackup
		}
		seen[row.RecordingID] = true
		result = append(result, row)
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') {
		return ErrRecordingPositionBackup
	}
	if _, err = decoder.Token(); err != io.EOF || positionBackupBudget(result) != nil {
		return ErrRecordingPositionBackup
	}
	*rows = result
	return nil
}

func (rows RecordingPlaybackPositionBackupRows) MarshalJSON() ([]byte, error) {
	if positionBackupBudget(rows) != nil {
		return nil, ErrRecordingPositionBackup
	}
	type plainRows RecordingPlaybackPositionBackupRows
	if rows == nil {
		return []byte("[]"), nil
	}
	return json.Marshal(plainRows(rows))
}

// Export only 085 payload. The surrounding existing export is sequential DATA,
// not an atomic snapshot of all database sections or an authorization grant.
func (ds *DataStore) ExportRecordingPlaybackPositions(ctx context.Context) (RecordingPlaybackPositionBackupRows, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrRecordingPositionBackup
	}
	connection, err := ds.pool.Acquire(ctx)
	if err != nil {
		return nil, ErrRecordingPositionBackup
	}
	defer func() {
		physical := connection.Conn().PgConn()
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
	}()
	var schema string
	if connection.QueryRow(ctx, `SELECT n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=pg_catalog.to_regclass('recordings') AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition`).Scan(&schema) != nil {
		return nil, ErrRecordingPositionBackup
	}
	query := `SELECT CASE WHEN octet_length(recording_id) BETWEEN 1 AND 128 THEN recording_id END,incarnation::text,
 CASE WHEN octet_length(artifact_id)=32 THEN artifact_id END,CASE WHEN octet_length(mapping_sha256)=64 THEN mapping_sha256 END,
 duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id::text,updated_at
 FROM ` + pgx.Identifier{schema, "recording_playback_positions"}.Sanitize() + ` ORDER BY recording_id`
	rows, err := connection.Query(ctx, query)
	if err != nil {
		return nil, ErrRecordingPositionBackup
	}
	defer rows.Close()
	result := make(RecordingPlaybackPositionBackupRows, 0)
	size := 2
	for rows.Next() {
		var value RecordingPlaybackPositionBackup
		if rows.Scan(&value.RecordingID, &value.Incarnation, &value.ArtifactID, &value.MappingSHA256, &value.DurationMS, &value.PositionMS, &value.AtDerivedEnd, &value.DescriptorObservedAt, &value.DescriptorExpiresAt, &value.Revision, &value.WriterGeneration, &value.OperationID, &value.UpdatedAt) != nil || !positionBackupValid(value) {
			return nil, ErrRecordingPositionBackup
		}
		encoded, err := json.Marshal(value)
		if len(result) != 0 {
			size++
		}
		if err != nil || len(encoded) > RecordingPlaybackPositionBackupByteLimit-size {
			return nil, ErrRecordingPositionBackup
		}
		size += len(encoded)
		result = append(result, value)
	}
	if rows.Err() != nil || ctx.Err() != nil {
		return nil, ErrRecordingPositionBackup
	}
	return result, nil
}

// This single fixed import joins the exact original Repositories body. It
// cannot accept a caller TX, an alternate pool, auth proof or a SQL callback.
func (t *SettingsRestoreTransaction) RestoreRecordingPlaybackPositions(body *Tx, rows RecordingPlaybackPositionBackupRows) error {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return settingscommit.ErrCoordinatorRequired
	}
	t.mu.Lock()
	if !t.claimed || t.closing || t.committed || t.bodyFinishing || t.positionsClaimed || body == nil || body != t.repositoryBody || t.ctx.Err() != nil {
		t.mu.Unlock()
		return settingscommit.ErrCoordinatorRequired
	}
	t.positionsClaimed = true
	t.operations.Add(1)
	t.mu.Unlock()
	defer t.operations.Done()
	if len(rows) > RecordingPlaybackPositionBackupByteLimit/positionBackupMinimumRowBytes {
		return ErrRecordingPositionBackup
	}
	// Retain server-owned DATA while a real query can be blocked. Never read a
	// caller's slice again after validation or adopt later replacement entries.
	owned := append(RecordingPlaybackPositionBackupRows(nil), rows...)
	if positionBackupBudget(owned) != nil {
		return ErrRecordingPositionBackup
	}
	if len(owned) == 0 {
		return nil
	}
	catalog, err := readRecordingPlaybackCatalog(t.ctx, t.tx)
	if err != nil {
		return ErrRecordingPositionBackup
	}
	for _, row := range owned {
		state, err := lockRecordingPlaybackState(t.ctx, t.tx, row.RecordingID)
		if err != nil || state.revision != 0 || state.generation != 0 || state.operationID != nil || !state.tombstone || state.incarnation == row.Incarnation {
			return ErrRecordingPositionBackup
		}
		var bornHere bool
		query := `SELECT xmin::text::bigint=(txid_current()%4294967296) FROM ` + pgx.Identifier{catalog.schema, "recording_playback_state"}.Sanitize() + ` WHERE recording_id=$1`
		if t.tx.QueryRow(t.ctx, query, row.RecordingID).Scan(&bornHere) != nil || !bornHere {
			return ErrRecordingPositionBackup
		}
		query = `INSERT INTO ` + pgx.Identifier{catalog.schema, "recording_playback_positions"}.Sanitize() + `
 (recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,updated_at)
 VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,$13)`
		tag, err := t.tx.Exec(t.ctx, query, row.RecordingID, row.Incarnation, row.ArtifactID, row.MappingSHA256, row.DurationMS, row.PositionMS, row.AtDerivedEnd, row.DescriptorObservedAt, row.DescriptorExpiresAt, row.Revision, row.WriterGeneration, row.OperationID, row.UpdatedAt)
		if err != nil || tag.RowsAffected() != 1 || t.ctx.Err() != nil {
			return ErrRecordingPositionBackup
		}
	}
	return nil
}
