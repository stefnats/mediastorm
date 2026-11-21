package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"novastream/internal/settingscommit"
)

// Match the existing JSON-import budget. This does not bound the whole ZIP.
const RecordingProgressHistoryBackupByteLimit = 50 << 20
const historyBackupMinimumRowBytes = len(`{"recordingId":"","dataJSON":""}`)

var ErrRecordingHistoryBackup = errors.New("invalid or unavailable recording history backup data")

// These are historical DATA. Import never grants current playback authority.
// Keeping canonical JSON as a string preserves its original bytes when the
// surrounding database export is indented.
type RecordingProgressHistoryBackup struct {
	RecordingID string `json:"recordingId"`
	DataJSON    string `json:"dataJSON"`
}
type RecordingProgressHistoryBackupRows []RecordingProgressHistoryBackup

func historyBackupRow(row RecordingProgressHistoryBackup) (*recordingProgressHistoryData, error) {
	if !historySafeRecording(row.RecordingID) || len(row.DataJSON) > recordingHistoryByteLimit {
		return nil, ErrRecordingHistoryBackup
	}
	d, err := decodeRecordingProgressHistory([]byte(row.DataJSON))
	if err != nil || d.tuple.recording != row.RecordingID {
		return nil, ErrRecordingHistoryBackup
	}
	return d, nil
}
func historyBackupBudget(rows RecordingProgressHistoryBackupRows) error {
	if len(rows) > RecordingProgressHistoryBackupByteLimit/historyBackupMinimumRowBytes {
		return ErrRecordingHistoryBackup
	}
	size := 2
	seen := make(map[string]bool)
	for i, row := range rows {
		if _, err := historyBackupRow(row); err != nil || seen[row.RecordingID] {
			return ErrRecordingHistoryBackup
		}
		seen[row.RecordingID] = true
		raw, err := json.Marshal(row)
		if i != 0 {
			size++
		}
		if err != nil || len(raw) > RecordingProgressHistoryBackupByteLimit-size {
			return ErrRecordingHistoryBackup
		}
		size += len(raw)
	}
	return nil
}
func (rows RecordingProgressHistoryBackupRows) MarshalJSON() ([]byte, error) {
	if err := historyBackupBudget(rows); err != nil {
		return nil, err
	}
	if rows == nil {
		return []byte("[]"), nil
	}
	type plain RecordingProgressHistoryBackupRows
	return json.Marshal(plain(rows))
}
func (rows *RecordingProgressHistoryBackupRows) UnmarshalJSON(raw []byte) error {
	if rows == nil || len(raw) > RecordingProgressHistoryBackupByteLimit || !utf8.Valid(raw) {
		return ErrRecordingHistoryBackup
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return ErrRecordingHistoryBackup
	}
	result := make(RecordingProgressHistoryBackupRows, 0)
	seen := make(map[string]bool)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil || token != json.Delim('{') {
			return ErrRecordingHistoryBackup
		}
		fields := make(map[string]string)
		for decoder.More() {
			token, err = decoder.Token()
			key, ok := token.(string)
			if err != nil || !ok || key != "recordingId" && key != "dataJSON" {
				return ErrRecordingHistoryBackup
			}
			if _, exists := fields[key]; exists {
				return ErrRecordingHistoryBackup
			}
			var value json.RawMessage
			if decoder.Decode(&value) != nil {
				return ErrRecordingHistoryBackup
			}
			max := 6*recordingHistoryByteLimit + 2
			if key == "recordingId" {
				max = 6*128 + 2
			}
			var text string
			if len(value) > max || bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &text) != nil {
				return ErrRecordingHistoryBackup
			}
			fields[key] = text
		}
		token, err = decoder.Token()
		if err != nil || token != json.Delim('}') || len(fields) != 2 {
			return ErrRecordingHistoryBackup
		}
		row := RecordingProgressHistoryBackup{RecordingID: fields["recordingId"], DataJSON: fields["dataJSON"]}
		if _, err = historyBackupRow(row); err != nil || seen[row.RecordingID] {
			return ErrRecordingHistoryBackup
		}
		seen[row.RecordingID] = true
		if len(result) >= RecordingProgressHistoryBackupByteLimit/historyBackupMinimumRowBytes {
			return ErrRecordingHistoryBackup
		}
		result = append(result, row)
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return ErrRecordingHistoryBackup
	}
	if _, err = decoder.Token(); err != io.EOF || historyBackupBudget(result) != nil {
		return ErrRecordingHistoryBackup
	}
	*rows = result
	return nil
}

// Export uses one owned read-only transaction and publishes nothing until the
// rollback and actual physical cleanup have finished. It is not a whole-DB
// atomic backup: the surrounding existing exporter reads its sections serially.
func (ds *DataStore) ExportRecordingProgressHistory(ctx context.Context) (result RecordingProgressHistoryBackupRows, err error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrRecordingHistoryBackup
	}
	connection, e := ds.pool.Acquire(ctx)
	if e != nil {
		return nil, ErrRecordingHistoryBackup
	}
	var tx pgx.Tx
	defer func() {
		var rollbackErr error
		if tx != nil {
			end, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rollbackErr = tx.Rollback(end)
			cancel()
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) && err == nil {
				err = ErrRecordingHistoryBackup
			}
		}
		physical := connection.Conn().PgConn()
		unhealthy := physical.IsClosed() || physical.IsBusy() || physical.TxStatus() != 'I'
		if unhealthy && err == nil {
			err = ErrRecordingHistoryBackup
		}
		if !physical.IsClosed() && (unhealthy || rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed)) {
			end, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = connection.Conn().Close(end)
			cancel()
		}
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
		if ctx.Err() != nil {
			err = ErrRecordingHistoryBackup
		}
		if err != nil {
			result = nil
		}
	}()
	tx, e = connection.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if e != nil {
		return nil, ErrRecordingHistoryBackup
	}
	schema, e := readRecordingHistorySchema(ctx, tx)
	if e != nil {
		return nil, ErrRecordingHistoryBackup
	}
	query := `SELECT CASE WHEN octet_length(recording_id) BETWEEN 1 AND 128 THEN recording_id END,
 CASE WHEN octet_length(data_text)<=65536 THEN data_text END,incarnation::text,
 CASE WHEN octet_length(artifact_id)=32 THEN artifact_id END,CASE WHEN octet_length(mapping_sha256)=64 THEN mapping_sha256 END,
 duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id::text
 FROM ` + pgx.Identifier{schema, "recording_progress_history"}.Sanitize() + ` ORDER BY recording_id`
	sqlRows, e := tx.Query(ctx, query)
	if e != nil {
		return nil, ErrRecordingHistoryBackup
	}
	defer sqlRows.Close()
	result = make(RecordingProgressHistoryBackupRows, 0)
	size := 2
	for sqlRows.Next() {
		var id, raw, artifact, mapping *string
		var incarnation, operation string
		var duration, position, revision, generation int64
		var end bool
		var observed, expires time.Time
		if sqlRows.Scan(&id, &raw, &incarnation, &artifact, &mapping, &duration, &position, &end, &observed, &expires, &revision, &generation, &operation) != nil || id == nil || raw == nil || artifact == nil || mapping == nil {
			return nil, ErrRecordingHistoryBackup
		}
		row := RecordingProgressHistoryBackup{RecordingID: *id, DataJSON: *raw}
		d, e := historyBackupRow(row)
		if e != nil {
			return nil, ErrRecordingHistoryBackup
		}
		t, v := d.tuple, d.envelope
		if t.incarnation != incarnation || t.artifact != *artifact || v.Mapping != *mapping || t.duration != duration || v.Position != position || v.End != end || v.Revision != revision || v.Generation != generation || v.Operation != operation || !historyPGTime(d.observed).Equal(observed) || !historyPGTime(d.expires).Equal(expires) {
			return nil, ErrRecordingHistoryBackup
		}
		encoded, e := json.Marshal(row)
		if len(result) != 0 {
			size++
		}
		if e != nil || len(encoded) > RecordingProgressHistoryBackupByteLimit-size {
			return nil, ErrRecordingHistoryBackup
		}
		size += len(encoded)
		result = append(result, row)
	}
	if sqlRows.Err() != nil || ctx.Err() != nil {
		return nil, ErrRecordingHistoryBackup
	}
	return result, nil
}

// Trusted caller contract: all repository/085/086 SQL on this original body is
// strictly serial. Call only after RestoreRecordingPlaybackPositions returned
// nil and actually ended. Admission flags and operation joins are not proof of
// predecessor success or a mutex. Any phase error aborts the whole restore.
func (t *SettingsRestoreTransaction) RestoreRecordingProgressHistory(body *Tx, rows RecordingProgressHistoryBackupRows) error {
	if t == nil || t.settingsRestoreState == nil || t.self != t {
		return settingscommit.ErrCoordinatorRequired
	}
	t.mu.Lock()
	if !t.claimed || !t.positionsClaimed || t.historyClaimed || t.closing || t.committed || t.bodyFinishing || body == nil || body != t.repositoryBody || t.ctx.Err() != nil {
		t.mu.Unlock()
		return settingscommit.ErrCoordinatorRequired
	}
	t.historyClaimed = true
	t.operations.Add(1)
	t.mu.Unlock()
	defer t.operations.Done()
	if len(rows) > RecordingProgressHistoryBackupByteLimit/historyBackupMinimumRowBytes {
		return ErrRecordingHistoryBackup
	}
	owned := append(RecordingProgressHistoryBackupRows(nil), rows...)
	if historyBackupBudget(owned) != nil {
		return ErrRecordingHistoryBackup
	}
	if len(owned) == 0 {
		return nil
	}
	schema, err := readRecordingHistorySchema(t.ctx, t.tx)
	if err != nil {
		return ErrRecordingHistoryBackup
	}
	for _, row := range owned {
		d, err := historyBackupRow(row)
		if err != nil {
			return err
		}
		state, err := lockRecordingPlaybackState(t.ctx, t.tx, row.RecordingID)
		if err != nil || state.revision != 0 || state.generation != 0 || state.operationID != nil || !state.tombstone || state.incarnation == d.tuple.incarnation {
			return ErrRecordingHistoryBackup
		}
		var fresh, parent bool
		query := `SELECT s.xmin::text::bigint=(txid_current()%4294967296),EXISTS(SELECT 1 FROM ` + pgx.Identifier{schema, "recording_playback_positions"}.Sanitize() + ` p WHERE p.recording_id=s.recording_id) FROM ` + pgx.Identifier{schema, "recording_playback_state"}.Sanitize() + ` s WHERE s.recording_id=$1`
		if t.tx.QueryRow(t.ctx, query, row.RecordingID).Scan(&fresh, &parent) != nil || !fresh || !parent {
			return ErrRecordingHistoryBackup
		}
		e, v := d.envelope, d.tuple
		query = `INSERT INTO ` + pgx.Identifier{schema, "recording_progress_history"}.Sanitize() + ` (recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,data_text) VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,$13)`
		tag, err := t.tx.Exec(t.ctx, query, v.recording, v.incarnation, v.artifact, e.Mapping, v.duration, e.Position, e.End, historyPGTime(d.observed), historyPGTime(d.expires), e.Revision, e.Generation, e.Operation, row.DataJSON)
		if err != nil || tag.RowsAffected() != 1 || t.ctx.Err() != nil {
			return ErrRecordingHistoryBackup
		}
	}
	return nil
}
