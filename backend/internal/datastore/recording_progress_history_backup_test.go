//go:build linux

package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/internal/settingscommit"
)

func historyBackupExample(t *testing.T) RecordingProgressHistoryBackup {
	t.Helper()
	return RecordingProgressHistoryBackup{RecordingID: "progress", DataJSON: string(historyEncoded(t, historyExample()))}
}
func TestRecordingHistoryBackupCanonicalRows(t *testing.T) {
	for _, kind := range []string{"canonical-string-roundtrip", "expired-historical-data", "missing-key", "unknown-key", "duplicate-key", "duplicate-recording-id", "present-null", "wrong-type", "noncanonical-data", "oversized-data"} {
		t.Run(kind, func(t *testing.T) {
			row := historyBackupExample(t)
			raw, err := json.Marshal(RecordingProgressHistoryBackupRows{row})
			if err != nil {
				t.Fatal(err)
			}
			valid := kind == "canonical-string-roundtrip" || kind == "expired-historical-data"
			switch kind {
			case "missing-key":
				raw = []byte(`[{"recordingId":"progress"}]`)
			case "unknown-key":
				raw = bytes.Replace(raw, []byte(`"recordingId":`), []byte(`"unknown":0,"recordingId":`), 1)
			case "duplicate-key":
				raw = bytes.Replace(raw, []byte(`"recordingId":`), []byte(`"recordingId":"progress","recordingId":`), 1)
			case "duplicate-recording-id":
				raw = append(append(append([]byte(nil), raw[:len(raw)-1]...), ','), raw[1:]...)
			case "present-null":
				raw = []byte(`null`)
			case "wrong-type":
				raw = []byte(`[{"recordingId":"progress","dataJSON":{}}]`)
			case "noncanonical-data":
				row.DataJSON = " " + row.DataJSON
				raw, _ = json.Marshal([]RecordingProgressHistoryBackup{row})
			case "oversized-data":
				row.DataJSON = strings.Repeat("x", recordingHistoryByteLimit+1)
				raw, _ = json.Marshal([]RecordingProgressHistoryBackup{row})
			}
			var got RecordingProgressHistoryBackupRows
			err = json.Unmarshal(raw, &got)
			if !valid {
				if err == nil || len(got) != 0 {
					t.Fatal("invalid history section accepted")
				}
				return
			}
			if err != nil || len(got) != 1 || got[0] != row {
				t.Fatal("canonical history changed", err)
			}
			indented, err := json.MarshalIndent(got, "", "  ")
			if err != nil || json.Unmarshal(indented, &got) != nil || got[0].DataJSON != row.DataJSON {
				t.Fatal("outer indentation rewrote canonical history")
			}
			d, err := historyBackupRow(got[0])
			if err != nil || !d.expires.Before(time.Now()) {
				t.Fatal("expired historical DATA renewed or rejected", err)
			}
		})
	}
}
func historyBackupSeed(t *testing.T, f *recordingProgressFixture, d *recordingProgressHistoryData) RecordingProgressHistoryBackupRows {
	t.Helper()
	g := historyGuard(t, f, t.Context())
	historyInsertPosition(t, g.tx, d)
	if err := g.writeRecordingProgressHistory(d); err != nil {
		t.Fatal(err)
	}
	if err := g.tx.Commit(g.ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := f.ds.ExportRecordingProgressHistory(t.Context())
	if err != nil || len(rows) != 1 {
		t.Fatal("export genuine086", err)
	}
	return rows
}
func historyBackupLarge(t *testing.T) *recordingProgressHistoryData {
	t.Helper()
	d := historyDecoded(t, historyExample())
	e := d.envelope
	spans := make([][3]int64, 256)
	e.Manifest = make([]recordingHistoryInput, 256)
	for i := range spans {
		spans[i] = [3]int64{int64(i + 1), int64(i), 1}
		e.Manifest[i] = recordingHistoryInput{int64(i + 1), 1, strings.Repeat("a", 64)}
	}
	d.tuple.duration = 256
	d.tuple.spans = spans
	e.Timeline, _ = d.tuple.bytes()
	h := sha256.Sum256(e.Timeline)
	e.Mapping = hex.EncodeToString(h[:])
	e.Position = 1
	return historyDecoded(t, e)
}
func TestRecordingHistoryBackupActualExport(t *testing.T) {
	for _, kind := range []string{"historical-data", "stale-data", "own-row-scalar-mismatch", "wire-budget-before-scan", "total-byte-limit"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			d := historyDecoded(t, historyExample())
			if kind == "total-byte-limit" {
				d = historyBackupLarge(t)
			}
			expected := historyBackupSeed(t, f, d)
			switch kind {
			case "stale-data":
				f.exec(t, `UPDATE recording_playback_positions SET revision=revision+1`)
			case "own-row-scalar-mismatch":
				f.exec(t, `UPDATE recording_progress_history SET revision=revision+1`)
			case "wire-budget-before-scan":
				f.exec(t, `ALTER TABLE recording_progress_history DROP CONSTRAINT recording_progress_history_data_budget;UPDATE recording_progress_history SET data_text=repeat('x',1000000)`)
			case "total-byte-limit":
				encoded, _ := json.Marshal(expected[0])
				count := RecordingProgressHistoryBackupByteLimit/len(encoded) + 2
				// Clone the real synthetic parent and payload with distinct canonical IDs.
				// PostgreSQL's actual SHA256 computes each changed tuple's own mapping;
				// no oversized malformed row can stand in for the aggregate budget case.
				f.exec(t, `INSERT INTO recordings SELECT (jsonb_populate_record(NULL::recordings,to_jsonb(r)||jsonb_build_object('id','budget-'||i))).* FROM recordings r CROSS JOIN generate_series(1,$1) AS series(i) WHERE r.id='progress'`, count)
				f.exec(t, `INSERT INTO recording_playback_positions(recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,updated_at)
 SELECT 'budget-'||i,p.incarnation,p.artifact_id,encode(sha256(convert_to(replace($2,'"progress"','"budget-'||i||'"'),'UTF8')),'hex'),p.duration_ms,p.position_ms,p.at_derived_end,p.descriptor_observed_at,p.descriptor_expires_at,p.revision,p.writer_generation,p.operation_id,p.updated_at FROM recording_playback_positions p CROSS JOIN generate_series(1,$1) AS series(i) WHERE p.recording_id='progress'`, count, string(d.envelope.Timeline))
				f.exec(t, `INSERT INTO recording_progress_history(recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,data_text)
 SELECT p.recording_id,h.incarnation,h.artifact_id,p.mapping_sha256,h.duration_ms,h.position_ms,h.at_derived_end,h.descriptor_observed_at,h.descriptor_expires_at,h.revision,h.writer_generation,h.operation_id,replace(replace(h.data_text,'"progress"','"'||p.recording_id||'"'),h.mapping_sha256,p.mapping_sha256) FROM recording_playback_positions p CROSS JOIN recording_progress_history h WHERE h.recording_id='progress' AND p.recording_id<>'progress'`)
			}
			got, err := f.ds.ExportRecordingProgressHistory(t.Context())
			if kind == "own-row-scalar-mismatch" || kind == "wire-budget-before-scan" || kind == "total-byte-limit" {
				if err == nil || got != nil {
					t.Fatal("invalid/oversized export published partial history")
				}
				return
			}
			if err != nil || len(got) != 1 || got[0] != expected[0] {
				t.Fatal("historical/stale DATA changed", err)
			}
			read, e := historyRead(t, f)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "stale-data" && read.state != "stale" {
				t.Fatal("stale history healed")
			}
		})
	}
}
func historyBackupRestoreFixture(t *testing.T) (*recordingProgressFixture, RecordingProgressHistoryBackupRows, RecordingPlaybackPositionBackupRows, *SettingsRestoreTransaction, *Tx, func(), *settingscommit.WriteGuard, string) {
	t.Helper()
	f := historyPG(t)
	history := historyBackupSeed(t, f, historyDecoded(t, historyExample()))
	positions, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	record := positionBackupRecording(t, f)
	old := f.state(t, "progress").incarnation
	r, body, finish, g := positionBackupRestore(t, f)
	positionBackupRecreate(t, r, body, record)
	return f, history, positions, r, body, finish, g, old
}
func TestRecordingHistoryBackupOriginalRestoreBody(t *testing.T) {
	for _, kind := range []string{"genuine-original-body", "foreign-body", "copied-owner", "missing-imported-085", "body-finishing", "duplicate-phase"} {
		t.Run(kind, func(t *testing.T) {
			f, h, p, r, body, finish, g, old := historyBackupRestoreFixture(t)
			if kind == "missing-imported-085" {
				p = nil
			}
			if err := r.RestoreRecordingPlaybackPositions(body, p); err != nil {
				t.Fatal(err)
			}
			candidate := body
			switch kind {
			case "foreign-body":
				candidate = &Tx{}
			case "copied-owner":
				copy := *r
				if copy.RestoreRecordingProgressHistory(body, h) == nil {
					t.Fatal("copied owner accepted")
				}
				return
			case "body-finishing":
				finish()
			}
			err := r.RestoreRecordingProgressHistory(candidate, h)
			if kind == "foreign-body" || kind == "missing-imported-085" || kind == "body-finishing" {
				if err == nil {
					t.Fatal("foreign/missing/finished body accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "duplicate-phase" && !errors.Is(r.RestoreRecordingProgressHistory(body, h), settingscommit.ErrCoordinatorRequired) {
				t.Fatal("history phase replayed")
			}
			finish()
			if err = r.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			g.Close()
			got, err := f.ds.ExportRecordingProgressHistory(t.Context())
			if err != nil || len(got) != 1 || got[0] != h[0] {
				t.Fatal("same original TX lost086", err)
			}
			if f.state(t, "progress").incarnation == old || f.state(t, "progress").incarnation == historyExampleIncarnation() {
				t.Fatal("old084 incarnation imported")
			}
		})
	}
}
func historyExampleIncarnation() string { return "11111111-1111-4111-8111-111111111111" }
func historyBackupInsertLock(t *testing.T, f *recordingProgressFixture) (pgx.Tx, uint32) {
	t.Helper()
	table := pgx.Identifier{f.schema, "history_backup_insert_guard"}.Sanitize()
	f.exec(t, `CREATE TABLE history_backup_insert_guard(recording_id text PRIMARY KEY);INSERT INTO history_backup_insert_guard VALUES('progress');ALTER TABLE recording_progress_history ADD CONSTRAINT history_backup_insert_guard_fk FOREIGN KEY(recording_id) REFERENCES history_backup_insert_guard(recording_id)`)
	tx, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { recordingStateRollback(tx) })
	var pid uint32
	if tx.QueryRow(t.Context(), `SELECT pg_backend_pid() FROM `+table+` WHERE recording_id='progress' FOR UPDATE`).Scan(&pid) != nil {
		t.Fatal("hold real086 INSERT FK")
	}
	return tx, pid
}
func TestRecordingHistoryBackupActualCleanup(t *testing.T) {
	for _, kind := range []string{"body-finish-held-insert", "close-cancels-held-insert", "physical-close-cleanup-join"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			h := historyBackupSeed(t, f, historyDecoded(t, historyExample()))
			p, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			record := positionBackupRecording(t, f)
			lock, blocker := historyBackupInsertLock(t, f)
			r, body, finish, g := positionBackupRestore(t, f)
			positionBackupRecreate(t, r, body, record)
			if err = r.RestoreRecordingPlaybackPositions(body, p); err != nil {
				t.Fatal(err)
			}
			physical := r.connection.Conn().PgConn()
			// The genuine WriteGuard wraps the original runtime DialFunc's
			// transport in its own owner. Inspect its exported embedded Conn
			// solely to hold the actual synthetic socket Close, never to forge
			// a guard, transaction or authorization receipt.
			primary, ok := physical.Conn().(*progressCommitConnection)
			if !ok {
				value := reflect.ValueOf(physical.Conn())
				if value.Kind() == reflect.Pointer {
					field := value.Elem().FieldByName("Conn")
					if field.IsValid() && field.CanInterface() {
						primary, ok = field.Interface().(*progressCommitConnection)
					}
				}
			}
			if !ok {
				t.Fatal("actual original wrapped transport missing")
			}
			pid := physical.PID()
			done := make(chan error, 1)
			workDone := make(chan struct{})
			finishDone := make(chan struct{})
			closeDone := make(chan error, 1)
			guardDone := make(chan struct{})
			closeStarted := false
			guardStarted := false
			finishStarted := false
			t.Cleanup(func() {
				f.closeReleaseOnce.Do(func() { close(f.closeRelease) })
				recordingStateRollback(lock)
				<-workDone
				finish()
				if finishStarted {
					<-finishDone
				}
				if closeStarted {
					<-closeDone
				}
				if guardStarted {
					<-guardDone
				}
			})
			go func() { defer close(workDone); done <- r.RestoreRecordingProgressHistory(body, h) }()
			settingsStoreWait(t, func() bool {
				var waiting bool
				e := f.admin.QueryRow(t.Context(), `SELECT $2::int=ANY(pg_blocking_pids($1))`, pid, blocker).Scan(&waiting)
				return e == nil && waiting
			})
			finishStarted = true
			go func() { finish(); close(finishDone) }()
			settingsStoreWait(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.bodyFinishing })
			select {
			case <-finishDone:
				t.Fatal("body finish preceded actual086 IO")
			default:
			}
			if kind == "body-finish-held-insert" {
				if err = lock.Rollback(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err = <-done; err != nil {
					t.Fatal(err)
				}
				<-workDone
				<-finishDone
				if err = r.Commit(); err != nil {
					t.Fatal(err)
				}
				if err = r.Close(); err != nil {
					t.Fatal(err)
				}
				g.Close()
				return
			}
			if kind == "physical-close-cleanup-join" {
				primary.holdClose.Store(true)
				if err = primary.Conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			closeStarted = true
			go func() { closeDone <- r.Close() }()
			if kind == "physical-close-cleanup-join" {
				select {
				case <-f.closeEntered:
				case <-time.After(8 * time.Second):
					t.Fatal("actual original Close not entered")
				}
				select {
				case <-physical.CleanupDone():
					t.Fatal("physical cleanup ended before held Close")
				default:
				}
				guardStarted = true
				go func() { g.Close(); close(guardDone) }()
				settingsStoreWait(t, func() bool { return g.Context().Err() != nil })
				select {
				case <-guardDone:
					t.Fatal("guard refunded before actual physical cleanup")
				default:
				}
				f.closeReleaseOnce.Do(func() { close(f.closeRelease) })
			}
			select {
			case err = <-done:
				if err == nil {
					t.Fatal("cancelled086 query succeeded")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("original query did not end")
			}
			<-workDone
			<-finishDone
			select {
			case err = <-closeDone:
				closeStarted = false
			case <-time.After(8 * time.Second):
				t.Fatal("Close did not join")
			}
			if kind == "physical-close-cleanup-join" {
				if err == nil {
					t.Fatal("unhealthy original connection reported successful cleanup")
				}
				select {
				case <-physical.CleanupDone():
				default:
					t.Fatal("Close returned before physical end")
				}
				<-guardDone
				guardStarted = false
			}
		})
	}
}
func TestRecordingHistoryBackupTransactionOutcomes(t *testing.T) {
	for _, kind := range []string{"known-rollback", "known-commit", "unknown-ack-no-replay"} {
		t.Run(kind, func(t *testing.T) {
			f, h, p, r, body, finish, g, old := historyBackupRestoreFixture(t)
			fresh, e := lockRecordingPlaybackState(r.Context(), r.tx, "progress")
			if e != nil {
				t.Fatal(e)
			}
			if err := r.RestoreRecordingPlaybackPositions(body, p); err != nil {
				t.Fatal(err)
			}
			if err := r.RestoreRecordingProgressHistory(body, h); err != nil {
				t.Fatal(err)
			}
			finish()
			var commitErr error
			if kind != "known-rollback" {
				if kind == "unknown-ack-no-replay" {
					f.ackEnabled.Store(true)
				}
				commitErr = r.Commit()
			}
			if kind == "unknown-ack-no-replay" {
				if commitErr == nil || !f.ackDropped.Load() || r.Commit() == nil {
					t.Fatal("actual unknownACK replayed or reported success", commitErr)
				}
				f.ackEnabled.Store(false)
			} else if commitErr != nil {
				t.Fatal(commitErr)
			}
			_ = r.Close()
			g.Close()
			got, err := f.ds.ExportRecordingProgressHistory(t.Context())
			if err != nil || len(got) != 1 || got[0] != h[0] {
				t.Fatal("known/unknown original086 payload lost", err)
			}
			if kind == "known-rollback" && (f.state(t, "progress").incarnation == fresh.incarnation || f.state(t, "progress").incarnation != old) {
				t.Fatal("rollback adopted new084")
			}
			if kind != "known-rollback" && f.state(t, "progress").incarnation != fresh.incarnation {
				t.Fatal("real committed restore not witnessed")
			}
		})
	}
}
