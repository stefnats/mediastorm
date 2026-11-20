//go:build linux

package datastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/internal/settingscommit"
	"novastream/models"
)

func positionBackupExample() RecordingPlaybackPositionBackup {
	at := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	return RecordingPlaybackPositionBackup{RecordingID: "progress", Incarnation: "11111111-1111-4111-8111-111111111111", ArtifactID: strings.Repeat("a", 32), MappingSHA256: strings.Repeat("b", 64), DurationMS: 60000, PositionMS: 25000, DescriptorObservedAt: at, DescriptorExpiresAt: at.Add(24 * time.Hour), Revision: 2, WriterGeneration: 1, OperationID: "22222222-2222-4222-8222-222222222222", UpdatedAt: at}
}

func TestRecordingPositionBackupRowsRoundTripAndBounds(t *testing.T) {
	row := positionBackupExample()
	encoded, err := json.Marshal(RecordingPlaybackPositionBackupRows{row})
	if err != nil {
		t.Fatal(err)
	}
	var round RecordingPlaybackPositionBackupRows
	if json.Unmarshal(encoded, &round) != nil || len(round) != 1 || round[0] != row {
		t.Fatal("historical data round trip changed")
	}
	for _, kind := range []string{"missing-field", "duplicate-field", "duplicate-id", "malformed", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			raw := encoded
			switch kind {
			case "missing-field":
				raw = bytes.Replace(raw, []byte(`"revision":2,`), nil, 1)
			case "duplicate-field":
				raw = bytes.Replace(raw, []byte(`"revision":2`), []byte(`"revision":2,"revision":2`), 1)
			case "duplicate-id":
				raw = []byte("[" + string(encoded[1:len(encoded)-1]) + "," + string(encoded[1:len(encoded)-1]) + "]")
			case "malformed":
				raw = bytes.Replace(raw, []byte(`"positionMs":25000`), []byte(`"positionMs":60001`), 1)
			case "oversize":
				raw = []byte("[" + strings.Repeat(" ", RecordingPlaybackPositionBackupByteLimit) + "]")
			}
			var denied RecordingPlaybackPositionBackupRows
			if bytes.Equal(raw, encoded) || json.Unmarshal(raw, &denied) == nil || len(denied) != 0 {
				t.Fatal("invalid subset accepted or silently truncated")
			}
		})
	}
	for _, bad := range []RecordingPlaybackPositionBackup{func() RecordingPlaybackPositionBackup { v := row; v.RecordingID = strings.Repeat("x", 129); return v }(), func() RecordingPlaybackPositionBackup { v := row; v.AtDerivedEnd = true; return v }()} {
		if _, err := json.Marshal(RecordingPlaybackPositionBackupRows{bad}); err == nil {
			t.Fatal("invalid actual encoded subset accepted")
		}
	}
}

func positionBackupSeed(t *testing.T, f *recordingProgressFixture) RecordingPlaybackPositionBackupRows {
	t.Helper()
	f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	f.commit(t, progressCommand(RecordingProgressTouch, 1, 1, 25000))
	rows, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(rows) != 1 {
		t.Fatal("export actual saved085 row", err)
	}
	return rows
}

func positionBackupRestore(t *testing.T, f *recordingProgressFixture) (*SettingsRestoreTransaction, *Tx, func(), *settingscommit.WriteGuard) {
	t.Helper()
	g, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.ds.BeginSettingsRestore(t.Context(), g)
	if err != nil {
		g.Close()
		t.Fatal(err)
	}
	body, finish, err := r.Repositories()
	if err != nil {
		r.Close()
		g.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { finish(); r.Close(); g.Close() })
	return r, body, finish, g
}

func positionBackupRecreate(t *testing.T, r *SettingsRestoreTransaction, body *Tx, recording *models.Recording) {
	t.Helper()
	if err := body.Recordings().Delete(r.Context(), recording.ID); err != nil {
		t.Fatal(err)
	}
	if err := body.Recordings().Create(r.Context(), recording); err != nil {
		t.Fatal(err)
	}
}
func positionBackupRecording(t *testing.T, f *recordingProgressFixture) *models.Recording {
	t.Helper()
	r, err := f.ds.Recordings().Get(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRecordingPositionBackupExportRetainsHistoricalData(t *testing.T) {
	f := progressPG(t, false)
	rows := positionBackupSeed(t, f)
	if rows[0].PositionMS != 25000 || rows[0].Revision != 2 || rows[0].WriterGeneration != 1 {
		t.Fatal("actual payload counters lost")
	}
	f.exec(t, `UPDATE recording_playback_positions SET descriptor_observed_at='2020-01-01',descriptor_expires_at='2020-01-02'`)
	historical, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(historical) != 1 || historical[0].PositionMS != 25000 || historical[0].DescriptorExpiresAt.After(time.Now()) {
		t.Fatal("historical expiry deleted or renewed data", err)
	}
	// Even an explicitly corrupted owned schema cannot send an oversized text
	// field to the client or silently omit that row from a successful export.
	f.exec(t, `ALTER TABLE recording_playback_positions DROP CONSTRAINT recording_playback_positions_artifact_id_check;UPDATE recording_playback_positions SET artifact_id=repeat('x',100000)`)
	if partial, err := f.ds.ExportRecordingPlaybackPositions(t.Context()); err == nil || partial != nil {
		t.Fatal("oversized actual field was exported or silently truncated")
	}
}

func TestRecordingPositionBackupRestoreRequiresOriginalBody(t *testing.T) {
	for _, kind := range []string{"foreign", "copy", "after-finish"} {
		t.Run(kind, func(t *testing.T) {
			f := progressPG(t, false)
			rows := positionBackupSeed(t, f)
			record := positionBackupRecording(t, f)
			r, body, finish, _ := positionBackupRestore(t, f)
			positionBackupRecreate(t, r, body, record)
			candidate := body
			switch kind {
			case "foreign":
				candidate = &Tx{}
			case "copy":
				copy := *body
				candidate = &copy
				copyRestore := *r
				if copyRestore.RestoreRecordingPlaybackPositions(body, rows) == nil {
					t.Fatal("copied original restore object accepted")
				}
			case "after-finish":
				finish()
			}
			if r.RestoreRecordingPlaybackPositions(candidate, rows) == nil {
				t.Fatal("foreign/copied/finished repository body accepted")
			}
			if kind != "after-finish" {
				if err := r.RestoreRecordingPlaybackPositions(body, rows); err != nil {
					t.Fatal("invalid foreign call consumed the original fixed port", err)
				}
				if !errors.Is(r.RestoreRecordingPlaybackPositions(body, rows), settingscommit.ErrCoordinatorRequired) {
					t.Fatal("fixed subset import replayed after its first claim")
				}
			}
		})
	}
}

func TestRecordingPositionBackupRestoreRequiresFresh084(t *testing.T) {
	for _, kind := range []string{"old-state", "same-incarnation", "orphan"} {
		t.Run(kind, func(t *testing.T) {
			f := progressPG(t, false)
			rows := positionBackupSeed(t, f)
			record := positionBackupRecording(t, f)
			r, body, _, _ := positionBackupRestore(t, f)
			if kind != "old-state" {
				positionBackupRecreate(t, r, body, record)
			}
			if kind == "same-incarnation" {
				if r.tx.QueryRow(r.Context(), `SELECT incarnation::text FROM recording_playback_state WHERE recording_id='progress'`).Scan(&rows[0].Incarnation) != nil {
					t.Fatal("new state missing")
				}
			}
			if kind == "orphan" {
				rows[0].RecordingID = "missing"
			}
			if r.RestoreRecordingPlaybackPositions(body, rows) == nil {
				t.Fatal("unproven fresh original084 accepted")
			}
		})
	}
}

// positionBackupLock blocks only the INSERT's foreign-key KeyShare, not the
// earlier recording DELETE/CASCADE. The guard schema and FK are installed
// before the original restore transaction starts; its RowExclusive table lock
// is therefore compatible with every later step in this fixture.
func positionBackupLock(t *testing.T, f *recordingProgressFixture) (pgx.Tx, uint32) {
	t.Helper()
	guard := pgx.Identifier{f.schema, "position_backup_insert_guard"}.Sanitize()
	positions := pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize()
	f.exec(t, `CREATE TABLE position_backup_insert_guard(recording_id text PRIMARY KEY);INSERT INTO position_backup_insert_guard VALUES ('progress');ALTER TABLE recording_playback_positions ADD CONSTRAINT position_backup_insert_guard_fk FOREIGN KEY(recording_id) REFERENCES position_backup_insert_guard(recording_id)`)
	lock, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { recordingStateRollback(lock) })
	var pid uint32
	if err = lock.QueryRow(t.Context(), `SELECT pg_backend_pid() FROM `+guard+` WHERE recording_id='progress' FOR UPDATE`).Scan(&pid); err != nil {
		t.Fatal("hold actual synthetic parent guard row", err)
	}
	var valid bool
	if f.admin.QueryRow(t.Context(), `SELECT convalidated FROM pg_constraint WHERE conrelid=$1::regclass AND conname='position_backup_insert_guard_fk'`, positions).Scan(&valid) != nil || !valid {
		t.Fatal("actual INSERT foreign-key guard was not installed")
	}
	return lock, pid
}
func positionBackupWaiting(t *testing.T, f *recordingProgressFixture, pid, blocker uint32, want bool) {
	t.Helper()
	settingsStoreWait(t, func() bool {
		var waiting bool
		err := f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND left(query,length($3::text))=$3 AND $2::int=ANY(pg_blocking_pids(pid)))`, pid, blocker, `INSERT INTO `+pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize()).Scan(&waiting)
		return err == nil && waiting == want
	})
}
func positionBackupPID(t *testing.T, r *SettingsRestoreTransaction) uint32 {
	t.Helper()
	var pid uint32
	if r.tx.QueryRow(r.Context(), `SELECT pg_backend_pid()`).Scan(&pid) != nil {
		t.Fatal("actual original backend missing")
	}
	return pid
}

func TestRecordingPositionBackupRestoreBodyFinishJoinsActualQuery(t *testing.T) {
	f := progressPG(t, false)
	rows := positionBackupSeed(t, f)
	record := positionBackupRecording(t, f)
	lock, blocker := positionBackupLock(t, f)
	r, body, finish, g := positionBackupRestore(t, f)
	positionBackupRecreate(t, r, body, record)
	pid := positionBackupPID(t, r)
	done := make(chan error, 1)
	actualDone := make(chan struct{})
	t.Cleanup(func() {
		recordingStateRollback(lock)
		<-actualDone
	})
	go func() {
		defer close(actualDone)
		done <- r.RestoreRecordingPlaybackPositions(body, rows)
	}()
	positionBackupWaiting(t, f, pid, blocker, true)
	finished := make(chan struct{})
	go func() { finish(); close(finished) }()
	settingsStoreWait(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.bodyFinishing })
	select {
	case <-finished:
		t.Fatal("bodyDone preceded actual SQL end")
	default:
	}
	if r.Commit() == nil {
		t.Fatal("commit entered while repository body still had active SQL")
	}
	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("real query did not join")
	}
	select {
	case <-finished:
	case <-time.After(8 * time.Second):
		t.Fatal("body completion did not join")
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	r.Close()
	g.Close()
	got, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(got) != 1 || got[0] != rows[0] {
		t.Fatal("same original transaction lost historical data", err)
	}
}

func TestRecordingPositionBackupRestoreCloseCancelsActualQuery(t *testing.T) {
	f := progressPG(t, false)
	rows := positionBackupSeed(t, f)
	record := positionBackupRecording(t, f)
	lock, blocker := positionBackupLock(t, f)
	r, body, finish, g := positionBackupRestore(t, f)
	positionBackupRecreate(t, r, body, record)
	pid := positionBackupPID(t, r)
	done := make(chan error, 1)
	actualDone := make(chan struct{})
	t.Cleanup(func() {
		recordingStateRollback(lock)
		<-actualDone
	})
	go func() {
		defer close(actualDone)
		done <- r.RestoreRecordingPlaybackPositions(body, rows)
	}()
	positionBackupWaiting(t, f, pid, blocker, true)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled original SQL succeeded")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("cancelled SQL did not join")
	}
	positionBackupWaiting(t, f, pid, blocker, false)
	select {
	case <-closed:
		t.Fatal("Close skipped original repository body completion")
	default:
	}
	finish()
	select {
	case <-closed:
	case <-time.After(8 * time.Second):
		t.Fatal("Close did not join real body")
	}
	g.Close()
	recordingStateRollback(lock)
	got, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(got) != 1 || got[0] != rows[0] {
		t.Fatal("cancelled restore changed durable payload", err)
	}
}

func TestRecordingPositionBackupRestoreRollbackDoesNotPersist(t *testing.T) {
	f := progressPG(t, false)
	rows := positionBackupSeed(t, f)
	old := f.state(t, "progress")
	record := positionBackupRecording(t, f)
	r, body, finish, g := positionBackupRestore(t, f)
	positionBackupRecreate(t, r, body, record)
	if err := r.RestoreRecordingPlaybackPositions(body, rows); err != nil {
		t.Fatal(err)
	}
	finish()
	r.Close()
	g.Close()
	got, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(got) != 1 || got[0] != rows[0] || f.state(t, "progress").incarnation != old.incarnation {
		t.Fatal("rollback adopted restored identity or lost payload", err)
	}
}

func TestRecordingPositionBackupRestoreUnknownACKNoReplay(t *testing.T) {
	f := progressPG(t, false)
	rows := positionBackupSeed(t, f)
	old := f.state(t, "progress")
	record := positionBackupRecording(t, f)
	r, body, finish, g := positionBackupRestore(t, f)
	positionBackupRecreate(t, r, body, record)
	if err := r.RestoreRecordingPlaybackPositions(body, rows); err != nil {
		t.Fatal(err)
	}
	finish()
	f.ackEnabled.Store(true)
	if err := r.Commit(); err == nil || !f.ackDropped.Load() {
		t.Fatal("actual missing COMMIT ACK was not observed", err)
	}
	if r.Commit() == nil {
		t.Fatal("unknown commit replayed")
	}
	f.ackEnabled.Store(false)
	r.Close()
	g.Close()
	got, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
	if err != nil || len(got) != 1 || got[0] != rows[0] || f.state(t, "progress").incarnation == old.incarnation {
		t.Fatal("test did not prove actual commit followed by lost ACK", err)
	}
	if !errors.Is(r.RestoreRecordingPlaybackPositions(body, rows), settingscommit.ErrCoordinatorRequired) {
		t.Fatal("terminal restore body retained import power")
	}
}
