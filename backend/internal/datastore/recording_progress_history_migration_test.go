package datastore

import (
	"bytes"
	"strings"
	"testing"
)

func historyLegacyRows(t *testing.T, f *recordingStatePGFixture) []byte {
	t.Helper()
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_array((SELECT to_jsonb(r) FROM recordings r WHERE id='progress'),(SELECT to_jsonb(s) FROM recording_playback_state s WHERE recording_id='progress'),(SELECT to_jsonb(p) FROM recording_playback_positions p WHERE recording_id='progress'))`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func historyMigrationSeed(t *testing.T, f *recordingStatePGFixture) *recordingProgressHistoryData {
	t.Helper()
	f.profiles(t)
	f.create(t, "progress")
	d := historyDecoded(t, historyExample())
	tx := f.begin(t)
	historyInsertPosition(t, tx, d)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	return d
}
func TestRecordingProgressHistoryMigrationPreservesLegacyRows(t *testing.T) {
	for _, kind := range []string{"up-no-backfill", "down-preserves-084-085", "fresh-084-with-old-085", "orphan-owner"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingStatePG(t, 85)
			d := historyMigrationSeed(t, f)
			if kind == "orphan-owner" {
				f.exec(t, `ALTER TABLE recordings DROP CONSTRAINT recordings_user_id_fkey;UPDATE recordings SET user_id='historical-orphan' WHERE id='progress'`)
			}
			var current string
			if f.ds.pool.QueryRow(t.Context(), `SELECT incarnation::text FROM recording_playback_state WHERE recording_id='progress'`).Scan(&current) != nil || current == d.tuple.incarnation {
				t.Fatal("fixture lacks original historical085/fresh084")
			}
			before := historyLegacyRows(t, f)
			f.migrate(t, 86, false)
			if !bytes.Equal(before, historyLegacyRows(t, f)) {
				t.Fatal("086 rewrote historical085 or current084/recording")
			}
			var count int
			if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_progress_history`).Scan(&count) != nil || count != 0 {
				t.Fatal("migration backfilled missing evidence")
			}
			if kind == "down-preserves-084-085" {
				f.migrate(t, 85, true)
				if !bytes.Equal(before, historyLegacyRows(t, f)) {
					t.Fatal("down rewrote original tables")
				}
				f.migrate(t, 86, false)
			}
		})
	}
}
func TestRecordingProgressHistoryMigrationAssociationAndBounds(t *testing.T) {
	for _, kind := range []string{"single-sidecar", "recording-delete-cascades", "position-delete-cascades", "no-current-084-constraint", "rawtext-65537-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			historyStore(t, f)
			switch kind {
			case "single-sidecar":
				if _, err := f.ds.pool.Exec(t.Context(), `INSERT INTO recording_progress_history SELECT * FROM recording_progress_history`); err == nil {
					t.Fatal("duplicate historical row allowed")
				}
			case "recording-delete-cascades":
				f.exec(t, `DELETE FROM recordings WHERE id='progress'`)
			case "position-delete-cascades":
				f.exec(t, `DELETE FROM recording_playback_positions WHERE recording_id='progress'`)
			case "no-current-084-constraint":
				f.exec(t, `UPDATE recordings SET user_id='two' WHERE id='progress'`)
			case "rawtext-65537-denied":
				if _, err := f.ds.pool.Exec(t.Context(), `UPDATE recording_progress_history SET data_text=$1`, strings.Repeat(" ", 65537)); err == nil {
					t.Fatal("SQL text byte budget missing")
				}
			}
			var count int
			if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_progress_history`).Scan(&count) != nil {
				t.Fatal("read actual sidecar count")
			}
			expected := 1
			if strings.HasSuffix(kind, "delete-cascades") {
				expected = 0
			}
			if count != expected {
				t.Fatal("sidecar uniqueness/cascade incorrect")
			}
		})
	}
}
