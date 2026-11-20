//go:build linux

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"novastream/internal/datastore"
)

// These are actual persisted rows, not a DTO reconstructed with default counts.
// Capture them after the new request is authenticated and compare them again
// after ReadSnapshot's acknowledged rollback and joined cleanup have returned.
func restoredSnapshotRows(t *testing.T, f *positionServiceFixture) ([]byte, []byte) {
	t.Helper()
	var positions, state []byte
	if err := f.store.Pool().QueryRow(t.Context(), `SELECT
 COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY recording_id) FROM recording_playback_positions p),'[]'::jsonb)::text,
 COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY recording_id) FROM recording_playback_state s),'[]'::jsonb)::text`).Scan(&positions, &state); err != nil {
		t.Fatal("read actual restored085 and084 rows", err)
	}
	return positions, state
}

func restoredSnapshotRead(t *testing.T, f *positionServiceFixture, want string) {
	t.Helper()
	// The old backup's request, frame and grants are never adopted. Insert a
	// distinct ordinary session only AFTER the complete restore, then call the
	// genuine PG authenticator to issue this request's original private frame.
	const token, client = "synthetic-post-restore-snapshot-session", "post-restore-snapshot-client"
	var created, expires time.Time
	if err := f.store.Pool().QueryRow(t.Context(), `INSERT INTO sessions(token,account_id,client_id,created_at,expires_at,scope,scope_resource)
 VALUES($1,'one',$2,clock_timestamp(),clock_timestamp()+interval '1 hour','','') RETURNING created_at,expires_at`, token, client).Scan(&created, &expires); err != nil || created.IsZero() || !expires.After(time.Now()) {
		t.Fatal("create fresh actual ordinary session after restore", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://synthetic.invalid/position-snapshot", nil)
	request.Header.Set("X-Client-ID", client)
	request, release, err := f.store.AuthenticateProfilePinRequest(request, token, client)
	if err != nil {
		t.Fatal("authenticate fresh normal PG frame after restore", err)
	}
	defer release()
	if len(request.Header.Values("X-Profile-Unlock")) != 0 {
		t.Fatal("fresh read imported an old grant carrier")
	}
	identity, err := f.store.ReadRecordingTimelineIdentity(request.Context(), "position-recording")
	if err != nil {
		t.Fatal("read fresh original084 identity", err)
	}
	var revision, generation int64
	var operation *string
	var tombstone bool
	if err = f.store.Pool().QueryRow(request.Context(), `SELECT revision,writer_generation,last_operation_id::text,tombstone
 FROM recording_playback_state WHERE recording_id='position-recording'`).Scan(&revision, &generation, &operation, &tombstone); err != nil || revision != 0 || generation != 0 || operation != nil || !tombstone {
		t.Fatal("fresh084 real counters/operation missing", err)
	}
	beforePositions, beforeState := restoredSnapshotRows(t, f)
	// This SAME still-current controlled Core DATA descriptor is deliberately
	// reused without retimestamping. It is not a GetDerived/media permission.
	originalObservation := f.observation
	if !originalObservation.ExpiresAt.After(time.Now()) {
		t.Fatal("controlled original DATA descriptor expired before read")
	}
	value, err := f.core.ReadSnapshot(request, identity, originalObservation)
	if err != nil {
		t.Fatal("genuine current snapshot after complete ZIP restore", err)
	}
	if value.RecordingID != "position-recording" || value.Incarnation != identity.Incarnation().String() || value.ArtifactID != originalObservation.ArtifactID || value.MappingSHA256 != originalObservation.MappingSHA256 || value.DurationMS != originalObservation.DurationMS || value.Revision != strconv.FormatInt(revision, 10) || value.WriterGeneration != strconv.FormatInt(generation, 10) || value.PositionState != want || value.PositionMS != nil || value.AtDerivedEnd != nil {
		t.Fatal("snapshot invented a position, default counts or matching restored identity")
	}
	afterPositions, afterState := restoredSnapshotRows(t, f)
	if !bytes.Equal(beforePositions, afterPositions) || !bytes.Equal(beforeState, afterState) || f.observation != originalObservation {
		t.Fatal("snapshot changed actual085 data/084 CAS state or descriptor time")
	}
}

func TestRecordingProgressRestoredZIPSnapshotConflicts(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			f := positionServicePG(t, false)
			historical := positionServiceRows(t, f)
			if len(historical) != 1 || historical[0].PositionMS != 25000 || historical[0].Revision != 2 || historical[0].WriterGeneration != 1 {
				t.Fatal("actual writer-created historical085 fixture missing")
			}
			old := positionServiceIdentity(t, f)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal("create actual ZIP backup", err)
			}
			positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: 2, WriterGeneration: 1, PositionMS: 30000})
			if mode == "named" {
				err = f.service.RestoreBackup(info.Filename)
			} else {
				raw, readErr := os.ReadFile(filepath.Join(f.service.backupDir, info.Filename))
				if readErr != nil {
					t.Fatal(readErr)
				}
				err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "snapshot-positions.zip")
			}
			if err != nil {
				t.Fatal("complete original ZIP restore", err)
			}
			positionServiceFresh(t, f, old)
			afterRestore := positionServiceRows(t, f)
			if len(afterRestore) != 1 || afterRestore[0] != historical[0] || afterRestore[0].Incarnation == positionServiceIdentity(t, f) {
				t.Fatal("restore changed historical payload or adopted old084")
			}
			restoredSnapshotRead(t, f, "conflict")
		})
	}
}

func TestRecordingProgressOldZIPSnapshotNone(t *testing.T) {
	f := positionServicePG(t, false)
	old := positionServiceIdentity(t, f)
	info, err := f.service.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal("create actual source ZIP", err)
	}
	raw := positionRewriteZIP(t, f, info.Filename, func(data map[string]json.RawMessage) { delete(data, "recordingPlaybackPositions") })
	if err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "snapshot-old.zip"); err != nil {
		t.Fatal("complete old ZIP restore", err)
	}
	positionServiceFresh(t, f, old)
	if len(positionServiceRows(t, f)) != 0 {
		t.Fatal("old ZIP fabricated historical positions")
	}
	restoredSnapshotRead(t, f, "none")
}
