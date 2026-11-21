//go:build linux

package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func captureHistoryObservation(t *testing.T, f *recordingProgressFixture) (RecordingTimelineIdentity, RecordingProgressHistoryObservation) {
	t.Helper()
	id, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("c", 64)
	h := RecordingProgressHistoryObservation{SourceStatus: "completed", OutputBytes: 4096, Spans: [][3]int64{{1, 0, 30000}, {2, 30000, 30000}}, Inputs: []RecordingProgressHistoryInput{{1, 32, strings.Repeat("a", 64)}, {2, 64, strings.Repeat("b", 64)}}, LedgerSHA256: strings.Repeat("d", 64), ToolSHA256: &digest, TransformSHA256: &digest, OutputSHA256: &digest}
	tuple := recordingHistoryTuple{"progress", id.Incarnation().String(), f.observation.ArtifactID, 60000, 4096, "completed", false, false, f.observation.ObservedAt.UTC().Format(time.RFC3339Nano), f.observation.ExpiresAt.UTC().Format(time.RFC3339Nano), h.Spans}
	raw, err := tuple.bytes()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	f.observation.MappingSHA256 = hex.EncodeToString(sum[:])
	return id, h
}
func captureHistoryIntent(t *testing.T, f *recordingProgressFixture, r *http.Request, command RecordingProgressCommand, h RecordingProgressHistoryObservation) *RecordingProgressIntent {
	t.Helper()
	id, err := f.ds.ReadRecordingTimelineIdentity(r.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := f.core.PrepareWithHistory(r, id, f.observation, command, h)
	if err != nil {
		t.Fatal("actual prepared history intent", err)
	}
	t.Cleanup(intent.Close)
	return intent
}
func captureHistoryRead(t *testing.T, f *recordingProgressFixture) *recordingProgressHistoryData {
	t.Helper()
	var raw string
	if err := f.admin.QueryRow(t.Context(), `SELECT data_text FROM `+pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()+` WHERE recording_id='progress'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	data, err := decodeRecordingProgressHistory([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func captureHistoryEmpty(t *testing.T, f *recordingProgressFixture) {
	t.Helper()
	var revision, positions, history int64
	err := f.admin.QueryRow(t.Context(), `SELECT (SELECT revision FROM `+pgx.Identifier{f.schema, "recording_playback_state"}.Sanitize()+` WHERE recording_id='progress'),(SELECT count(*) FROM `+pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize()+`),(SELECT count(*) FROM `+pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()+`)`).Scan(&revision, &positions, &history)
	if err != nil || revision != 0 || positions != 0 || history != 0 {
		t.Fatal("084/085/086 did not roll back atomically", err, revision, positions, history)
	}
}
func TestRecordingHistoryCaptureAtomicWrites(t *testing.T) {
	for _, kind := range []string{"from-start-touch-resume", "owned-data", "legacy-prepare-unchanged"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			_, h := captureHistoryObservation(t, f)
			if kind == "legacy-prepare-unchanged" {
				f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
				var count int
				if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_progress_history`).Scan(&count) != nil || count != 0 {
					t.Fatal("legacy API invented history evidence")
				}
				return
			}
			intent := captureHistoryIntent(t, f, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0), h)
			if kind == "owned-data" {
				h.Inputs[0].SHA256 = strings.Repeat("f", 64)
				h.Spans[0][2] = 1
				*h.ToolSHA256 = strings.Repeat("e", 64)
			}
			result, err := f.core.Commit(intent)
			intent.Close()
			if err != nil {
				t.Fatal(err)
			}
			initial := captureHistoryRead(t, f)
			if initial.envelope.Operation != result.OperationID || initial.envelope.Revision != 1 || initial.envelope.Generation != 1 || initial.envelope.Position != 0 {
				t.Fatal("history used caller counters or guessed position")
			}
			if kind == "owned-data" {
				if initial.envelope.Manifest[0].digest != strings.Repeat("a", 64) || *initial.envelope.Tool != strings.Repeat("c", 64) || initial.tuple.spans[0][2] != 30000 {
					t.Fatal("caller mutated prepared evidence")
				}
				return
			}
			for _, command := range []RecordingProgressCommand{progressCommand(RecordingProgressTouch, 1, 1, 35000), progressCommand(RecordingProgressResume, 2, 1, 0)} {
				intent = captureHistoryIntent(t, f, f.request(t, t.Context()), command, h)
				result, err = f.core.Commit(intent)
				intent.Close()
				if err != nil {
					t.Fatal(err)
				}
				data := captureHistoryRead(t, f)
				rev, _ := strconv.ParseInt(result.Revision, 10, 64)
				gen, _ := strconv.ParseInt(result.WriterGeneration, 10, 64)
				if data.envelope.Revision != rev || data.envelope.Generation != gen || data.envelope.Operation != result.OperationID || data.envelope.Position != 35000 || !bytes.Equal(data.envelope.Timeline, initial.envelope.Timeline) {
					t.Fatal("durable sidecar did not follow actual same-TX085 position/CAS")
				}
			}
		})
	}
}
func TestRecordingHistoryCaptureDenials(t *testing.T) {
	for _, kind := range []string{"mapping-mismatch", "foreign-frame", "copied-intent", "sidecar-failure-rollback"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			id, h := captureHistoryObservation(t, f)
			r := f.request(t, t.Context())
			if kind == "mapping-mismatch" {
				h.OutputBytes++
				if intent, err := f.core.PrepareWithHistory(r, id, f.observation, progressCommand(RecordingProgressFromStart, 0, 0, 0), h); err == nil || intent != nil {
					t.Fatal("unbound history accepted")
				}
				captureHistoryEmpty(t, f)
				return
			}
			if kind == "foreign-frame" {
				r = httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/progress", nil)
				if intent, err := f.core.PrepareWithHistory(r, id, f.observation, progressCommand(RecordingProgressFromStart, 0, 0, 0), h); err == nil || intent != nil {
					t.Fatal("DATA supplied original authority")
				}
				captureHistoryEmpty(t, f)
				return
			}
			intent := captureHistoryIntent(t, f, r, progressCommand(RecordingProgressFromStart, 0, 0, 0), h)
			if kind == "copied-intent" {
				copy := *intent
				if _, err := f.core.Commit(&copy); err == nil {
					t.Fatal("copied intent authorized write")
				}
				captureHistoryEmpty(t, f)
				return
			}
			f.exec(t, `ALTER TABLE recording_progress_history ADD CONSTRAINT synthetic_capture_reject CHECK(false) NOT VALID`)
			if _, err := f.core.Commit(intent); err == nil {
				t.Fatal("086 failure reported success")
			}
			intent.Close()
			captureHistoryEmpty(t, f)
		})
	}
}
func TestRecordingHistoryCaptureUnknownACK(t *testing.T) {
	t.Run("unknown-ack-no-replay", func(t *testing.T) {
		f := historyPG(t)
		_, h := captureHistoryObservation(t, f)
		intent := captureHistoryIntent(t, f, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0), h)
		f.ackEnabled.Store(true)
		result, err := f.core.Commit(intent)
		intent.Close()
		f.ackEnabled.Store(false)
		if !errors.Is(err, ErrRecordingProgressUnknown) || result.RecordingID != "" || !f.ackDropped.Load() {
			t.Fatal("actual lost COMMIT ACK reported success", err)
		}
		data := captureHistoryRead(t, f)
		if data.envelope.Revision != 1 || data.envelope.Generation != 1 {
			t.Fatal("086 did not commit with085")
		}
		var same bool
		err = f.admin.QueryRow(t.Context(), `SELECT (`+historyPayloadEqual+`) FROM `+pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()+` h JOIN `+pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize()+` p USING(recording_id)`).Scan(&same)
		if err != nil || !same {
			t.Fatal("unknown ACK separated085/086", err)
		}
		if _, err = f.core.Commit(intent); err == nil {
			t.Fatal("unknown operation replayed")
		}
	})
}
func TestRecordingHistoryCaptureCancellation(t *testing.T) {
	t.Run("held-sidecar-cancel-rollback", func(t *testing.T) {
		f := historyPG(t)
		_, h := captureHistoryObservation(t, f)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		intent := captureHistoryIntent(t, f, f.request(t, ctx), progressCommand(RecordingProgressFromStart, 0, 0, 0), h)
		lock, err := f.admin.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lock.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
			recordingStateRollback(lock)
			t.Fatal(err)
		}
		done := make(chan error, 1)
		workDone := make(chan struct{})
		t.Cleanup(func() { cancel(); recordingStateRollback(lock); <-workDone; intent.Close() })
		go func() { defer close(workDone); _, err := f.core.Commit(intent); done <- err }()
		f.waiting(t, "INSERT INTO", true)
		cancel()
		intent.Close()
		if err = <-done; err == nil {
			t.Fatal("cancelled086 write reported success")
		}
		<-workDone
		recordingStateRollback(lock)
		captureHistoryEmpty(t, f)
	})
}
