//go:build linux

package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRecordingHistoryResumeComparison(t *testing.T) {
	for _, kind := range []string{"fresh-identities-and-output-differ", "missing-tool", "missing-recipe", "changed-input", "changed-input-order", "changed-ledger", "changed-tool", "changed-recipe", "changed-spans", "changed-source-semantics", "exact-endpoint"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			tool, recipe, output := strings.Repeat("e", 64), strings.Repeat("f", 64), strings.Repeat("0", 64)
			e.Tool = &tool
			e.Transform = &recipe
			e.Output = &output
			if kind == "exact-endpoint" {
				e.Position = 4000
				e.End = true
			}
			old := historyDecoded(t, e)
			historyEditTuple(t, &e, func(v *recordingHistoryTuple) {
				v.incarnation = "33333333-3333-4333-8333-333333333333"
				v.artifact = strings.Repeat("1", 32)
				v.outputBytes = 8192
				v.observed = "2021-01-01T00:00:00Z"
				v.expires = "2021-01-02T00:00:00Z"
			})
			newOutput := strings.Repeat("2", 64)
			e.Output = &newOutput
			current := historyDecoded(t, e)
			switch kind {
			case "missing-tool":
				current.envelope.Tool = nil
			case "missing-recipe":
				current.envelope.Transform = nil
			case "changed-input":
				current.envelope.Manifest[0].size++
				if compatibleRecordingHistory(old, current) {
					t.Fatal("changed actual input byte length accepted")
				}
				current.envelope.Manifest[0].size--
				current.envelope.Manifest[0].digest = strings.Repeat("3", 64)
			case "changed-input-order":
				current.envelope.Manifest[0], current.envelope.Manifest[1] = current.envelope.Manifest[1], current.envelope.Manifest[0]
			case "changed-ledger":
				current.envelope.Ledger = strings.Repeat("4", 64)
			case "changed-tool":
				changed := strings.Repeat("5", 64)
				current.envelope.Tool = &changed
			case "changed-recipe":
				changed := strings.Repeat("6", 64)
				current.envelope.Transform = &changed
			case "changed-spans":
				current.tuple.spans[0][2]++
				current.tuple.spans[1][1]++
				current.tuple.spans[1][2]--
			case "changed-source-semantics":
				current.tuple.gaps = false
			}
			want := kind == "fresh-identities-and-output-differ" || kind == "exact-endpoint"
			if compatibleRecordingHistory(old, current) != want {
				t.Fatal("historical DATA comparison accepted insufficient/different semantics or required output identity")
			}
		})
	}
}
func historyResumeFixture(t *testing.T) (*recordingProgressFixture, RecordingProgressHistoryObservation) {
	t.Helper()
	f := historyPG(t)
	_, h := captureHistoryObservation(t, f)
	for _, cmd := range []RecordingProgressCommand{progressCommand(RecordingProgressFromStart, 0, 0, 0), progressCommand(RecordingProgressTouch, 1, 1, 15000)} {
		i := captureHistoryIntent(t, f, f.request(t, t.Context()), cmd, h)
		_, err := f.core.Commit(i)
		i.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return f, h
}
func historyResumeFresh(t *testing.T, f *recordingProgressFixture, h RecordingProgressHistoryObservation) RecordingProgressHistoryObservation {
	t.Helper()
	id, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	f.observation.ArtifactID = strings.Repeat("e", 32)
	f.observation.ObservedAt = time.Now().UTC()
	f.observation.ExpiresAt = f.observation.ObservedAt.Add(24 * time.Hour)
	h.OutputBytes = 8192
	output := strings.Repeat("f", 64)
	h.OutputSHA256 = &output
	tuple := recordingHistoryTuple{"progress", id.Incarnation().String(), f.observation.ArtifactID, f.observation.DurationMS, h.OutputBytes, h.SourceStatus, h.SourceIncomplete, h.GapsOmitted, f.observation.ObservedAt.Format(time.RFC3339Nano), f.observation.ExpiresAt.Format(time.RFC3339Nano), h.Spans}
	raw, err := tuple.bytes()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	f.observation.MappingSHA256 = hex.EncodeToString(digest[:])
	return h
}
func historyResumeRows(t *testing.T, f *recordingProgressFixture) []byte {
	t.Helper()
	var raw []byte
	query := `SELECT jsonb_build_array((SELECT to_jsonb(s) FROM ` + pgx.Identifier{f.schema, "recording_playback_state"}.Sanitize() + ` s WHERE recording_id='progress'),(SELECT to_jsonb(p) FROM ` + pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize() + ` p WHERE recording_id='progress'),(SELECT to_jsonb(h) FROM ` + pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize() + ` h WHERE recording_id='progress'))`
	if err := f.admin.QueryRow(t.Context(), query).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestRecordingHistoryResumeActualTransaction(t *testing.T) {
	for _, kind := range []string{"fresh-claim", "restored-counters", "ordinary-resume-strict", "explicit-from-start", "retired-writer-denied"} {
		t.Run(kind, func(t *testing.T) {
			f, h := historyResumeFixture(t)
			revision, generation := int64(2), int64(1)
			if kind == "restored-counters" {
				positions, err := f.ds.ExportRecordingPlaybackPositions(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				history, err := f.ds.ExportRecordingProgressHistory(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				record := positionBackupRecording(t, f)
				restore, body, finish, guard := positionBackupRestore(t, f)
				positionBackupRecreate(t, restore, body, record)
				if err = restore.RestoreRecordingPlaybackPositions(body, positions); err != nil {
					t.Fatal(err)
				}
				if err = restore.RestoreRecordingProgressHistory(body, history); err != nil {
					t.Fatal(err)
				}
				finish()
				if err = restore.Commit(); err != nil {
					t.Fatal(err)
				}
				if err = restore.Close(); err != nil {
					t.Fatal(err)
				}
				if err = guard.Close(); err != nil {
					t.Fatal(err)
				}
				revision, generation = 0, 0
			}

			if kind == "retired-writer-denied" {
				g := historyGuard(t, f, t.Context())
				state, e := lockRecordingPlaybackState(g.ctx, g.tx, "progress")
				if e != nil {
					t.Fatal(e)
				}
				retired, e := compareAndSwapRecordingPlaybackState(g.ctx, g.tx, state, recordingPlaybackDelete)
				if e != nil {
					t.Fatal(e)
				}
				revision, generation = retired.revision, retired.generation
				if e = g.tx.Commit(g.ctx); e != nil {
					t.Fatal(e)
				}
				if e = g.Close(); e != nil {
					t.Fatal(e)
				}
			}
			h = historyResumeFresh(t, f, h)
			before := historyResumeRows(t, f)
			if kind == "restored-counters" || kind == "retired-writer-denied" {
				var tombstone bool
				var actualRevision, actualGeneration int64
				var operation *string
				if e := f.admin.QueryRow(t.Context(), `SELECT tombstone,revision,writer_generation,last_operation_id::text FROM `+pgx.Identifier{f.schema, "recording_playback_state"}.Sanitize()+` WHERE recording_id='progress'`).Scan(&tombstone, &actualRevision, &actualGeneration, &operation); e != nil || !tombstone || actualRevision != revision || actualGeneration != generation {
					t.Fatal("actual restore/retirement state witness failed", e)
				}
				id, e := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
				if e != nil {
					t.Fatal(e)
				}
				candidate, e := f.core.ReadSnapshotWithHistory(snapshotRequest(t, f, t.Context()), id, f.observation, h)
				if e != nil {
					t.Fatal(e)
				}
				if kind == "restored-counters" {
					if operation != nil || candidate.HistoryState != "compatible" || candidate.HistoricalPositionMS == nil || *candidate.HistoricalPositionMS != 15000 || candidate.Revision != "0" || candidate.WriterGeneration != "0" {
						t.Fatal("fresh genuine restored tombstone did not expose bounded candidate with actual zero counters")
					}
				} else if operation == nil || candidate.HistoryState != "unverifiable" || candidate.HistoricalPositionMS != nil {
					t.Fatal("retired nonzero writer became historical authority")
				}
			}
			action := RecordingProgressResumeHistory
			if kind == "ordinary-resume-strict" {
				action = RecordingProgressResume
			}
			if kind == "explicit-from-start" {
				action = RecordingProgressFromStart
			}
			intent := captureHistoryIntent(t, f, f.request(t, t.Context()), progressCommand(action, revision, generation, 0), h)
			result, err := f.core.Commit(intent)
			intent.Close()
			if kind == "ordinary-resume-strict" || kind == "retired-writer-denied" {
				if !errors.Is(err, ErrRecordingProgressChanged) || !bytes.Equal(before, historyResumeRows(t, f)) {
					t.Fatal("ordinary Resume silently rebound history", err)
				}
				return
			}
			if err != nil {
				t.Fatal("actual historical claim failed", err)
			}
			wantPosition := int64(15000)
			if kind == "explicit-from-start" {
				wantPosition = 0
			}
			expectedRevision, expectedGeneration := "3", "2"
			if kind == "restored-counters" {
				expectedRevision, expectedGeneration = "1", "1"
			}
			data := captureHistoryRead(t, f)
			if result.PositionMS != wantPosition || result.Revision != expectedRevision || result.WriterGeneration != expectedGeneration || result.ArtifactID != f.observation.ArtifactID || data.envelope.Operation != result.OperationID || data.tuple.artifact != result.ArtifactID || data.envelope.Position != wantPosition {
				t.Fatal("historical claim adopted old counters/identity or separated085/086")
			}
		})
	}
}
func TestRecordingHistoryResumeFailures(t *testing.T) {
	for _, kind := range []string{"sidecar-rollback", "held-cancel", "unknown-ack", "copied-intent", "competing-writers"} {
		t.Run(kind, func(t *testing.T) {
			f, h := historyResumeFixture(t)
			h = historyResumeFresh(t, f, h)
			before := historyResumeRows(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			intent := captureHistoryIntent(t, f, f.request(t, ctx), progressCommand(RecordingProgressResumeHistory, 2, 1, 0), h)
			if kind == "sidecar-rollback" {
				f.exec(t, `ALTER TABLE recording_progress_history ADD CONSTRAINT synthetic_resume_reject CHECK(false) NOT VALID`)
				if _, err := f.core.Commit(intent); err == nil {
					t.Fatal("086 failure reported success")
				}
				intent.Close()
				if !bytes.Equal(before, historyResumeRows(t, f)) {
					t.Fatal("086 failure changed084/085")
				}
				return
			}
			if kind == "copied-intent" {
				copy := *intent
				if _, err := f.core.Commit(&copy); err == nil || !bytes.Equal(before, historyResumeRows(t, f)) {
					t.Fatal("copied intent authorized historical claim")
				}
				return
			}
			if kind == "unknown-ack" {
				f.ackEnabled.Store(true)
				result, err := f.core.Commit(intent)
				intent.Close()
				f.ackEnabled.Store(false)
				if !errors.Is(err, ErrRecordingProgressUnknown) || !f.ackDropped.Load() || result.RecordingID != "" {
					t.Fatal("unknown ACK reported success", err)
				}
				data := captureHistoryRead(t, f)
				if data.envelope.Revision != 3 || data.envelope.Generation != 2 || data.envelope.Position != 15000 {
					t.Fatal("actual durable rebind missing")
				}
				if _, err = f.core.Commit(intent); err == nil {
					t.Fatal("unknown claim replayed")
				}
				return
			}
			if kind == "competing-writers" {
				other := captureHistoryIntent(t, f, f.request(t, t.Context()), progressCommand(RecordingProgressResumeHistory, 2, 1, 0), h)
				var wg sync.WaitGroup
				results := make(chan error, 2)
				for _, i := range []*RecordingProgressIntent{intent, other} {
					wg.Add(1)
					go func(i *RecordingProgressIntent) { defer wg.Done(); _, e := f.core.Commit(i); results <- e }(i)
				}
				wg.Wait()
				close(results)
				winners := 0
				for err := range results {
					if err == nil {
						winners++
					}
				}
				if winners != 1 {
					t.Fatal("actual current CAS did not have exactly one winner", winners)
				}
				return
			}
			lock, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = lock.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				recordingStateRollback(lock)
				t.Fatal(err)
			}
			done := make(chan error, 1)
			joined := make(chan struct{})
			t.Cleanup(func() { cancel(); recordingStateRollback(lock); <-joined; intent.Close() })
			go func() { defer close(joined); _, e := f.core.Commit(intent); done <- e }()
			f.waiting(t, "data_text", true)
			cancel()
			intent.Close()
			if err = <-done; err == nil {
				t.Fatal("cancelled actual historical read reported success")
			}
			<-joined
			recordingStateRollback(lock)
			if !bytes.Equal(before, historyResumeRows(t, f)) {
				t.Fatal("cancelled historical claim changed084/085/086")
			}
		})
	}
}
func TestRecordingHistoryResumeSnapshot(t *testing.T) {
	for _, kind := range []string{"read-only-candidate", "legacy-output-not-proof"} {
		t.Run(kind, func(t *testing.T) {
			f, h := historyResumeFixture(t)
			if kind == "legacy-output-not-proof" {
				data := captureHistoryRead(t, f)
				data.envelope.Tool = nil
				data.envelope.Transform = nil
				f.exec(t, `UPDATE recording_progress_history SET data_text=$1`, string(historyEncoded(t, data.envelope)))
			}
			h = historyResumeFresh(t, f, h)
			before := historyResumeRows(t, f)
			id, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
			if err != nil {
				t.Fatal(err)
			}
			value, err := f.core.ReadSnapshotWithHistory(snapshotRequest(t, f, t.Context()), id, f.observation, h)
			if err != nil || value.PositionState != "conflict" || value.Revision != "2" || value.WriterGeneration != "1" || !bytes.Equal(before, historyResumeRows(t, f)) {
				t.Fatal("candidate read changed durable state or failed original cleanup", err)
			}
			if kind == "legacy-output-not-proof" {
				if value.HistoryState != "unverifiable" || value.HistoricalPositionMS != nil || value.HistoricalAtDerivedEnd != nil {
					t.Fatal("output digest or legacy flag supplied tool/recipe proof")
				}
			} else if value.HistoryState != "compatible" || value.HistoricalPositionMS == nil || *value.HistoricalPositionMS != 15000 || value.HistoricalAtDerivedEnd == nil || *value.HistoricalAtDerivedEnd {
				t.Fatal("safe bounded historical candidate missing")
			}

			if kind == "read-only-candidate" {
				if i, e := f.core.Prepare(f.request(t, t.Context()), id, f.observation, progressCommand(RecordingProgressResumeHistory, 2, 1, 0)); e == nil || i != nil {
					t.Fatal("legacy Prepare accepted history action without owned current evidence")
				}
				original := captureHistoryRead(t, f)
				check := func(want string) {
					t.Helper()
					value, e := f.core.ReadSnapshotWithHistory(snapshotRequest(t, f, t.Context()), id, f.observation, h)
					if e != nil || value.HistoryState != want || value.HistoricalPositionMS != nil || value.HistoricalAtDerivedEnd != nil {
						t.Fatal("missing/invalid/stale history exposed a candidate", e, value.HistoryState)
					}
				}
				f.exec(t, `UPDATE recording_progress_history SET data_text='{}'`)
				check("unverifiable")
				f.exec(t, `UPDATE recording_progress_history SET data_text=$1,position_ms=position_ms+1`, string(original.raw))
				check("unverifiable")
				f.exec(t, `DELETE FROM recording_progress_history`)
				check("absent")
			}
		})
	}
}
