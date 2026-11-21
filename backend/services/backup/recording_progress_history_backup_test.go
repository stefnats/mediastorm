//go:build linux

package backup

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
)

// This fixture constructs canonical historical DATA from actual saved085
// scalars. The controlled Core descriptor remains DATA, not GetDerived access.
func historyServiceSeed(t *testing.T, f *positionServiceFixture) datastore.RecordingProgressHistoryBackupRows {
	t.Helper()
	positions := positionServiceRows(t, f)
	if len(positions) != 1 {
		t.Fatal("real085 missing")
	}
	p := positions[0]
	tuple, err := json.Marshal([12]any{"recording-derived-timeline/v1", p.RecordingID, p.Incarnation, p.ArtifactID, p.DurationMS, int64(4096), "completed", false, false, p.DescriptorObservedAt.UTC().Format(time.RFC3339Nano), p.DescriptorExpiresAt.UTC().Format(time.RFC3339Nano), [][3]int64{{1, 0, 25000}, {2, 25000, 35000}}})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(tuple)
	mapping := hex.EncodeToString(digest[:])
	envelope := struct {
		Version    int             `json:"schemaVersion"`
		Timeline   json.RawMessage `json:"timeline"`
		Mapping    string          `json:"mappingSha256"`
		Position   int64           `json:"positionMs"`
		End        bool            `json:"atDerivedEnd"`
		Revision   int64           `json:"revision"`
		Generation int64           `json:"writerGeneration"`
		Operation  string          `json:"operationId"`
		Manifest   [][3]any        `json:"inputManifest"`
		Ledger     string          `json:"ledgerSha256"`
		Tool       *string         `json:"toolSha256"`
		Transform  *string         `json:"transformSha256"`
		Output     *string         `json:"outputSha256"`
	}{Version: 1, Timeline: tuple, Mapping: mapping, Position: p.PositionMS, End: p.AtDerivedEnd, Revision: p.Revision, Generation: p.WriterGeneration, Operation: p.OperationID, Manifest: [][3]any{{int64(1), int64(32), strings.Repeat("c", 64)}, {int64(2), int64(64), strings.Repeat("d", 64)}}, Ledger: strings.Repeat("e", 64)}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(ctx)
	}()
	if _, err = tx.Exec(t.Context(), `UPDATE recording_playback_positions SET mapping_sha256=$1 WHERE recording_id=$2`, mapping, p.RecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `INSERT INTO recording_progress_history(recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,data_text)VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,$13)`, p.RecordingID, p.Incarnation, p.ArtifactID, mapping, p.DurationMS, p.PositionMS, p.AtDerivedEnd, p.DescriptorObservedAt, p.DescriptorExpiresAt, p.Revision, p.WriterGeneration, p.OperationID, string(raw)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.observation.MappingSHA256 = mapping
	rows, err := f.store.ExportRecordingProgressHistory(t.Context())
	if err != nil || len(rows) != 1 || rows[0].DataJSON != string(raw) {
		t.Fatal("canonical historical fixture rejected", err)
	}
	return rows
}
func historyServiceRows(t *testing.T, f *positionServiceFixture) datastore.RecordingProgressHistoryBackupRows {
	t.Helper()
	rows, err := f.store.ExportRecordingProgressHistory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
func historyServiceTouch(t *testing.T, f *positionServiceFixture, position int64) {
	t.Helper()
	p := positionServiceRows(t, f)
	if len(p) != 1 {
		t.Fatal("actual position missing")
	}
	positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: p[0].Revision, WriterGeneration: p[0].WriterGeneration, PositionMS: position})
}
func TestRecordingHistoryBackupActualZIPRoundtrip(t *testing.T) {
	for _, mode := range []string{"named", "upload", "stale-named", "stale-upload"} {
		t.Run(mode, func(t *testing.T) {
			f := positionServicePG(t, false)
			history := historyServiceSeed(t, f)
			if strings.HasPrefix(mode, "stale-") {
				historyServiceTouch(t, f, 30000)
			}
			positions := positionServiceRows(t, f)
			old := positionServiceIdentity(t, f)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			historyServiceTouch(t, f, 35000)
			if strings.HasSuffix(mode, "named") {
				err = f.service.RestoreBackup(info.Filename)
			} else {
				raw, e := os.ReadFile(filepath.Join(f.service.backupDir, info.Filename))
				if e != nil {
					t.Fatal(e)
				}
				err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "history.zip")
			}
			if err != nil {
				t.Fatal("full history ZIP restore", err)
			}
			after := historyServiceRows(t, f)
			p := positionServiceRows(t, f)
			if len(after) != 1 || after[0] != history[0] || len(p) != 1 || p[0] != positions[0] {
				t.Fatal("085/086 bytes or scalar DATA changed")
			}
			positionServiceFresh(t, f, old)
			if strings.HasPrefix(mode, "stale-") && p[0].PositionMS == 25000 {
				t.Fatal("stale history rewrote current085 payload")
			}
			restoredSnapshotRead(t, f, "conflict")
		})
	}
}

func historyServiceRewriteZIP(t *testing.T, f *positionServiceFixture, filename, kind string) []byte {
	t.Helper()
	reader, err := zip.OpenReader(filepath.Join(f.service.backupDir, filename))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries := make(map[string][]byte)
	for _, file := range reader.File {
		in, e := file.Open()
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(in)
		in.Close()
		if e != nil {
			t.Fatal(e)
		}
		entries[file.Name] = data
	}
	data := entries["database.json"]
	switch kind {
	case "duplicate-top-section":
		data = append([]byte(`{"recordingProgressHistory":[],`), data[1:]...)
	case "casefold-top-alias":
		data = bytes.Replace(data, []byte(`"recordingProgressHistory"`), []byte(`"RecordingProgressHistory"`), 1)
	case "oversized-section":
		data = append(append([]byte(`{"recordingProgressHistory":[`), bytes.Repeat([]byte(" "), datastore.RecordingProgressHistoryBackupByteLimit)...), []byte(`]}`)...)
	default:
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			t.Fatal("actual database member invalid")
		}
		switch kind {
		case "missing-086":
			delete(fields, "recordingProgressHistory")
		case "missing-085-and-086":
			delete(fields, "recordingProgressHistory")
			delete(fields, "recordingPlaybackPositions")
		case "present-null-section":
			fields["recordingProgressHistory"] = json.RawMessage(`null`)
		}
		data, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
	}
	entries["database.json"] = data
	var manifest Manifest
	if json.Unmarshal(entries["manifest.json"], &manifest) != nil {
		t.Fatal("manifest invalid")
	}
	digest := sha256.Sum256(data)
	manifest.Files["database.json"] = hex.EncodeToString(digest[:])
	entries["manifest.json"], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry, e := writer.Create(name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = entry.Write(entries[name]); e != nil {
			t.Fatal(e)
		}
	}
	if writer.Close() != nil {
		t.Fatal("ZIP finalization failed")
	}
	return out.Bytes()
}
func TestRecordingHistoryBackupOldZIPAndInvalidSection(t *testing.T) {
	for _, kind := range []string{"missing-086", "missing-085-and-086", "duplicate-top-section", "casefold-top-alias", "present-null-section", "oversized-section"} {
		t.Run(kind, func(t *testing.T) {
			f := positionServicePG(t, false)
			historyServiceSeed(t, f)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			raw := historyServiceRewriteZIP(t, f, info.Filename, kind)
			historyServiceTouch(t, f, 30000)
			positions := positionServiceRows(t, f)
			history := historyServiceRows(t, f)
			old := positionServiceIdentity(t, f)
			settings, e := os.ReadFile(f.manager.ConfigPath())
			if e != nil {
				t.Fatal(e)
			}
			err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "history-section.zip")
			if strings.HasPrefix(kind, "missing-") {
				if err != nil {
					t.Fatal("legacy absent section rejected", err)
				}
				if len(historyServiceRows(t, f)) != 0 {
					t.Fatal("absent086 fabricated history")
				}
				positionServiceFresh(t, f, old)
				want := "conflict"
				if kind == "missing-085-and-086" {
					want = "none"
				}
				restoredSnapshotRead(t, f, want)
				return
			}
			if err == nil {
				t.Fatal("malformed/duplicate/oversized new section accepted")
			}
			after := positionServiceRows(t, f)
			h := historyServiceRows(t, f)
			current, e := os.ReadFile(f.manager.ConfigPath())
			if e != nil || !bytes.Equal(settings, current) || len(after) != 1 || after[0] != positions[0] || len(h) != 1 || h[0] != history[0] || positionServiceIdentity(t, f) != old {
				t.Fatal("invalid section changed original database/settings")
			}
		})
	}
}

func historyServiceCleanupFailure(t *testing.T, f *positionServiceFixture) {
	t.Helper()
	before, err := os.ReadFile(f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := f.service.reserveRestore()
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Close()
	guard, err := reservation.StoreMutationGuard(f.store.Pool())
	if err != nil {
		t.Fatal(err)
	}
	original, err := f.store.BeginSettingsRestore(t.Context(), guard)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	body, finish, err := original.Repositories()
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err = body.Recordings().Get(original.Context(), "position-recording"); err != nil {
		t.Fatal(err)
	}
	// Restore uses the WriteGuard's own original session-locked backend,
	// not the ordinary runtime pool. Observe its actual completed repository
	// query and require one unique live transaction in this owned database.
	var count int
	var pid uint32
	if err = f.observer.QueryRow(t.Context(), `SELECT count(*),COALESCE(min(pid),0) FROM pg_stat_activity WHERE datname=$1 AND state='idle in transaction' AND query ILIKE '%FROM recordings WHERE id = $1%'`, f.name).Scan(&count, &pid); err != nil || count != 1 || pid == 0 {
		t.Fatal("original backend/TX witness missing", err)
	}
	finish()
	var terminated bool
	if err = f.admin.QueryRow(t.Context(), `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatal("terminate exact owned fixture backend", err)
	}
	if err = finishRecordingHistoryRestore(original, nil); err == nil {
		t.Fatal("actual cleanup error disappeared into service success")
	}
	current, err := os.ReadFile(f.manager.ConfigPath())
	if err != nil || !bytes.Equal(before, current) {
		t.Fatal("failed actual cleanup replaced settings")
	}
}
func historyServiceForeignRead(t *testing.T, f *positionServiceFixture) {
	t.Helper()
	if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('history-foreign','Synthetic history foreign','synthetic');INSERT INTO users(id,account_id,name)VALUES('history-foreign-profile','history-foreign','Foreign');INSERT INTO sessions(token,account_id,client_id,created_at,expires_at)VALUES('synthetic-history-foreign-session','history-foreign','history-foreign-client',now(),now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://synthetic.invalid/history-foreign", nil)
	r.Header.Set("X-Client-ID", "history-foreign-client")
	r, release, err := f.store.AuthenticateProfilePinRequest(r, "synthetic-history-foreign-session", "history-foreign-client")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	id, err := f.store.ReadRecordingTimelineIdentity(r.Context(), "position-recording")
	if err != nil {
		t.Fatal(err)
	}
	beforeP, beforeS := restoredSnapshotRows(t, f)
	if value, e := f.core.ReadSnapshot(r, id, f.observation); e == nil || value.PositionMS != nil {
		t.Fatal("foreign owner received imported position")
	}
	afterP, afterS := restoredSnapshotRows(t, f)
	if !bytes.Equal(beforeP, afterP) || !bytes.Equal(beforeS, afterS) {
		t.Fatal("foreign read changed085/084")
	}
}
func TestRecordingHistoryBackupActualOwnerAndWriter(t *testing.T) {
	for _, kind := range []string{"writer-first", "restore-first", "fresh-owner-conflict", "foreign-owner-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := positionServicePG(t, kind == "foreign-owner-denied")
			history := historyServiceSeed(t, f)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "fresh-owner-conflict" || kind == "foreign-owner-denied" {
				if err = f.service.RestoreBackup(info.Filename); err != nil {
					t.Fatal(err)
				}
				if kind == "fresh-owner-conflict" {
					restoredSnapshotRead(t, f, "conflict")
					historyServiceCleanupFailure(t, f)
					return
				}
				historyServiceForeignRead(t, f)
				original, e := os.ReadFile(f.manager.ConfigPath())
				if e != nil {
					t.Fatal(e)
				}
				current := append(append([]byte(nil), original...), ' ')
				if os.WriteFile(f.manager.ConfigPath(), current, 0600) != nil {
					t.Fatal("distinct own settings")
				}
				f.proxy.armed.Store(true)
				if err = f.service.RestoreBackup(info.Filename); err == nil || !f.proxy.dropped.Load() {
					t.Fatal("actual lost commit ACK became success", err)
				}
				settings, e := os.ReadFile(f.manager.ConfigPath())
				after := historyServiceRows(t, f)
				if e != nil || !bytes.Equal(settings, current) || len(after) != 1 || after[0] != history[0] {
					t.Fatal("unknown ACK renamed settings or lost actual committed086")
				}
				return
			}
			positions := positionServiceRows(t, f)
			intent := positionServiceIntent(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: positions[0].Revision, WriterGeneration: positions[0].WriterGeneration, PositionMS: 30000})
			done := make(chan error, 1)
			joined := false
			release := positionHoldCommit(t, f)
			t.Cleanup(func() {
				release()
				if !joined {
					<-done
				}
			})
			if kind == "writer-first" {
				go func() { _, e := f.core.Commit(intent); done <- e }()
			} else {
				go func() { done <- f.service.RestoreBackup(info.Filename) }()
			}
			positionCommitWaiting(t, f)
			if kind == "writer-first" {
				if !errors.Is(f.service.RestoreBackup(info.Filename), settingscommit.ErrBusy) {
					t.Fatal("writer did not exclude restore")
				}
			} else {
				if _, e := f.core.Commit(intent); e == nil {
					t.Fatal("restore did not exclude original writer")
				}
			}
			release()
			select {
			case err = <-done:
				joined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("actual writer/restore did not join")
			}
			intent.Close()
			got := historyServiceRows(t, f)
			if len(got) != 1 || got[0] != history[0] {
				t.Fatal("writer or restore rewrote historical086")
			}
		})
	}
}
