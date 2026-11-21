//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"novastream/internal/datastore"
	"novastream/services/recordings"
)

func progressHTTPHistoryRaw(t *testing.T, f *progressHTTPFixture) []byte {
	t.Helper()
	var raw string
	if err := f.base.pin.store.Pool().QueryRow(t.Context(), `SELECT data_text FROM recording_progress_history WHERE recording_id=$1`, f.base.job.ID).Scan(&raw); err != nil {
		t.Fatal("actual history row", err)
	}
	return []byte(raw)
}
func TestRecordingProgressHTTPHistoryCapture(t *testing.T) {
	for _, kind := range []string{"actual-service-to-sidecar", "fresh-artifact-conflict"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			descriptor := f.base.generateAndPrepare(t)
			server := f.server(t)
			snapshot := f.snapshot(t, server, nil)
			result := f.write(t, server, snapshot, datastore.RecordingProgressFromStart, 0, nil)
			before := progressHTTPHistoryRaw(t, f)
			if kind == "actual-service-to-sidecar" {
				var data struct {
					Timeline   []json.RawMessage   `json:"timeline"`
					Mapping    string              `json:"mappingSha256"`
					Operation  string              `json:"operationId"`
					Position   int64               `json:"positionMs"`
					Revision   int64               `json:"revision"`
					Generation int64               `json:"writerGeneration"`
					Manifest   [][]json.RawMessage `json:"inputManifest"`
					Ledger     string              `json:"ledgerSha256"`
					Tool       *string             `json:"toolSha256"`
					Transform  *string             `json:"transformSha256"`
					Output     *string             `json:"outputSha256"`
				}
				if json.Unmarshal(before, &data) != nil || len(data.Timeline) != 12 || data.Mapping != snapshot.Timeline.MappingRevision || data.Operation != result.OperationID || data.Revision != 1 || data.Generation != 1 || data.Position != 0 || len(data.Manifest) != 2 || len(data.Ledger) != 64 || data.Tool != nil || data.Transform != nil || data.Output == nil {
					t.Fatal("actual registered POST did not persist truthful script-tool history DATA")
				}
				var artifact string
				if json.Unmarshal(data.Timeline[3], &artifact) != nil || artifact != descriptor.ID {
					t.Fatal("historical mapping did not come from actual service entry")
				}
				for _, secret := range []string{f.base.root, f.base.dir, f.base.tools.FFmpegPath, f.base.pin.session.Token, "/proc/self/fd/"} {
					if bytes.Contains(before, []byte(secret)) {
						t.Fatal("historical DATA leaked paths or authority")
					}
				}
				return
			}
			// Prepare a real fresh artifact under newly obtained native authority. Its
			// input bytes can match; its new artifact identity must still conflict.
			tools := f.base.tools
			if err := f.base.service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) { return tools, ctx.Err() }); err != nil {
				t.Fatal(err)
			}
			r, release := f.base.request(t)
			defer release()
			ctx, cancel, authorize, ok := f.base.h.guard.recordingDerivedAuthority(httptest.NewRecorder(), r, false)
			if !ok {
				t.Fatal("fresh original authority missing")
			}
			defer cancel()
			next, err := f.base.service.PrepareDerived(ctx, f.base.job.ID, authorize)
			if err != nil || next.ID == descriptor.ID {
				t.Fatal("fresh artifact was not prepared", err)
			}
			code, _, err := f.request(t, server, http.MethodPost, "/private/progress", progressHTTPBody(snapshot, datastore.RecordingProgressResume, 0), nil)
			if err != nil || code != http.StatusConflict || !bytes.Equal(before, progressHTTPHistoryRaw(t, f)) {
				t.Fatal("fresh artifact silently rebound historical progress", err, code)
			}
		})
	}
}
func TestRecordingProgressHTTPHistoryRejectsClientData(t *testing.T) {
	for _, kind := range []string{"history-field", "tool-hash-field", "revoked-original-session"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			f.base.generateAndPrepare(t)
			server := f.server(t)
			snapshot := f.snapshot(t, server, nil)
			raw := progressHTTPBody(snapshot, datastore.RecordingProgressFromStart, 0)
			if kind == "revoked-original-session" {
				if _, err := f.base.pin.store.Pool().Exec(t.Context(), `DELETE FROM sessions WHERE token=$1`, f.base.pin.session.Token); err != nil {
					t.Fatal(err)
				}
			} else {
				var body map[string]any
				if json.Unmarshal(raw, &body) != nil {
					t.Fatal("synthetic request malformed")
				}
				if kind == "history-field" {
					body["history"] = map[string]any{"ledgerSha256": strings.Repeat("a", 64)}
				} else {
					body["toolSha256"] = strings.Repeat("b", 64)
				}
				raw, _ = json.Marshal(body)
			}
			before := f.calls.Load()
			code, _, err := f.request(t, server, http.MethodPost, "/private/progress", raw, nil)
			expected := http.StatusBadRequest
			if kind == "revoked-original-session" {
				expected = http.StatusUnauthorized
			}
			if err != nil || code != expected || f.calls.Load() != before {
				t.Fatal("untrusted DATA/session reached source work", err, code)
			}
			var count int
			if err = f.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM recording_progress_history WHERE recording_id=$1`, f.base.job.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("untrusted client persisted history", err)
			}
		})
	}
}
