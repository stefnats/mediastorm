//go:build linux

package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/services/recordings"
)

func historyResumeHTTPConfigure(t *testing.T, f *progressHTTPFixture, strong bool) {
	t.Helper()
	tools := f.base.tools
	if strong {
		tools.FFmpegPath = "/usr/bin/ffmpeg"
		tools.FFprobePath = "/usr/bin/ffprobe"
	}
	f.base.tools = tools
	if err := f.base.service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) { f.calls.Add(1); return tools, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
}
func historyResumeHTTPFresh(t *testing.T, f *progressHTTPFixture, strong bool) {
	t.Helper()
	historyResumeHTTPConfigure(t, f, strong)
	r, release := f.base.request(t)
	defer release()
	ctx, cancel, authorize, ok := f.base.h.guard.recordingDerivedAuthority(httptest.NewRecorder(), r, false)
	if !ok {
		t.Fatal("fresh original source authority missing")
	}
	defer cancel()
	if value, err := f.base.service.PrepareDerived(ctx, f.base.job.ID, authorize); err != nil || value.State != "ready" {
		t.Fatal("fresh actual artifact preparation failed", err)
	}
}

// Synthetic aging models imported, expired DATA from an actual previous capture.
// It does not claim that 24 hours elapsed or mutate/renew any live cache TTL.
func historyResumeHTTPAge(t *testing.T, f *progressHTTPFixture) {
	t.Helper()
	var e struct {
		Version    int             `json:"schemaVersion"`
		Timeline   json.RawMessage `json:"timeline"`
		Mapping    string          `json:"mappingSha256"`
		Position   int64           `json:"positionMs"`
		End        bool            `json:"atDerivedEnd"`
		Revision   int64           `json:"revision"`
		Generation int64           `json:"writerGeneration"`
		Operation  string          `json:"operationId"`
		Manifest   json.RawMessage `json:"inputManifest"`
		Ledger     string          `json:"ledgerSha256"`
		Tool       *string         `json:"toolSha256"`
		Transform  *string         `json:"transformSha256"`
		Output     *string         `json:"outputSha256"`
	}
	if err := json.Unmarshal(progressHTTPHistoryRaw(t, f), &e); err != nil {
		t.Fatal(err)
	}
	var tuple []json.RawMessage
	if json.Unmarshal(e.Timeline, &tuple) != nil || len(tuple) != 12 {
		t.Fatal("invalid actual historical tuple")
	}
	tuple[9] = json.RawMessage(`"2020-01-01T00:00:00Z"`)
	tuple[10] = json.RawMessage(`"2020-01-02T00:00:00Z"`)
	e.Timeline, _ = json.Marshal(tuple)
	digest := sha256.Sum256(e.Timeline)
	e.Mapping = hex.EncodeToString(digest[:])
	raw, _ := json.Marshal(e)
	tx, err := f.base.pin.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for _, table := range []string{"recording_playback_positions", "recording_progress_history"} {
		extra := ""
		args := []any{e.Mapping, f.base.job.ID}
		if table == "recording_progress_history" {
			extra = ",data_text=$3"
			args = append(args, string(raw))
		}
		if _, err = tx.Exec(t.Context(), `UPDATE `+table+` SET mapping_sha256=$1,descriptor_observed_at='2020-01-01',descriptor_expires_at='2020-01-02'`+extra+` WHERE recording_id=$2`, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func historyResumeHTTPSnapshot(t *testing.T, f *progressHTTPFixture, server *httptest.Server) recordingProgressSnapshotData {
	t.Helper()
	code, raw, err := f.request(t, server, http.MethodGet, "/private/progress?includeHistory=true", nil, nil)
	var value recordingProgressSnapshotData
	if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.SchemaVersion != 1 || value.Progress.HistoryState == "" {
		t.Fatal("actual explicit historical snapshot unavailable", err, code)
	}
	return value
}
func TestRecordingProgressHTTPHistoricalResume(t *testing.T) {
	for _, kind := range []string{"actual-aged-history-fresh-media", "script-unverifiable", "revoked-session"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			strong := kind != "script-unverifiable"
			historyResumeHTTPConfigure(t, f, strong)
			old := f.base.generateAndPrepare(t)
			server := f.server(t)
			snapshot := f.snapshot(t, server, nil)
			f.write(t, server, snapshot, datastore.RecordingProgressFromStart, 0, nil)
			snapshot = f.snapshot(t, server, nil)
			f.write(t, server, snapshot, datastore.RecordingProgressTouch, 1000, nil)
			historyResumeHTTPAge(t, f)
			before := progressHTTPHistoryRaw(t, f)
			historyResumeHTTPFresh(t, f, strong)
			snapshot = historyResumeHTTPSnapshot(t, f, server)
			if snapshot.Timeline.ArtifactID == old.ID || snapshot.Progress.PositionState != "conflict" {
				t.Fatal("fresh current mapping did not differ from historical DATA")
			}
			if kind == "revoked-session" {
				if _, err := f.base.pin.store.Pool().Exec(t.Context(), `DELETE FROM sessions WHERE token=$1`, f.base.pin.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			code, raw, err := f.request(t, server, http.MethodPost, "/private/progress", progressHTTPBody(snapshot, datastore.RecordingProgressResumeHistory, 0), nil)
			if kind != "actual-aged-history-fresh-media" {
				expected := 409
				if kind == "revoked-session" {
					expected = 401
				}
				if err != nil || code != expected || !bytes.Equal(before, progressHTTPHistoryRaw(t, f)) {
					t.Fatal("unverifiable/revoked history authorized a claim", err, code)
				}
				if kind == "script-unverifiable" && (snapshot.Progress.HistoryState != "unverifiable" || snapshot.Progress.HistoricalPositionMS != nil) {
					t.Fatal("script output digest became transformation evidence")
				}
				return
			}
			var value recordingProgressWriteData
			if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.Result.PositionMS != 1000 || value.Result.ArtifactID != snapshot.Timeline.ArtifactID || value.Result.Revision != "3" || value.Result.WriterGeneration != "2" {
				t.Fatal("actual original authorized rebind failed", err, code)
			}
			after := f.snapshot(t, server, nil)
			if after.Progress.PositionState != "matching" || after.Progress.PositionMS == nil || *after.Progress.PositionMS != 1000 || after.Timeline.ExpiresAt == nil || after.Timeline.ExpiresAt.Before(time.Now()) {
				t.Fatal("fresh current result did not retain its own TTL/position")
			}
		})
	}
}
func TestRecordingProgressHTTPHistoricalResumeClientData(t *testing.T) {
	for _, kind := range []string{"wrong-position", "extra-history", "extra-output-hash"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			f.base.generateAndPrepare(t)
			server := f.server(t)
			snapshot := f.snapshot(t, server, nil)
			body := progressHTTPBody(snapshot, datastore.RecordingProgressResumeHistory, 0)
			var fields map[string]any
			if json.Unmarshal(body, &fields) != nil {
				t.Fatal("invalid synthetic command")
			}
			switch kind {
			case "wrong-position":
				fields["positionMs"] = 1
			case "extra-history":
				fields["history"] = map[string]any{"compatible": true}
			case "extra-output-hash":
				fields["outputSha256"] = strings.Repeat("a", 64)
			}
			body, _ = json.Marshal(fields)
			before := f.calls.Load()
			code, _, err := f.request(t, server, http.MethodPost, "/private/progress", body, nil)
			if err != nil || code != 400 || f.calls.Load() != before {
				t.Fatal("client candidate/proof reached real source work", err, code)
			}
		})
	}
}

func TestRecordingProgressHistoryQueryCompatibility(t *testing.T) {
	for _, kind := range []string{"default-exact-fields", "opt-in-true", "explicit-false-default", "invalid-history-query"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			f.base.generateAndPrepare(t)
			server := f.server(t)
			target := "/private/progress"
			if kind == "opt-in-true" {
				target += "?includeHistory=true"
			}
			if kind == "explicit-false-default" {
				target += "?includeHistory=false"
			}
			if kind == "invalid-history-query" {
				for _, query := range []string{"", "IncludeHistory=true", "includeHistory=1", "includeHistory=TRUE", "includeHistory=true&includeHistory=false", "includeHistory=%74rue", "includeHistory=", "includeHistory=true&profileId=other", "profileId=other"} {
					before := f.calls.Load()
					code, _, err := f.request(t, server, http.MethodGet, "/private/progress?"+query, nil, nil)
					if err != nil || code != 400 || f.calls.Load() != before {
						t.Fatal("invalid history/auth selector reached source", err, code)
					}
				}
				snapshot := f.snapshot(t, server, nil)
				before := f.calls.Load()
				code, _, err := f.request(t, server, http.MethodPost, "/private/progress?includeHistory=false", progressHTTPBody(snapshot, datastore.RecordingProgressFromStart, 0), nil)
				if err != nil || code != 400 || f.calls.Load() != before {
					t.Fatal("POST accepted presentation query", err, code)
				}
				return
			}
			code, raw, err := f.request(t, server, http.MethodGet, target, nil, nil)
			var value struct {
				Progress map[string]json.RawMessage `json:"progress"`
			}
			if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil {
				t.Fatal("actual compatibility snapshot failed", err, code)
			}
			keys := []string{"validUntil", "recordingId", "incarnation", "artifactId", "mappingSha256", "durationMs", "revision", "writerGeneration", "positionState"}
			expected := len(keys)
			if kind == "opt-in-true" {
				expected++
				if string(value.Progress["historyState"]) != `"absent"` {
					t.Fatal("explicit true did not return history DATA")
				}
			}
			if len(value.Progress) != expected {
				t.Fatal("legacy exact-key client contract changed", len(value.Progress), expected)
			}
			for _, key := range keys {
				if _, ok := value.Progress[key]; !ok {
					t.Fatal("legacy required field missing", key)
				}
			}
		})
	}
}
