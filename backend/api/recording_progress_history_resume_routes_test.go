//go:build linux

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"novastream/api/openapi"
	"novastream/internal/datastore"
	"novastream/services/recordings"
)

func TestRecordingProgressHistoryRegisteredContract(t *testing.T) {
	for _, kind := range []string{"strong-resume-contract", "weak-history-contract"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressRoutePG(t, false)
			validator, err := openapi.NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			tools := recordings.DerivedTools{FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe", ConfigurationKey: "synthetic-history-resume"}
			if kind == "weak-history-contract" {
				dir := t.TempDir()
				for _, name := range []string{"ffmpeg", "ffprobe"} {
					if err = os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexec /usr/bin/"+name+" \"$@\"\n"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				tools.FFmpegPath = filepath.Join(dir, "ffmpeg")
				tools.FFprobePath = filepath.Join(dir, "ffprobe")
			}
			configure := func() {
				t.Helper()
				if err = f.service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) { return tools, ctx.Err() }); err != nil {
					t.Fatal(err)
				}
			}
			configure()
			f.prepare(t)
			// Default GET must remain acceptable to exact-key native clients.
			_, legacyRaw := f.call(t, http.MethodGet, f.path(), f.token, nil, nil)
			var legacy struct {
				Progress map[string]json.RawMessage `json:"progress"`
			}
			if json.Unmarshal(legacyRaw, &legacy) != nil || len(legacy.Progress) != 9 || legacy.Progress["historyState"] != nil {
				t.Fatal("registered default GET changed native response shape")
			}

			perform := func(method string, body []byte, want int) []byte {
				t.Helper()
				target := f.path()
				if method == http.MethodGet {
					target += "?includeHistory=true"
				}
				request := httptest.NewRequest(method, "http://synthetic.invalid"+target, bytes.NewReader(body))
				request.Header.Set("Authorization", "Bearer "+f.token)
				if method == http.MethodPost {
					request.Header.Set("Content-Type", "application/json")
				}
				if e := validator.ValidateRequest(request, body); e != nil {
					t.Fatal("new action outside actual OpenAPI request contract", e)
				}
				code, raw := f.call(t, method, target, f.token, body, nil)
				if code != want {
					t.Fatal("actual registered history route", code, want)
				}
				header := http.Header{"Content-Type": []string{"application/json"}}
				if code != 200 {
					header.Set("Content-Type", "text/plain; charset=utf-8")
				}
				if e := validator.ValidateResponse(request, code, header, raw); e != nil {
					t.Fatal("actual response outside OpenAPI contract", e)
				}
				return raw
			}
			var snapshot recordingProgressRouteRead
			if json.Unmarshal(perform(http.MethodGet, nil, 200), &snapshot) != nil {
				t.Fatal("actual snapshot malformed")
			}
			var written struct {
				Result datastore.RecordingProgressResult `json:"result"`
			}
			if json.Unmarshal(perform(http.MethodPost, recordingProgressRouteBody(snapshot, "from-start", 0), 200), &written) != nil {
				t.Fatal("actual claim malformed")
			}
			snapshot = f.snapshot(t)
			perform(http.MethodPost, recordingProgressRouteBody(snapshot, "touch", 100), 200)
			configure()
			code, _ := f.call(t, http.MethodPost, "/api/live/recordings/"+f.job.ID+"/derived/join", f.token, nil, nil)
			if code != 200 {
				t.Fatal("fresh registered preparation", code)
			}
			if json.Unmarshal(perform(http.MethodGet, nil, 200), &snapshot) != nil {
				t.Fatal("fresh snapshot malformed")
			}
			if snapshot.Progress.PositionState != "conflict" {
				t.Fatal("fresh artifact lost ordinary mapping conflict")
			}
			expected := 200
			if kind == "weak-history-contract" {
				expected = 409
				if snapshot.Progress.HistoryState != "unverifiable" || snapshot.Progress.HistoricalPositionMS != nil {
					t.Fatal("weak candidate contract wrong")
				}
			} else if snapshot.Progress.HistoryState != "compatible" || snapshot.Progress.HistoricalPositionMS == nil || *snapshot.Progress.HistoricalPositionMS != 100 {
				t.Fatal("strong candidate missing")
			}
			raw := perform(http.MethodPost, recordingProgressRouteBody(snapshot, "resume-history", 0), expected)
			if expected == 200 && (json.Unmarshal(raw, &written) != nil || written.Result.PositionMS != 100 || written.Result.Revision != "3" || written.Result.WriterGeneration != "2") {
				t.Fatal("registered history claim did not use actual current CAS/position")
			}
		})
	}
}
