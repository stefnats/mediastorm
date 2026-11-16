package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/recordings"
)

func timingHTTPRequest(t *testing.T, f *exportHTTPFixture, refresh bool) *http.Request {
	t.Helper()
	r := f.request(t)
	r = mux.SetURLVars(r, map[string]string{"recordingID": "recording-one", "partIndex": "1"})
	r.URL.Path = "/api/live/recordings/recording-one/parts/1/timing"
	r.URL.RawQuery = "profileId=adult"
	if refresh {
		r.Method = http.MethodPost
		r.URL.Path += "/refresh"
	}
	return r
}
func TestRecordingPartTimingActualHTTPServiceAndFD(t *testing.T) {
	f := newExportHTTPFixture(t)
	service := f.h.service.(*recordings.Service)
	service.SetPartTimingProber("/usr/bin/ffprobe")
	cmd := exec.Command("/usr/bin/ffmpeg", "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-t", "2", "-c:v", "libx264", "-preset", "ultrafast", "-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "adult", "recording-one", "part-000001.ts")
	if err = os.WriteFile(path, media, 0600); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	f.h.GetPartTiming(w, timingHTTPRequest(t, f, false))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"unknown"`) {
		t.Fatalf("passive %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	f.h.RefreshPartTiming(w, timingHTTPRequest(t, f, true))
	if w.Code != 200 {
		t.Fatalf("inspect %d %s", w.Code, w.Body.String())
	}
	var got recordings.PartTiming
	if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != "container_estimate" || got.ContainerDurationMS == nil || *got.ContainerDurationMS != 2000 {
		t.Fatalf("actual FD estimate %+v", got)
	}
	if bytes.Contains(w.Body.Bytes(), []byte(path)) || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("private/non-cache contract failed")
	}
	original := append([]byte(nil), w.Body.Bytes()...)
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	f.h.GetPartTiming(w, timingHTTPRequest(t, f, false))
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), original) {
		t.Fatal("GET renewed/probed file")
	}
	f.repo.mu.Lock()
	f.repo.item.Capture = models.CopyRecordingCapture(f.repo.item.Capture)
	f.repo.item.Capture.Incomplete = true
	f.repo.mu.Unlock()
	w = httptest.NewRecorder()
	f.h.GetPartTiming(w, timingHTTPRequest(t, f, false))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reason":"ledger_changed"`) {
		t.Fatal("stale ledger fact reused")
	}
}
func TestRecordingPartTimingHTTPRejectsInvalidOperationBeforeProbe(t *testing.T) {
	for _, mode := range []string{"index0", "index257", "leading zero", "negative", "range", "body", "chunked", "no guard", "child", "scoped", "PIN"} {
		t.Run(mode, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			service := f.h.service.(*recordings.Service)
			marker := filepath.Join(t.TempDir(), "called")
			script := filepath.Join(t.TempDir(), "probe")
			if err := os.WriteFile(script, []byte("#!/bin/sh\necho called > "+strconv.Quote(marker)+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			service.SetPartTimingProber(script)
			r := timingHTTPRequest(t, f, true)
			want := 400
			switch mode {
			case "index0":
				r = mux.SetURLVars(r, map[string]string{"recordingID": "recording-one", "partIndex": "0"})
			case "index257":
				r = mux.SetURLVars(r, map[string]string{"recordingID": "recording-one", "partIndex": "257"})
			case "leading zero":
				r = mux.SetURLVars(r, map[string]string{"recordingID": "recording-one", "partIndex": "01"})
			case "negative":
				r = mux.SetURLVars(r, map[string]string{"recordingID": "recording-one", "partIndex": "-1"})
			case "range":
				r.Header.Set("Range", "bytes=0-1")
				want = 416
			case "body":
				r.ContentLength = 1
			case "chunked":
				r.TransferEncoding = []string{"chunked"}
				r.ContentLength = -1
			case "no guard":
				f.h.SetRecordingStreamAccess(nil)
				want = 404
			case "child":
				p := f.access.profiles["adult"]
				p.IsKidsProfile = true
				f.access.profiles[p.ID] = p
				want = 403
			case "scoped":
				f.access.session.Scope = models.SessionScopeStream
				f.access.session.ScopeResource = "recording:recording-one"
				r = r.WithContext(f.access.request(t, "").Context())
				want = 404
			case "PIN":
				f.access.enforce = true
				p := f.access.profiles["adult"]
				p.PinHash = "synthetic-hash"
				f.access.profiles[p.ID] = p
				want = 403
			}
			w := httptest.NewRecorder()
			f.h.RefreshPartTiming(w, r)
			if w.Code != want {
				t.Fatalf("%s: %d %s", mode, w.Code, w.Body.String())
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("denied request invoked process")
			}
		})
	}
}
func TestRecordingPartTimingHTTPOriginalAuthorityDuringActualProcess(t *testing.T) {
	for _, mode := range []string{"session revoked", "original deadline", "parent cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			service := f.h.service.(*recordings.Service)
			marker := filepath.Join(t.TempDir(), "started")
			script := filepath.Join(t.TempDir(), "probe")
			if err := os.WriteFile(script, []byte("#!/bin/sh\necho started > "+strconv.Quote(marker)+"\nexec sleep 60\n"), 0700); err != nil {
				t.Fatal(err)
			}
			service.SetPartTimingProber(script)
			var retired atomic.Bool
			base := f.access.session
			if mode == "original deadline" {
				base.ExpiresAt = time.Now().Add(300 * time.Millisecond)
				f.access.session = base
			}
			f.access.guard.sessions = recordingSessionFunc(func(ctx context.Context, _ string) (*models.Session, error) {
				if retired.Load() {
					return nil, errors.New("retired")
				}
				s := base
				return &s, ctx.Err()
			})
			r := timingHTTPRequest(t, f, true)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { f.h.RefreshPartTiming(w, r); close(done) }()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatal("process never started")
			}
			switch mode {
			case "session revoked":
				retired.Store(true)
			case "parent cancel":
				cancel()
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("actual process not cancelled/joined")
			}
			if w.Code != 403 || strings.Contains(w.Body.String(), "container_estimate") {
				t.Fatalf("retired output: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func recordingPartTimingOpenAPIContract(t *testing.T, v *openapi.Validator) {
	for _, refresh := range []bool{false, true} {
		t.Run(strconv.FormatBool(refresh), func(t *testing.T) {
			f := newExportHTTPFixture(t)
			r := timingHTTPRequest(t, f, refresh)
			if err := v.ValidateRequest(r, nil); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			if refresh {
				f.h.RefreshPartTiming(w, r)
			} else {
				f.h.GetPartTiming(w, r)
			}
			if w.Code != 200 {
				t.Fatalf("status %d", w.Code)
			}
			if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRecordingPartTimingOpenAPI(t *testing.T) {
	v, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recordingPartTimingOpenAPIContract(t, v)
}
