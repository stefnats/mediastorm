package handlers

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/recordings"
)

type exportHTTPFixture struct {
	h      *RecordingsHandler
	access *recordingAccessFixture
	repo   *capturePartHTTPRepo
	root   string
}

func newExportHTTPFixture(t *testing.T) *exportHTTPFixture {
	t.Helper()
	a := newRecordingAccessFixture(t)
	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-time.Minute)
	repo := &capturePartHTTPRepo{item: models.Recording{ID: "recording-one", UserID: "adult", CreatedAt: start, Status: models.RecordingStatusCompleted, Capture: &models.RecordingCapture{Version: 1, Phase: "complete", Parts: []models.RecordingPart{{Index: 1, StartedAt: start, EndedAt: &end, EndReason: "schedule"}}, Gaps: []models.RecordingGap{}}}}
	root := t.TempDir()
	dir := filepath.Join(root, "adult", "recording-one")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "part-000001.ts"), bytes.Repeat([]byte("OWNED_MEDIA_"), 50000), 0600); err != nil {
		t.Fatal(err)
	}
	service := recordings.NewService(repo, "must-not-run", root)
	a.guard.owners = service
	h := NewRecordingsHandler(service, nil)
	h.SetRecordingStreamAccess(a.guard)
	return &exportHTTPFixture{h: h, access: a, repo: repo, root: root}
}
func (f *exportHTTPFixture) request(t *testing.T) *http.Request {
	return mux.SetURLVars(f.access.request(t, ""), map[string]string{"recordingID": "recording-one"})
}

type exportMutationResponse struct {
	*httptest.ResponseRecorder
	once     sync.Once
	change   func()
	deadline time.Time
}

func (w *exportMutationResponse) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	w.once.Do(w.change)
	return n, err
}
func (w *exportMutationResponse) SetWriteDeadline(at time.Time) error { w.deadline = at; return nil }
func TestRecordingExportHTTPAbortAfterOriginalAuthorityChanges(t *testing.T) {
	for _, change := range []string{"PIN revision", "policy", "owner", "session", "parent cancel"} {
		t.Run(change, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			r := f.request(t)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			w := &exportMutationResponse{ResponseRecorder: httptest.NewRecorder(), change: func() {
				switch change {
				case "PIN revision":
					p := f.access.profiles["adult"]
					p.PinHash = "replaced"
					f.access.profiles[p.ID] = p
				case "policy":
					f.access.enforce = true
				case "session":
					f.access.session.Token = "revoked"
				case "parent cancel":
					cancel()
				case "owner":
					f.repo.mu.Lock()
					f.repo.item.UserID = "replacement"
					f.repo.mu.Unlock()
				}
			}}
			var caught any
			func() { defer func() { caught = recover() }(); f.h.ExportParts(w, r) }()
			if caught != http.ErrAbortHandler {
				t.Fatalf("partial transfer not aborted: %v", caught)
			}
			if w.Code != 200 || w.Body.Len() == 0 {
				t.Fatal("test did not reach actual ZIP transfer")
			}
			if _, err := zip.NewReader(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len())); err == nil {
				t.Fatal("revoked partial ZIP finalized")
			}
			if bytes.Contains(w.Body.Bytes(), []byte("recording parts export unavailable")) {
				t.Fatal("error appended to archive")
			}
			if !w.deadline.Equal(f.access.session.ExpiresAt) {
				t.Fatal("original session write deadline lost")
			}
		})
	}
}
func TestRecordingExportHTTPOriginalExpiryAndStoredIncomplete(t *testing.T) {
	f := newExportHTTPFixture(t)
	expiry := time.Now().Add(30 * time.Minute)
	a := f.access.accounts["account-one"]
	a.ExpiresAt = &expiry
	f.access.accounts[a.ID] = a
	w := &exportMutationResponse{ResponseRecorder: httptest.NewRecorder(), change: func() {}}
	f.h.ExportParts(w, f.request(t))
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/zip" || !w.deadline.Equal(expiry) {
		t.Fatalf("status/deadline mismatch %d %v", w.Code, w.deadline)
	}
	z, err := zip.NewReader(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range z.File {
		if file.Name == "manifest.json" {
			r, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(r)
			r.Close()
			if !bytes.Contains(raw, []byte(`"incomplete": false`)) {
				t.Fatal("stored capture flag changed")
			}
		}
	}
}
func TestRecordingExportHTTPRejectsRangeAndMissingGuard(t *testing.T) {
	for _, mode := range []string{"Range", "missing guard", "active"} {
		t.Run(mode, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			r := f.request(t)
			want := 416
			switch mode {
			case "Range":
				r.Header.Set("Range", "bytes=0-1")
			case "missing guard":
				f.h.SetRecordingStreamAccess(nil)
				want = 404
			case "active":
				f.repo.item.Status = models.RecordingStatusRunning
				want = 409
			}
			w := httptest.NewRecorder()
			f.h.ExportParts(w, r)
			if w.Code != want || w.Header().Get("Content-Disposition") != "" || bytes.HasPrefix(w.Body.Bytes(), []byte("PK")) {
				t.Fatalf("unsafe denial: %d %q", w.Code, w.Body.String())
			}
		})
	}
}

type exportGetCounter struct {
	*capturePartHTTPRepo
	reads atomic.Int64
}

func (r *exportGetCounter) Get(ctx context.Context, id string) (*models.Recording, error) {
	r.reads.Add(1)
	return r.capturePartHTTPRepo.Get(ctx, id)
}
func TestRecordingExportHTTPQuantifiesLedgerReads(t *testing.T) {
	f := newExportHTTPFixture(t)
	counter := &exportGetCounter{capturePartHTTPRepo: f.repo}
	service := recordings.NewService(counter, "must-not-run", f.root)
	f.h.service = service
	f.access.guard.owners = service
	data := bytes.Repeat([]byte("X"), 1<<20)
	if err := os.WriteFile(filepath.Join(f.root, "adult", "recording-one", "part-000001.ts"), data, 0600); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	f.h.ExportParts(w, f.request(t))
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	reads := counter.reads.Load()
	if reads <= 0 || reads > 512 {
		t.Fatalf("unbounded ledger reads for 1MiB: %d", reads)
	}
	t.Logf("Actual original-authority+ledger Repo.Get calls for 1MiB ZIP Store: %d; repeated checks intentionally retained, no throughput SLO", reads)
}

func recordingExportOpenAPIContract(t *testing.T, v *openapi.Validator) {
	for _, mode := range []string{"closed", "active", "range"} {
		t.Run(mode, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			r := f.request(t)
			r.URL.Path = "/api/live/recordings/recording-one/export"
			r.URL.RawQuery = "profileId=adult"
			want := 200
			if mode == "active" {
				f.repo.item.Status = models.RecordingStatusRunning
				want = 409
			}
			if mode == "range" {
				r.Header.Set("Range", "bytes=0-5")
				want = 416
			}
			if err := v.ValidateRequest(r, nil); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			f.h.ExportParts(w, r)
			if w.Code != want {
				t.Fatalf("status %d", w.Code)
			}
			if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRecordingExportOpenAPI(t *testing.T) {
	v, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recordingExportOpenAPIContract(t, v)
}

type exportActualHTTPWriter struct {
	http.ResponseWriter
	once   sync.Once
	change func()
}

func (w *exportActualHTTPWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *exportActualHTTPWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	w.once.Do(w.change)
	return n, err
}
func TestRecordingExportHTTPActualPartialTransferIsAborted(t *testing.T) {
	f := newExportHTTPFixture(t)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		ctx := context.WithValue(r.Context(), auth.ContextKeyAccountID, f.access.session.AccountID)
		ctx = context.WithValue(ctx, auth.ContextKeySession, f.access.session)
		r = mux.SetURLVars(r.WithContext(ctx), map[string]string{"recordingID": "recording-one"})
		f.h.ExportParts(&exportActualHTTPWriter{ResponseWriter: w, change: func() { f.access.enforce = true }}, r)
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/api/live/recordings/recording-one/export")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	<-done
	if response.StatusCode != 200 || len(body) == 0 || readErr == nil {
		t.Fatalf("partial HTTP did not signal transfer error: status %d, bytes %d, error %v", response.StatusCode, len(body), readErr)
	}
	if _, err := zip.NewReader(bytes.NewReader(body), int64(len(body))); err == nil {
		t.Fatal("partial HTTP finalized archive")
	}
	if bytes.Contains(body, []byte("recording parts export unavailable")) {
		t.Fatal("JSON/text appended after ZIP started")
	}
}
