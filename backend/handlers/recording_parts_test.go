package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/recordings"
)

type capturePartHTTPRepo struct {
	datastore.RecordingRepository
	mu   sync.Mutex
	item models.Recording
}

func (r *capturePartHTTPRepo) Get(_ context.Context, id string) (*models.Recording, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id != r.item.ID {
		return nil, nil
	}
	copy := r.item
	copy.Capture = models.CopyRecordingCapture(copy.Capture)
	return &copy, nil
}

type capturePartProgressWriter struct {
	*httptest.ResponseRecorder
	firstWrite chan struct{}
	once       sync.Once
}

func (w *capturePartProgressWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(data)
	if n > 0 {
		w.once.Do(func() { close(w.firstWrite) })
	}
	return n, err
}

func TestRecordingPartsHTTPGrowingFirstEndsWhenItsPartCloses(t *testing.T) {
	f := newCapturePartHTTPFixture(t)
	f.repo.item.Capture.Parts = f.repo.item.Capture.Parts[:1]
	f.repo.item.Capture.Parts[0].EndedAt = nil
	f.repo.item.Capture.Parts[0].EndReason = ""
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "account-one")
	req := httptest.NewRequest("GET", "/api/live/recordings/recording-one/stream", nil).WithContext(ctx)
	req = mux.SetURLVars(req, map[string]string{"recordingID": "recording-one"})
	w := &capturePartProgressWriter{ResponseRecorder: httptest.NewRecorder(), firstWrite: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		profileaccess.Middleware(f.users.BelongsToAccount)(http.HandlerFunc(f.handler.Stream)).ServeHTTP(w, req)
	}()
	select {
	case <-w.firstWrite:
	case <-ctx.Done():
		<-done
		t.Fatal("active first part never produced its existing bytes")
	}
	// Keep the overall job running and begin a different file. An HTTP
	// response already bound to part1 must finish without reading part2.
	f.repo.mu.Lock()
	closed := time.Now().UTC()
	f.repo.item.Capture.Parts[0].EndedAt = &closed
	f.repo.item.Capture.Parts[0].EndReason = "source_ended"
	f.repo.item.Capture.Parts = append(f.repo.item.Capture.Parts, models.RecordingPart{Index: 2, StartedAt: closed, Bytes: 14})
	f.repo.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		<-done
		t.Fatal("ongoing HTTP response waited for another part's running job")
	}
	if ctx.Err() != nil || w.Code != 200 || w.Body.String() != "FIRST-SECTION" {
		t.Fatalf("first-part response status=%d body=%q context=%v", w.Code, w.Body.String(), ctx.Err())
	}
	if globalStreamTracker.CountForAccount("account-one") != 0 {
		t.Fatal("closed part retained stream tracking")
	}
}

type capturePartHTTPFixture struct {
	handler *RecordingsHandler
	repo    *capturePartHTTPRepo
	root    string
	users   *fakeRecordingUsersProvider
}

func newCapturePartHTTPFixture(t *testing.T) *capturePartHTTPFixture {
	t.Helper()
	tracker := globalStreamTracker
	globalStreamTracker = newTestTracker()
	t.Cleanup(func() { globalStreamTracker = tracker })
	root := t.TempDir()
	dir := filepath.Join(root, "adult", "recording-one")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"part-000001.ts": "FIRST-SECTION", "part-000002.ts": "SECOND-SECTION"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().UTC().Add(-time.Minute)
	closed := at.Add(time.Second)
	repo := &capturePartHTTPRepo{item: models.Recording{ID: "recording-one", UserID: "adult", Status: models.RecordingStatusRunning,
		OutputPath: filepath.Join(root, "untrusted.ts"), Capture: &models.RecordingCapture{Version: 1, Phase: "capturing", Incomplete: true,
			Parts: []models.RecordingPart{{Index: 1, StartedAt: at, EndedAt: &closed, EndReason: "source_ended", Bytes: 13}, {Index: 2, StartedAt: closed, Bytes: 14}}, Gaps: []models.RecordingGap{}}}}
	users := &fakeRecordingUsersProvider{users: []models.User{{ID: "adult", AccountID: "account-one"}, {ID: "other", AccountID: "account-one"}, {ID: "foreign", AccountID: "account-two"}}}
	service := recordings.NewService(repo, "", root)
	return &capturePartHTTPFixture{NewRecordingsHandler(service, users), repo, root, users}
}

func (f *capturePartHTTPFixture) request(t *testing.T, method, query, account, byteRange string, guard func(http.ResponseWriter, *http.Request, string) bool) *httptest.ResponseRecorder {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, account)
	req := httptest.NewRequest(method, "/api/live/recordings/recording-one/stream"+query, nil).WithContext(ctx)
	req = mux.SetURLVars(req, map[string]string{"recordingID": "recording-one"})
	if guard != nil {
		req = profileaccess.WithResourceGuard(req, guard)
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	reply := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		profileaccess.Middleware(f.users.BelongsToAccount)(http.HandlerFunc(f.handler.Stream)).ServeHTTP(reply, req)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		<-done
		t.Fatal("closed or explicitly selected recording part did not end at its own EOF")
	}
	return reply
}

func TestRecordingPartsHTTPSelectedFileRangeHeadAndEOF(t *testing.T) {
	f := newCapturePartHTTPFixture(t)
	for _, tc := range []struct {
		name, method, query, byteRange, body, contentRange string
		status                                             int
	}{
		{"closed_first_while_later_part_running", "GET", "", "", "FIRST-SECTION", "", 200},
		{"explicit_second_active_is_snapshot", "GET", "?part=2", "", "SECOND-SECTION", "", 200},
		{"closed_range", "GET", "?part=1", "bytes=2-5", "RST-", "bytes 2-5/13", 206},
		{"second_range", "GET", "?part=2", "bytes=0-5", "SECOND", "bytes 0-5/14", 206},
		{"head", "HEAD", "?part=1", "", "", "", 200},
		{"head_range", "HEAD", "?part=2", "bytes=0-5", "", "bytes 0-5/14", 206},
		{"out_of_range", "GET", "?part=1", "bytes=100-200", "invalid range: failed to overlap\n", "bytes */13", 416},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := f.request(t, tc.method, tc.query, "account-one", tc.byteRange, nil)
			if r.Code != tc.status || r.Body.String() != tc.body || r.Header().Get("Content-Range") != tc.contentRange {
				t.Fatalf("status=%d body=%q contentRange=%q", r.Code, r.Body.String(), r.Header().Get("Content-Range"))
			}
			if globalStreamTracker.CountForAccount("account-one") != 0 {
				t.Fatal("finished part retained stream tracking")
			}
		})
	}
}

func TestRecordingPartsHTTPRejectsMalformedSelectorsAndForeignAccess(t *testing.T) {
	f := newCapturePartHTTPFixture(t)
	for _, query := range []string{"?part=", "?part=0", "?part=257", "?part=-1", "?part=%2B1", "?part=01", "?part=1.0", "?part=1/2", "?part=1&part=2", "?part=%201"} {
		t.Run(query, func(t *testing.T) {
			if r := f.request(t, "GET", query, "account-one", "", nil); r.Code != 400 {
				t.Fatalf("selector accepted: status=%d", r.Code)
			}
		})
	}
	for _, tc := range []struct{ query, account string }{{"?part=1", "account-two"}, {"?profileId=foreign&part=1", "account-one"}, {"?profileId=foreign&part=1", "account-two"}} {
		if r := f.request(t, "GET", tc.query, tc.account, "", nil); r.Code != 404 {
			t.Fatalf("foreign recording access status=%d", r.Code)
		}
	}
	for _, query := range []string{"?part=1", "?profileId=other&part=1", "?profileId=other"} {
		seen := ""
		r := f.request(t, "HEAD", query, "account-one", "", func(w http.ResponseWriter, _ *http.Request, profile string) bool {
			seen = profile
			http.Error(w, "synthetic locked profile", http.StatusForbidden)
			return false
		})
		if r.Code != 403 || seen != "adult" || strings.Contains(r.Body.String(), "FIRST-SECTION") {
			t.Fatal("resource guard did not enforce the recording's stored profile")
		}
	}
}

func TestRecordingPartsHTTPQuarantinesInvalidMetadataAndOwnedPaths(t *testing.T) {
	for _, mode := range []string{"restored", "version", "duplicate", "oversize", "missing", "symlink", "outside_output_path"} {
		t.Run(mode, func(t *testing.T) {
			f := newCapturePartHTTPFixture(t)
			c := f.repo.item.Capture
			part := filepath.Join(f.root, "adult", "recording-one", "part-000001.ts")
			switch mode {
			case "restored":
				c.Restored = true
			case "version":
				c.Version = 99
			case "duplicate":
				c.Parts = append(c.Parts, c.Parts[0])
			case "oversize":
				c.Gaps = make([]models.RecordingGap, models.MaxRecordingGaps+1)
			case "missing":
				if err := os.Remove(part); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(part); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(f.root, "adult", "recording-one", "part-000002.ts"), part); err != nil {
					t.Fatal(err)
				}
			case "outside_output_path":
				if err := os.WriteFile(f.repo.item.OutputPath, []byte("DO-NOT-SERVE"), 0600); err != nil {
					t.Fatal(err)
				}
				c.Parts[0].StreamPath = "recording:foreign/parts/1"
			}
			r := f.request(t, "GET", "?part=1", "account-one", "", nil)
			if mode == "outside_output_path" {
				if r.Code != 200 || r.Body.String() != "FIRST-SECTION" {
					t.Fatal("untrusted output path or stream path controlled selection")
				}
			} else if r.Code != 404 {
				t.Fatalf("unavailable part status=%d", r.Code)
			}
		})
	}
}
