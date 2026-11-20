//go:build linux

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

type recordingProgressRouteFixture struct {
	*publicPGPinFixture
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	pins        *profileunlock.PGProfilePins
	service     *recordings.Service
	progress    *handlers.RecordingProgressHandler
	job         models.Recording
	root        string
	token       string
	calls       atomic.Int64
	server      *httptest.Server
	settings    config.Settings
	cleanup     func()
	mu          sync.Mutex
	pending     chan struct{}
}

func recordingProgressRoutePG(t *testing.T, absent bool) *recordingProgressRouteFixture {
	t.Helper()
	f := &recordingProgressRouteFixture{publicPGPinFixture: newPublicPGPinFixture(t), root: t.TempDir()}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = f.ds.Pool().Config().ConnConfig.ConnString()
	settings.Security.EnforceProfilePins = true
	if err := f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	f.settings = settings
	objects, err := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.cleanup = sync.OnceFunc(func() {
		if f.server != nil {
			f.server.Close()
		}
		if f.progress != nil {
			f.progress.Close()
		}
		if f.service != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.service.Stop(ctx); err != nil {
				t.Error(err)
			}
		}
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		_ = objects.Close()
	})
	t.Cleanup(f.cleanup)
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.ds.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.pins = profileunlock.NewPGProfilePins(f.ds, f.sessions)
	f.service = recordings.NewService(f.ds.Recordings(), "/usr/bin/ffmpeg", f.root)
	tools := recordings.DerivedTools{FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe", ConfigurationKey: "synthetic-registered-progress"}
	if err = f.service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) { f.calls.Add(1); return tools, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	if !absent {
		f.progress, err = handlers.NewRecordingProgressHandler(f.ds, f.service, f.pins, f.manager, f.security, f.coordinator)
		if err != nil {
			t.Fatal(err)
		}
	}
	accountService, err := accounts.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	access := handlers.NewRecordingStreamAccess(f.service, f.sessions, accountService, f.users, nil, func() (config.SecuritySettings, error) {
		return config.SecuritySettings{EnforceProfilePins: f.enforce.Load()}, nil
	})
	access.SetPGProfilePins(f.pins)
	recordingHandler := handlers.NewRecordingsHandler(f.service, f.users)
	recordingHandler.SetRecordingStreamAccess(access)
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	router := mux.NewRouter()
	Register(router, nil, meta, nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{}, handlers.NewUsersHandler(f.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, recordingHandler, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, accountService, f.sessions, f.users, nil, nil, nil, "", nil,
		WithPGProfilePins(f.pins), WithProfileUnlocks(profileunlock.NewService(f.ds.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: f.enforce.Load()} }), WithRecordingProgress(f.progress))
	f.server = httptest.NewUnstartedServer(router)
	f.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			f.mu.Lock()
			if f.pending != nil {
				close(f.pending)
				f.pending = nil
			}
			f.mu.Unlock()
		}
	}
	f.server.Start()
	session, err := f.sessions.CreateForClient(f.account, false, "synthetic-progress", "loopback", "synthetic-progress-client")
	if err != nil {
		t.Fatal(err)
	}
	f.token = session.Token
	start := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	middle, end := start.Add(800*time.Millisecond), start.Add(1600*time.Millisecond)
	f.job = models.Recording{ID: "synthetic-progress-job", UserID: f.open, Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusCompleted, Title: "Synthetic registered progress", StartAt: start, EndAt: end, CreatedAt: start, UpdatedAt: start, Capture: &models.RecordingCapture{Version: 1, Phase: "completed", RunID: "synthetic-run", Parts: []models.RecordingPart{{Index: 1, StartedAt: start, EndedAt: &middle, Bytes: 1, EndReason: "schedule"}, {Index: 2, StartedAt: middle, EndedAt: &end, Bytes: 1, EndReason: "schedule"}}, Gaps: []models.RecordingGap{}}}
	if err := f.ds.Recordings().Create(t.Context(), &f.job); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *recordingProgressRouteFixture) call(t *testing.T, method, path, token string, raw []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	done := make(chan struct{})
	f.mu.Lock()
	if f.pending != nil {
		f.mu.Unlock()
		t.Fatal("previous actual connection cleanup pending")
	}
	f.pending = done
	f.mu.Unlock()
	req, err := http.NewRequestWithContext(t.Context(), method, f.server.URL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("X-Client-ID", "synthetic-progress-client")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Close = true
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal("actual loopback request", err)
	}
	defer response.Body.Close()
	var value json.RawMessage
	// Read raw text too: denial bodies are deliberately not all JSON.
	data, err := io.ReadAll(io.LimitReader(response.Body, 128<<10))
	if err != nil {
		t.Fatal("actual loopback response", err)
	}
	value = data
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	// StateClosed is after the actual registered handler and every PG/frame
	// defer, not merely after its Content-Length body reached the client.
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual registered handler/frame cleanup not finished")
	}
	return response.StatusCode, value
}
func (f *recordingProgressRouteFixture) path() string {
	return "/api/live/recordings/" + f.job.ID + "/progress"
}
func (f *recordingProgressRouteFixture) prepare(t *testing.T) {
	t.Helper()
	dir := filepath.Join(f.root, f.job.UserID, f.job.ID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for index, color := range []string{"red", "blue"} {
		name := filepath.Join(dir, fmt.Sprintf("part-%06d.ts", index+1))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		out, err := exec.CommandContext(ctx, "/usr/bin/ffmpeg", "-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c="+color+":s=64x64:r=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.8", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-g", "10", "-bf", "0", "-c:a", "aac", "-ac", "1", "-ar", "48000", "-f", "mpegts", name).CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("own generated media: %v %s", err, out)
		}
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		f.job.Capture.Parts[index].Bytes = info.Size()
	}
	if err := f.ds.Recordings().Update(t.Context(), &f.job); err != nil {
		t.Fatal(err)
	}
	code, _ := f.call(t, http.MethodPost, "/api/live/recordings/"+f.job.ID+"/derived/join", f.token, nil, nil)
	if code != 200 {
		t.Fatal("genuine registered preparation", code)
	}
}

type recordingProgressRouteRead struct {
	SchemaVersion int                                 `json:"schemaVersion"`
	Timeline      recordings.DerivedTimelineData      `json:"timeline"`
	Progress      datastore.RecordingProgressSnapshot `json:"progress"`
}

func recordingProgressRouteBody(value recordingProgressRouteRead, action string, position int64) []byte {
	raw, _ := json.Marshal(map[string]any{"action": action, "expectedRevision": value.Progress.Revision, "writerGeneration": value.Progress.WriterGeneration, "incarnation": value.Timeline.Incarnation, "artifactId": value.Timeline.ArtifactID, "mappingSha256": value.Timeline.MappingRevision, "durationMs": value.Timeline.DurationMS, "positionMs": position})
	return raw
}
func (f *recordingProgressRouteFixture) snapshot(t *testing.T) recordingProgressRouteRead {
	t.Helper()
	code, raw := f.call(t, http.MethodGet, f.path(), f.token, nil, nil)
	var value recordingProgressRouteRead
	if code != 200 || json.Unmarshal(raw, &value) != nil || value.SchemaVersion != 1 || value.Timeline.State != "known" || value.Progress.ValidUntil.IsZero() {
		t.Fatalf("actual registered snapshot: %d %s", code, raw)
	}
	return value
}
func TestRecordingProgressRegisteredCurrentPGSuccess(t *testing.T) {
	for _, enforce := range []bool{true, false} {
		f := recordingProgressRoutePG(t, false)
		if !enforce {
			if _, err := f.users.SetPin(f.open, "1357"); err != nil {
				t.Fatal(err)
			}
			f.settings.Security.EnforceProfilePins = false
			if err := f.manager.Save(f.settings); err != nil {
				t.Fatal(err)
			}
			f.enforce.Store(false)
		}
		f.prepare(t)
		value := f.snapshot(t)
		if value.Progress.PositionState != "none" {
			t.Fatal("absent position adopted")
		}
		write := func(action string, position int64) datastore.RecordingProgressResult {
			code, raw := f.call(t, http.MethodPost, f.path(), f.token, recordingProgressRouteBody(value, action, position), nil)
			var result struct {
				SchemaVersion int                               `json:"schemaVersion"`
				Result        datastore.RecordingProgressResult `json:"result"`
			}
			if code != 200 || json.Unmarshal(raw, &result) != nil || result.SchemaVersion != 1 || result.Result.ValidUntil.IsZero() {
				t.Fatalf("actual registered write: %d %s", code, raw)
			}
			value.Progress.Revision = result.Result.Revision
			value.Progress.WriterGeneration = result.Result.WriterGeneration
			return result.Result
		}
		first := write("from-start", 0)
		if first.PositionMS != 0 {
			t.Fatal("start position changed")
		}
		touch := write("touch", 100)
		if touch.PositionMS != 100 {
			t.Fatal("exact derived offset lost")
		}
		value = f.snapshot(t)
		if value.Progress.PositionState != "matching" || value.Progress.PositionMS == nil || *value.Progress.PositionMS != 100 {
			t.Fatal("actual matching position missing")
		}
		fresh, err := f.sessions.CreateForClient(f.account, false, "synthetic-fresh", "loopback", "synthetic-progress-client")
		if err != nil {
			t.Fatal(err)
		}
		f.token = fresh.Token
		value = f.snapshot(t)
		resumed := write("resume", 0)
		if resumed.PositionMS != 100 || resumed.WriterGeneration == touch.WriterGeneration {
			t.Fatal("fresh session did not claim exact existing position")
		}
		// Release the process-wide managed-security owner and join the real
		// server/source/native cleanup before the next policy fixture starts.
		// The same once-owned cleanup remains registered for every failure.
		f.cleanup()
		_ = first
	}
}
func TestRecordingProgressRegisteredCurrentPGDenial(t *testing.T) {
	for _, kind := range []string{"anonymous", "scoped", "child", "foreign", "locked", "pg-unavailable"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressRoutePG(t, false)
			token := f.token
			want := 403
			switch kind {
			case "anonymous":
				token = ""
				want = 401
			case "scoped":
				v, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic-scope", "loopback", time.Hour, models.SessionScopeJellyfin, f.open)
				if err != nil {
					t.Fatal(err)
				}
				token = v.Token
			case "child":
				if _, err := f.users.SetKidsProfile(f.open, true); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				// Recording.Update intentionally does not change user_id. Keep
				// the real owner unchanged and use an actual other-account actor.
				actor, err := f.ds.Accounts().Get(t.Context(), f.other)
				if err != nil || actor == nil || actor.ID != f.other || actor.IsMaster {
					t.Fatal("foreign actor is not a genuine current nonmaster", err)
				}
				stored, err := f.ds.Recordings().Get(t.Context(), f.job.ID)
				if err != nil || stored == nil || stored.UserID != f.open || f.job.UserID != f.open {
					t.Fatal("original recording owner changed before foreign request", err)
				}
				owner, err := f.ds.Users().Get(t.Context(), stored.UserID)
				if err != nil || owner == nil || owner.ID != f.open || owner.AccountID != f.account || owner.AccountID == actor.ID {
					t.Fatal("recording does not have the genuine other-account owner", err)
				}
				foreign, err := f.sessions.CreateForClient(f.other, false, "synthetic-progress-foreign", "loopback", "synthetic-progress-client")
				if err != nil {
					t.Fatal(err)
				}
				current, err := f.ds.Sessions().Get(t.Context(), foreign.Token)
				if err != nil || current == nil || current.Token != foreign.Token || current.AccountID != actor.ID || current.IsMaster || current.Scope != "" || current.ScopeResource != "" || current.ClientID != "synthetic-progress-client" || !current.ExpiresAt.After(time.Now()) {
					t.Fatal("foreign request lacks its genuine current full client session", err)
				}
				token = foreign.Token
				want = 404
			case "locked":
				if _, err := f.users.SetPin(f.open, "1357"); err != nil {
					t.Fatal(err)
				}
			case "pg-unavailable":
				if _, err := f.ds.Pool().Exec(t.Context(), `ALTER TABLE sessions RENAME TO unavailable_recording_progress_sessions`); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, err := f.ds.Pool().Exec(ctx, `ALTER TABLE unavailable_recording_progress_sessions RENAME TO sessions`); err != nil {
						t.Error(err)
					}
				})
				want = 503
			}
			before := f.calls.Load()
			code, raw := f.call(t, http.MethodGet, f.path(), token, nil, nil)
			if code != want || f.calls.Load() != before || bytes.Contains(raw, []byte(`"timeline"`)) {
				t.Fatalf("denial reached source: %s %d %s", kind, code, raw)
			}
			if kind == "foreign" {
				stored, err := f.ds.Recordings().Get(t.Context(), f.job.ID)
				if err != nil || stored == nil || stored.UserID != f.open || f.job.UserID != f.open {
					t.Fatal("foreign denial changed the original stored owner", err)
				}
			}
		})
	}
}
func TestRecordingProgressRegisteredMethodsAndSelectors(t *testing.T) {
	f := recordingProgressRoutePG(t, false)
	// The global cap must preserve the standard empty-body sentinel. This
	// checks framing identity only; actual deadline/source positives use the
	// registered loopback snapshot test, not this recorder.
	empty := httptest.NewRequest(http.MethodGet, "http://synthetic.test/empty", nil)
	if empty.Body != http.NoBody {
		t.Fatal("synthetic empty request lacks the canonical sentinel")
	}
	seen := false
	apiSecurityMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = true
		if r.Body != http.NoBody {
			t.Fatal("global body cap changed the canonical empty sentinel")
		}
	})).ServeHTTP(httptest.NewRecorder(), empty)
	if !seen {
		t.Fatal("empty request did not reach the next handler")
	}
	// Real nonempty GET bodies still pass through the unchanged global cap
	// and must be rejected before GetDerived or any source callback.
	for _, body := range [][]byte{[]byte("x"), bytes.Repeat([]byte("x"), 4097)} {
		before := f.calls.Load()
		code, _ := f.call(t, http.MethodGet, f.path(), f.token, body, nil)
		if code != 400 || f.calls.Load() != before {
			t.Fatal("nonempty registered GET body was accepted or reached source", code)
		}
	}
	for _, kind := range []string{"head", "options", "patch", "query", "range", "duplicate-json", "wrong-case-json"} {
		t.Run(kind, func(t *testing.T) {
			method, path := http.MethodGet, f.path()
			var body []byte
			headers := map[string]string{}
			want := 400
			switch kind {
			case "head":
				method = http.MethodHead
				want = 405
			case "options":
				method = http.MethodOptions
				want = 200
			case "patch":
				method = http.MethodPatch
				want = 405
			case "query":
				path += "?profileId=" + f.open
			case "range":
				headers["Range"] = "bytes=0-1"
			case "duplicate-json":
				method = http.MethodPost
				body = []byte(`{"action":"from-start","action":"resume"}`)
			case "wrong-case-json":
				method = http.MethodPost
				body = []byte(`{"Action":"from-start"}`)
			}
			before := f.calls.Load()
			code, _ := f.call(t, method, path, f.token, body, headers)
			if code != want || f.calls.Load() != before {
				t.Fatalf("method/selector reached source: %s %d", kind, code)
			}
		})
	}
}
func TestRecordingProgressRegisteredMissingRuntime(t *testing.T) {
	f := recordingProgressRoutePG(t, true)
	for _, kind := range []string{"get", "post", "head", "options"} {
		t.Run(kind, func(t *testing.T) {
			method := strings.ToUpper(kind)
			var body []byte
			if method == http.MethodPost {
				body = []byte(`{}`)
			}
			want := 503
			if method == http.MethodHead {
				want = 405
			}
			if method == http.MethodOptions {
				want = 200
			}
			before := f.calls.Load()
			code, raw := f.call(t, method, f.path(), f.token, body, nil)
			if code != want || f.calls.Load() != before || bytes.Contains(raw, []byte("<html")) {
				t.Fatal("missing runtime fell through", kind, code)
			}
		})
	}
}
