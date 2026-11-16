package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"golang.org/x/time/rate"
	"novastream/api"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
	user_settings "novastream/services/user_settings"
	"novastream/services/users"
)

type recordingWebRepo struct {
	datastore.RecordingRepository
	mu   sync.Mutex
	jobs map[string]models.Recording
}

func (r *recordingWebRepo) Get(ctx context.Context, id string) (*models.Recording, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return nil, recordings.ErrRecordingNotFound
	}
	job.Capture = models.CopyRecordingCapture(job.Capture)
	return &job, ctx.Err()
}
func (r *recordingWebRepo) List(ctx context.Context, filter models.RecordingListFilter) ([]models.Recording, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []models.Recording
	for _, job := range r.jobs {
		if !filter.IncludeAll && filter.UserID != "" && job.UserID != filter.UserID {
			continue
		}
		job.Capture = models.CopyRecordingCapture(job.Capture)
		out = append(out, job)
	}
	return out, ctx.Err()
}

type recordingWebService struct {
	*recordings.Service
	opens       atomic.Int32
	projections atomic.Int32
}

func (s *recordingWebService) Get(id string) (*models.Recording, error) {
	s.projections.Add(1)
	return s.Service.Get(id)
}
func (s *recordingWebService) OpenPartsExport(ctx context.Context, id string, check func() error) (*recordings.PartsExport, error) {
	s.opens.Add(1)
	return s.Service.OpenPartsExport(ctx, id, check)
}

type recordingWebFixture struct {
	handler               http.Handler
	accounts              *accounts.Service
	sessions              *sessions.Service
	profiles              *users.Service
	admin, user           models.Account
	adminToken, userToken string
	locked, open, foreign models.User
	service               *recordingWebService
	repo                  *recordingWebRepo
	verify                http.HandlerFunc
	data                  []byte
	unlocks               *profileunlock.Service
	retire                atomic.Bool
	output                string
	holdMu                sync.Mutex
	holdKind              string
	holdGate              chan struct{}
	waitingPIN            atomic.Bool
	waitingDenied         atomic.Bool
	finished              atomic.Int32
	slow                  atomic.Bool
}

func newRecordingWebFixture(t *testing.T) *recordingWebFixture {
	t.Helper()
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-web-export-admin")
	root := t.TempDir()
	accountService, err := accounts.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountService.GetByUsername("admin")
	if !ok {
		t.Fatal("master missing")
	}
	account, err := accountService.Create("synthetic-web-export-user", "synthetic-account-password")
	if err != nil {
		t.Fatal(err)
	}
	sessionService, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	adminSession, err := sessionService.Create(master.ID, true, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	userSession, err := sessionService.Create(account.ID, false, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	locked, err := profiles.CreateForAccount(account.ID, "Locked archive owner")
	if err != nil {
		t.Fatal(err)
	}
	locked, err = profiles.SetPin(locked.ID, "1234")
	if err != nil {
		t.Fatal(err)
	}
	open, err := profiles.CreateForAccount(account.ID, "Open archive owner")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := profiles.CreateForAccount(master.ID, "Foreign archive owner")
	if err != nil {
		t.Fatal(err)
	}
	unlocks := profileunlock.NewService(nil)
	userHandler := handlers.NewUsersHandler(profiles, accountService)
	userHandler.SetProfileUnlocks(unlocks, func() time.Duration { return time.Hour })
	at := time.Now().UTC().Add(-time.Minute)
	ended := at.Add(time.Second)
	repo := &recordingWebRepo{jobs: map[string]models.Recording{}}
	data := bytes.Repeat([]byte("SYNTHETIC_WEB_EXPORT_MEDIA\n"), 64)
	output := filepath.Join(root, "recordings")
	for _, profile := range []models.User{locked, open, foreign} {
		id := "job-" + profile.ID
		dir := filepath.Join(output, profile.ID, id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for _, index := range []int{1, 3} {
			if err := os.WriteFile(filepath.Join(dir, []string{"", "part-000001.ts", "", "part-000003.ts"}[index]), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		repo.jobs[id] = models.Recording{ID: id, UserID: profile.ID, CreatedAt: at, Title: "Original parts recording", ChannelName: "Synthetic channel", Status: models.RecordingStatusFailed, StartAt: at, EndAt: ended, SourceURL: "PRIVATE_WEB_SOURCE", Error: "PRIVATE_RAW_CAPTURE_ERROR", Capture: &models.RecordingCapture{Version: 1, Phase: "interrupted", Incomplete: true, SourceChanges: 1, RunID: "PRIVATE_WEB_RUN", Parts: []models.RecordingPart{{Index: 1, StartedAt: at, EndedAt: &ended, EndReason: "source_ended"}, {Index: 3, StartedAt: at, EndedAt: &ended, EndReason: "capture_failed"}}, Gaps: []models.RecordingGap{}}}
	}
	second := repo.jobs["job-"+locked.ID]
	second.ID = "second-" + locked.ID
	second.Title = "Second original recording"
	repo.jobs[second.ID] = second
	secondDir := filepath.Join(output, locked.ID, second.ID)
	if err := os.MkdirAll(secondDir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"part-000001.ts", "part-000003.ts"} {
		if err := os.WriteFile(filepath.Join(secondDir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	service := &recordingWebService{Service: recordings.NewService(repo, "must-not-execute", output)}
	recordingHandler := handlers.NewRecordingsHandler(service, profiles)
	recordingHandler.SetRecordingStreamAccess(handlers.NewRecordingStreamAccess(service.Service, sessionService, accountService, profiles, unlocks, func() (config.SecuritySettings, error) { return config.SecuritySettings{EnforceProfilePins: true}, nil }))
	settings := config.NewManager(filepath.Join(root, "settings.json"))
	if err := settings.Save(config.Settings{UI: config.UISettings{OnboardingCompleted: true, AdminWalkthroughDismissed: true}, Security: config.SecuritySettings{EnforceProfilePins: true}}); err != nil {
		t.Fatal(err)
	}
	prefs, err := user_settings.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	adminUI := handlers.NewAdminUIHandler(settings.ConfigPath(), "", nil, profiles, prefs, settings)
	adminUI.SetAccountsService(accountService)
	adminUI.SetSessionsService(sessionService)
	f := &recordingWebFixture{accounts: accountService, sessions: sessionService, profiles: profiles, admin: master, user: account, adminToken: adminSession.Token, userToken: userSession.Token, locked: locked, open: open, foreign: foreign, service: service, repo: repo, verify: userHandler.VerifyPin, data: data, output: output, unlocks: unlocks}
	router := mux.NewRouter()
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", handlers.NewStaticHandler()))
	limiter := api.NewIPRateLimiter(rate.Every(6*time.Second), 10)
	for _, prefix := range []string{"admin", "account"} {
		verify, download := newRecordingWebPOSTHandlers(sessionService, accountService, profiles, func(w http.ResponseWriter, r *http.Request) { f.verify(w, r); f.wait(r.Context(), "pin") }, func(w http.ResponseWriter, r *http.Request) {
			f.holdMu.Lock()
			held := f.holdKind == "denial"
			f.holdMu.Unlock()
			if held && mux.Vars(r)["recordingID"] == "job-"+locked.ID && r.Header.Get("X-Profile-Unlock") == "" {
				reply := httptest.NewRecorder()
				recordingHandler.ExportParts(reply, r)
				f.wait(r.Context(), "denial")
				if r.Context().Err() == nil {
					for key, values := range reply.Header() {
						w.Header()[key] = values
					}
					w.WriteHeader(reply.Code)
					_, _ = w.Write(reply.Body.Bytes())
				}
				return
			}
			recordingHandler.ExportParts(w, r)
		})
		router.HandleFunc("/"+prefix+"/recordings", adminUI.RequireAuth(adminUI.RecordingsPage)).Methods("GET")
		router.HandleFunc("/"+prefix+"/api/users/{userID}/pin/verify", recordingWebOrigin(adminUI.RequireAuth(api.RateLimitHandlerFunc(limiter, verify)))).Methods("POST")
		router.HandleFunc("/"+prefix+"/api/live/recordings/{recordingID}/export", recordingWebOrigin(adminUI.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			defer f.finished.Add(1)
			download(&recordingBrowserWriter{ResponseWriter: w, ctx: r.Context(), slow: &f.slow, retire: func() {
				if f.retire.Swap(false) {
					if _, err := profiles.SetPin(locked.ID, "5678"); err != nil {
						t.Error(err)
					}
				}
			}}, r)
		}))).Methods("POST")
		router.HandleFunc("/"+prefix+"/api/live/recordings", adminUI.RequireAuth(recordingHandler.List)).Methods("GET")
		router.PathPrefix("/" + prefix + "/api/").HandlerFunc(adminUI.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.HasSuffix(r.URL.Path, "/live/recording-rules"):
				_, _ = io.WriteString(w, `{"rules":[]}`)
			case strings.HasSuffix(r.URL.Path, "/live/recordings/plan"):
				_, _ = io.WriteString(w, `{"recordings":[],"projectedRecordings":[]}`)
			case strings.HasSuffix(r.URL.Path, "/live/recording-settings"):
				_, _ = io.WriteString(w, `{"paddingBeforeSeconds":0,"paddingAfterSeconds":0}`)
			case strings.HasSuffix(r.URL.Path, "/live/channels"):
				_, _ = io.WriteString(w, `{"channels":[]}`)
			case strings.HasSuffix(r.URL.Path, "/status"):
				_, _ = io.WriteString(w, `{"defaultPassword":false,"adminWalkthroughDismissed":true}`)
			default:
				_, _ = io.WriteString(w, `[]`)
			}
		}))
	}
	router.HandleFunc("/fixture/info", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"fixture": "synthetic-recording-export-web", "locked": locked.ID, "open": open.ID, "foreign": foreign.ID, "opens": service.opens.Load(), "projections": service.projections.Load(), "waitingPIN": f.waitingPIN.Load(), "waitingDenied": f.waitingDenied.Load(), "finished": f.finished.Load()})
	})
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		token := f.userToken
		if r.URL.Query().Get("role") == "admin" {
			token = f.adminToken
		}
		http.SetCookie(w, &http.Cookie{Name: "strmr_admin_session", Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/hold", func(w http.ResponseWriter, r *http.Request) {
		f.holdMu.Lock()
		defer f.holdMu.Unlock()
		if f.holdGate != nil {
			close(f.holdGate)
		}
		f.holdKind = r.URL.Query().Get("kind")
		f.holdGate = make(chan struct{})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/release", func(w http.ResponseWriter, r *http.Request) {
		f.holdMu.Lock()
		defer f.holdMu.Unlock()
		if f.holdGate != nil {
			close(f.holdGate)
		}
		f.holdGate = nil
		f.holdKind = ""
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/large", func(w http.ResponseWriter, r *http.Request) {
		if err := os.Truncate(filepath.Join(f.output, f.open.ID, "job-"+f.open.ID, "part-000001.ts"), 32<<20); err != nil {
			t.Error(err)
		}
		f.slow.Store(true)
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/retire", func(w http.ResponseWriter, r *http.Request) {
		if err := os.Truncate(filepath.Join(f.output, f.locked.ID, "job-"+f.locked.ID, "part-000001.ts"), 8<<20); err != nil {
			t.Error(err)
		}
		f.retire.Store(true)
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/slots", func(w http.ResponseWriter, r *http.Request) {
		one, e1 := service.Service.OpenPartsExport(r.Context(), "job-"+f.open.ID, func() error { return nil })
		if one != nil {
			defer one.Close()
		}
		two, e2 := service.Service.OpenPartsExport(r.Context(), "job-"+f.open.ID, func() error { return nil })
		if two != nil {
			defer two.Close()
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"free": e1 == nil && e2 == nil})
	})
	f.handler = router
	return f
}
func (f *recordingWebFixture) post(t *testing.T, target, contentType, body, origin, token string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "http://web.example"+target, strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	if token != "" {
		r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: token})
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}
func TestRecordingWebPOSTRejectsUnsafeTransportBeforeExporter(t *testing.T) {
	f := newRecordingWebFixture(t)
	valid := url.Values{"profileId": {f.open.ID}}
	cases := []struct {
		name, body, origin, media string
		status                    int
	}{
		{"missing origin", valid.Encode(), "", "application/x-www-form-urlencoded", 403},
		{"foreign origin", valid.Encode(), "http://foreign.example", "application/x-www-form-urlencoded", 403},
		{"null origin", valid.Encode(), "null", "application/x-www-form-urlencoded", 403},
		{"path origin", valid.Encode(), "http://web.example/path", "application/x-www-form-urlencoded", 403},
		{"duplicate", valid.Encode() + "&profileId=" + f.open.ID, "http://web.example", "application/x-www-form-urlencoded", 400},
		{"raw PIN", valid.Encode() + "&pin=1234", "http://web.example", "application/x-www-form-urlencoded", 400},
		{"unknown", valid.Encode() + "&extra=x", "http://web.example", "application/x-www-form-urlencoded", 400},
		{"oversized", valid.Encode() + "&profileUnlock=" + strings.Repeat("x", 9000), "http://web.example", "application/x-www-form-urlencoded", 400},
		{"JSON", `{"profileId":"` + f.open.ID + `"}`, "http://web.example", "application/json", 400},
		{"foreign owner", url.Values{"profileId": {f.foreign.ID}}.Encode(), "http://web.example", "application/x-www-form-urlencoded", 404},
		{"unknown owner", "profileId=missing", "http://web.example", "application/x-www-form-urlencoded", 404},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := f.service.opens.Load()
			w := f.post(t, "/account/api/live/recordings/job-"+f.open.ID+"/export", c.media, c.body, c.origin, f.userToken)
			if w.Code != c.status || f.service.opens.Load() != before || bytes.Contains(w.Body.Bytes(), f.data) {
				t.Fatalf("unsafe POST status%d opens%d", w.Code, f.service.opens.Load()-before)
			}
		})
	}
}
func TestRecordingWebPINAndHiddenPOSTUseActualCookieGuardAndZIP(t *testing.T) {
	f := newRecordingWebFixture(t)
	for _, prefix := range []string{"admin", "account"} {
		t.Run(prefix, func(t *testing.T) {
			token := f.userToken
			if prefix == "admin" {
				token = f.adminToken
			}
			target := "/" + prefix + "/api/live/recordings/job-" + f.locked.ID + "/export"
			before := f.service.opens.Load()
			locked := f.post(t, target, "application/x-www-form-urlencoded", url.Values{"profileId": {f.locked.ID}}.Encode(), "http://web.example", token)
			if locked.Code != 403 || f.service.opens.Load() != before {
				t.Fatal("locked export reached files")
			}
			invalid := f.post(t, "/"+prefix+"/api/users/"+f.locked.ID+"/pin/verify", "application/json", `{"pin":"wrong"}`, "http://web.example", token)
			if invalid.Code != 401 || strings.Contains(invalid.Body.String(), "unlockToken") {
				t.Fatal("wrong PIN granted")
			}
			verified := f.post(t, "/"+prefix+"/api/users/"+f.locked.ID+"/pin/verify", "application/json", `{"pin":"1234"}`, "http://web.example", token)
			var grant handlers.VerifyPinResponse
			if json.Unmarshal(verified.Body.Bytes(), &grant) != nil || verified.Code != 200 || grant.UnlockToken == "" {
				t.Fatalf("real PIN verify failed %d", verified.Code)
			}
			w := f.post(t, target, "application/x-www-form-urlencoded", url.Values{"profileId": {f.locked.ID}, "profileUnlock": {grant.UnlockToken}}.Encode(), "http://web.example", token)
			if w.Code != 200 || w.Header().Get("Content-Type") != "application/zip" {
				t.Fatalf("download%d %s", w.Code, w.Body.String())
			}
			zipReader, err := zip.NewReader(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if len(zipReader.File) != 3 {
				t.Fatal("archive incomplete")
			}
			for _, file := range zipReader.File {
				if file.Method != zip.Store {
					t.Fatal("archive recompressed")
				}
				r, _ := file.Open()
				raw, _ := io.ReadAll(r)
				r.Close()
				if file.Name != "manifest.json" && !bytes.Equal(raw, f.data) {
					t.Fatal("bytes changed")
				}
			}
			if bytes.Contains(w.Body.Bytes(), []byte("PRIVATE_")) {
				t.Fatal("private data leaked")
			}
		})
	}
}

func TestRecordingWebCurrentRoleAndPINReplyCannotChangeMidVerification(t *testing.T) {
	for _, mode := range []string{"PIN replaced", "session revoked", "role changed", "profile deleted"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecordingWebFixture(t)
			verify, _ := newRecordingWebPOSTHandlers(f.sessions, f.accounts, f.profiles, func(w http.ResponseWriter, r *http.Request) {
				f.verify(w, r)
				switch mode {
				case "PIN replaced":
					if _, err := f.profiles.SetPin(f.locked.ID, "5678"); err != nil {
						t.Fatal(err)
					}
				case "session revoked":
					if err := f.sessions.Revoke(f.adminToken); err != nil {
						t.Fatal(err)
					}
				case "role changed":
					if err := f.accounts.TransferMaster(f.admin.ID, f.user.ID); err != nil {
						t.Fatal(err)
					}
				case "profile deleted":
					if err := f.profiles.Delete(f.locked.ID); err != nil {
						t.Fatal(err)
					}
				}
			}, func(http.ResponseWriter, *http.Request) { t.Fatal("PIN verification invoked export") })
			session, err := f.sessions.SnapshotForAuthorization(context.Background(), f.adminToken)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "http://web.example/admin/api/users/"+f.locked.ID+"/pin/verify", strings.NewReader(`{"pin":"1234"}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Origin", "http://web.example")
			r = mux.SetURLVars(r, map[string]string{"userID": f.locked.ID})
			ctx := context.WithValue(r.Context(), auth.ContextKeySession, *session)
			ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
			w := httptest.NewRecorder()
			verify(w, r.WithContext(ctx))
			if w.Code != 404 || strings.Contains(w.Body.String(), "unlockToken") || strings.Contains(w.Body.String(), "pu_") {
				t.Fatalf("late PIN proof escaped %d", w.Code)
			}
		})
	}
}
func TestRecordingWebRejectsDuplicatePINAndConflictingCredentials(t *testing.T) {
	f := newRecordingWebFixture(t)
	for _, raw := range []string{`{"pin":"wrong","pin":"1234"}`, `{"pin":"1234","extra":true}`, `{"pin":"1234"} {}`, `{"pin":1234}`} {
		w := f.post(t, "/account/api/users/"+f.locked.ID+"/pin/verify", "application/json", raw, "http://web.example", f.userToken)
		if w.Code != 400 || strings.Contains(w.Body.String(), "unlockToken") {
			t.Fatalf("invalid PIN JSON accepted %d", w.Code)
		}
	}
	for _, kind := range []string{"foreign fetch site", "duplicate origin", "authorization header", "unlock header", "query unlock", "missing session", "scoped session", "revoked session", "demoted current actor"} {
		t.Run(kind, func(t *testing.T) {
			token := f.userToken
			profile := f.open.ID
			target := "/account/api/live/recordings/job-" + profile + "/export"
			want := 400
			if kind == "scoped session" {
				s, err := f.sessions.CreateScoped(f.user.ID, false, "synthetic-scope", "127.0.0.1", time.Hour, models.SessionScopeStream)
				if err != nil {
					t.Fatal(err)
				}
				token = s.Token
				want = 403
			}
			if kind == "revoked session" {
				s, err := f.sessions.Create(f.user.ID, false, "synthetic-revoked", "127.0.0.1")
				if err != nil {
					t.Fatal(err)
				}
				token = s.Token
				_ = f.sessions.Revoke(token)
				want = 303
			}
			if kind == "demoted current actor" {
				if err := f.accounts.TransferMaster(f.admin.ID, f.user.ID); err != nil {
					t.Fatal(err)
				}
				token = f.adminToken
				want = 404
			}
			if kind == "missing session" {
				token = ""
				want = 303
			}
			if kind == "foreign fetch site" || kind == "duplicate origin" {
				want = 403
			}
			if kind == "query unlock" {
				target += "?profileUnlock=synthetic"
			}
			r := httptest.NewRequest("POST", "http://web.example"+target, strings.NewReader(url.Values{"profileId": {profile}}.Encode()))
			r.Header.Set("Origin", "http://web.example")
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.Header.Set("Sec-Fetch-Site", "same-origin")
			if token != "" {
				r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: token})
			}
			switch kind {
			case "foreign fetch site":
				r.Header.Set("Sec-Fetch-Site", "same-site")
			case "duplicate origin":
				r.Header.Add("Origin", "http://web.example")
			case "authorization header":
				r.Header.Set("Authorization", "Bearer synthetic")
			case "unlock header":
				r.Header.Set("X-Profile-Unlock", "synthetic")
			}
			before := f.service.opens.Load()
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, r)
			if w.Code != want || f.service.opens.Load() != before {
				t.Fatalf("unsafe transport %s status%d", kind, w.Code)
			}
		})
	}
}

func (f *recordingWebFixture) wait(ctx context.Context, kind string) {
	f.holdMu.Lock()
	gate := f.holdGate
	held := f.holdKind == kind && gate != nil
	f.holdMu.Unlock()
	if !held {
		return
	}
	flag := &f.waitingPIN
	if kind == "denial" {
		flag = &f.waitingDenied
	}
	flag.Store(true)
	defer flag.Store(false)
	select {
	case <-gate:
	case <-ctx.Done():
	}
}

type recordingBrowserWriter struct {
	http.ResponseWriter
	ctx    context.Context
	slow   *atomic.Bool
	retire func()
}

func (w *recordingBrowserWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *recordingBrowserWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if w.retire != nil && strings.HasPrefix(w.Header().Get("Content-Type"), "application/zip") {
		w.retire()
	}
	if w.slow.Load() {
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-w.ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	return n, err
}

func TestRecordingWebOpaqueUnlockExpiryForeignAndRevocation(t *testing.T) {
	f := newRecordingWebFixture(t)
	for _, mode := range []string{"expired", "foreign profile", "other session", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			session := f.userToken
			profile := f.locked.ID
			ttl := time.Hour
			if mode == "expired" {
				ttl = time.Nanosecond
			}
			if mode == "foreign profile" {
				profile = f.foreign.ID
			}
			if mode == "other session" {
				session = f.adminToken
			}
			grant, _, err := f.unlocks.Issue(profileunlock.IssueRequest{SessionToken: session, SessionExpiresAt: time.Now().Add(time.Hour), AccountID: f.user.ID, ProfileID: profile, PinFingerprint: f.profiles.PinFingerprint(profile), TTL: ttl})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "expired" {
				time.Sleep(time.Millisecond)
			}
			if mode == "revoked" {
				f.unlocks.RevokeSession(session)
			}
			before := f.service.opens.Load()
			w := f.post(t, "/account/api/live/recordings/job-"+f.locked.ID+"/export", "application/x-www-form-urlencoded", url.Values{"profileId": {f.locked.ID}, "profileUnlock": {grant}}.Encode(), "http://web.example", f.userToken)
			if w.Code != 403 || f.service.opens.Load() != before || strings.Contains(w.Body.String(), grant) {
				t.Fatalf("invalid opaque grant accepted %s %d", mode, w.Code)
			}
		})
	}
}
func TestRecordingWebPINVerificationAliasesShareRateLimit(t *testing.T) {
	f := newRecordingWebFixture(t)
	for i := 0; i < 11; i++ {
		prefix := "admin"
		if i%2 != 0 {
			prefix = "account"
		}
		raw := `{"pin":1234}`
		want := 400
		if i == 0 {
			raw = `{"pin":"wrong"}`
			want = 401
		}
		if i == 10 {
			want = 429
		}
		// One real bcrypt check plus fast invalid bodies keeps this limiter
		// routing control below its real six-second token refill under -race.
		w := f.post(t, "/"+prefix+"/api/users/"+f.locked.ID+"/pin/verify", "application/json", raw, "http://web.example", f.userToken)
		if w.Code != want {
			t.Fatalf("shared PIN attempt%d status%d", i, w.Code)
		}
	}
}

func TestRecordingWebRejectsUnlockCollectionBeforeExporter(t *testing.T) {
	f := newRecordingWebFixture(t)
	verified := f.post(t, "/account/api/users/"+f.locked.ID+"/pin/verify", "application/json", `{"pin":"1234"}`, "http://web.example", f.userToken)
	var grant handlers.VerifyPinResponse
	if verified.Code != 200 || json.Unmarshal(verified.Body.Bytes(), &grant) != nil || grant.UnlockToken == "" {
		t.Fatal("actual PIN control did not produce one grant")
	}
	before := f.service.opens.Load()
	response := f.post(t, "/account/api/live/recordings/job-"+f.locked.ID+"/export", "application/x-www-form-urlencoded", url.Values{"profileId": {f.locked.ID}, "profileUnlock": {"unrelated-opaque," + grant.UnlockToken}}.Encode(), "http://web.example", f.userToken)
	if response.Code != 400 || f.service.opens.Load() != before {
		t.Fatalf("single-token protocol accepted a collection: status%d exporterCalls%d", response.Code, f.service.opens.Load()-before)
	}
}
