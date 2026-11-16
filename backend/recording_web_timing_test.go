package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/services/recordings"
	user_settings "novastream/services/user_settings"
)

type timingWebService struct {
	*recordingWebService
	reads, refreshes atomic.Int32
	finished         atomic.Int32
	mode             atomic.Int32
}

func (s *timingWebService) GetPartTiming(ctx context.Context, id string, index int, check func() error) (recordings.PartTiming, error) {
	s.reads.Add(1)
	value, err := s.Service.GetPartTiming(ctx, id, index, check)
	// Only the explicit transport fixture shortens the actual FD fact's TTL.
	if err == nil && s.mode.Load() == 1 && value.ObservedAt != nil {
		expiry := value.ObservedAt.Add(2 * time.Second)
		value.ExpiresAt = &expiry
	}
	return value, err
}
func (s *timingWebService) RefreshPartTiming(ctx context.Context, id string, index int, check func() error) (recordings.PartTiming, error) {
	s.refreshes.Add(1)
	defer s.finished.Add(1)
	return s.Service.RefreshPartTiming(ctx, id, index, check)
}

type timingWebFixture struct {
	base    *recordingWebFixture
	service *timingWebService
	handler http.Handler
	media   map[int][]byte
	started string
}

func newTimingWebFixture(t *testing.T) *timingWebFixture {
	t.Helper()
	base := newRecordingWebFixture(t)
	media := map[int][]byte{}
	for _, index := range []int{1, 3} {
		data, err := exec.Command("/usr/bin/ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-t", map[int]string{1: "2", 3: "3"}[index], "-c:v", "libx264", "-preset", "ultrafast", "-f", "mpegts", "pipe:1").Output()
		if err != nil {
			t.Fatal(err)
		}
		media[index] = data
	}
	base.repo.mu.Lock()
	for _, job := range base.repo.jobs {
		for index, data := range media {
			if err := os.WriteFile(filepath.Join(base.output, job.UserID, job.ID, map[int]string{1: "part-000001.ts", 3: "part-000003.ts"}[index]), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	base.repo.mu.Unlock()
	service := &timingWebService{recordingWebService: base.service}
	normalVerify := base.verify
	shortVerifier := handlers.NewUsersHandler(base.profiles, base.accounts)
	shortVerifier.SetProfileUnlocks(base.unlocks, func() time.Duration { return 2 * time.Second })
	base.verify = func(w http.ResponseWriter, r *http.Request) {
		if service.mode.Load() == 2 {
			shortVerifier.VerifyPin(w, r)
		} else {
			normalVerify(w, r)
		}
	}
	service.Service.SetPartTimingProber("/usr/bin/ffprobe")
	handler := handlers.NewRecordingsHandler(service, base.profiles)
	handler.SetRecordingStreamAccess(handlers.NewRecordingStreamAccess(service.Service, base.sessions, base.accounts, base.profiles, base.unlocks, func() (config.SecuritySettings, error) { return config.SecuritySettings{EnforceProfilePins: true}, nil }))
	root := filepath.Dir(base.output)
	settings := config.NewManager(filepath.Join(root, "settings.json"))
	prefs, err := user_settings.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler(settings.ConfigPath(), "", nil, base.profiles, prefs, settings)
	admin.SetAccountsService(base.accounts)
	admin.SetSessionsService(base.sessions)
	f := &timingWebFixture{base: base, service: service, media: media, started: filepath.Join(t.TempDir(), "probe-started")}
	router := mux.NewRouter()
	for _, prefix := range []string{"admin", "account"} {
		_, read := newRecordingWebPOSTHandlers(base.sessions, base.accounts, base.profiles, base.verify, handler.GetPartTiming)
		_, inspect := newRecordingWebPOSTHandlers(base.sessions, base.accounts, base.profiles, base.verify, handler.RefreshPartTiming)
		router.HandleFunc("/"+prefix+"/api/live/recordings/{recordingID}/parts/{partIndex}/timing/read", recordingWebOrigin(admin.RequireAuth(read))).Methods("POST")
		router.HandleFunc("/"+prefix+"/api/live/recordings/{recordingID}/parts/{partIndex}/timing/refresh", recordingWebOrigin(admin.RequireAuth(inspect))).Methods("POST")
	}
	router.HandleFunc("/fixture/timing", func(w http.ResponseWriter, r *http.Request) {
		hashes := map[int]string{}
		for index, data := range media {
			sum := sha256.Sum256(data)
			hashes[index] = hex.EncodeToString(sum[:])
		}
		_, err := os.Stat(f.started)
		alive := false
		if raw, e := os.ReadFile(f.started); e == nil {
			if pid, e := strconv.Atoi(strings.TrimSpace(string(raw))); e == nil {
				alive = syscall.Kill(pid, 0) == nil
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"reads": service.reads.Load(), "refreshes": service.refreshes.Load(), "started": err == nil, "alive": alive, "finished": service.finished.Load(), "hashes": hashes})
	})
	router.HandleFunc("/fixture/timing-mode", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("value") {
		case "real":
			service.Service.SetPartTimingProber("/usr/bin/ffprobe")
			service.mode.Store(0)
		case "slow":
			_ = os.Remove(f.started)
			path := filepath.Join(t.TempDir(), "probe")
			if err := os.WriteFile(path, []byte("#!/bin/sh\necho $$ > "+f.started+"\nexec sleep 60\n"), 0700); err != nil {
				t.Error(err)
			}
			service.Service.SetPartTimingProber(path)
		case "invalid":
			service.Service.SetPartTimingProber("must-not-exist")
		case "revoke":
			base.unlocks.RevokeProfile(base.locked.ID)
		case "short-timing":
			service.mode.Store(1)
		case "short-unlock":
			service.mode.Store(2)
		}
		w.WriteHeader(204)
	})
	router.PathPrefix("/").Handler(base.handler)
	f.handler = router
	return f
}
func (f *timingWebFixture) post(t *testing.T, prefix, operation, profile, index, body, origin, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest("POST", "http://recording.test/"+prefix+"/api/live/recordings/job-"+profile+"/parts/"+index+"/timing/"+operation, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", origin)
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: token})
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, request)
	return w
}
func TestRecordingWebTimingActualCookieBridgeAndFD(t *testing.T) {
	f := newTimingWebFixture(t)
	for _, prefix := range []string{"account", "admin"} {
		t.Run(prefix, func(t *testing.T) {
			token := f.base.userToken
			if prefix == "admin" {
				token = f.base.adminToken
			}
			body := url.Values{"profileId": {f.base.open.ID}}.Encode()
			before := f.service.refreshes.Load()
			w := f.post(t, prefix, "read", f.base.open.ID, "1", body, "http://recording.test", token)
			if w.Code != 200 {
				t.Fatalf("passive %d %s", w.Code, w.Body.String())
			}
			if f.service.refreshes.Load() != before {
				t.Fatal("GET auto-probed")
			}
			w = f.post(t, prefix, "refresh", f.base.open.ID, "1", body, "http://recording.test", token)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"containerDurationMs":2000`) {
				t.Fatalf("actual FD %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestRecordingWebTimingBadTransportNoTimingPort(t *testing.T) {
	for _, mode := range []string{"foreign origin", "no origin", "duplicate profile", "unlock collection", "unknown field", "query", "foreign profile", "missing cookie", "scoped cookie", "locked PIN", "index"} {
		t.Run(mode, func(t *testing.T) {
			f := newTimingWebFixture(t)
			profile := f.base.open.ID
			body := url.Values{"profileId": {profile}}.Encode()
			origin := "http://recording.test"
			token := f.base.userToken
			index := "1"
			operation := "refresh"
			switch mode {
			case "foreign origin":
				origin = "http://foreign.invalid"
			case "no origin":
				origin = ""
			case "duplicate profile":
				body += "&profileId=" + profile
			case "unlock collection":
				body += "&profileUnlock=opaque-one%2Copaque-two"
			case "unknown field":
				body += "&path=untrusted"
			case "query":
				operation = "refresh?source=untrusted"
			case "foreign profile":
				profile = f.base.foreign.ID
				body = url.Values{"profileId": {profile}}.Encode()
			case "missing cookie":
				token = ""
			case "scoped cookie":
				session, err := f.base.sessions.CreateScopedWithResource(f.base.user.ID, false, "test", "127.0.0.1", time.Hour, "stream", "recording:job-"+profile)
				if err != nil {
					t.Fatal(err)
				}
				token = session.Token
			case "locked PIN":
				profile = f.base.locked.ID
				body = url.Values{"profileId": {profile}}.Encode()
			case "index":
				index = "01"
			}
			w := f.post(t, "account", operation, profile, index, body, origin, token)
			if w.Code == 200 || f.service.reads.Load() != 0 || f.service.refreshes.Load() != 0 {
				t.Fatalf("invalid request reached timing %d", w.Code)
			}
		})
	}
}
func TestRecordingWebTimingBodyAuthorityExpiry(t *testing.T) {
	f := newTimingWebFixture(t)
	body := url.Values{"profileId": {f.base.open.ID}}.Encode()
	request := httptest.NewRequest("POST", "http://recording.test/account/api/live/recordings/job-"+f.base.open.ID+"/parts/1/timing/read", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", "http://recording.test")
	request.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.base.userToken})
	ctx, cancel := context.WithDeadline(request.Context(), time.Now().Add(-time.Second))
	defer cancel()
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, request.WithContext(ctx))
	if w.Code == 200 && w.Body.Len() != 0 || f.service.reads.Load() != 0 {
		t.Fatal("expired original authority used")
	}
}
