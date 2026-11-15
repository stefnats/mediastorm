package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"novastream/config"
	"novastream/services/accounts"
	"novastream/services/sessions"
	user_settings "novastream/services/user_settings"
	"novastream/services/users"
)

func recordingPlanningBrowserFixture(t *testing.T) http.Handler {
	t.Helper()
	h, repo := recordingPlanningFixture(t)
	dir := t.TempDir()
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-planning-admin-password")
	accountService, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountService.GetByUsername("admin")
	if !ok {
		t.Fatal("synthetic master missing")
	}
	sessionService, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := sessionService.Create(master.ID, true, "synthetic planning browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := profiles.CreateForAccount(master.ID, "Synthetic viewer")
	if err != nil {
		t.Fatal(err)
	}
	other, err := profiles.CreateForAccount(master.ID, "Other viewer")
	if err != nil {
		t.Fatal(err)
	}
	h.users.(*fakeRecordingUsersProvider).users = append(h.users.(*fakeRecordingUsersProvider).users, profiles.ListAll()...)
	j := repo.jobs["own"]
	j.UserID = viewer.ID
	j.Title = "Synthetic recording"
	repo.jobs[j.ID] = j
	prefs, err := user_settings.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	manager := config.NewManager(filepath.Join(dir, "settings.json"))
	if err := manager.Save(config.Settings{UI: config.UISettings{OnboardingCompleted: true, AdminWalkthroughDismissed: true}}); err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, profiles, prefs, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(sessionService)
	mux := http.NewServeMux()
	mux.Handle("/api/static/", http.StripPrefix("/api/static/", NewStaticHandler()))
	mux.HandleFunc("/admin/recordings", admin.RequireAuth(admin.RecordingsPage))
	mux.HandleFunc("/admin/api/live/recordings/plan", admin.RequireAuth(h.Plan))
	mux.HandleFunc("/admin/api/live/recordings/preview", admin.RequireAuth(h.Preview))
	mux.HandleFunc("/admin/api/live/recordings/time-block", admin.RequireAuth(h.CreateTimeBlock))
	mux.HandleFunc("/admin/api/live/recordings/epg", admin.RequireAuth(h.CreateEPG))
	mux.HandleFunc("/admin/api/live/recordings", admin.RequireAuth(h.List))
	mux.HandleFunc("/admin/api/live/channels", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
		channels, _ := h.liveChannels.FetchFilteredChannelsForRequest(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"channels": channels})
	}))
	mux.HandleFunc("/admin/api/", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/api/status", "/admin/api/onboarding/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"defaultPassword": false, "adminWalkthroughDismissed": true})
		case "/admin/api/live/recording-rules":
			_ = json.NewEncoder(w).Encode(map[string]any{"rules": []any{}})
		case "/admin/api/live/recording-settings":
			_ = json.NewEncoder(w).Encode(map[string]any{"paddingBeforeSeconds": 0, "paddingAfterSeconds": 0})
		default:
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))
	mux.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, _ *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	mux.HandleFunc("/fixture/info", func(w http.ResponseWriter, _ *http.Request) {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"fixture": "synthetic-recording-plan", "writes": repo.writes, "viewer": viewer.ID, "other": other.ID, "start": j.StartAt.Add(time.Minute).Format(time.RFC3339), "stop": j.EndAt.Add(-time.Minute).Format(time.RFC3339), "jobs": len(repo.jobs)})
	})
	return mux
}

func TestRecordingPlanningProductionTemplateBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("Playwright Python and Chromium required")
	}
	server := httptest.NewServer(recordingPlanningBrowserFixture(t))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	if err := os.MkdirAll(artifacts, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "recording_planning_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("recording planning browser: %v\n%s", err, output)
	}
}
