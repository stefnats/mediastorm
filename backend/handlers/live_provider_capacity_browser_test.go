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

	"novastream/services/accounts"
	"novastream/services/sessions"
	user_settings "novastream/services/user_settings"
	"novastream/services/users"
)

func newProviderCapacityBrowserFixture(t *testing.T) http.Handler {
	t.Helper()
	h, manager, calls := capacityFixture(t, nil)
	settings, _ := manager.Load()
	enabled := true
	for i := range settings.Live.Sources {
		settings.Live.Sources[i].Enabled = &enabled
	}
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-capacity-admin-password")
	accountService, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountService.GetByUsername("admin")
	if !ok {
		t.Fatal("synthetic administrator missing")
	}
	sessionService, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := sessionService.Create(master.ID, true, "synthetic capacity browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.CreateForAccount(master.ID, "Synthetic Viewer"); err != nil {
		t.Fatal(err)
	}
	prefs, err := user_settings.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, profiles, prefs, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(sessionService)
	settingsHandler := NewSettingsHandler(manager)
	router := http.NewServeMux()
	router.Handle("/api/static/", http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/admin/settings", admin.RequireMasterAuth(admin.SettingsPage))
	router.HandleFunc("/admin/api/live/provider-capacity", admin.RequireMasterAuth(h.CheckProviderCapacity))
	router.HandleFunc("/admin/api/settings", admin.RequireMasterAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			settingsHandler.PutSettings(w, r)
		} else {
			settingsHandler.GetSettings(w, r)
		}
	}))
	router.HandleFunc("/admin/api/schema", admin.RequireMasterAuth(admin.GetSchema))
	router.HandleFunc("/admin/api/profiles", admin.RequireMasterAuth(admin.GetProfiles))
	router.HandleFunc("/admin/api/accounts", admin.RequireMasterAuth(admin.GetUserAccounts))
	router.HandleFunc("/admin/api/user-settings", admin.RequireMasterAuth(admin.GetUserSettings))
	router.HandleFunc("/admin/api/", admin.RequireMasterAuth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/api/status", "/admin/api/onboarding/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": "Synthetic capacity fixture", "defaultPassword": false, "adminWalkthroughDismissed": true})
		case "/admin/api/scheduled-tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		default:
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"fixture": "synthetic-provider-capacity", "providerRequests": calls.Load()})
	})
	return router
}

func TestProviderCapacityProductionSettingsBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("Playwright Python and Chromium required")
	}
	server := httptest.NewServer(newProviderCapacityBrowserFixture(t))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	if err := os.MkdirAll(artifacts, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "provider_capacity_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("provider capacity browser: %v\n%s", err, output)
	}
}
