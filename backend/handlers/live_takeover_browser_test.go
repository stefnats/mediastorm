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

func newLivePermissionsBrowserFixture(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-master-initial-password")
	dir := t.TempDir()
	accountService, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	account, err := accountService.Create("synthetic-household", "synthetic-parent-confirmation")
	if err != nil {
		t.Fatal(err)
	}
	other, err := accountService.Create("synthetic-other-household", "synthetic-other-confirmation")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	adult, err := profiles.CreateForAccount(account.ID, "Synthetic Viewer")
	if err != nil {
		t.Fatal(err)
	}
	child, err := profiles.CreateForAccount(account.ID, "Synthetic Child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.SetKidsProfile(child.ID, true); err != nil {
		t.Fatal(err)
	}
	foreign, err := profiles.CreateForAccount(other.ID, "Synthetic Foreign")
	if err != nil {
		t.Fatal(err)
	}
	sessionService, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := sessionService.Create(account.ID, false, "synthetic browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	prefs, err := user_settings.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(dir, "settings.json")
	manager := config.NewManager(settingsPath)
	settings := config.DefaultSettings()
	settings.Cache.Directory = filepath.Join(dir, "cache")
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler(settingsPath, "", nil, profiles, prefs, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(sessionService)
	accountUI := NewAccountUIHandler(accountService, sessionService, profiles, prefs, nil, manager, nil)
	router := http.NewServeMux()
	router.Handle("/api/static/", http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/account", accountUI.RequireAuth(accountUI.Dashboard))
	router.HandleFunc("/account/users", admin.RequireAuth(admin.AccountsPage))
	router.HandleFunc("/account/api/profiles", admin.RequireAuth(admin.GetProfiles))
	router.HandleFunc("/account/api/profiles/live-permissions", admin.RequireAuth(admin.SetProfileLivePermissions))
	router.HandleFunc("/account/api/accounts", admin.RequireAuth(admin.GetUserAccounts))
	router.HandleFunc("/account/api/user-settings", admin.RequireAuth(admin.GetUserSettings))
	router.HandleFunc("/account/api/", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/account/api/clients":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/account/api/scheduled-tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	})
	router.HandleFunc("/fixture/info", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"fixture": "synthetic-live-permissions", "adult": adult.ID, "child": child.ID, "foreign": foreign.ID})
	})
	return router
}

func TestLivePermissionsProductionTemplatesBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("Playwright Python and Chromium are required")
	}
	server := httptest.NewServer(newLivePermissionsBrowserFixture(t))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	if err := os.MkdirAll(artifacts, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "live_permissions_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("live permissions browser: %v\n%s", err, output)
	}
}
