package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/database"
	"novastream/internal/importer"
	nzbmetadata "novastream/internal/nzb/metadata"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/sessions"
	"novastream/services/users"
)

type sabWebAccounts struct {
	sabRouteAccounts
	demoted atomic.Bool
}

func (a *sabWebAccounts) Get(ctx context.Context, id string) (*models.Account, error) {
	account, err := a.sabRouteAccounts.Get(ctx, id)
	if account != nil && a.demoted.Load() {
		account.IsMaster = false
	}
	return account, err
}

type sabWebFixture struct {
	handler         http.Handler
	provider, lists atomic.Int32
	accounts        *sabWebAccounts
	token, other    string
}
type sabWebService struct {
	real    *importer.Service
	fixture *sabWebFixture
}

func (s sabWebService) GetSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	return s.real.GetSABFallbackStatus(ctx, id)
}
func (s sabWebService) RefreshSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	return s.real.RefreshSABFallbackStatus(ctx, id)
}
func (s sabWebService) ListSABFallbackStatuses(ctx context.Context, c string, n int) (importer.SABFallbackPage, error) {
	s.fixture.lists.Add(1)
	return s.real.ListSABFallbackStatuses(ctx, c, n)
}

func newSABWebFixture(t *testing.T) *sabWebFixture {
	t.Helper()
	f := &sabWebFixture{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.provider.Add(1)
		if r.URL.Path != "/api" || r.URL.Query().Get("mode") != "queue" || r.URL.Query().Get("nzo_ids") != "private_job_1" {
			t.Error("unexpected provider operation")
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `{"queue":{"slots":[{"nzo_id":"private_job_1","status":"Downloading","percentage":"10"}]}}`)
	}))
	t.Cleanup(provider.Close)
	dir := t.TempDir()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "queue.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	enabled := true
	cfg := &config.AltMountConfig{SABnzbd: config.SABnzbdConfig{Enabled: &enabled, FallbackHost: provider.URL, FallbackAPIKey: "synthetic-private-key"}}
	service, err := importer.NewService(importer.ServiceConfig{}, nzbmetadata.NewMetadataService(filepath.Join(dir, "metadata")), db, nil, func() *config.AltMountConfig { return cfg })
	if err != nil {
		t.Fatal(err)
	}
	source, _ := json.Marshal([]string{provider.URL, cfg.SABnzbd.FallbackAPIKey})
	fingerprint := sha256.Sum256(source)
	at := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	for n := 0; n < 53; n++ {
		metadata := `{"private":"<img src=x onerror=window.SAB_XSS=true> URL-PATH-KEY-CANARY"}`
		item := &database.ImportQueueItem{NzbPath: fmt.Sprintf("private-%d.nzb", n), Status: database.QueueStatusFailed, Metadata: &metadata}
		if err := db.Repository.AddToQueue(item); err != nil {
			t.Fatal(err)
		}
		status := importer.SABFallbackStatus{QueueID: item.ID, State: "submitted", SubmittedAt: &at}
		if n == 0 {
			status.State = "submission_unknown"
			status.Reason = "acknowledgement_unavailable"
		}
		receipt := map[string]any{"version": 1, "attempt": strings.Repeat("a", 32), "source": hex.EncodeToString(fingerprint[:]), "jobId": "private_job_1", "status": status}
		data, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: receipt, "private": "<img src=x onerror=window.SAB_XSS=true> URL-PATH-KEY-CANARY"})
		value := string(data)
		if err := db.Repository.UpdateMetadata(item.ID, &value); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-web-fallback")
	accountService, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountService.GetMasterAccount()
	if !ok {
		t.Fatal("master missing")
	}
	other, err := accountService.Create("synthetic-web-regular", "synthetic-web-password")
	if err != nil {
		t.Fatal(err)
	}
	sessionService, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	full, err := sessionService.Create(master.ID, true, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	f.token = full.Token
	plain, err := sessionService.Create(other.ID, false, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	f.other = plain.Token
	profileService, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profileService.CreateForAccount(master.ID, "Synthetic Owner"); err != nil {
		t.Fatal(err)
	}
	manager := config.NewManager(filepath.Join(dir, "settings.json"))
	if err := manager.Save(config.Settings{UI: config.UISettings{OnboardingCompleted: true, AdminWalkthroughDismissed: true}}); err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler(manager.ConfigPath(), "", nil, profileService, nil, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(sessionService)
	f.accounts = &sabWebAccounts{sabRouteAccounts: sabRouteAccounts{accountService}}
	handler := handlers.NewUsenetFallbackHandler(sabWebService{service, f}, sessionService, f.accounts)
	router := mux.NewRouter()
	registerUsenetFallbackRoutes(router, admin, handler)
	router.HandleFunc("/admin/tools", admin.RequireAuth(admin.ToolsPage)).Methods("GET")
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", handlers.NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		token := f.token
		if r.URL.Query().Get("regular") == "true" {
			token = f.other
		}
		http.SetCookie(w, &http.Cookie{Name: "strmr_admin_session", Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/info", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"provider": f.provider.Load(), "lists": f.lists.Load()})
	})
	router.HandleFunc("/fixture/demote", func(w http.ResponseWriter, r *http.Request) { f.accounts.demoted.Store(true); w.WriteHeader(204) })
	// Existing unrelated Maintenance widgets have fixed synthetic empty responses.
	router.PathPrefix("/admin/api/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tasks":[],"workers":[],"defaultPassword":false,"adminWalkthroughDismissed":true}`)
	})
	f.handler = router
	return f
}

func TestSABFallbackListActualCookieRoutesAndOrigin(t *testing.T) {
	f := newSABWebFixture(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	client := server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for _, tc := range []struct {
		name, method, path, token, origin string
		want                              int
		lists, provider                   int32
	}{
		{"missing cookie", "GET", "/admin/api/usenet/fallback", "", "", 303, 0, 0},
		{"other account", "GET", "/admin/api/usenet/fallback", f.other, "", 403, 0, 0},
		{"saved page", "GET", "/admin/api/usenet/fallback?limit=1", f.token, "", 200, 1, 0},
		{"bad query", "GET", "/admin/api/usenet/fallback?state=done", f.token, "", 400, 0, 0},
		{"foreign origin", "POST", "/admin/api/usenet/fallback/53/refresh", f.token, "https://foreign.example", 403, 0, 0},
		{"explicit status only", "POST", "/admin/api/usenet/fallback/53/refresh", f.token, server.URL, 200, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list, provider := f.lists.Load(), f.provider.Load()
			r, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
			if tc.token != "" {
				r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: tc.token})
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			response, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.want || f.lists.Load()-list != tc.lists || f.provider.Load()-provider != tc.provider {
				t.Fatalf("status/calls %d/%d/%d: %s", response.StatusCode, f.lists.Load()-list, f.provider.Load()-provider, data)
			}
			for _, s := range []string{"private_job_1", "URL-PATH-KEY-CANARY", "synthetic-private-key", "nzbPath"} {
				if strings.Contains(string(data), s) {
					t.Fatal("private data", s)
				}
			}
			if dir := os.Getenv("SAB_FALLBACK_WEB_CONTRACT_DIR"); dir != "" && tc.want != 303 {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				name := strings.ReplaceAll(tc.name, " ", "_") + ".json"
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSABFallbackProductionWebBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local Playwright and Chromium required")
	}
	f := newSABWebFixture(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifact == "" {
		artifact = t.TempDir()
	}
	artifact = filepath.Join(artifact, "sab-fallback-web")
	if err := os.MkdirAll(artifact, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-sfw-browser"
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), python, filepath.Join("testdata", "sab_fallback_web_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual SAB web browser: %v\n%s", err, out)
	}
}
