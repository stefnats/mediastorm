package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/importer"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/sessions"
)

type sabRouteAccounts struct{ real *accounts.Service }

func (a sabRouteAccounts) Get(ctx context.Context, id string) (*models.Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, ok := a.real.Get(id)
	if !ok {
		return nil, nil
	}
	return &value, nil
}

type sabRouteStatus struct{ gets, refreshes atomic.Int32 }

func (s *sabRouteStatus) GetSABFallbackStatus(_ context.Context, id int64) (importer.SABFallbackStatus, error) {
	s.gets.Add(1)
	return importer.SABFallbackStatus{QueueID: id, State: "submitted"}, nil
}
func (s *sabRouteStatus) RefreshSABFallbackStatus(_ context.Context, id int64) (importer.SABFallbackStatus, error) {
	s.refreshes.Add(1)
	return importer.SABFallbackStatus{QueueID: id, State: "downloading"}, nil
}
func TestSABFallbackActualAdminRouteCookieAndOrigin(t *testing.T) {
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-sab-admin")
	root := t.TempDir()
	accountStore, err := accounts.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountStore.GetMasterAccount()
	if !ok {
		t.Fatal("master missing")
	}
	other, err := accountStore.Create("synthetic-sab-user", "synthetic-sab-password")
	if err != nil {
		t.Fatal(err)
	}
	sessionStore, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	full, err := sessionStore.Create(master.ID, true, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := sessionStore.Create(other.ID, false, "synthetic-browser", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := sessionStore.CreateScoped(master.ID, true, "synthetic-browser", "127.0.0.1", time.Hour, models.SessionScopeStream)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.NewManager(filepath.Join(root, "settings.json"))
	if err := cfg.Save(config.Settings{UI: config.UISettings{OnboardingCompleted: true, AdminWalkthroughDismissed: true}}); err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler(cfg.ConfigPath(), "", nil, nil, nil, cfg)
	admin.SetAccountsService(accountStore)
	admin.SetSessionsService(sessionStore)
	status := &sabRouteStatus{}
	handler := handlers.NewUsenetFallbackHandler(status, sessionStore, sabRouteAccounts{accountStore})
	router := mux.NewRouter()
	registerUsenetFallbackRoutes(router, admin, handler)
	server := httptest.NewServer(router)
	defer server.Close()
	client := server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for _, tc := range []struct {
		name, method, path, token, origin string
		want                              int
		called                            bool
	}{
		{"no cookie", "GET", "/admin/api/usenet/fallback/17", "", "", 303, false},
		{"actual full master", "GET", "/admin/api/usenet/fallback/17", full.Token, "", 200, true},
		{"other current account", "GET", "/admin/api/usenet/fallback/17", plain.Token, "", 403, false},
		{"scoped cookie", "GET", "/admin/api/usenet/fallback/17", scoped.Token, "", 403, false},
		{"cross origin", "POST", "/admin/api/usenet/fallback/17/refresh", full.Token, "https://foreign.example", 403, false},
		{"missing origin", "POST", "/admin/api/usenet/fallback/17/refresh", full.Token, "", 403, false},
		{"same origin explicit refresh", "POST", "/admin/api/usenet/fallback/17/refresh", full.Token, server.URL, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := status.gets.Load() + status.refreshes.Load()
			req, err := http.NewRequest(tc.method, server.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.token != "" {
				req.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: tc.token})
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status%d want%d", resp.StatusCode, tc.want)
			}
			delta := status.gets.Load() + status.refreshes.Load() - before
			if tc.called && delta != 1 || !tc.called && delta != 0 {
				t.Fatalf("wrong service calls%d", delta)
			}
		})
	}
}
