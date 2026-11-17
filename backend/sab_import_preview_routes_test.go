package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/database"
	"novastream/internal/importer"
	nzbmetadata "novastream/internal/nzb/metadata"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/sessions"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type previewRouteTransport struct {
	mu     sync.RWMutex
	hosts  map[string]bool
	denied atomic.Int32
	inner  *http.Transport
}

func (g *previewRouteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	g.mu.RLock()
	ok := g.hosts[r.URL.Host]
	g.mu.RUnlock()
	if r.URL.Scheme != "http" || !ok {
		g.denied.Add(1)
		return nil, errors.New("synthetic transport denied")
	}
	return g.inner.RoundTrip(r)
}
func TestSABImportPreviewActualCookieOriginAndLocalInventory(t *testing.T) {
	// Deny all HTTP before constructing any service. Only these two explicit
	// literal loopback listeners are ever allowed; proxy lookup is disabled.
	guard := &previewRouteTransport{hosts: map[string]bool{}}
	guard.inner = &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		guard.mu.RLock()
		ok := guard.hosts[address]
		guard.mu.RUnlock()
		if !ok {
			guard.denied.Add(1)
			return nil, errors.New("synthetic literal dial denied")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	}}
	original := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() { http.DefaultTransport = original; guard.inner.CloseIdleConnections() })
	start := func(handler http.Handler) *httptest.Server {
		l, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			t.Fatal("synthetic loopback")
		}
		guard.mu.Lock()
		guard.hosts[l.Addr().String()] = true
		guard.mu.Unlock()
		s := httptest.NewUnstartedServer(handler)
		s.Listener = l
		s.Start()
		t.Cleanup(s.Close)
		return s
	}
	var statuses atomic.Int32
	provider := start(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" || r.URL.Query().Get("nzo_ids") != "private_job_1" {
			t.Error("unexpected synthetic SAB request")
			w.WriteHeader(500)
			return
		}
		switch r.URL.Query().Get("mode") {
		case "queue":
			fmt.Fprint(w, `{"queue":{"slots":[]}}`)
		case "history":
			statuses.Add(1)
			fmt.Fprint(w, `{"history":{"slots":[{"nzo_id":"private_job_1","status":"Completed","storage":"/PRIVATE_REMOTE/job","filename":"PRIVATE_TITLE","fail_message":"PRIVATE_ERROR"}]}}`)
		default:
			t.Error("unexpected operation, never submit")
			w.WriteHeader(500)
		}
	}))
	dir := t.TempDir()
	local := filepath.Join(dir, "PRIVATE_LOCAL")
	if e := os.MkdirAll(filepath.Join(local, "job"), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(local, "job", "PRIVATE_FILENAME.mkv"), []byte("metadata-only synthetic bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	db, e := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "queue.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	enabled := true
	settings := config.SABnzbdSettings{Enabled: &enabled, FallbackHost: provider.URL, FallbackAPIKey: "synthetic-private-key", ImportPreviewRemoteRoot: "/PRIVATE_REMOTE", ImportPreviewLocalRoot: local}
	cfg := &config.AltMountConfig{SABnzbd: config.SABnzbdConfig{Enabled: &enabled, FallbackHost: provider.URL, FallbackAPIKey: settings.FallbackAPIKey}}
	real, e := importer.NewService(importer.ServiceConfig{}, nzbmetadata.NewMetadataService(filepath.Join(dir, "metadata")), db, nil, func() *config.AltMountConfig { return cfg })
	if e != nil {
		t.Fatal(e)
	}
	item := &database.ImportQueueItem{NzbPath: "PRIVATE_NZB", Status: database.QueueStatusFailed}
	if e := db.Repository.AddToQueue(item); e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal([]string{provider.URL, settings.FallbackAPIKey})
	sum := sha256.Sum256(raw)
	at := time.Now().UTC()
	receipt := map[string]any{"version": 1, "attempt": strings.Repeat("a", 32), "source": hex.EncodeToString(sum[:]), "jobId": "private_job_1", "status": importer.SABFallbackStatus{QueueID: item.ID, State: "download_completed", SubmittedAt: &at, CheckedAt: &at}}
	raw, _ = json.Marshal(map[string]any{database.SABFallbackMetadataKey: receipt, "foreign": "PRIVATE_METADATA"})
	metadata := string(raw)
	if e := db.Repository.UpdateMetadata(item.ID, &metadata); e != nil {
		t.Fatal(e)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-preview-master")
	accountStore, e := accounts.NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	master, ok := accountStore.GetMasterAccount()
	if !ok {
		t.Fatal("master")
	}
	other, e := accountStore.Create("synthetic-preview-other", "synthetic-preview-password")
	if e != nil {
		t.Fatal(e)
	}
	sessionStore, e := sessions.NewService("", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	full, e := sessionStore.Create(master.ID, true, "test", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	plain, e := sessionStore.Create(other.ID, false, "test", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	scoped, e := sessionStore.CreateScoped(master.ID, true, "test", "127.0.0.1", time.Hour, models.SessionScopeStream)
	if e != nil {
		t.Fatal(e)
	}
	manager := config.NewManager(filepath.Join(dir, "settings.json"))
	if e := manager.Save(config.Settings{UI: config.UISettings{OnboardingCompleted: true, AdminWalkthroughDismissed: true}, SABnzbd: settings}); e != nil {
		t.Fatal(e)
	}
	admin := handlers.NewAdminUIHandler(manager.ConfigPath(), "", nil, nil, nil, manager)
	admin.SetAccountsService(accountStore)
	admin.SetSessionsService(sessionStore)
	current := &sabWebAccounts{sabRouteAccounts: sabRouteAccounts{accountStore}}
	h := handlers.NewUsenetFallbackHandler(real, sessionStore, current)
	h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { s, e := manager.Load(); return s.SABnzbd, e })
	router := mux.NewRouter()
	registerUsenetFallbackRoutes(router, admin, h)
	server := start(router)
	client := &http.Client{Transport: guard, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	target := fmt.Sprintf("/admin/api/usenet/fallback/%d/import-preview", item.ID)
	for _, tc := range []struct {
		name, method, path, token, origin, site, body string
		want                                          int
		work                                          bool
	}{
		{"missing cookie", "POST", target, "", "same", "", "", 303, false},
		{"other account", "POST", target, plain.Token, "same", "", "", 403, false},
		{"scoped", "POST", target, scoped.Token, "same", "", "", 403, false},
		{"foreign origin", "POST", target, full.Token, "https://foreign.invalid", "", "", 403, false},
		{"missing origin", "POST", target, full.Token, "", "", "", 403, false},
		{"foreign fetch site", "POST", target, full.Token, "same", "cross-site", "", 403, false},
		{"body forbidden", "POST", target, full.Token, "same", "", "{}", 400, false},
		{"query forbidden", "POST", target + "?path=PRIVATE", full.Token, "same", "", "", 400, false},
		{"GET not preview", "GET", target, full.Token, "", "", "", 405, false},
		{"actual local complete inventory", "POST", target, full.Token, "same", "same-origin", "", 200, true},
		{"current role overrides cached master", "POST", target, full.Token, "same", "", "", 403, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.HasPrefix(tc.name, "current role") {
				current.demoted.Store(true)
			}
			before := statuses.Load()
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			r, e := http.NewRequest(tc.method, server.URL+tc.path, body)
			if e != nil {
				t.Fatal(e)
			}
			if tc.token != "" {
				r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: tc.token})
			}
			if tc.origin != "" {
				origin := tc.origin
				if origin == "same" {
					origin = server.URL
				}
				r.Header.Set("Origin", origin)
			}
			if tc.site != "" {
				r.Header.Set("Sec-Fetch-Site", tc.site)
			}
			response, e := client.Do(r)
			if e != nil {
				t.Fatal("guarded local request")
			}
			data, e := io.ReadAll(io.LimitReader(response.Body, 16384))
			response.Body.Close()
			if e != nil || response.StatusCode != tc.want {
				t.Fatalf("status%d want%d", response.StatusCode, tc.want)
			}
			if tc.work {
				var v importer.SABImportPreview
				if json.Unmarshal(data, &v) != nil || v.EntryCount != 1 || v.Files[0].Extension != "mkv" || v.ObservedBytes != 29 {
					t.Fatal("actual inventory DTO")
				}
				if statuses.Load()-before != 1 {
					t.Fatal("exact actual status check missing")
				}
			} else if statuses.Load() != before {
				t.Fatal("denial demanded provider")
			}
			if strings.Contains(string(data), "PRIVATE") || strings.Contains(string(data), "jobId") {
				t.Fatal("private inventory information leaked")
			}
		})
	}
	if guard.denied.Load() != 0 {
		t.Fatal("unexpected nonlocal network attempt")
	}
}
