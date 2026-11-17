package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"novastream/services/users"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type managedWebFixture struct {
	t                                    *testing.T
	handler                              http.Handler
	guard                                *previewRouteTransport
	service                              *importer.Service
	db                                   *database.DB
	manager                              *config.Manager
	accounts                             *sabWebAccounts
	provider, reads, copies              atomic.Int32
	token, other, scoped, source, target string
	afterCopy                            func(context.Context, int64)
}
type managedWebService struct{ f *managedWebFixture }

func (s managedWebService) GetSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	return s.f.service.GetSABFallbackStatus(ctx, id)
}
func (s managedWebService) RefreshSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	return s.f.service.RefreshSABFallbackStatus(ctx, id)
}
func (s managedWebService) ListSABFallbackStatuses(ctx context.Context, c string, n int) (importer.SABFallbackPage, error) {
	return s.f.service.ListSABFallbackStatuses(ctx, c, n)
}
func (s managedWebService) GetSABManagedCopy(ctx context.Context, id int64, a importer.SABImportPreviewSettings, b importer.SABImportStageSettings, c func(context.Context) error) (importer.SABManagedCopyView, error) {
	s.f.reads.Add(1)
	return s.f.service.GetSABManagedCopy(ctx, id, a, b, c)
}
func (s managedWebService) StageSABImport(ctx context.Context, id int64, a importer.SABImportPreviewSettings, b importer.SABImportStageSettings, c func(context.Context) error) (importer.SABImportStageStatus, error) {
	s.f.copies.Add(1)
	v, e := s.f.service.StageSABImport(ctx, id, a, b, c)
	if e == nil && s.f.afterCopy != nil {
		s.f.afterCopy(ctx, id)
	}
	return v, e
}
func (f *managedWebFixture) start(handler http.Handler) *httptest.Server {
	f.t.Helper()
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		f.t.Fatal("synthetic loopback")
	}
	f.guard.mu.Lock()
	f.guard.hosts[l.Addr().String()] = true
	f.guard.mu.Unlock()
	s := httptest.NewUnstartedServer(handler)
	s.Listener = l
	s.Start()
	f.t.Cleanup(s.Close)
	return s
}
func newManagedWebFixture(t *testing.T) *managedWebFixture {
	t.Helper()
	f := &managedWebFixture{t: t}
	f.guard = &previewRouteTransport{hosts: map[string]bool{}}
	f.guard.inner = &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		f.guard.mu.RLock()
		ok := f.guard.hosts[address]
		f.guard.mu.RUnlock()
		if !ok {
			f.guard.denied.Add(1)
			return nil, fmt.Errorf("synthetic literal dial denied")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	}}
	original := http.DefaultTransport
	http.DefaultTransport = f.guard
	t.Cleanup(func() { http.DefaultTransport = original; f.guard.inner.CloseIdleConnections() })
	dir := t.TempDir()
	f.source = filepath.Join(dir, "PRIVATE_SOURCE")
	f.target = filepath.Join(dir, "PRIVATE_TARGET")
	for _, p := range []string{f.source, f.target} {
		if e := os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	provider := f.start(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.provider.Add(1)
		job := strings.TrimPrefix(r.URL.Query().Get("nzo_ids"), "private_job_")
		var id int
		if _, e := fmt.Sscanf(job, "%d", &id); e != nil || id < 1 || id > 53 || r.URL.Path != "/api" {
			t.Error("unexpected synthetic operation")
			w.WriteHeader(500)
			return
		}
		switch r.URL.Query().Get("mode") {
		case "queue":
			fmt.Fprint(w, `{"queue":{"slots":[]}}`)
		case "history":
			json.NewEncoder(w).Encode(map[string]any{"history": map[string]any{"slots": []any{map[string]any{"nzo_id": fmt.Sprintf("private_job_%d", id), "status": "Completed", "storage": fmt.Sprintf("/PRIVATE_REMOTE/job-%d", id), "filename": "PRIVATE_TITLE", "fail_message": "PRIVATE_ERROR"}}}})
		default:
			t.Error("never submit/poll")
			w.WriteHeader(500)
		}
	}))
	enabled := true
	cfg := config.SABnzbdSettings{Enabled: &enabled, FallbackHost: provider.URL, FallbackAPIKey: "synthetic-managed-copy-key", ImportPreviewRemoteRoot: "/PRIVATE_REMOTE", ImportPreviewLocalRoot: f.source, ImportStageTargetRoot: f.target, ImportStageMaxBytes: 8 << 20}
	f.manager = config.NewManager(filepath.Join(dir, "settings.json"))
	settings := config.DefaultSettings()
	settings.SABnzbd = cfg
	settings.UI.OnboardingCompleted = true
	settings.UI.AdminWalkthroughDismissed = true
	if e := f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	db, e := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "queue.db")})
	if e != nil {
		t.Fatal(e)
	}
	f.db = db
	t.Cleanup(func() { db.Close() })
	f.service, e = importer.NewService(importer.ServiceConfig{}, nzbmetadata.NewMetadataService(filepath.Join(dir, "metadata")), db, nil, func() *config.AltMountConfig {
		s, e := f.manager.Load()
		if e != nil {
			return &config.AltMountConfig{}
		}
		return &config.AltMountConfig{SABnzbd: config.SABnzbdConfig{Enabled: s.SABnzbd.Enabled, FallbackHost: s.SABnzbd.FallbackHost, FallbackAPIKey: s.SABnzbd.FallbackAPIKey}}
	})
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal([]string{provider.URL, cfg.FallbackAPIKey})
	sum := sha256.Sum256(raw)
	at := time.Now().UTC()
	for n := 1; n <= 53; n++ {
		job := filepath.Join(f.source, fmt.Sprintf("job-%d", n))
		if e := os.Mkdir(job, 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(job, "PRIVATE_TITLE.mkv"), []byte("synthetic managed copy bytes"), 0600); e != nil {
			t.Fatal(e)
		}
		item := &database.ImportQueueItem{NzbPath: fmt.Sprintf("PRIVATE_NZB_%d", n), Status: database.QueueStatusFailed}
		if e := db.Repository.AddToQueue(item); e != nil {
			t.Fatal(e)
		}
		receipt := map[string]any{"version": 1, "attempt": fmt.Sprintf("%032x", n), "source": hex.EncodeToString(sum[:]), "jobId": fmt.Sprintf("private_job_%d", n), "status": importer.SABFallbackStatus{QueueID: item.ID, State: "download_completed", SubmittedAt: &at, CheckedAt: &at}}
		b, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: receipt, "private": "<img src=x onerror=window.COPY_XSS=true> PRIVATE_PATH_KEY"})
		metadata := string(b)
		if e := db.Repository.UpdateMetadata(item.ID, &metadata); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-managed-web")
	ac, e := accounts.NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	master, ok := ac.GetMasterAccount()
	if !ok {
		t.Fatal("master")
	}
	other, e := ac.Create("synthetic-managed-other", "synthetic-managed-password")
	if e != nil {
		t.Fatal(e)
	}
	ss, e := sessions.NewService("", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	full, e := ss.Create(master.ID, true, "browser", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	plain, e := ss.Create(other.ID, false, "browser", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	scoped, e := ss.CreateScoped(master.ID, true, "browser", "127.0.0.1", time.Hour, models.SessionScopeStream)
	if e != nil {
		t.Fatal(e)
	}
	f.token = full.Token
	f.other = plain.Token
	f.scoped = scoped.Token
	us, e := users.NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := us.CreateForAccount(master.ID, "Synthetic Owner"); e != nil {
		t.Fatal(e)
	}
	admin := handlers.NewAdminUIHandler(f.manager.ConfigPath(), "", nil, us, nil, f.manager)
	admin.SetAccountsService(ac)
	admin.SetSessionsService(ss)
	f.accounts = &sabWebAccounts{sabRouteAccounts: sabRouteAccounts{ac}}
	h := handlers.NewUsenetFallbackHandler(managedWebService{f}, ss, f.accounts)
	h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) {
		v, e := f.manager.Load()
		return v.SABnzbd, e
	})
	router := mux.NewRouter()
	registerUsenetFallbackRoutes(router, admin, h)
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
		json.NewEncoder(w).Encode(map[string]any{"provider": f.provider.Load(), "reads": f.reads.Load(), "copies": f.copies.Load(), "denied": f.guard.denied.Load()})
	})
	router.HandleFunc("/fixture/demote", func(w http.ResponseWriter, r *http.Request) { f.accounts.demoted.Store(true); w.WriteHeader(204) })
	router.PathPrefix("/admin/api/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tasks":[],"workers":[],"defaultPassword":false,"adminWalkthroughDismissed":true}`)
	})
	f.handler = router
	return f
}
func TestSABManagedCopyActualCookieOriginFilesAndPassiveReads(t *testing.T) {
	f := newManagedWebFixture(t)
	server := f.start(f.handler)
	client := &http.Client{Transport: f.guard, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, tc := range []struct {
		name, method, token, origin, query, body, site string
		want                                           int
	}{
		{"missing cookie", "GET", "", "", "", "", "", 303}, {"other account", "POST", f.other, "same", "", "", "", 403}, {"scope", "POST", f.scoped, "same", "", "", "", 403}, {"foreign origin", "POST", f.token, "https://foreign.invalid", "", "", "", 403}, {"missing origin", "POST", f.token, "", "", "", "", 403}, {"cross site", "POST", f.token, "same", "", "", "cross-site", 403}, {"query", "POST", f.token, "same", "?target=PRIVATE", "", "", 400}, {"body", "POST", f.token, "same", "", "{}", "", 400}, {"passive unknown", "GET", f.token, "", "", "", "", 200}, {"confirmed actual copy", "POST", f.token, "same", "", "", "same-origin", 200}, {"saved record", "GET", f.token, "", "", "", "", 200}, {"duplicate", "POST", f.token, "same", "", "", "", 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := f.provider.Load()
			copies := f.copies.Load()
			reads := f.reads.Load()
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			r, e := http.NewRequest(tc.method, server.URL+"/admin/api/usenet/fallback/53/stage"+tc.query, body)
			if e != nil {
				t.Fatal(e)
			}
			if tc.token != "" {
				r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: tc.token})
			}
			if tc.origin != "" {
				o := tc.origin
				if o == "same" {
					o = server.URL
				}
				r.Header.Set("Origin", o)
			}
			if tc.site != "" {
				r.Header.Set("Sec-Fetch-Site", tc.site)
			}
			resp, e := client.Do(r)
			if e != nil {
				t.Fatal("guarded HTTP")
			}
			data, e := io.ReadAll(io.LimitReader(resp.Body, 32768))
			resp.Body.Close()
			if e != nil || resp.StatusCode != tc.want {
				t.Fatalf("status%d want%d", resp.StatusCode, tc.want)
			}
			if tc.name == "confirmed actual copy" {
				if f.provider.Load()-before != 2 {
					t.Fatal("actual fresh queue+history missing")
				}
				var v importer.SABManagedCopyView
				if json.Unmarshal(data, &v) != nil || v.State != "copy_recorded" || v.ChargedBytes == nil || *v.ChargedBytes != int64(len("synthetic managed copy bytes")) || f.reads.Load() != reads {
					t.Fatal("original completed copy projection or unwanted getter")
				}
				var op string
				if e := f.db.Connection().QueryRow("SELECT operation_id FROM sab_import_stages").Scan(&op); e != nil {
					t.Fatal(e)
				}
				copied, e := os.ReadFile(filepath.Join(f.target, op, "data-0001.bin"))
				if e != nil || string(copied) != "synthetic managed copy bytes" {
					t.Fatal("actual destination bytes")
				}
			} else if f.provider.Load() != before {
				t.Fatal("passive/denial/duplicate demanded SAB")
			}
			if tc.want != 200 && tc.name != "duplicate" && (copies != f.copies.Load() || reads != f.reads.Load()) {
				t.Fatal("denial reached copy/get port")
			}
			if strings.Contains(string(data), "PRIVATE") || strings.Contains(string(data), "jobId") || strings.Contains(string(data), "operationId") {
				t.Fatal("private values leaked")
			}
			if tc.name == "passive unknown" && (strings.Contains(string(data), "chargedBytes") || strings.Contains(string(data), "not_recorded")) {
				t.Fatal("unknown target inferred zero")
			}
		})
	}
	if f.guard.denied.Load() != 0 {
		t.Fatal("nonlocal request attempted")
	}
}
func TestSABManagedCopyActualStageReturnConfigAndReceiptBarriers(t *testing.T) {
	for _, mode := range []string{"config", "receipt"} {
		t.Run(mode, func(t *testing.T) {
			f := newManagedWebFixture(t)
			f.afterCopy = func(ctx context.Context, id int64) {
				if mode == "config" {
					cfg, e := f.manager.Load()
					if e != nil {
						t.Fatal(e)
					}
					cfg.SABnzbd.ImportStageTargetRoot = t.TempDir()
					if e := f.manager.Save(cfg); e != nil {
						t.Fatal(e)
					}
				} else {
					item, e := f.db.Repository.GetQueueItemContext(ctx, id)
					if e != nil {
						t.Fatal(e)
					}
					var fields map[string]json.RawMessage
					json.Unmarshal([]byte(*item.Metadata), &fields)
					var receipt map[string]any
					json.Unmarshal(fields[database.SABFallbackMetadataKey], &receipt)
					receipt["attempt"] = strings.Repeat("e", 32)
					b, _ := json.Marshal(receipt)
					fields[database.SABFallbackMetadataKey] = b
					b, _ = json.Marshal(fields)
					next := string(b)
					ok, e := f.db.Repository.CompareAndSwapMetadata(ctx, id, item.Metadata, &next, false)
					if e != nil || !ok {
						t.Fatal("controlled actual receipt change")
					}
				}
			}
			server := f.start(f.handler)
			r, _ := http.NewRequest("POST", server.URL+"/admin/api/usenet/fallback/53/stage", nil)
			r.Header.Set("Origin", server.URL)
			r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.token})
			resp, e := (&http.Client{Transport: f.guard}).Do(r)
			if e != nil {
				t.Fatal(e)
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			want := 200
			if mode == "config" {
				want = 503
			}
			if resp.StatusCode != want || f.reads.Load() != 0 {
				t.Fatal("stage-return adopted a different config/receipt getter")
			}
			if mode == "receipt" && !strings.Contains(string(data), "copy_recorded") {
				t.Fatal("known original completed copy lost")
			}
			if mode == "config" && (!strings.Contains(string(data), "may already have been copied") || strings.Contains(string(data), "copy_recorded")) {
				t.Fatal("config transition pretended rollback or success")
			}
			var state string
			if e := f.db.Connection().QueryRow("SELECT state FROM sab_import_stages").Scan(&state); e != nil || state != "copied" {
				t.Fatal("real prior copy did not remain recorded")
			}
		})
	}
}
func TestSABManagedCopyProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local browser fixture required")
	}
	f := newManagedWebFixture(t)
	server := f.start(f.handler)
	artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifact == "" {
		artifact = t.TempDir()
	}
	artifact = filepath.Join(artifact, "managed-copy")
	if e := os.MkdirAll(artifact, 0700); e != nil {
		t.Fatal(e)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-mcw-browser"
	if e := os.MkdirAll(scratch, 0700); e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(t.Context(), python, "testdata/sab_managed_copy_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("synthetic managed copy browser: %v\n%s", e, out)
	}
}
