package main

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
	"sync/atomic"
	"testing"

	"novastream/config"
	"novastream/handlers"
	"novastream/services/accounts"
)

// The fixture renders the actual templates and invokes Main's actual Cookie
// registration. Dashboard/setup status replies are explicitly fixture data;
// this does not start Main, authenticate through login, or contact a provider.
type creationClientBrowserFixture struct {
	base                                               *mainKidsFlagFixture
	handler                                            http.Handler
	posts, retries, commits, reads, icons, pins, other atomic.Int32
	profilesGET                                        atomic.Int32
	maskNext                                           atomic.Bool
}

func newCreationClientBrowserFixture(t *testing.T, master bool) *creationClientBrowserFixture {
	t.Helper()
	// Installs the default-deny transport before constructing any service.
	base, _ := newMainKidsFlagFixture(t, master, false)
	f := &creationClientBrowserFixture{base: base}
	actual, err := accounts.NewServiceWithStore(base.base.store)
	if err != nil {
		t.Fatal("construct real synthetic account facade", err)
	}
	if _, err := actual.Create("synthetic-browser-other-household", "synthetic-browser-other-password"); err != nil {
		t.Fatal("create own synthetic second household", err)
	}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	cfg := config.DefaultSettings()
	cfg.UI.OnboardingCompleted = true
	cfg.UI.AdminWalkthroughDismissed = true
	if err := manager.Save(cfg); err != nil {
		t.Fatal("save synthetic rendering config", err)
	}
	admin := handlers.NewAdminUIHandler(manager.ConfigPath(), "", nil, base.base.users, base.settings, manager)
	admin.SetAccountsService(actual)
	admin.SetSessionsService(base.base.sessions)
	admin.SetPGProfilePins(base.base.pins)
	wireParentKidsFlagHandler(admin, base.shared)
	registerParentProfileCreationCookieRoutes(base.router, admin, base.shared, base.settings)
	for _, prefix := range []string{"/admin", "/account"} {
		base.router.HandleFunc(prefix+"/accounts", admin.RequireAuth(admin.AccountsPage)).Methods(http.MethodGet)
		base.router.HandleFunc(prefix+"/onboarding", admin.RequireAuth(admin.OnboardingPage)).Methods(http.MethodGet)
		base.router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			f.profilesGET.Add(1)
			admin.GetProfiles(w, r)
		})).Methods(http.MethodGet)
		base.router.HandleFunc(prefix+"/api/accounts", admin.RequireAuth(admin.GetUserAccounts)).Methods(http.MethodGet)
		// Followups are counted and refused, rather than reaching an icon URL or
		// secretly changing the compound-create defaults being tested.
		base.router.HandleFunc(prefix+"/api/profiles/icon", func(w http.ResponseWriter, _ *http.Request) {
			f.icons.Add(1)
			http.Error(w, "synthetic followup denied", http.StatusServiceUnavailable)
		}).Methods(http.MethodPut)
		base.router.HandleFunc(prefix+"/api/profiles/pin", func(w http.ResponseWriter, _ *http.Request) {
			f.pins.Add(1)
			http.Error(w, "synthetic followup denied", http.StatusServiceUnavailable)
		}).Methods(http.MethodPut)
	}
	base.router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", handlers.NewStaticHandler()))
	base.router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, _ *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "strmr_admin_session", Value: base.base.session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	})
	base.router.HandleFunc("/fixture/mask-next", func(w http.ResponseWriter, _ *http.Request) {
		f.maskNext.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	base.router.HandleFunc("/fixture/retire-session", func(w http.ResponseWriter, r *http.Request) {
		if err := base.base.sessions.Revoke(base.base.session.Token); err != nil {
			t.Error("retire own synthetic session", err)
			http.Error(w, "synthetic session unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	base.router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		var users, settings, created int
		var profiles []byte
		err := base.base.store.Pool().QueryRow(r.Context(), `SELECT
 (SELECT count(*) FROM users), (SELECT count(*) FROM user_settings),
 (SELECT count(*) FROM profile_creation_results WHERE outcome='created'),
 (SELECT coalesce(jsonb_agg(jsonb_build_object('id',id,'name',name,'accountId',account_id,'color',color,'isKidsProfile',is_kids_profile) ORDER BY id),'[]'::jsonb) FROM users)`).Scan(&users, &settings, &created, &profiles)
		if err != nil {
			t.Error("observe own synthetic creation rows", err)
			http.Error(w, "synthetic state unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"users": users, "settings": settings, "createdResults": created, "profiles": json.RawMessage(profiles),
			"posts": f.posts.Load(), "retries": f.retries.Load(), "commits": f.commits.Load(), "reads": f.reads.Load(),
			"icons": f.icons.Load(), "pins": f.pins.Load(), "otherWrites": f.other.Load(), "profilesGET": f.profilesGET.Load(),
		})
	})
	base.router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/api/clients") || strings.HasSuffix(r.URL.Path, "/api/media-libraries") {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		// Only unrelated setup/dashboard observations are synthetic. Setting
		// these true unlocks the real Onboarding Profiles step without calling
		// streaming, indexer or metadata services.
		_, _ = io.WriteString(w, `{"needsOnboarding":false,"adminWalkthroughDismissed":true,"defaultPassword":false,"hasStreamingProvider":true,"hasSearchSource":true,"hasMetadataProvider":true,"hasUsableProfile":true,"setupComplete":true,"tasks":[],"workers":[],"accounts":[],"libraries":[],"clients":[]}`)
	})
	f.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		create := r.Method == http.MethodPost && (r.URL.Path == "/admin/api/profiles" || r.URL.Path == "/account/api/profiles")
		read := r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/api/profiles/creation-operations/") && strings.HasSuffix(r.URL.Path, "/read")
		if read {
			f.reads.Add(1)
		}
		if !create {
			if strings.Contains(r.URL.Path, "/api/") && r.Method != http.MethodGet && !read && !strings.HasSuffix(r.URL.Path, "/profiles/icon") && !strings.HasSuffix(r.URL.Path, "/profiles/pin") {
				f.other.Add(1)
			}
			base.router.ServeHTTP(w, r)
			return
		}
		f.posts.Add(1)
		if r.Header.Get("X-Parent-Intent") != "" {
			f.retries.Add(1)
		}
		reply := httptest.NewRecorder()
		base.router.ServeHTTP(reply, r)
		if reply.Code == http.StatusOK && reply.Header().Get("X-Profile-Creation-Operation") != "" {
			f.commits.Add(1)
			if f.maskNext.Swap(false) {
				// This is an HTTP reply masked AFTER an observed successful
				// handler response. It is not a database COMMIT-ACK fault.
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"code":"parent_confirmation_outcome_unknown","error":"Synthetic known-COMMIT HTTP response loss"}`)
				return
			}
		}
		for key, values := range reply.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(reply.Code)
		_, _ = w.Write(reply.Body.Bytes())
	})
	return f
}

func (f *creationClientBrowserFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen on literal synthetic loopback", err)
	}
	server := httptest.NewUnstartedServer(f.handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func TestParentProfileCreationClientActualCookieRoutesAndOrigin(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newCreationClientBrowserFixture(t, master)
			server := f.server(t)
			client := &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
				if address != server.Listener.Addr().String() {
					return nil, fmt.Errorf("non-fixture destination denied")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
			}}}
			t.Cleanup(client.CloseIdleConnections)
			for _, prefix := range []string{"/admin", "/account"} {
				call := func(method, path, origin string) (int, []byte) {
					t.Helper()
					r, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, nil)
					if err != nil {
						t.Fatal(err)
					}
					r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.base.base.session.Token})
					if origin != "" {
						r.Header.Set("Origin", origin)
					}
					reply, err := client.Do(r)
					if err != nil {
						t.Fatal(err)
					}
					defer reply.Body.Close()
					raw, err := io.ReadAll(reply.Body)
					if err != nil {
						t.Fatal(err)
					}
					return reply.StatusCode, raw
				}
				if status, raw := call(http.MethodGet, prefix+"/accounts", ""); status != http.StatusOK || !bytes.Contains(raw, []byte("account-parent-auth-v1.js")) {
					t.Fatal("actual Accounts page unavailable", status)
				}
				status, _ := call(http.MethodGet, prefix+"/onboarding", "")
				if master && status != http.StatusOK || !master && status != http.StatusForbidden {
					t.Fatal("actual Onboarding role gate changed", status)
				}
				read := prefix + "/api/profiles/creation-operations/pcr_" + strings.Repeat("f", 64) + "/read"
				for _, origin := range []string{"", "https://foreign.invalid"} {
					if status, _ := call(http.MethodPost, read, origin); status != http.StatusForbidden {
						t.Fatal("actual Main read Origin guard bypassed", status)
					}
				}
				if status, raw := call(http.MethodPost, read, server.URL); status != http.StatusNotFound || !bytes.Contains(raw, []byte(`"outcome":"unqualified_or_missing"`)) {
					t.Fatal("actual authorized missing read unavailable", status)
				}
			}
		})
	}
}

func TestParentProfileCreationClientProductionTemplatesBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if os.Getenv("MEDIASTORM_PROFILE_CREATION_CLIENT_BROWSER") != "1" || python == "" || chromium == "" {
		t.Skip("dedicated creation-client loopback browser fixture required")
	}
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newCreationClientBrowserFixture(t, master)
			server := f.server(t)
			role := "regular"
			if master {
				role = "master"
			}
			artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifact == "" {
				artifact = t.TempDir()
			}
			artifact = filepath.Join(artifact, "profile-creation-client", role)
			if err := os.MkdirAll(artifact, 0700); err != nil {
				t.Fatal(err)
			}
			// Chromium's singleton socket cannot fit a long Go test-name path.
			// Use only the supplied persistent root and clean our own child dir.
			scratchRoot := os.Getenv("MEDIASTORM_BROWSER_TMPDIR")
			if scratchRoot == "" {
				scratchRoot = os.Getenv("TMPDIR")
			}
			if !filepath.IsAbs(scratchRoot) {
				t.Fatal("explicit persistent browser scratch root required")
			}
			scratch, err := os.MkdirTemp(scratchRoot, "pcc-")
			if err != nil {
				t.Fatal("create short browser scratch directory", err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(scratch); err != nil {
					t.Error("remove own browser scratch directory", err)
				}
			})
			cmd := exec.CommandContext(t.Context(), python, "handlers/testdata/parent_profile_creation_client_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact, "--role", role)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual profile creation client browser: %v\n%s", err, output)
			}
		})
	}
}
