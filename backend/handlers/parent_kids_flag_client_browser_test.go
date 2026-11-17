package handlers

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

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/services/accounts"
	profilerecovery "novastream/services/profile_recovery"
	"novastream/services/user_settings"
)

type kidsFlagClientFixture struct {
	pg                                                           *publicPinCarrierFixture
	handler                                                      http.Handler
	attempts, retries, commits, recoveries, profilesGET, privacy atomic.Int32
	unknown                                                      atomic.Bool
}

// Actual templates, cookie/PG services and original confirmation, not Main
// startup/login. Unrelated dashboard endpoints are fixed local fixture replies.
func newKidsFlagClientFixture(t *testing.T, master bool) *kidsFlagClientFixture {
	t.Helper()
	kidsCookieNoHTTP(t) // Install deny transport before every constructor.
	f := &kidsFlagClientFixture{pg: newPublicPinCarrierFixture(t)}
	if _, err := f.pg.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=$1 WHERE id='actor'", master); err != nil {
		t.Fatal(err)
	}
	settings, err := user_settings.NewServiceWithStore(f.pg.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-client-master")
	accountService, err := accounts.NewServiceWithStore(f.pg.store)
	if err != nil {
		t.Fatal(err)
	}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	cfg := config.DefaultSettings()
	cfg.UI.OnboardingCompleted = true
	cfg.UI.AdminWalkthroughDismissed = true
	if err := manager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, f.pg.users, settings, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(f.pg.sessions)
	admin.SetPGProfilePins(f.pg.pins)
	shared := NewUsersHandler(f.pg.users, accountService)
	shared.SetPGProfilePins(f.pg.pins)
	admin.SetParentKidsHandler(shared)
	recovery := NewProfileRecoveryHandler(profilerecovery.NewService(f.pg.store, f.pg.users, settings, f.pg.pins), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} })
	router := mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/accounts", admin.RequireAuth(admin.AccountsPage)).Methods("GET")
		router.HandleFunc(prefix+"/settings", admin.RequireAuth(admin.SettingsPage)).Methods("GET")
		router.HandleFunc(prefix+"/kids-settings", admin.RequireAuth(admin.KidsSettingsPage)).Methods("GET")
		router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) { f.profilesGET.Add(1); admin.GetProfiles(w, r) })).Methods("GET")
		router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(admin.RenameProfile)).Methods("PUT")
		router.HandleFunc(prefix+"/api/profiles/color", admin.RequireAuth(admin.SetProfileColor)).Methods("PUT")
		router.HandleFunc(prefix+"/api/profiles/activity-privacy", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) { f.privacy.Add(1); admin.SetProfileActivityPrivacy(w, r) })).Methods("PUT")
		router.HandleFunc(prefix+"/api/user-settings", admin.RequireAuth(admin.GetUserSettings)).Methods("GET")
		router.HandleFunc(prefix+"/api/settings", admin.RequireAuth(NewSettingsHandler(manager).GetSettings)).Methods("GET")
		router.HandleFunc(prefix+"/api/profiles/kids", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			f.attempts.Add(1)
			if r.Header.Get("X-Parent-Intent") != "" {
				f.retries.Add(1)
			}
			reply := httptest.NewRecorder()
			admin.SetKidsProfile(reply, r)
			if reply.Code == 200 {
				f.commits.Add(1)
				if f.unknown.Swap(false) {
					// Response loss after known commit, not a DB COMMIT ACK fault.
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(503)
					io.WriteString(w, `{"code":"parent_confirmation_unavailable"}`)
					return
				}
			}
			for key, values := range reply.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(reply.Code)
			w.Write(reply.Body.Bytes())
		})).Methods("PUT")
		router.HandleFunc(prefix+"/api/users/{userID}/state/reload", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			f.recoveries.Add(1)
			recovery.Cookie(w, r)
		})).Methods("POST")
	}
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/reset", func(w http.ResponseWriter, r *http.Request) {
		desired := r.URL.Query().Get("kids") == "true"
		if err := f.pg.users.ResyncStorage(r.Context()); err != nil {
			t.Error(err)
			w.WriteHeader(503)
			return
		}
		if _, err := f.pg.users.SetKidsProfile(f.pg.profile.ID, desired); err != nil {
			t.Error(err)
			w.WriteHeader(503)
			return
		}
		f.unknown.Store(false)
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/rights", func(w http.ResponseWriter, r *http.Request) {
		// Actual external stored changes are adopted only by explicit recovery.
		if _, err := f.pg.store.Pool().Exec(r.Context(), `UPDATE users SET allow_live_takeover=true,allow_recording_interruption=false,updated_at=clock_timestamp() WHERE id=$1`, f.pg.profile.ID); err != nil {
			t.Error(err)
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/unknown", func(w http.ResponseWriter, r *http.Request) { f.unknown.Store(true); w.WriteHeader(204) })
	router.HandleFunc("/fixture/retire", func(w http.ResponseWriter, r *http.Request) {
		if err := f.pg.sessions.Revoke(f.pg.session.Token); err != nil {
			t.Error(err)
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		user, err := f.pg.store.Users().Get(r.Context(), f.pg.profile.ID)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": user.ID, "name": user.Name, "color": user.Color, "privacy": user.ActivityPrivacy, "kids": user.IsKidsProfile,
			"live": user.AllowLiveTakeover, "recording": user.AllowRecordingInterruption, "attempts": f.attempts.Load(), "retries": f.retries.Load(), "commits": f.commits.Load(),
			"recoveries": f.recoveries.Load(), "profilesGET": f.profilesGET.Load(), "privacyCalls": f.privacy.Load()})
	})
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/api/accounts") {
			io.WriteString(w, `{"accounts":[{"id":"actor","username":"synthetic-actor"}]}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/api/clients") || strings.HasSuffix(r.URL.Path, "/api/media-libraries") {
			io.WriteString(w, `[]`)
			return
		}
		io.WriteString(w, `{"needsOnboarding":false,"adminWalkthroughDismissed":true,"tasks":[],"workers":[],"accounts":[],"libraries":[],"clients":[],"defaultPassword":false}`)
	})
	f.handler = router
	return f
}
func (f *kidsFlagClientFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal("synthetic literal loopback required")
	}
	server := httptest.NewUnstartedServer(f.handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}
func TestParentKidsFlagClientActualCookieAndRecovery(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newKidsFlagClientFixture(t, master)
			server := f.server(t)
			client := &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
				if address != server.Listener.Addr().String() {
					return nil, fmt.Errorf("synthetic destination denied")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
			}}}
			t.Cleanup(client.CloseIdleConnections)
			for _, prefix := range []string{"/admin", "/account"} {
				call := func(method, path, body, intent, password string) (int, []byte) {
					t.Helper()
					r, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, bytes.NewBufferString(body))
					if err != nil {
						t.Fatal(err)
					}
					r.Header.Set("Content-Type", "application/json")
					r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token})
					if intent != "" {
						r.Header.Set("X-Parent-Intent", intent)
					}
					if password != "" {
						r.Header.Set("X-Account-Password", password)
					}
					response, err := client.Do(r)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					raw, err := io.ReadAll(response.Body)
					if err != nil {
						t.Fatal(err)
					}
					return response.StatusCode, raw
				}
				path := prefix + "/api/profiles/kids?profileId=" + f.pg.profile.ID
				status, raw := call("PUT", path, `{"isKidsProfile":true}`, "", "")
				var challenge map[string]string
				if json.Unmarshal(raw, &challenge) != nil || status != 403 || !strings.HasPrefix(challenge["parentIntent"], "pkf_") {
					t.Fatal("original fresh challenge missing")
				}
				status, raw = call("PUT", path, `{"isKidsProfile":true}`, challenge["parentIntent"], "synthetic-parent-password")
				var partial map[string]json.RawMessage
				if status != 200 || json.Unmarshal(raw, &partial) != nil || string(partial["allowLiveTakeover"]) != "false" || string(partial["allowRecordingInterruption"]) != "false" {
					t.Fatal("confirmed flag rights missing", status)
				}
				before := f.commits.Load()
				status, raw = call("POST", prefix+"/api/users/"+f.pg.profile.ID+"/state/reload", "", "", "")
				var value struct{ Profile map[string]json.RawMessage }
				if status != 200 || json.Unmarshal(raw, &value) != nil || len(value.Profile) != 10 || string(value.Profile["allowLiveTakeover"]) != "false" || f.commits.Load() != before {
					t.Fatal("direct safe recovery mismatch", status)
				}
			}
		})
	}
}
func TestParentKidsFlagClientProductionTemplatesBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newKidsFlagClientFixture(t, master)
			server := f.server(t)
			artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifact == "" {
				artifact = t.TempDir()
			}
			role := "regular"
			if master {
				role = "master"
			}
			artifact = filepath.Join(artifact, "kids-flag-client", role)
			if err := os.MkdirAll(artifact, 0700); err != nil {
				t.Fatal(err)
			}
			scratch := "/home/stefan/ops-runbook/tmp/kfc17-browser"
			if err := os.MkdirAll(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), python, "testdata/parent_kids_flag_client_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact, "--role", role, "--profile", f.pg.profile.ID)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("profile flag actual client browser: %v\n%s", err, output)
			}
		})
	}
}
