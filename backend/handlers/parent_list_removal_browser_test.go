package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/services/accounts"
)

const parentListRemovalIdentity = "https://mdblist.com/lists/synthetic/quote-');window.__listRemovalInjected=1;//\"-雪?encoded=%2F&literal=+"
const parentListRemovalKept = "https://mdblist.com/lists/synthetic/keep"
const parentListRemovalScheme = "javascript:window.__listRemovalScheme=1"
const parentListRemovalInvalidURL = "http://["

type parentListRemovalBrowserFixture struct {
	pg       *publicPinCarrierFixture
	handler  http.Handler
	attempts atomic.Int32
	retries  atomic.Int32
	writes   atomic.Int32
}

func newParentListRemovalBrowserFixture(t *testing.T, master bool) *parentListRemovalBrowserFixture {
	t.Helper()
	f := &parentListRemovalBrowserFixture{pg: newPublicPinCarrierFixture(t)}
	if _, err := f.pg.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=$1 WHERE id='actor'", master); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsProfile(f.pg.profile.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsMode(f.pg.profile.ID, "content_list"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsAllowedLists(f.pg.profile.ID, []string{parentListRemovalIdentity, parentListRemovalKept, parentListRemovalIdentity, parentListRemovalScheme, parentListRemovalInvalidURL}); err != nil {
		t.Fatal(err)
	}

	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-parent-mode-master")
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
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, f.pg.users, nil, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(f.pg.sessions)
	admin.SetPGProfilePins(f.pg.pins)
	u := NewUsersHandler(f.pg.users, accountService)
	u.SetPGProfilePins(f.pg.pins)
	readVersion := func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var version string
		err := f.pg.store.Pool().QueryRow(ctx, "SELECT xmin::text FROM users WHERE id=$1", f.pg.profile.ID).Scan(&version)
		return version, err
	}
	mode := func(w http.ResponseWriter, r *http.Request) {
		f.attempts.Add(1)
		if r.Header.Get("X-Parent-Intent") != "" {
			f.retries.Add(1)
		}
		before, err := readVersion()
		if err != nil {
			t.Error("read own mode version")
			http.Error(w, "fixture unavailable", 503)
			return
		}
		reply := httptest.NewRecorder()
		u.RemoveKidsAllowedList(reply, r)
		after, err := readVersion()
		if err != nil {
			t.Error("observe own mode version")
			http.Error(w, "fixture unavailable", 503)
			return
		}
		if before != after {
			f.writes.Add(1)
		}
		for key, values := range reply.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(reply.Code)
		_, _ = w.Write(reply.Body.Bytes())
	}
	router := mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/kids-settings", admin.RequireAuth(admin.KidsSettingsPage)).Methods("GET")
		router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(admin.GetProfiles)).Methods("GET")
		// These are the same existing cookie wrapper and handler used by Main.
		// The primary /api route is checked separately through actual api.Register.
		router.HandleFunc(prefix+"/api/users/{userID}/kids/lists", admin.RequireAuth(mode)).Methods("DELETE")
	}
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/rotate", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(adminSessionCookieName)
		if err != nil {
			http.Error(w, "fixture cookie missing", 400)
			return
		}
		checked, cancel, err := f.pg.pins.Authenticate(r, cookie.Value, "")
		if err != nil {
			http.Error(w, "fixture session unavailable", 400)
			return
		}
		defer cancel()
		next, err := f.pg.sessions.RefreshProfilePinSession(checked)
		if err != nil {
			http.Error(w, "fixture refresh unavailable", 400)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookieName, Value: next.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		profile, err := f.pg.store.Users().Get(r.Context(), f.pg.profile.ID)
		if err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"profileId": profile.ID, "mode": profile.KidsMode, "kids": profile.IsKidsProfile, "lists": profile.KidsAllowedLists, "attempts": f.attempts.Load(), "retries": f.retries.Load(), "writes": f.writes.Load()})
	})
	router.HandleFunc("/fixture/seed", func(w http.ResponseWriter, r *http.Request) {
		if _, err := f.pg.users.SetKidsAllowedLists(f.pg.profile.ID, []string{parentListRemovalIdentity, parentListRemovalKept, parentListRemovalIdentity, parentListRemovalScheme, parentListRemovalInvalidURL}); err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/mutate-list", func(w http.ResponseWriter, r *http.Request) {
		if _, err := f.pg.users.AddKidsAllowedList(f.pg.profile.ID, "https://mdblist.com/lists/synthetic/concurrent"); err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/retire-actor", func(w http.ResponseWriter, r *http.Request) {
		if master {
			if _, err := f.pg.store.Pool().Exec(r.Context(), "UPDATE accounts SET is_master=false WHERE id='actor'"); err != nil {
				http.Error(w, "fixture unavailable", 503)
				return
			}
		} else {
			if _, err := f.pg.store.Pool().Exec(r.Context(), "UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'"); err != nil {
				http.Error(w, "fixture unavailable", 503)
				return
			}
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/away", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "<p>Different page</p>") })
	// Unrelated dashboard background endpoints remain local fixed fixtures.
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"needsOnboarding":false,"adminWalkthroughDismissed":true,"tasks":[],"workers":[],"defaultPassword":false}`)
	})
	f.handler = router
	return f
}

func (f *parentListRemovalBrowserFixture) server(t *testing.T) *httptest.Server {
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

func TestParentListRemovalActualCookieAliasesRequireFreshPassword(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%v", master), func(t *testing.T) {
			f := newParentListRemovalBrowserFixture(t, master)
			server := f.server(t)
			client := &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _ string, address string) (net.Conn, error) {
				if address != server.Listener.Addr().String() {
					return nil, fmt.Errorf("synthetic destination denied")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
			}}}
			t.Cleanup(client.CloseIdleConnections)
			for _, prefix := range []string{"/admin", "/account"} {
				endpoint := server.URL + prefix + "/api/users/" + f.pg.profile.ID + "/kids/lists?url=" + url.QueryEscape(parentListRemovalIdentity)
				request := func(token, password string) *http.Response {
					t.Helper()
					r, err := http.NewRequest("DELETE", endpoint, nil)
					if err != nil {
						t.Fatal(err)
					}
					r.Header.Set("Content-Type", "application/json")
					r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token})
					if token != "" {
						r.Header.Set("X-Parent-Intent", token)
					}
					if password != "" {
						r.Header.Set("X-Account-Password", password)
					}
					response, err := client.Do(r)
					if err != nil {
						t.Fatal("actual local cookie route failed")
					}
					return response
				}
				before := f.writes.Load()
				first := request("", "")
				var challenge map[string]string
				err := json.NewDecoder(first.Body).Decode(&challenge)
				first.Body.Close()
				if err != nil || first.StatusCode != 403 || challenge["code"] != "parent_authorization_required" || !regexp.MustCompile(`^pli_[0-9a-f]{64}$`).MatchString(challenge["parentIntent"]) || f.writes.Load() != before {
					t.Fatal("fresh first challenge missing or master bypassed")
				}
				retry := request(challenge["parentIntent"], "synthetic-parent-password")
				data, err := io.ReadAll(retry.Body)
				retry.Body.Close()
				var saved struct {
					ID    string   `json:"id"`
					Lists []string `json:"kidsAllowedLists"`
				}
				decoded := json.Unmarshal(data, &saved)
				if err != nil || decoded != nil || retry.StatusCode != 200 || f.writes.Load() != before+1 || saved.ID != f.pg.profile.ID || !reflect.DeepEqual(saved.Lists, []string{parentListRemovalKept, parentListRemovalScheme, parentListRemovalInvalidURL}) {
					t.Fatal("actual confirmed cookie list removal failed")
				}
			}
		})
	}
}

func TestParentListRemovalProductionTemplatesBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%v", master), func(t *testing.T) {
			f := newParentListRemovalBrowserFixture(t, master)
			server := f.server(t)
			artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifact == "" {
				artifact = t.TempDir()
			}
			role := "regular"
			if master {
				role = "master"
			}
			artifact = filepath.Join(artifact, "parent-list-removal", role)
			if err := os.MkdirAll(artifact, 0700); err != nil {
				t.Fatal(err)
			}
			scratch := "/home/stefan/ops-runbook/tmp/plr17-browser"
			if err := os.MkdirAll(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), python, "testdata/parent_list_removal_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact, "--role", role, "--profile", f.pg.profile.ID)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("synthetic list removal browser: %v\n%s", err, output)
			}
		})
	}
}
