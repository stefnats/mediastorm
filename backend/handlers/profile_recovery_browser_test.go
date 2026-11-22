package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"novastream/api/openapi"
	"novastream/config"
	"novastream/models"
	"novastream/services/accounts"
	profilerecovery "novastream/services/profile_recovery"
	"novastream/services/user_settings"
)

type recoveryDenyHTTP struct{ calls atomic.Int32 }

func (d *recoveryDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("HTTP denied outside the explicit synthetic fixture client")
}

type profileRecoveryBrowserFixture struct {
	pg            *publicPinCarrierFixture
	handler       http.Handler
	posts         atomic.Int32
	parentHeaders atomic.Int32
	mutationCalls atomic.Int32
	profilesGET   atomic.Int32
	seeds         atomic.Int32
}

// Real cookie/PG/template/asset integration, not an application Main startup or
// password login. Main's shared registration/Origin helper is covered separately.
func newProfileRecoveryBrowserFixture(t *testing.T, master bool) *profileRecoveryBrowserFixture {
	t.Helper()
	prior := http.DefaultTransport
	denied := &recoveryDenyHTTP{}
	http.DefaultTransport = denied
	t.Cleanup(func() {
		http.DefaultTransport = prior
		if denied.calls.Load() != 0 {
			t.Error("unexpected outgoing HTTP from recovery fixture")
		}
	})
	f := &profileRecoveryBrowserFixture{pg: newPublicPinCarrierFixture(t)}
	if _, err := f.pg.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=$1 WHERE id='actor'", master); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsProfile(f.pg.profile.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsMaxMovieRating(f.pg.profile.ID, "G"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pg.users.SetKidsMaxTVRating(f.pg.profile.ID, "TV-Y"); err != nil {
		t.Fatal(err)
	}
	settings, err := user_settings.NewServiceWithStore(f.pg.store)
	if err != nil {
		t.Fatal(err)
	}
	if err := settings.Update(f.pg.profile.ID, models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-recovery-master")
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
	coordinator := profilerecovery.NewService(f.pg.store, f.pg.users, settings, f.pg.pins)
	recovery := NewProfileRecoveryHandler(coordinator, func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true}
	})
	liveSettings := NewUserSettingsHandler(settings, f.pg.users, manager)
	router := mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/kids-settings", admin.RequireAuth(admin.KidsSettingsPage)).Methods("GET")
		router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			f.profilesGET.Add(1)
			admin.GetProfiles(w, r)
		})).Methods("GET")
		router.HandleFunc(prefix+"/api/users/{userID}/state/reload", func(w http.ResponseWriter, r *http.Request) {
			f.posts.Add(1)
			if r.Header.Get("X-Parent-Intent") != "" || r.Header.Get("X-Account-Password") != "" {
				f.parentHeaders.Add(1)
			}
			admin.RequireAuth(recovery.Cookie)(w, r)
		}).Methods("POST")
		router.HandleFunc(prefix+"/api/users/{userID}/kids/live-policy", admin.RequireAuth(liveSettings.KidsLivePolicy)).Methods(http.MethodGet)
		// Any accidental save fallback is observable and stays outside the
		// real Recovery handler; these are deliberately denied fixture routes.
		router.PathPrefix(prefix + "/api/users/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.mutationCalls.Add(1)
			http.Error(w, "synthetic save forbidden", 405)
		})
	}
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/seed", func(w http.ResponseWriter, r *http.Request) {
		f.seed(t, r.URL.Query().Get("kind"))
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		stored, err := f.pg.store.Users().Get(r.Context(), f.pg.profile.ID)
		if err != nil {
			http.Error(w, "synthetic state unavailable", 503)
			return
		}
		cached, _ := f.pg.users.Get(f.pg.profile.ID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": stored.ID, "storedName": stored.Name, "cachedName": cached.Name,
			"movie": stored.KidsMaxMovieRating, "tv": stored.KidsMaxTVRating, "lists": stored.KidsAllowedLists,
			"posts": f.posts.Load(), "parentHeaders": f.parentHeaders.Load(), "mutationCalls": f.mutationCalls.Load(),
			"profilesGET": f.profilesGET.Load(), "physicalDigest": f.digest(t)})
	})
	router.HandleFunc("/fixture/retire", func(w http.ResponseWriter, r *http.Request) {
		if err := f.pg.sessions.Revoke(f.pg.session.Token); err != nil {
			http.Error(w, "synthetic revoke unavailable", 503)
			return
		}
		w.WriteHeader(204)
	})
	router.HandleFunc("/fixture/away", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "<p>Other page</p>") })
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"needsOnboarding":false,"adminWalkthroughDismissed":true,"tasks":[],"workers":[],"defaultPassword":false}`)
	})
	f.handler = router
	return f
}

func (f *profileRecoveryBrowserFixture) seed(t *testing.T, kind string) {
	t.Helper()
	name := fmt.Sprintf("Stored profile %d", f.seeds.Add(1))
	movie, tv := "PG", "TV-G"
	lists := []string{"https://example.invalid/saved/'quote'?name=雪&key=\"x\""}
	if kind == "unsafe" {
		name = `<img src=x onerror="window.recoveryXSS=1"> 雪`
		movie, tv = `saved "movie" 雪`, `saved TV ß`
		lists = []string{`javascript:window.recoveryXSS=2`, `https://example.invalid/a?x='雪'`}
	}
	raw, _ := json.Marshal(lists)
	_, err := f.pg.store.Pool().Exec(t.Context(), `UPDATE users SET name=$2,kids_max_movie_rating=$3,kids_max_tv_rating=$4,kids_allowed_lists=$5,updated_at=clock_timestamp() WHERE id=$1`, f.pg.profile.ID, name, movie, tv, raw)
	if err != nil {
		t.Fatal("seed actual stored profile", err)
	}
	_, err = f.pg.store.Pool().Exec(t.Context(), `UPDATE user_settings SET settings=jsonb_set(settings,'{futureRecovery}', '{"integer":9007199254740993}'::jsonb),updated_at=clock_timestamp() WHERE user_id=$1`, f.pg.profile.ID)
	if err != nil {
		t.Fatal("seed actual stored settings", err)
	}
}

func (f *profileRecoveryBrowserFixture) digest(t *testing.T) string {
	t.Helper()
	var raw []byte
	err := f.pg.store.Pool().QueryRow(t.Context(), `SELECT jsonb_build_object(
       'users',(SELECT jsonb_agg(to_jsonb(u)||jsonb_build_object('version',concat(u.xmin::text,':',u.ctid::text)) ORDER BY id) FROM users u),
       'settings',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('version',concat(s.xmin::text,':',s.ctid::text)) ORDER BY user_id) FROM user_settings s),
       'versions',(SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object('version',concat(v.xmin::text,':',v.ctid::text)) ORDER BY user_id) FROM user_settings_versions v))`).Scan(&raw)
	if err != nil {
		t.Fatal("observe own physical rows", err)
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func (f *profileRecoveryBrowserFixture) server(t *testing.T) *httptest.Server {
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

func TestProfileRecoveryActualCookieAliases(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newProfileRecoveryBrowserFixture(t, master)
			server := f.server(t)
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _ string, address string) (net.Conn, error) {
				if address != server.Listener.Addr().String() {
					return nil, errors.New("synthetic destination denied")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
			}}}
			t.Cleanup(client.CloseIdleConnections)
			validator, err := openapi.NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, prefix := range []string{"/admin", "/account"} {
				f.seed(t, "normal")
				before := f.digest(t)
				headersBefore := f.parentHeaders.Load()
				request, err := http.NewRequest("POST", server.URL+prefix+"/api/users/"+f.pg.profile.ID+"/state/reload", nil)
				if err != nil {
					t.Fatal(err)
				}
				request.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token})
				request.Header.Set("Origin", server.URL)
				if err := validator.ValidateRequest(request, nil); err != nil {
					t.Fatal(err)
				}
				response, err := client.Do(request)
				if err != nil {
					t.Fatal("actual recovery cookie request", err)
				}
				raw, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil || response.StatusCode != 200 {
					t.Fatal("actual recovery did not succeed", response.StatusCode)
				}
				if err := validator.ValidateResponse(request, response.StatusCode, response.Header, raw); err != nil {
					t.Fatal("actual recovery response schema", err)
				}
				var dto struct {
					Profile  struct{ ID, Name string }
					Recovery struct{ UsersWriteBlocked, SettingsPendingUncertainty bool }
				}
				if json.Unmarshal(raw, &dto) != nil || dto.Profile.ID != f.pg.profile.ID || dto.Recovery.UsersWriteBlocked || dto.Recovery.SettingsPendingUncertainty {
					t.Fatal("safe recovery DTO mismatch")
				}
				cached, _ := f.pg.users.Get(f.pg.profile.ID)
				if cached.Name != dto.Profile.Name || f.digest(t) != before || f.mutationCalls.Load() != 0 || f.parentHeaders.Load() != headersBefore {
					t.Fatal("recovery wrote, retried or failed to adopt stored profile")
				}
				// Header/body errors must never become password confirmation.
				for _, bad := range []string{"body", "intent", "password"} {
					f.seed(t, "normal")
					old, _ := f.pg.users.Get(f.pg.profile.ID)
					body := ""
					if bad == "body" {
						body = "{}"
					}
					r, _ := http.NewRequest("POST", request.URL.String(), strings.NewReader(body))
					r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.pg.session.Token})
					if bad == "intent" {
						r.Header.Set("X-Parent-Intent", "pmi_"+strings.Repeat("a", 64))
					}
					if bad == "password" {
						r.Header.Set("X-Account-Password", "synthetic-unneeded")
					}
					result, e := client.Do(r)
					if e != nil {
						t.Fatal(e)
					}
					content, _ := io.ReadAll(result.Body)
					result.Body.Close()
					now, _ := f.pg.users.Get(f.pg.profile.ID)
					if result.StatusCode != 400 || now.Name != old.Name || strings.Contains(string(content), "parent_authorization_required") {
						t.Fatal("invalid recovery initiated confirmation/adoption")
					}
				}
			}
		})
	}
}

func TestProfileRecoveryProductionTemplatesBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newProfileRecoveryBrowserFixture(t, master)
			server := f.server(t)
			artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifact == "" {
				artifact = t.TempDir()
			}
			role := "regular"
			if master {
				role = "master"
			}
			artifact = filepath.Join(artifact, "profile-recovery", role)
			if err := os.MkdirAll(artifact, 0700); err != nil {
				t.Fatal(err)
			}
			scratch := "/home/stefan/ops-runbook/tmp/prc17-browser"
			if err := os.MkdirAll(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), python, "testdata/profile_recovery_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact, "--role", role, "--profile", f.pg.profile.ID)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("synthetic recovery browser: %v\n%s", err, output)
			}
		})
	}
}
