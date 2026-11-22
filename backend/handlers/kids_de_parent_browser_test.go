package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/kids"
	"novastream/services/user_settings"
)

// This fixture uses the actual cookie, PG, handlers, template and static assets.
// Only unrelated dashboard reads and explicit synthetic seeding are local fixtures.
func kidsDEParentBrowserFixture(t *testing.T, master bool) (*httptest.Server, string) {
	t.Helper()
	kidsCookieNoHTTP(t)
	pg := newPublicPinCarrierFixture(t)
	if _, err := pg.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=$1 WHERE id='actor'", master); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.users.SetKidsProfile(pg.profile.ID, true); err != nil {
		t.Fatal(err)
	}
	seed := func(empty bool) error {
		if _, err := pg.users.SetKidsMode(pg.profile.ID, "rating"); err != nil {
			return err
		}
		if _, err := pg.users.SetKidsMaxRating(pg.profile.ID, "PG"); err != nil {
			return err
		}
		movie, series := "G", "TV-Y"
		if empty {
			movie, series = "", ""
		}
		_, err := pg.users.SetKidsMaxRatings(pg.profile.ID, movie, series)
		return err
	}
	if err := seed(false); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-kids-de-master")
	accountService, err := accounts.NewServiceWithStore(pg.store)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := user_settings.NewServiceWithStore(pg.store)
	if err != nil {
		t.Fatal(err)
	}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	cfg := config.DefaultSettings()
	cfg.UI.OnboardingCompleted, cfg.UI.AdminWalkthroughDismissed = true, true
	if err := manager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, pg.users, settings, manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(pg.sessions)
	admin.SetPGProfilePins(pg.pins)
	usersHandler := NewUsersHandler(pg.users, accountService)
	usersHandler.SetPGProfilePins(pg.pins)
	liveSettings := NewUserSettingsHandler(settings, pg.users, manager)
	var modeAttempts, modeRetries, modeWrites, pairAttempts, pairRetries, pairWrites, unexpected, policyReads atomic.Int32
	readVersion := func(r *http.Request) (string, error) {
		var value string
		err := pg.store.Pool().QueryRow(r.Context(), "SELECT xmin::text FROM users WHERE id=$1", pg.profile.ID).Scan(&value)
		return value, err
	}
	observe := func(next http.HandlerFunc, attempts, retries, writes *atomic.Int32) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			if r.Header.Get("X-Parent-Intent") != "" {
				retries.Add(1)
			}
			before, err := readVersion(r)
			if err != nil {
				t.Error("read own profile version", err)
				http.Error(w, "fixture unavailable", 503)
				return
			}
			reply := httptest.NewRecorder()
			next(reply, r)
			after, err := readVersion(r)
			if err != nil {
				t.Error("observe own profile version", err)
				http.Error(w, "fixture unavailable", 503)
				return
			}
			if before != after {
				writes.Add(1)
			}
			for name, values := range reply.Header() {
				for _, value := range values {
					w.Header().Add(name, value)
				}
			}
			w.WriteHeader(reply.Code)
			_, _ = w.Write(reply.Body.Bytes())
		}
	}
	router := mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/kids-settings", admin.RequireAuth(admin.KidsSettingsPage)).Methods(http.MethodGet)
		router.HandleFunc(prefix+"/api/profiles", admin.RequireAuth(admin.GetProfiles)).Methods(http.MethodGet)
		router.HandleFunc(prefix+"/api/users/{userID}/kids/mode", admin.RequireAuth(observe(usersHandler.SetKidsMode, &modeAttempts, &modeRetries, &modeWrites))).Methods(http.MethodPut)
		router.HandleFunc(prefix+"/api/users/{userID}/kids/ratings", admin.RequireAuth(observe(usersHandler.SetKidsRatings, &pairAttempts, &pairRetries, &pairWrites))).Methods(http.MethodPut)
		router.HandleFunc(prefix+"/api/users/{userID}/kids/live-policy", admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			policyReads.Add(1)
			liveSettings.KidsLivePolicy(w, r)
		})).Methods(http.MethodGet)
		router.PathPrefix(prefix + "/api/users/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			unexpected.Add(1)
			http.Error(w, "unexpected profile operation", 405)
		})
	}
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookieName, Value: pg.session.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodGet)
	router.HandleFunc("/fixture/seed", func(w http.ResponseWriter, r *http.Request) {
		if err := seed(r.URL.Query().Get("kind") == "empty"); err != nil {
			t.Error("seed own profile", err)
			http.Error(w, "fixture unavailable", 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodGet)
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		profile, err := pg.store.Users().Get(r.Context(), pg.profile.ID)
		if err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		var raw []byte
		if err := pg.store.Pool().QueryRow(r.Context(), "SELECT to_jsonb(u)::text || ':' || xmin::text FROM users u WHERE id=$1", pg.profile.ID).Scan(&raw); err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		digest := sha256.Sum256(raw)
		at := time.Now().UTC()
		expires := at.Add(time.Hour)
		title := func(kind, scope string, age string) models.Title {
			id := "tmdb:" + kind + ":41"
			return models.Title{ID: id, TMDBID: 41, MediaType: kind, CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: age, Source: "tmdb", SourceType: "metadata", SourceTitleID: id, Scope: scope, RetrievedAt: at, ExpiresAt: &expires}}}
		}
		movieLimit, seriesLimit, enabled := kids.RatingLimits(*profile)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode": profile.KidsMode, "movie": profile.KidsMaxMovieRating, "series": profile.KidsMaxTVRating, "generic": profile.KidsMaxRating,
			"rowDigest": hex.EncodeToString(digest[:]), "modeAttempts": modeAttempts.Load(), "modeRetries": modeRetries.Load(), "modeWrites": modeWrites.Load(),
			"pairAttempts": pairAttempts.Load(), "pairRetries": pairRetries.Load(), "pairWrites": pairWrites.Load(), "unexpected": unexpected.Load(), "policyReads": policyReads.Load(),
			"knownMovie6Allowed":   enabled && kids.IsTitleRatingAllowed(title("movie", "movie_release", "6"), movieLimit),
			"knownSeries12Allowed": enabled && kids.IsTitleRatingAllowed(title("series", "series", "12"), seriesLimit),
		})
	}).Methods(http.MethodGet)
	// Dashboard read fixtures never acknowledge a profile write.
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			unexpected.Add(1)
			http.Error(w, "unexpected fixture write", 405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"needsOnboarding":false,"adminWalkthroughDismissed":true,"tasks":[],"workers":[],"defaultPassword":false}`)
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(router)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server, pg.profile.ID
}

func TestKidsDEParentEditorBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			server, profile := kidsDEParentBrowserFixture(t, master)
			role := "regular"
			if master {
				role = "master"
			}
			artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifact == "" {
				artifact = t.TempDir()
			}
			artifact = filepath.Join(artifact, "kids-de-parent", role)
			scratch := os.Getenv("MEDIASTORM_BROWSER_TMPDIR")
			if scratch == "" {
				scratch = "/home/stefan/ops-runbook/tmp/mediastorm-tmdb-de-profile-filter-20261122/browser-scratch"
			}
			for _, directory := range []string{artifact, scratch} {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.CommandContext(t.Context(), python, "testdata/kids_de_parent_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact, "--profile", profile)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual DE parent editor browser: %v\n%s", err, output)
			}
		})
	}
}
