package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/user_settings"
	"novastream/services/users"
)

// This component fixture reuses the real Guide/User/Parent fixture services and
// account context from kids_live_test.go. It tests the Live form, not login,
// cookie/PG admission, unrelated dashboard APIs or actual media playback.
func kidsLivePolicyBrowserFixture(t *testing.T) http.Handler {
	t.Helper()
	guide := kidsLiveGuide(t, time.Now().Add(time.Hour))
	accountStore, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	account, err := accountStore.Create("synthetic-parent", "synthetic-parent-password")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	child, err := profiles.CreateForAccount(account.ID, "Synthetic child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.SetKidsProfile(child.ID, true); err != nil {
		t.Fatal(err)
	}
	preferences, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	initial := models.KidsLiveAgePolicy{Enabled: false, Country: "DE", MaximumAge: 12}
	if err := preferences.UpdateKidsLive(child.ID, initial); err != nil {
		t.Fatal(err)
	}
	handler := NewUserSettingsHandler(preferences, profiles, guide.config)
	handler.kidsLiveEPG = guide.guide
	handler.kidsLiveParents = parentauth.New(accountStore)
	// Extract the actual production section, rather than maintaining a second form.
	template, err := os.ReadFile(filepath.Join("admin_templates", "kids_settings.html"))
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(template, []byte(`<div class="card" id="kids-live-policy"`))
	if start < 0 {
		t.Fatal("actual Live rule section is missing")
	}
	tail := template[start:]
	end := bytes.Index(tail, []byte("<!-- Allowed Lists -->"))
	if end < 0 {
		t.Fatal("actual Live rule section boundary is missing")
	}
	section := string(tail[:end])
	var attempts, confirmed, accepted, reads atomic.Int32
	router := mux.NewRouter()
	router.HandleFunc("/component", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintf(w, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Synthetic Live rule editor</title><style>body{font-family:sans-serif;max-width:50rem;margin:1rem auto;padding:0 1rem}fieldset{display:grid;gap:1rem}label{display:block}input,select,button{max-width:100%%;padding:.5rem}</style></head><body>%s<script>const basePath = '';</script><script src="/api/static/account-parent-auth-v1.js"></script><script src="/api/static/kids-live-policy-v1.js"></script></body></html>`, section)
	}).Methods(http.MethodGet)
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", NewStaticHandler()))
	router.HandleFunc("/api/users/{userID}/kids/live-policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			attempts.Add(1)
			if r.Header.Get(parentauth.PasswordHeader) != "" {
				confirmed.Add(1)
			}
		} else {
			reads.Add(1)
		}
		response := httptest.NewRecorder()
		handler.KidsLivePolicy(response, r)
		if r.Method == http.MethodPut && response.Code == http.StatusOK {
			var reply struct {
				Policy models.KidsLiveAgePolicy `json:"policy"`
			}
			stored, readErr := preferences.Get(child.ID)
			if json.Unmarshal(response.Body.Bytes(), &reply) != nil || readErr != nil || stored == nil || stored.LiveTV.KidsAgeLimit != reply.Policy {
				t.Error("actual successful policy response does not match saved fixture state")
				http.Error(w, "fixture save mismatch", 500)
				return
			}
			accepted.Add(1)
		}
		for key, values := range response.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}).Methods(http.MethodGet, http.MethodPut)
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		stored, err := preferences.Get(child.ID)
		if err != nil || stored == nil {
			http.Error(w, "fixture state unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"profile": child.ID, "guide": guide.source, "policy": stored.LiveTV.KidsAgeLimit, "attempts": attempts.Load(), "confirmed": confirmed.Load(), "acceptedWrites": accepted.Load(), "policyReads": reads.Load(), "guideRequests": guide.requests.Load()})
	}).Methods(http.MethodGet)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { router.ServeHTTP(w, kidsLiveRequest(r, account.ID)) })
}

func TestKidsLivePolicyEditorBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("set MEDIASTORM_BROWSER_PYTHON and MEDIASTORM_CHROMIUM for this synthetic component browser test")
	}
	server := httptest.NewServer(kidsLivePolicyBrowserFixture(t))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	artifacts = filepath.Join(artifacts, "kids-live-policy")
	if err := os.MkdirAll(artifacts, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := os.Getenv("MEDIASTORM_BROWSER_TMPDIR")
	if scratch == "" {
		scratch = t.TempDir()
	}
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	script := os.Getenv("MEDIASTORM_KIDS_LIVE_BROWSER_SCRIPT")
	if script == "" {
		script = filepath.Join("testdata", "kids_live_policy_browser.py")
	}
	cmd := exec.CommandContext(t.Context(), python, script, "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("synthetic Live rule component browser: %v\n%s", err, output)
	}
}
