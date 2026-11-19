package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httptest"
	"novastream/config"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/backup"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegionalPolicyActualCookieWrappersRequireOriginAndFreshPasswordForBothAliases(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%v", master), func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			if _, err := f.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master); err != nil {
				t.Fatal(err)
			}
			t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-regional-master")
			accountService, err := accounts.NewServiceWithStore(f.store)
			if err != nil {
				t.Fatal(err)
			}
			manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
			cfg := config.DefaultSettings()
			cfg.UI.OnboardingCompleted = true
			if err = manager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, f.users, nil, manager)
			admin.SetAccountsService(accountService)
			admin.SetSessionsService(f.sessions)
			admin.SetPGProfilePins(f.pins)
			handler := NewUsersHandler(f.users, accountService)
			handler.SetPGProfilePins(f.pins)
			router := mux.NewRouter()
			for _, prefix := range []string{"/admin", "/account"} {
				router.HandleFunc(prefix+"/api/users/{userID}/kids/regional-policy", admin.RequireAuth(handler.RegionalChildPolicy)).Methods("GET", "PUT")
			}
			for _, prefix := range []string{"/admin", "/account"} {
				path := prefix + "/api/users/" + f.profile.ID + "/kids/regional-policy"
				body := []byte(`{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}`)
				call := func(origin, intent, password string) *httptest.ResponseRecorder {
					r := httptest.NewRequest(http.MethodPut, "http://synthetic.test"+path, bytes.NewReader(body))
					r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
					if origin != "" {
						r.Header.Set("Origin", origin)
						r.Header.Set("Sec-Fetch-Site", "same-origin")
					}
					if intent != "" {
						r.Header.Set("X-Parent-Intent", intent)
					}
					if password != "" {
						r.Header.Set("X-Account-Password", password)
					}
					w := httptest.NewRecorder()
					router.ServeHTTP(w, r)
					return w
				}
				if got := call("", "", ""); got.Code != 403 || strings.Contains(got.Body.String(), "parentIntent") {
					t.Fatal("cookie write without origin reached confirmation")
				}
				if got := call("http://foreign.invalid", "", ""); got.Code != 403 {
					t.Fatal("foreign cookie origin accepted")
				}
				first := call("http://synthetic.test", "", "")
				var challenge struct {
					Intent string `json:"parentIntent"`
				}
				if first.Code != 403 || json.Unmarshal(first.Body.Bytes(), &challenge) != nil || !strings.HasPrefix(challenge.Intent, "prf_") {
					t.Fatalf("fresh parent challenge missing for cookie alias: HTTP %d", first.Code)
				}
				// The synthetic fixture actor uses the same current credential as its other cookie tests.
				saved := call("http://synthetic.test", challenge.Intent, "synthetic-parent-password")
				var policy models.RegionalChildPolicy
				if saved.Code != 200 || json.Unmarshal(saved.Body.Bytes(), &policy) != nil || policy.MaxMovieAge != 6 || policy.MaxEpisodeAge != 12 {
					t.Fatalf("current cookie regional mutation failed: HTTP %d", saved.Code)
				}
			}
		})
	}
}

func TestRegionalPolicyProtectedFamilyImportReturnsForbiddenAndLeavesOriginalRows(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	service, err := backup.NewService(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	service.SetDataStore(f.store)
	raw, err := service.ExportDatabaseJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id=$1`, f.profile.ID); err != nil {
		t.Fatal(err)
	}
	handler := NewBackupHandler(service)
	request := httptest.NewRequest(http.MethodPost, "/admin/api/backups/import", bytes.NewReader(raw))
	reply := httptest.NewRecorder()
	handler.ImportData(reply, request)
	if reply.Code != http.StatusForbidden {
		t.Fatalf("protected import was not forbidden: HTTP %d", reply.Code)
	}
	current, err := f.store.Users().Get(t.Context(), f.profile.ID)
	if err != nil || current == nil || !current.IsKidsProfile {
		t.Fatal("forbidden import replaced current family profile")
	}
}
