package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/client_settings"
	"novastream/services/clients"
	"novastream/services/user_settings"
)

func TestMinimumResolutionFrontendHTTP(t *testing.T) {
	for _, scope := range []string{"profile", "device"} {
		t.Run(scope, func(t *testing.T) {
			manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
			cfg := config.DefaultSettings()
			cfg.Filtering.MinResolution = "2160p"
			cfg.UI.UserEditableSettings = []string{"filtering.minResolution"}
			if err := manager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			profiles, err := user_settings.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := profiles.Update("profile", models.UserSettings{Filtering: models.FilterSettings{MaxResolution: models.StringPtr("1080p")}}); err != nil {
				t.Fatal(err)
			}
			devices, err := client_settings.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := devices.Update("device", "profile", models.ClientFilterSettings{MaxResolution: models.StringPtr("1080p")}); err != nil {
				t.Fatal(err)
			}
			clientService, err := clients.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := clientService.Register("device", "profile", "Android TV", "Android", "1.0", "Synthetic TV", ""); err != nil {
				t.Fatal(err)
			}
			profileHandler := handlers.NewUserSettingsHandler(profiles, &fakeUserExistsService{exists: true}, manager)
			clientHandler := handlers.NewClientsHandler(clientService, devices, &fakeClientOwnership{membership: map[string]map[string]bool{"acct-1": {"profile": true}}})
			clientHandler.SetConfigManager(manager)
			router := mux.NewRouter()
			router.HandleFunc("/api/settings", handlers.NewSettingsHandler(manager).GetSettings).Methods(http.MethodGet)
			router.HandleFunc("/api/profiles/{userID}/settings/frontend", profileHandler.PatchFrontendSetting).Methods(http.MethodPatch)
			router.HandleFunc("/api/clients/{clientID}/settings/frontend", clientHandler.PatchFrontendSetting).Methods(http.MethodPatch)
			settingsResponse := httptest.NewRecorder()
			router.ServeHTTP(settingsResponse, clientsAuthRequest(http.MethodGet, "/api/settings", nil, nil, "acct-1", true))
			if settingsResponse.Code != http.StatusOK {
				t.Fatalf("settings: %d %s", settingsResponse.Code, settingsResponse.Body.String())
			}
			var settings struct {
				UserEditableSettings       []string                                      `json:"userEditableSettings"`
				UserEditableSettingsSchema map[string]handlers.UserEditableSettingSchema `json:"userEditableSettingsSchema"`
			}
			if err := json.Unmarshal(settingsResponse.Body.Bytes(), &settings); err != nil {
				t.Fatal(err)
			}
			schema, found := settings.UserEditableSettingsSchema["filtering.minResolution"]
			if !found || schema.Type != "select" || !slices.Contains(settings.UserEditableSettings, "filtering.minResolution") || !slices.Contains(schema.Scopes, "profile") || !slices.Contains(schema.Scopes, "device") {
				t.Fatalf("minimum missing from real frontend schema: %+v", settings)
			}
			path := "/api/profiles/profile/settings/frontend"
			if scope == "device" {
				path = "/api/clients/device/settings/frontend?userId=profile"
			}
			for _, minimum := range []string{"720p", ""} {
				w := httptest.NewRecorder()
				r := clientsAuthRequest(http.MethodPatch, path, map[string]any{"path": "filtering.minResolution", "value": minimum}, nil, "acct-1", true)
				router.ServeHTTP(w, r)
				if w.Code != http.StatusOK {
					t.Fatalf("%s minimum=%q: %d %s", scope, minimum, w.Code, w.Body.String())
				}
				var got, maximum *string
				if scope == "profile" {
					stored, err := profiles.Get("profile")
					if err != nil || stored == nil {
						t.Fatalf("profile storage: %v", err)
					}
					got, maximum = stored.Filtering.MinResolution, stored.Filtering.MaxResolution
				} else {
					stored, err := devices.Get("device", "profile")
					if err != nil || stored == nil {
						t.Fatalf("device storage: %v", err)
					}
					got, maximum = stored.MinResolution, stored.MaxResolution
				}
				if got == nil || *got != minimum || maximum == nil || *maximum != "1080p" {
					t.Fatalf("%s minimum=%v maximum=%v; explicit empty must disable inheritance without changing maximum", scope, got, maximum)
				}
			}
			// The normal global-save cleanup must not turn an explicit "no minimum"
			// into inheritance merely because the parent's value is also empty now.
			cfg.Filtering.MinResolution = ""
			if err := manager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			profiles.StripRedundantOverrides(cfg, clientService, devices)
			cfg.Filtering.MinResolution = "2160p"
			if err := manager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			profiles.StripRedundantOverrides(cfg, clientService, devices)
			if scope == "profile" {
				stored, err := profiles.GetWithDefaults("profile", models.UserSettings{Filtering: models.FilterSettings{MinResolution: models.StringPtr(cfg.Filtering.MinResolution)}})
				if err != nil || stored.Filtering.MinResolution == nil || *stored.Filtering.MinResolution != "" || models.StringVal(stored.Filtering.MaxResolution, "") != "1080p" {
					t.Fatalf("profile disable changed after parent save/cleanup: %+v %v", stored.Filtering, err)
				}
			} else {
				stored, err := devices.Get("device", "profile")
				if err != nil || stored == nil || stored.MinResolution == nil || *stored.MinResolution != "" {
					t.Fatalf("device disable changed after parent save/cleanup: %+v %v", stored, err)
				}
				// Existing maximum-resolution cleanup may inherit an equal profile
				// value. Its effective limit must remain unchanged, while the new
				// explicit minimum-disable override must remain stored above.
				profile, err := profiles.GetWithDefaults("profile", models.UserSettings{Filtering: models.FilterSettings{MaxResolution: models.StringPtr(cfg.Filtering.MaxResolution)}})
				if err != nil {
					t.Fatal(err)
				}
				effectiveMaximum := models.StringVal(stored.MaxResolution, models.StringVal(profile.Filtering.MaxResolution, cfg.Filtering.MaxResolution))
				if effectiveMaximum != "1080p" {
					t.Fatalf("effective device maximum changed to %q", effectiveMaximum)
				}
			}
		})
	}
}
