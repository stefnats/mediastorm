package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"novastream/config"
	"novastream/handlers"
	"novastream/services/user_settings"
	"novastream/services/users"
)

func TestStationFavoritesRegisteredRouteRequiresOwnedUnlockedAdultFullSession(t *testing.T) {
	settings, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err = mgr.Save(config.Settings{}); err != nil {
		t.Fatal(err)
	}
	f := newKidsRatingRouteFixture(t, func(profiles *users.Service) *handlers.UserSettingsHandler {
		return handlers.NewUserSettingsHandler(settings, profiles, mgr)
	})
	if _, err = f.users.SetKidsProfile(f.profile, false); err != nil {
		t.Fatal(err)
	}
	suffix := "/live-tv/station-favorites"
	body := `{"stationId":"station:000000000000000000000001","favorite":true}`
	for _, tc := range []struct{ name, profile, token, unlock string }{{"anonymous", f.profile, "", ""}, {"scoped", f.profile, f.scoped, f.unlock}, {"foreign", f.foreign, f.token, f.unlock}, {"locked", f.profile, f.token, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			w := f.request(http.MethodPut, tc.profile, suffix, tc.token, tc.unlock, "", body)
			if w.Code == 200 || w.Code == 404 && tc.name != "foreign" {
				t.Fatalf("route missing or guard bypassed: %d %s", w.Code, w.Body.String())
			}
		})
	}
	accepted := f.request(http.MethodPut, f.profile, suffix, f.token, f.unlock, "", body)
	if accepted.Code != 200 {
		t.Fatalf("registered route: %d %s", accepted.Code, accepted.Body.String())
	}
	w := f.request(http.MethodGet, f.profile, "/settings", f.token, f.unlock, "", "")
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var result map[string]any
	if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["stationFavoritesSupported"] != true {
		t.Fatal("effective settings marker absent")
	}
	if _, err = f.users.SetKidsProfile(f.profile, true); err != nil {
		t.Fatal(err)
	}
	blocked := f.request(http.MethodPut, f.profile, suffix, f.token, f.unlock, "", body)
	if blocked.Code != 403 {
		t.Fatalf("kids route: %d %s", blocked.Code, blocked.Body.String())
	}
	stored, _ := settings.Get(f.profile)
	if stored == nil || len(stored.LiveTV.FavoriteStationIDs) != 1 {
		t.Fatal("rejected writes altered saved station set")
	}
}
