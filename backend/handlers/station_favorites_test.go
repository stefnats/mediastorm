package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	user_settings "novastream/services/user_settings"
)

type stationProfiles struct{}

func (stationProfiles) Exists(id string) bool { return id == "adult" || id == "child" }
func (stationProfiles) BelongsToAccount(id, account string) bool {
	return stationProfiles{}.Exists(id) && account == "household"
}
func (stationProfiles) Get(id string) (models.User, bool) {
	return models.User{ID: id, AccountID: "household", IsKidsProfile: id == "child"}, stationProfiles{}.Exists(id)
}

func stationFavoriteRequest(profile, account, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPut, "/api/users/"+profile+"/live-tv/station-favorites", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"userID": profile})
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, account))
}

func TestStationFavoritesHTTPStrictIsolationAndIndependentLegacyFavorites(t *testing.T) {
	s, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Update("adult", models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteChannels: []string{"source:one"}}}); err != nil {
		t.Fatal(err)
	}
	h := NewUserSettingsHandler(s, stationProfiles{}, nil)
	body := `{"stationId":"station:000000000000000000000001","favorite":true}`
	for _, tc := range []struct {
		profile, account, body string
		want                   int
	}{{"adult", "household", body, 200}, {"adult", "other", body, 404}, {"child", "household", body, 403}, {"unknown", "household", body, 404}, {"adult", "household", `{}`, 400}, {"adult", "household", `null`, 400}, {"adult", "household", `{"stationId":"station:000000000000000000000001","favorite":null}`, 400}, {"adult", "household", `{"stationId":"station:000000000000000000000001","favorite":false,"extra":1}`, 400}, {"adult", "household", body + ` {}`, 400}, {"adult", "household", `{"stationId":"rtl","favorite":true}`, 400}, {"adult", "household", `{"stationId":"station:000000000000000000000001","favorite":"true"}`, 400}} {
		w := httptest.NewRecorder()
		h.SetStationFavorite(w, stationFavoriteRequest(tc.profile, tc.account, tc.body))
		if w.Code != tc.want {
			t.Fatalf("%s/%s %s: %d %s", tc.profile, tc.account, tc.body, w.Code, w.Body.String())
		}
	}
	got, _ := s.Get("adult")
	if len(got.LiveTV.FavoriteStationIDs) != 1 || len(got.LiveTV.FavoriteChannels) != 1 {
		t.Fatal("individual favorites replaced")
	}
	stale := stationFavoriteRequest("adult", "household", `{"liveTV":{"favoriteChannels":["source:one"],"favoriteStationIds":["station:000000000000000000000009"]}}`)
	stale.Method = http.MethodPut
	staleReply := httptest.NewRecorder()
	h.PutSettings(staleReply, stale)
	var saved models.UserSettings
	if err = json.Unmarshal(staleReply.Body.Bytes(), &saved); err != nil || len(saved.LiveTV.FavoriteStationIDs) != 1 || saved.LiveTV.FavoriteStationIDs[0] != "station:000000000000000000000001" {
		t.Fatal("settings PUT echoed stale station favorites instead of actual saved set")
	}
	for _, mode := range []string{"locked", "stream"} {
		r := stationFavoriteRequest("adult", "household", body)
		if mode == "locked" {
			r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, _ *http.Request, _ string) bool {
				http.Error(w, "locked", 403)
				return false
			})
		} else {
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
		}
		w := httptest.NewRecorder()
		profileaccess.Middleware(stationProfiles{}.BelongsToAccount)(http.HandlerFunc(h.SetStationFavorite)).ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("%s: %d", mode, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.SetStationFavorite(w, stationFavoriteRequest("adult", "household", strings.Replace(body, `true`, `false`, 1)))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var result struct {
		IDs []string `json:"favoriteStationIds"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.IDs == nil || len(result.IDs) != 0 {
		t.Fatal("false was omitted or empty response was not an array")
	}
	cleared := stationFavoriteRequest("adult", "household", `{"liveTV":{"favoriteStationIds":["station:000000000000000000000009"]}}`)
	clearedReply := httptest.NewRecorder()
	h.PutSettings(clearedReply, cleared)
	var empty models.UserSettings
	if err = json.Unmarshal(clearedReply.Body.Bytes(), &empty); err != nil || len(empty.LiveTV.FavoriteStationIDs) != 0 {
		t.Fatal("stale PUT echoed a removed last station bookmark")
	}
}
