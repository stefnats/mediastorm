package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"novastream/api/openapi"
	"novastream/config"
	"novastream/models"
	user_settings "novastream/services/user_settings"
)

func stationFavoritesOpenAPIContract(t *testing.T, v *openapi.Validator) {
	s, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Update("adult", models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteChannels: []string{"legacy:stream"}}}); err != nil {
		t.Fatal(err)
	}
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err = mgr.Save(config.Settings{}); err != nil {
		t.Fatal(err)
	}
	h := NewUserSettingsHandler(s, stationProfiles{}, mgr)
	station, _ := liveStationIdentity(LiveChannel{Name: "RTL HD", TvgID: "RTL.de", TvgLanguage: "de"}, nil)
	call := contractCall{method: "PUT", target: "/api/users/adult/live-tv/station-favorites", vars: map[string]string{"userID": "adult"}, master: true, body: map[string]any{"stationId": station, "favorite": true}}
	favorite := exercise(t, v, call, h.SetStationFavorite, 200)
	settings := exercise(t, v, contractCall{method: "GET", target: "/api/users/adult/settings", vars: call.vars, master: true}, h.GetSettings, 200)
	call.body = map[string]any{"stationId": station}
	call.invalid = true
	exercise(t, v, call, h.SetStationFavorite, 400)
	call.body = map[string]any{"stationId": station, "favorite": nil}
	exercise(t, v, call, h.SetStationFavorite, 400)
	call.body = map[string]any{"stationId": station, "favorite": false}
	call.invalid = false
	empty := exercise(t, v, call, h.SetStationFavorite, 200)
	var playlistReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		playlistReads.Add(1)
		_, _ = w.Write([]byte(stationM3U(stationM3URow("RTL.de", "RTL HD", "General", "de"), stationM3URow("RTLHD.de", "RTL Television FHD", "General", "de"))))
	}))
	defer server.Close()
	if err = mgr.Save(config.Settings{Live: config.LiveSettings{Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL)}}); err != nil {
		t.Fatal(err)
	}
	live := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, s)
	channels := stationChannelCall(t, live, "&favoriteStationId="+station+"&favoritesOnly=true&limit=1", nil)
	if playlistReads.Load() != 1 {
		t.Fatalf("initial playlist reads=%d", playlistReads.Load())
	}
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	channelBody, err := json.Marshal(channels)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err = json.Unmarshal(channelBody, &decoded); err != nil {
		t.Fatal(err)
	}
	if err = doc.Components.Schemas["StationFavoritesChannelsResponse"].Value.VisitJSON(decoded); err != nil {
		t.Fatal(err)
	}
	emptyChannels := stationChannelCall(t, live, "&favoriteStationId=station:000000000000000000000000&favoritesOnly=true", nil)
	if emptyChannels.Channels == nil {
		t.Fatal("empty HTTP catalogue did not preserve an array")
	}
	emptyBody, err := json.Marshal(emptyChannels)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(emptyBody, &decoded); err != nil {
		t.Fatal(err)
	}
	if err = doc.Components.Schemas["StationFavoritesChannelsResponse"].Value.VisitJSON(decoded); err != nil {
		t.Fatal("legacy empty channel projection", err)
	}
	request := map[string]any{"profileId": "adult", "favoriteStationIds": []string{station}, "favoritesOnly": true, "includeHidden": true, "limit": 1}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err = doc.Components.Schemas["StationFavoritesChannelsRequest"].Value.VisitJSON(decoded); err != nil {
		t.Fatal(err)
	}
	if dir := os.Getenv("STATION_FAVORITES_CONTRACT_DIR"); dir != "" {
		if !filepath.IsAbs(dir) {
			t.Fatal("contract path must be absolute")
		}
		if err = os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string][]byte{"settings-supported.json": settings.Body.Bytes(), "favorite-snapshot.json": favorite.Body.Bytes(), "empty-snapshot.json": empty.Body.Bytes(), "channels-station-favorites.json": channelBody, "channels-request.json": raw} {
			if err = os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
