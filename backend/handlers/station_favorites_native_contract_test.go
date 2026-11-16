package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"novastream/api/openapi"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
	user_settings "novastream/services/user_settings"
)

// Native production StationFavoritesApi sends PUT to the profile path;
// LiveApi sends POST /api/live/channels with no query selectors. These five
// byte-for-byte exports come from their actual production body helpers.
func TestNativeStationFavoriteTransportMatchesRealHandlersAndSchema(t *testing.T) {
	paths, err := filepath.Glob("testdata/station_favorites_native/native-*.json")
	if err != nil || len(paths) != 5 {
		t.Fatal("five native transport exports are required")
	}
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("testdata/station_favorites_native", name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	prefs, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	var playlistReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		playlistReads.Add(1)
		if r.Method != "GET" || r.URL.Path != "/playlist" {
			t.Errorf("unexpected provider or stream action %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(stationM3U(stationM3URow("news.de", "News HD", "News", "de"), stationM3URow("RTL.de", "RTL HD", "General", "de"), stationM3URow("RTLHD.de", "RTL Television FHD", "General", "de"))))
	}))
	defer server.Close()
	if err = mgr.Save(config.Settings{Live: config.LiveSettings{Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL+"/playlist")}}); err != nil {
		t.Fatal(err)
	}
	live := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, prefs)
	live.SetUsersService(stationProfiles{})
	initial := stationChannelCall(t, live, "", nil)
	if playlistReads.Load() != 1 {
		t.Fatalf("initial playlist reads=%d", playlistReads.Load())
	}
	var station, hidden string
	for _, channel := range initial.Channels {
		if channel.Name == "RTL HD" {
			station = channel.StationID
		}
		if channel.Name == "RTL Television FHD" {
			hidden = channel.ID
		}
	}
	if station == "" || hidden == "" {
		t.Fatal("synthetic baseline membership missing")
	}
	if err = prefs.Update("adult", models.UserSettings{LiveTV: models.LiveTVSettings{HiddenChannels: []string{hidden}, FavoriteChannels: []string{"legacy:unrelated"}}}); err != nil {
		t.Fatal(err)
	}
	settings := NewUserSettingsHandler(prefs, stationProfiles{}, mgr)
	write := func(name string, want bool) {
		t.Helper()
		raw := read(name)
		call := contractCall{method: "PUT", target: "/api/users/adult/live-tv/station-favorites", vars: map[string]string{"userID": "adult"}, master: true, body: raw}
		response := exercise(t, validator, call, settings.SetStationFavorite, 200)
		var snapshot struct {
			IDs []string `json:"favoriteStationIds"`
		}
		if err = json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.IDs == nil || want && (len(snapshot.IDs) != 1 || snapshot.IDs[0] != station) || !want && len(snapshot.IDs) != 0 {
			t.Fatalf("native desired-state mutation not applied: %s", response.Body.String())
		}
	}
	write("native-station-add.json", true)
	for _, name := range []string{"native-channels-favorites.json", "native-channels-supported-all.json", "native-channels-legacy.json"} {
		t.Run(name, func(t *testing.T) {
			raw := read(name)
			var body map[string]any
			if err = json.Unmarshal(raw, &body); err != nil {
				t.Fatal(err)
			}
			if err = doc.Components.Schemas["StationFavoritesChannelsRequest"].Value.VisitJSON(body); err != nil {
				t.Fatal(err)
			}
			newRequest := func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/api/live/channels", strings.NewReader(string(raw)))
				r.Header.Set("Content-Type", "application/json")
				return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "household"))
			}
			request := newRequest()
			if request.Method != "POST" || request.URL.Path != "/api/live/channels" || request.URL.RawQuery != "" {
				t.Fatal("native transport selector changed")
			}
			parsed, paginated, err := parseLiveChannelsRequest(httptest.NewRecorder(), request)
			if err != nil || !paginated || parsed.ProfileID != "adult" || *parsed.Limit != 1 || *parsed.Offset != 0 {
				t.Fatalf("native channel parser: %+v %v", parsed, err)
			}
			w := httptest.NewRecorder()
			live.GetChannels(w, newRequest())
			if w.Code != 200 {
				t.Fatalf("native channel handler %d %s", w.Code, w.Body.String())
			}
			var response LiveChannelsResponse
			if err = json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			var decoded any
			if err = json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if err = doc.Components.Schemas["StationFavoritesChannelsResponse"].Value.VisitJSON(decoded); err != nil {
				t.Fatal(err)
			}
			_, present := body["favoriteStationIds"]
			switch name {
			case "native-channels-favorites.json":
				if !present || !parsed.IncludeHidden || len(parsed.FavoriteStationIDs) != 1 || response.Total != 2 || len(response.Channels) != 1 || response.Channels[0].StationID != station || response.Channels[0].StationVariants != 2 || !response.HasMore {
					t.Fatalf("native opt-in/ShowHidden/page contract: %+v", response)
				}
			case "native-channels-supported-all.json":
				if !present || len(body["favoriteStationIds"].([]any)) != 0 || len(parsed.FavoriteStationIDs) != 0 || response.Total != 3 || response.Channels[0].Name != "News HD" {
					t.Fatal("supported empty station IDs changed legacy full-catalog prefix/order")
				}
			case "native-channels-legacy.json":
				if present || body["includeHidden"] != nil || len(parsed.FavoriteStationIDs) != 0 || response.Total != 0 || response.Channels == nil || len(response.Channels) != 0 {
					t.Fatal("legacy request silently expanded persisted station favorites")
				}
			}
			stored, _ := prefs.Get("adult")
			if stored == nil || len(stored.LiveTV.FavoriteStationIDs) != 1 {
				t.Fatal("read mutated persisted station favorite")
			}
		})
	}
	write("native-station-remove.json", false)
	stored, _ := prefs.Get("adult")
	if stored == nil || len(stored.LiveTV.FavoriteChannels) != 1 || len(stored.LiveTV.HiddenChannels) != 1 {
		t.Fatal("native station removal altered legacy settings")
	}
}
