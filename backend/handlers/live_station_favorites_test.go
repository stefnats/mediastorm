package handlers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
	user_settings "novastream/services/user_settings"
)

func stationM3U(rows ...string) string { return "#EXTM3U\n" + strings.Join(rows, "\n") + "\n" }
func stationM3URow(id, name, group, language string) string {
	return fmt.Sprintf("#EXTINF:-1 tvg-id=%q tvg-language=%q group-title=%q,%s\nhttps://example.invalid/never-open/%s", id, language, group, name, id)
}

// Persistent playlist caches outlive loopback servers whose ports can be reused.
// Keep each synthetic source distinct without changing its asserted URL path.
func stationPlaylistURL(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("stationFixture", fmt.Sprintf("%x", nonce))
	u.RawQuery = query.Encode()
	return u.String()
}

func stationChannelCall(t *testing.T, h *LiveHandler, query string, body any) LiveChannelsResponse {
	t.Helper()
	method := http.MethodGet
	var input string
	if body != nil {
		method = http.MethodPost
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		input = string(b)
	}
	r := httptest.NewRequest(method, "/api/live/channels?profileId=adult"+query, strings.NewReader(input))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "household"))
	w := httptest.NewRecorder()
	h.GetChannels(w, r)
	if w.Code != 200 {
		t.Fatalf("channels %d: %s", w.Code, w.Body.String())
	}
	var result LiveChannelsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestStationFavoritesExpandAcrossAllowedM3UListsBeforeFiltersAndPages(t *testing.T) {
	var mu sync.Mutex
	requests := map[string]int{}
	lists := map[string]string{
		"/a": stationM3U(stationM3URow("news.de", "News HD", "News", "de"), stationM3URow("RTL.de", "RTL HD", "Entertainment", "de")),
		"/b": stationM3U(stationM3URow("RTLHD.de", "RTL Television FHD", "Other", "ger"), stationM3URow("RTL.at", "RTL HD AT", "Other", "de"), stationM3URow("RTL.ch", "RTL HD CH", "Other", "de"), stationM3URow("RTLplus1.de", "RTL +1 HD", "Other", "de"), stationM3URow("RTL.de", "RTL HD English", "Other", "en"), stationM3URow("different.de", "RTL SD", "Other", "de"), stationM3URow("RTLZWEI.de", "RTLZWEI HD", "Other", "de")),
		"/c": stationM3U(stationM3URow("RTL.de", "RTL UHD", "Other", "de")),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.URL.Path]++
		mu.Unlock()
		if body, ok := lists[r.URL.Path]; ok {
			_, _ = w.Write([]byte(body))
			return
		}
		t.Errorf("unexpected stream or disallowed-source request %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	defer server.Close()
	enabled := true
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	sources := []config.LivePlaylistSource{{ID: "a", Name: "A", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL+"/a"), Enabled: &enabled}, {ID: "b", Name: "B", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL+"/b"), Enabled: &enabled}, {ID: "restricted", Name: "Restricted", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL+"/restricted"), Enabled: &enabled, AllowedProfiles: []string{"other"}}}
	if err := mgr.Save(config.Settings{Live: config.LiveSettings{Sources: sources}}); err != nil {
		t.Fatal(err)
	}
	prefs, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, prefs)
	h.SetUsersService(stationProfiles{})
	all := stationChannelCall(t, h, "", nil)
	mu.Lock()
	fetchedA, fetchedB, fetchedRestricted := requests["/a"], requests["/b"], requests["/restricted"]
	mu.Unlock()
	if fetchedA == 0 || fetchedB == 0 || fetchedRestricted != 0 {
		t.Fatal("first request did not fetch its own allowed synthetic playlists")
	}
	var station, hiddenID string
	for _, c := range all.Channels {
		if c.Name == "RTL HD" {
			station = c.StationID
		}
		if c.Name == "RTL Television FHD" {
			hiddenID = c.ID
		}
	}
	if station == "" || hiddenID == "" {
		t.Fatal("baseline station not resolved")
	}
	q := "&favoriteStationId=" + url.QueryEscape(station) + "&favoritesOnly=true&limit=1"
	first := stationChannelCall(t, h, q, nil)
	if first.Total != 2 || len(first.Channels) != 1 || first.Channels[0].StationVariants != 2 || !first.HasMore {
		t.Fatalf("station favorite membership/page: %+v", first)
	}
	second := stationChannelCall(t, h, q+"&offset=1", nil)
	if len(second.Channels) != 1 || first.Channels[0].ID == second.Channels[0].ID {
		t.Fatal("pagination duplicated or dropped alternate")
	}
	post := stationChannelCall(t, h, "", map[string]any{"profileId": "adult", "favoriteStationIds": []string{station}, "favoritesOnly": true, "categories": []string{"News"}, "groupByStation": true})
	if len(post.Channels) != 2 {
		t.Fatalf("requested soft category removed alternate: %+v", post)
	}
	explicit := stationChannelCall(t, h, q+"&sourceId=a", nil)
	if explicit.Total != 1 || explicit.Channels[0].SourceID != "a" || explicit.Channels[0].StationVariants != 1 {
		t.Fatal("explicit source selection widened")
	}
	search := stationChannelCall(t, h, q+"&filter=does-not-exist", nil)
	if search.Total != 0 {
		t.Fatal("favorites-only search pinned every station match")
	}
	if err = prefs.Update("adult", models.UserSettings{LiveTV: models.LiveTVSettings{HiddenChannels: []string{hiddenID}, FavoriteChannels: []string{"source:legacy"}}}); err != nil {
		t.Fatal(err)
	}
	visible := stationChannelCall(t, h, q, nil)
	if visible.Total != 1 || visible.Channels[0].StationVariants != 1 {
		t.Fatal("hidden alternate or count remained visible")
	}
	shown := stationChannelCall(t, h, q+"&includeHidden=true", nil)
	if shown.Total != 2 || shown.Channels[0].StationVariants != 2 {
		t.Fatal("explicit ShowHidden could not restore profile-hidden alternate")
	}
	sources = append(sources, config.LivePlaylistSource{ID: "c", Name: "C", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL+"/c"), Enabled: &enabled})
	if err = mgr.Save(config.Settings{Live: config.LiveSettings{Sources: sources}}); err != nil {
		t.Fatal(err)
	}
	added := stationChannelCall(t, h, q, nil)
	if added.Total != 2 || added.Channels[0].StationVariants != 2 {
		t.Fatalf("new allowed alternate did not join favorite automatically: %+v", added)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests["/a"] == 0 || requests["/b"] == 0 || requests["/c"] == 0 || requests["/restricted"] != 0 {
		t.Fatal("station intent pruned an alternate source or fetched denied source")
	}
}

func TestStationFavoritesPinBeforeSourceCountCapWithoutBypassingAllowedCategories(t *testing.T) {
	list := stationM3U(stationM3URow("a.de", "A", "General", "de"), stationM3URow("b.de", "B", "General", "de"), stationM3URow("RTL.de", "RTL HD", "General", "de"), stationM3URow("RTLHD.de", "RTL Television FHD", "Denied", "de"))
	var playlistRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		playlistRequests.Add(1)
		_, _ = w.Write([]byte(list))
	}))
	defer server.Close()
	enabled := true
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err := mgr.Save(config.Settings{Live: config.LiveSettings{Sources: []config.LivePlaylistSource{{ID: "a", Name: "A", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL), Enabled: &enabled}}, Filtering: config.LiveTVFilterSettings{EnabledCategories: []string{"General"}, MaxChannels: 2}}}); err != nil {
		t.Fatal(err)
	}
	h := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, nil)
	station, _ := liveStationIdentity(LiveChannel{Name: "RTL HD", TvgID: "RTL.de", TvgLanguage: "de"}, nil)
	legacy := stationChannelCall(t, h, "", nil)
	if playlistRequests.Load() != 1 {
		t.Fatal("first request did not fetch its own synthetic playlist")
	}
	if legacy.Total != 2 || legacy.Channels[0].Name != "A" || legacy.Channels[1].Name != "B" {
		t.Fatal("legacy prefix cap changed")
	}
	q := "&favoriteStationId=" + station
	all := stationChannelCall(t, h, q, nil)
	if all.Total != 2 || all.Channels[0].Name != "RTL HD" || all.Channels[0].StationVariants != 1 {
		t.Fatalf("late favorite not pinned before bounded count cap: %+v", all)
	}
	favorites := stationChannelCall(t, h, q+"&favoritesOnly=true", nil)
	if favorites.Total != 1 || favorites.Channels[0].Name != "RTL HD" {
		t.Fatal("favorites-only capped unrelated rows or included disabled category")
	}
	shown := stationChannelCall(t, h, q+"&favoritesOnly=true&includeHidden=true", nil)
	if shown.Total != 1 {
		t.Fatal("ShowHidden bypassed the administrator category allowlist")
	}
}

func TestStationFavoritesStalkerUsesBoundedCatalogueBeforeStationPinning(t *testing.T) {
	var mu sync.Mutex
	actions := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := r.URL.Query().Get("action")
		mu.Lock()
		actions[action]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch action {
		case "handshake":
			fmt.Fprint(w, `{"js":{"token":"synthetic-station-token"}}`)
		case "get_profile":
			fmt.Fprint(w, `{"js":{"id":"1"}}`)
		case "get_genres":
			fmt.Fprint(w, `{"js":[{"id":"1","title":"General"}]}`)
		case "get_all_channels":
			fmt.Fprint(w, `{"js":{"data":[{"id":"1","name":"A","tv_genre_id":"1","xmltv_id":"a.de","cmd":"ffmpeg http://example.invalid/a"},{"id":"2","name":"RTL HD","tv_genre_id":"1","xmltv_id":"RTL.de","cmd":"ffmpeg http://example.invalid/rtl"},{"id":"3","name":"RTL Television FHD","tv_genre_id":"1","xmltv_id":"RTLHD.de","cmd":"ffmpeg http://example.invalid/rtl2"}]}}`)
		default:
			t.Errorf("unexpected paging or stream-open action %s", action)
			http.Error(w, "unexpected", 400)
		}
	}))
	defer server.Close()
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err := mgr.Save(config.Settings{Live: config.LiveSettings{Sources: []config.LivePlaylistSource{{ID: "portal", Name: "Synthetic portal", Mode: "stalker", StalkerPortalURL: server.URL + "/stalker_portal/c/", StalkerMAC: "00:1a:79:00:01:16"}}}}); err != nil {
		t.Fatal(err)
	}
	h := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, nil)
	station, _ := liveStationIdentity(LiveChannel{Name: "RTL HD", TvgID: "RTL.de"}, nil)
	result := stationChannelCall(t, h, "&favoriteStationId="+station+"&category=General&limit=1", nil)
	if result.Total != 3 || len(result.Channels) != 1 || result.Channels[0].StationID != station || result.Channels[0].StationVariants != 2 {
		t.Fatalf("Stalker membership/page reduced before station pinning: %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if actions["get_all_channels"] != 1 || actions["get_ordered_list"] != 0 || actions["create_link"] != 0 {
		t.Fatal("station request used page-truncated category discovery or opened stream")
	}
}

func TestStationFavoritesChannelRequestValidation(t *testing.T) {
	for _, id := range []string{"rtl", "station:00000000000000000000000G", "station:000000000000000000000001-extra"} {
		_, _, err := normalizeLiveChannelsRequest(liveChannelsRequest{FavoriteStationIDs: []string{id}})
		if err == nil {
			t.Fatalf("accepted invalid station ID %q", id)
		}
	}
	ids := make([]string, models.MaxFavoriteStations+1)
	for i := range ids {
		ids[i] = "station:000000000000000000000001"
	}
	if _, _, err := normalizeLiveChannelsRequest(liveChannelsRequest{FavoriteStationIDs: ids}); err == nil {
		t.Fatal("over-limit station IDs accepted")
	}
	request, _, err := normalizeLiveChannelsRequest(liveChannelsRequest{FavoriteStationIDs: ids[:2]})
	if err != nil || len(request.FavoriteStationIDs) != 1 {
		t.Fatal("duplicate station IDs did not normalize")
	}
}

func TestStationFavoritesExplicitRegionalCountryNeverCollapsesIntoUnknown(t *testing.T) {
	for _, pair := range [][3]string{{"ES", "MX", "es"}, {"NL", "BE", "nl"}} {
		a := LiveChannel{ID: "a:rtl", SourceID: "a", Name: "RTL HD", TvgID: "RTL", TvgLanguage: pair[2], TvgCountry: pair[0]}
		b := a
		b.ID = "b:rtl"
		b.SourceID = "b"
		b.TvgCountry = pair[1]
		first, _ := liveStationIdentity(a, nil)
		second, _ := liveStationIdentity(b, nil)
		if first == second {
			t.Fatalf("distinct explicit broadcasts merged: %s/%s", pair[0], pair[1])
		}
		b.TvgCountry = " " + strings.ToLower(pair[0]) + " "
		if same, _ := liveStationIdentity(b, nil); same != first {
			t.Fatal("equivalent explicit ISO2 country did not normalize")
		}
	}
	legacy := LiveChannel{ID: "a", Name: "RTL HD", TvgID: "RTL.de"}
	if got, _ := liveStationIdentity(legacy, nil); got != stationID("rtl|DE|de") {
		t.Fatal("existing German station identity changed")
	}
	unknown := LiveChannel{ID: "a", SourceID: "a", Name: "RTL HD", TvgID: "RTL", TvgLanguage: "es", Group: "TV"}
	if got, _ := liveStationIdentity(unknown, nil); got != stationID("rtl||es") {
		t.Fatal("category TV was inferred as country metadata")
	}
	unknown.TvgCountry = "Atlantis"
	first, _ := liveStationIdentity(unknown, nil)
	unknown.ID = "b"
	unknown.SourceID = "b"
	second, _ := liveStationIdentity(unknown, nil)
	if first == second {
		t.Fatal("unrecognized explicit countries gained automatic shared identity")
	}
}

func TestStationFavoritesRealM3UExplicitCountryMembership(t *testing.T) {
	list := stationM3U(
		`#EXTINF:-1 tvg-id="RTL" tvg-country="ES" tvg-language="es",RTL HD`+"\nhttps://example.invalid/es",
		`#EXTINF:-1 tvg-id="RTL" tvg-country="MX" tvg-language="es",RTL HD`+"\nhttps://example.invalid/mx",
		`#EXTINF:-1 tvg-id="RTL" tvg-country="ES" tvg-language="es",RTL FHD`+"\nhttps://example.invalid/es2",
	)
	var playlistRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		playlistRequests.Add(1)
		_, _ = w.Write([]byte(list))
	}))
	defer server.Close()
	mgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err := mgr.Save(config.Settings{Live: config.LiveSettings{Mode: "m3u", PlaylistURL: stationPlaylistURL(t, server.URL)}}); err != nil {
		t.Fatal(err)
	}
	h := NewLiveHandler(server.Client(), false, "", 0, 0, 0, false, mgr, nil)
	all := stationChannelCall(t, h, "", nil)
	if playlistRequests.Load() != 1 {
		t.Fatal("first request did not fetch its own synthetic playlist")
	}
	var station string
	for _, channel := range all.Channels {
		if channel.TvgCountry == "ES" {
			station = channel.StationID
		}
	}
	if station == "" {
		t.Fatal("explicit country not parsed")
	}
	favorites := stationChannelCall(t, h, "&favoriteStationId="+station+"&favoritesOnly=true", nil)
	if favorites.Total != 2 {
		t.Fatalf("country membership: %+v", favorites)
	}
	for _, channel := range favorites.Channels {
		if channel.TvgCountry != "ES" || channel.StationVariants != 2 {
			t.Fatal("favorite expanded across explicit country boundary")
		}
	}
}
