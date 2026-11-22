package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/playback"
)

func TestLocalCombinedEpisodePlaybackUsesFileProgressWithoutOffsets(t *testing.T) {
	item := &models.LocalMediaItem{ID: "physical-file", FilePath: "/synthetic/Show.S01E01E02.mkv", FileName: "Show.S01E01E02.mkv", LibraryType: models.LocalMediaLibraryTypeShow, MatchedTitleID: "tvdb:series:505", MatchedMediaType: "series", MatchedName: "Synthetic Show", SeasonNumber: 1, EpisodeNumber: 1, EpisodeTitle: "Stale first episode", Metadata: &models.Title{ID: "tvdb:series:505", Name: "Synthetic Show", TVDBID: 505}, Episodes: []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 2}, {SeasonNumber: 1, EpisodeNumber: 1}}}
	handler := NewLocalMediaHandler(&fakeLocalMediaPlaybackService{item: item}, fakeLocalMediaUsersProvider{}, true)
	request := httptest.NewRequest(http.MethodGet, "/api/library/items/physical-file/playback", nil)
	request = mux.SetURLVars(request, map[string]string{"itemID": item.ID})
	response := httptest.NewRecorder()
	handler.GetPlayback(response, request)
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	var data models.LocalMediaPlaybackResponse
	if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data.ItemID != item.ID || data.TitleID != item.ID || data.ProgressScope != "file" || len(data.CombinedEpisodes) != 2 || !strings.Contains(data.PlaybackNotice, "beginning") || !strings.Contains(data.DisplayName, "s01e01 + s01e02") || data.EpisodeTitle != "" {
		t.Fatalf("combined playback was labelled as one episode: %#v", data)
	}
	for _, raw := range []string{data.StreamURL, data.HLSStartURL} {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		query := parsed.Query()
		if query.Get("itemId") != item.ID || query.Get("mediaType") != "episode" || query.Get("path") != data.StreamPath {
			t.Fatal("lost physical stream/progress identity", query)
		}
		for _, key := range []string{"seasonNumber", "episodeNumber", "seriesId", "episodeName", "imdb", "tmdb", "tvdb", "start", "startTime", "offset"} {
			if query.Has(key) {
				t.Fatal("invented episode identity or seek position", key, query)
			}
		}
	}
	if item.EpisodeNumber != 1 || item.EpisodeTitle != "Stale first episode" || item.Episodes[0].EpisodeNumber != 2 {
		t.Fatal("response mutated stored catalogue row")
	}
}

func TestLocalCombinedEpisodePlaybackRejectsInvalidMembership(t *testing.T) {
	handler := NewLocalMediaHandler(&fakeLocalMediaPlaybackService{item: &models.LocalMediaItem{ID: "file", Episodes: []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 0}}}}, fakeLocalMediaUsersProvider{}, false)
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/library/items/file/playback", nil), map[string]string{"itemID": "file"})
	response := httptest.NewRecorder()
	handler.GetPlayback(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatal(response.Code, response.Body.String())
	}
}

func TestLocalPreparedEpisodeCannotAdoptCombinedMembership(t *testing.T) {
	catalog := &fallbackCatalog{item: models.LocalMediaItem{ID: "local-item", LibraryID: "library", LibraryType: models.LocalMediaLibraryTypeShow, SeasonNumber: 1, EpisodeNumber: 1}}
	h := &PrequeueHandler{users: fallbackProfiles{profile: models.User{ID: "profile", AccountID: "account"}}, localFallback: catalog, localAccess: &fallbackAccess{}}
	request := httptest.NewRequest(http.MethodGet, "/synthetic", nil)
	entry := &playback.PrequeueEntry{StreamPath: "localmedia:local-item/Show.S01E01.mkv", UserID: "profile"}
	if !h.localEntryAllowed(request, entry) {
		t.Fatal("positive single-episode control denied")
	}
	catalog.item.Episodes = []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}
	if h.localEntryAllowed(request, entry) {
		t.Fatal("prepared single episode adopted combined file without offsets")
	}
}
