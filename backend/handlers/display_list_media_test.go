package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"

	"novastream/models"
)

type savedMediaLocalFixture struct {
	libraries []models.LocalMediaLibrary
	rows      map[string][]models.LocalMediaItem
	reads     int
	fail      bool
}

func (f *savedMediaLocalFixture) ListLibraries(context.Context) ([]models.LocalMediaLibrary, error) {
	if f.fail {
		return nil, errors.New("fixture read failed")
	}
	return f.libraries, nil
}
func (f *savedMediaLocalFixture) ListAllItemsByLibrary(_ context.Context, id string) ([]models.LocalMediaItem, error) {
	f.reads++
	return f.rows[id], nil
}

type savedMediaRemoteFixture struct {
	libraries []models.RemoteMediaLibrary
	rows      map[string][]models.LocalMediaItem
	reads     int
}

func (f *savedMediaRemoteFixture) ListLibraries(context.Context) ([]models.RemoteMediaLibrary, error) {
	return f.libraries, nil
}
func (f *savedMediaRemoteFixture) StoredFilterVersions(_ context.Context, id string) ([]models.LocalMediaItem, error) {
	f.reads++
	return f.rows[id], nil
}

type savedMediaAccessFixture struct{}

func (f savedMediaAccessFixture) CanAccess(context.Context, string, string, string, bool) (bool, error) {
	return true, nil
}

type savedMediaAllowedLibraries map[string]bool

func (f savedMediaAllowedLibraries) CanAccess(_ context.Context, id, _, _ string, _ bool) (bool, error) {
	return f[id], nil
}

type savedMediaNoFetchMetadata struct {
	*catalogueLanguageMetadata
	network int
}

func (f *savedMediaNoFetchMetadata) BatchMovieReleases(context.Context, []models.BatchMovieReleasesQuery) []models.BatchMovieReleasesItem {
	f.network++
	return nil
}

func savedMediaProbe(lang, codec, video string) *models.LocalMediaProbe {
	return &models.LocalMediaProbe{TracksScanned: true, VideoCodec: video, Height: 2160, HDRFormat: "hdr10", AudioStreams: 1, SubtitleStreams: 1, AudioTracks: []models.MediaAudioFacts{{Index: 1, Language: lang, Codec: codec}}, SubtitleTracks: []models.MediaSubtitleFacts{{Index: 2, Language: "spa", Codec: "subrip"}}}
}
func savedMediaRow(id, title string, probe *models.LocalMediaProbe) models.LocalMediaItem {
	return models.LocalMediaItem{ID: id, LibraryID: "local", LibraryType: models.LocalMediaLibraryTypeMovie, MatchedTitleID: title, MatchStatus: models.LocalMediaMatchStatusMatched, Probe: probe}
}

func TestSavedMediaFiltersMatchOneVersionAndAudioTrack(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	poison := &savedMediaNoFetchMetadata{catalogueLanguageMetadata: m}
	h.MetadataService = poison
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("tmdb:movie:%d", i)
		catalogueLanguageSeed(t, h, m, id, fmt.Sprintf("Film %d", i), "fra")
		if _, err := h.WatchlistService.AddOrUpdate("profile-a", models.WatchlistUpsert{ID: id, MediaType: "movie", Name: fmt.Sprintf("Film %d", i), ExternalIDs: map[string]string{"tmdb": fmt.Sprint(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	first := savedMediaProbe("fra", "ac3", "hevc")
	first.AudioStreams = 2
	first.AudioTracks = append(first.AudioTracks, models.MediaAudioFacts{Index: 2, Language: "eng", Codec: "aac"})
	local := &savedMediaLocalFixture{libraries: []models.LocalMediaLibrary{{ID: "local", Type: models.LocalMediaLibraryTypeMovie}}, rows: map[string][]models.LocalMediaItem{"local": {
		savedMediaRow("wrong-track", "tmdb:movie:1", first),
		savedMediaRow("version-a", "tmdb:movie:2", savedMediaProbe("fra", "ac3", "hevc")),
		savedMediaRow("version-b", "tmdb:movie:2", savedMediaProbe("fra", "aac", "h264")),
		savedMediaRow("good", "tmdb:movie:3", savedMediaProbe("fra", "aac", "hevc")),
		savedMediaRow("wrong-language", "tmdb:movie:4", savedMediaProbe("eng", "aac", "hevc")),
	}}}
	h.SetStoredMediaLibraries(local, nil, savedMediaAccessFixture{})
	q := "audioLanguage=fr&audioCodec=aac&videoCodec=hevc&subtitleLanguage=es&subtitles=present&hdr=hdr10&minHeight=2160&mediaUnknown=exclude&sortBy=name"
	response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "source=watchlist&"+q+"&originalLanguages=fra"))
	if response.Total != 1 || len(items) != 1 || items[0].ID != "tmdb:movie:3" || !response.StoredMediaFilterSupported || response.StoredMediaFilterBasis != "stored_library_versions" {
		t.Fatalf("facts combined across tracks/files: %+v %+v", response, items)
	}
	if local.reads != 1 || poison.network != 0 {
		t.Fatalf("catalogue reads=%d network=%d", local.reads, poison.network)
	}
	response, items = catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "source=watchlist&audioLanguage=fra&audioCodec=aac&videoCodec=hevc&mediaUnknown=only"))
	if response.Total != 1 || len(items) != 1 || items[0].ID != "tmdb:movie:5" {
		t.Fatalf("missing version is not unknown: %+v %+v", response, items)
	}
}

func TestSavedMediaFiltersWholeListBeforePageAndCustomShelf(t *testing.T) {
	for _, source := range []string{"watchlist", "custom-list", "custom-shelf"} {
		t.Run(source, func(t *testing.T) {
			h, m := catalogueLanguageFixture(t)
			rows := []models.WatchlistItem{}
			versions := []models.LocalMediaItem{}
			for i := 1; i <= 65; i++ {
				id := fmt.Sprintf("tmdb:movie:%d", i)
				catalogueLanguageSeed(t, h, m, id, fmt.Sprintf("Film %03d", i), "")
				rows = append(rows, models.WatchlistItem{ID: id, Name: fmt.Sprintf("Film %03d", i), MediaType: "movie"})
				lang := "eng"
				if i >= 63 {
					lang = "fra"
				}
				versions = append(versions, savedMediaRow(fmt.Sprint(i), id, savedMediaProbe(lang, "aac", "hevc")))
			}
			h.CustomListsService = catalogueLargeCustom{items: rows}
			local := &savedMediaLocalFixture{libraries: []models.LocalMediaLibrary{{ID: "local", Type: models.LocalMediaLibraryTypeMovie}}, rows: map[string][]models.LocalMediaItem{"local": versions}}
			h.SetStoredMediaLibraries(local, nil, savedMediaAccessFixture{})
			query := "source=watchlist"
			if source == "custom-list" {
				query = "source=custom-list&listId=large-list"
			}
			if source == "custom-shelf" {
				query = "source=mdblist-shelf&url=" + url.QueryEscape(customListShelfURLPrefix+"large-list") + "&deferArtwork=true"
			}
			response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, query+"&audioLanguage=fra&mediaUnknown=exclude&sortBy=name&offset=1&limit=1"))
			if source == "custom-shelf" {
				var raw struct {
					Items []models.TrendingItem `json:"items"`
				}
				w := catalogueLanguageGet(t, h, query+"&audioLanguage=fra&mediaUnknown=exclude&sortBy=name&offset=1&limit=1")
				if err := jsonDecodeSavedMedia(w, &raw); err != nil {
					t.Fatal(err)
				}
				if len(raw.Items) != 1 || raw.Items[0].Title.ID != "tmdb:movie:64" {
					t.Fatalf("shelf page=%+v", raw.Items)
				}
			} else if len(items) != 1 || items[0].ID != "tmdb:movie:64" {
				t.Fatalf("page=%+v", items)
			}
			if response.Total != 3 {
				t.Fatalf("filtered total=%d", response.Total)
			}
		})
	}
}

func jsonDecodeSavedMedia(w *httptest.ResponseRecorder, target any) error {
	if w.Code != 200 {
		return fmt.Errorf("HTTP%d %s", w.Code, w.Body.String())
	}
	return json.Unmarshal(w.Body.Bytes(), target)
}

func TestSavedMediaFiltersAccessIdentityUnknownAndNoDiscovery(t *testing.T) {
	h, m := catalogueLanguageFixture(t)
	for _, id := range []string{"tmdb:movie:1", "tmdb:movie:2", "tmdb:movie:3", "tmdb:movie:4", "tt12345"} {
		catalogueLanguageSeed(t, h, m, id, id, "")
	}
	local := &savedMediaLocalFixture{libraries: []models.LocalMediaLibrary{{ID: "private", Type: models.LocalMediaLibraryTypeMovie}, {ID: "local", Type: models.LocalMediaLibraryTypeMovie}}, rows: map[string][]models.LocalMediaItem{
		"private": {savedMediaRow("private-file", "tmdb:movie:1", savedMediaProbe("fra", "aac", "hevc"))},
		"local":   {savedMediaRow("wrong-kind", "tmdb:movie:2", savedMediaProbe("eng", "aac", "hevc")), savedMediaRow("missing", "tmdb:movie:3", savedMediaProbe("fra", "aac", "hevc"))},
	}}
	local.rows["local"][1].IsMissing = true
	remote := &savedMediaRemoteFixture{libraries: []models.RemoteMediaLibrary{{ID: "remote", Provider: "jellyfin", Type: models.LocalMediaLibraryTypeMovie}}, rows: map[string][]models.LocalMediaItem{"remote": {savedMediaRow("remote-file", "tmdb:movie:4", savedMediaProbe("fra", "aac", "hevc"))}}}
	remote.libraries = append(remote.libraries, models.RemoteMediaLibrary{ID: "show-library", Provider: "plex", Type: models.LocalMediaLibraryTypeShow})
	series := savedMediaRow("series-with-same-imdb", "tt12345", savedMediaProbe("fra", "aac", "hevc"))
	series.LibraryType = models.LocalMediaLibraryTypeShow
	remote.rows["show-library"] = []models.LocalMediaItem{series}
	h.SetStoredMediaLibraries(local, remote, savedMediaAllowedLibraries{"local": true, "remote": true, "show-library": true})
	response, items := catalogueLanguageDecode(t, catalogueLanguageGet(t, h, "audioLanguage=fra&mediaUnknown=exclude"))
	if response.Total != 1 || items[0].ID != "tmdb:movie:4" || local.reads != 1 || remote.reads != 2 {
		t.Fatalf("library visibility/unknown mismatch: %+v %+v", response, items)
	}
	for _, source := range []string{"trending", "hidden", "collection-cached", "similar-cached"} {
		w := catalogueLanguageGet(t, h, "source="+source+"&audioLanguage=fra")
		if w.Code != 400 {
			t.Fatalf("unsupported source %s: %d", source, w.Code)
		}
	}
	local.fail = true
	if w := catalogueLanguageGet(t, h, "audioLanguage=fra&mediaUnknown=include"); w.Code != 503 {
		t.Fatalf("database error became unknown: %d", w.Code)
	}
}
