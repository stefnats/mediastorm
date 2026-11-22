package remotemedia

import (
	"encoding/json"
	"testing"

	"novastream/models"
	"novastream/services/jellyfin"
	"novastream/services/localmedia"
	"novastream/services/plex"
)

const remoteFilterJellyfinJSON = `[{"Id":"movie","Type":"Movie","Name":"Synthetic film","MediaSources":[
 {"Id":"one","Path":"/movies/one.mkv","Size":100,"MediaStreams":[{"Index":0,"Type":"Video","Codec":"h264","Width":1920,"Height":1080,"VideoRange":"SDR"},{"Index":1,"Type":"Audio","Codec":"ac3","Language":"fre"},{"Index":2,"Type":"Audio","Codec":"aac","Language":"eng"},{"Index":3,"Type":"Subtitle","Codec":"subrip","Language":"spa"}]},
 {"Id":"two","Path":"/movies/two.mkv","Size":200,"MediaStreams":[{"Index":0,"Type":"Video","Codec":"hevc","Width":3840,"Height":2160,"VideoRange":"HDR","VideoRangeType":"HDR10"},{"Index":1,"Type":"Audio","Codec":"aac","Language":"fra"},{"Index":2,"Type":"Subtitle","Codec":"subrip","Language":"fra"}]}
]}]`
const remoteFilterPlexJSON = `[{"ratingKey":"movie","type":"movie","title":"Synthetic film","Media":[
 {"videoCodec":"h264","width":1920,"height":1080,"Part":[{"id":10,"key":"/library/parts/10/file","file":"/movies/one.mkv","size":100,"Stream":[{"index":0,"streamType":1,"codec":"h264"},{"index":1,"streamType":2,"codec":"ac3","languageCode":"fre"},{"index":2,"streamType":2,"codec":"aac","languageCode":"eng"},{"index":3,"streamType":3,"codec":"subrip","languageCode":"spa"}]}]},
 {"videoCodec":"hevc","width":3840,"height":2160,"videoDynamicRange":"HDR10","Part":[{"id":20,"key":"/library/parts/20/file","file":"/movies/two.mkv","size":200,"Stream":[{"index":0,"streamType":1,"codec":"hevc"},{"index":1,"streamType":2,"codec":"aac","languageCode":"fra"},{"index":2,"streamType":3,"codec":"subrip","languageCode":"fra"}]}]}
]}]`

func normalizedRemoteFilterFixture(t *testing.T, provider string) (*models.RemoteMediaLibrary, []models.RemoteMediaItem) {
	t.Helper()
	library := &models.RemoteMediaLibrary{ID: "filters", Type: models.LocalMediaLibraryTypeMovie, Provider: provider}
	var items []models.RemoteMediaItem
	if provider == models.MediaSourceJellyfin {
		var source []jellyfin.JellyfinItem
		if err := json.Unmarshal([]byte(remoteFilterJellyfinJSON), &source); err != nil {
			t.Fatal(err)
		}
		items = normalizeJellyfin(library, source)
	} else {
		var source []plex.PlexLibraryItem
		if err := json.Unmarshal([]byte(remoteFilterPlexJSON), &source); err != nil {
			t.Fatal(err)
		}
		items = normalizePlex(library, source)
	}
	for i := range items {
		items[i].LibraryID = library.ID
	}
	return library, items
}

func TestRemoteStoredMediaFiltersSameVersionAndTrack(t *testing.T) {
	for _, provider := range []string{models.MediaSourceJellyfin, models.MediaSourcePlex} {
		t.Run(provider, func(t *testing.T) {
			library, items := normalizedRemoteFilterFixture(t, provider)
			if len(items) != 2 || items[0].Probe == nil || !items[0].Probe.TracksScanned || len(items[0].Probe.AudioTracks) != 2 || items[0].Probe.AudioTracks[0].Language != "fra" || items[0].Probe.AudioTracks[0].Index != 1 {
				t.Fatalf("per-file inventory not normalized: %+v", items)
			}
			s := &Service{repo: &fakeRemoteMediaRepo{libraries: []models.RemoteMediaLibrary{*library}, items: map[string][]models.RemoteMediaItem{library.ID: items}}}
			filter := models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", VideoCodec: "hevc", SubtitleLanguage: "fra", Subtitles: "present", MinHeight: 2160, HDR: "hdr10", Unknown: "exclude"}
			groups, err := s.ListGroups(t.Context(), library.ID, models.LocalMediaItemListQuery{MediaFilters: filter, Limit: 1})
			if err != nil || groups.Total != 1 || len(groups.Groups) != 1 || groups.Groups[0].ItemCount != 1 || len(groups.Groups[0].Items) != 1 || groups.Groups[0].Items[0].FileName != "two.mkv" {
				t.Fatalf("features mixed across versions/tracks: %+v %v", groups, err)
			}
			page, err := s.ListItems(t.Context(), library.ID, models.LocalMediaItemListQuery{MediaFilters: filter, Limit: 1})
			if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0].FileName != "two.mkv" {
				t.Fatalf("flat stored filters: %+v %v", page, err)
			}
			filter.AudioCodec = "ac3" // French AC-3 exists only in the other H.264 file.
			groups, err = s.ListGroups(t.Context(), library.ID, models.LocalMediaItemListQuery{MediaFilters: filter})
			if err != nil || groups.Total != 0 {
				t.Fatalf("cross-file facts admitted: %+v %v", groups, err)
			}
			// A legacy aggregate language and primary codec cannot assert a track pair.
			legacy := models.RemoteMediaItem{ID: "legacy", LibraryID: library.ID, ExternalItemID: "legacy", GroupKey: "legacy", Title: "Legacy", AudioCodec: "aac", AudioLanguages: []string{"fra"}}
			s.repo.(*fakeRemoteMediaRepo).items[library.ID] = append(items, legacy)
			q := models.LocalMediaItemListQuery{MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", Unknown: "only"}}
			page, err = s.ListItems(t.Context(), library.ID, q)
			if err != nil || page.Total != 1 || page.Items[0].ID != "legacy" {
				t.Fatalf("legacy unknown semantics: %+v %v", page, err)
			}
		})
	}
}

func TestRemoteTrackInventoryUnknownAndEmbeddedSubtitles(t *testing.T) {
	for _, provider := range []string{models.MediaSourceJellyfin, models.MediaSourcePlex} {
		t.Run(provider, func(t *testing.T) {
			base := models.RemoteMediaItem{VideoCodec: "h264", Height: 1080}
			var missing, empty, external *models.LocalMediaProbe
			if provider == models.MediaSourceJellyfin {
				missing = jellyfinFilterProbe(base, nil)
				empty = jellyfinFilterProbe(base, []jellyfin.JellyfinMediaStream{})
				index := 3
				external = jellyfinFilterProbe(base, []jellyfin.JellyfinMediaStream{{Index: &index, Type: "Subtitle", Language: "fra", Codec: "subrip", IsExternal: true}})
			} else {
				missing = plexFilterProbe(base, nil)
				empty = plexFilterProbe(base, []plex.PlexStream{})
				index := 3
				external = plexFilterProbe(base, []plex.PlexStream{{Index: &index, StreamType: 3, LanguageCode: "fra", Codec: "srt", Key: "/library/streams/sidecar"}})
			}
			absent := models.LocalMediaFilters{Subtitles: "absent", Unknown: "exclude"}
			if localmedia.MatchesMediaFilters(missing, absent) || !localmedia.MatchesMediaFilters(empty, absent) || !localmedia.MatchesMediaFilters(external, absent) {
				t.Fatal("missing inventory or external sidecar became embedded subtitles")
			}
			if !localmedia.MatchesMediaFilters(missing, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "include"}) || localmedia.MatchesMediaFilters(empty, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "include"}) {
				t.Fatal("known absence was treated as unknown")
			}
			if remoteHDR("HDR") != "" {
				t.Fatal("generic HDR was promoted to HDR10")
			}
		})
	}
}
