package remotemedia

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
	"novastream/services/jellyfin"
	"novastream/services/plex"
)

func TestOrdinaryRemoteSyncPersistsMediaFilterFacts(t *testing.T) {
	for _, provider := range []string{models.MediaSourceJellyfin, models.MediaSourcePlex} {
		t.Run(provider, func(t *testing.T) {
			f := newCataloguePGFixture(t)
			var calls, unexpected atomic.Int64
			var omitTracks, listHasTracks atomic.Bool
			wire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case provider == models.MediaSourceJellyfin && r.URL.Path == "/Users/user/Items":
					if !strings.Contains(r.URL.Query().Get("Fields"), "MediaStreams") {
						unexpected.Add(1)
					}
					var items []jellyfin.JellyfinItem
					if err := json.Unmarshal([]byte(remoteFilterJellyfinJSON), &items); err != nil {
						unexpected.Add(1)
						http.Error(w, "fixture", 500)
						return
					}
					if omitTracks.Load() {
						for i := range items[0].MediaSources {
							items[0].MediaSources[i].MediaStreams = nil
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Items": items, "TotalRecordCount": len(items)})
				case provider == models.MediaSourcePlex && (r.URL.Path == "/library/sections/movies/all" || r.URL.Path == "/library/metadata/movie"):
					var items []plex.PlexLibraryItem
					if err := json.Unmarshal([]byte(remoteFilterPlexJSON), &items); err != nil {
						unexpected.Add(1)
						http.Error(w, "fixture", 500)
						return
					}
					if r.URL.Path == "/library/sections/movies/all" && !listHasTracks.Load() || omitTracks.Load() {
						for mi := range items[0].Media {
							for pi := range items[0].Media[mi].Part {
								items[0].Media[mi].Part[pi].Stream = nil
							}
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"MediaContainer": map[string]any{"Metadata": items, "totalSize": 1, "size": 1}})
				default:
					unexpected.Add(1)
					http.Error(w, "unexpected provider request", 500)
				}
			}))
			t.Cleanup(wire.Close)
			cfg := config.DefaultSettings()
			cfg.Jellyfin.Accounts = []config.JellyfinAccount{{ID: "account", UserID: "user", Token: "synthetic", ServerURL: wire.URL}}
			cfg.Plex.Accounts = []config.PlexAccount{{ID: "account", AuthToken: "synthetic"}}
			if err := f.service.cfg.Save(cfg); err != nil {
				t.Fatal(err)
			}
			f.service.jellyfin = jellyfin.NewClient()
			f.service.plex = plex.NewClient("synthetic")
			server := plex.PlexResource{ClientIdentifier: "server", AccessToken: "synthetic", Connections: []plex.PlexConnection{{URI: wire.URL, Local: true}}}
			f.service.servers.entries = map[string]plexServerCacheEntry{"account\x00server": {server: server, authToken: "synthetic", expiresAt: time.Now().Add(time.Minute)}}
			now := time.Now().UTC()
			library := &models.RemoteMediaLibrary{ID: "synced", Name: "Synthetic sync", Provider: provider, Type: models.LocalMediaLibraryTypeMovie, AccountID: "account", ServerID: "server", ServerURL: wire.URL, ExternalLibraryID: "movies", CreatedAt: now, UpdatedAt: now}
			if err := f.store.RemoteMedia().CreateLibrary(t.Context(), library); err != nil {
				t.Fatal(err)
			}
			if n, err := f.service.Sync(t.Context(), library.ID); err != nil || n != 2 {
				t.Fatalf("actual sync: %d %v", n, err)
			}
			afterSync := calls.Load()
			wantCalls := int64(1)
			if provider == models.MediaSourcePlex {
				wantCalls = 2
			}
			if afterSync != wantCalls || unexpected.Load() != 0 {
				t.Fatalf("normal sync requests=%d unexpected=%d", afterSync, unexpected.Load())
			}
			rows, err := f.store.RemoteMedia().ListItems(t.Context(), library.ID, false)
			if err != nil || len(rows) != 2 {
				t.Fatalf("persisted versions: %+v %v", rows, err)
			}
			for _, row := range rows {
				if row.Probe == nil || !row.Probe.TracksScanned || len(row.Probe.AudioTracks) == 0 {
					t.Fatalf("track facts did not roundtrip: %+v", row)
				}
			}
			filter := models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", VideoCodec: "hevc", MinHeight: 2160, HDR: "hdr10", Subtitles: "present", Unknown: "exclude"}
			q := models.LocalMediaItemListQuery{MediaFilters: filter, Limit: 1}
			page, err := f.service.ListItems(t.Context(), library.ID, q)
			if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0].FileName != "two.mkv" {
				t.Fatalf("synced flat filter: %+v %v", page, err)
			}
			groups, err := f.service.ListGroups(t.Context(), library.ID, q)
			if err != nil || groups.Total != 1 || groups.Groups[0].ItemCount != 1 {
				t.Fatalf("synced group filter: %+v %v", groups, err)
			}
			if calls.Load() != afterSync {
				t.Fatal("browsing performed provider HTTP")
			}
			if provider == models.MediaSourcePlex {
				listHasTracks.Store(true)
				before := calls.Load()
				if _, err := f.service.Sync(t.Context(), library.ID); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != before+1 {
					t.Fatal("complete Plex list inventory triggered redundant metadata lookup")
				}
				listHasTracks.Store(false)
			}
			omitTracks.Store(true)
			if _, err := f.service.Sync(t.Context(), library.ID); err != nil {
				t.Fatal(err)
			}
			page, err = f.service.ListItems(t.Context(), library.ID, models.LocalMediaItemListQuery{MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "exclude"}})
			if err != nil || page.Total != 0 {
				t.Fatalf("missing normal-sync inventory retained stale tracks: %+v %v", page, err)
			}
			page, err = f.service.ListItems(t.Context(), library.ID, models.LocalMediaItemListQuery{MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "only"}})
			if err != nil || page.Total != 2 {
				t.Fatalf("unknown resync inventory not filterable: %+v %v", page, err)
			}
			if unexpected.Load() != 0 {
				t.Fatal("unexpected provider request")
			}
		})
	}
}

func TestRemoteMediaFiltersBeforeDatabasePages(t *testing.T) {
	f := newCataloguePGFixture(t)
	_, versions := normalizedRemoteFilterFixture(t, models.MediaSourceJellyfin)
	for i := 0; i < 8; i++ {
		f.put(t, "early-"+string(rune('a'+i)), "Earlier title", "early.mkv", "early", false)
	}
	for _, title := range []string{"Zebra", "Zulu"} {
		for i, version := range versions {
			version.LibraryID = "catalogue"
			version.ID = title + version.ID
			version.ExternalItemID = title
			version.GroupKey = title
			version.Title = title
			version.StreamPath = "jellyfinmedia:" + version.ID
			if err := f.store.RemoteMedia().UpsertItem(t.Context(), &version); err != nil {
				t.Fatal(err)
			}
			if i == 1 { // Verify file binding prevents one stored part's inventory following another.
				raw, err := f.store.RemoteMedia().GetItem(t.Context(), version.ID)
				if err != nil || raw == nil || raw.Probe == nil {
					t.Fatalf("stored probe unavailable: %+v %v", raw, err)
				}
				raw.ExternalMediaID = "different-part"
				models.HydrateRemoteMediaFilterFacts(raw)
				if raw.Probe != nil {
					t.Fatal("different part retained stored track facts")
				}
			}
		}
	}
	q := models.LocalMediaItemListQuery{Sort: "name", Limit: 1, Offset: 1, MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", VideoCodec: "hevc", Unknown: "exclude"}}
	page, err := f.service.ListItems(t.Context(), "catalogue", q)
	if err != nil || page.Total != 2 || len(page.Items) != 1 || page.Items[0].MatchedName != "Zulu" {
		t.Fatalf("flat filtering occurred after page/count: %+v %v", page, err)
	}
	groups, err := f.service.ListGroups(t.Context(), "catalogue", q)
	if err != nil || groups.Total != 2 || len(groups.Groups) != 1 || groups.Groups[0].Title != "Zulu" || groups.Groups[0].ItemCount != 1 {
		t.Fatalf("group filtering occurred after page/count: %+v %v", groups, err)
	}
	q.IncludeCards = true
	cards, err := f.service.ListGroups(t.Context(), "catalogue", q)
	if err != nil || cards.Total != 2 || len(cards.Groups) != 1 || cards.Groups[0].ItemCount != 1 || len(cards.Groups[0].Items) != 0 {
		t.Fatalf("cards bypassed version filtering: %+v %v", cards, err)
	}
}
