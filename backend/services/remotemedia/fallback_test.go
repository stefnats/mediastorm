package remotemedia

import (
	"context"
	"errors"
	"novastream/models"
	"novastream/services/jellyfin"
	"novastream/services/plex"
	"testing"
)

func TestAutomaticRemoteCandidatesRequireExactParentLeafAndPart(t *testing.T) {
	item := models.RemoteMediaItem{ID: "version", LibraryID: "library", LibraryType: models.LocalMediaLibraryTypeShow, ExternalItemID: "episode", ExternalMediaID: "part", StreamPath: "plexmedia:version", SinglePart: true, SeasonNumber: 1, EpisodeNumber: 2, SeriesExternalIDs: &models.LocalMediaExternalIDs{TMDB: "123", TVDB: "74852"}, EpisodeExternalIDs: &models.LocalMediaExternalIDs{TMDB: "321", TVDB: "456"}, ProviderData: map[string]string{"partKey": "/library/parts/part/file"}}
	query := models.LocalMediaMatchQuery{TitleID: "tmdb:series:123", MediaType: "series"}
	ep := &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tmdb:episode:321", TvdbID: "456", Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:series:123", Ordering: "official"}}
	for _, mode := range []string{"valid", "legacy canonical alias", "unproven parent", "conflicting parent", "wrong leaf", "conflicting leaf", "missing leaf", "wrong coordinate", "alternate order", "no owner", "multipart", "missing part", "denied", "missing item", "path substitution"} {
		t.Run(mode, func(t *testing.T) {
			current := item
			currentEpisode := *ep
			allow := true
			switch mode {
			case "legacy canonical alias":
				currentEpisode.Numbering = &models.EpisodeNumbering{SeriesID: "tmdb:tv:123"}
			case "unproven parent":
				current.SeriesExternalIDs = nil
			case "conflicting parent":
				current.SeriesExternalIDs = &models.LocalMediaExternalIDs{TMDB: "999", TVDB: "74852"}
			case "wrong leaf":
				currentEpisode.EpisodeID = "tmdb:episode:999"
			case "conflicting leaf":
				currentEpisode.TvdbID = "999"
			case "missing leaf":
				current.EpisodeExternalIDs = nil
			case "wrong coordinate":
				current.EpisodeNumber = 3
			case "alternate order":
				currentEpisode.Numbering = &models.EpisodeNumbering{SeriesID: "tmdb:series:123", Ordering: "dvd"}
			case "no owner":
				currentEpisode.Numbering = nil
			case "multipart":
				current.SinglePart = false
			case "missing part":
				current.ProviderData = nil
			case "denied":
				allow = false
			case "missing item":
				current.IsMissing = true
			case "path substitution":
				current.StreamPath = "plexmedia:other"
			}
			repo := &fakeRemoteMediaRepo{libraries: []models.RemoteMediaLibrary{{ID: "library", Type: models.LocalMediaLibraryTypeShow, Provider: models.MediaSourcePlex}}, items: map[string][]models.RemoteMediaItem{"library": {current}}}
			service := &Service{repo: repo}
			got, err := service.FindPlaybackCandidates(t.Context(), query, &currentEpisode, func(string) bool { return allow })
			want := mode == "valid" || mode == "legacy canonical alias"
			if err != nil || (len(got) == 1) != want {
				t.Fatalf("got=%v error=%v want candidate=%v", got, err, want)
			}
		})
	}
}

func TestJellyfinSyncKeepsParentAndEpisodeNamespacesSeparate(t *testing.T) {
	library := &models.RemoteMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeShow, Provider: models.MediaSourceJellyfin}
	parent := jellyfin.JellyfinItem{ID: "show", Type: "Series", ProviderIDs: map[string]string{"tmdb": "123", "tvdb": "74852"}}
	ep := jellyfin.JellyfinItem{ID: "ep", Type: "Episode", SeriesID: "show", SeasonNum: 1, EpisodeNum: 2, ProviderIDs: map[string]string{"tmdb": "321", "tvdb": "456"}, MediaSources: []jellyfin.JellyfinMediaSource{{ID: "source", Path: "/media/episode.mkv", Size: 100, MediaStreams: []jellyfin.JellyfinMediaStream{{Type: "Audio", Codec: "aac", Language: "fra"}}}}}
	for _, mode := range []string{"parent after episode", "parent missing", "parent conflicting", "multipart"} {
		t.Run(mode, func(t *testing.T) {
			source := []jellyfin.JellyfinItem{ep, parent}
			if mode == "parent missing" {
				source = source[:1]
			}
			if mode == "parent conflicting" {
				other := parent
				other.ProviderIDs = map[string]string{"tmdb": "999"}
				source = append(source, other)
			}
			if mode == "multipart" {
				source[0].PartCount = models.IntPtr(2)
			}
			items := normalizeJellyfin(library, source)
			if len(items) != 1 || items[0].EpisodeExternalIDs.TMDB != "321" || len(items[0].AudioLanguages) != 1 || items[0].AudioLanguages[0] != "fra" {
				t.Fatalf("incorrect normalized playable rows: %+v", items)
			}
			if (items[0].SeriesExternalIDs != nil) != (mode != "parent missing" && mode != "parent conflicting") {
				t.Fatalf("parent proof=%+v", items[0].SeriesExternalIDs)
			}
			if items[0].SinglePart != (mode != "multipart") {
				t.Fatalf("part completeness=%v", items[0].SinglePart)
			}
		})
	}
}

func TestJellyfinCompleteMovieOmissionAndVersionProof(t *testing.T) {
	library := &models.RemoteMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeMovie, Provider: models.MediaSourceJellyfin}
	for _, mode := range []string{"ordinary omission", "different complete versions", "duplicate source ID", "missing source path", "missing source payload", "unsupported item", "explicit multipart"} {
		t.Run(mode, func(t *testing.T) {
			item := jellyfin.JellyfinItem{ID: "movie", Type: "Movie", ProviderIDs: map[string]string{"tmdb": "123"}, MediaSources: []jellyfin.JellyfinMediaSource{{ID: "version-1", Path: "/media/movie-1.mkv", Size: 100}}}
			switch mode {
			case "different complete versions":
				item.MediaSources = append(item.MediaSources, jellyfin.JellyfinMediaSource{ID: "version-2", Path: "/media/movie-2.mkv", Size: 200})
			case "duplicate source ID":
				item.MediaSources = append(item.MediaSources, jellyfin.JellyfinMediaSource{ID: "version-1", Path: "/media/another-file.mkv", Size: 200})
			case "missing source path":
				item.MediaSources[0].Path = ""
			case "missing source payload":
				item.MediaSources = nil
			case "unsupported item":
				item.Type = "Video"
			case "explicit multipart":
				item.PartCount = models.IntPtr(2)
			}
			items := normalizeJellyfin(library, []jellyfin.JellyfinItem{item})
			for i := range items {
				items[i].LibraryID = library.ID
			}
			want := mode == "ordinary omission" || mode == "different complete versions"
			for _, version := range items {
				if version.SinglePart != want {
					t.Fatalf("single complete part=%v want=%v for %+v", version.SinglePart, want, version)
				}
			}
			repo := &fakeRemoteMediaRepo{libraries: []models.RemoteMediaLibrary{*library}, items: map[string][]models.RemoteMediaItem{library.ID: items}}
			got, err := (&Service{repo: repo}).FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{TitleID: "tmdb:movie:123", MediaType: "movie"}, nil, func(string) bool { return true })
			if err != nil || (len(got) > 0) != want {
				t.Fatalf("automatic complete candidates=%d want any=%v error=%v", len(got), want, err)
			}
		})
	}
}

func TestAutomaticRemoteDiscoveryNeverPublishesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	repo := &fakeRemoteMediaRepo{libraries: []models.RemoteMediaLibrary{{ID: "library", Type: models.LocalMediaLibraryTypeMovie, Provider: models.MediaSourcePlex}}, items: map[string][]models.RemoteMediaItem{"library": {{ID: "version", LibraryID: "library", LibraryType: models.LocalMediaLibraryTypeMovie, SinglePart: true, ExternalItemID: "movie", ExternalMediaID: "part", ExternalIDs: &models.LocalMediaExternalIDs{TMDB: "123"}, StreamPath: "plexmedia:version", ProviderData: map[string]string{"partKey": "/part"}}}}}
	got, err := (&Service{repo: repo}).FindPlaybackCandidates(ctx, models.LocalMediaMatchQuery{TitleID: "tmdb:movie:123", MediaType: "movie"}, nil, func(string) bool { cancel(); return true })
	if !errors.Is(err, context.Canceled) || len(got) != 0 {
		t.Fatalf("canceled discovery published stale candidates: %v %v", got, err)
	}
}

func TestPlexMultipartNeverBecomesAutomaticCompleteMovie(t *testing.T) {
	library := &models.RemoteMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeMovie, Provider: models.MediaSourcePlex}
	items := normalizePlex(library, []plex.PlexLibraryItem{{RatingKey: "movie", Type: "movie", Guid: []plex.PlexGuid{{ID: "tmdb://123"}}, Media: []plex.PlexMedia{{Part: []plex.PlexPart{{ID: 1, Key: "/part/1", File: "part1.mkv"}, {ID: 2, Key: "/part/2", File: "part2.mkv"}}}}}})
	if len(items) != 2 || items[0].SinglePart || items[1].SinglePart {
		t.Fatalf("multipart rows must remain manual: %+v", items)
	}
}
