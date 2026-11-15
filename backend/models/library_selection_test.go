package models

import (
	"reflect"
	"testing"
)

func TestContentSourceOverridesRequireOfficialOwner(t *testing.T) {
	for _, tc := range []struct {
		name, owner, ordering string
		nilNumbering, child   bool
	}{
		{"unproven", "", "", true, false},
		{"official", "tmdb:series:123", "official", false, true},
		{"legacy alias", "tmdb:tv:123", "", false, true},
		{"DVD", "tmdb:series:123", "dvd", false, false},
		{"foreign owner", "tmdb:series:999", "official", false, false},
		{"different provider unproven", "tvdb:series:74852", "official", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &EpisodeReference{SeasonNumber: 0, EpisodeNumber: 1}
			if !tc.nilNumbering {
				ep.Numbering = &EpisodeNumbering{SeriesID: tc.owner, Ordering: tc.ordering}
			}
			want := []string{"tmdb:series:123"}
			if tc.child {
				want = []string{"tmdb:series:123:season:0:episode:1", "tmdb:series:123:season:0", "tmdb:series:123"}
			}
			if got := ContentPreferenceSourceIDs("tmdb:series:123", ep); !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
	if !OfficialEpisodeCoordinates("tvdb:series:74852", &EpisodeReference{SeasonNumber: 1, EpisodeNumber: 1, Numbering: &EpisodeNumbering{SeriesID: "tvdb:74852"}}) {
		t.Fatal("real legacy TVDB alias was rejected")
	}
}

func TestRemoteSelectionProofPersistenceAndImportBoundary(t *testing.T) {
	item := RemoteMediaItem{ID: "version", ExternalItemID: "movie", ExternalMediaID: "part", GroupKey: "group", SinglePart: true, SeriesExternalIDs: &LocalMediaExternalIDs{TMDB: "123"}, EpisodeExternalIDs: &LocalMediaExternalIDs{TVDB: "456"}, AudioLanguages: []string{"fra"}, ProviderData: map[string]string{"partKey": "/library/parts/part/file"}}
	stored := RemoteSelectionProviderData(&item)
	if len(item.ProviderData) != 1 {
		t.Fatal("serialization mutated the caller")
	}
	reloaded := RemoteMediaItem{ID: item.ID, ExternalItemID: item.ExternalItemID, ExternalMediaID: item.ExternalMediaID, GroupKey: item.GroupKey, ProviderData: stored}
	HydrateRemoteSelectionProof(&reloaded)
	if !reloaded.SinglePart || !reflect.DeepEqual(reloaded.SeriesExternalIDs, item.SeriesExternalIDs) || !reflect.DeepEqual(reloaded.EpisodeExternalIDs, item.EpisodeExternalIDs) || !reflect.DeepEqual(reloaded.AudioLanguages, item.AudioLanguages) {
		t.Fatalf("proof lost on persistence: %+v", reloaded)
	}
	wrong := reloaded
	wrong.ExternalMediaID = "another-part"
	wrong.SinglePart, wrong.SeriesExternalIDs = false, nil
	HydrateRemoteSelectionProof(&wrong)
	if wrong.SinglePart || wrong.SeriesExternalIDs != nil {
		t.Fatal("proof crossed media part anchors")
	}
	ClearRemoteSelectionProof(&reloaded)
	HydrateRemoteSelectionProof(&reloaded)
	if reloaded.SinglePart || reloaded.SeriesExternalIDs != nil || reloaded.EpisodeExternalIDs != nil || reloaded.AudioLanguages != nil || reloaded.ProviderData["partKey"] != item.ProviderData["partKey"] {
		t.Fatal("restore retained automatic proof or removed manual playback")
	}
	if stored[remoteSelectionProofKey] == "" {
		t.Fatal("sanitation mutated a shared map")
	}
}
