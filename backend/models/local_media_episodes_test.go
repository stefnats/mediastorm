package models

import (
	"reflect"
	"testing"
)

func TestLocalEpisodeMembershipRequiresExplicitBoundedCoordinates(t *testing.T) {
	input := []LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 2}, {SeasonNumber: 0, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}
	got, ok := NormalizeLocalMediaEpisodes(input)
	if !ok || !reflect.DeepEqual(got, []LocalMediaEpisode{{SeasonNumber: 0, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}) {
		t.Fatal(got, ok)
	}
	got[0].EpisodeNumber = 99
	if input[1].EpisodeNumber != 1 {
		t.Fatal("normalization mutated input")
	}
	for _, invalid := range [][]LocalMediaEpisode{nil, {{SeasonNumber: -1, EpisodeNumber: 1}}, {{SeasonNumber: 1, EpisodeNumber: 0}}, make([]LocalMediaEpisode, 1001)} {
		if _, ok = NormalizeLocalMediaEpisodes(invalid); ok {
			t.Fatal("invalid membership accepted", invalid)
		}
	}
}
