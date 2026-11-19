package models

import (
	"encoding/json"
	"testing"
)

func TestRegionalChildPolicyStrictCanonicalBodyAndIndependentCaps(t *testing.T) {
	for _, movie := range []int{0, 6, 12, 16, 18} {
		for _, episode := range []int{0, 6, 12, 16, 18} {
			wanted := RegionalChildPolicy{Version: 1, Country: "DE", MaxMovieAge: movie, MaxEpisodeAge: episode}
			raw, err := json.Marshal(wanted)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeRegionalChildPolicy(raw)
			if err != nil || got != wanted {
				t.Fatal("supported independent caps did not round trip")
			}
		}
	}
}
func TestRegionalChildPolicyRejectsDuplicateUnknownNullTrailingAndLegacyClaims(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12,"enabled":false}`, `{"version":1,"country":"DE","maxMovieAge":6,"maxMovieAge":18,"maxEpisodeAge":12}`, `{"Version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}`, `{"version":1,"country":"DE","maxMovieAge":null,"maxEpisodeAge":12}`, `{"version":1,"country":"DE","maxMovieAge":7,"maxEpisodeAge":12}`, `{"version":1,"country":"US","maxMovieAge":6,"maxEpisodeAge":12}`, `{"version":1,"country":"DE","maxMovieAge":"PG","maxEpisodeAge":12}`, `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12} {}`, `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12,"revision":100}`} {
		var policy RegionalChildPolicy
		if json.Unmarshal([]byte(raw), &policy) == nil {
			t.Fatalf("invalid regional policy accepted: %s", raw)
		}
	}
}
