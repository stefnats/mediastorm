package localmedia

import (
	"testing"

	"novastream/models"
)

func TestPlaybackCandidatesKeepHistoricalSeriesAnchors(t *testing.T) {
	for _, tc := range []struct {
		name, stored, requested, tmdb, tvdb string
		want                                bool
	}{
		{"tmdb historical to modern", "tmdb:tv:123", "tmdb:series:123", "123", "", true},
		{"tmdb modern to historical", "tmdb:series:123", "tmdb:tv:123", "", "", true},
		{"tvdb historical to modern", "tvdb:123", "tvdb:series:123", "", "123", true},
		{"tvdb modern to historical", "tvdb:series:123", "tvdb:123", "", "", true},
		{"different tmdb title", "tmdb:tv:123", "tmdb:series:456", "", "", false},
		{"different tvdb title", "tvdb:123", "tvdb:series:456", "", "", false},
		{"different provider", "tmdb:tv:123", "tvdb:series:123", "", "", false},
		{"movie is not a series alias", "tmdb:tv:123", "tmdb:movie:123", "", "", false},
		{"conflicting tmdb input", "tmdb:tv:123", "tmdb:series:123", "456", "", false},
		{"conflicting tvdb input", "tvdb:123", "tvdb:series:123", "", "456", false},
		{"opaque legacy tvdb", "tvdb:opaque", "tvdb:series:opaque", "", "", false},
		{"opaque legacy tmdb", "tmdb:tv:opaque", "tmdb:series:opaque", "", "", false},
		{"different numeric spelling", "tmdb:tv:0123", "tmdb:series:123", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := &models.LocalMediaItem{ID: "synthetic-episode", LibraryID: "library", LibraryType: models.LocalMediaLibraryTypeShow,
				MatchedTitleID: tc.stored, MatchStatus: models.LocalMediaMatchStatusManual, SeasonNumber: 2, EpisodeNumber: 3,
				FilePath: "/nonexistent/synthetic/episode.mkv"}
			service := &Service{repo: &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeShow},
				items: map[string]*models.LocalMediaItem{item.ID: item}}}
			query := models.LocalMediaMatchQuery{MediaType: "series", TitleID: tc.requested, TMDBID: tc.tmdb, TVDBID: tc.tvdb}
			check := func(episode int, allowed bool, want bool) {
				t.Helper()
				items, err := service.FindPlaybackCandidates(t.Context(), query, &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: episode}, func(string) bool { return allowed })
				if err != nil || (len(items) == 1) != want || len(items) > 1 {
					t.Fatalf("candidates=%d error=%v; want exact candidate=%v", len(items), err, want)
				}
			}
			check(3, true, tc.want)
			check(4, true, false)
			check(3, false, false)
		})
	}
}
