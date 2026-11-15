package indexer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"novastream/config"
	"novastream/internal/httpheaders"
)

func TestSearchTorznabMediaUserAgents(t *testing.T) {
	for _, tc := range []struct {
		name, mediaType, searchType, movieAgent, seriesAgent, fallback, want string
	}{
		{"movie", "movie", "search", " Radarr/6.0 ", "Sonarr/4.0", "Legacy/1.0", "Radarr/6.0"},
		{"series text search", "series", "search", "Radarr/6.0", "Sonarr/4.0", "Legacy/1.0", "Sonarr/4.0"},
		{"daily show ID search", "series", "tvsearch", "Radarr/6.0", "Sonarr/4.0", "Legacy/1.0", "Sonarr/4.0"},
		{"blank movie override", "movie", "search", "  ", "Sonarr/4.0", " Legacy/1.0 ", "Legacy/1.0"},
		{"blank series override", "series", "search", "Radarr/6.0", "", "Legacy/1.0", "Legacy/1.0"},
		{"movie built-in default", "movie", "search", "", "Sonarr/4.0", "", httpheaders.UserAgent},
		{"series built-in default", "series", "search", "Radarr/6.0", "", "", httpheaders.UserAgent},
		{"untyped search", "", "search", "Radarr/6.0", "Sonarr/4.0", "Legacy/1.0", "Legacy/1.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.UserAgent(); got != tc.want {
					t.Errorf("User-Agent = %q, want %q", got, tc.want)
				}
				if got := r.URL.Query().Get("t"); got != tc.searchType {
					t.Errorf("search type = %q, want %q", got, tc.searchType)
				}
				fmt.Fprint(w, dedupFeed)
			}))
			defer server.Close()
			svc := &Service{httpc: server.Client()}
			idx := config.IndexerConfig{
				Name: "TestIndexer", URL: server.URL, Type: "newznab",
				UserAgent: tc.fallback, MovieUserAgent: tc.movieAgent, SeriesUserAgent: tc.seriesAgent,
			}
			opts := SearchOptions{Query: "Example", MediaType: tc.mediaType, usenetSearchType: tc.searchType}
			if tc.searchType == "tvsearch" {
				opts.usenetUseID = true
				opts.TVDBID = 1234
			}
			if _, err := svc.searchTorznab(t.Context(), idx, opts); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSearchTorznabUserAgentChangeBypassesCachedCall(t *testing.T) {
	for _, mediaType := range []string{"movie", "series"} {
		t.Run(mediaType, func(t *testing.T) {
			headers := make(chan string, 2)
			svc, idx := dedupTestService(t, func(w http.ResponseWriter, r *http.Request) {
				headers <- r.UserAgent()
				fmt.Fprint(w, dedupFeed)
			})
			opts := SearchOptions{Query: "Example", MediaType: mediaType}
			if _, err := svc.searchTorznab(t.Context(), idx, opts); err != nil {
				t.Fatal(err)
			}
			<-headers
			idx.MovieUserAgent = "Radarr/6.0"
			idx.SeriesUserAgent = "Sonarr/4.0"
			if _, err := svc.searchTorznab(t.Context(), idx, opts); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-headers:
				if want := idx.SearchUserAgent(mediaType); got != want {
					t.Fatalf("User-Agent = %q, want %q", got, want)
				}
			default:
				t.Fatal("changed User-Agent reused an earlier indexer response")
			}
		})
	}
}
