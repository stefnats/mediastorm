package config

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestIndexerUserAgentMatchesByName(t *testing.T) {
	s := Settings{Indexers: []IndexerConfig{
		{Name: "NZBGeek", MovieUserAgent: "Radarr/6.0", SeriesUserAgent: "Sonarr/4.0"},
		{Name: "SceneIndexer", UserAgent: " Sonarr/4.0.20.3014 (ubuntu 24.04) ", MovieUserAgent: "Radarr/6.0", SeriesUserAgent: "Sonarr/4.0"},
	}}
	if got := s.IndexerUserAgent("sceneindexer"); got != "Sonarr/4.0.20.3014 (ubuntu 24.04)" {
		t.Fatalf("got %q", got)
	}
	if got := s.IndexerUserAgent("NZBGeek"); got != "" {
		t.Fatalf("expected no override, got %q", got)
	}
	if got := s.IndexerUserAgent(""); got != "" {
		t.Fatalf("expected empty for blank name, got %q", got)
	}
}

func TestIndexerSearchUserAgentsPersist(t *testing.T) {
	mgr := NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := DefaultSettings()
	settings.Indexers = []IndexerConfig{{
		Name: "TestIndexer", URL: "https://indexer.invalid/api", Type: "newznab", Enabled: true,
		UserAgent: "SABnzbd/5.1.3", MovieUserAgent: "Radarr/6.0", SeriesUserAgent: "Sonarr/4.0",
	}}
	if err := mgr.Save(settings); err != nil {
		t.Fatal(err)
	}
	loaded, err := mgr.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.Indexers, settings.Indexers) {
		t.Fatalf("reloaded indexers = %#v, want %#v", loaded.Indexers, settings.Indexers)
	}
	for _, tc := range []struct{ mediaType, want string }{
		{"movie", "Radarr/6.0"}, {"series", "Sonarr/4.0"},
		{" SERIES ", "Sonarr/4.0"}, {" Movie ", "Radarr/6.0"},
		{"", "SABnzbd/5.1.3"}, {"unknown", "SABnzbd/5.1.3"},
	} {
		if got := loaded.Indexers[0].SearchUserAgent(tc.mediaType); got != tc.want {
			t.Errorf("SearchUserAgent(%q) = %q, want %q", tc.mediaType, got, tc.want)
		}
	}
}
