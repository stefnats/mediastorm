package content_preferences

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"novastream/models"
)

func localPreferenceService(t *testing.T) *Service {
	t.Helper()
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestContentLocalPreferenceHierarchyAndExplicitFalse(t *testing.T) {
	s := localPreferenceService(t)
	set := func(id string, value *bool) {
		t.Helper()
		if err := s.Set("viewer", models.ContentPreference{ContentID: id, ContentType: "series", PreferLocalFiles: value}); err != nil {
			t.Fatal(err)
		}
	}
	title := "tmdb:tv:123"
	episode := &models.EpisodeReference{Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:tv:123", Ordering: "official"}, SeasonNumber: 2, EpisodeNumber: 3}
	check := func(profile string, ep *models.EpisodeReference, fallback, want bool) {
		t.Helper()
		got, err := s.ResolveLocalPreference(profile, title, ep, fallback)
		if err != nil || got != want {
			t.Fatalf("resolved=%v error=%v want=%v", got, err, want)
		}
	}
	check("viewer", episode, false, false)
	set(title, models.BoolPtr(true))
	check("viewer", episode, false, true)
	set(title+":season:2", models.BoolPtr(false))
	check("viewer", episode, true, false)
	check("viewer", &models.EpisodeReference{Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:tv:123", Ordering: "official"}, SeasonNumber: 1, EpisodeNumber: 3}, false, true)
	set(title+":season:2:episode:3", models.BoolPtr(true))
	check("viewer", episode, false, true)
	check("viewer", &models.EpisodeReference{Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:tv:123", Ordering: "official"}, SeasonNumber: 2, EpisodeNumber: 4}, true, false)
	set(title+":season:2:episode:3", nil)
	check("viewer", episode, true, false)
	set(title+":season:2", nil)
	check("viewer", episode, false, true)
	set(title, nil)
	check("viewer", episode, false, false)
	check("viewer", episode, true, true)
	check("other", episode, false, false)
	set(title+":season:0", models.BoolPtr(true))
	check("viewer", &models.EpisodeReference{Numbering: &models.EpisodeNumbering{SeriesID: "tmdb:tv:123", Ordering: "official"}, SeasonNumber: 0, EpisodeNumber: 1}, false, true)
}

func TestContentLocalPreferencePersistencePreservesThreeStates(t *testing.T) {
	s := localPreferenceService(t)
	for id, flag := range map[string]*bool{"movie:true": models.BoolPtr(true), "movie:false": models.BoolPtr(false), "movie:inherit": nil} {
		if err := s.Set("viewer", models.ContentPreference{ContentID: id, ContentType: "movie", PreferLocalFiles: flag, AudioLanguage: "fra", SubtitleLanguage: "spa", SubtitleMode: "forced-only"}); err != nil {
			t.Fatal(err)
		}
	}
	reloaded, err := NewService(filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"movie:true", "movie:false", "movie:inherit"} {
		before, _ := s.Get("viewer", id)
		after, _ := reloaded.Get("viewer", id)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("state or tracks changed after reload: %s", id)
		}
	}
}

func TestContentLocalPreferencePointersAreDefensiveCopies(t *testing.T) {
	s := localPreferenceService(t)
	flag := true
	if err := s.Set("viewer", models.ContentPreference{ContentID: "movie", PreferLocalFiles: &flag}); err != nil {
		t.Fatal(err)
	}
	flag = false
	pref, _ := s.Get("viewer", "movie")
	if !*pref.PreferLocalFiles {
		t.Fatal("caller mutated stored value")
	}
	*pref.PreferLocalFiles = false
	listed, _ := s.List("viewer")
	*listed[0].PreferLocalFiles = false
	pref, _ = s.Get("viewer", "movie")
	if !*pref.PreferLocalFiles {
		t.Fatal("returned pointer mutated stored value")
	}
}

func TestContentLocalPreferenceFingerprintsAreStableAndScoped(t *testing.T) {
	s := localPreferenceService(t)
	for _, id := range []string{"series:1:season:2:episode:4", "series:10", "series:1:season:2", "series:1"} {
		if err := s.Set("viewer", models.ContentPreference{ContentID: id, PreferLocalFiles: models.BoolPtr(false)}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.LocalOverrides("viewer", "SERIES:1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].ContentID != "series:1" || items[1].ContentID != "series:1:season:2" {
		t.Fatalf("wrong scoped fingerprint: %+v", items)
	}
	if err := s.Set("viewer", models.ContentPreference{ContentID: "series:1", AudioLanguage: "eng", PreferLocalFiles: models.BoolPtr(false)}); err != nil {
		t.Fatal(err)
	}
	changed, _ := s.LocalOverrides("viewer", "series:1")
	if !reflect.DeepEqual(items, changed) {
		t.Fatal("source fingerprint changed solely because audio or timestamp changed")
	}
	if other, _ := s.LocalOverrides("another-viewer", "series:1"); len(other) != 0 {
		t.Fatal("profile preference leaked")
	}
}

func TestContentLocalPreferenceRejectsMalformedScopes(t *testing.T) {
	s := localPreferenceService(t)
	for _, id := range []string{"series:1:season:-1", "series:1:season:02", "series:1:season:10001", "series:1:season:2:episode:0", "series:1:season:2:episode:01", "series:1:season:2:episode:100001", "series:1:season:2:season:3"} {
		t.Run(id, func(t *testing.T) {
			if err := s.Set("viewer", models.ContentPreference{ContentID: id, ContentType: "series", PreferLocalFiles: models.BoolPtr(true)}); err == nil {
				t.Fatal("malformed scope accepted")
			}
		})
	}
	if err := s.Set("viewer", models.ContentPreference{ContentID: "movie:1:season:2", ContentType: "movie"}); err == nil {
		t.Fatal("movie season accepted")
	}
}

func TestContentLocalPreferenceFailedPersistenceRestoresState(t *testing.T) {
	s := localPreferenceService(t)
	pref := models.ContentPreference{ContentID: "movie", AudioLanguage: "fra", PreferLocalFiles: models.BoolPtr(true)}
	if err := s.Set("viewer", pref); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Get("viewer", "movie")
	oldPath := s.path
	s.path = filepath.Join(t.TempDir(), "directory")
	if err := os.Mkdir(s.path, 0700); err != nil {
		t.Fatal(err)
	}
	pref.PreferLocalFiles = models.BoolPtr(false)
	if err := s.Set("viewer", pref); err == nil {
		t.Fatal("invalid persistence target accepted")
	}
	after, _ := s.Get("viewer", "movie")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed update changed in-memory preference")
	}
	if err := s.Delete("viewer", "movie"); err == nil {
		t.Fatal("failed deletion accepted")
	}
	after, _ = s.Get("viewer", "movie")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed deletion changed preference")
	}
	s.path = oldPath
	reloaded, err := NewService(filepath.Dir(oldPath))
	if err != nil {
		t.Fatal(err)
	}
	after, _ = reloaded.Get("viewer", "movie")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed update replaced prior stored file")
	}
}

func TestContentLocalPreferenceConcurrentReadsAndUpdates(t *testing.T) {
	s := localPreferenceService(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 12; j++ {
				if err := s.Set("viewer", models.ContentPreference{ContentID: "series:1:season:2", PreferLocalFiles: models.BoolPtr((i+j)%2 == 0)}); err != nil {
					t.Error(err)
				}
				_, _ = s.Get("viewer", "series:1:season:2")
				_, _ = s.LocalOverrides("viewer", "series:1")
				_, _ = s.ResolveLocalPreference("viewer", "series:1", &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 1}, false)
			}
		}(i)
	}
	wg.Wait()
}
