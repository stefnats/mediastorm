package user_settings

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"novastream/models"
)

func stationFixture(n int) string { return fmt.Sprintf("station:%024x", n) }

func TestStationFavoritesPreserveSettingsAndSurviveLegacyWritesAndReload(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	original := models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteChannels: []string{"source:rtl"}, HiddenChannels: []string{"hidden"}, SelectedCategories: []string{"News"}}, Metadata: models.MetadataSettings{PrimaryLanguage: "fr"}}
	if err = s.Update("viewer", original); err != nil {
		t.Fatal(err)
	}
	ids, err := s.SetFavoriteStation("viewer", stationFixture(1), true)
	if err != nil {
		t.Fatal(err)
	}
	ids[0] = "mutated"
	stale := original
	stale.LiveTV.FavoriteStationIDs = []string{stationFixture(9)}
	if err = s.Update("viewer", stale); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("viewer")
	if !reflect.DeepEqual(got.LiveTV.FavoriteStationIDs, []string{stationFixture(1)}) || !reflect.DeepEqual(got.LiveTV.FavoriteChannels, original.LiveTV.FavoriteChannels) || got.Metadata.PrimaryLanguage != "fr" {
		t.Fatalf("dedicated field or unrelated data lost: %+v", got)
	}
	got.LiveTV.FavoriteStationIDs[0] = "mutated"
	merged, _ := s.GetWithDefaults("viewer", models.DefaultUserSettings())
	merged.LiveTV.FavoriteStationIDs[0] = "also-mutated"
	s, err = NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get("viewer")
	if got.LiveTV.FavoriteStationIDs[0] != stationFixture(1) {
		t.Fatal("read shared a mutable set or reload lost data")
	}
	if _, err = s.SetFavoriteStation("other", stationFixture(2), true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation("viewer", stationFixture(1), false); err != nil {
		t.Fatal(err)
	}
	other, _ := s.Get("other")
	if len(other.LiveTV.FavoriteStationIDs) != 1 {
		t.Fatal("profile mutation crossed scope")
	}
}

func TestStationFavoritesConcurrentIndependentActionsAndFailureRollback(t *testing.T) {
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := s.SetFavoriteStation("viewer", stationFixture(n), true); err != nil {
				t.Error(err)
			}
			if err := s.Update("viewer", models.UserSettings{}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	got, _ := s.Get("viewer")
	if got == nil || len(got.LiveTV.FavoriteStationIDs) != 32 {
		t.Fatalf("concurrent idempotent mutations lost: %+v", got)
	}
	before := append([]string{}, got.LiveTV.FavoriteStationIDs...)
	s.path = filepath.Join(t.TempDir(), "missing", "settings.json")
	if _, err = s.SetFavoriteStation("viewer", stationFixture(1), true); err != nil {
		t.Fatal("idempotent no-op attempted a write", err)
	}
	if _, err = s.SetFavoriteStation("viewer", stationFixture(99), true); err == nil {
		t.Fatal("save failure ignored")
	}
	got, _ = s.Get("viewer")
	if !reflect.DeepEqual(before, got.LiveTV.FavoriteStationIDs) {
		t.Fatal("failed write changed cached set")
	}
	if _, err = s.SetFavoriteStation("new-profile", stationFixture(1), true); err == nil {
		t.Fatal("first save failure ignored")
	}
	if got, _ := s.Get("new-profile"); got != nil {
		t.Fatal("failed first write left a profile override")
	}
}

func TestStationFavoriteLimitIsRecoverableAndLastRemovalIsDurable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, models.MaxFavoriteStations)
	for i := range ids {
		ids[i] = stationFixture(i + 1)
	}
	s.settings["viewer"] = models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteStationIDs: ids}}
	if _, err = s.SetFavoriteStation("viewer", stationFixture(models.MaxFavoriteStations+1), true); err != ErrFavoriteStationLimit {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation("viewer", ids[0], false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation("viewer", stationFixture(models.MaxFavoriteStations+1), true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation("single", stationFixture(1), true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation("single", stationFixture(1), false); err != nil {
		t.Fatal(err)
	}
	s, err = NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("single"); got != nil {
		t.Fatal("last removal not persisted")
	}
	if _, err := os.Stat(filepath.Join(dir, "user_settings.json")); err != nil {
		t.Fatal(err)
	}
}
