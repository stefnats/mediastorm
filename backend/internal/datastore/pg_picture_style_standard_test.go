package datastore

import (
	"novastream/models"
	"testing"
)

func TestPictureStyleExplicitStandardSurvivesPostgreSQLReopen(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "picture-style-viewer")
	pref := models.ContentPreference{ContentID: "tmdb:movie:66", ContentType: "movie", AudioLanguage: "fra", PosterUseStandard: true, BackdropURL: "https://synthetic.invalid/manual"}
	if err := f.ds.ContentPreferences().Upsert(t.Context(), "picture-style-viewer", &pref); err != nil {
		t.Fatal(err)
	}
	f.ds.Close()
	f.ds = f.open(t)
	saved, err := f.ds.ContentPreferences().Get(t.Context(), "picture-style-viewer", pref.ContentID)
	if err != nil || saved == nil || !saved.PosterUseStandard || saved.BackdropUseStandard || saved.BackdropURL != pref.BackdropURL || saved.AudioLanguage != "fra" {
		t.Fatal("standard intent lost on reopen", saved, err)
	}
	list, err := f.ds.ContentPreferences().ListByUser(t.Context(), "picture-style-viewer")
	if err != nil || len(list) != 1 || !list[0].PosterUseStandard {
		t.Fatal("list lost standard intent", list, err)
	}
	saved.PosterUseStandard = false
	saved.PosterURL = "https://synthetic.invalid/manual-poster"
	if err = f.ds.ContentPreferences().Upsert(t.Context(), "picture-style-viewer", saved); err != nil {
		t.Fatal(err)
	}
	saved, err = f.ds.ContentPreferences().Get(t.Context(), "picture-style-viewer", pref.ContentID)
	if err != nil || saved == nil || saved.PosterUseStandard || saved.PosterURL == "" {
		t.Fatal("new explicit choice retained standard mode", saved, err)
	}
}

func TestPictureStyleStandardMigrationPreservesLegacyRowsAndReapplies(t *testing.T) {
	f := recordingStatePG(t, 94)
	f.profiles(t)
	f.exec(t, `INSERT INTO content_preferences(user_id,content_id,content_type,audio_language,poster_url,backdrop_url) VALUES('one','tmdb:movie:77','movie','spa','','https://synthetic.invalid/retained')`)
	f.migrate(t, 95, false)
	got, err := f.ds.ContentPreferences().Get(t.Context(), "one", "tmdb:movie:77")
	if err != nil || got == nil || got.PosterUseStandard || got.BackdropUseStandard || got.AudioLanguage != "spa" || got.BackdropURL != "https://synthetic.invalid/retained" {
		t.Fatal("legacy row changed or acquired default intent", got, err)
	}
	f.migrate(t, 95, false)
	got, err = f.ds.ContentPreferences().Get(t.Context(), "one", "tmdb:movie:77")
	if err != nil || got == nil || got.PosterUseStandard || got.BackdropUseStandard || got.AudioLanguage != "spa" {
		t.Fatal("repeat migration changed row", got, err)
	}
}
