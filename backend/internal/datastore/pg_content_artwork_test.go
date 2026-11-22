package datastore

import (
	"novastream/models"
	"testing"
)

func TestContentArtworkPreferenceRoundtripsPostgreSQL(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "artwork-viewer")
	local := false
	pref := models.ContentPreference{ContentID: "tmdb:movie:55", ContentType: "movie", AudioLanguage: "fra", SubtitleLanguage: "spa", PosterURL: "https://images.example/french.jpg", BackdropURL: "https://images.example/backdrop.jpg", PreferLocalFiles: &local}
	if err := f.ds.ContentPreferences().Upsert(t.Context(), "artwork-viewer", &pref); err != nil {
		t.Fatal(err)
	}
	f.ds.Close()
	f.ds = f.open(t)
	got, err := f.ds.ContentPreferences().Get(t.Context(), "artwork-viewer", pref.ContentID)
	if err != nil || got == nil || got.PosterURL != pref.PosterURL || got.BackdropURL != pref.BackdropURL || got.AudioLanguage != "fra" || got.PreferLocalFiles == nil || *got.PreferLocalFiles {
		t.Fatal("artwork did not survive database reopen", got, err)
	}
	list, err := f.ds.ContentPreferences().ListByUser(t.Context(), "artwork-viewer")
	if err != nil || len(list) != 1 || list[0].PosterURL != pref.PosterURL {
		t.Fatal("artwork missing from profile list", list, err)
	}
	got.PosterURL = ""
	if err = f.ds.ContentPreferences().Upsert(t.Context(), "artwork-viewer", got); err != nil {
		t.Fatal(err)
	}
	got, err = f.ds.ContentPreferences().Get(t.Context(), "artwork-viewer", pref.ContentID)
	if err != nil || got.PosterURL != "" || got.BackdropURL != pref.BackdropURL {
		t.Fatal("poster default did not preserve backdrop", got, err)
	}
}
