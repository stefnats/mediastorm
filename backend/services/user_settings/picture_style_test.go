package user_settings

import (
	"fmt"
	"testing"

	"novastream/models"
)

func TestPictureStylePersistsOnlyOptedInBoundedDistinctChoices(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sample := models.PictureStyleSample{TitleID: "movie:tmdb:1", Kind: "poster", Language: "fra"}
	if err = s.RecordPictureStyle("viewer", []string{sample.TitleID}, sample.Kind, &sample); err != nil {
		t.Fatal(err)
	}
	state, _ := s.GetPictureStyle("viewer")
	if state.Enabled || len(state.Samples) != 0 {
		t.Fatal("disabled profile learned", state)
	}
	enabled := true
	if _, err = s.UpdatePictureStyle("viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < models.MaxPictureStyleSamples+2; i++ {
		sample.TitleID = fmt.Sprintf("movie:tmdb:%d", i)
		if err = s.RecordPictureStyle("viewer", []string{sample.TitleID}, sample.Kind, &sample); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.RecordPictureStyle("viewer", []string{sample.TitleID}, sample.Kind, &sample); err != nil {
		t.Fatal(err)
	}
	state, _ = s.GetPictureStyle("viewer")
	if !state.Enabled || len(state.Samples) != 128 || state.Samples[0].TitleID != "movie:tmdb:2" {
		t.Fatal("unbounded/duplicate learning", state)
	}
	reopened, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	state, _ = reopened.GetPictureStyle("viewer")
	if !state.Enabled || len(state.Samples) != 128 {
		t.Fatal("learning did not survive restart", state)
	}
	state.Samples[0].Language = "deu"
	again, _ := reopened.GetPictureStyle("viewer")
	if again.Samples[0].Language != "fra" {
		t.Fatal("returned sample aliased stored snapshot")
	}
	other, _ := reopened.GetPictureStyle("other")
	if other.Enabled || len(other.Samples) != 0 {
		t.Fatal("cross-profile training")
	}
}
func TestPictureStyleResetAndLegacySettingsPreserveUnrelatedData(t *testing.T) {
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Update("viewer", models.UserSettings{Metadata: models.MetadataSettings{PrimaryLanguage: "spa"}}); err != nil {
		t.Fatal(err)
	}
	enabled := true
	if _, err = s.UpdatePictureStyle("viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	value := models.PictureStyleSample{TitleID: "movie:tmdb:1", Kind: "poster", Textless: true}
	if err = s.RecordPictureStyle("viewer", []string{value.TitleID}, value.Kind, &value); err != nil {
		t.Fatal(err)
	}
	if err = s.Update("viewer", models.UserSettings{Metadata: models.MetadataSettings{PrimaryLanguage: "fra"}}); err != nil {
		t.Fatal(err)
	}
	current, _ := s.GetPictureStyle("viewer")
	if !current.Enabled || len(current.Samples) != 1 {
		t.Fatal("legacy settings erased dedicated state", current)
	}
	if _, err = s.UpdatePictureStyle("viewer", nil, true); err != nil {
		t.Fatal(err)
	}
	current, _ = s.GetPictureStyle("viewer")
	settings, _ := s.Get("viewer")
	if current.Enabled || len(current.Samples) != 0 || settings.Metadata.PrimaryLanguage != "fra" {
		t.Fatal("reset changed another preference", settings, current)
	}
	if _, err = s.UpdatePictureStyle("viewer", nil, false); err == nil {
		t.Fatal("accepted empty action")
	}
	if _, err = s.UpdatePictureStyle("viewer", &enabled, true); err == nil {
		t.Fatal("accepted conflicting actions")
	}
}
func TestPictureStyleStorageFailureDoesNotInstallNewSnapshot(t *testing.T) {
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.path = t.TempDir() + "/absent/settings.json"
	enabled := true
	if _, err = s.UpdatePictureStyle("viewer", &enabled, false); err == nil {
		t.Fatal("unwritable snapshot reported success")
	}
	state, _ := s.GetPictureStyle("viewer")
	if state.Enabled {
		t.Fatal("failed write installed snapshot")
	}
}

func TestPictureStyleSnapshotRoundtripsPostgreSQLAndPreservesSettings(t *testing.T) {
	f := newSettingsPG(t)
	f.user(t, "style-viewer")
	s := f.service(t, f.ds)
	if err := s.Update("style-viewer", models.UserSettings{Metadata: models.MetadataSettings{PrimaryLanguage: "spa"}}); err != nil {
		t.Fatal(err)
	}
	enabled := true
	if _, err := s.UpdatePictureStyle("style-viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	sample := models.PictureStyleSample{TitleID: "movie:tmdb:1", Kind: "poster", Language: "fra"}
	if err := s.RecordPictureStyle("style-viewer", []string{sample.TitleID}, sample.Kind, &sample); err != nil {
		t.Fatal(err)
	}
	f.ds.Close()
	f.ds = f.open(t)
	s = f.service(t, f.ds)
	state, err := s.GetPictureStyle("style-viewer")
	settings, _ := s.Get("style-viewer")
	if err != nil || !state.Enabled || len(state.Samples) != 1 || settings.Metadata.PrimaryLanguage != "spa" {
		t.Fatal("learned snapshot did not survive PostgreSQL reopen", state, err)
	}
	if _, err = s.UpdatePictureStyle("style-viewer", nil, true); err != nil {
		t.Fatal(err)
	}
	state, err = s.GetPictureStyle("style-viewer")
	settings, _ = s.Get("style-viewer")
	if err != nil || state.Enabled || len(state.Samples) != 0 || settings.Metadata.PrimaryLanguage != "spa" {
		t.Fatal("reset changed unrelated PostgreSQL settings", state, err)
	}
}
