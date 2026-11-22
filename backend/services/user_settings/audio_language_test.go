package user_settings

import (
	"novastream/models"
	"testing"
)

func TestAutomaticAudioLanguagePreferenceInheritsWithoutLosingExplicitFalse(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	defaults := models.DefaultUserSettings()
	if models.BoolVal(defaults.Playback.AutoCheckAudioLanguage, true) {
		t.Fatal("new profiles enabled analysis by default")
	}
	defaults.Playback.AutoCheckAudioLanguage = models.BoolPtr(true)
	if err := svc.Update("inherited", models.UserSettings{Playback: models.PlaybackSettings{PreferredAudioLanguage: "fra"}}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Update("disabled", models.UserSettings{Playback: models.PlaybackSettings{AutoCheckAudioLanguage: models.BoolPtr(false), PreferredAudioLanguage: "spa"}}); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetWithDefaults("inherited", defaults)
	if err != nil || !models.BoolVal(got.Playback.AutoCheckAudioLanguage, false) {
		t.Fatal("missing preference did not inherit")
	}
	got, err = svc.GetWithDefaults("disabled", defaults)
	if err != nil || models.BoolVal(got.Playback.AutoCheckAudioLanguage, true) || got.Playback.PreferredAudioLanguage != "spa" {
		t.Fatal("explicit opt-out or unrelated language was lost")
	}
	reopened, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = reopened.GetWithDefaults("disabled", defaults)
	if err != nil || models.BoolVal(got.Playback.AutoCheckAudioLanguage, true) {
		t.Fatal("opt-out did not survive reload")
	}
}
