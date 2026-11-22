package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/user_settings"
)

func TestAutomaticAudioLanguageProfilePatchPreservesLanguageAndExplicitFalse(t *testing.T) {
	svc, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	initial := models.UserSettings{Playback: models.PlaybackSettings{PreferredAudioLanguage: "fra", PreferLocalFiles: models.BoolPtr(true)}}
	if err := svc.Update("profile", initial); err != nil {
		t.Fatal(err)
	}
	cfg := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	global := config.DefaultSettings()
	global.Playback.AutoCheckAudioLanguage = true
	global.UI.UserEditableSettings = []string{"playback.autoCheckAudioLanguage"}
	if err := cfg.Save(global); err != nil {
		t.Fatal(err)
	}
	h := handlers.NewUserSettingsHandler(svc, &fakeUserExistsService{exists: true}, cfg)
	for _, wanted := range []bool{false, true, false} {
		request := userSettingsRequest(http.MethodPatch, "/", map[string]any{"path": "playback.autoCheckAudioLanguage", "value": wanted}, map[string]string{"userID": "profile"})
		w := httptest.NewRecorder()
		h.PatchFrontendSetting(w, request)
		if w.Code != http.StatusOK {
			t.Fatalf("profile patch status=%d body=%s", w.Code, w.Body.String())
		}
		stored, err := svc.Get("profile")
		if err != nil || stored.Playback.AutoCheckAudioLanguage == nil || *stored.Playback.AutoCheckAudioLanguage != wanted || stored.Playback.PreferredAudioLanguage != "fra" || !models.BoolVal(stored.Playback.PreferLocalFiles, false) {
			t.Fatal("patch lost explicit choice or unrelated source/language preferences")
		}
	}
}
