package user_settings

import (
	"novastream/models"
	"testing"
)

func TestKidsLivePolicyRequiresDedicatedUpdateAndSurvivesReload(t *testing.T) {
	dir := t.TempDir()
	service, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	enabled := models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 6, GuideSourceID: "chosen-guide"}
	// A regular profile write cannot enable or replace the parental choice.
	if err = service.Update("ella", models.UserSettings{LiveTV: models.LiveTVSettings{KidsAgeLimit: enabled}}); err != nil {
		t.Fatal(err)
	}
	got, err := service.Get("ella")
	if err != nil || got != nil && got.LiveTV.KidsAgeLimit.Enabled {
		t.Fatal("ordinary settings enabled kids Live", err)
	}
	if err = service.UpdateKidsLive("ella", enabled); err != nil {
		t.Fatal(err)
	}
	if err = service.Update("ella", models.UserSettings{}); err != nil {
		t.Fatal(err)
	}
	restored, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = restored.Get("ella")
	if err != nil || got == nil || got.LiveTV.KidsAgeLimit != enabled {
		t.Fatal("legacy settings or reload lost explicit guide choice", got, err)
	}
	other, _ := restored.Get("other")
	if other != nil && other.LiveTV.KidsAgeLimit.Enabled {
		t.Fatal("choice crossed profile")
	}
	enabled.Enabled = false
	if err = restored.UpdateKidsLive("ella", enabled); err != nil {
		t.Fatal(err)
	}
	if err = restored.Update("ella", models.UserSettings{LiveTV: models.LiveTVSettings{KidsAgeLimit: models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 18, GuideSourceID: "other"}}}); err != nil {
		t.Fatal(err)
	}
	final, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = final.Get("ella")
	if err != nil || got == nil || got.LiveTV.KidsAgeLimit != enabled {
		t.Fatal("explicit off was replaced", got, err)
	}
}
