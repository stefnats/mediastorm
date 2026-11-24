package picturestyle_test

import (
	"fmt"
	"testing"

	"novastream/models"
	"novastream/services/picturestyle"
	"novastream/services/user_settings"
)

type preferences struct {
	values []models.ContentPreference
	err    error
}

func (p *preferences) List(string) ([]models.ContentPreference, error) { return p.values, p.err }
func learner(t *testing.T) (*picturestyle.Service, *user_settings.Service, *preferences) {
	t.Helper()
	settings, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	prefs := &preferences{}
	return &picturestyle.Service{Settings: settings, Preferences: prefs}, settings, prefs
}
func train(t *testing.T, s *picturestyle.Service, n int, image *models.Image) {
	t.Helper()
	enabled := true
	if _, err := s.Update("viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		title := models.Title{ID: fmt.Sprintf("tmdb:movie:%d", i), MediaType: "movie", TMDBID: int64(i)}
		if err := s.ObserveChoice("viewer", title, "poster", image); err != nil {
			t.Fatal(err)
		}
	}
}
func candidate() models.Title {
	return models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "https://synthetic.invalid/default", Language: "en"}, Posters: []models.Image{{URL: "https://synthetic.invalid/french", Language: "fr"}, {URL: "https://synthetic.invalid/unknown", IsDark: true}}}
}
func TestPictureStyleRequiresDistinctKnownChoicesBeforeSelectingCurrentVariant(t *testing.T) {
	s, _, _ := learner(t)
	title := candidate()
	train(t, s, 2, &models.Image{URL: "chosen", Language: "fr"})
	if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != title.Poster.URL {
		t.Fatal("insufficient choices changed standard")
	}
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	got := s.Annotate("viewer", []models.Title{title})
	if got[0].Poster.URL != title.Posters[0].URL || got[0].TextPoster.URL != title.Posters[0].URL {
		t.Fatal("learned style not applied to current variant", got)
	}
	if title.Poster.URL != "https://synthetic.invalid/default" || title.TextPoster != nil || title.Posters[0].Language != "fr" {
		t.Fatal("shared metadata mutated")
	}
	if other := s.Annotate("other", []models.Title{title}); other[0].Poster.URL != title.Poster.URL {
		t.Fatal("profile state leaked")
	}
}
func TestPictureStyleUnknownContradictoryAndBrightnessAreNotTraining(t *testing.T) {
	for _, image := range []models.Image{{URL: "unknown"}, {URL: "dark", IsDark: true}, {URL: "contradiction", Language: "fr", IsTextless: true}, {URL: "unknown-language", Language: "zz"}} {
		t.Run(image.URL, func(t *testing.T) {
			s, _, _ := learner(t)
			train(t, s, 3, &image)
			status, err := s.Status("viewer")
			if err != nil || status.PosterSamples != 0 {
				t.Fatal("unproven descriptor trained", status, err)
			}
			title := candidate()
			if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != title.Poster.URL {
				t.Fatal("unknown signal personalized")
			}
		})
	}
}
func TestPictureStyleManualAndExplicitStandardAlwaysWin(t *testing.T) {
	s, _, prefs := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := candidate()
	for _, pref := range []models.ContentPreference{{ContentID: title.ID, PosterURL: "manual"}, {ContentID: title.ID, PosterUseStandard: true}} {
		prefs.values = []models.ContentPreference{pref}
		if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != title.Poster.URL {
			t.Fatal("automatic selection overrode manual intent")
		}
	}
}
func TestPictureStyleTextlessAndNeutralTiesKeepExistingOrder(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", IsTextless: true})
	title := candidate()
	title.Posters = append(title.Posters, models.Image{URL: "textless", IsTextless: true})
	if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != "textless" {
		t.Fatal("textless style not learned")
	}
	title.Poster = &models.Image{URL: "standard-textless", IsTextless: true}
	if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != "standard-textless" {
		t.Fatal("equal evidence rotated picture")
	}
}
func TestPictureStyleResetRemovesOnlyRequestedTrainingAndAllLearningCanBeCleared(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := models.Title{ID: "tmdb:movie:1", MediaType: "movie", TMDBID: 1}
	if err := s.ObserveChoice("viewer", title, "poster", nil); err != nil {
		t.Fatal(err)
	}
	status, _ := s.Status("viewer")
	if status.PosterSamples != 2 {
		t.Fatal("manual standard reset retained signal", status)
	}
	if status, err := s.Update("viewer", nil, true); err != nil || status.Enabled || status.PosterSamples != 0 {
		t.Fatal("clear did not disable and erase", status, err)
	}
}

func TestPictureStylePreviewsRetainActualDefaultsAndClearWhenDisabled(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := candidate()
	title.TextPoster = &models.Image{URL: "localized-standard", Language: "en"}
	got := s.Annotate("viewer", []models.Title{title})
	if got[0].StandardPoster == nil || got[0].StandardPoster.URL != "localized-standard" || got[0].StandardBackdrop != nil {
		t.Fatal("incorrect standard preview", got)
	}
	disabled := false
	if _, err := s.Update("viewer", &disabled, false); err != nil {
		t.Fatal(err)
	}
	restored := s.Annotate("viewer", got)
	if restored[0].Poster.URL != "localized-standard" || restored[0].StandardPoster != nil {
		t.Fatal("stale auto preview survived disable", restored)
	}
	if got[0].StandardPoster == nil || got[0].Poster.URL != title.Posters[0].URL || title.TextPoster.URL != "localized-standard" {
		t.Fatal("restoration mutated source")
	}
}
func TestPictureStyleRequiresTwoThirdsLanguageMajority(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := models.Title{ID: "tmdb:movie:3", MediaType: "movie", TMDBID: 3}
	if err := s.ObserveChoice("viewer", title, "poster", &models.Image{URL: "english", Language: "en"}); err != nil {
		t.Fatal(err)
	}
	candidate := candidate()
	if got := s.Annotate("viewer", []models.Title{candidate}); got[0].Poster.URL != candidate.Posters[0].URL {
		t.Fatal("two-thirds evidence did not select")
	}
	title = models.Title{ID: "tmdb:movie:4", MediaType: "movie", TMDBID: 4}
	if err := s.ObserveChoice("viewer", title, "poster", &models.Image{URL: "spanish", Language: "es"}); err != nil {
		t.Fatal(err)
	}
	if got := s.Annotate("viewer", []models.Title{candidate}); got[0].Poster.URL != candidate.Poster.URL || got[0].StandardPoster != nil {
		t.Fatal("split language choices changed default")
	}
}

func TestPictureStyleAliasOverlapCannotInflateEvidenceAndSparseResetRemovesIt(t *testing.T) {
	s, _, _ := learner(t)
	enabled := true
	if _, err := s.Update("viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	image := &models.Image{URL: "verified-choice", Language: "fr"}
	original := models.Title{ID: "tt1234567", MediaType: "movie", TMDBID: 1, IMDBID: "tt1234567"}
	if err := s.ObserveChoice("viewer", original, "poster", image); err != nil {
		t.Fatal(err)
	}
	alternate := models.Title{ID: "tmdb:movie:1", MediaType: "movie", TMDBID: 1}
	if err := s.ObserveChoice("viewer", alternate, "poster", image); err != nil {
		t.Fatal(err)
	}
	sparse := models.Title{ID: "tt1234567", MediaType: "movie"}
	if err := s.ObserveChoice("viewer", sparse, "poster", image); err != nil {
		t.Fatal(err)
	}
	status, _ := s.Status("viewer")
	if status.PosterSamples != 1 {
		t.Fatal("aliases inflated distinct title count", status)
	}
	if err := s.ObserveChoice("viewer", sparse, "poster", nil); err != nil {
		t.Fatal(err)
	}
	status, _ = s.Status("viewer")
	if status.PosterSamples != 0 {
		t.Fatal("sparse reset retained enriched training", status)
	}
}

func TestPictureStyleDoesNotReplaceNegativeOriginalWithUnknownVariant(t *testing.T) {
	for _, chosen := range []models.Image{{URL: "textless", IsTextless: true}, {URL: "french", Language: "fr"}} {
		t.Run(chosen.URL, func(t *testing.T) {
			s, _, _ := learner(t)
			train(t, s, 3, &chosen)
			original := &models.Image{URL: "default", Language: "en"}
			if !chosen.IsTextless {
				original = &models.Image{URL: "default", IsTextless: true}
			}
			title := models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: original, Posters: []models.Image{{URL: "unknown"}, {URL: "conflicting", IsTextless: true, Language: "fr"}}}
			if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != original.URL || got[0].StandardPoster != nil {
				t.Fatal("unproven image replaced fallback", got)
			}
		})
	}
}
func TestPictureStyleAutomaticGalleryPreservesOriginalPrimaryAndTextChoices(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "A", Language: "en"}, TextPoster: &models.Image{URL: "B", Language: "en"}, Posters: []models.Image{{URL: "C", Language: "fr"}, {URL: "D", Language: "en"}}}
	got := s.Annotate("viewer", []models.Title{title})
	if got[0].Poster.URL != "C" || got[0].StandardPoster.URL != "B" {
		t.Fatal(got)
	}
	want := []string{"A", "B", "C", "D"}
	if len(got[0].Posters) != len(want) {
		t.Fatal("original gallery choices lost", got)
	}
	for i, url := range want {
		if got[0].Posters[i].URL != url {
			t.Fatal("gallery ordering", got)
		}
	}
	if len(title.Posters) != 2 || title.Posters[0].URL != "C" || title.Poster.URL != "A" || title.TextPoster.URL != "B" {
		t.Fatal("shared gallery mutated")
	}
}
func TestPictureStyleManualIntentSurvivesTrustedMetadataAliases(t *testing.T) {
	s, _, prefs := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	title := candidate()
	title.IMDBID = "tt7654321"
	for _, pref := range []models.ContentPreference{{ContentID: "tt7654321", ContentType: "movie", PosterURL: "manual"}, {ContentID: "tt7654321", ContentType: "movie", PosterUseStandard: true}} {
		prefs.values = []models.ContentPreference{pref}
		if got := s.Annotate("viewer", []models.Title{title}); got[0].Poster.URL != title.Poster.URL {
			t.Fatal("trusted alias lost explicit manual intent", got)
		}
	}
}

func TestPictureStyleMissingBothStandardPicturesKeepsMetadataAbsence(t *testing.T) {
	s, _, _ := learner(t)
	train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
	for i := 1; i <= 3; i++ {
		if err := s.ObserveChoice("viewer", models.Title{ID: fmt.Sprintf("tmdb:movie:%d", i), MediaType: "movie", TMDBID: int64(i)}, "backdrop", &models.Image{URL: "chosen-backdrop", Language: "fr"}); err != nil {
			t.Fatal(err)
		}
	}
	title := models.Title{ID: "tmdb:movie:999", MediaType: "movie", Posters: []models.Image{{URL: "french", Language: "fr"}}, Backdrops: []models.Image{{URL: "french-background", Language: "fr"}}}
	got := s.Annotate("viewer", []models.Title{title})[0]
	if got.Poster != nil || got.TextPoster != nil || got.StandardPoster != nil || got.Backdrop != nil || got.StandardBackdrop != nil {
		t.Fatal("missing standard was invented by automatic style", got)
	}
	title.Poster = &models.Image{URL: "", Language: "en"}
	got = s.Annotate("viewer", []models.Title{title})[0]
	if got.Poster.URL != "" || got.StandardPoster != nil {
		t.Fatal("empty standard URL was personalized", got)
	}
}
func TestPictureStyleSameEffectiveURLAndConflictingDuplicatesCannotPersonalize(t *testing.T) {
	for _, tc := range []struct {
		name  string
		title models.Title
	}{
		{"same-effective-url", models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "standard", Language: "en"}, Posters: []models.Image{{URL: "standard", Language: "fr"}}}},
		{"same-text-effective-url", models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "primary", Language: "en"}, TextPoster: &models.Image{URL: "standard", Language: "en"}, Posters: []models.Image{{URL: "standard", Language: "fr"}}}},
		{"conflicting-variant-url", models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "standard", Language: "en"}, Posters: []models.Image{{URL: "variant", Language: "fr"}, {URL: "variant", Language: "en"}}}},
		{"unknown-duplicate-url", models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "standard", Language: "en"}, Posters: []models.Image{{URL: "variant", Language: "fr"}, {URL: "variant"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := learner(t)
			train(t, s, 3, &models.Image{URL: "chosen", Language: "fr"})
			got := s.Annotate("viewer", []models.Title{tc.title})[0]
			if got.Poster.URL != tc.title.Poster.URL || got.StandardPoster != nil {
				t.Fatal("same/ambiguous URL acquired automatic marker", got)
			}
		})
	}
}
