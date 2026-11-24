package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"novastream/models"
	"novastream/services/picturestyle"
	"novastream/services/titleratings"
	"novastream/services/user_settings"
)

func TestPictureStyleLearningWireUsesExplicitChoicesAndKeepsStandardReset(t *testing.T) {
	h, prefs := contentLocalHandler(t)
	settings, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.PictureStyles = &picturestyle.Service{Settings: settings, Preferences: prefs}
	registry := titleratings.New(nil)
	h.ArtworkTitles = registry
	learning := func(body string) picturestyle.Status {
		t.Helper()
		w := httptest.NewRecorder()
		h.PictureStyleLearning(w, contentLocalRequest(http.MethodPut, "viewer", body))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var value picturestyle.Status
		if json.Unmarshal(w.Body.Bytes(), &value) != nil {
			t.Fatal(w.Body.String())
		}
		return value
	}
	if value := learning(`{"enabled":true}`); !value.Supported || !value.Enabled || value.ProfileID != "viewer" || value.MinimumSamples != 3 || value.MaxSamples != 128 {
		t.Fatal(value)
	}
	choose := func(id, url string) {
		t.Helper()
		body := fmt.Sprintf(`{"contentId":%q,"contentType":"movie","posterUrl":%s}`, id, url)
		r := contentLocalRequest(http.MethodPut, "viewer", body)
		r.Header.Set("X-Client-ID", "screen")
		w := httptest.NewRecorder()
		h.SetArtworkPreference(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for i := 1; i <= 3; i++ {
		title := models.Title{ID: fmt.Sprintf("tmdb:movie:%d", i), MediaType: "movie", Poster: &models.Image{URL: fmt.Sprintf("https://synthetic.invalid/%d/fr", i), Language: "fr"}}
		registry.Observe("viewer", "screen", []models.Title{title})
		choose(title.ID, fmt.Sprintf("%q", title.Poster.URL))
	}
	status, err := h.PictureStyles.Status("viewer")
	if err != nil || status.PosterSamples != 3 {
		t.Fatal(status, err)
	}
	title := models.Title{ID: "tmdb:movie:999", MediaType: "movie", Poster: &models.Image{URL: "https://synthetic.invalid/default", Language: "en"}, Posters: []models.Image{{URL: "https://synthetic.invalid/french", Language: "fr"}}}
	metadata := &MetadataHandler{PictureStyles: h.PictureStyles}
	render := func() models.Title {
		t.Helper()
		r := contentLocalRequest(http.MethodGet, "viewer", "")
		w := httptest.NewRecorder()
		if err := metadata.encodeMetadata(w, r, title); err != nil {
			t.Fatal(err)
		}
		var got models.Title
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := render(); got.Poster.URL != title.Posters[0].URL {
		t.Fatal("normal catalogue response did not learn", got)
	}
	choose(title.ID, "null")
	stored, _ := prefs.Get("viewer", title.ID)
	if stored == nil || !stored.PosterUseStandard {
		t.Fatal("explicit standard intent not stored", stored)
	}
	if got := render(); got.Poster.URL != title.Poster.URL {
		t.Fatal("null reset silently became automatic", got)
	}
	w := httptest.NewRecorder()
	h.SetPreference(w, contentLocalRequest(http.MethodPut, "viewer", `{"contentId":"tmdb:movie:999","audioLanguage":"spa"}`))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	stored, _ = prefs.Get("viewer", title.ID)
	if !stored.PosterUseStandard || stored.AudioLanguage != "spa" {
		t.Fatal("old client erased standard intent", stored)
	}
	if value := learning(`{"reset":true}`); value.Enabled || value.PosterSamples != 0 || value.BackdropSamples != 0 {
		t.Fatal("clear did not erase and disable", value)
	}
	stored, _ = prefs.Get("viewer", title.ID)
	if !stored.PosterUseStandard || stored.AudioLanguage != "spa" {
		t.Fatal("clear erased explicit choices")
	}
}
func TestPictureStyleLearningRejectsInvalidActionsAndOtherProfile(t *testing.T) {
	h, prefs := contentLocalHandler(t)
	settings, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.PictureStyles = &picturestyle.Service{Settings: settings, Preferences: prefs}
	for _, body := range []string{`{}`, `null`, `{"enabled":null}`, `{"enabled":"yes"}`, `{"enabled":true,"reset":true}`, `{"reset":false}`, `{"samples":[]}`, `{"enabled":true} {}`} {
		w := httptest.NewRecorder()
		h.PictureStyleLearning(w, contentLocalRequest(http.MethodPut, "viewer", body))
		if w.Code != 400 {
			t.Fatal(body, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.PictureStyleLearning(w, contentLocalRequest(http.MethodPut, "foreign", `{"enabled":true}`))
	if w.Code != 404 {
		t.Fatal("changed another account profile", w.Code)
	}
	status, _ := h.PictureStyles.Status("viewer")
	if status.Enabled || status.PosterSamples != 0 {
		t.Fatal("invalid write changed state", status)
	}
}

type pictureStyleDetailsMetadata struct {
	metadataService
	title   models.Title
	similar []models.Title
}

func (m *pictureStyleDetailsMetadata) MovieDetails(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	return &m.title, nil
}
func (m *pictureStyleDetailsMetadata) Similar(context.Context, string, int64) ([]models.Title, error) {
	return m.similar, nil
}
func (m *pictureStyleDetailsMetadata) Trailers(context.Context, models.TrailerQuery) (*models.TrailerResponse, error) {
	return nil, nil
}

type pictureStyleDetailsHistory struct{ historyService }

func (*pictureStyleDetailsHistory) ListPlaybackProgress(string) ([]models.PlaybackProgress, error) {
	return nil, nil
}
func TestPictureStyleDetailsBundleAndShellProjectOnlyResponseCopies(t *testing.T) {
	prefsHandler, prefs := contentLocalHandler(t)
	settings, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	styles := &picturestyle.Service{Settings: settings, Preferences: prefs}
	enabled := true
	if _, err = styles.Update("viewer", &enabled, false); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err = styles.ObserveChoice("viewer", models.Title{ID: fmt.Sprintf("tmdb:movie:%d", i), MediaType: "movie", TMDBID: int64(i)}, "poster", &models.Image{URL: "synthetic", Language: "fr"}); err != nil {
			t.Fatal(err)
		}
	}
	original := models.Title{ID: "tmdb:movie:999", MediaType: "movie", TMDBID: 999, Poster: &models.Image{URL: "default", Language: "en"}, TextPoster: &models.Image{URL: "default-text", Language: "en"}, Posters: []models.Image{{URL: "french", Language: "fr"}}}
	similar := original
	similar.ID = "tmdb:movie:998"
	similar.TMDBID = 998
	meta := &pictureStyleDetailsMetadata{title: original, similar: []models.Title{similar}}
	h := NewDetailsBundleHandler(meta, &pictureStyleDetailsHistory{}, prefs, prefsHandler.Users)
	h.PictureStyles = styles
	observed := false
	h.SetCatalogueObserver(func(_ *http.Request, titles []models.Title) {
		observed = true
		for _, title := range titles {
			if title.Poster.URL != "default" || title.StandardPoster != nil {
				t.Fatal("observer received personalized instead of original gallery")
			}
		}
	})
	call := func(bundle bool) (*models.Title, []models.Title) {
		t.Helper()
		r := contentLocalRequest(http.MethodGet, "viewer", "")
		q := r.URL.Query()
		q.Set("type", "movie")
		q.Set("titleId", original.ID)
		q.Set("tmdbId", "999")
		r.URL.RawQuery = q.Encode()
		w := httptest.NewRecorder()
		if bundle {
			h.GetDetailsBundle(w, r)
		} else {
			h.GetDetailsShell(w, r)
		}
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var result DetailsBundleResponse
		if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result.MovieDetails, result.Similar
	}
	for _, bundle := range []bool{false, true} {
		movie, items := call(bundle)
		if movie == nil || movie.Poster.URL != "french" || movie.StandardPoster == nil || movie.StandardPoster.URL != "default-text" {
			t.Fatal("detail route missed automatic projection", movie)
		}
		if bundle && (len(items) != 1 || items[0].Poster.URL != "french") {
			t.Fatal("similar cards missed automatic projection", items)
		}
	}
	if !observed || meta.title.Poster.URL != "default" || meta.title.StandardPoster != nil || len(meta.title.Posters) != 1 || meta.similar[0].Poster.URL != "default" {
		t.Fatal("detail cache mutated")
	}
	if err = prefs.Set("viewer", models.ContentPreference{ContentID: original.ID, ContentType: "movie", PosterUseStandard: true}); err != nil {
		t.Fatal(err)
	}
	movie, _ := call(true)
	if movie.Poster.URL != "default" || movie.StandardPoster != nil {
		t.Fatal("explicit default ignored in details", movie)
	}
	other, _, _ := h.applyPictureStyles("other", &original, nil, nil)
	if other.Poster.URL != "default" || other.StandardPoster != nil {
		t.Fatal("another profile inherited picture style")
	}
}
