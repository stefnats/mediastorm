package handlers

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"novastream/models"
	"novastream/services/content_preferences"
	"novastream/services/titleratings"
)

func TestContentArtworkChoicePreservesPreferencesAndRestoresDefaults(t *testing.T) {
	h, _ := contentLocalHandler(t)
	dir := t.TempDir()
	service, err := content_preferences.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	h.Service = service
	registry := titleratings.New(nil)
	h.ArtworkTitles = registry
	title := models.Title{ID: "tmdb:movie:55", MediaType: "movie", Poster: &models.Image{URL: "https://images.example/default.jpg"}, Posters: []models.Image{{URL: "https://images.example/french.jpg"}}, Backdrop: &models.Image{URL: "https://images.example/backdrop.jpg"}}
	registry.Observe("viewer", "screen", []models.Title{title})
	local := true
	if err := service.Set("viewer", models.ContentPreference{ContentID: title.ID, ContentType: "movie", AudioLanguage: "fra", SubtitleLanguage: "spa", PreferLocalFiles: &local}); err != nil {
		t.Fatal(err)
	}
	call := func(body string) *httptest.ResponseRecorder {
		r := contentLocalRequest("PUT", "viewer", body)
		r.Header.Set("X-Client-ID", "screen")
		w := httptest.NewRecorder()
		h.SetArtworkPreference(w, r)
		return w
	}
	w := call(`{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":"https://images.example/french.jpg","backdropUrl":"https://images.example/backdrop.jpg"}`)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var reply map[string]any
	if json.Unmarshal(w.Body.Bytes(), &reply) != nil || reply["artworkPreferenceSupported"] != true {
		t.Fatal(w.Body.String())
	}
	pref, _ := service.Get("viewer", title.ID)
	if pref.PosterURL != title.Posters[0].URL || pref.BackdropURL != title.Backdrop.URL || pref.AudioLanguage != "fra" || pref.SubtitleLanguage != "spa" || pref.PreferLocalFiles == nil || !*pref.PreferLocalFiles {
		t.Fatal("artwork erased existing choices", pref)
	}
	// A generic older audio client cannot erase pictures it does not know about.
	r := contentLocalRequest("PUT", "viewer", `{"contentId":"tmdb:movie:55","audioLanguage":"eng"}`)
	w = httptest.NewRecorder()
	h.SetPreference(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	pref, _ = service.Get("viewer", title.ID)
	if pref.PosterURL != title.Posters[0].URL || pref.BackdropURL != title.Backdrop.URL {
		t.Fatal("old client erased personal images", pref)
	}
	if w = call(`{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":null}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	pref, _ = service.Get("viewer", title.ID)
	if pref.PosterURL != "" || pref.BackdropURL != title.Backdrop.URL || pref.AudioLanguage != "eng" {
		t.Fatal("reset erased omitted fields", pref)
	}
	// Reload the actual file-backed service, rather than reusing its memory.
	restarted, err := content_preferences.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := restarted.Get("viewer", title.ID)
	if err != nil || saved == nil || saved.PosterURL != "" || saved.BackdropURL != title.Backdrop.URL || saved.AudioLanguage != "eng" {
		t.Fatal("artwork choice did not survive actual reload", saved, err)
	}
}

func TestContentArtworkChoiceUsesAvailableTitleVariants(t *testing.T) {
	h, service := contentLocalHandler(t)
	h.ArtworkTitles = titleratings.New(nil)
	registry := h.ArtworkTitles.(*titleratings.Service)
	title := models.Title{ID: "tmdb:movie:55", MediaType: "movie", Poster: &models.Image{URL: "https://images.example/available.jpg"}}
	registry.Observe("viewer", "screen", []models.Title{title})
	for _, tc := range []struct {
		name, body, client string
		code               int
	}{
		{"not-listed", `{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":"https://images.example/other.jpg"}`, "screen", 400},
		{"other-title", `{"contentId":"tmdb:movie:56","contentType":"movie","posterUrl":"https://images.example/available.jpg"}`, "screen", 409},
		{"missing-field", `{"contentId":"tmdb:movie:55","contentType":"movie"}`, "screen", 400},
		{"invalid-type", `{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":false}`, "screen", 400},
		{"old-title-view", `{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":"https://images.example/available.jpg"}`, "other-screen", 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := contentLocalRequest("PUT", "viewer", tc.body)
			r.Header.Set("X-Client-ID", tc.client)
			w := httptest.NewRecorder()
			h.SetArtworkPreference(w, r)
			if w.Code != tc.code {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	stored, _ := service.Get("viewer", title.ID)
	if stored != nil {
		t.Fatal("failed selection changed saved preference", stored)
	}
	if title.Poster.URL != "https://images.example/available.jpg" {
		t.Fatal("selection changed provider metadata")
	}
}

func TestContentArtworkResetDoesNotRequireARecentTitleView(t *testing.T) {
	h, service := contentLocalHandler(t)
	h.ArtworkTitles = titleratings.New(nil)
	original := models.ContentPreference{ContentID: "tmdb:movie:55", ContentType: "movie", PosterURL: "https://images.example/old.jpg", BackdropURL: "https://images.example/background.jpg", AudioLanguage: "fra"}
	if err := service.Set("viewer", original); err != nil {
		t.Fatal(err)
	}
	r := contentLocalRequest("PUT", "viewer", `{"contentId":"tmdb:movie:55","contentType":"movie","posterUrl":null}`)
	w := httptest.NewRecorder()
	h.SetArtworkPreference(w, r)
	if w.Code != 200 {
		t.Fatal("standard reset required a new provider view", w.Code, w.Body.String())
	}
	pref, _ := service.Get("viewer", original.ContentID)
	if pref.PosterURL != "" || pref.BackdropURL != original.BackdropURL || pref.AudioLanguage != "fra" {
		t.Fatal("reset changed other choices", pref)
	}
}
