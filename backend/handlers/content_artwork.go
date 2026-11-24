package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"novastream/models"
)

// SetArtworkPreference updates only explicitly submitted image choices. Null
// restores the standard picture; omitted fields and track preferences survive.
func (h *ContentPreferencesHandler) SetArtworkPreference(w http.ResponseWriter, r *http.Request) {
	user, ok := h.requireUser(w, r)
	if !ok {
		return
	}
	if h.ArtworkTitles == nil {
		http.Error(w, "personal artwork is unavailable", http.StatusNotImplemented)
		return
	}
	var body map[string]json.RawMessage
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10))
	if decoder.Decode(&body) != nil || body == nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid artwork choice", http.StatusBadRequest)
		return
	}
	var request struct{ ContentID, ContentType, PosterURL, BackdropURL string }
	for key, value := range body {
		var target *string
		switch key {
		case "contentId":
			target = &request.ContentID
		case "contentType":
			target = &request.ContentType
		case "posterUrl":
			target = &request.PosterURL
		case "backdropUrl":
			target = &request.BackdropURL
		default:
			http.Error(w, "invalid artwork field", http.StatusBadRequest)
			return
		}
		if json.Unmarshal(value, target) != nil {
			http.Error(w, "invalid artwork field value", http.StatusBadRequest)
			return
		}
	}
	_, poster := body["posterUrl"]
	_, backdrop := body["backdropUrl"]
	if !poster && !backdrop || request.ContentID == "" || request.ContentType != "movie" && request.ContentType != "series" {
		http.Error(w, "title and artwork choice are required", http.StatusBadRequest)
		return
	}
	var title models.Title
	if request.PosterURL != "" || request.BackdropURL != "" {
		var exists bool
		title, exists = h.ArtworkTitles.ResolveArtwork(user, requestClientID(r), request.ContentType, request.ContentID)
		if !exists {
			http.Error(w, "reload this title before choosing a picture", http.StatusConflict)
			return
		}
	}
	permitted := func(url string, primary, text *models.Image, variants []models.Image) bool {
		if url == "" {
			return true
		}
		if primary != nil && primary.URL == url || text != nil && text.URL == url {
			return true
		}
		for _, image := range variants {
			if image.URL == url {
				return true
			}
		}
		return false
	}
	if poster && !permitted(request.PosterURL, title.Poster, title.TextPoster, title.Posters) || backdrop && !permitted(request.BackdropURL, title.Backdrop, title.TextBackdrop, title.Backdrops) {
		http.Error(w, "choose one of this title's available pictures", http.StatusBadRequest)
		return
	}
	id := strings.ToLower(strings.TrimSpace(request.ContentID))
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	previous, err := h.Service.Get(user, id)
	if err != nil {
		http.Error(w, "artwork preference unavailable", http.StatusInternalServerError)
		return
	}
	preference := models.ContentPreference{ContentID: id, ContentType: request.ContentType}
	if previous != nil {
		preference = *previous
	}
	if poster {
		preference.PosterURL = request.PosterURL
		preference.PosterUseStandard = request.PosterURL == ""
	}
	if backdrop {
		preference.BackdropURL = request.BackdropURL
		preference.BackdropUseStandard = request.BackdropURL == ""
	}
	if err := h.Service.Set(user, preference); err != nil {
		http.Error(w, "artwork preference could not be saved", http.StatusInternalServerError)
		return
	}
	stored, err := h.Service.Get(user, id)
	if err != nil || stored == nil {
		http.Error(w, "saved artwork preference could not be read", http.StatusInternalServerError)
		return
	}
	if h.PictureStyles != nil {
		// Training is optional: a descriptor persistence error cannot undo an already
		// confirmed manual choice or turn it into an automatic image override.
		selected := func(url string, primary, text *models.Image, variants []models.Image) *models.Image {
			if url == "" {
				return nil
			}
			if primary != nil && primary.URL == url {
				return primary
			}
			if text != nil && text.URL == url {
				return text
			}
			for _, image := range variants {
				if image.URL == url {
					copy := image
					return &copy
				}
			}
			return nil
		}
		identity := title
		if identity.ID == "" {
			identity = models.Title{ID: id, MediaType: request.ContentType}
		}
		if poster {
			_ = h.PictureStyles.ObserveChoice(user, identity, "poster", selected(request.PosterURL, title.Poster, title.TextPoster, title.Posters))
		}
		if backdrop {
			_ = h.PictureStyles.ObserveChoice(user, identity, "backdrop", selected(request.BackdropURL, title.Backdrop, title.TextBackdrop, title.Backdrops))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(contentPreferenceResponse{ContentPreference: stored, LocalSourcePreferenceSupported: true, ArtworkPreferenceSupported: true})
}
