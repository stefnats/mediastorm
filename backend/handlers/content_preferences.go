package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"novastream/services/picturestyle"
	"reflect"
	"strings"
	"sync"

	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	content_preferences "novastream/services/content_preferences"

	"github.com/gorilla/mux"
)

type contentPreferencesService interface {
	Get(userID, contentID string) (*models.ContentPreference, error)
	Set(userID string, pref models.ContentPreference) error
	Delete(userID, contentID string) error
	List(userID string) ([]models.ContentPreference, error)
}

var _ contentPreferencesService = (*content_preferences.Service)(nil)

type ContentPreferencesHandler struct {
	PictureStyles *picturestyle.Service
	ArtworkTitles interface {
		ResolveArtwork(string, string, string, string) (models.Title, bool)
	}
	Service       contentPreferencesService
	Users         userService
	PrequeueStore interface{ InvalidateSourceSelectionByUser(string) }
	writeMu       sync.Mutex
}

func (h *ContentPreferencesHandler) SetPrequeueStore(store interface{ InvalidateSourceSelectionByUser(string) }) {
	h.PrequeueStore = store
}

func NewContentPreferencesHandler(service contentPreferencesService, users userService) *ContentPreferencesHandler {
	return &ContentPreferencesHandler{
		Service: service,
		Users:   users,
	}
}

type contentPreferenceResponse struct {
	*models.ContentPreference
	LocalSourcePreferenceSupported bool `json:"localSourcePreferenceSupported"`
	ArtworkPreferenceSupported     bool `json:"artworkPreferenceSupported,omitempty"`
}

// GetPreference returns the content preference for a specific content item.
func (h *ContentPreferencesHandler) GetPreference(w http.ResponseWriter, r *http.Request) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	contentID := strings.TrimSpace(vars["contentID"])
	if contentID == "" {
		http.Error(w, "content id is required", http.StatusBadRequest)
		return
	}

	pref, err := h.Service.Get(userID, contentID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(contentPreferenceResponse{ContentPreference: pref, LocalSourcePreferenceSupported: true, ArtworkPreferenceSupported: h.ArtworkTitles != nil})
}

// SetPreference creates or updates a content preference.
func (h *ContentPreferencesHandler) SetPreference(w http.ResponseWriter, r *http.Request) {
	h.setPreference(w, r, false)
}

// SetLocalSourcePreference is deliberately a separate mutation route. Older
// servers must reject it instead of treating a source-only patch as a full
// audio/subtitle preference replacement.
func (h *ContentPreferencesHandler) SetLocalSourcePreference(w http.ResponseWriter, r *http.Request) {
	h.setPreference(w, r, true)
}

func (h *ContentPreferencesHandler) setPreference(w http.ResponseWriter, r *http.Request, sourceOnly bool) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}

	var body json.RawMessage
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	if err := dec.Decode(&body); err != nil {
		http.Error(w, "invalid content preference", http.StatusBadRequest)
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid content preference", http.StatusBadRequest)
		return
	}
	var pref models.ContentPreference
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &pref) != nil || json.Unmarshal(body, &fields) != nil || fields == nil {
		http.Error(w, "invalid content preference", http.StatusBadRequest)
		return
	}

	if sourceOnly {
		if _, present := fields["preferLocalFiles"]; !present {
			http.Error(w, "preferLocalFiles is required", http.StatusBadRequest)
			return
		}
		for key := range fields {
			if key != "contentId" && key != "contentType" && key != "preferLocalFiles" {
				http.Error(w, "invalid local source preference field", http.StatusBadRequest)
				return
			}
		}
	}
	pref.ContentID = strings.ToLower(strings.TrimSpace(pref.ContentID))
	if pref.ContentID == "" {
		http.Error(w, "content id is required", http.StatusBadRequest)
		return
	}

	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	existing, err := h.Service.Get(userID, pref.ContentID)
	if err != nil {
		http.Error(w, "content preference unavailable", http.StatusInternalServerError)
		return
	}
	// Apply only submitted fields. Source-only changes must not erase language
	// choices, and old clients saving audio must preserve the source override.
	merged := models.ContentPreference{ContentID: pref.ContentID}
	if existing != nil {
		merged = *existing
		merged.ContentID = pref.ContentID
	}
	if _, ok := fields["contentType"]; ok {
		merged.ContentType = pref.ContentType
	}
	if _, ok := fields["audioLanguage"]; ok {
		merged.AudioLanguage = pref.AudioLanguage
	}
	if _, ok := fields["subtitleLanguage"]; ok {
		merged.SubtitleLanguage = pref.SubtitleLanguage
	}
	if _, ok := fields["subtitleMode"]; ok {
		merged.SubtitleMode = pref.SubtitleMode
	}
	if _, ok := fields["preferLocalFiles"]; ok {
		merged.PreferLocalFiles = pref.PreferLocalFiles
	}
	if err := h.Service.Set(userID, merged); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, content_preferences.ErrInvalidPreference) {
			status = http.StatusBadRequest
		}
		http.Error(w, "content preference could not be saved", status)
		return
	}
	var previousLocal *bool
	if existing != nil {
		previousLocal = existing.PreferLocalFiles
	}
	if h.PrequeueStore != nil && !reflect.DeepEqual(previousLocal, merged.PreferLocalFiles) {
		h.PrequeueStore.InvalidateSourceSelectionByUser(userID)
	}
	pref = merged
	if saved, err := h.Service.Get(userID, pref.ContentID); err == nil && saved != nil {
		pref = *saved
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(contentPreferenceResponse{ContentPreference: &pref, LocalSourcePreferenceSupported: true, ArtworkPreferenceSupported: h.ArtworkTitles != nil})
}

// DeletePreference removes a content preference.
func (h *ContentPreferencesHandler) DeletePreference(w http.ResponseWriter, r *http.Request) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	contentID := strings.TrimSpace(vars["contentID"])
	if contentID == "" {
		http.Error(w, "content id is required", http.StatusBadRequest)
		return
	}

	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	previous, err := h.Service.Get(userID, contentID)
	if err != nil {
		http.Error(w, "content preference unavailable", http.StatusInternalServerError)
		return
	}
	if err := h.Service.Delete(userID, contentID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.PrequeueStore != nil && previous != nil && previous.PreferLocalFiles != nil {
		h.PrequeueStore.InvalidateSourceSelectionByUser(userID)
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListPreferences returns all content preferences for a user.
func (h *ContentPreferencesHandler) ListPreferences(w http.ResponseWriter, r *http.Request) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}

	prefs, err := h.Service.List(userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(prefs)
}

func (h *ContentPreferencesHandler) Options(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *ContentPreferencesHandler) requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	vars := mux.Vars(r)
	userID := strings.TrimSpace(vars["userID"])

	if userID == "" {
		http.Error(w, "user id is required", http.StatusBadRequest)
		return "", false
	}

	if h.Users != nil && !h.Users.Exists(userID) {
		http.Error(w, "user not found", http.StatusNotFound)
		return "", false
	}
	if auth.GetSessionScope(r) != "" {
		http.Error(w, "full account session required", http.StatusForbidden)
		return "", false
	}
	if !auth.IsMaster(r) {
		owner, ok := h.Users.(interface{ BelongsToAccount(string, string) bool })
		if !ok || auth.GetAccountID(r) == "" || !owner.BelongsToAccount(userID, auth.GetAccountID(r)) {
			http.Error(w, "user not found", http.StatusNotFound)
			return "", false
		}
	}

	if !profileaccess.CheckResource(w, r, userID, "") {
		return "", false
	}
	return userID, true
}
