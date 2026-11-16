package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	user_settings "novastream/services/user_settings"
)

type stationFavoriteService interface {
	SetFavoriteStation(string, string, bool) ([]string, error)
}

// SetStationFavorite never resolves a playlist or opens a stream. An absent
// station may remain bookmarked and reappear in a later allowed catalogue.
func (h *UserSettingsHandler) SetStationFavorite(w http.ResponseWriter, r *http.Request) {
	id, ok := h.requireUser(w, r)
	if !ok {
		return
	}
	if auth.GetSessionScope(r) != "" {
		writeJSONError(w, "full account session required", http.StatusForbidden)
		return
	}
	profiles, ok := h.Users.(interface {
		BelongsToAccount(string, string) bool
		Get(string) (models.User, bool)
	})
	if !ok {
		writeJSONError(w, "profile access unavailable", http.StatusServiceUnavailable)
		return
	}
	if !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || !profiles.BelongsToAccount(id, auth.GetAccountID(r))) {
		writeJSONError(w, "user not found", http.StatusNotFound)
		return
	}
	profile, exists := profiles.Get(id)
	if !exists {
		writeJSONError(w, "user not found", http.StatusNotFound)
		return
	}
	if profile.IsKidsProfile {
		writeJSONError(w, "Live TV is not available on kids profiles", http.StatusForbidden)
		return
	}
	if !profileaccess.CheckResource(w, r, id, "") {
		return
	}
	service, ok := h.Service.(stationFavoriteService)
	if !ok {
		writeJSONError(w, "station favorites unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var body struct {
		StationID string `json:"stationId"`
		Favorite  *bool  `json:"favorite"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&body)
	if err == nil {
		err = ensureJSONEOF(decoder)
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(w, "station favorite request too large", http.StatusRequestEntityTooLarge)
		} else {
			writeJSONError(w, "invalid station favorite request", http.StatusBadRequest)
		}
		return
	}
	if body.Favorite == nil || !models.ValidFavoriteStationID(body.StationID) {
		writeJSONError(w, "stationId and favorite are required", http.StatusBadRequest)
		return
	}
	ids, err := service.SetFavoriteStation(id, body.StationID, *body.Favorite)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, user_settings.ErrFavoriteStationLimit) {
			status = http.StatusConflict
		}
		writeJSONError(w, "unable to save station favorites", status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(struct {
		FavoriteStationIDs []string `json:"favoriteStationIds"`
	}{ids})
}
