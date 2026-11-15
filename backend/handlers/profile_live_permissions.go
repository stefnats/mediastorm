package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"novastream/internal/auth"
	"novastream/services/users"
)

// SetProfileLivePermissions is exposed only through account-session routes.
// Ownership is required even if callers submit a different profileId in JSON.
func (h *AdminUIHandler) SetProfileLivePermissions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if auth.GetAccountID(r) == "" && !auth.IsMaster(r) {
		http.Error(w, "Account management authentication required", http.StatusForbidden)
		return
	}
	if h.usersService == nil {
		http.Error(w, "Profile service unavailable", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("profileId"))
	if id == "" {
		http.Error(w, "profileId parameter required", http.StatusBadRequest)
		return
	}
	if ok, _ := h.requireProfileScope(w, r, id); !ok {
		return
	}
	if !h.requireParentAuthorization(w, r) {
		return
	}
	var payload struct {
		Live       *bool `json:"allowLiveTakeover"`
		Recordings *bool `json:"allowRecordingInterruption"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		http.Error(w, "Invalid live permission request", http.StatusBadRequest)
		return
	}
	if payload.Live == nil || payload.Recordings == nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "Both live permissions are required", http.StatusBadRequest)
		return
	}
	profile, err := h.usersService.SetLivePermissions(id, *payload.Live, *payload.Recordings)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, users.ErrUserNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, users.ErrLivePermissions) {
			status = http.StatusBadRequest
		}
		http.Error(w, "Live permissions could not be updated", status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"allowLiveTakeover": profile.AllowLiveTakeover, "allowRecordingInterruption": profile.AllowRecordingInterruption})
}
