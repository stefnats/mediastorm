package handlers

import (
	"net/http"
	"novastream/internal/livecapacity"
	"strings"

	"novastream/internal/profileaccess"
)

func (h *VideoHandler) requireHLSSessionAccess(w http.ResponseWriter, r *http.Request, sessionID string) bool {
	// Read identity without touching LastAccess: a denied request must neither
	// keep somebody else's stream alive nor disclose its playback status.
	h.hlsManager.mu.RLock()
	session, exists := h.hlsManager.sessions[sessionID]
	h.hlsManager.mu.RUnlock()
	if !exists {
		http.NotFound(w, r)
		return false
	}
	session.mu.RLock()
	profileID, resource := session.ProfileID, session.RequestedSource
	if resource == "" {
		resource = session.OriginalPath
	}
	if resource == "" {
		resource = session.Path
	}
	session.mu.RUnlock()
	if !profileaccess.CheckResource(w, r, profileID, resource) {
		return false
	}
	session.mu.RLock()
	revoked := session.LiveRevoked
	session.mu.RUnlock()
	if revoked && !(r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/stop")) {
		writeLiveAdmissionError(w, liveStreamTarget{}, livecapacity.ErrTakenOver)
		return false
	}
	return true
}
