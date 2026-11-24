package handlers

import (
	"encoding/json"
	"io"
	"net/http"
)

// PictureStyleLearning exposes only an opt-in and sample counts. Clients cannot
// submit learned descriptors or replace a manual image through this endpoint.
func (h *ContentPreferencesHandler) PictureStyleLearning(w http.ResponseWriter, r *http.Request) {
	profile, ok := h.requireUser(w, r)
	if !ok {
		return
	}
	if h.PictureStyles == nil {
		http.Error(w, "picture-style learning is unavailable", http.StatusNotImplemented)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet {
		status, err := h.PictureStyles.Status(profile)
		if err != nil {
			http.Error(w, "picture-style learning is unavailable", 500)
			return
		}
		_ = json.NewEncoder(w).Encode(status)
		return
	}
	var body map[string]json.RawMessage
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	if decoder.Decode(&body) != nil || len(body) != 1 || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "choose enabled or reset", 400)
		return
	}
	var enabled *bool
	reset := false
	if raw, exists := body["enabled"]; exists {
		var value bool
		if string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
			http.Error(w, "enabled must be a boolean", 400)
			return
		}
		enabled = &value
	} else if raw, exists := body["reset"]; exists {
		if json.Unmarshal(raw, &reset) != nil || !reset {
			http.Error(w, "reset must be true", 400)
			return
		}
	} else {
		http.Error(w, "invalid picture-style action", 400)
		return
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	status, err := h.PictureStyles.Update(profile, enabled, reset)
	if err != nil {
		http.Error(w, "picture-style learning could not be saved", 500)
		return
	}
	_ = json.NewEncoder(w).Encode(status)
}
