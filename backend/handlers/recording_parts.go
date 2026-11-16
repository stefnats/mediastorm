package handlers

import (
	"net/http"
	"strconv"

	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/recordings"
)

type recordingPartResolver interface {
	ResolvePartFile(string, int) (string, *models.RecordingPart, error)
}

// bindRecordingPart chooses one persisted owned file. Explicit selectors are
// snapshots, including an active part; no HTTP request can cross part borders.
func (h *RecordingsHandler) bindRecordingPart(w http.ResponseWriter, r *http.Request, recording *models.Recording) (bool, bool) {
	values, explicit := r.URL.Query()["part"]
	index := 0
	if explicit {
		if len(values) != 1 {
			http.Error(w, "invalid recording part", 400)
			return false, false
		}
		value, err := strconv.Atoi(values[0])
		if err != nil || value < 1 || value > models.MaxRecordingParts || strconv.Itoa(value) != values[0] {
			http.Error(w, "invalid recording part", 400)
			return false, false
		}
		index = value
	}
	resource := recordings.BuildStreamPath(*recording)
	if explicit {
		resource = recordings.BuildPartStreamPath(recording.ID, index)
	}
	if !profileaccess.CheckResource(w, r, recording.UserID, resource) {
		return false, false
	}
	if !explicit && recording.Capture == nil {
		return true, recording.Status == models.RecordingStatusRunning
	}
	resolver, ok := h.service.(recordingPartResolver)
	if !ok {
		http.Error(w, "recording parts unavailable", 503)
		return false, false
	}
	if recording.Capture == nil || recording.Capture.Version != 1 || recording.Capture.Restored || len(recording.Capture.Parts) > models.MaxRecordingParts || len(recording.Capture.Gaps) > models.MaxRecordingGaps {
		http.Error(w, "recording part unavailable", 404)
		return false, false
	}
	seen := map[int]bool{}
	for _, part := range recording.Capture.Parts {
		if part.Index < 1 || part.Index > models.MaxRecordingParts || seen[part.Index] {
			http.Error(w, "recording part unavailable", 404)
			return false, false
		}
		seen[part.Index] = true
		if !explicit && part.Bytes > 0 && part.StreamPath != "" && (index == 0 || part.Index < index) {
			index = part.Index
		}
	}
	if index == 0 {
		http.Error(w, "recording part unavailable", 404)
		return false, false
	}
	path, part, err := resolver.ResolvePartFile(recording.ID, index)
	if err != nil || part == nil {
		http.Error(w, "recording part unavailable", 404)
		return false, false
	}

	recording.OutputPath = path
	recording.OutputSizeBytes = part.Bytes
	growing := !explicit && recording.Status == models.RecordingStatusRunning && recording.Capture.Phase == "capturing" && part.EndedAt == nil
	return true, growing
}
