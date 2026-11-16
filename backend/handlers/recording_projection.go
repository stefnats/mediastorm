package handlers

import (
	"novastream/models"
	"path/filepath"
	"strings"
)

// Only public handler responses use this projection. Repository/backup models
// keep private source and filesystem locations for actual recorder operation.
func publicRecording(recording models.Recording) models.Recording {
	recording.SourceURL = ""
	if strings.TrimSpace(recording.OutputPath) != "" {
		recording.OutputPath = filepath.Base(strings.ReplaceAll(recording.OutputPath, `\`, "/"))
	}
	if recording.Error == "recording rule paused" || strings.HasPrefix(recording.Error, "cancelled because the recording rule") {
		recording.Error = ""
	}
	if recording.Error != "" {
		if recording.Status == models.RecordingStatusPending {
			recording.Error = "Recording cannot start yet; waiting for an available IPTV connection"
		} else {
			if recording.Status == models.RecordingStatusCancelled {
				recording.Error = "Recording was stopped; preserved media may be incomplete"
			} else {
				recording.Error = "Recording is incomplete or unavailable; preserved parts may be played individually"
			}
		}
	}
	recording.Capture = models.CopyRecordingCapture(recording.Capture)
	if recording.Capture != nil {
		if len(recording.Capture.Parts) == 0 {
			recording.OutputPath = ""
			recording.OutputSizeBytes = 0
		}
		if recording.Capture.Restored {
			for i := range recording.Capture.Parts {
				recording.Capture.Parts[i].StreamPath = ""
			}
			recording.OutputPath = ""
			recording.OutputSizeBytes = 0
		}
		recording.Capture.StationID = ""
		recording.Capture.RunID = ""
		recording.Capture.Restored = false
	}
	return recording
}

func publicRecordings(items []models.Recording) []models.Recording {
	result := make([]models.Recording, len(items))
	for i := range items {
		result[i] = publicRecording(items[i])
	}
	return result
}
