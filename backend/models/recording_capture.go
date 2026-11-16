package models

import "time"

const MaxRecordingParts = 256
const MaxRecordingGaps = 256

// RecordingCapture describes preserved input sections, not a continuous or
// content-verified programme. A closed part has EndedAt and EndReason; an
// active part's Bytes are a provisional last-observed file-size snapshot.
type RecordingCapture struct {
	Version       int             `json:"version"`
	Phase         string          `json:"phase"`
	Incomplete    bool            `json:"incomplete"`
	SourceChanges int             `json:"sourceChanges"`
	Parts         []RecordingPart `json:"parts"`
	Gaps          []RecordingGap  `json:"gaps"`
	// StationID is private scheduling context. Restore clears it and every
	// actual attempt must independently revalidate current catalogue rights.
	StationID string `json:"stationId,omitempty"`
	Restored  bool   `json:"restored,omitempty"`
	RunID     string `json:"runId,omitempty"`
}

type RecordingPart struct {
	Index      int        `json:"index"`
	SourceID   string     `json:"sourceId,omitempty"`
	ChannelID  string     `json:"channelId,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	EndedAt    *time.Time `json:"endedAt,omitempty"`
	Bytes      int64      `json:"bytes"`
	EndReason  string     `json:"endReason,omitempty"`
	StreamPath string     `json:"streamPath,omitempty"`
}

type RecordingGap struct {
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	Reason    string     `json:"reason"`
}

func CopyRecordingCapture(value *RecordingCapture) *RecordingCapture {
	if value == nil {
		return nil
	}
	copy := *value
	copy.Parts = append([]RecordingPart{}, value.Parts...)
	for i := range copy.Parts {
		if copy.Parts[i].EndedAt != nil {
			at := *copy.Parts[i].EndedAt
			copy.Parts[i].EndedAt = &at
		}
	}
	copy.Gaps = append([]RecordingGap{}, value.Gaps...)
	for i := range copy.Gaps {
		if copy.Gaps[i].EndedAt != nil {
			at := *copy.Gaps[i].EndedAt
			copy.Gaps[i].EndedAt = &at
		}
	}
	return &copy
}

// Restore preserves historical capture metadata, not active inputs or source
// authority. Part reads derive their exact owned path and verify local files.
func RestoreRecordingCapture(recording *Recording) {
	if recording == nil {
		return
	}
	recording.SourceURL = ""
	if recording.Capture == nil {
		return
	}
	recording.Capture = CopyRecordingCapture(recording.Capture)
	capture := recording.Capture
	capture.StationID = ""
	capture.Restored = true
	capture.RunID = ""
	if capture.Version != 1 || len(capture.Parts) > MaxRecordingParts || len(capture.Gaps) > MaxRecordingGaps {
		recording.Capture = nil
		recording.OutputPath = ""
		recording.SourceURL = ""
		recording.Status = RecordingStatusFailed
		recording.Error = "restored recording capture metadata is unavailable"
		return
	}
	if recording.Status == RecordingStatusPending || recording.Status == RecordingStatusStarting || recording.Status == RecordingStatusRunning {
		recording.Status = RecordingStatusFailed
		recording.Error = "restored recording is incomplete; automatic capture was not resumed"
		capture.Phase = "interrupted"
		capture.Incomplete = true
	}
	for i := range capture.Parts {
		capture.Parts[i].StreamPath = ""
	}
}
