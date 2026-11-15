package models

import "time"

// RecordingPlan is an advisory snapshot, never a reservation or admission token.
// Only already materialized jobs are covered. Live viewers, external clients and
// future rule expansion cannot be predicted by this report.
type RecordingPlan struct {
	CheckedAt  time.Time           `json:"checkedAt"`
	Complete   bool                `json:"complete"`
	Recordings []RecordingPlanItem `json:"recordings"`
	Candidate  *RecordingPlanItem  `json:"candidate,omitempty"`
}

type RecordingPlanItem struct {
	RecordingID              string     `json:"recordingId,omitempty"`
	State                    string     `json:"state"`
	Start                    time.Time  `json:"start"`
	End                      time.Time  `json:"end"`
	MaxStreams               int        `json:"maxStreams"`
	PeakScheduledConnections int        `json:"peakScheduledConnections"`
	FirstConflictStart       *time.Time `json:"firstConflictStart,omitempty"`
	FirstConflictEnd         *time.Time `json:"firstConflictEnd,omitempty"`
}
