package models

import "time"

// RecordingPlan is an advisory snapshot, never a reservation or admission token.
// Known cached guide matches can contribute projected rule jobs. Live viewers,
// external clients and missing future guide entries cannot be predicted.
type RecordingPlan struct {
	CheckedAt           time.Time                    `json:"checkedAt"`
	Complete            bool                         `json:"complete"`
	Recordings          []RecordingPlanItem          `json:"recordings"`
	Candidate           *RecordingPlanItem           `json:"candidate,omitempty"`
	RuleForecast        *RecordingRuleForecast       `json:"ruleForecast,omitempty"`
	ProjectedRecordings []ProjectedRecordingPlanItem `json:"projectedRecordings,omitempty"`
}

type RecordingRuleForecast struct {
	Status         string    `json:"status"`
	HorizonStart   time.Time `json:"horizonStart"`
	HorizonEnd     time.Time `json:"horizonEnd"`
	ProjectedCount int       `json:"projectedCount"`
}

// ProjectedRecordingPlanItem is an owned rule match, never a persisted job ID.
type ProjectedRecordingPlanItem struct {
	RecordingPlanItem
	RuleID      string `json:"ruleId"`
	ChannelName string `json:"channelName"`
	Title       string `json:"title"`
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
