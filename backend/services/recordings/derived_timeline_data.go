package recordings

import (
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"novastream/internal/datastore"
	"novastream/models"
)

var ErrDerivedTimelineData = errors.New("derived timeline data unavailable")

type DerivedTimelineSpanData struct {
	PartIndex  int   `json:"partIndex"`
	StartMS    int64 `json:"startMs"`
	DurationMS int64 `json:"durationMs"`
}

// DerivedTimelineData is a separate data schema. No field authorizes media or a
// progress write, and its revision is not a media-content version.
type DerivedTimelineData struct {
	SchemaVersion    int                       `json:"schemaVersion"`
	RecordingID      string                    `json:"recordingId"`
	State            string                    `json:"state"`
	CoordinateSystem string                    `json:"coordinateSystem"`
	Reason           string                    `json:"reason,omitempty"`
	Incarnation      string                    `json:"incarnation,omitempty"`
	ArtifactID       string                    `json:"artifactId,omitempty"`
	MappingRevision  string                    `json:"mappingRevision,omitempty"`
	DurationMS       int64                     `json:"durationMs,omitempty"`
	Spans            []DerivedTimelineSpanData `json:"spans"`
	SourceStatus     models.RecordingStatus    `json:"sourceStatus,omitempty"`
	SourceIncomplete *bool                     `json:"sourceIncomplete,omitempty"`
	GapsOmitted      *bool                     `json:"gapsOmitted,omitempty"`
	ObservedAt       *time.Time                `json:"observedAt,omitempty"`
	ExpiresAt        *time.Time                `json:"expiresAt,omitempty"`
}

// DerivedTimelineReady proves only the concrete service's repository identity.
func (s *Service) DerivedTimelineReady(expected *datastore.DataStore) bool {
	return s != nil && expected != nil && expected.RecordingRepositoryReady(s.repo)
}

// ProjectDerivedTimelineData is pure. Its caller must obtain actual current
// recording authorization, PG incarnation and GetDerived observation separately.
func ProjectDerivedTimelineData(incarnation uuid.UUID, descriptor DerivedRecording, at time.Time) (DerivedTimelineData, error) {
	var zero DerivedTimelineData
	if !safeRecordingComponent(descriptor.RecordingID) {
		return zero, ErrDerivedTimelineData
	}
	value := DerivedTimelineData{SchemaVersion: 1, RecordingID: descriptor.RecordingID, State: "unknown", CoordinateSystem: "derived-media", Spans: []DerivedTimelineSpanData{}}
	if descriptor.State == "unknown" {
		switch descriptor.Reason {
		case "not_prepared", "configuration_changed", "source_changed", "expired":
			return valueWithTimelineReason(value, descriptor.Reason), nil
		default:
			return zero, ErrDerivedTimelineData
		}
	}
	timeline, err := newDerivedTimeline(incarnation, descriptor, at)
	if err != nil {
		return zero, ErrDerivedTimelineData
	}
	value.State = "known"
	value.Incarnation = timeline.binding.incarnation.String()
	value.ArtifactID = timeline.binding.artifactID
	value.MappingRevision = hex.EncodeToString(timeline.binding.revision[:])
	value.DurationMS = timeline.durationMS
	for _, span := range timeline.spans {
		value.Spans = append(value.Spans, DerivedTimelineSpanData{span.partIndex, span.startMS, span.durationMS})
	}
	incomplete, gaps := descriptor.SourceIncomplete, descriptor.GapsOmitted
	observed, expires := timeline.observedAt, timeline.expires
	value.SourceStatus = descriptor.SourceStatus
	value.SourceIncomplete = &incomplete
	value.GapsOmitted = &gaps
	value.ObservedAt = &observed
	value.ExpiresAt = &expires
	return value, nil
}
func valueWithTimelineReason(value DerivedTimelineData, reason string) DerivedTimelineData {
	value.Reason = reason
	return value
}
