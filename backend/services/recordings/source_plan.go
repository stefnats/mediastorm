package recordings

import (
	"context"
	"novastream/internal/livecapacity"
	"novastream/models"
)

// RecordingSource is private server-derived input selection. It is never
// accepted from a client, persisted as authority, or emitted in public parts.
type RecordingSource struct {
	SourceID  string
	ChannelID string
	StationID string
	Name      string
	URL       string
	Original  bool
}

type RecordingInput struct {
	Source        RecordingSource
	Lease         *livecapacity.Lease
	releaseLegacy func()
}

func (i RecordingInput) release() {
	if i.Lease != nil {
		i.Lease.Release()
	} else if i.releaseLegacy != nil {
		i.releaseLegacy()
	}
}

type RecordingSourcePlanner interface {
	PlanRecordingSources(context.Context, models.Recording) ([]RecordingSource, error)
	PrepareRecordingSource(context.Context, models.Recording, RecordingSource) (RecordingInput, error)
}
