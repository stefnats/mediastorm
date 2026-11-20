package recordings

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"novastream/models"
)

const derivedTimelineMaximumMS int64 = 12 * 60 * 60 * 1000

var (
	errDerivedTimelineInvalid  = errors.New("derived timeline data invalid")
	errDerivedTimelineExpired  = errors.New("derived timeline observation unavailable")
	errDerivedTimelinePosition = errors.New("derived timeline position out of range")
	errDerivedTimelineAnchor   = errors.New("derived timeline anchor changed")
)

type derivedTimelineBinding struct {
	recordingID, artifactID string
	incarnation             uuid.UUID
	revision                [32]byte
}

type derivedTimelineSpan struct {
	partIndex           int
	startMS, durationMS int64
}

// Data for one joined container, not an original-part seek or programme clock.
type derivedTimeline struct {
	binding             derivedTimelineBinding
	spans               []derivedTimelineSpan
	durationMS          int64
	observedAt, expires time.Time
}

type derivedTimelineAnchor struct {
	binding                   derivedTimelineBinding
	partIndex                 int
	offsetWithinDerivedSpanMS int64
	atDerivedEnd              bool
}

// Normalize without UnixNano, whose representable range is much narrower than
// RFC3339. Supported UTC years and exact nanoseconds have one canonical spelling.
func derivedTimelineTime(value time.Time) (time.Time, string, bool) {
	value = value.UTC().Round(0)
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 {
		return time.Time{}, "", false
	}
	return value, value.Format(time.RFC3339Nano), true
}

// Incarnation and descriptor are DATA. This constructor does not obtain a PG
// incarnation or validate files, ownership, permissions, cache or media quality.
// Future server adapters must perform those original-source checks separately.
func newDerivedTimeline(incarnation uuid.UUID, descriptor DerivedRecording, at time.Time) (*derivedTimeline, error) {
	if incarnation == uuid.Nil || descriptor.State != "ready" || descriptor.Reason != "" || !safeRecordingComponent(descriptor.RecordingID) || !validDerivedID(descriptor.ID) || !terminalExportStatus(descriptor.SourceStatus) || descriptor.DurationMS <= 0 || descriptor.DurationMS > derivedTimelineMaximumMS || descriptor.Bytes <= 0 || descriptor.Bytes > derivedCacheLimit || len(descriptor.PartOffsets) < 2 || len(descriptor.PartOffsets) > models.MaxRecordingParts || descriptor.GapsOmitted && !descriptor.SourceIncomplete || descriptor.ObservedAt == nil || descriptor.ExpiresAt == nil {
		return nil, errDerivedTimelineInvalid
	}
	observed, observedText, observedOK := derivedTimelineTime(*descriptor.ObservedAt)
	expires, expiresText, expiresOK := derivedTimelineTime(*descriptor.ExpiresAt)
	_, _, atOK := derivedTimelineTime(at)
	if !observedOK || !expiresOK || !atOK || !expires.After(observed) || expires.After(observed.Add(derivedTTL)) {
		return nil, errDerivedTimelineInvalid
	}
	value := &derivedTimeline{durationMS: descriptor.DurationMS, observedAt: observed, expires: expires}
	if !value.current(at) {
		return nil, errDerivedTimelineExpired
	}
	value.spans = make([]derivedTimelineSpan, len(descriptor.PartOffsets))
	// Fixed arrays encode an ordered tuple with typed integers, never a map or
	// delimiter-joined strings. The digest is correlation only, not a media hash.
	triples := make([][3]int64, len(descriptor.PartOffsets))
	var end int64
	for index, part := range descriptor.PartOffsets {
		if part.PartIndex != index+1 || part.StartMS != end || part.DurationMS <= 0 || part.DurationMS > derivedTimelineMaximumMS-end {
			return nil, errDerivedTimelineInvalid
		}
		value.spans[index] = derivedTimelineSpan{part.PartIndex, part.StartMS, part.DurationMS}
		triples[index] = [3]int64{int64(part.PartIndex), part.StartMS, part.DurationMS}
		end += part.DurationMS
	}
	if end != value.durationMS {
		return nil, errDerivedTimelineInvalid
	}
	encoded, err := json.Marshal([12]any{
		"recording-derived-timeline/v1", descriptor.RecordingID, incarnation.String(), descriptor.ID,
		descriptor.DurationMS, descriptor.Bytes, string(descriptor.SourceStatus),
		descriptor.SourceIncomplete, descriptor.GapsOmitted, observedText, expiresText, triples,
	})
	if err != nil {
		return nil, errDerivedTimelineInvalid
	}
	value.binding = derivedTimelineBinding{descriptor.RecordingID, descriptor.ID, incarnation, sha256.Sum256(encoded)}
	return value, nil
}

func (t *derivedTimeline) current(at time.Time) bool {
	if t == nil {
		return false
	}
	now, _, ok := derivedTimelineTime(at)
	return ok && !now.Before(t.observedAt) && now.Before(t.expires)
}

// Internal boundaries belong to the NEXT span at zero; the final endpoint is
// represented explicitly and says nothing about programme/watch completion.
func (t *derivedTimeline) locate(positionMS int64, at time.Time) (derivedTimelineAnchor, error) {
	var zero derivedTimelineAnchor
	if t == nil || len(t.spans) < 2 {
		return zero, errDerivedTimelineInvalid
	}
	if !t.current(at) {
		return zero, errDerivedTimelineExpired
	}
	if positionMS < 0 || positionMS > t.durationMS {
		return zero, errDerivedTimelinePosition
	}
	if positionMS == t.durationMS {
		last := t.spans[len(t.spans)-1]
		return derivedTimelineAnchor{t.binding, last.partIndex, last.durationMS, true}, nil
	}
	for _, part := range t.spans {
		if positionMS < part.startMS+part.durationMS {
			return derivedTimelineAnchor{t.binding, part.partIndex, positionMS - part.startMS, false}, nil
		}
	}
	return zero, errDerivedTimelinePosition
}

func (t *derivedTimeline) position(anchor derivedTimelineAnchor, at time.Time) (int64, error) {
	if t == nil || len(t.spans) < 2 {
		return 0, errDerivedTimelineInvalid
	}
	if !t.current(at) {
		return 0, errDerivedTimelineExpired
	}
	if anchor.binding != t.binding || anchor.partIndex <= 0 || anchor.partIndex > len(t.spans) {
		return 0, errDerivedTimelineAnchor
	}
	part := t.spans[anchor.partIndex-1]
	last := anchor.partIndex == len(t.spans)
	if anchor.offsetWithinDerivedSpanMS < 0 || anchor.offsetWithinDerivedSpanMS > part.durationMS || !last && anchor.offsetWithinDerivedSpanMS == part.durationMS {
		return 0, errDerivedTimelineAnchor
	}
	end := last && anchor.offsetWithinDerivedSpanMS == part.durationMS
	if anchor.atDerivedEnd != end {
		return 0, errDerivedTimelineAnchor
	}
	return part.startMS + anchor.offsetWithinDerivedSpanMS, nil
}
