package recordings

import (
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"novastream/models"
)

var timelineTestIncarnation = uuid.MustParse("11111111-2222-4333-8444-555555555555")

func timelineDescriptor() (DerivedRecording, time.Time) {
	observed := time.Date(2026, 11, 20, 1, 0, 0, 1, time.UTC)
	expires := observed.Add(derivedTTL)
	return DerivedRecording{RecordingID: "recording-A", ID: strings.Repeat("a", 32), State: "ready", DurationMS: 4000, Bytes: 4096, SourceIncomplete: true, GapsOmitted: true, SourceStatus: models.RecordingStatusCompleted, PartOffsets: []DerivedPartOffset{{1, 0, 1001}, {2, 1001, 2002}, {3, 3003, 997}}, ObservedAt: &observed, ExpiresAt: &expires}, observed.Add(time.Hour)
}

func timelineForTest(t *testing.T, descriptor DerivedRecording, at time.Time) *derivedTimeline {
	t.Helper()
	value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at)
	if err != nil {
		t.Fatal("construct pure timeline", err)
	}
	return value
}

func TestDerivedTimelineCanonicalHashTuple(t *testing.T) {
	descriptor, at := timelineDescriptor()
	value := timelineForTest(t, descriptor, at)
	// An explicit protocol vector, independent of struct/map serialization or
	// timestamp epoch arithmetic. This checksum grants no playback permission.
	canonical := `["recording-derived-timeline/v1","recording-A","11111111-2222-4333-8444-555555555555","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",4000,4096,"completed",true,true,"2026-11-20T01:00:00.000000001Z","2026-11-21T01:00:00.000000001Z",[[1,0,1001],[2,1001,2002],[3,3003,997]]]`
	if value.binding.revision != sha256.Sum256([]byte(canonical)) {
		t.Fatal("canonical typed tuple or timestamp spelling changed")
	}
}

func TestDerivedTimelineMeasuredBoundariesAndFinalEnd(t *testing.T) {
	descriptor, at := timelineDescriptor()
	value := timelineForTest(t, descriptor, at)
	for _, tc := range []struct {
		position, offset int64
		part             int
		end              bool
	}{
		{0, 0, 1, false}, {1000, 1000, 1, false},
		{1001, 0, 2, false}, {1002, 1, 2, false},
		{3002, 2001, 2, false}, {3003, 0, 3, false},
		{3999, 996, 3, false}, {4000, 997, 3, true},
	} {
		anchor, err := value.locate(tc.position, at)
		if err != nil || anchor.partIndex != tc.part || anchor.offsetWithinDerivedSpanMS != tc.offset || anchor.atDerivedEnd != tc.end {
			t.Fatal("measured boundary mapped to wrong derived span", tc.position, err)
		}
		position, err := value.position(anchor, at)
		if err != nil || position != tc.position {
			t.Fatal("canonical anchor did not return original integer coordinate", err)
		}
	}
}

func TestDerivedTimelineExhaustiveSmallRoundTrips(t *testing.T) {
	for _, lengths := range [][]int64{{1, 1}, {1, 2, 1}, {7, 13, 2}, {1001, 2002, 997}} {
		descriptor, at := timelineDescriptor()
		descriptor.PartOffsets = nil
		var end int64
		for i, length := range lengths {
			descriptor.PartOffsets = append(descriptor.PartOffsets, DerivedPartOffset{i + 1, end, length})
			end += length
		}
		descriptor.DurationMS = end
		value := timelineForTest(t, descriptor, at)
		for input := int64(0); input <= end; input++ {
			anchor, err := value.locate(input, at)
			if err != nil {
				t.Fatal(err)
			}
			output, err := value.position(anchor, at)
			if err != nil || input != output || anchor.atDerivedEnd != (input == end) {
				t.Fatal("bounded exhaustive roundtrip or end classification failed")
			}
			if anchor.partIndex < len(lengths) && anchor.offsetWithinDerivedSpanMS >= lengths[anchor.partIndex-1] {
				t.Fatal("internal end has two canonical representations")
			}
		}
	}
}

func TestDerivedTimelineMaximum256SpansAndTwelveHours(t *testing.T) {
	descriptor, at := timelineDescriptor()
	descriptor.PartOffsets = make([]DerivedPartOffset, 256)
	for i := range descriptor.PartOffsets {
		descriptor.PartOffsets[i] = DerivedPartOffset{i + 1, int64(i), 1}
	}
	descriptor.PartOffsets[255].DurationMS = derivedTimelineMaximumMS - 255
	descriptor.DurationMS, descriptor.Bytes = derivedTimelineMaximumMS, derivedCacheLimit
	value := timelineForTest(t, descriptor, at)
	for i := 0; i < 256; i++ {
		anchor, err := value.locate(int64(i), at)
		if err != nil || anchor.partIndex != i+1 || anchor.offsetWithinDerivedSpanMS != 0 || anchor.atDerivedEnd {
			t.Fatal("maximum bounded span boundary failed", i, err)
		}
	}
	anchor, err := value.locate(derivedTimelineMaximumMS, at)
	if err != nil || anchor.partIndex != 256 || !anchor.atDerivedEnd || anchor.offsetWithinDerivedSpanMS != derivedTimelineMaximumMS-255 {
		t.Fatal("maximum final endpoint failed", err)
	}
	if output, err := value.position(anchor, at); err != nil || output != derivedTimelineMaximumMS {
		t.Fatal("maximum final endpoint overflowed", err)
	}
}

func TestDerivedTimelinePositionRangeNeverClamps(t *testing.T) {
	descriptor, at := timelineDescriptor()
	value := timelineForTest(t, descriptor, at)
	for _, input := range []int64{math.MinInt64, -1, 4001, math.MaxInt64} {
		if _, err := value.locate(input, at); !errors.Is(err, errDerivedTimelinePosition) {
			t.Fatal("invalid position was rounded/clamped/adopted", input, err)
		}
	}
}

func TestDerivedTimelineMalformedLayoutsFailBeforeOverflow(t *testing.T) {
	for _, change := range []func(*DerivedRecording){
		func(d *DerivedRecording) { d.PartOffsets = nil },
		func(d *DerivedRecording) { d.PartOffsets = d.PartOffsets[:1] },
		func(d *DerivedRecording) { d.PartOffsets = make([]DerivedPartOffset, 257) },
		func(d *DerivedRecording) { d.PartOffsets[0].StartMS = 1 },
		func(d *DerivedRecording) { d.PartOffsets[0].StartMS = math.MinInt64 },
		func(d *DerivedRecording) { d.PartOffsets[1].StartMS++ },
		func(d *DerivedRecording) { d.PartOffsets[1].StartMS-- },
		func(d *DerivedRecording) { d.PartOffsets[2].StartMS = math.MaxInt64 },
		func(d *DerivedRecording) { d.PartOffsets[1].PartIndex = 1 },
		func(d *DerivedRecording) { d.PartOffsets[1].PartIndex = 4 },
		func(d *DerivedRecording) { d.PartOffsets[0], d.PartOffsets[1] = d.PartOffsets[1], d.PartOffsets[0] },
		func(d *DerivedRecording) { d.PartOffsets[0].DurationMS = 0 },
		func(d *DerivedRecording) { d.PartOffsets[0].DurationMS = -1 },
		func(d *DerivedRecording) { d.PartOffsets[0].DurationMS = math.MaxInt64 },
		func(d *DerivedRecording) { d.PartOffsets[2].DurationMS++ },
		func(d *DerivedRecording) { d.DurationMS = 0 },
		func(d *DerivedRecording) { d.DurationMS = -1 },
		func(d *DerivedRecording) { d.DurationMS = derivedTimelineMaximumMS + 1 },
		func(d *DerivedRecording) { d.DurationMS = math.MaxInt64 },
	} {
		descriptor, at := timelineDescriptor()
		change(&descriptor)
		if value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineInvalid) {
			t.Fatal("malformed/overflow-shaped layout accepted", err)
		}
	}
}

func TestDerivedTimelineUnknownAndInvalidDescriptorDataRejects(t *testing.T) {
	for _, change := range []func(*DerivedRecording){
		func(d *DerivedRecording) { d.State = "unknown" },
		func(d *DerivedRecording) { d.State = "preparing" },
		func(d *DerivedRecording) { d.Reason = "expired" },
		func(d *DerivedRecording) { d.RecordingID = "../other" },
		func(d *DerivedRecording) { d.ID = strings.Repeat("A", 32) },
		func(d *DerivedRecording) { d.ID += "/x" },
		func(d *DerivedRecording) { d.SourceStatus = models.RecordingStatusRunning },
		func(d *DerivedRecording) { d.SourceIncomplete = false },
		func(d *DerivedRecording) { d.Bytes = 0 },
		func(d *DerivedRecording) { d.Bytes = -1 },
		func(d *DerivedRecording) { d.Bytes = derivedCacheLimit + 1 },
		func(d *DerivedRecording) { d.Bytes = math.MaxInt64 },
	} {
		descriptor, at := timelineDescriptor()
		change(&descriptor)
		if value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineInvalid) {
			t.Fatal("unknown/invalid descriptor data accepted", err)
		}
	}
	descriptor, at := timelineDescriptor()
	if value, err := newDerivedTimeline(uuid.Nil, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineInvalid) {
		t.Fatal("missing incarnation correlation accepted", err)
	}
}

func TestDerivedTimelineInvalidObservationShapeRejects(t *testing.T) {
	for _, change := range []func(*DerivedRecording){
		func(d *DerivedRecording) { d.ObservedAt = nil },
		func(d *DerivedRecording) { d.ExpiresAt = nil },
		func(d *DerivedRecording) { *d.ObservedAt = time.Time{} },
		func(d *DerivedRecording) { *d.ExpiresAt = time.Time{} },
		func(d *DerivedRecording) { *d.ExpiresAt = *d.ObservedAt },
		func(d *DerivedRecording) { *d.ExpiresAt = d.ObservedAt.Add(-time.Nanosecond) },
		func(d *DerivedRecording) { *d.ExpiresAt = d.ObservedAt.Add(derivedTTL + time.Nanosecond) },
		func(d *DerivedRecording) { *d.ObservedAt = time.Date(0, 1, 1, 1, 0, 0, 0, time.UTC) },
		func(d *DerivedRecording) { *d.ExpiresAt = time.Date(10000, 1, 1, 1, 0, 0, 0, time.UTC) },
	} {
		descriptor, at := timelineDescriptor()
		change(&descriptor)
		if value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineInvalid) {
			t.Fatal("invalid or unsupported timestamp interval accepted", err)
		}
	}
	descriptor, _ := timelineDescriptor()
	for _, at := range []time.Time{{}, time.Date(10000, 1, 1, 1, 0, 0, 0, time.UTC)} {
		if value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineInvalid) {
			t.Fatal("invalid supplied observation clock accepted", err)
		}
	}
}

func TestDerivedTimelineExpiryBoundariesAndNoLifetimeRenewal(t *testing.T) {
	descriptor, _ := timelineDescriptor()
	first := timelineForTest(t, descriptor, *descriptor.ObservedAt)
	last := timelineForTest(t, descriptor, descriptor.ExpiresAt.Add(-time.Nanosecond))
	if first.binding != last.binding || !first.expires.Equal(last.expires) {
		t.Fatal("receipt time renewed original observation or mapping")
	}
	anchor, err := first.locate(1001, descriptor.ExpiresAt.Add(-time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{descriptor.ObservedAt.Add(-time.Nanosecond), *descriptor.ExpiresAt, descriptor.ExpiresAt.Add(time.Hour)} {
		if value, err := newDerivedTimeline(timelineTestIncarnation, descriptor, at); value != nil || !errors.Is(err, errDerivedTimelineExpired) {
			t.Fatal("old/future observation became usable", err)
		}
		if _, err := first.locate(0, at); !errors.Is(err, errDerivedTimelineExpired) {
			t.Fatal("position lookup renewed original deadline")
		}
		if _, err := first.position(anchor, at); !errors.Is(err, errDerivedTimelineExpired) {
			t.Fatal("anchor lookup renewed original deadline")
		}
	}
}

func TestDerivedTimelineCanonicalAnchorRejectsInternalEndAliases(t *testing.T) {
	descriptor, at := timelineDescriptor()
	value := timelineForTest(t, descriptor, at)
	anchor, _ := value.locate(1000, at)
	for _, change := range []func(*derivedTimelineAnchor){
		func(a *derivedTimelineAnchor) { a.offsetWithinDerivedSpanMS = 1001 },
		func(a *derivedTimelineAnchor) { a.offsetWithinDerivedSpanMS = -1 },
		func(a *derivedTimelineAnchor) { a.offsetWithinDerivedSpanMS = math.MaxInt64 },
		func(a *derivedTimelineAnchor) { a.partIndex = 0 },
		func(a *derivedTimelineAnchor) { a.partIndex = -1 },
		func(a *derivedTimelineAnchor) { a.partIndex = 4 },
		func(a *derivedTimelineAnchor) { a.atDerivedEnd = true },
		func(a *derivedTimelineAnchor) { a.binding.revision[0] ^= 1 },
	} {
		copy := anchor
		change(&copy)
		if _, err := value.position(copy, at); !errors.Is(err, errDerivedTimelineAnchor) {
			t.Fatal("noncanonical/foreign anchor was silently normalized", err)
		}
	}
	end, _ := value.locate(4000, at)
	end.atDerivedEnd = false
	if _, err := value.position(end, at); !errors.Is(err, errDerivedTimelineAnchor) {
		t.Fatal("final endpoint lost its explicit data flag")
	}
}

func TestDerivedTimelineDifferentArtifactOrMappingNeverAdoptsOldAnchor(t *testing.T) {
	descriptor, at := timelineDescriptor()
	original := timelineForTest(t, descriptor, at)
	anchor, _ := original.locate(1001, at)
	for _, change := range []func(*DerivedRecording){
		func(d *DerivedRecording) { d.RecordingID = "recording-B" },
		func(d *DerivedRecording) { d.ID = strings.Repeat("b", 32) },
		func(d *DerivedRecording) { d.Bytes++ },
		func(d *DerivedRecording) { d.SourceStatus = models.RecordingStatusFailed },
		func(d *DerivedRecording) { d.GapsOmitted = false },
		func(d *DerivedRecording) { d.GapsOmitted = false; d.SourceIncomplete = false },
		func(d *DerivedRecording) { d.DurationMS++; d.PartOffsets[2].DurationMS++ },
		func(d *DerivedRecording) {
			d.PartOffsets[0].DurationMS++
			d.PartOffsets[1].StartMS++
			d.PartOffsets[1].DurationMS--
		},
		func(d *DerivedRecording) {
			*d.ObservedAt = d.ObservedAt.Add(time.Nanosecond)
			*d.ExpiresAt = d.ExpiresAt.Add(time.Nanosecond)
		},
		func(d *DerivedRecording) { *d.ExpiresAt = d.ExpiresAt.Add(-time.Nanosecond) },
	} {
		changed, now := timelineDescriptor()
		change(&changed)
		other := timelineForTest(t, changed, now)
		if other.binding.revision == original.binding.revision {
			t.Fatal("changed data collided with original mapping tuple")
		}
		if _, err := other.position(anchor, now); !errors.Is(err, errDerivedTimelineAnchor) {
			t.Fatal("old anchor adopted regenerated/different artifact", err)
		}
	}
	other, err := newDerivedTimeline(uuid.MustParse("11111111-2222-4333-8444-666666666666"), descriptor, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.position(anchor, at); !errors.Is(err, errDerivedTimelineAnchor) {
		t.Fatal("recording incarnation replacement adopted old anchor")
	}
}

func TestDerivedTimelineCopiesInputOffsetsAndTimestampPointers(t *testing.T) {
	descriptor, at := timelineDescriptor()
	value := timelineForTest(t, descriptor, at)
	binding, expiry := value.binding, value.expires
	descriptor.PartOffsets[0].DurationMS = math.MaxInt64
	descriptor.PartOffsets[1].StartMS = -1
	*descriptor.ObservedAt = at.Add(time.Hour)
	*descriptor.ExpiresAt = at.Add(-time.Hour)
	descriptor.PartOffsets = append(descriptor.PartOffsets, DerivedPartOffset{4, 1, 1})
	anchor, err := value.locate(1001, at)
	if err != nil || anchor.partIndex != 2 || anchor.offsetWithinDerivedSpanMS != 0 || value.binding != binding || !value.expires.Equal(expiry) || len(value.spans) != 3 {
		t.Fatal("caller mutation changed immutable mapping", err)
	}
	anchor.partIndex, anchor.binding.artifactID = 99, "changed"
	if fresh, err := value.locate(1001, at); err != nil || fresh.partIndex != 2 || fresh.binding != binding {
		t.Fatal("returned anchor mutation changed original map")
	}
}

func TestDerivedTimelineTimestampNormalizationUsesExactWideRange(t *testing.T) {
	descriptor, at := timelineDescriptor()
	original := timelineForTest(t, descriptor, at)
	zone := time.FixedZone("synthetic-offset", 5*3600+30*60)
	*descriptor.ObservedAt = descriptor.ObservedAt.In(zone)
	*descriptor.ExpiresAt = descriptor.ExpiresAt.In(zone)
	other := timelineForTest(t, descriptor, at.In(zone))
	if other.binding != original.binding {
		t.Fatal("same instants in another zone changed canonical hash")
	}
	// The two observed instants differ by exactly 2^64 nanoseconds. A narrowed
	// signed nanosecond epoch could alias them; supported civil timestamps cannot.
	early := time.Date(1000, 1, 2, 3, 4, 5, 6, time.UTC)
	later := time.Unix(early.Unix()+18_446_744_073, int64(early.Nanosecond())+709_551_616).UTC()
	var revisions [][32]byte
	for _, observed := range []time.Time{early, later, time.Date(1, 1, 2, 0, 0, 0, 1, time.UTC), time.Date(9999, 12, 30, 0, 0, 0, 1, time.UTC)} {
		d, _ := timelineDescriptor()
		expires := observed.Add(derivedTTL)
		d.ObservedAt, d.ExpiresAt = &observed, &expires
		mapped := timelineForTest(t, d, observed.Add(time.Minute))
		for _, revision := range revisions {
			if revision == mapped.binding.revision {
				t.Fatal("supported distinct timestamp tuple was narrowed or aliased")
			}
		}
		revisions = append(revisions, mapped.binding.revision)
	}
}

func TestDerivedTimelineNilValuesNeverProvideCoordinates(t *testing.T) {
	var value *derivedTimeline
	_, at := timelineDescriptor()
	if value.current(at) {
		t.Fatal("nil timeline became current")
	}
	if _, err := value.locate(0, at); !errors.Is(err, errDerivedTimelineInvalid) {
		t.Fatal("nil timeline returned a position")
	}
	if _, err := value.position(derivedTimelineAnchor{}, at); !errors.Is(err, errDerivedTimelineInvalid) {
		t.Fatal("nil timeline adopted an empty anchor")
	}
}
