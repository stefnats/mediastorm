package recordings

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"novastream/internal/datastore"
	"novastream/models"
)

func timelineDataDescriptor() (DerivedRecording, time.Time) {
	observed := time.Date(2026, 11, 20, 1, 0, 0, 123456789, time.UTC)
	expires := observed.Add(time.Hour)
	return DerivedRecording{RecordingID: "synthetic-recording", ID: "0123456789abcdef0123456789abcdef", State: "ready", DurationMS: 1600, Bytes: 4096, SourceStatus: models.RecordingStatusCompleted, PartOffsets: []DerivedPartOffset{{PartIndex: 1, StartMS: 0, DurationMS: 800}, {PartIndex: 2, StartMS: 800, DurationMS: 800}}, ObservedAt: &observed, ExpiresAt: &expires}, observed.Add(time.Second)
}
func TestDerivedTimelineDataUsesOriginalCanonicalMapperAndCopies(t *testing.T) {
	desc, at := timelineDataDescriptor()
	inc := uuid.New()
	before, err := json.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := newDerivedTimeline(inc, desc, at)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ProjectDerivedTimelineData(inc, desc, at)
	if err != nil || value.State != "known" || value.SchemaVersion != 1 || value.CoordinateSystem != "derived-media" || value.MappingRevision != hex.EncodeToString(mapped.binding.revision[:]) || value.Incarnation != inc.String() || value.ArtifactID != desc.ID || value.DurationMS != 1600 || len(value.Spans) != 2 || value.Spans[1].StartMS != 800 {
		t.Fatal("projection changed canonical measured data", err)
	}
	if value.SourceIncomplete == nil || *value.SourceIncomplete || value.GapsOmitted == nil || *value.GapsOmitted || value.ObservedAt == nil || !value.ObservedAt.Equal(*desc.ObservedAt) || !value.ExpiresAt.Equal(*desc.ExpiresAt) {
		t.Fatal("projection lost false flags or original lifetime")
	}
	after, _ := json.Marshal(desc)
	if !bytes.Equal(before, after) {
		t.Fatal("projection modified legacy descriptor")
	}
	desc.PartOffsets[0].DurationMS = 1
	*desc.ObservedAt = time.Time{}
	*desc.ExpiresAt = time.Time{}
	if value.Spans[0].DurationMS != 800 || value.ObservedAt.IsZero() || value.ExpiresAt.IsZero() {
		t.Fatal("projection borrowed mutable descriptor storage")
	}
}
func TestDerivedTimelineDataUnknownHasNoCoordinateOrIdentity(t *testing.T) {
	for _, reason := range []string{"not_prepared", "configuration_changed", "source_changed", "expired"} {
		t.Run(reason, func(t *testing.T) {
			desc, at := timelineDataDescriptor()
			desc.State = "unknown"
			desc.Reason = reason
			value, err := ProjectDerivedTimelineData(uuid.New(), desc, at)
			if err != nil || value.State != "unknown" || value.Reason != reason || value.Incarnation != "" || value.ArtifactID != "" || value.MappingRevision != "" || value.DurationMS != 0 || len(value.Spans) != 0 || value.ObservedAt != nil || value.ExpiresAt != nil || value.SourceStatus != "" || value.SourceIncomplete != nil || value.GapsOmitted != nil {
				t.Fatal("unknown invented measured coordinates", err)
			}
			raw, _ := json.Marshal(value)
			for _, key := range []string{"incarnation", "artifactId", "mappingRevision", "durationMs", "observedAt", "expiresAt", "sourceStatus", "gapsOmitted"} {
				if bytes.Contains(raw, []byte(`"`+key+`"`)) {
					t.Fatal("unknown leaked a known-only field")
				}
			}
		})
	}
}
func TestDerivedTimelineDataRejectsInvalidAndExpiredWithoutRenewal(t *testing.T) {
	for _, name := range []string{"no incarnation", "unknown reason", "expired", "before observed", "missing timestamp", "bad spans", "gap flags", "wrong state"} {
		t.Run(name, func(t *testing.T) {
			desc, at := timelineDataDescriptor()
			inc := uuid.New()
			switch name {
			case "no incarnation":
				inc = uuid.Nil
			case "unknown reason":
				desc.State = "unknown"
				desc.Reason = "pretend_ready"
			case "expired":
				at = *desc.ExpiresAt
			case "before observed":
				at = desc.ObservedAt.Add(-time.Nanosecond)
			case "missing timestamp":
				desc.ObservedAt = nil
			case "bad spans":
				desc.PartOffsets[1].StartMS++
			case "gap flags":
				desc.GapsOmitted = true
			case "wrong state":
				desc.State = "prepared"
			}
			if _, err := ProjectDerivedTimelineData(inc, desc, at); err == nil {
				t.Fatal("invalid source became a known timeline")
			}
		})
	}
}
func TestDerivedTimelineDataCorrelatesOnlyExactArtifactAndIncarnation(t *testing.T) {
	desc, at := timelineDataDescriptor()
	inc := uuid.New()
	first, err := ProjectDerivedTimelineData(inc, desc, at)
	if err != nil {
		t.Fatal(err)
	}
	later, err := ProjectDerivedTimelineData(inc, desc, at.Add(time.Minute))
	if err != nil || later.MappingRevision != first.MappingRevision || !later.ExpiresAt.Equal(*first.ExpiresAt) {
		t.Fatal("reading renewed or rewrote original map")
	}
	other, err := ProjectDerivedTimelineData(uuid.New(), desc, at)
	if err != nil || other.MappingRevision == first.MappingRevision {
		t.Fatal("foreign incarnation adopted map")
	}
	desc.ID = "1123456789abcdef0123456789abcdef"
	other, err = ProjectDerivedTimelineData(inc, desc, at)
	if err != nil || other.MappingRevision == first.MappingRevision {
		t.Fatal("different artifact silently rebased map")
	}
}
func TestDerivedTimelineDataPreservesIncompleteMeasuredLengthWithoutProgrammeClaims(t *testing.T) {
	desc, at := timelineDataDescriptor()
	desc.SourceIncomplete = true
	desc.GapsOmitted = true
	desc.SourceStatus = models.RecordingStatusCancelled
	value, err := ProjectDerivedTimelineData(uuid.New(), desc, at)
	if err != nil || !*value.SourceIncomplete || !*value.GapsOmitted || value.Spans[1].StartMS+value.Spans[1].DurationMS != value.DurationMS {
		t.Fatal("gaps changed actual derived media coordinates", err)
	}
	raw, _ := json.Marshal(value)
	for _, key := range []string{"position", "completed", "programme", "url", "path", "token", "writerGeneration"} {
		if bytes.Contains(raw, []byte(`"`+key+`"`)) {
			t.Fatal("data projection invented playback/progress authority")
		}
	}
}
func TestDerivedTimelineDataReadinessRequiresActualRepository(t *testing.T) {
	var nilService *Service
	if nilService.DerivedTimelineReady(nil) || (&Service{}).DerivedTimelineReady(&datastore.DataStore{}) {
		t.Fatal("empty or synthetic repository reported ready")
	}
	f := derivedLedgerFixture(t)
	if f.service.DerivedTimelineReady(&datastore.DataStore{}) {
		t.Fatal("fake legacy repository became genuine PG source")
	}
}
