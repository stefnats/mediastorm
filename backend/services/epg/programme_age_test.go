package epg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

func programmeAgeGuide(t *testing.T) (*Service, *atomic.Int32) {
	t.Helper()
	ratings := map[string]string{
		"allowed":           `<rating system="FSK"><value>6</value></rating>`,
		"zero":              `<rating system="FSK"><value>0</value></rating>`,
		"too-old":           `<rating system="FSK"><value>12</value></rating>`,
		"missing-system":    `<rating><value>6</value></rating>`,
		"missing-rating":    ``,
		"german-with-mpaa":  `<rating system="FSK"><value>6</value></rating><rating system="MPAA"><value>PG</value></rating>`,
		"german-with-us-tv": `<rating system="TV Parental Guidelines"><value>TV-G</value></rating><rating system="FSK"><value>12</value></rating>`,
		"foreign":           `<rating system="MPAA"><value>PG</value></rating>`,
		"conflicting":       `<rating system="FSK"><value>6</value></rating><rating system="FSK"><value>12</value></rating>`,
		"duplicate":         `<rating system="FSK"><value>6</value></rating><rating system="fsk"><value>FSK 6</value></rating>`,
		"malformed":         `<rating system="FSK"><value>7</value></rating>`,
		"overlap":           `<rating system="FSK"><value>6</value></rating>`,
		"alias-one":         `<rating system="FSK"><value>6</value></rating>`,
		"alias-two":         `<rating system="FSK"><value>6</value></rating>`,
	}
	var xml strings.Builder
	xml.WriteString(`<tv>`)
	for channel, rating := range ratings {
		name := channel
		if strings.HasPrefix(channel, "alias-") {
			name = "Shared station"
		}
		fmt.Fprintf(&xml, `<channel id="%s"><display-name>%s</display-name></channel><programme start="20261122100000 +0000" stop="20261122110000 +0000" channel="%s"><title>Programme %s</title>%s</programme>`, channel, name, channel, channel, rating)
	}
	xml.WriteString(`<programme start="20261122110000 +0000" stop="20261122120000 +0000" channel="allowed"><title>Later programme</title><rating system="FSK"><value>12</value></rating></programme>`)
	xml.WriteString(`<programme start="20261122102000 +0000" stop="20261122104000 +0000" channel="overlap"><title>Overlapping programme</title><rating system="FSK"><value>12</value></rating></programme></tv>`)
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(xml.String()))
	}))
	t.Cleanup(server.Close)
	s := &Service{storageDir: t.TempDir(), client: server.Client(), schedule: &models.EPGSchedule{Channels: map[string]models.EPGChannel{}, Programs: map[string][]models.EPGProgram{}}}
	if err := s.fetchXMLTV(context.Background(), server.URL+"/guide.xml?fixture=source-one", s.schedule); err != nil {
		t.Fatal(err)
	}
	return s, calls
}

func programmeAgeTime(hour, minute int) time.Time {
	return time.Date(2026, 11, 22, hour, minute, 0, 0, time.UTC)
}

func TestXMLTVProgrammeRatingsPersistThroughCache(t *testing.T) {
	s, calls := programmeAgeGuide(t)
	p := s.schedule.Programs["conflicting"][0]
	if p.Rating != "6" || len(p.Ratings) != 2 || p.Ratings[0].System != "FSK" || p.Ratings[1].Value != "12" || p.Source == nil || p.Source.Type != "xmltv" || p.Source.ID == "" || p.Source.ObservedAt.IsZero() {
		t.Fatalf("lost guide rating/source: %+v", p)
	}
	encoded, err := json.Marshal(p.Source)
	if err != nil || strings.Contains(string(encoded), "/guide.xml") || strings.Contains(string(encoded), "fixture=source-one") {
		t.Fatalf("source description contains request address: %s %v", encoded, err)
	}
	if err := s.saveToDisk(); err != nil {
		t.Fatal(err)
	}
	restored := NewService(s.storageDir, nil)
	<-restored.restoreDone
	got := restored.CurrentProgramAgeDecision("allowed", 6, programmeAgeTime(10, 30))
	if got.State != "within_limit" || got.Age == nil || *got.Age != 6 || got.Program == nil || got.Program.Source.ID != p.Source.ID {
		t.Fatalf("cached decision: %+v", got)
	}
	// The returned value is descriptive; editing it must not change the cache.
	got.Program.Ratings[0].Value = "18"
	got.Program.Source.ID = "changed-copy"
	fresh := restored.CurrentProgramAgeDecision("allowed", 6, programmeAgeTime(10, 30))
	if fresh.State != "within_limit" || fresh.Program.Rating != "6" || fresh.Program.Source.ID != p.Source.ID || calls.Load() != 1 {
		t.Fatalf("cache changed or was fetched again: %+v calls=%d", fresh, calls.Load())
	}
	// A historical cache with only the old first rating text stays unknown.
	legacy := restored.schedule.Programs["allowed"][0]
	legacy.Ratings, legacy.Source = nil, nil
	restored.schedule.Programs["allowed"] = []models.EPGProgram{legacy}
	if old := restored.CurrentProgramAgeDecision("allowed", 6, programmeAgeTime(10, 30)); old.State != "unknown" {
		t.Fatalf("legacy text guessed FSK: %+v", old)
	}
}

func TestEPGCurrentProgrammeAgeDecision(t *testing.T) {
	s, calls := programmeAgeGuide(t)
	for _, tc := range []struct {
		name, channel string
		limit         int
		state, reason string
	}{
		{"age-six", "allowed", 6, "within_limit", "guide_age_within_limit"},
		{"age-zero", "zero", 0, "within_limit", "guide_age_within_limit"},
		{"above-zero", "allowed", 0, "too_old", "guide_age_above_limit"},
		{"age-twelve", "too-old", 6, "too_old", "guide_age_above_limit"},
		{"system-missing", "missing-system", 6, "unknown", "rating_system_unknown"},
		{"rating-missing", "missing-rating", 6, "unknown", "rating_unknown"},
		{"german-six-with-mpaa", "german-with-mpaa", 6, "within_limit", "guide_age_within_limit"},
		{"german-twelve-with-us-tv", "german-with-us-tv", 6, "too_old", "guide_age_above_limit"},
		{"foreign-system", "foreign", 6, "unknown", "rating_system_unknown"},
		{"conflicting-values", "conflicting", 6, "unknown", "conflicting_ratings"},
		{"duplicate-agrees", "duplicate", 6, "within_limit", "guide_age_within_limit"},
		{"invalid-age", "malformed", 6, "unknown", "rating_unknown"},
		{"overlapping-shows", "overlap", 6, "unknown", "overlapping_programmes"},
		{"ambiguous-alias", "Shared station", 6, "unknown", "ambiguous_channel"},
		{"missing-channel", "unknown-channel", 6, "unknown", "channel_unknown"},
		{"invalid-configured-limit", "allowed", 7, "unknown", "invalid_age_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := s.CurrentProgramAgeDecision(tc.channel, tc.limit, programmeAgeTime(10, 30))
			if got.State != tc.state || got.Reason != tc.reason {
				t.Fatalf("decision=%+v", got)
			}
			if got.State != "unknown" && (got.Program == nil || got.Program.ChannelID != got.ChannelID || got.Program.Start != programmeAgeTime(10, 0) || got.Program.Stop != programmeAgeTime(11, 0)) {
				t.Fatalf("decision lost exact programme: %+v", got)
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatalf("cache reads fetched guide: %d", calls.Load())
	}
}

func TestEPGProgrammeAgeBoundaryTimes(t *testing.T) {
	s, calls := programmeAgeGuide(t)
	for _, tc := range []struct {
		name  string
		at    time.Time
		state string
	}{
		{"before-start", programmeAgeTime(9, 59), "unknown"},
		{"exact-start", programmeAgeTime(10, 0), "within_limit"},
		{"next-show-at-stop", programmeAgeTime(11, 0), "too_old"},
		{"no-show-after-stop", programmeAgeTime(12, 0), "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := s.CurrentProgramAgeDecision("allowed", 6, tc.at)
			if got.State != tc.state {
				t.Fatalf("boundary decision: %+v", got)
			}
			if tc.name == "next-show-at-stop" && got.Program.Title != "Later programme" {
				t.Fatalf("previous show remained current: %+v", got)
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatalf("boundary checks fetched guide: %d", calls.Load())
	}
}

func TestXMLTVProgrammeOriginsRemainDistinct(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		age := 6
		if r.URL.Path == "/second" {
			age = 12
		}
		fmt.Fprintf(w, `<tv><channel id="station"><display-name>Station</display-name></channel><programme start="20261122100000 +0000" stop="20261122110000 +0000" channel="station"><title>Programme</title><rating system="FSK"><value>%d</value></rating></programme></tv>`, age)
	}))
	defer server.Close()
	s := &Service{client: server.Client(), schedule: &models.EPGSchedule{Channels: map[string]models.EPGChannel{}, Programs: map[string][]models.EPGProgram{}}}
	for _, route := range []string{"/first", "/second"} {
		if err := s.fetchXMLTV(t.Context(), server.URL+route, s.schedule); err != nil {
			t.Fatal(err)
		}
	}
	rows := s.schedule.Programs["station"]
	if len(rows) != 2 || rows[0].Source == nil || rows[1].Source == nil || rows[0].Source.ID == rows[1].Source.ID || rows[0].Ratings[0].Value == rows[1].Ratings[0].Value {
		t.Fatalf("source merge lost ratings or origins: %+v", rows)
	}
	decision := s.CurrentProgramAgeDecision("station", 6, programmeAgeTime(10, 30))
	if decision.State != "unknown" || decision.Reason != "overlapping_programmes" || calls.Load() != 2 {
		t.Fatalf("conflicting source overlap: %+v calls=%d", decision, calls.Load())
	}
}
