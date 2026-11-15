package recordings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"novastream/models"
)

type planningChannels struct {
	calls int
	fail  bool
}

func (p *planningChannels) RecordingChannels(context.Context, string) ([]Channel, error) {
	panic("forecast must not fetch a stream")
}
func (p *planningChannels) PlanRecordingCapacity(ctx context.Context, job models.Recording) (RecordingCapacity, error) {
	p.calls++
	if p.fail || ctx.Err() != nil || job.SourceURL == "unresolved" {
		return RecordingCapacity{}, errors.New("synthetic-private-error")
	}
	limit := 1
	if job.SourceURL == "unknown" {
		limit = 0
	}
	return RecordingCapacity{Key: "shared-private-bucket", Limit: limit}, nil
}

func planJob(id, profile string, start, end time.Time) models.Recording {
	return models.Recording{ID: id, UserID: profile, ChannelID: "channel", SourceURL: "synthetic-private-input", Title: "synthetic-private-title", Status: models.RecordingStatusPending, StartAt: start, EndAt: end}
}

func TestRecordingPlanOverlapBoundariesAndPadding(t *testing.T) {
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		start, end    int
		before, after int
		limit         int
		want          string
		peak          int
	}{
		{"overlap", 30, 90, 0, 0, 1, "conflict", 2},
		{"touching", 60, 90, 0, 0, 1, "clear", 1},
		{"before", -30, 0, 0, 0, 1, "clear", 1},
		{"one second padding", 60, 90, 1, 0, 1, "conflict", 2},
		{"after padding", -30, 0, 0, 1, 1, "conflict", 2},
		{"two slots", 30, 90, 0, 0, 2, "clear", 2},
		{"unconfigured", 30, 90, 0, 0, 0, "unknown", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := planJob("own", "viewer", now.Add(time.Hour), now.Add(time.Hour+60*time.Second))
			b := planJob("foreign", "other", now.Add(time.Hour+time.Duration(tc.start)*time.Second), now.Add(time.Hour+time.Duration(tc.end)*time.Second))
			b.PaddingBeforeSeconds = tc.before
			b.PaddingAfterSeconds = tc.after
			windows := []planningWindow{}
			for _, j := range []models.Recording{a, b} {
				start, end, _ := planningTimes(j, now)
				windows = append(windows, planningWindow{job: j, start: start, end: end, resolved: true, capacity: RecordingCapacity{"provider", tc.limit}})
			}
			plan := buildRecordingPlan(now, "viewer", windows, true)
			if len(plan.Recordings) != 1 || plan.Recordings[0].State != tc.want || plan.Recordings[0].PeakScheduledConnections != tc.peak {
				t.Fatalf("unexpected plan: %+v", plan)
			}
			item := plan.Recordings[0]
			if tc.want == "conflict" && (item.FirstConflictStart == nil || !item.FirstConflictEnd.After(*item.FirstConflictStart)) {
				t.Fatal("missing exact conflict window")
			}
		})
	}
}

func TestRecordingPlanUsesConcurrentPeakRatherThanTotalOverlaps(t *testing.T) {
	now := time.Now().UTC()
	windows := []planningWindow{}
	for i, span := range [][2]int{{0, 100}, {0, 30}, {30, 60}, {60, 100}} {
		profile := "other"
		if i == 0 {
			profile = "viewer"
		}
		j := planJob(fmt.Sprint(i), profile, now.Add(time.Duration(span[0])*time.Second), now.Add(time.Duration(span[1])*time.Second))
		windows = append(windows, planningWindow{job: j, start: j.StartAt, end: j.EndAt, resolved: true, capacity: RecordingCapacity{"provider", 2}})
	}
	item := buildRecordingPlan(now, "viewer", windows, true).Recordings[0]
	if item.State != "clear" || item.PeakScheduledConnections != 2 {
		t.Fatalf("sequential jobs counted together: %+v", item)
	}
}

func TestRecordingPlanSweepMatchesIndependentDiscreteOracle(t *testing.T) {
	now := time.Now().UTC()
	rng := rand.New(rand.NewSource(8293))
	for iteration := 0; iteration < 100; iteration++ {
		windows := []planningWindow{}
		limit := 1 + rng.Intn(4)
		for i := 0; i < 12; i++ {
			start := rng.Intn(30)
			end := start + 1 + rng.Intn(12)
			j := planJob(fmt.Sprint(i), "viewer", now.Add(time.Duration(start)*time.Second), now.Add(time.Duration(end)*time.Second))
			windows = append(windows, planningWindow{job: j, start: j.StartAt, end: j.EndAt, resolved: true, capacity: RecordingCapacity{fmt.Sprint(i % 3), limit}})
		}
		plan := buildRecordingPlan(now, "viewer", windows, true)
		for _, item := range plan.Recordings {
			var own planningWindow
			for _, w := range windows {
				if w.job.ID == item.RecordingID {
					own = w
				}
			}
			peak := 0
			for at := own.start; at.Before(own.end); at = at.Add(time.Second) {
				count := 0
				for _, w := range windows {
					if own.capacity.Key == w.capacity.Key && !at.Before(w.start) && at.Before(w.end) {
						count++
					}
				}
				if count > peak {
					peak = count
				}
			}
			if item.PeakScheduledConnections != peak || (item.State == "conflict") != (peak > limit) {
				t.Fatalf("oracle mismatch: %+v peak=%d", item, peak)
			}
		}
	}
}

func TestRecordingPlanPrivateJobsAndCandidateNeverWriteOrReserve(t *testing.T) {
	now := time.Now().UTC().Add(time.Hour)
	a := planJob("own", "viewer", now, now.Add(time.Hour))
	b := planJob("synthetic-private-id", "other", now, now.Add(time.Hour))
	repo := newFakeRecordingRepo(a, b)
	s := newTestService(repo, "must-not-execute", t.TempDir())
	channels := &planningChannels{}
	s.channels = channels
	candidate := planJob("", "viewer", now, now.Add(time.Hour))
	plan, err := s.Plan(context.Background(), "viewer", &candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Recordings) != 1 || plan.Candidate == nil || plan.Candidate.State != "conflict" || plan.Candidate.PeakScheduledConnections != 3 {
		t.Fatalf("wrong plan: %+v", plan)
	}
	data, _ := json.Marshal(plan)
	for _, secret := range []string{"synthetic-private", "other", "shared-private-bucket", "sourceUrl", "userId", "title"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("private data leaked: %s", secret)
		}
	}
	if channels.calls != 2 {
		t.Fatalf("repeated same-profile airing was not coalesced: %d", channels.calls)
	}
	if got, _ := repo.Get(context.Background(), a.ID); !reflect.DeepEqual(*got, a) {
		t.Fatal("forecast wrote recording")
	}
	if n, _ := repo.Count(context.Background()); n != 2 {
		t.Fatal("preview persisted candidate")
	}
}

func TestRecordingPlanIncompleteNeverClaimsClear(t *testing.T) {
	now := time.Now().UTC().Add(time.Hour)
	for _, mode := range []string{"unresolved", "unknown", "cancelled", "completed", "expired", "cancelled context", "truncated", "invalid padding"} {
		t.Run(mode, func(t *testing.T) {
			a := planJob("own", "viewer", now, now.Add(time.Hour))
			b := planJob("other", "foreign", now, now.Add(time.Hour))
			b.SourceURL = mode
			switch mode {
			case "cancelled":
				b.Status = models.RecordingStatusCancelled
			case "completed":
				b.Status = models.RecordingStatusCompleted
			case "expired":
				b.StartAt = now.Add(-3 * time.Hour)
				b.EndAt = now.Add(-2 * time.Hour)
			case "invalid padding":
				b.PaddingAfterSeconds = int(^uint(0) >> 1)
			}
			repo := newFakeRecordingRepo(a, b)
			if mode == "truncated" {
				for i := 0; i < planningJobLimit; i++ {
					j := b
					j.ID = fmt.Sprint(i)
					repo.recordings[j.ID] = j
				}
			}
			s := newTestService(repo, "must-not-execute", t.TempDir())
			s.channels = &planningChannels{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled context" {
				cancel()
			}
			plan, err := s.Plan(ctx, "viewer", nil)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "cancelled" || mode == "completed" || mode == "expired" {
				if plan.Recordings[0].State != "clear" {
					t.Fatalf("inactive job counted: %+v", plan)
				}
			} else {
				for _, item := range plan.Recordings {
					if item.State == "clear" {
						t.Fatalf("incomplete plan claims clear: %+v", plan)
					}
				}
			}
		})
	}
}

func TestRecordingPlanStrictestAliasesAndDifferentAccounts(t *testing.T) {
	now := time.Now().UTC()
	a := planJob("own", "viewer", now, now.Add(time.Hour))
	b := planJob("foreign", "other", now, now.Add(time.Hour))
	for _, key := range []string{"same", "different"} {
		windows := []planningWindow{{a, now, a.EndAt, RecordingCapacity{"same", 3}, true}, {b, now, b.EndAt, RecordingCapacity{key, 1}, true}}
		item := buildRecordingPlan(now, "viewer", windows, true).Recordings[0]
		if key == "same" && (item.State != "conflict" || item.MaxStreams != 1) {
			t.Fatalf("alias weakened cap: %+v", item)
		}
		if key == "different" && (item.State != "clear" || item.PeakScheduledConnections != 1) {
			t.Fatalf("separate accounts merged: %+v", item)
		}
	}
}

func TestRecordingPlanTimezoneAndRunningWindow(t *testing.T) {
	now := time.Date(2026, 10, 25, 1, 10, 0, 0, time.UTC)
	start, _ := time.Parse(time.RFC3339, "2026-10-25T02:00:00+02:00")
	end, _ := time.Parse(time.RFC3339, "2026-10-25T02:30:00+01:00")
	j := planJob("own", "viewer", start, end)
	j.Status = models.RecordingStatusRunning
	j.PaddingAfterSeconds = 60
	a, b, valid := planningTimes(j, now)
	if !valid || !a.Equal(now) || !b.Equal(time.Date(2026, 10, 25, 1, 31, 0, 0, time.UTC)) {
		t.Fatalf("wrong DST/running window %v %v", a, b)
	}
}

type planningReadRepo struct {
	*fakeRecordingRepo
	filter models.RecordingListFilter
	fail   bool
}

func (r *planningReadRepo) List(ctx context.Context, filter models.RecordingListFilter) ([]models.Recording, error) {
	r.filter = filter
	if r.fail {
		return nil, errors.New("synthetic-private-storage-error")
	}
	return r.fakeRecordingRepo.List(ctx, filter)
}

func TestRecordingPlanBoundedReadAndSanitizedStorageFailure(t *testing.T) {
	now := time.Now().Add(time.Hour)
	repo := &planningReadRepo{fakeRecordingRepo: newFakeRecordingRepo(planJob("own", "viewer", now, now.Add(time.Hour)))}
	s := NewService(repo, "must-not-execute", t.TempDir())
	plan, err := s.Plan(context.Background(), "viewer", nil)
	if err != nil || plan.Complete || plan.Recordings[0].State != "unavailable" {
		t.Fatalf("missing planner claimed a forecast: %+v %v", plan, err)
	}
	if !repo.filter.IncludeAll || repo.filter.Limit != planningJobLimit+1 || len(repo.filter.Statuses) != 3 {
		t.Fatalf("unbounded or profile-only read: %+v", repo.filter)
	}
	repo.fail = true
	_, err = s.Plan(context.Background(), "viewer", nil)
	if err == nil || strings.Contains(err.Error(), "synthetic-private") {
		t.Fatal("storage failure missing or leaked")
	}
}

func TestRecordingPlanUnresolvedOutsideWindowDoesNotInventConflict(t *testing.T) {
	now := time.Now().UTC()
	a := planJob("own", "viewer", now, now.Add(time.Hour))
	b := planJob("foreign", "other", now.Add(2*time.Hour), now.Add(3*time.Hour))
	plan := buildRecordingPlan(now, "viewer", []planningWindow{{a, a.StartAt, a.EndAt, RecordingCapacity{"provider", 1}, true}, {b, b.StartAt, b.EndAt, RecordingCapacity{}, false}}, true)
	if plan.Complete || plan.Recordings[0].State != "clear" {
		t.Fatalf("non-overlapping unknown job changed known demand: %+v", plan)
	}
}
