package recordings

import (
	"context"
	"errors"
	"sort"
	"time"

	"novastream/models"
)

const planningJobLimit = 2000

// RecordingCapacity is private routing information and must not be serialized.
type RecordingCapacity struct {
	Key   string
	Limit int
}

type CapacityPlanner interface {
	PlanRecordingCapacity(context.Context, models.Recording) (RecordingCapacity, error)
}

type planningWindow struct {
	job      models.Recording
	start    time.Time
	end      time.Time
	capacity RecordingCapacity
	resolved bool
}

// Plan combines materialized jobs and known rule matches across shared provider
// budgets, but returns only profileID's items. No job or lease is changed.
func (s *Service) Plan(ctx context.Context, profileID string, candidate *models.Recording) (models.RecordingPlan, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	now := time.Now().UTC()
	jobs, err := s.repo.List(ctx, models.RecordingListFilter{IncludeAll: true, Limit: planningJobLimit + 1,
		Statuses: []models.RecordingStatus{models.RecordingStatusPending, models.RecordingStatusStarting, models.RecordingStatusRunning}})
	if err != nil {
		return models.RecordingPlan{}, errors.New("recording plan unavailable")
	}
	complete := len(jobs) <= planningJobLimit
	if !complete {
		jobs = jobs[:planningJobLimit]
	}
	if candidate != nil {
		copy := *candidate
		copy.ID = ""
		copy.RuleID = ""
		copy.UserID = profileID
		copy.Status = models.RecordingStatusPending
		jobs = append(jobs, copy)
	}
	planner, supported := s.channels.(CapacityPlanner)
	windows := make([]planningWindow, 0, len(jobs))
	// One routing check per profile/channel/input in this snapshot, including
	// repeated EPG airings. Never cache across settings or catalog changes.
	type cacheKey struct{ profile, channel, source string }
	type cached struct {
		target RecordingCapacity
		ok     bool
	}
	cache := make(map[cacheKey]cached)
	resolve := func(job models.Recording) (RecordingCapacity, bool) {
		if !supported || ctx.Err() != nil {
			return RecordingCapacity{}, false
		}
		key := cacheKey{job.UserID, job.ChannelID, job.SourceURL}
		target, exists := cache[key]
		if !exists {
			capacity, resolveErr := planner.PlanRecordingCapacity(ctx, job)
			target = cached{capacity, resolveErr == nil && capacity.Key != "" && capacity.Limit >= 0}
			cache[key] = target
		}
		return target.target, target.ok
	}
	projection := s.projectRecordingRules(ctx, profileID, now, jobs, resolve)
	jobs = projection.jobs
	windows = make([]planningWindow, 0, len(jobs))
	for _, job := range jobs {
		if !planningStatus(job.Status) {
			continue
		}
		start, end, valid := planningTimes(job, now)
		if valid && !end.After(now) {
			continue
		}
		window := planningWindow{job: job, start: start, end: end}
		if !valid {
			complete = false
		} else {
			window.capacity, window.resolved = resolve(job)
		}
		windows = append(windows, window)
	}
	return buildRecordingPlan(now, profileID, windows, complete, &projection), nil
}

func planningStatus(status models.RecordingStatus) bool {
	return status == models.RecordingStatusPending || status == models.RecordingStatusStarting || status == models.RecordingStatusRunning
}

func planningTimes(job models.Recording, now time.Time) (time.Time, time.Time, bool) {
	// Bound legacy malformed values before converting seconds to a duration.
	if job.PaddingBeforeSeconds < 0 || job.PaddingBeforeSeconds > 86400 || job.PaddingAfterSeconds < 0 || job.PaddingAfterSeconds > 86400 || job.StartAt.IsZero() || !job.EndAt.After(job.StartAt) {
		return job.StartAt, job.EndAt, false
	}
	start := job.StartAt.Add(-time.Duration(job.PaddingBeforeSeconds) * time.Second).UTC()
	end := job.EndAt.Add(time.Duration(job.PaddingAfterSeconds) * time.Second).UTC()
	if start.Year() < 1 || end.Year() > 9999 {
		return job.StartAt, job.EndAt, false
	}
	if start.Before(now) {
		start = now
	}
	return start, end, true
}

type planningEvent struct {
	at    time.Time
	delta int
}

func buildRecordingPlan(now time.Time, profileID string, windows []planningWindow, complete bool, projections ...*ruleProjection) models.RecordingPlan {
	plan := models.RecordingPlan{CheckedAt: now, Complete: complete, Recordings: []models.RecordingPlanItem{}}
	var projection *ruleProjection
	if len(projections) > 0 {
		projection = projections[0]
	}
	if projection != nil {
		plan.RuleForecast = projection.metadata
		if projection.metadata != nil && (projection.metadata.Status == "partial" || projection.metadata.Status == "unavailable") {
			plan.Complete = false
		}
	}
	events := make(map[string][]planningEvent)
	limits := make(map[string]int)
	for _, window := range windows {
		if !window.resolved {
			plan.Complete = false
			continue
		}
		key := window.capacity.Key
		if limit := window.capacity.Limit; limit > 0 && (limits[key] == 0 || limit < limits[key]) {
			limits[key] = limit
		}
		// Each recorder currently opens a private FFmpeg input, even for the
		// same channel. Only the live upstream hub shares provider connections.
		events[key] = append(events[key], planningEvent{window.start, 1}, planningEvent{window.end, -1})
	}
	for key := range events {
		sort.Slice(events[key], func(i, j int) bool { return events[key][i].at.Before(events[key][j].at) })
	}
	for _, window := range windows {
		if window.job.UserID != profileID {
			continue
		}
		item := models.RecordingPlanItem{RecordingID: window.job.ID, State: "unknown", Start: window.start, End: window.end}
		if !window.resolved {
			item.State = "unavailable"
		} else {
			item.MaxStreams = limits[window.capacity.Key]
			count := 0
			bucket := events[window.capacity.Key]
			for i := 0; i < len(bucket); {
				at := bucket[i].at
				for i < len(bucket) && bucket[i].at.Equal(at) {
					count += bucket[i].delta
					i++
				}
				if i == len(bucket) || !bucket[i].at.After(window.start) || !at.Before(window.end) {
					continue
				}
				if count > item.PeakScheduledConnections {
					item.PeakScheduledConnections = count
				}
				if item.MaxStreams > 0 && count > item.MaxStreams && item.FirstConflictStart == nil {
					start, end := at, bucket[i].at
					if start.Before(window.start) {
						start = window.start
					}
					if end.After(window.end) {
						end = window.end
					}
					item.FirstConflictStart, item.FirstConflictEnd = &start, &end
				}
			}
			uncertain := !complete
			if projection != nil && projection.uncertain(window) {
				uncertain = true
				plan.Complete = false
			}
			for _, other := range windows {
				if !other.resolved && other.start.Before(window.end) && other.end.After(window.start) {
					uncertain = true
				}
			}
			if item.FirstConflictStart != nil {
				item.State = "conflict"
			} else if item.MaxStreams > 0 && !uncertain {
				item.State = "clear"
			}
		}
		if window.job.ID == "" && window.job.RuleID != "" {
			plan.ProjectedRecordings = append(plan.ProjectedRecordings, models.ProjectedRecordingPlanItem{
				RecordingPlanItem: item, RuleID: window.job.RuleID, ChannelName: window.job.ChannelName, Title: window.job.Title,
			})
		} else if window.job.ID == "" {
			plan.Candidate = &item
		} else {
			plan.Recordings = append(plan.Recordings, item)
		}
	}
	sort.Slice(plan.Recordings, func(i, j int) bool { return plan.Recordings[i].RecordingID < plan.Recordings[j].RecordingID })
	sort.Slice(plan.ProjectedRecordings, func(i, j int) bool {
		a, b := plan.ProjectedRecordings[i], plan.ProjectedRecordings[j]
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		if a.RuleID != b.RuleID {
			return a.RuleID < b.RuleID
		}
		return a.ChannelName < b.ChannelName
	})
	return plan
}
