package recordings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"novastream/models"
)

type forecastRules struct{ fakeRuleRepo }

func (r *forecastRules) ListRules(ctx context.Context, _ string, _, _ bool) ([]models.RecordingRule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]models.RecordingRule, 0, len(r.rules))
	for _, rule := range r.rules {
		out = append(out, rule)
	}
	return out, nil
}
func (r *forecastRules) CreateScheduledRecordingIfAbsent(context.Context, *models.Recording) (bool, error) {
	panic("forecast must not materialize a rule")
}

type forecastRepo struct {
	*fakeRecordingRepo
	historyExtra    []models.Recording
	historyOverride []models.Recording
	failHistory     bool
}

func (r *forecastRepo) List(ctx context.Context, filter models.RecordingListFilter) ([]models.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(filter.Statuses) == 0 && r.failHistory {
		return nil, errors.New("synthetic-private-history-error")
	}
	rows, _ := r.fakeRecordingRepo.List(ctx, filter)
	if len(filter.Statuses) == 0 {
		if r.historyOverride != nil {
			rows = append([]models.Recording{}, r.historyOverride...)
		}
		rows = append(rows, r.historyExtra...)
	}
	out := []models.Recording{}
	for _, job := range rows {
		allowed := len(filter.Statuses) == 0
		for _, status := range filter.Statuses {
			allowed = allowed || job.Status == status
		}
		if allowed {
			out = append(out, job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

type forecastChannels struct {
	byProfile    map[string][]Channel
	catalogCalls map[string]int
	fail         bool
}

func (c *forecastChannels) RecordingChannels(ctx context.Context, profile string) ([]Channel, error) {
	c.catalogCalls[profile]++
	if c.fail || ctx.Err() != nil {
		return nil, errors.New("synthetic-private-catalog-error")
	}
	return c.byProfile[profile], nil
}
func (c *forecastChannels) PlanRecordingCapacity(ctx context.Context, job models.Recording) (RecordingCapacity, error) {
	if ctx.Err() != nil || job.SourceURL == "unresolved" {
		return RecordingCapacity{}, errors.New("synthetic-private-source-error")
	}
	return RecordingCapacity{Key: job.SourceURL, Limit: 1}, nil
}

type forecastEPG struct {
	programs   map[string][]models.EPGProgram
	start, end time.Time
	calls      int
}

func (e *forecastEPG) GetScheduleMultiple(ids []string, start, end time.Time) map[string][]models.EPGProgram {
	e.calls++
	e.start, e.end = start, end
	out := map[string][]models.EPGProgram{}
	for _, id := range ids {
		for _, p := range e.programs[strings.ToLower(id)] {
			if p.Stop.After(start) && p.Start.Before(end) {
				out[id] = append(out[id], p)
			}
		}
	}
	return out
}

type forecastFixture struct {
	now      time.Time
	service  *Service
	repo     *forecastRepo
	rules    *forecastRules
	channels *forecastChannels
	epg      *forecastEPG
}

func newForecastFixture(t *testing.T) *forecastFixture {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	rules := &forecastRules{fakeRuleRepo{rules: map[string]models.RecordingRule{"own-rule": {ID: "own-rule", UserID: "viewer", Enabled: true, MatchType: models.RecordingRuleMatchTitle, Pattern: "Show", ChannelID: "channel"}}}}
	repo := &forecastRepo{fakeRecordingRepo: newFakeRecordingRepo()}
	channels := &forecastChannels{byProfile: map[string][]Channel{"viewer": {{ID: "channel", TvgID: "Guide.One", Name: "Own channel", SourceURL: "provider-a"}}, "foreign": {{ID: "foreign-channel", TvgID: "Guide.One", Name: "synthetic-private-channel", SourceURL: "provider-a"}}}, catalogCalls: map[string]int{}}
	epg := &forecastEPG{programs: map[string][]models.EPGProgram{"guide.one": {
		{Title: "Before", Start: now.Add(-24 * time.Hour), Stop: now.Add(time.Hour)},
		{Title: "Show", Start: now.Add(time.Hour), Stop: now.Add(2 * time.Hour)},
		{Title: "After", Start: now.Add(2 * time.Hour), Stop: now.Add(8 * 24 * time.Hour)},
	}}}
	s := NewService(repo, "must-not-execute", t.TempDir())
	s.SetRuleRepository(rules)
	s.SetRuleProviders(epg, channels, nil)
	return &forecastFixture{now, s, repo, rules, channels, epg}
}
func (f *forecastFixture) candidate() *models.Recording {
	return &models.Recording{UserID: "viewer", ChannelID: "channel", SourceURL: "provider-a", StartAt: f.now.Add(90 * time.Minute), EndAt: f.now.Add(110 * time.Minute)}
}
func (f *forecastFixture) plan(t *testing.T, candidate *models.Recording) models.RecordingPlan {
	t.Helper()
	p, err := f.service.Plan(context.Background(), "viewer", candidate)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f *forecastFixture) prior(status models.RecordingStatus) models.Recording {
	job := planJob("persisted-airing", "viewer", f.now.Add(time.Hour), f.now.Add(2*time.Hour))
	job.RuleID = "own-rule"
	job.TvgID = "GUIDE.ONE"
	job.ChannelID = "channel"
	job.SourceURL = "provider-a"
	job.Status = status
	job.ScheduleKey = "guide.one:" + job.StartAt.UTC().Format(time.RFC3339Nano)
	return job
}

func TestRecordingRuleForecastCountsKnownMatchesWithoutMaterialization(t *testing.T) {
	f := newForecastFixture(t)
	foreign := f.rules.rules["own-rule"]
	foreign.ID = "synthetic-private-rule"
	foreign.UserID = "foreign"
	foreign.ChannelID = "foreign-channel"
	f.rules.rules[foreign.ID] = foreign
	base := planJob("persisted-own", "viewer", f.now.Add(time.Hour), f.now.Add(2*time.Hour))
	base.SourceURL = "provider-a"
	f.repo.recordings[base.ID] = base
	plan := f.plan(t, f.candidate())
	if plan.RuleForecast == nil || plan.RuleForecast.Status != "available" || plan.RuleForecast.ProjectedCount != 1 || len(plan.ProjectedRecordings) != 1 {
		t.Fatalf("wrong projection: %+v", plan)
	}
	if len(plan.Recordings) != 1 || plan.Candidate == nil || plan.Candidate.State != "conflict" || plan.Candidate.PeakScheduledConnections != 4 {
		t.Fatalf("missing future shared demand: %+v", plan)
	}
	if plan.ProjectedRecordings[0].RecordingID != "" || plan.ProjectedRecordings[0].RuleID != "own-rule" {
		t.Fatal("projection manufactured a persisted ID")
	}
	data, _ := json.Marshal(plan)
	for _, secret := range []string{"synthetic-private-rule", "synthetic-private-channel", "foreign", "provider-a", "sourceUrl", "scheduleKey"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("foreign/private routing leaked: %s", secret)
		}
	}
	if len(f.repo.recordings) != 1 || !reflect.DeepEqual(f.repo.recordings[base.ID], base) {
		t.Fatal("forecast changed persisted jobs")
	}
	if f.rules.paused != 0 || f.channels.catalogCalls["viewer"] != 1 || f.channels.catalogCalls["foreign"] != 1 {
		t.Fatal("unexpected side effect or repeated catalog reads")
	}
}

func TestRecordingRuleForecastNeverConfusesProjectionWithCandidate(t *testing.T) {
	f := newForecastFixture(t)
	plan := f.plan(t, nil)
	if plan.Candidate != nil || len(plan.Recordings) != 0 || len(plan.ProjectedRecordings) != 1 {
		t.Fatalf("projection appeared as a candidate or saved job: %+v", plan)
	}
	candidate := f.candidate()
	candidate.RuleID = "caller-supplied-rule"
	plan = f.plan(t, candidate)
	if plan.Candidate == nil || len(plan.ProjectedRecordings) != 1 {
		t.Fatal("candidate rule metadata was trusted")
	}
}

func TestRecordingRuleForecastDeduplicatesSchedulerTerminalStates(t *testing.T) {
	for _, status := range []models.RecordingStatus{models.RecordingStatusPending, models.RecordingStatusStarting, models.RecordingStatusRunning, models.RecordingStatusCompleted, models.RecordingStatusFailed, models.RecordingStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			f := newForecastFixture(t)
			job := f.prior(status)
			f.repo.recordings[job.ID] = job
			plan := f.plan(t, f.candidate())
			if len(plan.ProjectedRecordings) != 0 || plan.RuleForecast.ProjectedCount != 0 {
				t.Fatalf("existing airing projected again: %+v", plan)
			}
			wantPeak := 1
			if planningStatus(status) {
				wantPeak = 2
			}
			if plan.Candidate.PeakScheduledConnections != wantPeak {
				t.Fatalf("wrong count for %s: %+v", status, plan.Candidate)
			}
		})
	}
	t.Run("paused rule is revivable", func(t *testing.T) {
		f := newForecastFixture(t)
		job := f.prior(models.RecordingStatusCancelled)
		job.Error = "recording rule paused"
		f.repo.recordings[job.ID] = job
		plan := f.plan(t, f.candidate())
		if len(plan.ProjectedRecordings) != 1 || plan.Candidate.PeakScheduledConnections != 2 {
			t.Fatalf("paused airing was not projected: %+v", plan)
		}
	})
	t.Run("legacy schedule key derives normalized tvg and instant", func(t *testing.T) {
		f := newForecastFixture(t)
		job := f.prior(models.RecordingStatusCancelled)
		job.ScheduleKey = ""
		f.repo.recordings[job.ID] = job
		if len(f.plan(t, f.candidate()).ProjectedRecordings) != 0 {
			t.Fatal("legacy key produced a duplicate")
		}
	})
}

func TestRecordingRuleForecastReconcilesConcurrentMaterialization(t *testing.T) {
	f := newForecastFixture(t)
	job := f.prior(models.RecordingStatusPending)
	f.repo.historyExtra = []models.Recording{job}
	plan := f.plan(t, f.candidate())
	if len(plan.ProjectedRecordings) != 0 || len(plan.Recordings) != 1 || plan.Recordings[0].RecordingID != job.ID || plan.Candidate.PeakScheduledConnections != 2 {
		t.Fatalf("later materialization vanished from demand: %+v", plan)
	}
}

func TestRecordingRuleForecastReconcilesLaterTerminalSnapshot(t *testing.T) {
	f := newForecastFixture(t)
	job := f.prior(models.RecordingStatusPending)
	f.repo.recordings[job.ID] = job
	job.Status = models.RecordingStatusCancelled
	f.repo.historyOverride = []models.Recording{job}
	plan := f.plan(t, f.candidate())
	if len(plan.Recordings) != 0 || len(plan.ProjectedRecordings) != 0 || plan.Candidate.State != "clear" {
		t.Fatalf("later terminal snapshot ignored: %+v", plan)
	}
}

func TestRecordingRuleForecastGuideGapsAreScopedAndPadded(t *testing.T) {
	for _, tc := range []struct {
		name         string
		minute       int
		source, want string
	}{{"covered", 10, "provider-a", "clear"}, {"gap", 40, "provider-a", "unknown"}, {"padding expands gap", 25, "provider-a", "unknown"}, {"other provider", 40, "provider-b", "clear"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForecastFixture(t)
			rule := f.rules.rules["own-rule"]
			rule.PaddingBeforeSeconds = 600
			f.rules.rules[rule.ID] = rule
			programs := f.epg.programs["guide.one"]
			programs[0].Stop = f.now.Add(30 * time.Minute)
			f.epg.programs["guide.one"] = programs
			candidate := f.candidate()
			candidate.StartAt = f.now.Add(time.Duration(tc.minute) * time.Minute)
			candidate.EndAt = candidate.StartAt.Add(time.Minute)
			candidate.SourceURL = tc.source
			plan := f.plan(t, candidate)
			if plan.RuleForecast.Status != "partial" || plan.Complete || plan.Candidate.State != tc.want {
				t.Fatalf("gap scope wrong: %+v", plan)
			}
		})
	}
}

func TestRecordingRuleForecastUnknownFutureBeyondHorizon(t *testing.T) {
	f := newForecastFixture(t)
	candidate := f.candidate()
	candidate.StartAt = f.now.Add(8 * 24 * time.Hour)
	candidate.EndAt = candidate.StartAt.Add(time.Hour)
	plan := f.plan(t, candidate)
	if plan.RuleForecast.Status != "available" || plan.Candidate.State != "unknown" || plan.Complete {
		t.Fatalf("finite guide promised a later free slot: %+v", plan)
	}
}

func TestRecordingRuleForecastMissingGuideIsNotNoMatches(t *testing.T) {
	for _, mode := range []string{"missing guide", "missing epg provider", "missing channel", "catalog error", "unresolved provider", "invalid stored regex", "invalid padding", "invalid programme"} {
		t.Run(mode, func(t *testing.T) {
			f := newForecastFixture(t)
			switch mode {
			case "missing guide":
				f.epg.programs = nil
			case "missing epg provider":
				f.service.epg = nil
			case "missing channel":
				f.channels.byProfile["viewer"] = nil
			case "catalog error":
				f.channels.fail = true
			case "unresolved provider":
				f.channels.byProfile["viewer"][0].SourceURL = "unresolved"
			case "invalid stored regex":
				rule := f.rules.rules["own-rule"]
				rule.MatchType = models.RecordingRuleMatchRegex
				rule.Pattern = "["
				f.rules.rules[rule.ID] = rule
			case "invalid padding":
				rule := f.rules.rules["own-rule"]
				rule.PaddingAfterSeconds = 3601
				f.rules.rules[rule.ID] = rule
			case "invalid programme":
				f.epg.programs["guide.one"][1].Start = time.Time{}
			}
			plan := f.plan(t, f.candidate())
			if plan.Complete || plan.Candidate.State == "clear" || plan.RuleForecast.Status == "available" {
				t.Fatalf("unknown guide claimed clear: %+v", plan)
			}
		})
	}
}

func TestRecordingRuleForecastRuleMatchingAndNewOnlyMirrorScheduler(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, title                 string
		regex, newOnly, new, rerun, otherNew bool
		matches                              bool
	}{
		{"normalized title", "HÄNNES & Co", "hännes co", false, false, false, false, false, true},
		{"regex defaults insensitive", "show.*", "SHOW episode", true, false, false, false, false, true},
		{"regex explicit sensitive", "(?-i)show.*", "SHOW episode", true, false, false, false, false, false},
		{"new episode", "Show", "Show", false, true, true, false, false, true},
		{"rerun", "Show", "Show", false, true, false, true, false, false},
		{"unmarked while guide marks new", "Show", "Show", false, true, false, false, true, false},
		{"unflagged guide preserves scheduler fallback", "Show", "Show", false, true, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForecastFixture(t)
			rule := f.rules.rules["own-rule"]
			rule.Pattern = tc.pattern
			rule.NewOnly = tc.newOnly
			if tc.regex {
				rule.MatchType = models.RecordingRuleMatchRegex
			}
			f.rules.rules[rule.ID] = rule
			f.epg.programs["guide.one"][1].Title = tc.title
			f.epg.programs["guide.one"][1].New = tc.new
			f.epg.programs["guide.one"][1].PreviouslyShown = tc.rerun
			f.epg.programs["guide.one"][0].New = tc.otherNew
			plan := f.plan(t, nil)
			if (len(plan.ProjectedRecordings) == 1) != tc.matches {
				t.Fatalf("matching drifted from scheduler: %+v", plan)
			}
		})
	}
}

func TestRecordingRuleForecastAppliesGuideOffsetOnce(t *testing.T) {
	f := newForecastFixture(t)
	f.service.epgoffset = func(string) time.Duration { return 90 * time.Minute }
	plan := f.plan(t, nil)
	if len(plan.ProjectedRecordings) != 1 || !plan.ProjectedRecordings[0].Start.Equal(f.now.Add(150*time.Minute)) {
		t.Fatalf("wrong offset: %+v", plan)
	}
	if delta := time.Since(f.epg.start.Add(90 * time.Minute)); delta < 0 || delta > 5*time.Second {
		t.Fatalf("cache query did not subtract offset: %v", delta)
	}
}

func TestRecordingRuleForecastDSTUsesAbsoluteInstantForWindowAndDedupKey(t *testing.T) {
	f := newForecastFixture(t)
	now := time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC)
	start, err := time.Parse(time.RFC3339, "2026-10-25T02:45:00+02:00")
	if err != nil {
		t.Fatal(err)
	}
	stop, err := time.Parse(time.RFC3339, "2026-10-25T02:15:00+01:00")
	if err != nil {
		t.Fatal(err)
	}
	f.epg.programs["guide.one"] = []models.EPGProgram{
		{Title: "Before", Start: now.Add(-time.Hour), Stop: start},
		{Title: "Show", Start: start, Stop: stop},
		{Title: "After", Start: stop, Stop: now.Add(8 * 24 * time.Hour)},
	}
	projection := f.service.projectRecordingRules(context.Background(), "viewer", now, nil, func(job models.Recording) (RecordingCapacity, bool) {
		return RecordingCapacity{Key: job.SourceURL, Limit: 1}, true
	})
	if len(projection.jobs) != 1 {
		t.Fatalf("DST airing disappeared: %+v", projection.metadata)
	}
	job := projection.jobs[0]
	if !job.StartAt.Equal(time.Date(2026, 10, 25, 0, 45, 0, 0, time.UTC)) || !job.EndAt.Equal(time.Date(2026, 10, 25, 1, 15, 0, 0, time.UTC)) || job.ScheduleKey != "guide.one:2026-10-25T00:45:00Z" {
		t.Fatalf("local-clock ordering or key was used: %+v", job)
	}
}

func TestRecordingRuleForecastDuplicateGuideEntryUsesOneAiring(t *testing.T) {
	f := newForecastFixture(t)
	programme := f.epg.programs["guide.one"][1]
	programme.Stop = programme.Stop.Add(10 * time.Minute)
	f.epg.programs["guide.one"] = append(f.epg.programs["guide.one"], programme)
	plan := f.plan(t, nil)
	if len(plan.ProjectedRecordings) != 1 || !plan.ProjectedRecordings[0].End.Equal(programme.Stop) {
		t.Fatalf("duplicate guide key was counted twice: %+v", plan)
	}
}

func TestRecordingRuleForecastDisabledRulesDoNotCreateUncertainty(t *testing.T) {
	f := newForecastFixture(t)
	rule := f.rules.rules["own-rule"]
	rule.Enabled = false
	f.rules.rules[rule.ID] = rule
	plan := f.plan(t, f.candidate())
	if plan.RuleForecast.Status != "not_configured" || !plan.Complete || plan.Candidate.State != "clear" || f.epg.calls != 0 {
		t.Fatalf("disabled rule affected plan: %+v", plan)
	}
	f.service.rules = nil
	plan = f.plan(t, f.candidate())
	if plan.RuleForecast != nil || plan.Candidate.State != "clear" {
		t.Fatal("absent rule service changed legacy plan")
	}
}

func TestRecordingRuleForecastBoundsRemainConservative(t *testing.T) {
	for _, mode := range []string{"rule count", "channel count", "program count", "history count", "history error", "invalid legacy key"} {
		t.Run(mode, func(t *testing.T) {
			f := newForecastFixture(t)
			switch mode {
			case "rule count":
				base := f.rules.rules["own-rule"]
				f.rules.rules = map[string]models.RecordingRule{}
				for i := 0; i <= planningRuleLimit; i++ {
					r := base
					r.ID = fmt.Sprintf("r%03d", i)
					r.Pattern = "Does not match"
					f.rules.rules[r.ID] = r
				}
			case "channel count":
				for i := 0; i < planningRuleChannelLimit; i++ {
					f.channels.byProfile["viewer"] = append(f.channels.byProfile["viewer"], Channel{ID: fmt.Sprint(i), TvgID: fmt.Sprint(i), SourceURL: "provider-a"})
				}
			case "program count":
				p := f.epg.programs["guide.one"][1]
				f.epg.programs["guide.one"] = make([]models.EPGProgram, planningRuleProgramLimit+1)
				for i := range f.epg.programs["guide.one"] {
					f.epg.programs["guide.one"][i] = p
				}
			case "history count":
				for i := 0; i <= planningJobLimit; i++ {
					j := f.prior(models.RecordingStatusCancelled)
					j.ID = fmt.Sprint(i)
					f.repo.recordings[j.ID] = j
				}
			case "history error":
				f.repo.failHistory = true
			case "invalid legacy key":
				j := f.prior(models.RecordingStatusCancelled)
				j.ScheduleKey = ""
				j.TvgID = ""
				f.repo.recordings[j.ID] = j
			}
			plan := f.plan(t, f.candidate())
			if plan.Candidate.State == "clear" || plan.Complete || plan.RuleForecast.Status == "available" {
				t.Fatalf("bound or missing history promised clear: %+v", plan)
			}
		})
	}
}

func TestRecordingRuleForecastMatchBoundDoesNotInventMoreJobs(t *testing.T) {
	f := newForecastFixture(t)
	programs := []models.EPGProgram{{Title: "Before", Start: f.now.Add(-time.Hour), Stop: f.now.Add(time.Minute)}}
	for i := 0; i <= planningRuleMatchLimit; i++ {
		start := f.now.Add(time.Duration(i+1) * time.Minute)
		programs = append(programs, models.EPGProgram{Title: "Show", Start: start, Stop: start.Add(time.Minute)})
	}
	programs = append(programs, models.EPGProgram{Title: "After", Start: f.now.Add(time.Duration(planningRuleMatchLimit+2) * time.Minute), Stop: f.now.Add(8 * 24 * time.Hour)})
	f.epg.programs["guide.one"] = programs
	plan := f.plan(t, nil)
	if len(plan.ProjectedRecordings) != planningRuleMatchLimit || plan.RuleForecast.ProjectedCount != planningRuleMatchLimit || plan.RuleForecast.Status != "partial" || plan.Complete {
		t.Fatalf("match limit not conservative: count=%d metadata=%+v", len(plan.ProjectedRecordings), plan.RuleForecast)
	}
}
