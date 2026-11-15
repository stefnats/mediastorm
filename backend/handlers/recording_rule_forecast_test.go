package handlers

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/recordings"
)

type forecastHTTPRules struct{ rules []models.RecordingRule }

func (p forecastHTTPRules) ListRules(ctx context.Context, _ string, _, enabled bool) ([]models.RecordingRule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := []models.RecordingRule{}
	for _, r := range p.rules {
		if !enabled || r.Enabled {
			out = append(out, r)
		}
	}
	return out, nil
}
func (forecastHTTPRules) GetRule(context.Context, string) (*models.RecordingRule, error) {
	panic("forecast must not get a rule for mutation")
}
func (forecastHTTPRules) CreateRule(context.Context, *models.RecordingRule) error {
	panic("forecast wrote a rule")
}
func (forecastHTTPRules) UpdateRule(context.Context, *models.RecordingRule) error {
	panic("forecast updated a rule")
}
func (forecastHTTPRules) DeleteRule(context.Context, string) error { panic("forecast deleted a rule") }
func (forecastHTTPRules) CancelPendingRuleRecordings(context.Context, string, time.Time) error {
	panic("forecast cancelled jobs")
}
func (forecastHTTPRules) PausePendingRuleRecordings(context.Context, string, time.Time) error {
	panic("forecast paused jobs")
}
func (forecastHTTPRules) UpdatePendingRuleRecordingBuffers(context.Context, string, int, int) error {
	panic("forecast changed padding")
}
func (forecastHTTPRules) CreateScheduledRecordingIfAbsent(context.Context, *models.Recording) (bool, error) {
	panic("forecast materialized a job")
}

type forecastHTTPGuide struct{ programs []models.EPGProgram }

func (p forecastHTTPGuide) GetScheduleMultiple(ids []string, start, end time.Time) map[string][]models.EPGProgram {
	out := map[string][]models.EPGProgram{}
	for _, id := range ids {
		if id != "visible.epg" {
			continue
		}
		for _, program := range p.programs {
			if program.Stop.After(start) && program.Start.Before(end) {
				out[id] = append(out[id], program)
			}
		}
	}
	return out
}

type forecastHTTPChannels struct{ planningTestChannels }

func (p forecastHTTPChannels) RecordingChannels(context.Context, string) ([]recordings.Channel, error) {
	return []recordings.Channel{{ID: "visible", TvgID: "visible.epg", Name: "Synthetic channel", SourceURL: "https://fixture.example/shared"}}, nil
}

func enableHTTPRuleForecast(t *testing.T, h *RecordingsHandler, repo *planningTestRepo, profile string) {
	t.Helper()
	now := time.Now().UTC()
	job := repo.jobs["own"]
	program := models.EPGProgram{Title: "Synthetic upcoming programme", Start: job.StartAt, Stop: job.EndAt}
	guide := forecastHTTPGuide{programs: []models.EPGProgram{{Title: "Before", Start: now.Add(-time.Hour), Stop: program.Start}, program, {Title: "After", Start: program.Stop, Stop: now.Add(8 * 24 * time.Hour)}}}
	rules := forecastHTTPRules{rules: []models.RecordingRule{
		{ID: "own-rule", UserID: profile, Enabled: true, MatchType: models.RecordingRuleMatchTitle, Pattern: program.Title, ChannelID: "visible"},
		{ID: "synthetic-private-foreign-rule", UserID: "foreign", Enabled: true, MatchType: models.RecordingRuleMatchTitle, Pattern: program.Title, ChannelID: "visible"},
	}}
	service := h.service.(*recordings.Service)
	service.SetRuleRepository(rules)
	service.SetRuleProviders(guide, forecastHTTPChannels{planningTestChannels{fixedAccountChannels{channels: []LiveChannel{{ID: "visible", TvgID: "visible.epg", Name: "Synthetic channel", URL: "https://fixture.example/shared"}}}}}, nil)
}

func ruleForecastHTTPFixture(t *testing.T) (*RecordingsHandler, *planningTestRepo) {
	t.Helper()
	h, repo := recordingPlanningFixture(t)
	enableHTTPRuleForecast(t, h, repo, "viewer")
	return h, repo
}

func TestRecordingRuleForecastHTTPIncludesOwnProjectionsWithoutPersisting(t *testing.T) {
	h, repo := ruleForecastHTTPFixture(t)
	for _, path := range []string{"/api/live/recordings/plan", "/admin/api/live/recordings/plan", "/account/api/live/recordings/plan"} {
		w := httptest.NewRecorder()
		h.Plan(w, planningRequest("GET", path+"?profileId=viewer", "", false))
		if w.Code != 200 {
			t.Fatalf("plan %d %s", w.Code, w.Body.String())
		}
		var plan models.RecordingPlan
		if err := json.Unmarshal(w.Body.Bytes(), &plan); err != nil {
			t.Fatal(err)
		}
		if plan.Candidate != nil || len(plan.Recordings) != 1 || len(plan.ProjectedRecordings) != 1 || plan.RuleForecast == nil || plan.RuleForecast.ProjectedCount != 1 {
			t.Fatalf("wrong scoped forecast: %+v", plan)
		}
		projected := plan.ProjectedRecordings[0]
		if projected.RuleID != "own-rule" || projected.State != "conflict" || projected.PeakScheduledConnections != 4 || projected.RecordingID != "" {
			t.Fatalf("projection lost shared demand: %+v", projected)
		}
		for _, marker := range []string{"synthetic-private-foreign-rule", "synthetic-private-id", "https://fixture.example", "sourceUrl", "userId"} {
			if strings.Contains(w.Body.String(), marker) {
				t.Fatalf("forecast leaked %s", marker)
			}
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("forecast response cacheable")
		}
	}
	if repo.writes != 0 || len(repo.jobs) != 2 {
		t.Fatal("forecast wrote jobs")
	}
}

func TestRecordingRuleForecastPreviewCountsProjectedInputs(t *testing.T) {
	h, repo := ruleForecastHTTPFixture(t)
	w := httptest.NewRecorder()
	h.Preview(w, planningRequest("POST", "/api/live/recordings/preview", previewBody(t, "visible"), false))
	var plan models.RecordingPlan
	_ = json.Unmarshal(w.Body.Bytes(), &plan)
	if w.Code != 200 || plan.Candidate == nil || plan.Candidate.State != "conflict" || plan.Candidate.PeakScheduledConnections != 5 {
		t.Fatalf("preview omitted projected demand: %d %+v", w.Code, plan)
	}
	if repo.writes != 0 {
		t.Fatal("preview materialized a rule")
	}
}
