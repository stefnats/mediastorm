package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/models"
	"novastream/services/recordings"
)

type planningTestRepo struct {
	mu     sync.Mutex
	jobs   map[string]models.Recording
	writes int
}

func (p *planningTestRepo) Get(_ context.Context, id string) (*models.Recording, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j, ok := p.jobs[id]
	if !ok {
		return nil, nil
	}
	return &j, nil
}
func (p *planningTestRepo) List(ctx context.Context, f models.RecordingListFilter) ([]models.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []models.Recording{}
	for _, j := range p.jobs {
		if !f.IncludeAll && j.UserID != f.UserID {
			continue
		}
		matches := len(f.Statuses) == 0
		for _, status := range f.Statuses {
			matches = matches || j.Status == status
		}
		if matches {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}
func (p *planningTestRepo) Create(_ context.Context, j *models.Recording) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.jobs[j.ID] = *j
	p.writes++
	return nil
}
func (p *planningTestRepo) Update(ctx context.Context, j *models.Recording) error {
	return p.Create(ctx, j)
}
func (p *planningTestRepo) Delete(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.jobs, id)
	p.writes++
	return nil
}
func (p *planningTestRepo) Count(context.Context) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.jobs)), nil
}
func (p *planningTestRepo) MarkStaleActiveAsFailed(context.Context, time.Time) (int64, error) {
	panic("planner must not start scheduler")
}

type planningTestChannels struct{ fixedAccountChannels }

func (p planningTestChannels) RecordingChannels(context.Context, string) ([]recordings.Channel, error) {
	panic("forecast must not open or enumerate provider inputs")
}
func (p planningTestChannels) PlanRecordingCapacity(_ context.Context, j models.Recording) (recordings.RecordingCapacity, error) {
	if strings.Contains(j.SourceURL, "unknown") {
		return recordings.RecordingCapacity{}, errors.New("synthetic-private-error")
	}
	return recordings.RecordingCapacity{Key: j.SourceURL, Limit: 1}, nil
}

func recordingPlanningFixture(t *testing.T) (*RecordingsHandler, *planningTestRepo) {
	t.Helper()
	now := time.Now().UTC().Add(2 * time.Hour)
	repo := &planningTestRepo{jobs: map[string]models.Recording{}}
	for id, profile := range map[string]string{"own": "viewer", "synthetic-private-id": "foreign"} {
		repo.jobs[id] = models.Recording{ID: id, UserID: profile, ChannelID: "visible", ChannelName: "Synthetic channel", Title: "synthetic-private-title", SourceURL: "https://fixture.example/shared", Status: models.RecordingStatusPending, StartAt: now, EndAt: now.Add(time.Hour)}
	}
	channels := planningTestChannels{fixedAccountChannels{channels: []LiveChannel{{ID: "visible", Name: "Synthetic channel", URL: "https://fixture.example/shared"}, {ID: "separate", Name: "Separate provider", URL: "https://fixture.example/separate"}}}}
	service := recordings.NewService(repo, "must-not-execute", t.TempDir())
	service.SetRuleProviders(nil, channels, nil)
	profiles := &fakeRecordingUsersProvider{users: []models.User{{ID: "viewer", AccountID: "household"}, {ID: "other-viewer", AccountID: "household"}, {ID: "foreign", AccountID: "other"}, {ID: "kid", AccountID: "household", IsKidsProfile: true}}}
	h := NewRecordingsHandler(service, profiles)
	h.liveChannels = channels
	return h, repo
}

func planningRequest(method, path, body string, master bool) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	ctx := context.WithValue(r.Context(), auth.ContextKeyAccountID, "household")
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, master)
	return r.WithContext(ctx)
}

func TestRecordingPlanningHTTPPrivacyAndOwnership(t *testing.T) {
	h, repo := recordingPlanningFixture(t)
	for _, tc := range []struct {
		id     string
		master bool
		status int
	}{{"viewer", false, 200}, {"foreign", false, 404}, {"kid", false, 403}, {"missing", false, 404}, {"", false, 400}, {"foreign", true, 200}, {"kid", true, 403}} {
		t.Run(tc.id+"/"+map[bool]string{true: "master", false: "account"}[tc.master], func(t *testing.T) {
			w := httptest.NewRecorder()
			h.Plan(w, planningRequest("GET", "/api/live/recordings/plan?profileId="+tc.id, "", tc.master))
			if w.Code != tc.status {
				t.Fatalf("status %d %s", w.Code, w.Body.String())
			}
			for _, secret := range []string{"synthetic-private-title", "https://fixture", "SourceURL", "UserID"} {
				if strings.Contains(w.Body.String(), secret) {
					t.Fatal("plan disclosed recording metadata")
				}
			}
			if tc.id == "viewer" && strings.Contains(w.Body.String(), "synthetic-private-id") {
				t.Fatal("foreign id disclosed")
			}
		})
	}
	if repo.writes != 0 {
		t.Fatal("forecast wrote repository")
	}
}

func TestRecordingPlanningRejectsScopedSessions(t *testing.T) {
	h, repo := recordingPlanningFixture(t)
	r := planningRequest("GET", "/api/live/recordings/plan?profileId=viewer", "", true)
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
	w := httptest.NewRecorder()
	h.Plan(w, r)
	if w.Code != http.StatusForbidden || repo.writes != 0 {
		t.Fatal("scoped session reached planning")
	}
}

func previewBody(t *testing.T, channel string) string {
	t.Helper()
	now := time.Now().UTC().Add(2*time.Hour + time.Minute)
	body, _ := json.Marshal(map[string]any{"profileId": "viewer", "channelId": channel, "sourceUrl": "https://attacker.invalid/ignore", "start": now.Format(time.RFC3339), "stop": now.Add(30 * time.Minute).Format(time.RFC3339), "paddingBeforeSeconds": 30})
	return string(body)
}

func TestRecordingPlanningPreviewUsesCanonicalChannelAndDoesNotSchedule(t *testing.T) {
	h, repo := recordingPlanningFixture(t)
	for channel, want := range map[string]string{"visible": "conflict", "separate": "clear"} {
		w := httptest.NewRecorder()
		h.Preview(w, planningRequest("POST", "/api/live/recordings/preview", previewBody(t, channel), false))
		if w.Code != 200 {
			t.Fatalf("preview: %d %s", w.Code, w.Body.String())
		}
		var plan models.RecordingPlan
		_ = json.Unmarshal(w.Body.Bytes(), &plan)
		if plan.Candidate == nil || plan.Candidate.State != want {
			t.Fatalf("caller URL overrode canonical source: %+v", plan)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("private forecast cacheable")
		}
	}
	if repo.writes != 0 || len(repo.jobs) != 2 {
		t.Fatal("preview changed schedule")
	}
}

func TestRecordingPlanningPreviewRejectsMalformedAndUnauthorizedRequests(t *testing.T) {
	h, repo := recordingPlanningFixture(t)
	valid := previewBody(t, "visible")
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"empty", "{}", 400}, {"trailing", valid + "{}", 400}, {"unknown field", strings.Replace(valid, "{", "{\"limit\":99,", 1), 400}, {"oversized", `{"profileId":"` + strings.Repeat("x", 17000) + `"}`, 400},
		{"foreign", strings.Replace(valid, "viewer", "foreign", 1), 404}, {"kid", strings.Replace(valid, "viewer", "kid", 1), 403}, {"unknown channel", strings.Replace(valid, "visible", "missing", 1), 404},
		{"negative padding", strings.Replace(valid, "\"paddingBeforeSeconds\":30", "\"paddingBeforeSeconds\":-1", 1), 400}, {"too large padding", strings.Replace(valid, "\"paddingBeforeSeconds\":30", "\"paddingBeforeSeconds\":3601", 1), 400},
		{"bad time", strings.Replace(valid, "\"start\":\"", "\"start\":\"invalid", 1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.Preview(w, planningRequest("POST", "/api/live/recordings/preview", tc.body, false))
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if repo.writes != 0 {
		t.Fatal("rejected preview wrote schedule")
	}
}

func TestRecordingPlanningLiveTargetMatchesAdmissionAndDoesNotReserve(t *testing.T) {
	accounts := []string{"household"}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.Settings{Live: config.LiveSettings{RequireAccountAssignment: true, Sources: []config.LivePlaylistSource{
		{ID: "source", Mode: "xtream", XtreamHost: "https://synthetic-planning.example", XtreamUsername: "synthetic-user", XtreamPassword: "synthetic-password", MaxStreams: 3, AllowedAccounts: &accounts},
		{ID: "alias", Mode: "m3u", PlaylistURL: "https://synthetic-planning.example/get.php?username=synthetic-user&password=synthetic-password", MaxStreams: 1, AllowedAccounts: &accounts},
	}}}
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	h := &LiveHandler{cfgManager: manager, usersSvc: liveAccountUsers{"viewer": {ID: "viewer", AccountID: "household"}, "kid": {ID: "kid", AccountID: "household", IsKidsProfile: true}, "foreign": {ID: "foreign", AccountID: "other"}}}
	// Hide the second source from the profile. Its stricter alias still caps the budget.
	settings.Live.Sources[1].AllowedProfiles = []string{"another-profile"}
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	job := models.Recording{UserID: "viewer", SourceURL: "https://synthetic-planning.example/live/input.ts"}
	target, err := h.PlanRecordingCapacity(context.Background(), job)
	if err != nil || target.Limit != 1 {
		t.Fatalf("target: %+v %v", target, err)
	}
	if livecapacity.Default.Current(target.Key) != 0 {
		t.Fatal("forecast reserved capacity")
	}
	for _, id := range []string{"kid", "foreign", "missing"} {
		job.UserID = id
		if _, err := h.PlanRecordingCapacity(context.Background(), job); err == nil {
			t.Fatalf("unauthorized target accepted: %s", id)
		}
	}
}
