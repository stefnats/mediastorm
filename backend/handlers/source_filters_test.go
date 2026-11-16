package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/content_preferences"
	"novastream/services/debrid"
	"novastream/services/indexer"
	"novastream/services/mediaevidence"
	"novastream/services/sourcefilters"
	"novastream/services/sourcehealth"
	"novastream/services/trackselect"
)

type filterConfig struct {
	mu       sync.Mutex
	settings config.Settings
}

func (c *filterConfig) Load() (config.Settings, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.settings, nil
}
func (c *filterConfig) change() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings.Playback.PreferredAudioLanguage = "fra"
}

type filterSearch struct {
	fakeIndexerService
	calls int
}

func (s *filterSearch) SearchWithScoring(ctx context.Context, opts indexer.SearchOptions) ([]models.ScoredNZBResult, error) {
	s.calls++
	return s.fakeIndexerService.SearchWithScoring(ctx, opts)
}
func sourceFilterFixture(t *testing.T, count int) (*IndexerHandler, *filterSearch, *filterConfig) {
	t.Helper()
	svc := &filterSearch{}
	at := time.Now().UTC().Add(-time.Second)
	for i := 0; i < count; i++ {
		r := models.NZBResult{Title: fmt.Sprintf("Synthetic.Movie.2024.1080p.%d", i), GUID: "synthetic-" + strconv.Itoa(i), Indexer: "Synthetic", ServiceType: models.ServiceTypeDebrid, Attributes: map[string]string{"languages": "en", "resolution": "1080p"}}
		if i == count-1 {
			r.Attributes["languages"] = "fr"
		}
		mediaevidence.ObserveCandidate(&r, at)
		svc.results = append(svc.results, r)
	}
	cfg := &filterConfig{settings: config.DefaultSettings()}
	h := NewIndexerHandler(svc, false)
	h.TrackPreferences = &trackselect.Resolver{Config: cfg}
	h.ConfigureSourceFilters(func(id string) (models.User, bool) {
		if id != "profile" {
			return models.User{}, false
		}
		return models.User{ID: id, AccountID: "account"}, true
	}, nil)
	return h, svc, cfg
}
func filterRequest(method, target string, body []byte) *http.Request {
	r := httptest.NewRequest(method, target, bytes.NewReader(body))
	r.Header.Set("X-Client-ID", "device")
	r.Header.Set("Content-Type", "application/json")
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
}
func initialFilterPage(t *testing.T, h *IndexerHandler) sourcefilters.Page {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Search(rec, filterRequest("GET", "/api/indexers/search?q=Synthetic+Movie&titleId=tmdb:movie:41&mediaType=movie&year=2024&userId=profile&limit=50&sourceFiltersVersion=1", nil))
	if rec.Code != 200 {
		t.Fatalf("initial %d: %s", rec.Code, rec.Body.String())
	}
	var page sourcefilters.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}
func filterPageRequest(h *IndexerHandler, id string, body any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	r := filterRequest("POST", "/api/users/profile/source-searches/"+id+"/page", data)
	r = mux.SetURLVars(r, map[string]string{"userID": "profile", "snapshotID": id})
	rec := httptest.NewRecorder()
	h.SourceSearchPage(rec, r)
	return rec
}

func TestSourceFilterNegotiationRetainsLateMatchWithoutSearchingOnApply(t *testing.T) {
	h, svc, _ := sourceFilterFixture(t, 140)
	first := initialFilterPage(t, h)
	if len(first.Rows) != 50 || first.Total != 140 || first.Rows[49].SourceIndex != 49 || svc.calls != 1 || !svc.lastOpts.RetainForSourceFilters || svc.lastOpts.MaxResults != 0 {
		t.Fatal("negotiation did not retain pre-cap pool")
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(first.Rows[0].Result, &raw)
	if _, present := raw["filterStatus"]; present {
		t.Fatal("plain result was replaced by scoring DTO")
	}
	rec := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": sourcefilters.Filters{Basis: "candidate", Unknown: "exclude", AudioLanguage: "fra"}, "limit": 50})
	var page sourcefilters.Page
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if rec.Code != 200 || page.Total != 1 || page.MatchedTotal != 1 || page.UnknownTotal != 139 || page.Rows[0].SourceIndex != 139 || svc.calls != 1 {
		t.Fatalf("cache-only late match: %d %+v", rec.Code, page)
	}
	legacy := httptest.NewRecorder()
	h.Search(legacy, filterRequest("GET", "/api/indexers/search?q=Synthetic&userId=profile", nil))
	if !bytes.HasPrefix(legacy.Body.Bytes(), []byte("[")) {
		t.Fatal("legacy array compatibility lost")
	}
}

func TestSourceFilterContextAndOwnedProfileResourceGuards(t *testing.T) {
	h, svc, cfg := sourceFilterFixture(t, 2)
	first := initialFilterPage(t, h)
	body := map[string]any{"version": 1, "filters": sourcefilters.Filters{Basis: "candidate", Unknown: "include"}, "limit": 50}
	cfg.change()
	rec := filterPageRequest(h, first.SnapshotID, body)
	if rec.Code != 409 || !bytes.Contains(rec.Body.Bytes(), []byte("source_snapshot_context_changed")) || svc.calls != 1 {
		t.Fatal("changed settings triggered search or reused snapshot")
	}
	for _, test := range []struct {
		name   string
		change func(*http.Request) *http.Request
		want   int
	}{
		{"foreign account", func(r *http.Request) *http.Request {
			return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "other"))
		}, 404},
		{"scoped stream", func(r *http.Request) *http.Request {
			return r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
		}, 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, _ := json.Marshal(body)
			r := mux.SetURLVars(filterRequest("POST", "/api/users/profile/source-searches/"+first.SnapshotID+"/page", data), map[string]string{"userID": "profile", "snapshotID": first.SnapshotID})
			rec := httptest.NewRecorder()
			h.SourceSearchPage(rec, test.change(r))
			if rec.Code != test.want {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
	data, _ := json.Marshal(body)
	r := mux.SetURLVars(filterRequest("POST", "/api/users/profile/source-searches/"+first.SnapshotID+"/page", data), map[string]string{"userID": "profile", "snapshotID": first.SnapshotID})
	rec = httptest.NewRecorder()
	profileaccess.Middleware(func(string, string) bool { return true })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, r *http.Request, id string) bool {
			if id != "profile" {
				t.Fatal("wrong stored resource profile")
			}
			http.Error(w, "locked", 403)
			return false
		})
		h.SourceSearchPage(w, r)
	})).ServeHTTP(rec, r)
	if rec.Code != 403 {
		t.Fatal("PIN resource guard bypassed")
	}
}

type scopedDebridProbe struct {
	before   func(models.NZBResult)
	response *debrid.DebridHealthCheck
	last     models.NZBResult
	calls    int
}

func (s *scopedDebridProbe) CheckHealthQuick(_ context.Context, result models.NZBResult) (*debrid.DebridHealthCheck, error) {
	s.last = result
	s.calls++
	if s.before != nil {
		s.before(result)
	}
	return s.response, nil
}
func (s *scopedDebridProbe) CheckHealthFull(ctx context.Context, r models.NZBResult) (*debrid.DebridHealthCheck, error) {
	return s.CheckHealthQuick(ctx, r)
}
func (s *scopedDebridProbe) CheckQuickCacheOnly(ctx context.Context, r models.NZBResult) (*debrid.DebridHealthCheck, error) {
	return s.CheckHealthQuick(ctx, r)
}
func (s *scopedDebridProbe) CheckQuickCacheOnlyBulk(context.Context, []models.NZBResult) ([]*debrid.DebridHealthCheck, error) {
	return nil, nil
}

func TestSourceFilterScopedProbeUsesServerRowAndRetiresOnlyOldView(t *testing.T) {
	h, _, cfg := sourceFilterFixture(t, 3)
	first := initialFilterPage(t, h)
	before := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": first.Filters, "limit": 1})
	var frozen sourcefilters.Page
	_ = json.Unmarshal(before.Body.Bytes(), &frozen)
	health := &scopedDebridProbe{}
	probe := NewDebridHandler(nil, health)
	probe.SourceFilters = h
	health.before = func(row models.NZBResult) {
		health.response = &debrid.DebridHealthCheck{Status: "cached", Healthy: true, MediaEvidence: mediaevidence.Probed(row, "synthetic-exact-file", "", 100, evidenceFacts(time.Now()), "", nil, time.Now())}
	}
	// Return the current response built inside the fake provider callback.
	health.response = &debrid.DebridHealthCheck{Status: "cached"}
	data, _ := json.Marshal(map[string]any{"snapshotId": first.SnapshotID, "sourceIndex": 2, "profileId": "profile", "verifyUncached": true, "result": models.NZBResult{GUID: "FORGED_CLIENT_ROW", ServiceType: models.ServiceTypeDebrid}})
	rec := httptest.NewRecorder()
	probe.CheckCached(rec, filterRequest("POST", "/api/debrid/cached", data))
	if rec.Code != 200 || health.last.GUID != "synthetic-2" {
		t.Fatalf("server row not used: %d %s %+v", rec.Code, rec.Body.String(), health.last)
	}
	old := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": first.Filters, "cursor": frozen.NextCursor, "limit": 1})
	if old.Code != 409 || !bytes.Contains(old.Body.Bytes(), []byte("source_view_changed")) {
		t.Fatalf("old view silently changed after F2: %d %s", old.Code, old.Body.String())
	}
	file := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": sourcefilters.Filters{Basis: "probed", Unknown: "exclude", AudioLanguage: "fra"}, "limit": 50})
	var page sourcefilters.Page
	_ = json.Unmarshal(file.Body.Bytes(), &page)
	if file.Code != 200 || page.MatchedTotal != 1 || page.Rows[0].SourceIndex != 2 {
		t.Fatalf("file facts not linked: %d %s", file.Code, file.Body.String())
	}
	health.before = func(models.NZBResult) { cfg.change() }
	rec = httptest.NewRecorder()
	probe.CheckCached(rec, filterRequest("POST", "/api/debrid/cached", data))
	if rec.Code != 409 || !bytes.Contains(rec.Body.Bytes(), []byte("source_snapshot_context_changed")) {
		t.Fatalf("post-probe context not checked: %d %s", rec.Code, rec.Body.String())
	}
	previous := health.calls
	rec = httptest.NewRecorder()
	probe.CheckCached(rec, filterRequest("POST", "/api/debrid/cached", data))
	if rec.Code != 409 || health.calls != previous {
		t.Fatal("stale scoped probe reached provider")
	}
}

func TestSourceFilterChildPreferenceChangesInvalidateCurrentSnapshot(t *testing.T) {
	h, svc, _ := sourceFilterFixture(t, 2)
	prefs, err := content_preferences.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.TrackPreferences.ContentPreferences = prefs
	rec := httptest.NewRecorder()
	h.Search(rec, filterRequest("GET", "/api/indexers/search?q=Synthetic+S02E03&titleId=tmdb:series:41&mediaType=series&userId=profile&sourceFiltersVersion=1", nil))
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	var first sourcefilters.Page
	_ = json.Unmarshal(rec.Body.Bytes(), &first)
	if err = prefs.Set("profile", models.ContentPreference{ContentID: "tmdb:series:41:season:2", ContentType: "series", PreferLocalFiles: models.BoolPtr(false)}); err != nil {
		t.Fatal(err)
	}
	page := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": first.Filters, "limit": 50})
	if page.Code != 409 || !bytes.Contains(page.Body.Bytes(), []byte("source_snapshot_context_changed")) || svc.calls != 1 {
		t.Fatalf("child preference not scoped: %d %s", page.Code, page.Body.String())
	}
}

type filterHealthBudget struct{ rows atomic.Int32 }

func (s *filterHealthBudget) CheckQuickCacheOnlyBulk(_ context.Context, rows []models.NZBResult) ([]*debrid.DebridHealthCheck, error) {
	s.rows.Add(int32(len(rows)))
	out := make([]*debrid.DebridHealthCheck, len(rows))
	for i := range out {
		out[i] = &debrid.DebridHealthCheck{Status: "cached", Healthy: true, Cached: true}
	}
	return out, nil
}

func TestSourceFilterRetentionAndPagingDoNotExpandInitialHealthBudget(t *testing.T) {
	h, svc, _ := sourceFilterFixture(t, 600)
	checker := &filterHealthBudget{}
	h.SourceHealthSvc = sourcehealth.New(checker, sourcehealth.DefaultConfig())
	rec := httptest.NewRecorder()
	h.Search(rec, filterRequest("GET", "/api/indexers/search?q=Synthetic&titleId=tmdb:movie:41&mediaType=movie&userId=profile&sourceFiltersVersion=1&includeSourceHealth=true&limit=50", nil))
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	var first sourcefilters.Page
	_ = json.Unmarshal(rec.Body.Bytes(), &first)
	if first.Coverage.RetainedCandidates != 512 || !first.Coverage.Truncated || checker.rows.Load() != 50 {
		t.Fatalf("hidden rows caused additional health work: retained=%+v checked=%d", first.Coverage, checker.rows.Load())
	}
	for i := 0; i < 3; i++ {
		page := filterPageRequest(h, first.SnapshotID, map[string]any{"version": 1, "filters": sourcefilters.Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "fra"}, "limit": 100})
		if page.Code != 200 {
			t.Fatal(page.Body.String())
		}
	}
	if checker.rows.Load() != 50 || svc.calls != 1 {
		t.Fatal("Apply performed provider/health/search work")
	}
}

func TestSourceFilterStrictPageRejectsClientProofAndIncompatibleFields(t *testing.T) {
	h, svc, _ := sourceFilterFixture(t, 2)
	first := initialFilterPage(t, h)
	for _, body := range []string{`{"version":1,"filters":{"basis":"candidate","unknown":"include"},"limit":50,"result":{}}`, `{"version":1,"filters":{"basis":"candidate","unknown":"include","audioCodec":"aac"},"limit":50}`, `{"version":1,"filters":{"basis":"probed","unknown":"include","audioDescription":false},"limit":50}`, `{"version":1,"filters":null,"limit":50}`, `{"version":1,"filters":{"basis":"candidate","unknown":"include","audioLanguage":"zzz"},"limit":50}`} {
		r := mux.SetURLVars(filterRequest("POST", "/api/users/profile/source-searches/"+first.SnapshotID+"/page", []byte(body)), map[string]string{"userID": "profile", "snapshotID": first.SnapshotID})
		rec := httptest.NewRecorder()
		h.SourceSearchPage(rec, r)
		if rec.Code != 400 {
			t.Fatalf("accepted client/incompatible fields: %d %s", rec.Code, body)
		}
	}
	if svc.calls != 1 {
		t.Fatal("invalid filter initiated search")
	}
}
