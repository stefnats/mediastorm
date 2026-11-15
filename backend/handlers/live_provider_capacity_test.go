package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
)

const capacityAccountResponse = `{"user_info":{"auth":"1","status":"Active","max_connections":"2","active_cons":"1","username":"synthetic-private-user","password":"synthetic-private-password","message":"synthetic-private-message"},"server_info":{"url":"synthetic-private-server"}}`

func capacityFixture(t *testing.T, respond http.HandlerFunc) (*LiveHandler, *config.Manager, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/panel/player_api.php" || len(r.URL.Query()) != 2 || r.URL.Query().Get("action") != "" || r.Header.Get("User-Agent") != "MediaStorm" {
			t.Error("capacity check requested something other than account information")
		}
		if respond != nil {
			respond(w, r)
			return
		}
		_, _ = w.Write([]byte(capacityAccountResponse))
	}))
	t.Cleanup(provider.Close)
	dir := t.TempDir()
	manager := config.NewManager(filepath.Join(dir, "settings.json"))
	settings := config.DefaultSettings()
	settings.Cache.Directory = filepath.Join(dir, "cache")
	settings.Live.Sources = []config.LivePlaylistSource{
		{ID: "xtream", Name: "Synthetic account", Mode: "xtream", XtreamHost: provider.URL + "/panel", XtreamUsername: "synthetic-user", XtreamPassword: "synthetic-password", MaxStreams: 3},
		{ID: "export", Name: "Synthetic export", Mode: "m3u", PlaylistURL: provider.URL + "/panel/get.php?username=synthetic-user&password=synthetic-password&type=m3u_plus", MaxStreams: 1},
		{ID: "plain", Name: "Synthetic plain playlist", Mode: "m3u", PlaylistURL: provider.URL + "/list.m3u"},
	}
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	return NewLiveHandler(provider.Client(), false, "", 1, 0, 0, false, manager, nil), manager, &calls
}

func capacityCheck(t *testing.T, h *LiveHandler, source string) (*httptest.ResponseRecorder, ProviderCapacityReport) {
	t.Helper()
	r := httptest.NewRequest("POST", "/admin/api/live/provider-capacity", strings.NewReader(fmt.Sprintf(`{"sourceId":%q}`, source)))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
	w := httptest.NewRecorder()
	h.CheckProviderCapacity(w, r)
	var report ProviderCapacityReport
	if w.Code == 200 {
		if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
	}
	return w, report
}

func TestProviderCapacityParsesOnlyAuthenticatedPositiveLimits(t *testing.T) {
	for _, tc := range []struct{ name, body, state string }{
		{"strings", capacityAccountResponse, "available"},
		{"numbers", `{"user_info":{"auth":1,"status":"active","max_connections":2,"active_cons":0}}`, "available"},
		{"zero-is-unknown", `{"user_info":{"auth":1,"status":"Active","max_connections":0}}`, "unknown"},
		{"missing-limit", `{"user_info":{"auth":1,"status":"Active"}}`, "unknown"},
		{"negative-limit", `{"user_info":{"auth":1,"status":"Active","max_connections":-1}}`, "unknown"},
		{"decimal-limit", `{"user_info":{"auth":1,"status":"Active","max_connections":2.5}}`, "unknown"},
		{"huge-limit", `{"user_info":{"auth":1,"status":"Active","max_connections":"99999999999999999"}}`, "unknown"},
		{"invalid-limit", `{"user_info":{"auth":1,"status":"Active","max_connections":{}}}`, "unknown"},
		{"missing-auth", `{"user_info":{"status":"Active","max_connections":2}}`, "unavailable"},
		{"invalid-auth", `{"user_info":{"auth":0,"status":"Active","max_connections":2}}`, "unavailable"},
		{"banned", `{"user_info":{"auth":1,"status":"Banned","max_connections":2}}`, "unavailable"},
		{"expired", `{"user_info":{"auth":1,"status":"Expired","max_connections":2}}`, "unavailable"},
		{"disabled", `{"user_info":{"auth":1,"status":"Disabled","max_connections":2}}`, "unavailable"},
		{"trailing", capacityAccountResponse + `{}`, "unavailable"},
		{"html", `<html>provider failure</html>`, "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := parseProviderCapacity([]byte(tc.body))
			if report.State != tc.state || (report.ReportedLimit != nil) != (tc.state == "available") {
				t.Fatalf("unexpected capacity state %q", report.State)
			}
		})
	}
}

func TestProviderCapacityEndpointUsesSavedCredentialsAndNoAction(t *testing.T) {
	for _, source := range []resolvedM3USource{
		{Mode: "xtream", XtreamHost: "https://provider.example/panel/", XtreamUsername: "synthetic + ü", XtreamPassword: "synthetic&?+"},
		{Mode: "m3u", PlaylistURL: "https://provider.example/panel/get.php?" + url.Values{"username": {"synthetic + ü"}, "password": {"synthetic&?+"}, "type": {"m3u_plus"}, "action": {"get_live_streams"}}.Encode()},
	} {
		endpoint, ok := providerCapacityEndpoint(source)
		if !ok || endpoint.Path != "/panel/player_api.php" || len(endpoint.Query()) != 2 || endpoint.Query().Get("username") != "synthetic + ü" || endpoint.Query().Get("password") != "synthetic&?+" {
			t.Fatal("endpoint changed credentials or requested content")
		}
	}
	for _, source := range []resolvedM3USource{
		{Mode: "m3u", PlaylistURL: "https://provider.example/list.m3u"},
		{Mode: "m3u", PlaylistURL: "https://provider.example/get.php?username=a&username=b&password=p"},
		{Mode: "m3u", PlaylistURL: "https://provider.example/get.php?username=a&password=p&password=q"},
		{Mode: "xtream", XtreamHost: "file:///private", XtreamUsername: "a", XtreamPassword: "b"},
		{Mode: "xtream", XtreamHost: "https://user:password@provider.example", XtreamUsername: "a", XtreamPassword: "b"},
		{Mode: "stalker", PlaylistURL: "https://provider.example/get.php?username=a&password=b"},
	} {
		if _, ok := providerCapacityEndpoint(source); ok {
			t.Fatal("unsupported source was guessed as an account endpoint")
		}
	}
}

func TestProviderCapacityAliasesShareChecksAndNeverSaveOrExposeSecrets(t *testing.T) {
	h, manager, calls := capacityFixture(t, nil)
	before, _ := manager.Load()
	w, first := capacityCheck(t, h, "xtream")
	if w.Code != 200 || first.ReportedLimit == nil || *first.ReportedLimit != 2 || first.ConfiguredLimit != 3 || first.SharedConfiguredLimit != 1 || first.EnforcedLimit != 1 || first.LocalConnections != 0 || first.Cached {
		t.Fatal("provider report or stricter alias limit is incorrect")
	}
	if first.CheckedAt == nil || first.CacheExpiresAt == nil || first.LimitOrigin != "provider_account" || !first.CacheExpiresAt.After(*first.CheckedAt) {
		t.Fatal("report has no provenance or freshness")
	}
	for _, secret := range []string{"synthetic-private", "synthetic-password", "username", "server_info", "http://", "externalConnections"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("provider payload or unreliable attribution escaped to the response")
		}
	}
	w, alias := capacityCheck(t, h, "export")
	if w.Code != 200 || !alias.Cached || alias.SourceID != "export" || calls.Load() != 1 {
		t.Fatal("one account alias caused an additional provider check")
	}
	after, _ := manager.Load()
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("checking silently changed saved settings")
	}
	w, plain := capacityCheck(t, h, "plain")
	if w.Code != 200 || plain.State != "unsupported" || plain.ReportedLimit != nil || plain.CheckedAt != nil || calls.Load() != 1 {
		t.Fatal("plain M3U caused a guessed provider request")
	}
}

func TestProviderCapacityRejectsUntrustedAndDisabledRequestsBeforeNetwork(t *testing.T) {
	h, manager, calls := capacityFixture(t, nil)
	settings, _ := manager.Load()
	disabled := false
	settings.Live.Sources[0].Enabled = &disabled
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, method, body string
		master, scoped     bool
		status             int
	}{
		{"normal-account", "POST", `{"sourceId":"export"}`, false, false, 403},
		{"scoped-grant", "POST", `{"sourceId":"export"}`, true, true, 403},
		{"method", "GET", `{"sourceId":"export"}`, true, false, 405},
		{"unknown-url", "POST", `{"sourceId":"export","url":"http://127.0.0.1/arbitrary"}`, true, false, 400},
		{"missing", "POST", `{}`, true, false, 400},
		{"trailing", "POST", `{"sourceId":"export"}{}`, true, false, 400},
		{"unknown-source", "POST", `{"sourceId":"unknown"}`, true, false, 404},
		{"disabled", "POST", `{"sourceId":"xtream"}`, true, false, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "/admin/api/live/provider-capacity", strings.NewReader(tc.body))
			ctx := context.WithValue(r.Context(), auth.ContextKeyIsMaster, tc.master)
			if tc.scoped {
				ctx = context.WithValue(ctx, auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream})
			}
			w := httptest.NewRecorder()
			h.CheckProviderCapacity(w, r.WithContext(ctx))
			if w.Code != tc.status || w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatalf("status %d, want %d", w.Code, tc.status)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request contacted a provider")
	}
}

func TestProviderCapacityNeverFollowsRedirectsOrFallsBackFromInvalidProxy(t *testing.T) {
	var stolen atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { stolen.Add(1) }))
	defer other.Close()
	h, manager, calls := capacityFixture(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL+"/credentials", 302) })
	w, report := capacityCheck(t, h, "xtream")
	if w.Code != 200 || report.State != "unavailable" || stolen.Load() != 0 || calls.Load() != 1 {
		t.Fatal("provider redirect was followed")
	}
	settings, _ := manager.Load()
	settings.Live.Sources[0].ProxyURL = "unsupported://synthetic-private-proxy"
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	w, report = capacityCheck(t, h, "xtream")
	if w.Code != 200 || report.State != "unavailable" || calls.Load() != 1 || strings.Contains(w.Body.String(), "synthetic-private") {
		t.Fatal("invalid proxy fell back to direct access or exposed a secret")
	}
}

func TestProviderCapacityBoundsResponsesAndRejectsHTTPFailures(t *testing.T) {
	for _, mode := range []string{"http-failure", "oversize", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			h, _, _ := capacityFixture(t, func(w http.ResponseWriter, _ *http.Request) {
				switch mode {
				case "http-failure":
					w.WriteHeader(403)
					_, _ = w.Write([]byte("synthetic-private-message"))
				case "oversize":
					_, _ = w.Write([]byte(capacityAccountResponse + strings.Repeat(" ", providerCapacityBodyLimit)))
				case "malformed":
					_, _ = w.Write([]byte("synthetic-private-message"))
				}
			})
			w, report := capacityCheck(t, h, "xtream")
			if w.Code != 200 || report.State != "unavailable" || report.ReportedLimit != nil || strings.Contains(w.Body.String(), "synthetic-private") {
				t.Fatal("invalid provider result was adopted or exposed")
			}
		})
	}
}

func TestProviderCapacitySourceChangesDiscardInFlightResults(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	h, manager, _ := capacityFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		_, _ = w.Write([]byte(capacityAccountResponse))
	})
	result := make(chan int, 1)
	go func() { w, _ := capacityCheck(t, h, "xtream"); result <- w.Code }()
	<-arrived
	settings, _ := manager.Load()
	settings.Live.Sources[0].XtreamPassword = "synthetic-changed-password"
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	close(release)
	if <-result != 409 {
		t.Fatal("a changed source received a stale provider report")
	}
}

func TestProviderCapacityCoalescesChecksWhenAWaiterCancels(t *testing.T) {
	var checks providerCapacityChecks
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	fetch := func(context.Context) ProviderCapacityReport {
		calls.Add(1)
		close(started)
		<-release
		return parseProviderCapacity([]byte(capacityAccountResponse))
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, _, err := checks.check(ctx, "synthetic-account", fetch); first <- err }()
	<-started
	second := make(chan ProviderCapacityReport, 1)
	go func() {
		report, _, _ := checks.check(context.Background(), "synthetic-account", fetch)
		second <- report
	}()
	cancel()
	if !errors.Is(<-first, context.Canceled) {
		t.Fatal("cancelled waiter remained active")
	}
	close(release)
	if (<-second).State != "available" || calls.Load() != 1 {
		t.Fatal("one waiter cancelled or duplicated a shared check")
	}
}

func TestProviderCapacityCacheExpiresFailuresAndBoundsMemory(t *testing.T) {
	var tick atomic.Int64
	tick.Store(1700000000)
	checks := providerCapacityChecks{now: func() time.Time { return time.Unix(tick.Load(), 0).UTC() }}
	calls := 0
	fetch := func(context.Context) ProviderCapacityReport {
		calls++
		return ProviderCapacityReport{State: "unavailable"}
	}
	if _, cached, err := checks.check(context.Background(), "one", fetch); err != nil || cached {
		t.Fatal("first check reused a cache")
	}
	if _, cached, err := checks.check(context.Background(), "one", fetch); err != nil || !cached || calls != 1 {
		t.Fatal("failed check was not cached")
	}
	tick.Add(31)
	if _, cached, err := checks.check(context.Background(), "one", fetch); err != nil || cached || calls != 2 {
		t.Fatal("failure did not expire")
	}
	for i := range maxProviderCapacityChecks + 8 {
		if _, _, err := checks.check(context.Background(), fmt.Sprint(i), fetch); err != nil {
			t.Fatal(err)
		}
	}
	checks.mu.Lock()
	count := len(checks.entries)
	checks.mu.Unlock()
	if count != maxProviderCapacityChecks {
		t.Fatal("provider report cache is not bounded")
	}
}

func TestProviderCapacityConcurrentAliasesUseOneProviderRequest(t *testing.T) {
	h, _, calls := capacityFixture(t, nil)
	var workers sync.WaitGroup
	for i := range 24 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id := "xtream"
			if i%2 == 1 {
				id = "export"
			}
			w, report := capacityCheck(t, h, id)
			if w.Code != 200 || report.SourceID != id || report.ReportedLimit == nil {
				t.Error("shared report was assigned to the wrong source")
			}
		}()
	}
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatal("concurrent aliases multiplied account queries")
	}
}

func TestProviderCapacitySupportsSavedLegacyXtreamWithoutAPlaylistFetch(t *testing.T) {
	h, manager, calls := capacityFixture(t, nil)
	settings, _ := manager.Load()
	source := settings.Live.Sources[0]
	settings.Live.Sources, settings.Live.PlaylistSources = nil, nil
	settings.Live.Mode, settings.Live.PlaylistURL = "xtream", ""
	settings.Live.XtreamHost, settings.Live.XtreamUsername, settings.Live.XtreamPassword = source.XtreamHost, source.XtreamUsername, source.XtreamPassword
	settings.Live.MaxStreams = 4
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	w, report := capacityCheck(t, h, "default")
	if w.Code != 200 || report.ReportedLimit == nil || report.ConfiguredLimit != 4 || report.SourceID != "default" || calls.Load() != 1 {
		t.Fatalf("legacy source report: status=%d state=%s sourceId=%s configuredLimit=%d requests=%d", w.Code, report.State, report.SourceID, report.ConfiguredLimit, calls.Load())
	}
}

func TestProviderCapacityUsesTheConfiguredProxyWithoutDirectFallback(t *testing.T) {
	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		if r.URL.Path != "/panel/player_api.php" || r.URL.Query().Get("username") != "synthetic-user" {
			t.Error("proxy received an unexpected target")
		}
		_, _ = w.Write([]byte(capacityAccountResponse))
	}))
	defer proxy.Close()
	h, manager, directCalls := capacityFixture(t, nil)
	settings, _ := manager.Load()
	settings.Live.Sources[0].ProxyURL = proxy.URL
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	w, report := capacityCheck(t, h, "xtream")
	if w.Code != 200 || report.State != "available" || proxyCalls.Load() != 1 || directCalls.Load() != 0 {
		t.Fatal("configured proxy was bypassed")
	}
}

type capacityContextTransport struct{}

func (capacityContextTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, errors.New("synthetic-private-timeout")
}

func TestProviderCapacityReadHonorsCancellationAndSanitizesTransportErrors(t *testing.T) {
	h := &LiveHandler{client: &http.Client{Transport: capacityContextTransport{}}}
	endpoint, _ := url.Parse("http://127.0.0.1/player_api.php?username=synthetic-user&password=synthetic-private-password")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	report := h.fetchProviderCapacity(ctx, endpoint, "")
	encoded, _ := json.Marshal(report)
	if time.Since(start) > time.Second || report.State != "unavailable" || strings.Contains(string(encoded), "synthetic-private") {
		t.Fatal("cancelled check hung or exposed request details")
	}
}

func TestProviderCapacityBoundsConcurrentDistinctChecks(t *testing.T) {
	var checks providerCapacityChecks
	started := make(chan struct{}, maxProviderCapacityChecks)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var workers sync.WaitGroup
	for i := range maxProviderCapacityChecks {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, _, err := checks.check(context.Background(), fmt.Sprint(i), func(ctx context.Context) ProviderCapacityReport {
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return ProviderCapacityReport{State: "unknown"}
			})
			if err != nil {
				t.Error("admitted check failed")
			}
		}()
	}
	for range maxProviderCapacityChecks {
		<-started
	}
	if _, _, err := checks.check(context.Background(), "overflow", func(context.Context) ProviderCapacityReport {
		t.Error("excess query started")
		return ProviderCapacityReport{}
	}); err == nil {
		t.Error("in-flight query bound was not enforced")
	}
	releaseAll()
	workers.Wait()
}
