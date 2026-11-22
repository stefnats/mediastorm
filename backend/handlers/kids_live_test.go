package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/epg"
	user_settings "novastream/services/user_settings"
	"novastream/services/users"
)

type kidsLiveGuideFixture struct {
	guide      *epg.Service
	config     *config.Manager
	xml        *atomic.Value
	requests   *atomic.Int32
	source     string
	start, end time.Time
}

func kidsLiveGuide(t *testing.T, end time.Time) *kidsLiveGuideFixture {
	t.Helper()
	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	end = end.UTC().Truncate(time.Second)
	xml := &atomic.Value{}
	xml.Store(fmt.Sprintf(`<tv><channel id="station.de"><display-name>Synthetic</display-name></channel><programme channel="station.de" start="%s" stop="%s"><title>Current programme</title><rating system="FSK"><value>6</value></rating></programme><programme channel="station.de" start="%s" stop="%s"><title>Next adult programme</title><rating system="FSK"><value>12</value></rating></programme></tv>`, start.Format("20060102150405 -0700"), end.Format("20060102150405 -0700"), end.Format("20060102150405 -0700"), end.Add(time.Hour).Format("20060102150405 -0700")))
	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, xml.Load().(string))
	}))
	t.Cleanup(server.Close)
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.Settings{Live: config.LiveSettings{EPG: config.EPGSettings{Enabled: true, XmltvUrl: server.URL + "/guide.xml", RetentionDays: 7}}}
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	guide := epg.NewService(t.TempDir(), manager)
	if err := guide.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	sources := guide.CachedProgrammeSources()
	if len(sources) != 1 {
		t.Fatal("actual imported guide is missing", sources)
	}
	return &kidsLiveGuideFixture{guide, manager, xml, requests, sources[0].ID, start, end}
}

func kidsLiveRequest(r *http.Request, account string) *http.Request {
	ctx := context.WithValue(r.Context(), auth.ContextKeyAccountID, account)
	return r.WithContext(context.WithValue(ctx, auth.ContextKeySession, models.Session{AccountID: account}))
}

func kidsLiveProgrammeFor(t *testing.T, f *kidsLiveGuideFixture) *kidsLiveProgramme {
	t.Helper()
	decision := f.guide.CurrentProgramAgeDecisionForSource("station.de", f.source, 6, time.Now().UTC())
	if decision.State != "within_limit" || decision.Program == nil || decision.Age == nil {
		t.Fatal("actual cached current programme missing", decision)
	}
	return &kidsLiveProgramme{channel: LiveChannel{ID: "catalog:station", TvgID: "station.de", SourceID: "guide", URL: "https://provider.example/private"}, policy: models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 6, GuideSourceID: f.source}, program: *decision.Program, age: *decision.Age, guide: f.guide}
}

func TestKidsLiveParentalSettingsAndCachedSource(t *testing.T) {
	f := kidsLiveGuide(t, time.Now().Add(time.Hour))
	accountStore, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	account, err := accountStore.Create("synthetic-parent", "synthetic-parent-password")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	child, err := profiles.CreateForAccount(account.ID, "Ella")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = profiles.SetKidsProfile(child.ID, true); err != nil {
		t.Fatal(err)
	}
	preferences, err := user_settings.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := NewUserSettingsHandler(preferences, profiles, f.config)
	h.kidsLiveEPG = f.guide
	h.kidsLiveParents = parentauth.New(accountStore)
	enabled := models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 6, GuideSourceID: f.source}
	call := func(policy models.KidsLiveAgePolicy, password string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(policy)
		r := httptest.NewRequest(http.MethodPut, "/api/users/"+child.ID+"/kids/live-policy", strings.NewReader(string(body)))
		r = mux.SetURLVars(kidsLiveRequest(r, account.ID), map[string]string{"userID": child.ID})
		if password != "" {
			r.Header.Set(parentauth.PasswordHeader, password)
		}
		w := httptest.NewRecorder()
		h.KidsLivePolicy(w, r)
		return w
	}
	t.Run("password-confirmation", func(t *testing.T) {
		w := call(enabled, "")
		if w.Code != 403 {
			t.Fatal(w.Code, w.Body.String())
		}
		got, _ := preferences.Get(child.ID)
		if got != nil && got.LiveTV.KidsAgeLimit.Enabled {
			t.Fatal("unconfirmed choice saved")
		}
	})
	t.Run("explicit-guide-choice", func(t *testing.T) {
		wrong := enabled
		wrong.GuideSourceID = "not-in-cache"
		if w := call(wrong, "synthetic-parent-password"); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
		w := call(enabled, "synthetic-parent-password")
		if w.Code != 200 || strings.Contains(w.Body.String(), "/guide.xml") || !strings.Contains(w.Body.String(), "selected_guide_claim") {
			t.Fatal(w.Code, w.Body.String())
		}
		got, _ := preferences.Get(child.ID)
		if got == nil || got.LiveTV.KidsAgeLimit != enabled {
			t.Fatal("explicit choice was not stored", got)
		}
		if f.requests.Load() != 1 {
			t.Fatal("settings selection fetched guide again")
		}
	})
	t.Run("explicit-off", func(t *testing.T) {
		off := enabled
		off.Enabled = false
		if w := call(off, "synthetic-parent-password"); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		got, _ := preferences.Get(child.ID)
		if got == nil || got.LiveTV.KidsAgeLimit.Enabled {
			t.Fatal("off did not persist")
		}
	})
}

func TestKidsLiveStartRejectsUnboundedPathsAndUnknownProgramme(t *testing.T) {
	f := kidsLiveGuide(t, time.Now().Add(time.Hour))
	policy := models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 6, GuideSourceID: f.source}
	preferences := &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"ella": {LiveTV: models.LiveTVSettings{KidsAgeLimit: policy}}}}
	channels := staticLiveChannelProvider{channels: []LiveChannel{{ID: "catalog:station", TvgID: "station.de", SourceID: "source", URL: "https://provider.example/private"}}}
	h := &VideoHandler{kidsLiveEPG: f.guide, userSettingsSvc: preferences, usersSvc: liveAccountUsers{"ella": {ID: "ella", AccountID: "home", IsKidsProfile: true}}, liveChannels: channels}
	for _, tc := range []struct {
		name, query string
		policy      models.KidsLiveAgePolicy
		want        int
	}{
		{"disabled", "channelId=catalog:station", models.KidsLiveAgePolicy{}, 403},
		{"direct-provider-url", "channelId=catalog:station&url=https://other.example/stream", policy, 403},
		{"timeshift", "channelId=catalog:station&timeshift=1", policy, 403},
		{"direct-format", "channelId=catalog:station&format=direct", policy, 403},
		{"unknown-channel", "channelId=station.de", policy, 403},
		{"wrong-source", "channelId=catalog:station&sourceId=other", policy, 403},
		{"unknown-guide", "channelId=catalog:station", models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 6, GuideSourceID: "absent"}, 403},
		{"guide-age-too-old", "channelId=catalog:station", models.KidsLiveAgePolicy{Enabled: true, Country: "DE", MaximumAge: 0, GuideSourceID: f.source}, 403},
		// Correct selection reaches the real existing HLS-enabled check. No fake
		// provider/session is substituted to claim that this is encoded playback.
		{"bounded-start-reaches-hls", "channelId=catalog:station", policy, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preferences.settings["ella"].LiveTV.KidsAgeLimit = tc.policy
			r := kidsLiveRequest(httptest.NewRequest(http.MethodGet, "/api/live/kids/hls/start?profileId=ella&"+tc.query, nil), "home")
			w := httptest.NewRecorder()
			h.StartKidsLiveHLSSession(w, r)
			if w.Code != tc.want || strings.Contains(w.Body.String(), "provider.example") {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	if f.requests.Load() != 1 {
		t.Fatal("age decisions refreshed guide")
	}
}

func TestKidsLiveHLSProgrammeEndAndGuideCorrection(t *testing.T) {
	for _, change := range []string{"title", "rating", "missing", "end"} {
		t.Run(change, func(t *testing.T) {
			f := kidsLiveGuide(t, time.Now().Add(time.Hour))
			programme := kidsLiveProgrammeFor(t, f)
			if !programme.current(time.Now().UTC()) {
				t.Fatal("original programme is not current")
			}
			body := f.xml.Load().(string)
			switch change {
			case "title":
				body = strings.Replace(body, "Current programme", "Replacement programme", 1)
			case "rating":
				body = strings.Replace(body, "<value>6</value>", "<value>12</value>", 1)
			case "missing":
				body = `<tv><channel id="station.de"><display-name>Synthetic</display-name></channel></tv>`
			case "end":
				body = strings.Replace(body, f.end.Format("20060102150405 -0700"), f.end.Add(time.Minute).Format("20060102150405 -0700"), 1)
			}
			f.xml.Store(body)
			// A guide without programmes is rejected by the import. Use a genuine
			// non-current programme to replace the cache instead of fabricating a result.
			if change == "missing" {
				f.xml.Store(fmt.Sprintf(`<tv><channel id="station.de"><display-name>Synthetic</display-name></channel><programme channel="station.de" start="%s" stop="%s"><title>Future</title></programme></tv>`, f.end.Format("20060102150405 -0700"), f.end.Add(time.Hour).Format("20060102150405 -0700")))
			}
			if err := f.guide.Refresh(t.Context()); err != nil {
				t.Fatal(err)
			}
			if programme.current(time.Now().UTC()) {
				t.Fatal("changed cache kept old programme playable")
			}
		})
	}
}

func TestKidsLiveCatalogueNeverReturnsProviderURLs(t *testing.T) {
	f := kidsLiveGuide(t, time.Now().Add(time.Hour))
	programme := kidsLiveProgrammeFor(t, f)
	channel := programme.channel
	channel.Name = "Synthetic station"
	channel.Logo = "https://private.example/user/password/logo.png"
	channel.StreamURL = channel.URL
	h := &VideoHandler{kidsLiveEPG: f.guide, liveChannels: staticLiveChannelProvider{channels: []LiveChannel{channel}}, userSettingsSvc: &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"ella": {LiveTV: models.LiveTVSettings{KidsAgeLimit: programme.policy}}}}, usersSvc: liveAccountUsers{"ella": {ID: "ella", IsKidsProfile: true}}}
	w := httptest.NewRecorder()
	h.KidsLiveChannels(w, httptest.NewRequest(http.MethodGet, "/api/live/kids/channels?profileId=ella", nil))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result struct {
		Channels []struct{ ID, Name, ProgrammeState string }
		Total    int
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Channels) != 1 || result.Channels[0].ID != channel.ID || result.Channels[0].ProgrammeState != "within_limit" {
		t.Fatal("missing actual catalogue identity", w.Body.String())
	}
	for _, forbidden := range []string{"provider.example", "private.example", "streamUrl", "logo", "sourceId", "password", "url"} {
		if strings.Contains(w.Body.String(), forbidden) {
			t.Fatal("provider detail in kids catalogue", forbidden, w.Body.String())
		}
	}
	if f.requests.Load() != 1 {
		t.Fatal("catalogue age check refreshed guide")
	}
}

func TestKidsLiveHTTPHLSBoundaryAndOtherViewer(t *testing.T) {
	f := kidsLiveGuide(t, time.Now().UTC().Add(3*time.Second))
	programme := kidsLiveProgrammeFor(t, f)
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "stream.m3u8"), []byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nsegment000000.ts\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "segment000000.ts"), []byte("synthetic approved segment"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &HLSSession{ID: "child-session", ProfileID: "ella", IsLive: true, Path: programme.channel.URL, OriginalPath: programme.channel.URL, OutputDir: output, kidsProgramme: programme}
	adultOutput := t.TempDir()
	if err := os.WriteFile(filepath.Join(adultOutput, "unrelated"), []byte("adult viewer"), 0600); err != nil {
		t.Fatal(err)
	}
	adult := &HLSSession{ID: "adult-session", ProfileID: "adult", IsLive: true, OutputDir: adultOutput}
	manager := &HLSManager{sessions: map[string]*HLSSession{child.ID: child, adult.ID: adult}}
	h := &VideoHandler{hlsManager: manager, userSettingsSvc: &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"ella": {LiveTV: models.LiveTVSettings{KidsAgeLimit: programme.policy}}}}, usersSvc: liveAccountUsers{"ella": {ID: "ella", IsKidsProfile: true}}}
	// Register the exact keepalive route first: a segment wildcard must not eat it.
	router := mux.NewRouter()
	router.HandleFunc("/api/video/hls/{sessionID}/keepalive", h.KeepAliveHLSSession).Methods(http.MethodPost)
	router.HandleFunc("/api/video/hls/{sessionID}/stream.m3u8", h.ServeHLSPlaylist)
	router.HandleFunc("/api/video/hls/{sessionID}/{segment}", h.ServeHLSSegment)
	server := httptest.NewServer(router)
	t.Cleanup(func() { server.Close(); manager.CleanupSession(child.ID); manager.CleanupSession(adult.ID) })
	manager.armKidsProgrammeEnd(child)
	request := func(method, path string) (int, []byte) {
		t.Helper()
		r, err := http.NewRequest(method, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatal("incomplete HTTP response", err, closeErr)
		}
		return response.StatusCode, data
	}
	code, data := request(http.MethodGet, "/api/video/hls/child-session/segment000000.ts?profileId=ella")
	if code != 200 || string(data) != "synthetic approved segment" {
		t.Fatal(code, string(data))
	}
	// Keep-alive is active right up to the fixed end, but cannot extend it.
	end := time.NewTimer(time.Until(programme.program.Stop) + 50*time.Millisecond)
	defer end.Stop()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	waiting := true
	for waiting {
		select {
		case <-end.C:
			waiting = false
		case <-ticker.C:
			_, _ = request(http.MethodPost, "/api/video/hls/child-session/keepalive?profileId=ella")
		}
	}
	code, data = request(http.MethodGet, "/api/video/hls/child-session/segment000000.ts?profileId=ella")
	if code == 200 || strings.Contains(string(data), "synthetic approved segment") {
		t.Fatal("segment survived programme end", code, string(data))
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := os.Stat(output)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("programme output did not retire", err)
		}
		time.Sleep(time.Millisecond)
	}
	manager.mu.RLock()
	same := manager.sessions[adult.ID] == adult
	manager.mu.RUnlock()
	if !same {
		t.Fatal("child timer stopped unrelated adult viewer")
	}
	if _, err := os.Stat(filepath.Join(adultOutput, "unrelated")); err != nil {
		t.Fatal("adult files removed", err)
	}
	if programme.current(programme.program.Stop) || f.guide.CurrentProgramAgeDecisionForSource("station.de", f.source, 6, programme.program.Stop).State != "too_old" {
		t.Fatal("next programme was not decided again")
	}
	remaining := programme.remainingMillis(programme.program.Stop.Add(-1500 * time.Millisecond))
	if remaining != 1500 || programme.remainingMillis(programme.program.Stop.Add(time.Second)) != 0 {
		t.Fatal("remaining duration is incorrect")
	}
	encoded, _ := json.Marshal(programme.description())
	if strings.Contains(string(encoded), "provider.example") {
		t.Fatal("guide description leaked playback URL")
	}
}
