//go:build linux

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
)

func TestKidsLiveActualEncoderUsesFixedProgrammeEnd(t *testing.T) {
	ffmpeg, ffprobe := ordinaryLifeTools(t)
	// Reuse the existing generated synthetic file, not any configured provider.
	path := ordinaryLifeMedia(t)
	transport := filepath.Join(t.TempDir(), "synthetic.ts")
	remux := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-v", "error", "-i", path, "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-f", "mpegts", transport)
	if output, err := remux.CombinedOutput(); err != nil {
		t.Fatalf("synthetic live transport: %v %.1000s", err, output)
	}
	data, err := os.ReadFile(transport)
	if err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".m3u") {
			_, _ = w.Write([]byte("#EXTM3U\n#EXTINF:-1 tvg-id=\"station.de\",Synthetic\n"))
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		flusher := w.(http.Flusher)
		// Keep a real TS channel open until the fixed programme timer closes it.
		for {
			for offset := 0; offset < len(data); offset += 188 * 7 {
				end := min(offset+188*7, len(data))
				if _, err := w.Write(data[offset:end]); err != nil {
					return
				}
				flusher.Flush()
				timer := time.NewTimer(20 * time.Millisecond)
				select {
				case <-r.Context().Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}))
	t.Cleanup(source.Close)
	handler := NewVideoHandlerWithProvider(true, ffmpeg, ffprobe, t.TempDir(), nil)
	t.Cleanup(handler.hlsManager.Shutdown)
	guide := kidsLiveGuide(t, time.Now().Add(10*time.Second))
	settings, err := guide.config.Load()
	if err != nil {
		t.Fatal(err)
	}
	settings.Live.ProbeSizeMB = 1
	settings.Live.AnalyzeDurationSec = 1
	settings.Live.Sources = []config.LivePlaylistSource{
		{ID: "other", Mode: "m3u", PlaylistURL: source.URL + "/other-playlist.m3u", StreamFormat: "hls", MaxStreams: 1},
		{ID: "family", Mode: "m3u", PlaylistURL: source.URL + "/playlist.m3u", StreamFormat: "direct", MaxStreams: 2},
	}
	if err = guide.config.Save(settings); err != nil {
		t.Fatal(err)
	}
	programme := kidsLiveProgrammeFor(t, guide)
	handler.SetConfigManager(guide.config)
	handler.kidsLiveEPG = guide.guide
	handler.usersSvc = liveAccountUsers{"ella": {ID: "ella", AccountID: "home", IsKidsProfile: true}}
	handler.userSettingsSvc = &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"ella": {LiveTV: models.LiveTVSettings{KidsAgeLimit: programme.policy}}}}
	handler.liveChannels = staticLiveChannelProvider{channels: []LiveChannel{
		{ID: "other:station", SourceID: "other", TvgID: "station.de", Name: "Other catalogue entry", URL: source.URL + "/synthetic.mkv"},
		{ID: "family:station", SourceID: "family", TvgID: "station.de", Name: "Synthetic", URL: source.URL + "/synthetic.mkv"},
	}}
	// Native supplies only the exact channel ID. Its URL is shared by two
	// source entries; the server-selected source must survive into slot/tuning.
	request := kidsLiveRequest(httptest.NewRequest(http.MethodGet, "/api/live/kids/hls/start?profileId=ella&channelId=family:station&target=web", nil), "home")
	response := httptest.NewRecorder()
	handler.StartKidsLiveHLSSession(response, request)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var result struct {
		SessionID   string    `json:"sessionId"`
		PlaylistURL string    `json:"playlistUrl"`
		IsDirect    bool      `json:"isDirect"`
		Remaining   int64     `json:"programmeRemainingMillis"`
		EndsAt      time.Time `json:"programmeEndsAt"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SessionID == "" || result.IsDirect || !strings.Contains(result.PlaylistURL, "/stream.m3u8?") || !result.EndsAt.Equal(guide.end) || result.Remaining <= 0 || result.Remaining > 10000 || strings.Contains(response.Body.String(), source.URL) {
		t.Fatal("unbounded or direct response", response.Body.String())
	}
	manager := handler.hlsManager
	manager.mu.RLock()
	session := manager.sessions[result.SessionID]
	manager.mu.RUnlock()
	if session == nil || session.kidsProgramme == nil || session.LiveTuning.TimeshiftWindow != 0 || session.LiveTuning.StationFailover != nil {
		t.Fatal("missing dedicated bounded session")
	}
	selectedTarget := handler.resolveLiveStreamTargetForSource("ella", "family")
	otherTarget := handler.resolveLiveStreamTargetForSource("ella", "other")
	if request.URL.Query().Get("sourceId") != "" || selectedTarget.BucketKey == otherTarget.BucketKey || session.LiveBucket != selectedTarget.BucketKey || session.LiveTuning.MaxStreams != 2 || session.LiveTuning.ProbeSizeMB != selectedTarget.ProbeSizeMB || session.LiveTuning.AnalyzeDurationSec != selectedTarget.AnalyzeDurationSec {
		t.Fatal("selected catalogue source lost its slot/tuning or changed the caller request", session.LiveBucket, session.LiveTuning)
	}
	command := ordinaryLifeCommand(t, session)
	ordinaryLifeWait(t, "actual encoder did not produce media before the programme ended", func() bool { return len(listLiveSegments(session.OutputDir)) > 0 })
	// Report activity until the wall-clock boundary; it must never turn this
	// session into an unlimited Live stream.
	end := time.NewTimer(time.Until(guide.end) + 20*time.Millisecond)
	defer end.Stop()
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	waiting := true
	for waiting {
		select {
		case <-end.C:
			waiting = false
		case <-tick.C:
			w := httptest.NewRecorder()
			manager.KeepAlive(w, httptest.NewRequest(http.MethodPost, "/keepalive", nil), session.ID)
		}
	}
	ordinaryLifeChannel(t, session.liveSupervisorDone, "encoder supervisor outlived the programme")
	ordinaryLifeChannel(t, session.ordinary.done, "fixed end did not clean up the actual HLS session")
	ordinaryLifeGone(t, command)
	ordinaryLifeWait(t, "fixed-end session remained registered", func() bool {
		manager.mu.RLock()
		defer manager.mu.RUnlock()
		return manager.sessions[result.SessionID] == nil
	})
	// CleanupSession unregisters and joins the encoder before removing its output.
	ordinaryLifeWait(t, "fixed-end programme output was not removed", func() bool {
		_, statErr := os.Stat(session.OutputDir)
		return os.IsNotExist(statErr)
	})
	if _, err = os.Stat(session.OutputDir); !os.IsNotExist(err) {
		t.Fatal("old programme output remains", err)
	}
}
