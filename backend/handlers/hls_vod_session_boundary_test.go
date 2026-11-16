package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

type rootHLSSessionFixture struct {
	handler *VideoHandler
	manager *HLSManager
	state   *vodTestState
	session *HLSSession
	payload string
	last    time.Time
}

func newRootHLSSessionFixture(t *testing.T, mode vodApplicabilityMode, kid bool) *rootHLSSessionFixture {
	t.Helper()
	root := t.TempDir()
	manager := NewHLSManager(root, "", "", nil)
	t.Cleanup(manager.Shutdown)
	state := newVODTestState(t, mode, kid)
	last := time.Now().Add(-time.Hour)
	claim, err := manager.claimHLSSessionDirectory("synthetic-hls")
	if err != nil {
		t.Fatal(err)
	}
	dir := claim.path
	payload := "SYNTHETIC-PRIVATE-HLS-SEGMENT"
	if err := os.WriteFile(filepath.Join(dir, "segment0.ts"), []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	playlist := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.000,\nsegment0.ts\n#EXT-X-ENDLIST\n"
	if err := os.WriteFile(filepath.Join(dir, "stream.m3u8"), []byte(playlist), 0600); err != nil {
		t.Fatal(err)
	}
	session := &HLSSession{ID: "synthetic-hls", ProfileID: "profile", ClientID: "device", Path: "debrid/synthetic/input.mkv", OriginalPath: "debrid/synthetic/input.mkv", OutputDir: dir, LastAccess: last, LastSegmentRequest: last, Completed: true, Duration: 4, CreatedAt: last, StreamStartTime: last}
	session.vodDirectory = claim
	if !manager.publishHLSSession(session) {
		t.Fatal("normal runtime publication rejected its private directory claim")
	}
	return &rootHLSSessionFixture{handler: &VideoHandler{hlsManager: manager, vodBoundary: state.boundary}, manager: manager, state: state, session: session, payload: payload, last: last}
}
func (f *rootHLSSessionFixture) request(action string) *http.Request {
	method := http.MethodGet
	if action == "keepalive" || action == "stop" || action == "seek" {
		method = http.MethodPost
	}
	r := f.state.request(method, "/api/video/hls/synthetic-hls/"+action+"?profileId=profile&time=2", "")
	return mux.SetURLVars(r, map[string]string{"sessionID": f.session.ID, "segment": "segment0.ts", "track": "0"})
}
func (f *rootHLSSessionFixture) invoke(action string, w http.ResponseWriter, r *http.Request) {
	switch action {
	case "playlist":
		f.handler.ServeHLSPlaylist(w, r)
	case "segment":
		f.handler.ServeHLSSegment(w, r)
	case "status":
		f.handler.GetHLSSessionStatus(w, r)
	case "keepalive":
		f.handler.KeepAliveHLSSession(w, r)
	case "stop":
		f.handler.StopHLSSession(w, r)
	case "seek":
		f.handler.SeekHLSSession(w, r)
	case "master":
		f.handler.ServeHLSMasterPlaylist(w, r)
	case "subtitles":
		f.handler.ServeHLSSubtitles(w, r)
	}
}
func TestHLSVODSessionDenialsCannotTouchOrExposeStoredSession(t *testing.T) {
	for _, mode := range []string{"unknown", "policy error", "child", "missing selector", "conflicting selector", "missing PIN", "revoked session", "missing stored owner", "unstamped import"} {
		for _, action := range []string{"playlist", "segment", "status", "keepalive", "stop", "seek", "master", "subtitles"} {
			if action == "stop" && (mode == "unknown" || mode == "policy error" || mode == "child") {
				continue // Owned cleanup is independently permitted without a playback grant.
			}
			t.Run(mode+"/"+action, func(t *testing.T) {
				f := newRootHLSSessionFixture(t, vodAccountActive, mode == "child")
				r := f.request(action)
				switch mode {
				case "unknown":
					f.state.change(func(s *vodTestState) { s.states["account"] = vodAccountApplicability{} })
				case "policy error":
					f.state.change(func(s *vodTestState) { s.failure = errors.New("synthetic policy outage") })
				case "missing selector":
					q := r.URL.Query()
					q.Del("profileId")
					r.URL.RawQuery = q.Encode()
				case "conflicting selector":
					q := r.URL.Query()
					q.Add("UserID", "foreign")
					r.URL.RawQuery = q.Encode()
				case "missing PIN":
					r.Header.Del("X-Profile-Unlock")
				case "revoked session":
					f.state.change(func(s *vodTestState) { s.session.Token = "retired-token" })
				case "missing stored owner":
					f.session.mu.Lock()
					f.session.ProfileID = ""
					f.session.mu.Unlock()
				case "unstamped import":
					imported := &HLSSession{ID: f.session.ID, ProfileID: "profile", ClientID: "device", Path: f.session.Path, OriginalPath: f.session.OriginalPath, OutputDir: f.session.OutputDir, LastAccess: f.last, LastSegmentRequest: f.last, Completed: true}
					f.manager.mu.Lock()
					f.manager.sessions[imported.ID] = imported
					f.manager.mu.Unlock()
					f.session = imported
				}
				w := httptest.NewRecorder()
				f.invoke(action, w, r)
				if w.Code != http.StatusForbidden && w.Code != http.StatusNotFound && w.Code != http.StatusUnauthorized {
					t.Fatalf("denial=%d body=%s", w.Code, w.Body.String())
				}
				f.session.mu.RLock()
				unchanged := f.session.LastAccess.Equal(f.last) && f.session.LastSegmentRequest.Equal(f.last) && !f.session.Stopped
				f.session.mu.RUnlock()
				f.manager.mu.RLock()
				same := f.manager.sessions[f.session.ID] == f.session
				f.manager.mu.RUnlock()
				if !unchanged || !same {
					t.Fatal("initial denial touched, stopped or removed stored session")
				}
				if strings.Contains(w.Body.String(), f.payload) || strings.Contains(w.Body.String(), "segment0.ts") {
					t.Fatal("initial denial exposed cached media")
				}
				if _, err := os.Stat(filepath.Join(f.session.OutputDir, "segment0.ts")); err != nil {
					t.Fatal("initial denial removed actual segment")
				}
			})
		}
	}
}

func TestHLSVODSessionOwnedCleanupRemainsAvailableWithoutPlaybackGrant(t *testing.T) {
	for _, mode := range []string{"unknown", "policy error", "child"} {
		t.Run(mode, func(t *testing.T) {
			f := newRootHLSSessionFixture(t, vodAccountActive, mode == "child")
			if mode == "unknown" {
				f.state.change(func(s *vodTestState) { s.states["account"] = vodAccountApplicability{} })
			}
			if mode == "policy error" {
				f.state.change(func(s *vodTestState) { s.failure = errors.New("synthetic policy outage") })
			}
			w := httptest.NewRecorder()
			f.invoke("stop", w, f.request("stop"))
			if w.Code != http.StatusOK {
				t.Fatalf("owned cleanup denied without playback grant: %d %s", w.Code, w.Body.String())
			}
			f.manager.mu.RLock()
			_, exists := f.manager.sessions[f.session.ID]
			f.manager.mu.RUnlock()
			if exists {
				t.Fatal("owned cleanup retained the stopped runtime instance")
			}
			if _, err := os.Stat(f.session.OutputDir); !os.IsNotExist(err) {
				t.Fatal("owned cleanup did not remove its actual directory")
			}
		})
	}
}
func TestHLSVODSessionOwnedReadAndLegacyControls(t *testing.T) {
	for _, mode := range []string{"nil legacy", "inactive", "active adult"} {
		for _, action := range []string{"playlist", "segment", "status", "keepalive"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				state := vodAccountActive
				if mode == "inactive" {
					state = vodExplicitInactive
				}
				f := newRootHLSSessionFixture(t, state, false)
				if mode == "nil legacy" {
					f.handler.vodBoundary = nil
				}
				w := httptest.NewRecorder()
				f.invoke(action, w, f.request(action))
				if w.Code != http.StatusOK {
					t.Fatalf("owned control=%d body=%s", w.Code, w.Body.String())
				}
				if action == "segment" && w.Body.String() != f.payload {
					t.Fatalf("owned segment=%q", w.Body.String())
				}
				if action == "playlist" && !strings.Contains(w.Body.String(), "segment0.ts") {
					t.Fatal("owned playlist unavailable")
				}
			})
		}
	}
}
func TestHLSVODSessionLatePlaylistCannotPublishAfterRetirement(t *testing.T) {
	f := newRootHLSSessionFixture(t, vodAccountActive, false)
	path := filepath.Join(f.session.OutputDir, "stream.m3u8")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	r := f.request("playlist")
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); f.invoke("playlist", w, r) }()
	deadline := time.Now().Add(5 * time.Second)
	touched := false
	for time.Now().Before(deadline) {
		f.session.mu.RLock()
		touched = !f.session.LastAccess.Equal(f.last)
		f.session.mu.RUnlock()
		if touched {
			break
		}
		time.Sleep(time.Millisecond)
	}
	f.state.unlocks.RevokeProfile("profile")
	if err := os.WriteFile(path, []byte("#EXTM3U\n#EXTINF:4,\nsegment0.ts\n#EXT-X-ENDLIST\n"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("late playlist wait ignored retirement")
	}
	if !touched {
		t.Fatal("positive owner never entered actual playlist wait")
	}
	if w.Code == http.StatusOK || strings.Contains(w.Body.String(), "segment0.ts") {
		t.Fatalf("late playlist escaped: %d %s", w.Code, w.Body.String())
	}
}
func TestHLSVODSessionSnapshotCleanupCannotDeleteReplacement(t *testing.T) {
	f := newRootHLSSessionFixture(t, vodAccountActive, false)
	snapshot, ok := f.manager.snapshotHLSSession(f.session.ID)
	if !ok {
		t.Fatal("normal publication had no runtime snapshot")
	}
	claim, err := f.manager.claimHLSSessionDirectory(f.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	dir := claim.path
	file := filepath.Join(dir, "replacement.ts")
	if err := os.WriteFile(file, []byte("SYNTHETIC-REPLACEMENT"), 0600); err != nil {
		t.Fatal(err)
	}
	replacement := &HLSSession{ID: f.session.ID, ProfileID: "profile", ClientID: "device", Path: f.session.Path, OriginalPath: f.session.OriginalPath, OutputDir: dir, Completed: true}
	replacement.vodDirectory = claim
	if !f.manager.publishHLSSession(replacement) {
		t.Fatal("replacement runtime publication rejected its new directory claim")
	}
	f.manager.cleanupHLSSessionSnapshot(snapshot)
	f.manager.mu.RLock()
	same := f.manager.sessions[replacement.ID] == replacement
	f.manager.mu.RUnlock()
	if !same {
		t.Fatal("old snapshot removed replacement runtime instance")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal("old snapshot removed actual replacement directory")
	}
}
