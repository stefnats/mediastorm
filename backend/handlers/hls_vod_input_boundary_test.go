package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
)

type rootHLSInputPoisonProvider struct {
	streams   atomic.Int32
	directs   atomic.Int32
	durations atomic.Int32
	growing   atomic.Int32
}

func (p *rootHLSInputPoisonProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	p.streams.Add(1)
	return nil, errors.New("synthetic source must not be opened")
}
func (p *rootHLSInputPoisonProvider) GetDirectURL(context.Context, string) (string, error) {
	p.directs.Add(1)
	return "", errors.New("synthetic source must not be resolved")
}
func (p *rootHLSInputPoisonProvider) GetDuration(context.Context, string) (float64, error) {
	p.durations.Add(1)
	return 0, errors.New("synthetic duration must not be queried")
}
func (p *rootHLSInputPoisonProvider) IsGrowing(context.Context, string) (bool, error) {
	p.growing.Add(1)
	return false, errors.New("synthetic growth must not be queried")
}
func rootHLSInputHandler(t *testing.T, mode vodApplicabilityMode, kid bool) (*VideoHandler, *vodTestState, *rootHLSInputPoisonProvider, string) {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("local ffmpeg required")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("local ffprobe required")
	}
	base := t.TempDir()
	provider := &rootHLSInputPoisonProvider{}
	h := NewVideoHandlerWithProvider(true, ffmpeg, ffprobe, base, provider)
	t.Cleanup(h.Shutdown)
	state := newVODTestState(t, mode, kid)
	if err := h.installHLSInputBoundary(state.boundary); err != nil {
		t.Fatal(err)
	}
	path := "debrid/synthetic/owned-candidate.mkv"
	h.setCachedMetadata(path, &videoMetadataResponse{Path: "SYNTHETIC-PRIVATE-WARM-METADATA", DurationSeconds: 120})
	return h, state, provider, path
}
func TestHLSVODInputEntryDeniesBeforeWarmMetadataOrSourceDemand(t *testing.T) {
	for _, mode := range []string{"unknown", "policy error", "child", "missing selector", "conflicting selector", "missing PIN", "revoked session", "expired session", "scoped stream", "inactive scoped stream", "imported authority", "spoofed master", "caller live flags", "loopback child", "missing library guard"} {
		t.Run(mode, func(t *testing.T) {
			stateMode := vodAccountActive
			if mode == "inactive scoped stream" {
				stateMode = vodExplicitInactive
			}
			h, state, p, path := rootHLSInputHandler(t, stateMode, mode == "child" || mode == "caller live flags" || mode == "loopback child")
			if mode == "scoped stream" || mode == "inactive scoped stream" {
				state.change(func(s *vodTestState) { s.session.Scope = models.SessionScopeStream; s.session.ScopeResource = path })
			}
			if mode == "missing library guard" {
				path = "plexmedia:synthetic-unknown-owner"
			}
			r := state.request(http.MethodGet, "/api/video/hls/start?path="+url.QueryEscape(path)+"&profileId=profile&clientId=device&startPercent=40", "")
			switch mode {
			case "unknown":
				state.change(func(s *vodTestState) { s.states["account"] = vodAccountApplicability{} })
			case "policy error":
				state.change(func(s *vodTestState) { s.failure = errors.New("synthetic policy unavailable") })
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
				state.change(func(s *vodTestState) { s.session.Token = "replacement-session" })
			case "expired session":
				state.change(func(s *vodTestState) { s.session.ExpiresAt = time.Now().Add(-time.Second) })
			case "imported authority":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{}))
				q := r.URL.Query()
				q.Set("ownerGrant", `{"account":"account","profile":"profile","verified":true}`)
				r.URL.RawQuery = q.Encode()
			case "spoofed master":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
			case "caller live flags":
				q := r.URL.Query()
				q.Set("isLive", "true")
				q.Set("growing", "true")
				q.Set("verified", "true")
				q.Set("age", "0")
				r.URL.RawQuery = q.Encode()
			case "loopback child":
				r.RemoteAddr = "127.0.0.1:4567"
				r.Header.Set("X-Internal-Stream", "true")
				r.Header.Set("X-Forwarded-For", "127.0.0.1")
			}
			before, err := os.ReadDir(h.hlsManager.baseDir)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.StartHLSSession(w, r)
			if w.Code != http.StatusForbidden && w.Code != http.StatusNotFound && w.Code != http.StatusUnauthorized {
				t.Fatalf("denial=%d body=%s", w.Code, w.Body.String())
			}
			if p.streams.Load() != 0 || p.directs.Load() != 0 || p.durations.Load() != 0 || p.growing.Load() != 0 {
				t.Fatalf("denied entry reached source methods: stream=%d direct=%d duration=%d growing=%d", p.streams.Load(), p.directs.Load(), p.durations.Load(), p.growing.Load())
			}
			h.hlsManager.mu.RLock()
			sessions := len(h.hlsManager.sessions)
			h.hlsManager.mu.RUnlock()
			after, err := os.ReadDir(h.hlsManager.baseDir)
			if err != nil {
				t.Fatal(err)
			}
			if sessions != 0 || len(after) != len(before) {
				t.Fatal("denied entry created a runtime or output directory")
			}
			if strings.Contains(w.Body.String(), "SYNTHETIC-PRIVATE-WARM-METADATA") || strings.Contains(w.Body.String(), "playlistUrl") {
				t.Fatal("denied entry used warm metadata as playback proof")
			}
		})
	}
}

// URI consistency tests do not issue source or playback authority.
func TestHLSVODInputOwnedURIQueriesAndScope(t *testing.T) {
	id := "0123456789abcdef0123456789abcdef"
	auth := url.Values{"profileId": {"profile"}, "token": {"synthetic-account-token"}, "profileUnlock": {"synthetic-owner-unlock"}}
	for _, raw := range []string{"segment0.ts?opaque=a%2Fb+%26x&quality=hd&quality=sd", "/video/hls/" + id + "/stream.m3u8?opaque=%2f", "/api/video/hls/" + id + "/master.m3u8", "init.mp4", "subtitle-0.m3u8", "subtitles-0.vtt", "subtitles.vtt", "segment0.ts?profileId=profile&UserID=profile&token=synthetic-account-token&token=synthetic-account-token&profileUnlock=synthetic-owner-unlock"} {
		t.Run(raw, func(t *testing.T) {
			got, err := rewriteOwnedHLSURI(raw, id, auth)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := url.Parse(got)
			if err != nil {
				t.Fatal(err)
			}
			original, _ := url.Parse(raw)
			if original.RawQuery != "" && !strings.HasPrefix(parsed.RawQuery, original.RawQuery) {
				t.Fatalf("original encoded query changed: %q", parsed.RawQuery)
			}
			if parsed.Query().Get("token") != auth.Get("token") || parsed.Query().Get("profileUnlock") != auth.Get("profileUnlock") || parsed.Query().Get("profileId") != "profile" {
				t.Fatal("owned child lost original auth/profile")
			}
			if original.Query().Has("quality") && len(parsed.Query()["quality"]) != 2 {
				t.Fatal("opaque duplicate query lost")
			}
			if original.Query().Has("token") && len(parsed.Query()["token"]) != len(original.Query()["token"]) {
				t.Fatal("existing auth values were duplicated")
			}
		})
	}
	for _, raw := range []string{"https://external.invalid/segment0.ts", "//external.invalid/segment0.ts", "/api/video/hls/other-session/segment0.ts", "../segment0.ts", "%2e%2e/segment0.ts", "segment0.ts#fragment", "key.bin", "segment0.ts?profileId=foreign", "segment0.ts?profileId=profile&UserID=foreign", "segment0.ts?token=foreign-token", "segment0.ts?profileUnlock=foreign-unlock"} {
		t.Run("reject/"+raw, func(t *testing.T) {
			got, err := rewriteOwnedHLSURI(raw, id, auth)
			if err == nil || got != "" {
				t.Fatalf("unsafe URI received auth-bearing output: %q %v", got, err)
			}
		})
	}
	initial, err := hlsOwnedInitialURL(id, "profile")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(initial)
	if err != nil || parsed.IsAbs() || parsed.Path != "/video/hls/"+id+"/stream.m3u8" || parsed.Query().Get("profileId") != "profile" {
		t.Fatalf("initial locator changed ownership/path: %q %v", initial, err)
	}
}

func TestHLSVODInputOwnerPINTransportNeverForwardsForeignOrAmbiguousTokens(t *testing.T) {
	for _, mode := range []string{"single owner", "foreign plus owner", "duplicate owner", "two distinct owner tokens", "PIN not required", "revoked during discovery"} {
		t.Run(mode, func(t *testing.T) {
			h, state, _, _ := rootHLSInputHandler(t, vodAccountActive, false)
			issue := func(profile, fingerprint string) string {
				token, _, err := state.unlocks.Issue(profileunlock.IssueRequest{SessionToken: state.session.Token, SessionExpiresAt: state.session.ExpiresAt, AccountID: state.session.AccountID, ProfileID: profile, PinFingerprint: fingerprint})
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			tokens := []string{state.token}
			foreign := ""
			if mode == "foreign plus owner" {
				foreign = issue("foreign", "other-pin")
				tokens = []string{foreign, state.token}
			}
			if mode == "duplicate owner" {
				tokens = append(tokens, state.token)
			}
			if mode == "two distinct owner tokens" {
				tokens = append(tokens, issue("profile", "pin-v1"))
			}
			if mode == "PIN not required" {
				state.boundary.pin = func(http.ResponseWriter, *http.Request, string) bool { return true }
			}
			r := state.request(http.MethodGet, "/api/video/hls/start?profileId=profile", "")
			r.Header.Set(profileunlock.Header, strings.Join(tokens, ","))
			owner, ok := state.boundary.requestOwner(httptest.NewRecorder(), r, vodMedia, "")
			if !ok {
				t.Fatal("synthetic original owner rejected")
			}
			defer owner.release()
			if mode == "revoked during discovery" {
				original := state.boundary.pin
				sawEmptyCheck := false
				state.boundary.pin = func(w http.ResponseWriter, r *http.Request, profile string) bool {
					if r.Header.Get(profileunlock.Header) == "" {
						sawEmptyCheck = true
					}
					allowed := original(w, r, profile)
					if sawEmptyCheck && allowed && r.Header.Get(profileunlock.Header) == state.token {
						state.unlocks.RevokeProfile("profile")
					}
					return allowed
				}
			}
			auth, err := h.ownedHLSAuth(r.Context(), owner)
			denied := mode == "two distinct owner tokens" || mode == "revoked during discovery"
			if denied {
				if err == nil || auth != nil {
					t.Fatal("ambiguous/retired PIN context yielded URI auth")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if auth.Get("token") != state.session.Token || auth.Get("profileId") != "profile" {
				t.Fatal("URI auth changed original owner")
			}
			if mode == "PIN not required" {
				if auth.Get(profileunlock.QueryParam) != "" {
					t.Fatal("unneeded PIN was exposed")
				}
			} else if auth.Get(profileunlock.QueryParam) != state.token {
				t.Fatal("exact owner PIN not selected")
			}
			if foreign != "" && strings.Contains(auth.Encode(), foreign) {
				t.Fatal("foreign profile token was exposed")
			}
		})
	}
}
