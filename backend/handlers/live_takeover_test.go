package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/internal/profileaccess"
	"novastream/models"
)

func takeoverRequest(profile, attempt, input string) *http.Request {
	q := url.Values{"profileId": {profile}, "playAttemptId": {attempt}, "url": {input}, "clientId": {"synthetic-device"}}
	request := httptest.NewRequest("GET", "/api/live/hls/start?"+q.Encode(), nil)
	return request.WithContext(context.WithValue(request.Context(), auth.ContextKeyAccountID, "household"))
}

func TestLiveAdmissionUsesOnlyCurrentServerPermissionsAndBoundedAttempts(t *testing.T) {
	profiles := liveAccountUsers{"one": {ID: "one", AccountID: "household", AllowLiveTakeover: true}, "two": {ID: "two", AccountID: "other", AllowLiveTakeover: true}, "kid": {ID: "kid", AccountID: "household", IsKidsProfile: true, AllowLiveTakeover: true}}
	for _, tc := range []struct {
		id, attempt string
		allowed     bool
	}{
		{"one", "synthetic-attempt-one", true}, {"one", "", false}, {"one", "short", false}, {"one", "http://127.0.0.1/invalid-attempt", false}, {"two", "synthetic-attempt-two", false}, {"kid", "synthetic-attempt-kid", false}, {"missing", "synthetic-attempt-missing", false},
	} {
		t.Run(tc.id+tc.attempt, func(t *testing.T) {
			admission := liveAdmissionForRequest(takeoverRequest(tc.id, tc.attempt, "http://127.0.0.1/synthetic"), profiles, "http://127.0.0.1/synthetic")
			if (admission.Policy.AllowLive && admission.Policy.Authorized != nil && admission.Policy.Authorized()) != tc.allowed {
				t.Fatal("client identity bypassed server policy")
			}
			if admission.Policy.AllowRecording {
				t.Fatal("live right granted recording interruption")
			}
		})
	}
	request := takeoverRequest("one", "synthetic-attempt-one", "http://127.0.0.1/synthetic")
	admission := liveAdmissionForRequest(request, profiles, "http://127.0.0.1/synthetic")
	p := profiles["one"]
	p.AllowLiveTakeover = false
	profiles["one"] = p
	if admission.Policy.Authorized() {
		t.Fatal("grant revocation was ignored")
	}
}

func TestLiveAdmissionSharesAndExpiredPINChecksCannotPreempt(t *testing.T) {
	profiles := liveAccountUsers{"one": {ID: "one", AccountID: "household", AllowLiveTakeover: true, AllowRecordingInterruption: true}}
	automatic := takeoverRequest("one", "synthetic-attempt-one", "http://127.0.0.1/synthetic")
	query := automatic.URL.Query()
	query.Set("automaticRetry", "1")
	automatic.URL.RawQuery = query.Encode()
	if admission := liveAdmissionForRequest(automatic, profiles, "http://127.0.0.1/synthetic"); admission.Owner.RevocationKey == "" || admission.Policy.AllowLive || admission.Policy.AllowRecording {
		t.Fatal("automatic retune lost revocation tracking or gained preemption")
	}
	scoped := takeoverRequest("one", "synthetic-attempt-one", "http://127.0.0.1/synthetic")
	scoped = scoped.WithContext(context.WithValue(scoped.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream, ScopeResource: "http://127.0.0.1/synthetic"}))
	if admission := liveAdmissionForRequest(scoped, profiles, "http://127.0.0.1/synthetic"); admission.Policy.AllowLive || admission.Policy.AllowRecording {
		t.Fatal("share gained takeover rights")
	}
	var admission *liveCapacityAdmission
	middleware := profileaccess.Middleware(func(string, string) bool { return true })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admission = liveAdmissionForRequest(r, profiles, "http://127.0.0.1/synthetic")
	}))
	request := takeoverRequest("one", "synthetic-attempt-one", "http://127.0.0.1/synthetic")
	request = profileaccess.WithResourceGuard(request, func(http.ResponseWriter, *http.Request, string) bool { return false })
	middleware.ServeHTTP(httptest.NewRecorder(), request)
	if admission == nil || admission.Policy.Authorized() {
		t.Fatal("current PIN guard was ignored")
	}
}

func TestLiveConfirmationIntentChangesWithSourceDeviceOrAttempt(t *testing.T) {
	profiles := liveAccountUsers{"one": {ID: "one", AccountID: "household", AllowRecordingInterruption: true}}
	first := takeoverRequest("one", "synthetic-attempt-one", "http://127.0.0.1/synthetic")
	baseline := liveAdmissionForRequest(first, profiles, "http://127.0.0.1/synthetic")
	for _, key := range []string{"sourceId", "clientId", "playAttemptId", "stremioStreamIndex"} {
		changed := first.Clone(first.Context())
		value := changed.URL.Query()
		value.Set(key, "synthetic-other-choice")
		changed.URL.RawQuery = value.Encode()
		next := liveAdmissionForRequest(changed, profiles, "http://127.0.0.1/synthetic")
		if next.Policy.Intent == baseline.Policy.Intent {
			t.Fatal("confirmation intent accepted a changed " + key)
		}
	}
	next := liveAdmissionForRequest(first, profiles, "http://127.0.0.1/another")
	if next.Policy.Intent == baseline.Policy.Intent || liveDigest("a\x00b", "c") == liveDigest("a", "b\x00c") {
		t.Fatal("source or digest coordinates collided")
	}
}

func TestRevokedHLSResourcesReturnGoneAfterOwnershipCheck(t *testing.T) {
	session := &HLSSession{ID: "revoked", ProfileID: "viewer", OriginalPath: "/synthetic.ts", IsLive: true, LiveRevoked: true}
	handler := &VideoHandler{hlsManager: &HLSManager{sessions: map[string]*HLSSession{"revoked": session}}}
	for _, handle := range []http.HandlerFunc{handler.ServeHLSPlaylist, handler.ServeHLSMasterPlaylist, handler.ServeHLSSegment, handler.KeepAliveHLSSession, handler.GetHLSSessionStatus, handler.ServeHLSLiveCaptions} {
		for _, owner := range []bool{true, false} {
			request := httptest.NewRequest("GET", "/api/video/hls/revoked/resource", nil)
			request = mux.SetURLVars(request, map[string]string{"sessionID": "revoked", "segment": "segment0.ts"})
			request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeyAccountID, "household"))
			response := httptest.NewRecorder()
			profileaccess.Middleware(func(string, string) bool { return owner })(handle).ServeHTTP(response, request)
			expected := http.StatusNotFound
			if owner {
				expected = http.StatusGone
			}
			if response.Code != expected {
				t.Fatalf("resource status %d, want %d", response.Code, expected)
			}
			if owner {
				var result map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result["code"] != "LIVE_STREAM_TAKEN_OVER" || result["retryAutomatically"] != false {
					t.Fatal("missing terminal takeover response")
				}
			}
		}
	}
}

func TestHLSOwnedTakeoverClosesPreviousInputAndRejectsOldAttempt(t *testing.T) {
	_, provider := newLiveTSServer(t, 0)
	manager := NewHLSManager(t.TempDir(), stubBinary(t, "true"), "", nil)
	bucket := t.Name()
	t.Cleanup(func() { manager.Shutdown(); waitCapacityReleased(t, bucket) })
	profiles := liveAccountUsers{"old": {ID: "old", AccountID: "household"}, "new": {ID: "new", AccountID: "household", AllowLiveTakeover: true}}
	create := func(profile, attempt, path string) (*HLSSession, error) {
		admission := liveAdmissionForRequest(takeoverRequest(profile, attempt, provider.URL+path), profiles, provider.URL+path)
		return manager.CreateLiveSession(context.Background(), provider.URL+path, "m3u", bucket, profile, "", "", "native", LiveTuningSettings{MaxStreams: 1, Capacity: admission}, false)
	}
	old, err := create("old", "synthetic-old-attempt", "/old.ts")
	if err != nil {
		t.Fatal(err)
	}
	current, err := create("new", "synthetic-new-attempt", "/new.ts")
	if err != nil {
		t.Fatal(err)
	}
	if current == nil || livecapacity.Default.Current(bucket) != 1 {
		t.Fatal("handoff did not reserve exactly one provider input")
	}
	old.mu.RLock()
	revoked, stopped := old.LiveRevoked, old.Stopped
	old.mu.RUnlock()
	if !revoked || !stopped {
		t.Fatal("old HLS playback remained authorized")
	}
	if _, err := create("old", "synthetic-old-attempt", "/old.ts"); !errors.Is(err, livecapacity.ErrTakenOver) {
		t.Fatal("old automatic retry was admitted")
	}
}

func TestAdmissionErrorsDoNotExposeForeignActivityOrInternalStopErrors(t *testing.T) {
	m := livecapacity.NewManager()
	old, err := m.AcquireOwned("private-bucket", "", 1, livecapacity.Owner{AccountID: "household", ProfileID: "viewer", Kind: livecapacity.KindLive, Stop: func(context.Context) error { return errors.New("private upstream detail") }})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	_, err = m.AcquireWithTakeover(context.Background(), "private-bucket", "", 1, livecapacity.Owner{AccountID: "household", ProfileID: "new", Kind: livecapacity.KindLive, RevocationKey: "synthetic-explicit-play"}, livecapacity.TakeoverPolicy{AccountID: "household", ProfileID: "new", Intent: "synthetic", AllowLive: true, Authorized: func() bool { return true }})
	response := httptest.NewRecorder()
	if !writeLiveAdmissionError(response, liveStreamTarget{}, err) || response.Code != http.StatusConflict {
		t.Fatal("missing safe shutdown error")
	}
	var payload map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &payload)
	if payload["code"] != "LIVE_TAKEOVER_STOP_FAILED" || payload["message"] == "private upstream detail" {
		t.Fatal("unsafe stop details returned")
	}
	response = httptest.NewRecorder()
	writeLiveAdmissionError(response, liveStreamTarget{}, &livecapacity.ConfirmationRequired{Token: "synthetic-token", RecordingCount: 1})
	if response.Code != http.StatusConflict || response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("confirmation was cached or misclassified")
	}
}
