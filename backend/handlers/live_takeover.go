package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/internal/profileaccess"
	"novastream/models"
)

var validLiveAttempt = regexp.MustCompile(`^[a-zA-Z0-9_-]{16,80}$`)

// Clients retain one nonce for automatic retries and choose a fresh nonce only
// for an explicit Play action. Legacy callers remain counted and stoppable but
// cannot silently preempt another stream without this retry-revocation contract.
type liveCapacityAdmission struct {
	Owner   livecapacity.Owner
	Policy  livecapacity.TakeoverPolicy
	Current func() bool
}

func liveDigest(parts ...string) string {
	encoded, _ := json.Marshal(parts)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// No response body from a repeated ownership/PIN check is exposed or retained.
type liveAccessSink struct{ header http.Header }

func (s liveAccessSink) Header() http.Header            { return s.header }
func (s liveAccessSink) Write(data []byte) (int, error) { return len(data), nil }
func (s liveAccessSink) WriteHeader(int)                {}

func liveAdmissionForRequest(r *http.Request, users UsersProvider, input string) *liveCapacityAdmission {
	result := &liveCapacityAdmission{}
	id := strings.TrimSpace(r.URL.Query().Get("profileId"))
	if users == nil || id == "" || auth.GetAccountID(r) == "" {
		return result
	}
	profile, ok := users.Get(id)
	if !ok || profile.IsKidsProfile || profile.AccountID == "" || profile.AccountID != auth.GetAccountID(r) {
		return result
	}
	result.Owner = livecapacity.Owner{AccountID: profile.AccountID, ProfileID: profile.ID, Kind: livecapacity.KindLive}
	attempt := r.URL.Query().Get("playAttemptId")
	if !validLiveAttempt.MatchString(attempt) {
		return result
	}
	result.Owner.RevocationKey = liveDigest(profile.AccountID, profile.ID, requestClientID(r), attempt)
	// Explicitly marked retunes keep revocation tracking but never displace
	// another input, even after the bounded admission history has expired.
	if parseOptionalBool(r.URL.Query().Get("automaticRetry")) {
		return result
	}
	if session, ok := auth.GetSession(r); ok && session.Scope != "" {
		return result
	}
	result.Policy = livecapacity.TakeoverPolicy{
		AccountID: profile.AccountID, ProfileID: profile.ID,
		Intent:    liveDigest(result.Owner.RevocationKey, input, r.URL.Query().Get("sourceId"), r.URL.Query().Get("stremioStreamIndex")),
		AllowLive: profile.AllowLiveTakeover, AllowRecording: profile.AllowRecordingInterruption,
		Confirmation: strings.TrimSpace(r.Header.Get("X-Live-Recording-Confirmation")),
		Authorized: func() bool {
			current, ok := users.Get(id)
			return ok && current.AccountID == profile.AccountID && !current.IsKidsProfile &&
				current.AllowLiveTakeover == profile.AllowLiveTakeover && current.AllowRecordingInterruption == profile.AllowRecordingInterruption &&
				profileaccess.CheckResource(liveAccessSink{header: make(http.Header)}, r, id, input)
		},
	}
	return result
}

type liveAdmissionPreferences interface {
	Get(string) (*models.UserSettings, error)
}

// Revalidate local source assignments after a handoff wait. This reads only
// local settings; it never refetches a playlist or probes a provider.
func (a *liveCapacityAdmission) bindSettings(r *http.Request, manager ConfigProvider, preferences liveAdmissionPreferences) {
	stamp := func() (string, bool) {
		var live config.LiveSettings
		if manager != nil {
			settings, err := manager.Load()
			if err != nil {
				return "", false
			}
			live = settings.Live
		}
		var personal models.LiveTVSettings
		if preferences != nil && r.URL.Query().Get("profileId") != "" {
			settings, err := preferences.Get(r.URL.Query().Get("profileId"))
			if err != nil {
				return "", false
			}
			if settings != nil {
				personal = settings.LiveTV
			}
		}
		encoded, err := json.Marshal(struct {
			Global  config.LiveSettings
			Profile models.LiveTVSettings
		}{live, personal})
		return liveDigest(string(encoded)), err == nil
	}
	initial, ok := stamp()
	a.Current = func() bool { current, valid := stamp(); return ok && valid && initial == current }
	authorized := a.Policy.Authorized
	if authorized != nil {
		a.Policy.Authorized = func() bool { return a.Current() && authorized() }
	}
}

func (a *liveCapacityAdmission) acquire(ctx context.Context, key, share string, limit int) (*livecapacity.Lease, error) {
	if a == nil {
		return livecapacity.Default.Acquire(key, share, limit)
	}
	// Handoff never waits indefinitely for a broken player or provider input.
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return livecapacity.Default.AcquireWithTakeover(bounded, key, share, limit, a.Owner, a.Policy)
}

func writeLiveAdmissionError(w http.ResponseWriter, target liveStreamTarget, err error) bool {
	var confirmation *livecapacity.ConfirmationRequired
	var shutdown *livecapacity.ShutdownError
	status := 0
	payload := map[string]any{}
	switch {
	case errors.As(err, &confirmation):
		status = http.StatusConflict
		payload = map[string]any{"code": "LIVE_RECORDING_CONFIRMATION_REQUIRED", "message": "A recording is running. Continuing will stop it and keep the recorded portion as incomplete.", "confirmationToken": confirmation.Token, "recordingCount": confirmation.RecordingCount, "expiresInSeconds": 30}
	case errors.As(err, &shutdown):
		status = http.StatusConflict
		payload = map[string]any{"code": "LIVE_TAKEOVER_STOP_FAILED", "message": shutdown.Error()}
	case errors.Is(err, livecapacity.ErrTakenOver):
		status = http.StatusGone
		payload = map[string]any{"code": "LIVE_STREAM_TAKEN_OVER", "message": livecapacity.ErrTakenOver.Error(), "retryAutomatically": false}
	case errors.Is(err, livecapacity.ErrConfirmationExpired):
		status = http.StatusConflict
		payload = map[string]any{"code": "LIVE_RECORDING_CONFIRMATION_EXPIRED", "message": err.Error()}
	case errors.Is(err, livecapacity.ErrPermission):
		status = http.StatusForbidden
		payload = map[string]any{"code": "LIVE_TAKEOVER_PERMISSION_CHANGED", "message": err.Error()}
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusConflict
		payload = map[string]any{"code": "LIVE_TAKEOVER_PENDING_CLOSE", "message": "The previous input has not closed. No additional provider connection was opened."}
	case errors.Is(err, livecapacity.ErrLimit):
		writeLiveCapacityError(w, target)
		return true
	default:
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
	return true
}

func revokeLiveSession(session *HLSSession) {
	session.mu.Lock()
	session.LiveRevoked = true
	session.Stopped = true
	session.Completed = true
	session.FatalError = livecapacity.ErrTakenOver.Error()
	session.FatalErrorTime = time.Now()
	cancel := session.Cancel
	session.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
