package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/models"
	"novastream/services/recordings"
)

type recordingPlanner interface {
	Plan(context.Context, string, *models.Recording) (models.RecordingPlan, error)
}

// PlanRecordingCapacity uses the same source identity, alias cap and profile
// routing as actual recorder admission, without acquiring or opening an input.
func (h *LiveHandler) PlanRecordingCapacity(ctx context.Context, job models.Recording) (recordings.RecordingCapacity, error) {
	if h.usersSvc == nil {
		return recordings.RecordingCapacity{}, errors.New("recording profile unavailable")
	}
	profile, ok := h.usersSvc.Get(job.UserID)
	if !ok || profile.IsKidsProfile {
		return recordings.RecordingCapacity{}, errors.New("recording profile unavailable")
	}
	if h.cfgManager == nil {
		return recordings.RecordingCapacity{}, errors.New("recording provider unavailable")
	}
	settings, err := h.cfgManager.Load()
	if err != nil {
		return recordings.RecordingCapacity{}, errors.New("recording provider unavailable")
	}
	resolved := h.resolveProfileLiveSourceForID(job.UserID, settings)
	if len(resolvedLiveSources(resolved)) == 0 {
		return recordings.RecordingCapacity{}, errors.New("recording provider unavailable")
	}
	// This internal read assesses a persisted job's own routing, including jobs
	// owned by other accounts. The HTTP response remains scoped to its caller.
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, profile.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/api/live/channels", nil)
	query := r.URL.Query()
	query.Set("profileId", job.UserID)
	r.URL.RawQuery = query.Encode()
	target, err := resolveLiveCapacityTarget(r, job.SourceURL, settings, resolved, h)
	if err != nil {
		return recordings.RecordingCapacity{}, errors.New("recording provider unavailable")
	}
	_, limit := livecapacity.Default.Usage(target.BucketKey, target.MaxStreams)
	return recordings.RecordingCapacity{Key: target.BucketKey, Limit: limit}, nil
}

func (h *RecordingsHandler) planningProfile(w http.ResponseWriter, r *http.Request, id string) (string, bool) {
	if auth.GetSessionScope(r) != "" {
		http.Error(w, "recording planning requires a full account session", http.StatusForbidden)
		return "", false
	}
	id = strings.TrimSpace(id)
	if !auth.IsMaster(r) {
		var ok bool
		id, ok = h.requireProfileOwnership(w, r, id)
		if !ok {
			return "", false
		}
	} else if !h.requireExistingProfile(w, id) {
		return "", false
	}
	// Cookie routes do not use the bearer API's kids middleware.
	if h.users == nil {
		http.Error(w, "profile unavailable", http.StatusServiceUnavailable)
		return "", false
	}
	for _, profile := range h.users.ListAll() {
		if profile.ID == id {
			if profile.IsKidsProfile {
				http.Error(w, "Live TV is unavailable for kids profiles", http.StatusForbidden)
				return "", false
			}
			return id, true
		}
	}
	http.Error(w, "profile not found", http.StatusNotFound)
	return "", false
}

func (h *RecordingsHandler) Plan(w http.ResponseWriter, r *http.Request) {
	id, ok := h.planningProfile(w, r, r.URL.Query().Get("profileId"))
	if !ok {
		return
	}
	h.writeRecordingPlan(w, r, id, nil)
}

func (h *RecordingsHandler) Preview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	var payload createRecordingPayload
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		http.Error(w, "invalid preview body", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid preview body", http.StatusBadRequest)
		return
	}
	id, ok := h.planningProfile(w, r, payload.ProfileID)
	if !ok {
		return
	}
	start, startErr := time.Parse(time.RFC3339, payload.Start)
	end, endErr := time.Parse(time.RFC3339, payload.Stop)
	if startErr != nil || endErr != nil || !end.After(start) || !end.After(time.Now()) {
		http.Error(w, "invalid preview time window", http.StatusBadRequest)
		return
	}
	before, after := h.defaultRecordingPadding(id)
	if payload.PaddingBeforeSeconds != nil {
		before = *payload.PaddingBeforeSeconds
	}
	if payload.PaddingAfterSeconds != nil {
		after = *payload.PaddingAfterSeconds
	}
	if before < 0 || before > 3600 || after < 0 || after > 3600 {
		http.Error(w, "recording buffers must be between 0 and 60 minutes", http.StatusBadRequest)
		return
	}
	// Resolve the visible channel even when the optional account assignment
	// policy is disabled. Never forecast a caller-selected provider URL.
	channels, err := visibleLiveChannels(r, h.liveChannels, id)
	if err != nil {
		http.Error(w, "channel unavailable", http.StatusServiceUnavailable)
		return
	}
	for _, channel := range channels {
		if !liveChannelMatches(channel, payload.ChannelID) {
			continue
		}
		job := models.Recording{UserID: id, ChannelID: channel.ID, TvgID: channel.TvgID, SourceURL: channel.URL,
			StartAt: start.UTC(), EndAt: end.UTC(), PaddingBeforeSeconds: before, PaddingAfterSeconds: after}
		h.writeRecordingPlan(w, r, id, &job)
		return
	}
	http.Error(w, "channel not found", http.StatusNotFound)
}

func (h *RecordingsHandler) writeRecordingPlan(w http.ResponseWriter, r *http.Request, id string, candidate *models.Recording) {
	planner, ok := h.service.(recordingPlanner)
	if !ok {
		http.Error(w, "recording plan unavailable", http.StatusServiceUnavailable)
		return
	}
	plan, err := planner.Plan(r.Context(), id, candidate)
	if err != nil {
		http.Error(w, "recording plan unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(plan)
}
