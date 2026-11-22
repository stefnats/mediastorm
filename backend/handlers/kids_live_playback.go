package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"novastream/internal/apierror"
	"novastream/models"
	"novastream/services/epg"
)

// A programme is bound to one selected stream and one deliberately selected
// cached guide. It describes that guide's claim, not an independent FSK check.
type kidsLiveProgramme struct {
	channel LiveChannel
	policy  models.KidsLiveAgePolicy
	program models.EPGProgram
	age     int
	guide   *epg.Service
	retire  sync.Once
}

func kidsLiveDeny(w http.ResponseWriter, reason string) {
	apierror.Write(w, http.StatusForbidden, "kids_live_programme_unavailable", reason)
}

func (h *VideoHandler) isKidsLiveProfile(r *http.Request) bool {
	if h == nil || h.usersSvc == nil {
		return false
	}
	profile, ok := h.usersSvc.Get(strings.TrimSpace(r.URL.Query().Get("profileId")))
	return ok && profile.IsKidsProfile
}

func (h *VideoHandler) kidsLivePolicy(profileID string) (models.KidsLiveAgePolicy, error) {
	if h == nil || h.userSettingsSvc == nil {
		return models.KidsLiveAgePolicy{}, fmt.Errorf("Live settings unavailable")
	}
	settings, err := h.userSettingsSvc.Get(profileID)
	if err != nil || settings == nil {
		return models.KidsLiveAgePolicy{}, fmt.Errorf("programme-limited Live TV is disabled")
	}
	policy := settings.LiveTV.KidsAgeLimit
	if !policy.Enabled || !policy.Valid() {
		return policy, fmt.Errorf("programme-limited Live TV is disabled")
	}
	return policy, nil
}

func (p *kidsLiveProgramme) current(now time.Time) bool {
	if p == nil || p.guide == nil || !now.Before(p.program.Stop) {
		return false
	}
	decision := p.guide.CurrentProgramAgeDecisionForSource(p.channel.TvgID, p.policy.GuideSourceID, p.policy.MaximumAge, now)
	program := decision.Program
	return decision.State == "within_limit" && decision.Age != nil && *decision.Age == p.age && program != nil &&
		program.ChannelID == p.program.ChannelID && program.Title == p.program.Title && program.Episode == p.program.Episode &&
		program.Start.Equal(p.program.Start) && program.Stop.Equal(p.program.Stop)
}

// Remaining milliseconds are measured when answering, so clients can use a
// monotonic deadline anchored at the start of their HTTP request.
func (p *kidsLiveProgramme) remainingMillis(now time.Time) int64 {
	remaining := p.program.Stop.Sub(now).Milliseconds()
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (p *kidsLiveProgramme) description() any {
	return struct {
		Title               string                   `json:"title"`
		StartsAt            time.Time                `json:"startsAt"`
		EndsAt              time.Time                `json:"endsAt"`
		Age                 int                      `json:"age"`
		Country             string                   `json:"country"`
		Guide               *models.EPGProgramSource `json:"guide"`
		ClassificationBasis string                   `json:"classificationBasis"`
	}{p.program.Title, p.program.Start, p.program.Stop, p.age, "DE", p.program.Source, "selected_guide_claim"}
}

// StartKidsLiveHLSSession accepts a catalog ID, never a playback URL or a
// caller-supplied rating. Ordinary raw/direct/catch-up routes remain blocked.
func (h *VideoHandler) StartKidsLiveHLSSession(w http.ResponseWriter, r *http.Request) {
	if !h.isKidsLiveProfile(r) {
		kidsLiveDeny(w, "a kids profile is required")
		return
	}
	query := r.URL.Query()
	if query.Get("url") != "" || query.Get("timeshift") != "" || query.Get("start") != "" || query.Get("offset") != "" || query.Get("format") != "" && query.Get("format") != "hls" {
		kidsLiveDeny(w, "only current programme-limited HLS playback is supported")
		return
	}
	profileID := strings.TrimSpace(query.Get("profileId"))
	policy, err := h.kidsLivePolicy(profileID)
	if err != nil {
		kidsLiveDeny(w, err.Error())
		return
	}
	channelID := strings.TrimSpace(query.Get("channelId"))
	if channelID == "" {
		http.Error(w, "channelId is required", 400)
		return
	}
	channels, err := visibleLiveChannels(r, h.liveChannels, profileID)
	if err != nil {
		http.Error(w, "channel catalogue unavailable", 503)
		return
	}
	var selected *LiveChannel
	for _, channel := range channels {
		// Do not infer a station from the URL, display name, or an approximate alias.
		if channel.ID != channelID {
			continue
		}
		if selected != nil {
			kidsLiveDeny(w, "channel identity is ambiguous")
			return
		}
		copy := channel
		selected = &copy
	}
	if selected == nil || selected.TvgID == "" || selected.URL == "" || query.Get("sourceId") != "" && query.Get("sourceId") != selected.SourceID {
		kidsLiveDeny(w, "channel is not available with an unambiguous guide mapping")
		return
	}
	target := h.resolveLiveStreamTargetForSource(profileID, selected.SourceID)
	// These sources already contain the exact playable URL. Add-on/portal
	// resolution and automatic cross-source failover stay outside this first cut.
	if target.Provider != "m3u" && target.Provider != "xtream" && target.Provider != "hdhomerun" {
		kidsLiveDeny(w, "this channel source does not support programme-limited playback")
		return
	}
	decision := h.kidsLiveEPG.CurrentProgramAgeDecisionForSource(selected.TvgID, policy.GuideSourceID, policy.MaximumAge, time.Now().UTC())
	if decision.State != "within_limit" || decision.Program == nil || decision.Age == nil {
		kidsLiveDeny(w, "the chosen guide has no unique current FSK programme within the age limit")
		return
	}
	programme := &kidsLiveProgramme{channel: *selected, policy: policy, program: *decision.Program, age: *decision.Age, guide: h.kidsLiveEPG}
	// Keep the actual catalog source through the existing start pipeline. Two
	// allowed sources can share a URL while having different tuning or limits.
	bound := r.Clone(r.Context())
	boundURL := *r.URL
	boundQuery := boundURL.Query()
	boundQuery.Set("sourceId", selected.SourceID)
	boundURL.RawQuery = boundQuery.Encode()
	bound.URL = &boundURL
	h.startLiveHLSSession(w, bound, programme)
}

// Each child tune has its own new HLS session. No adult session is adopted, and
// releasing this session cannot stop another viewer's session.
func (m *HLSManager) armKidsProgrammeEnd(session *HLSSession) {
	session.mu.Lock()
	if session.kidsProgramme == nil || session.Stopped {
		session.mu.Unlock()
		return
	}
	end := session.kidsProgramme.program.Stop
	session.kidsProgrammeTimer = time.AfterFunc(time.Until(end), func() {
		m.mu.RLock()
		current := m.sessions[session.ID] == session
		m.mu.RUnlock()
		if current {
			m.CleanupSession(session.ID)
		}
	})
	session.mu.Unlock()
}

func (h *VideoHandler) checkKidsLiveSession(w http.ResponseWriter, r *http.Request, session *HLSSession) bool {
	session.mu.RLock()
	live, profileID, programme := session.IsLive, session.ProfileID, session.kidsProgramme
	session.mu.RUnlock()
	if !live {
		return true
	}
	profile, exists := models.User{}, false
	if h.usersSvc != nil {
		profile, exists = h.usersSvc.Get(profileID)
	}
	if programme == nil {
		if exists && profile.IsKidsProfile {
			kidsLiveDeny(w, "an adult Live session cannot be used for kids playback")
			return false
		}
		return true
	}
	// Explicit stop is always permitted after the normal profile ownership check.
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/stop") {
		return true
	}
	path := r.URL.Path
	if strings.HasSuffix(path, "/seek") || strings.Contains(path, "/subtitle") || strings.Contains(path, "/captions") || strings.HasSuffix(path, "/cc-status") {
		kidsLiveDeny(w, "only the current programme playlist and segments are available")
		return false
	}
	policy, err := h.kidsLivePolicy(profileID)
	if !exists || !profile.IsKidsProfile || err != nil || policy != programme.policy || !programme.current(time.Now().UTC()) {
		// Do not synchronously join HLS while its request may hold an output reference.
		// Revocation stops new media immediately; normal detached cleanup joins it.
		programme.retire.Do(func() {
			if session.Cancel != nil {
				session.Cancel()
			}
			go h.hlsManager.CleanupSession(session.ID)
		})
		kidsLiveDeny(w, "the selected programme ended or its guide information changed")
		return false
	}
	return true
}

// KidsLiveChannels exposes catalog identities and cached guide claims only.
// Provider addresses, source credentials, and unproxied logos never leave here.
func (h *VideoHandler) KidsLiveChannels(w http.ResponseWriter, r *http.Request) {
	if !h.isKidsLiveProfile(r) {
		kidsLiveDeny(w, "a kids profile is required")
		return
	}
	profileID := strings.TrimSpace(r.URL.Query().Get("profileId"))
	policy, err := h.kidsLivePolicy(profileID)
	if err != nil {
		kidsLiveDeny(w, err.Error())
		return
	}
	channels, err := visibleLiveChannels(r, h.liveChannels, profileID)
	if err != nil {
		http.Error(w, "channel catalogue unavailable", 503)
		return
	}
	type channelView struct {
		ID              string `json:"id"`
		Name            string `json:"name"`
		ProgrammeState  string `json:"programmeState"`
		ProgrammeReason string `json:"programmeReason"`
		Programme       any    `json:"programme,omitempty"`
	}
	counts := map[string]int{}
	for _, channel := range channels {
		counts[channel.ID]++
	}
	result := []channelView{}
	now := time.Now().UTC()
	for _, channel := range channels {
		if channel.ID == "" || counts[channel.ID] != 1 {
			continue
		}
		target := h.resolveLiveStreamTargetForSource(profileID, channel.SourceID)
		if target.Provider != "m3u" && target.Provider != "xtream" && target.Provider != "hdhomerun" {
			continue
		}
		decision := h.kidsLiveEPG.CurrentProgramAgeDecisionForSource(channel.TvgID, policy.GuideSourceID, policy.MaximumAge, now)
		view := channelView{ID: channel.ID, Name: channel.Name, ProgrammeState: decision.State, ProgrammeReason: decision.Reason}
		if decision.Program != nil && decision.Age != nil {
			description := &kidsLiveProgramme{program: *decision.Program, age: *decision.Age}
			view.Programme = description.description()
		}
		result = append(result, view)
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Channels []channelView `json:"channels"`
		Total    int           `json:"total"`
	}{result, len(result)})
}
