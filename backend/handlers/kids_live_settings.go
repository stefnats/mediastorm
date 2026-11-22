package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"novastream/internal/auth"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/epg"
)

// ConfigureKidsLive attaches the existing guide and parental password verifier.
// This does not enable Live TV on any profile.
func (h *VideoHandler) ConfigureKidsLive(guide *EPGHandler, settings *UserSettingsHandler, accounts parentauth.Accounts) {
	if guide != nil {
		h.kidsLiveEPG = guide.epgService
	}
	if settings != nil {
		settings.kidsLiveEPG = h.kidsLiveEPG
		settings.kidsLiveParents = parentauth.New(accounts)
	}
}

func cachedGuideSelected(guide *epg.Service, id string) bool {
	for _, source := range guide.CachedProgrammeSources() {
		if source.ID == id {
			return true
		}
	}
	return false
}

// KidsLivePolicy uses the same owned-profile route and fresh parent password as
// the existing parental controls. The guide is consciously selected, not verified.
func (h *UserSettingsHandler) KidsLivePolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := h.requireUser(w, r)
	if !ok {
		return
	}
	profiles, available := h.Users.(interface {
		Get(string) (models.User, bool)
	})
	if !available {
		http.Error(w, "profile settings unavailable", 503)
		return
	}
	profile, exists := profiles.Get(id)
	if !exists || !auth.IsMaster(r) && profile.AccountID != auth.GetAccountID(r) {
		http.NotFound(w, r)
		return
	}
	if !profile.IsKidsProfile {
		http.Error(w, "a kids profile is required", 400)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodPut {
		var policy models.KidsLiveAgePolicy
		decoder := json.NewDecoder(io.LimitReader(r.Body, 4097))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&policy) != nil || decoder.Decode(new(any)) != io.EOF || !policy.Valid() {
			http.Error(w, "invalid Live programme age policy", 400)
			return
		}
		if !h.kidsLiveParents.RequireFresh(w, r) {
			return
		}
		if policy.Enabled && !cachedGuideSelected(h.kidsLiveEPG, policy.GuideSourceID) {
			http.Error(w, "select a guide already loaded in the programme cache", 400)
			return
		}
		writer, ok := h.Service.(interface {
			UpdateKidsLive(string, models.KidsLiveAgePolicy) error
		})
		if !ok {
			http.Error(w, "Live profile settings unavailable", 503)
			return
		}
		if err := writer.UpdateKidsLive(id, policy); err != nil {
			http.Error(w, "Live profile settings could not be saved", 503)
			return
		}
	}
	settings, err := h.Service.Get(id)
	if err != nil {
		http.Error(w, "Live profile settings unavailable", 503)
		return
	}
	policy := models.KidsLiveAgePolicy{Country: "DE", MaximumAge: 6}
	if settings != nil && settings.LiveTV.KidsAgeLimit.Valid() {
		policy = settings.LiveTV.KidsAgeLimit
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Policy              models.KidsLiveAgePolicy  `json:"policy"`
		GuideSources        []models.EPGProgramSource `json:"guideSources"`
		ClassificationBasis string                    `json:"classificationBasis"`
	}{policy, h.kidsLiveEPG.CachedProgrammeSources(), "selected_guide_claim"})
}
