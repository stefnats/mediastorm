package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"novastream/models"
	"novastream/services/audiolanguage"
	"novastream/services/playback"
	"novastream/utils/language"
)

type audioLanguageChecks struct {
	mu       sync.Mutex
	endpoint string
	checker  *audiolanguage.Checker
}

type preparedAudioSampler interface {
	SamplePreparedAudio(context.Context, string, int, float64) ([]byte, error)
}

func (h *PrequeueHandler) audioLanguageChecker() (*audiolanguage.Checker, error) {
	if h == nil || h.configManager == nil {
		return nil, nil
	}
	settings, err := h.configManager.Load()
	if err != nil {
		return nil, errors.New("audio language settings unavailable")
	}
	endpoint := strings.TrimSpace(settings.Playback.AudioLanguageCheckURL)
	if endpoint == "" {
		return nil, nil
	}
	prequeueLifecycleInit.Lock()
	if h.audioLanguageChecks == nil {
		h.audioLanguageChecks = &audioLanguageChecks{}
	}
	state := h.audioLanguageChecks
	prequeueLifecycleInit.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.checker != nil && state.endpoint == endpoint {
		return state.checker, nil
	}
	checker, err := audiolanguage.New(endpoint)
	if err != nil {
		return nil, err
	}
	state.endpoint, state.checker = endpoint, checker
	return checker, nil
}

func (h *PrequeueHandler) audioLanguageCheckAvailable() bool {
	_, supported := h.fullProber.(preparedAudioSampler)
	checker, err := h.audioLanguageChecker()
	return supported && err == nil && checker != nil
}

func (h *PrequeueHandler) autoCheckAudioLanguageFor(profile string) bool {
	defaults := models.DefaultUserSettings()
	if h.configManager != nil {
		global, err := h.configManager.Load()
		if err != nil {
			return false
		}
		defaults.Playback.AutoCheckAudioLanguage = models.BoolPtr(global.Playback.AutoCheckAudioLanguage)
	}
	if h.userSettingsSvc == nil {
		return models.BoolVal(defaults.Playback.AutoCheckAudioLanguage, false)
	}
	settings, err := h.userSettingsSvc.GetWithDefaults(profile, defaults)
	return err == nil && models.BoolVal(settings.Playback.AutoCheckAudioLanguage, false)
}

type audioLanguageRequest struct {
	SourceVersion   string  `json:"sourceVersion"`
	AudioTrackIndex *int    `json:"audioTrackIndex"`
	StartOffset     float64 `json:"startOffset"`
}

// CheckAudioLanguage samples only an already selected source. It never searches
// for another release, saves an exclusion or replaces the source being played.
func (h *PrequeueHandler) CheckAudioLanguage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r, ok := h.vodBoundary.boundBody(w, r)
	if !ok {
		return
	}
	entry := h.reportableEntry(w, r)
	if entry == nil {
		return
	}
	var request audioLanguageRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.AudioTrackIndex == nil || !finiteAudioOffset(request.StartOffset) {
		http.Error(w, "invalid audio language check", http.StatusBadRequest)
		return
	}
	if request.SourceVersion == "" || request.SourceVersion != sourceReportVersion(entry) {
		http.Error(w, "the selected source changed; reopen audio controls", http.StatusConflict)
		return
	}
	knownTrack := false
	for _, track := range entry.AudioTracks {
		if track.Index == *request.AudioTrackIndex {
			knownTrack = true
			break
		}
	}
	if !knownTrack {
		http.Error(w, "choose an audio track from the selected release", http.StatusBadRequest)
		return
	}
	checker, err := h.audioLanguageChecker()
	sampler, supported := h.fullProber.(preparedAudioSampler)
	if err != nil || checker == nil || !supported {
		http.Error(w, "audio language checking is not configured", http.StatusServiceUnavailable)
		return
	}
	owner, permitted := h.vodBoundary.requestOwner(w, r, vodReady, entry.UserID)
	if !permitted {
		return
	}
	defer owner.release()
	r = h.vodBoundary.checkedRequest(r, owner)
	if !h.vodBoundary.recheckHTTP(w, r, owner, vodReady) {
		return
	}
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	wanted := h.vodReportContext(entry, client).AudioLanguage
	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Second)
	defer cancel()
	stop := context.AfterFunc(h.prequeueRuntimeContext(), cancel)
	defer stop()
	authorized, stale := true, false
	current := func() bool {
		latest, exists := h.store.Get(entry.ID)
		return exists && latest.Status == playback.PrequeueStatusReady && sourceReportVersion(latest) == request.SourceVersion && language.NormalizeToCode(h.vodReportContext(latest, client).AudioLanguage) == language.NormalizeToCode(wanted)
	}
	result := audiolanguage.Result{Status: "unknown", Reason: "insufficient_duration", RequestedLanguage: language.NormalizeToCode(wanted)}
	if entry.Duration >= audiolanguage.SampleSeparation+audiolanguage.SampleSeconds && finiteAudioOffset(entry.Duration) {
		start := math.Min(request.StartOffset, entry.Duration-audiolanguage.SampleSeparation-audiolanguage.SampleSeconds)
		result, err = checker.Check(ctx, wanted, start, func(ctx context.Context, offset float64) ([]byte, error) {
			if !h.vodBoundary.recheckHTTP(w, r, owner, vodReady) {
				authorized = false
				return nil, context.Canceled
			}
			if !current() {
				stale = true
				return nil, context.Canceled
			}
			return sampler.SamplePreparedAudio(ctx, entry.StreamPath, *request.AudioTrackIndex, offset)
		})
	}
	if !authorized {
		return
	}
	if !h.vodBoundary.recheckHTTP(w, r, owner, vodReady) {
		return
	}
	if stale || !current() {
		http.Error(w, "playback or audio preference changed; check the current selection", http.StatusConflict)
		return
	}
	if err != nil {
		if errors.Is(err, audiolanguage.ErrBusy) {
			http.Error(w, "an audio language check is already running", http.StatusTooManyRequests)
		} else {
			http.Error(w, "audio language check did not finish", http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		audiolanguage.Result
		SourceVersion   string `json:"sourceVersion"`
		AudioTrackIndex int    `json:"audioTrackIndex"`
	}{result, request.SourceVersion, *request.AudioTrackIndex})
}

func finiteAudioOffset(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
