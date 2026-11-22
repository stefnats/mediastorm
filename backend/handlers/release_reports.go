package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/history"
	"novastream/services/kidsvod"
	"novastream/services/playback"
	"novastream/utils/language"
)

type releaseReportRequest struct {
	AudioLanguage   string  `json:"audioLanguage,omitempty"`
	Reason          string  `json:"reason"`
	SourceVersion   string  `json:"sourceVersion"`
	AudioTrackIndex *int    `json:"audioTrackIndex,omitempty"`
	Retry           bool    `json:"retry"`
	ResetHistory    bool    `json:"resetHistory,omitempty"`
	StartOffset     float64 `json:"startOffset,omitempty"`
	SkipHLS         bool    `json:"skipHLS,omitempty"`
	ClientID        string  `json:"clientId,omitempty"`
}

func sourceReportVersion(entry *playback.PrequeueEntry) string {
	data, _ := json.Marshal([]any{entry.ID, entry.StreamPath, entry.SelectedResult, entry.FileSize, entry.AudioTracks, entry.SelectedAudioTrack})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *PrequeueHandler) reportableEntry(w http.ResponseWriter, r *http.Request) *playback.PrequeueEntry {
	if h == nil || h.store == nil || h.badStreamsSvc == nil {
		http.Error(w, "release reports unavailable", http.StatusServiceUnavailable)
		return nil
	}
	if session, ok := auth.GetSession(r); auth.IsShareLinkRequest(r) || (ok && session.Scope == models.SessionScopeStream) {
		http.Error(w, "release reports require a profile session", http.StatusForbidden)
		return nil
	}
	_, owner, permitted := h.vodEntry(w, r, strings.TrimSpace(mux.Vars(r)["prequeueID"]), vodReady)
	if !permitted {
		return nil
	}
	defer owner.release()
	r = h.vodBoundary.checkedRequest(r, owner)
	if !h.vodBoundary.recheckHTTP(w, r, owner, vodReady) {
		return nil
	}
	entry, ok := h.store.Get(strings.TrimSpace(mux.Vars(r)["prequeueID"]))
	if !ok {
		http.NotFound(w, r)
		return nil
	}
	if !h.canAccessUser(r, entry.UserID) {
		http.NotFound(w, r)
		return nil
	}
	if !profileaccess.CheckResource(w, r, entry.UserID, entry.StreamPath) {
		return nil
	}
	if libraryID := reportLibraryID(entry); libraryID != "" {
		if h.localAccess == nil {
			http.NotFound(w, r)
			return nil
		}
		allowed, err := h.localAccess.CanAccess(r.Context(), libraryID, auth.GetAccountID(r), entry.UserID, auth.IsMaster(r))
		if err != nil || !allowed {
			http.NotFound(w, r)
			return nil
		}
	}
	if entry.SelectedResult == nil || entry.StreamPath == "" || entry.Status != playback.PrequeueStatusReady {
		http.Error(w, "a prepared selected release is required", http.StatusConflict)
		return nil
	}
	if !h.vodBoundary.recheckHTTP(w, r, owner, vodReady) {
		return nil
	}
	return entry
}

func (h *PrequeueHandler) canShareReleaseReport(r *http.Request, userID string) bool {
	if !auth.IsMaster(r) {
		return false
	}
	profiles, ok := h.users.(interface {
		Get(string) (models.User, bool)
	})
	if !ok {
		return false
	}
	profile, exists := profiles.Get(userID)
	return exists && !profile.IsKidsProfile
}

// ReleaseReportOptions binds the dialog to the server's actual selected source,
// rather than accepting a caller-supplied release name or private URL.
func (h *PrequeueHandler) ReleaseReportOptions(w http.ResponseWriter, r *http.Request) {
	entry := h.reportableEntry(w, r)
	if entry == nil {
		return
	}
	actionOwner, permitted := h.vodBoundary.requestOwner(w, r, vodReady, entry.UserID)
	if !permitted {
		return
	}
	defer actionOwner.release()
	r = h.vodBoundary.checkedRequest(r, actionOwner)

	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	ctx := h.vodReportContext(entry, client)
	if !h.vodBoundary.recheckHTTP(w, r, actionOwner, vodReady) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"sourceVersion": sourceReportVersion(entry), "releaseName": entry.SelectedResult.Title, "audioLanguage": ctx.AudioLanguage, "audioTracks": entry.AudioTracks, "selectedAudioTrack": entry.SelectedAudioTrack, "canShare": h.canShareReleaseReport(r, entry.UserID), "hasEpisode": entry.TargetEpisode != nil, "canResetHistory": h.historySvc != nil})
}

func (h *PrequeueHandler) ReportRelease(w http.ResponseWriter, r *http.Request) {
	h.releaseAction(w, r, true)
}
func (h *PrequeueHandler) RetryRelease(w http.ResponseWriter, r *http.Request) {
	h.releaseAction(w, r, false)
}
func (h *PrequeueHandler) releaseAction(w http.ResponseWriter, r *http.Request, reporting bool) {
	r, bodyOK := h.vodBoundary.boundBody(w, r)
	if !bodyOK {
		return
	}
	entry := h.reportableEntry(w, r)
	if entry == nil {
		return
	}
	var req releaseReportRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid release report", http.StatusBadRequest)
		return
	}
	if req.ResetHistory && (!reporting || (req.Reason != "wrong_title" && req.Reason != "wrong_episode")) {
		http.Error(w, "history reset requires a wrong-content report", http.StatusBadRequest)
		return
	}
	if req.ResetHistory && h.historySvc == nil {
		http.Error(w, "history reset unavailable", http.StatusServiceUnavailable)
		return
	}
	if req.SourceVersion == "" || req.SourceVersion != sourceReportVersion(entry) {
		http.Error(w, "the selected source changed; reopen the report dialog", http.StatusConflict)
		return
	}
	client := normalizeClientID(req.ClientID)
	if client == "" {
		client = normalizeClientID(r.Header.Get("X-Client-ID"))
	}
	operation := vodReady
	if !reporting || req.Retry {
		operation = vodDemand
	}
	actionOwner, permitted := h.vodBoundary.requestOwner(w, r, operation, entry.UserID)
	if !permitted {
		return
	}
	defer actionOwner.release()
	if !h.vodBoundary.selectClient(w, actionOwner, client) || !h.vodBoundary.recheckHTTP(w, r, actionOwner, operation) {
		return
	}
	if actionOwner != nil {
		client = actionOwner.client
		r = h.vodBoundary.checkedRequest(r, actionOwner)
	}

	ctx := h.vodReportContext(entry, client)
	if reporting && req.Reason == "missing_language" && (req.AudioLanguage == "" || language.NormalizeToCode(req.AudioLanguage) != language.NormalizeToCode(ctx.AudioLanguage)) {
		http.Error(w, "audio preference changed; reopen the report dialog", http.StatusConflict)
		return
	}

	if req.Reason == "audio_problem" {
		index := entry.SelectedAudioTrack
		if req.AudioTrackIndex != nil {
			index = *req.AudioTrackIndex
		}
		known := false
		for _, track := range entry.AudioTracks {
			if track.Index == index {
				known = true
				break
			}
		}
		if !known {
			http.Error(w, "choose an audio track from the selected release", http.StatusBadRequest)
			return
		}
		ctx.AudioTrackIndex = &index
	}
	response := map[string]any{}
	if reporting {
		report, err := h.badStreamsSvc.Report(badstreams.ReportRequest{Result: *entry.SelectedResult, Context: ctx, Reason: req.Reason, ReporterID: entry.UserID, Shared: h.canShareReleaseReport(r, entry.UserID)})
		if err != nil {
			var pathError *os.PathError
			if errors.As(err, &pathError) {
				http.Error(w, "could not save the release report", http.StatusServiceUnavailable)
			} else {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			return
		}
		response["report"] = report
		if req.ResetHistory {
			if err := h.resetReportedHistory(entry); err != nil {
				// The report is already saved. Keep this partial outcome explicit,
				// and do not prepare a replacement until the reset is confirmed.
				response["historyReset"] = false
				response["historyResetError"] = "history_reset_failed"
				if !h.vodBoundary.recheckHTTP(w, r, actionOwner, operation) {
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
				return
			}
			response["historyReset"] = true
		}
	} else {
		if req.Reason != "" {
			http.Error(w, "a manual retry does not submit a report", http.StatusBadRequest)
			return
		}
		req.Retry = true
	}

	if req.Retry {
		owner := actionOwner
		if !h.vodBoundary.selectClient(w, owner, client) || !h.vodBoundary.recheckHTTP(w, r, owner, vodDemand) {
			return
		}
		offset := req.StartOffset
		if offset < 0 || req.Reason == "wrong_title" || req.Reason == "wrong_episode" {
			offset = 0
		}
		if entry.Duration > 0 && offset > entry.Duration {
			offset = 0
		}
		h.store.CancelWork(entry.ID)
		scope, _ := h.prequeueSettingsScope(entry.UserID, client, entry.TitleID, adaptiveThroughputFromRequest(r))
		scope = automaticLibraryScope(scope, entry.AutomaticLibrarySelection)
		next, created, createErr := h.store.CreateReportReplacement(entry.ID, scope)
		if createErr != nil {
			http.Error(w, "could not prepare an alternative", http.StatusServiceUnavailable)
			return
		}
		h.latencyTracker.NotePrequeueRequested(next.ID, entry.TitleID, entry.UserID, entry.TitleName, entry.MediaType)
		if created {
			work, workErr := h.beginPrequeueWork(context.Background(), next.ID)
			if workErr != nil {
				http.Error(w, "could not prepare an alternative", http.StatusServiceUnavailable)
				return
			}
			setup, stopSetup := prequeueSetupContext(r.Context(), work)
			bound := h.vodBoundary.bindWorkerContext(setup, work.ctx, next.ID, owner, r)
			stopSetup()
			if !bound {
				work.finish()
				vodDeny(w, kidsvod.CodeContextChanged)
				return
			}
			finishOutput, outputErr := owner.originalHTTPOutput(w, r)
			if outputErr != nil || work.ctx.Err() != nil {
				// The saved report remains saved even when its separate retry cannot start.
				http.Error(w, "could not prepare an alternative", http.StatusServiceUnavailable)
				if finishOutput != nil {
					finishOutput()
				}
				h.vodBoundary.discardBoundWorker(next.ID)
				work.finish()
				return
			}
			defer finishOutput()
			throughput := adaptiveThroughputFromRequest(r)
			local := prequeueLocalScope{accountID: auth.GetAccountID(r), master: auth.IsMaster(r), automaticLibrarySelection: entry.AutomaticLibrarySelection}
			if !reporting {
				result := *entry.SelectedResult
				local.excludedResult = &result
			}
			go func() {
				defer work.finish()
				h.runPrequeueWorkerJob(work, next.ID, entry.TitleID, entry.TitleName, "", entry.MediaType, entry.Year, entry.UserID, client, throughput, entry.TargetEpisode, offset, req.SkipHLS, prequeueWorkerInteractive, local)
			}()
		}
		response["replacement"] = playback.PrequeueResponse{PrequeueID: next.ID, TargetEpisode: entry.TargetEpisode, Status: playback.PrequeueStatusQueued}
		response["resumePosition"] = offset
	}
	if !h.vodBoundary.recheckHTTP(w, r, actionOwner, operation) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (h *PrequeueHandler) CancelReleaseRetry(w http.ResponseWriter, r *http.Request) {
	if session, ok := auth.GetSession(r); auth.IsShareLinkRequest(r) || (ok && session.Scope == models.SessionScopeStream) {
		http.Error(w, "source changes require a profile session", http.StatusForbidden)
		return
	}
	id := strings.TrimSpace(mux.Vars(r)["prequeueID"])
	if h.vodBoundary != nil {
		entry, ok := h.vodCleanupEntry(w, r, id)
		if !ok {
			return
		}
		h.store.CancelWork(entry.ID)
		h.store.Delete(entry.ID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	entry, ok := h.store.Get(id)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !h.authorizeEntry(w, r, entry) || !profileaccess.CheckResource(w, r, entry.UserID, entry.StreamPath) {
		return
	}
	h.store.CancelWork(id)
	h.store.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

func reportLibraryID(entry *playback.PrequeueEntry) string {
	if entry.SelectedResult == nil {
		return ""
	}
	return entry.SelectedResult.Attributes["localLibraryId"]
}

func (h *PrequeueHandler) vodReportContext(entry *playback.PrequeueEntry, client string) badstreams.PlaybackContext {
	if h.vodBoundary == nil {
		return h.reportContextForPrequeue(entry.ID, entry.UserID, client, entry.TargetEpisode)
	}
	return reportContext(h.trackPreferenceResolver(), entry.UserID, client, entry.TitleID, entry.TargetEpisode)
}

// resetReportedHistory reuses manual unwatch semantics for exactly the requested
// movie or episode. There is no prior-position snapshot to roll back to.
func (h *PrequeueHandler) resetReportedHistory(entry *playback.PrequeueEntry) error {
	if strings.TrimSpace(entry.TitleID) == "" || entry.TargetEpisode == nil && entry.MediaType != "movie" {
		return fmt.Errorf("a movie or specific episode is required for history reset")
	}
	unwatched := false
	update := models.WatchHistoryUpdate{MediaType: "movie", ItemID: entry.TitleID, Name: entry.TitleName, Year: entry.Year, Watched: &unwatched}
	if episode := entry.TargetEpisode; episode != nil {
		update.MediaType = "episode"
		update.SeriesID, update.SeriesName = entry.TitleID, entry.TitleName
		update.SeasonNumber, update.EpisodeNumber = episode.SeasonNumber, episode.EpisodeNumber
		update.Name = episode.Title
		update.ItemID = episode.ItemID
		if update.ItemID == "" {
			update.ItemID = history.EpisodeItemID(entry.TitleID, nil, episode.SeasonNumber, episode.EpisodeNumber)
		}
		if episode.TvdbID != "" {
			update.ExternalIDs = map[string]string{"episodeTvdb": episode.TvdbID}
		}
	}
	_, err := h.historySvc.UpdateWatchHistory(entry.UserID, update)
	return err
}
