package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/indexer"
	"novastream/services/mediaevidence"
	"novastream/services/playback"
	"novastream/services/sourcefilters"
)

type sourceFilterRequest struct {
	target sourcefilters.Target
	scope  sourcefilters.Scope
}

func (h *IndexerHandler) ConfigureSourceFilters(profile func(string) (models.User, bool), prepared *PrequeueHandler) {
	h.SourceFilters = sourcefilters.NewStore()
	h.SourceFilterProfile = profile
	h.SourceFilterPrepared = prepared
}

func sourceFilterError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	code := "invalid_source_filters"
	switch {
	case errors.Is(err, sourcefilters.ErrUnavailable):
		status = http.StatusNotFound
		code = err.Error()
	case errors.Is(err, sourcefilters.ErrSnapshotExpired):
		status = http.StatusGone
		code = err.Error()
	case errors.Is(err, sourcefilters.ErrContextChanged), errors.Is(err, sourcefilters.ErrViewChanged), errors.Is(err, sourcefilters.ErrViewExpired):
		status = http.StatusConflict
		code = err.Error()
	}
	apierror.Write(w, status, code, code)
}

func sourceFilterToken(value string) bool {
	if len(value) < 1 || len(value) > 1024 {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func sourceFilterTarget(r *http.Request) (sourcefilters.Target, error) {
	q := r.URL.Query()
	target := sourcefilters.Target{TitleID: q.Get("titleId"), MediaType: q.Get("mediaType")}
	if !sourceFilterToken(target.TitleID) || (target.MediaType != "movie" && target.MediaType != "series") {
		return target, errors.New("invalid target")
	}
	if q.Has("episodeId") || q.Has("seasonNumber") || q.Has("episodeNumber") || q.Has("episodeNumberingId") || q.Has("episodeOrdering") {
		s, err := strconv.Atoi(q.Get("seasonNumber"))
		if err != nil || s < 0 || s > 10000 {
			return target, errors.New("invalid season")
		}
		e, err := strconv.Atoi(q.Get("episodeNumber"))
		if err != nil || e < 1 || e > 100000 || target.MediaType != "series" {
			return target, errors.New("invalid episode")
		}
		ep := &models.EpisodeReference{EpisodeID: q.Get("episodeId"), SeasonNumber: s, EpisodeNumber: e}
		if !sourceFilterToken(ep.EpisodeID) {
			return target, errors.New("invalid episode ID")
		}
		if q.Has("episodeNumberingId") || q.Has("episodeOrdering") {
			if !sourceFilterToken(q.Get("episodeNumberingId")) {
				return target, errors.New("invalid numbering")
			}
			ordering := q.Get("episodeOrdering")
			if ordering != "" && (!sourceFilterToken(ordering) || len(ordering) > 128) {
				return target, errors.New("invalid ordering")
			}
			ep.Numbering = &models.EpisodeNumbering{SeriesID: q.Get("episodeNumberingId"), Ordering: ordering}
		}
		target.Episode = ep
	}
	return target, nil
}

func (h *IndexerHandler) sourceFilterOwner(w http.ResponseWriter, r *http.Request, profile string) (sourcefilters.Scope, bool) {
	if h.SourceFilters == nil || h.SourceFilterProfile == nil {
		apierror.Write(w, 503, "source_filters_unavailable", "source filters unavailable")
		return sourcefilters.Scope{}, false
	}
	if auth.GetSessionScope(r) != "" {
		apierror.Write(w, 403, "forbidden", "full session required")
		return sourcefilters.Scope{}, false
	}
	u, ok := h.SourceFilterProfile(profile)
	account := auth.GetAccountID(r)
	if !ok || profile == "" || !auth.IsMaster(r) && (account == "" || u.AccountID != account) {
		sourceFilterError(w, sourcefilters.ErrUnavailable)
		return sourcefilters.Scope{}, false
	}
	if !profileaccess.CheckResource(w, r, profile, "") {
		return sourcefilters.Scope{}, false
	}
	if account == "" {
		account = "master"
	}
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	if client == "" {
		client = normalizeClientID(r.URL.Query().Get("clientId"))
	}
	if client != "" && !sourceFilterToken(client) {
		sourceFilterError(w, errors.New("invalid client"))
		return sourcefilters.Scope{}, false
	}
	return sourcefilters.Scope{AccountID: account, ProfileID: profile, ClientID: client}, true
}

// Only local configuration/repositories participate here. Hashes and secrets
// stay private; a failed lookup cannot preserve an old authorization snapshot.
func (h *IndexerHandler) sourceFilterScope(scope sourcefilters.Scope, target sourcefilters.Target) (sourcefilters.Scope, error) {
	resolver := h.TrackPreferences
	if resolver == nil || resolver.Config == nil {
		return scope, sourcefilters.ErrContextChanged
	}
	global, err := resolver.Config.Load()
	if err != nil {
		return scope, sourcefilters.ErrContextChanged
	}
	u, ok := h.SourceFilterProfile(scope.ProfileID)
	if !ok {
		return scope, sourcefilters.ErrContextChanged
	}
	values := []any{global, u, u.PinHash, target}
	if resolver.UserSettings != nil {
		value, err := resolver.UserSettings.GetWithDefaults(scope.ProfileID, models.DefaultUserSettings())
		if err != nil {
			return scope, sourcefilters.ErrContextChanged
		}
		values = append(values, value)
	}
	if resolver.ClientSettings != nil && scope.ClientID != "" {
		value, err := resolver.ClientSettings.Get(scope.ClientID, scope.ProfileID)
		if err != nil {
			return scope, sourcefilters.ErrContextChanged
		}
		values = append(values, value)
	}
	if resolver.ContentPreferences != nil {
		value, err := resolver.ContentPreferences.Get(scope.ProfileID, target.TitleID)
		if err != nil {
			return scope, sourcefilters.ErrContextChanged
		}
		values = append(values, value)
	}
	if provider, ok := resolver.ContentPreferences.(interface {
		LocalOverrides(string, string) ([]models.ContentLocalOverride, error)
	}); ok {
		value, err := provider.LocalOverrides(scope.ProfileID, target.TitleID)
		if err != nil {
			return scope, sourcefilters.ErrContextChanged
		}
		values = append(values, value)
	}
	if h.BadStreams != nil {
		values = append(values, h.BadStreams.RevisionFor(reportContext(resolver, scope.ProfileID, scope.ClientID, target.TitleID, target.Episode)))
	}
	data, err := json.Marshal(values)
	if err != nil {
		return scope, sourcefilters.ErrContextChanged
	}
	digest := sha256.Sum256(data)
	scope.Signature = hex.EncodeToString(digest[:])
	return scope, nil
}

func (h *IndexerHandler) beginSourceFilters(w http.ResponseWriter, r *http.Request) (*sourceFilterRequest, bool) {
	if !r.URL.Query().Has("sourceFiltersVersion") {
		return nil, true
	}
	if r.URL.Query().Get("sourceFiltersVersion") != "1" {
		sourceFilterError(w, errors.New("unsupported source filters version"))
		return nil, false
	}
	target, err := sourceFilterTarget(r)
	if err != nil {
		sourceFilterError(w, err)
		return nil, false
	}
	scope, ok := h.sourceFilterOwner(w, r, r.URL.Query().Get("userId"))
	if !ok {
		return nil, false
	}
	scope, err = h.sourceFilterScope(scope, target)
	if err != nil {
		sourceFilterError(w, err)
		return nil, false
	}
	return &sourceFilterRequest{target, scope}, true
}

func (h *IndexerHandler) sourceFilterPreparedEvidence(r *http.Request, scope sourcefilters.Scope, target sourcefilters.Target) map[string]*models.MediaEvidence {
	result := map[string]*models.MediaEvidence{}
	ambiguous := map[string]bool{}
	prepared := h.SourceFilterPrepared
	if prepared == nil || prepared.store == nil {
		return result
	}
	settingsScope := prepared.PrequeueSettingsScopeKey(scope.ProfileID, scope.ClientID, target.TitleID)
	configurationKey := prepared.preparationConfigurationKey()
	if configurationKey == "" {
		return result
	}
	proofRequest := r.Clone(r.Context())
	proofRequest.Header = r.Header.Clone()
	proofRequest.Header.Set("X-Client-ID", scope.ClientID)
	for _, entry := range prepared.store.ListAll() {
		if entry.UserID != scope.ProfileID || entry.TitleID != target.TitleID || entry.Status != playback.PrequeueStatusReady || entry.ReuseDisabled || entry.MediaEvidenceClientID != scope.ClientID || entry.MediaEvidenceConfigurationKey != configurationKey || entry.SettingsScopeKey != automaticLibraryScope(settingsScope, entry.AutomaticLibrarySelection) || entry.SelectedResult == nil {
			continue
		}
		value := prepared.currentPreparationEvidence(proofRequest, entry)
		if value == nil || value.Probed == nil || value.CandidateVersion != mediaevidence.CandidateVersion(*entry.SelectedResult) {
			continue
		}
		// Only a current exact-leaf preparation can add series associations.
		if target.MediaType == "series" && !sourcefilters.ExactEpisodeBinding(target, value) {
			continue
		}
		if previous := result[value.CandidateVersion]; previous != nil && previous.SourceVersion != value.SourceVersion {
			ambiguous[value.CandidateVersion] = true
		}
		if !ambiguous[value.CandidateVersion] {
			result[value.CandidateVersion] = value
		} else {
			delete(result, value.CandidateVersion)
		}
	}
	return result
}

func (h *IndexerHandler) searchSourceFilters(w http.ResponseWriter, r *http.Request, opts indexer.SearchOptions, request *sourceFilterRequest, includeFiltered bool) {
	initialLimit := opts.MaxResults
	if initialLimit < 1 {
		initialLimit = 50
	}
	if initialLimit > sourcefilters.MaxPageSize {
		initialLimit = sourcefilters.MaxPageSize
	}
	opts.IncludeFiltered = true
	opts.RetainForSourceFilters = true
	opts.MaxResults = 0
	scored, err := h.Service.SearchWithScoring(r.Context(), opts)
	if err != nil {
		status, response := classifySearchError(err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	markBadScoredResults(scored, h.BadStreams, reportContextForSearch(h.TrackPreferences, opts))
	if !includeFiltered {
		eligible := make([]models.ScoredNZBResult, 0, len(scored))
		for _, row := range scored {
			if row.FilterStatus == "passed" {
				eligible = append(eligible, row)
			}
		}
		scored = eligible
	}
	truncated := len(scored) > sourcefilters.MaxCandidates
	if truncated {
		scored = scored[:sourcefilters.MaxCandidates]
	}
	annotateScoredResultsProfile(scored, opts.UserID, opts.TitleID)
	for i := range scored {
		annotateReportEpisode(scored[i].Attributes, opts)
		if h.BadStreams == nil || h.BadStreams.MatchFor(scored[i].NZBResult, reportContextForSearch(h.TrackPreferences, opts)) == nil {
			scored[i].MediaEvidence = mediaevidence.Candidate(scored[i].NZBResult, time.Now())
		}
	}
	if h.DemoMode {
		for i := range scored {
			scored[i].Title = buildMaskedTitle(opts.Query, opts.Year, opts.MediaType)
			scored[i].Indexer = "Demo"
		}
	}
	healthToken := ""
	healthLimit := initialLimit
	if healthLimit > len(scored) {
		healthLimit = len(scored)
	}
	if h.SourceHealthSvc != nil && wantsSourceHealth(r) {
		healthToken = h.SourceHealthSvc.Annotate(r.Context(), scored[:healthLimit])
	}
	prepared := h.sourceFilterPreparedEvidence(r, request.scope, request.target)
	candidates := make([]sourcefilters.Candidate, 0, len(scored))
	retainedBytes := 0
	for _, row := range scored {
		var raw []byte
		if includeFiltered {
			raw, _ = json.Marshal(row)
		} else {
			raw, _ = json.Marshal(annotatedNZBResult{NZBResult: row.NZBResult, SourceHealth: row.SourceHealth, MediaEvidence: row.MediaEvidence})
		}
		evidence := row.MediaEvidence
		if existing := prepared[mediaevidence.CandidateVersion(row.NZBResult)]; existing != nil {
			evidence = existing
		}
		facts, _ := json.Marshal(evidence)
		if retainedBytes+len(raw)+len(facts) > sourcefilters.MaxSnapshotBytes {
			truncated = true
			break
		}
		retainedBytes += len(raw) + len(facts)
		candidates = append(candidates, sourcefilters.Candidate{Result: row.NZBResult, Raw: raw, Evidence: evidence})
	}
	current, err := h.sourceFilterScope(request.scope, request.target)
	if err != nil || current.Signature != request.scope.Signature {
		sourceFilterError(w, sourcefilters.ErrContextChanged)
		return
	}
	id := h.SourceFilters.Create(current, request.target, candidates, truncated, healthToken)
	page, err := h.SourceFilters.Page(id, current, sourcefilters.Filters{Basis: "candidate", Unknown: "include"}, "", initialLimit)
	if err != nil {
		sourceFilterError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page)
}

func (h *IndexerHandler) SourceSearchPage(w http.ResponseWriter, r *http.Request) {
	profile := mux.Vars(r)["userID"]
	scope, ok := h.sourceFilterOwner(w, r, profile)
	if !ok {
		return
	}
	id := mux.Vars(r)["snapshotID"]
	if !sourceFilterToken(id) {
		sourceFilterError(w, sourcefilters.ErrUnavailable)
		return
	}
	var body struct {
		Version int                    `json:"version"`
		Filters *sourcefilters.Filters `json:"filters"`
		Cursor  string                 `json:"cursor,omitempty"`
		Limit   int                    `json:"limit"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		sourceFilterError(w, err)
		return
	}
	if dec.Decode(&struct{}{}) != io.EOF || body.Version != 1 || body.Filters == nil || body.Cursor != "" && !sourceFilterToken(body.Cursor) {
		sourceFilterError(w, errors.New("invalid page request"))
		return
	}
	target, err := h.SourceFilters.Target(id, scope)
	if err != nil {
		sourceFilterError(w, err)
		return
	}
	scope, err = h.sourceFilterScope(scope, target)
	if err != nil {
		sourceFilterError(w, err)
		return
	}
	h.SourceFilters.ObserveExisting(id, scope, h.sourceFilterPreparedEvidence(r, scope, target))
	page, err := h.SourceFilters.Page(id, scope, *body.Filters, body.Cursor, body.Limit)
	if err != nil {
		sourceFilterError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page)
}

type sourceFilterProbeReceipt struct {
	id         string
	index      int
	scope      sourcefilters.Scope
	version    string
	generation uint64
}

func (h *IndexerHandler) resolveSourceFilterProbe(w http.ResponseWriter, r *http.Request, id string, index *int, profile string) (models.NZBResult, *sourceFilterProbeReceipt, bool) {
	scope, ok := h.sourceFilterOwner(w, r, profile)
	if !ok {
		return models.NZBResult{}, nil, false
	}
	if !sourceFilterToken(id) || index == nil {
		sourceFilterError(w, errors.New("invalid source correlation"))
		return models.NZBResult{}, nil, false
	}
	target, err := h.SourceFilters.Target(id, scope)
	if err != nil {
		sourceFilterError(w, err)
		return models.NZBResult{}, nil, false
	}
	scope, err = h.sourceFilterScope(scope, target)
	if err != nil {
		sourceFilterError(w, err)
		return models.NZBResult{}, nil, false
	}
	result, version, generation, err := h.SourceFilters.Resolve(id, *index, scope)
	if err != nil {
		sourceFilterError(w, err)
		return models.NZBResult{}, nil, false
	}
	if h.BadStreams != nil && h.BadStreams.MatchFor(result, reportContext(h.TrackPreferences, profile, scope.ClientID, target.TitleID, target.Episode)) != nil {
		sourceFilterError(w, sourcefilters.ErrContextChanged)
		return models.NZBResult{}, nil, false
	}
	return result, &sourceFilterProbeReceipt{id, *index, scope, version, generation}, true
}
func (h *IndexerHandler) attachSourceFilterProbe(receipt *sourceFilterProbeReceipt, evidence *models.MediaEvidence) error {
	if receipt == nil {
		return nil
	}
	target, err := h.SourceFilters.Target(receipt.id, receipt.scope)
	if err != nil {
		return err
	}
	current, err := h.sourceFilterScope(receipt.scope, target)
	if err != nil || current.Signature != receipt.scope.Signature {
		return sourcefilters.ErrContextChanged
	}
	if evidence != nil && evidence.Probed != nil && !h.SourceFilters.Attach(receipt.id, receipt.index, current, receipt.version, receipt.generation, evidence) {
		return sourcefilters.ErrViewChanged
	}
	return nil
}
