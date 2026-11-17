package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/database"
	"novastream/internal/importer"
	"novastream/models"
)

type usenetFallbackService interface {
	GetSABFallbackStatus(context.Context, int64) (importer.SABFallbackStatus, error)
	RefreshSABFallbackStatus(context.Context, int64) (importer.SABFallbackStatus, error)
}
type fallbackSessionReader interface {
	SnapshotForAuthorization(context.Context, string) (*models.Session, error)
}
type fallbackAccountReader interface {
	Get(context.Context, string) (*models.Account, error)
}
type UsenetFallbackHandler struct {
	service         usenetFallbackService
	sessions        fallbackSessionReader
	accounts        fallbackAccountReader
	previewSettings importer.SABImportPreviewSettings
}

func NewUsenetFallbackHandler(service usenetFallbackService, sessions fallbackSessionReader, accounts fallbackAccountReader) *UsenetFallbackHandler {
	return &UsenetFallbackHandler{service: service, sessions: sessions, accounts: accounts}
}

type fallbackAuthority struct {
	session        models.Session
	accountID      string
	accountUpdated time.Time
	accountExpiry  *time.Time
	claimExpiry    time.Time
}

func (h *UsenetFallbackHandler) current(r *http.Request) (fallbackAuthority, bool) {
	var denied fallbackAuthority
	if h == nil || h.service == nil || h.sessions == nil || h.accounts == nil || r.Context().Err() != nil {
		return denied, false
	}
	claimed, ok := auth.GetSession(r)
	if !ok || claimed.Token == "" || claimed.Scope != "" || claimed.ScopeResource != "" || !claimed.ExpiresAt.After(time.Now()) {
		return denied, false
	}
	current, err := h.sessions.SnapshotForAuthorization(r.Context(), claimed.Token)
	if err != nil || current == nil || current.Token != claimed.Token || current.AccountID != claimed.AccountID || current.AccountID != auth.GetAccountID(r) || current.Scope != "" || current.ScopeResource != "" || current.ClientID != claimed.ClientID || !current.CreatedAt.Equal(claimed.CreatedAt) || !current.ExpiresAt.After(time.Now()) {
		return denied, false
	}
	actor, err := h.accounts.Get(r.Context(), current.AccountID)
	if err != nil || actor == nil || actor.ID != current.AccountID || !actor.IsMaster || actor.ExpiresAt != nil && !actor.ExpiresAt.After(time.Now()) || r.Context().Err() != nil {
		return denied, false
	}
	// Cached IsMaster is deliberately excluded; only the actual reader is authority.
	session := *current
	session.IsMaster = false
	var expiry *time.Time
	if actor.ExpiresAt != nil {
		copy := *actor.ExpiresAt
		expiry = &copy
	}
	return fallbackAuthority{session: session, accountID: actor.ID, accountUpdated: actor.UpdatedAt, accountExpiry: expiry, claimExpiry: claimed.ExpiresAt}, true
}
func fallbackHTTPError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": "Usenet fallback status unavailable"})
}
func (h *UsenetFallbackHandler) Get(w http.ResponseWriter, r *http.Request)     { h.serve(w, r, false) }
func (h *UsenetFallbackHandler) Refresh(w http.ResponseWriter, r *http.Request) { h.serve(w, r, true) }

// List reads only durable safe receipt projections; the service is optional so
// older single-item consumers retain their original contract.
func (h *UsenetFallbackHandler) List(w http.ResponseWriter, r *http.Request) {
	deadline := time.Now().Add(5 * time.Second)
	if claim, ok := auth.GetSession(r); ok && claim.ExpiresAt.Before(deadline) {
		deadline = claim.ExpiresAt
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	r = r.WithContext(ctx)
	before, ok := h.current(r)
	if !ok {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	limit := 50
	valid := err == nil && len(r.URL.RawQuery) <= 512 && r.ContentLength == 0 && len(r.TransferEncoding) == 0 && (r.Body == nil || r.Body == http.NoBody)
	for key, values := range query {
		if len(values) != 1 || values[0] == "" || key != "limit" && key != "cursor" {
			valid = false
		}
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		valid = valid && err == nil && strconv.Itoa(limit) == raw
	}
	if limit < 1 || limit > 100 {
		valid = false
	}
	if !valid {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	reader, ok := h.service.(interface {
		ListSABFallbackStatuses(context.Context, string, int) (importer.SABFallbackPage, error)
	})
	if !ok {
		fallbackHTTPError(w, 503, "fallback_unavailable")
		return
	}
	if before.session.ExpiresAt.Before(deadline) {
		deadline = before.session.ExpiresAt
	}
	if before.accountExpiry != nil && before.accountExpiry.Before(deadline) {
		deadline = *before.accountExpiry
	}
	work, stop := context.WithDeadline(ctx, deadline)
	defer stop()
	r = r.WithContext(work)
	page, err := reader.ListSABFallbackStatuses(work, query.Get("cursor"), limit)
	after, still := h.current(r)
	if !still || !reflect.DeepEqual(before, after) {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if err != nil {
		if errors.Is(err, importer.ErrSABFallbackInvalidPage) {
			fallbackHTTPError(w, 400, "invalid_fallback_request")
		} else {
			fallbackHTTPError(w, 503, "fallback_unavailable")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(page)
}
func (h *UsenetFallbackHandler) serve(w http.ResponseWriter, r *http.Request, refresh bool) {
	// Bound the initial authority readers too, before any database/service work.
	initialDeadline := time.Now().Add(5 * time.Second)
	if claimed, ok := auth.GetSession(r); ok && claimed.ExpiresAt.Before(initialDeadline) {
		initialDeadline = claimed.ExpiresAt
	}
	initialCtx, initialStop := context.WithDeadline(r.Context(), initialDeadline)
	defer initialStop()
	r = r.WithContext(initialCtx)
	before, ok := h.current(r)
	if !ok {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	raw := mux.Vars(r)["queueID"]
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != raw || r.URL.RawQuery != "" {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	if before.claimExpiry.Before(deadline) {
		deadline = before.claimExpiry
	}
	if before.session.ExpiresAt.Before(deadline) {
		deadline = before.session.ExpiresAt
	}
	if before.accountExpiry != nil && before.accountExpiry.Before(deadline) {
		deadline = *before.accountExpiry
	}
	ctx, stop := context.WithDeadline(r.Context(), deadline)
	defer stop()
	r = r.WithContext(ctx)
	var result importer.SABFallbackStatus
	if refresh {
		result, err = h.service.RefreshSABFallbackStatus(ctx, id)
	} else {
		result, err = h.service.GetSABFallbackStatus(ctx, id)
	}
	after, still := h.current(r)
	if !still || !reflect.DeepEqual(before, after) {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if err != nil {
		status, code := 503, "fallback_unavailable"
		switch {
		case errors.Is(err, importer.ErrSABFallbackNotFound):
			status, code = 404, "fallback_not_found"
		case errors.Is(err, importer.ErrSABFallbackSourceChanged):
			status, code = 409, "fallback_source_changed"
		case errors.Is(err, importer.ErrSABFallbackBusy):
			status, code = 429, "fallback_busy"
		}
		fallbackHTTPError(w, status, code)
		return
	}
	if result.QueueID != id {
		fallbackHTTPError(w, 503, "fallback_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(result)
}

// SetImportPreviewSettings installs only the server's private current settings
// reader. No route body or persisted receipt can supply a root mapping.
func (h *UsenetFallbackHandler) SetImportPreviewSettings(reader importer.SABImportPreviewSettings) {
	h.previewSettings = reader
}

// Preview performs an explicit metadata-only local completed-directory inventory.
func (h *UsenetFallbackHandler) Preview(w http.ResponseWriter, r *http.Request) {
	deadline := time.Now().Add(5 * time.Second)
	if claim, ok := auth.GetSession(r); ok && claim.ExpiresAt.Before(deadline) {
		deadline = claim.ExpiresAt
	}
	ctx, stop := context.WithDeadline(r.Context(), deadline)
	defer stop()
	r = r.WithContext(ctx)
	original, ok := h.current(r)
	if !ok {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	raw := mux.Vars(r)["queueID"]
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != raw || r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	if original.session.ExpiresAt.Before(deadline) {
		deadline = original.session.ExpiresAt
	}
	if original.accountExpiry != nil && original.accountExpiry.Before(deadline) {
		deadline = *original.accountExpiry
	}
	ctx, stopWork := context.WithDeadline(ctx, deadline)
	defer stopWork()
	r = r.WithContext(ctx)
	service, ok := h.service.(interface {
		PreviewSABImport(context.Context, int64, importer.SABImportPreviewSettings, func(context.Context) error) (importer.SABImportPreview, error)
	})
	if !ok || h.previewSettings == nil {
		fallbackHTTPError(w, 503, "import_preview_unavailable")
		return
	}
	check := func(work context.Context) error {
		current, valid := h.current(r.WithContext(work))
		if !valid || !reflect.DeepEqual(original, current) {
			return importer.ErrSABImportPreviewUnavailable
		}
		return work.Err()
	}
	// Bound settings reads by the same original authority before/after the file
	// configuration read; the returned value never appears in a response.
	settings := func(work context.Context) (config.SABnzbdSettings, error) {
		if check(work) != nil {
			return config.SABnzbdSettings{}, importer.ErrSABImportPreviewUnavailable
		}
		cfg, e := h.previewSettings(work)
		if e != nil || check(work) != nil {
			return config.SABnzbdSettings{}, importer.ErrSABImportPreviewUnavailable
		}
		return cfg, nil
	}
	result, err := service.PreviewSABImport(ctx, id, settings, check)
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if err != nil {
		status, code := 503, "import_preview_unavailable"
		switch {
		case errors.Is(err, importer.ErrSABFallbackNotFound):
			status, code = 404, "fallback_not_found"
		case errors.Is(err, importer.ErrSABFallbackSourceChanged):
			status, code = 409, "fallback_source_changed"
		case errors.Is(err, importer.ErrSABFallbackBusy):
			status, code = 429, "fallback_busy"
		}
		fallbackHTTPError(w, status, code)
		return
	}
	if result.QueueID != id || result.State != "local_inventory" {
		fallbackHTTPError(w, 503, "import_preview_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *UsenetFallbackHandler) GetManagedCopy(w http.ResponseWriter, r *http.Request) {
	h.managedCopy(w, r, false)
}
func (h *UsenetFallbackHandler) CopyFiles(w http.ResponseWriter, r *http.Request) {
	h.managedCopy(w, r, true)
}
func (h *UsenetFallbackHandler) managedCopy(w http.ResponseWriter, r *http.Request, copyFiles bool) {
	budget := 5 * time.Second
	if copyFiles {
		budget = 5 * time.Minute
	}
	deadline := time.Now().Add(budget)
	if claim, ok := auth.GetSession(r); ok && claim.ExpiresAt.Before(deadline) {
		deadline = claim.ExpiresAt
	}
	ctx, stop := context.WithDeadline(r.Context(), deadline)
	defer stop()
	r = r.WithContext(ctx)
	original, valid := h.current(r)
	if !valid {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	raw := mux.Vars(r)["queueID"]
	id, e := strconv.ParseInt(raw, 10, 64)
	if e != nil || id <= 0 || strconv.FormatInt(id, 10) != raw || r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	if original.session.ExpiresAt.Before(deadline) {
		deadline = original.session.ExpiresAt
	}
	if original.accountExpiry != nil && original.accountExpiry.Before(deadline) {
		deadline = *original.accountExpiry
	}
	ctx, end := context.WithDeadline(ctx, deadline)
	defer end()
	r = r.WithContext(ctx)
	service, ok := h.service.(interface {
		GetSABManagedCopy(context.Context, int64, importer.SABImportPreviewSettings, importer.SABImportStageSettings, func(context.Context) error) (importer.SABManagedCopyView, error)
		StageSABImport(context.Context, int64, importer.SABImportPreviewSettings, importer.SABImportStageSettings, func(context.Context) error) (importer.SABImportStageStatus, error)
	})
	if !ok || h.previewSettings == nil {
		fallbackHTTPError(w, 503, "managed_copy_unavailable")
		return
	}
	check := func(work context.Context) error {
		current, ok := h.current(r.WithContext(work))
		if !ok || !reflect.DeepEqual(original, current) {
			return importer.ErrSABImportStageUnavailable
		}
		return work.Err()
	}
	cloneConfig := func(cfg config.SABnzbdSettings) config.SABnzbdSettings {
		if cfg.Enabled != nil {
			enabled := *cfg.Enabled
			cfg.Enabled = &enabled
		}
		return cfg
	}
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	frozen, e := h.previewSettings(ctx)
	frozen = cloneConfig(frozen)
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if e != nil {
		fallbackHTTPError(w, 503, "managed_copy_unavailable")
		return
	}
	source := func(work context.Context) (config.SABnzbdSettings, error) {
		if check(work) != nil {
			return config.SABnzbdSettings{}, importer.ErrSABImportStageUnavailable
		}
		current, e := h.previewSettings(work)
		current = cloneConfig(current)
		if e != nil || check(work) != nil || !reflect.DeepEqual(current, frozen) {
			return config.SABnzbdSettings{}, importer.ErrSABImportStageUnavailable
		}
		return cloneConfig(frozen), nil
	}
	target := func(work context.Context) (importer.SABImportStagePolicy, error) {
		cfg, e := source(work)
		return importer.SABImportStagePolicy{TargetRoot: cfg.ImportStageTargetRoot, MaxBytes: cfg.ImportStageMaxBytes}, e
	}
	var value importer.SABManagedCopyView
	if copyFiles {
		result, err := service.StageSABImport(ctx, id, source, target, check)
		e = err
		valid := result.QueueID == id && result.State == "staged_copy" && !result.ObservedAt.IsZero() && !result.ObservedAt.After(time.Now()) && len(result.Files) > 0 && len(result.Files) <= 128 && result.ObservedBytes >= 0 && result.ObservedBytes <= 1<<40
		var total int64
		for i, f := range result.Files {
			if f.Ordinal != i+1 || f.Bytes < 0 || f.Bytes > (1<<40)-total {
				valid = false
				break
			}
			total += f.Bytes
		}
		if e == nil && (!valid || total != result.ObservedBytes) {
			e = importer.ErrSABImportStageUnavailable
		}
		if e == nil {
			bytes := result.ObservedBytes
			count := len(result.Files)
			at := result.ObservedAt.UTC()
			value = importer.SABManagedCopyView{QueueID: id, State: "copy_recorded", Configured: true, ChargedBytes: &bytes, FileCount: &count, ObservedAt: &at}
		}
	} else {
		value, e = service.GetSABManagedCopy(ctx, id, source, target, check)
	}
	if e == nil {
		_, e = source(ctx)
	}
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if e == nil {
		_, e = source(ctx)
	}
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if e != nil {
		status, code := 503, "managed_copy_unavailable"
		switch {
		case errors.Is(e, database.ErrSABStageExists):
			status, code = 409, "copy_already_recorded"
		case errors.Is(e, database.ErrSABStageQuota):
			status, code = 409, "copy_budget_exceeded"
		case errors.Is(e, database.ErrSABStageOutcomeUnknown):
			code = "copy_outcome_unknown"
		case errors.Is(e, importer.ErrSABFallbackBusy):
			status, code = 429, "fallback_busy"
		case errors.Is(e, importer.ErrSABFallbackNotFound):
			status, code = 404, "fallback_not_found"
		case errors.Is(e, importer.ErrSABFallbackSourceChanged):
			status, code = 409, "fallback_source_changed"
		}
		if copyFiles {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "private, no-store")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": "Copy was not confirmed; files may already have been copied. View saved copy."})
		} else {
			fallbackHTTPError(w, status, code)
		}
		return
	}
	if value.QueueID != id {
		fallbackHTTPError(w, 503, "managed_copy_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(value)
}
