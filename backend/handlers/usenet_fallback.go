package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
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
	service  usenetFallbackService
	sessions fallbackSessionReader
	accounts fallbackAccountReader
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
