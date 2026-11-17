package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/apierror"
	"novastream/internal/datastore"
	"novastream/services/profile_recovery"
)

type ProfileRecoveryHandler struct {
	service *profile_recovery.Service
	policy  func() config.SecuritySettings
}

func NewProfileRecoveryHandler(service *profile_recovery.Service, policy func() config.SecuritySettings) *ProfileRecoveryHandler {
	return &ProfileRecoveryHandler{service: service, policy: policy}
}
func (h *ProfileRecoveryHandler) API(w http.ResponseWriter, r *http.Request)    { h.serve(w, r, false) }
func (h *ProfileRecoveryHandler) Cookie(w http.ResponseWriter, r *http.Request) { h.serve(w, r, true) }
func invalidRecoveryRequest(r *http.Request) bool {
	if r == nil || r.URL == nil || r.Method != http.MethodPost {
		return true
	}
	if len(r.Header.Values("X-Parent-Intent")) > 0 || len(r.Header.Values("X-Account-Password")) > 0 {
		return true
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return true
	}
	for key := range query {
		switch strings.ToLower(key) {
		case "userid", "profileid", "target", "mode", "movierating", "tvrating", "lists", "url", "parentintent":
			return true
		}
	}
	if r.Body != nil {
		data, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(data) > 0 {
			return true
		}
	}
	return false
}
func (h *ProfileRecoveryHandler) serve(w http.ResponseWriter, r *http.Request, cookie bool) {
	w.Header().Set("Cache-Control", "no-store")
	if h == nil || h.service == nil {
		writeRecoveryError(w, r, datastore.ErrProfileRecoveryUnavailable)
		return
	}
	if invalidRecoveryRequest(r) {
		apierror.Write(w, http.StatusBadRequest, "profile_recovery_invalid", "Reload requires a bodyless request without confirmation data.")
		return
	}
	target := mux.Vars(r)["userID"]
	if target == "" {
		target = mux.Vars(r)["id"]
	}
	if target == "" {
		apierror.Write(w, http.StatusBadRequest, "profile_recovery_invalid", "Profile target is required.")
		return
	}
	var result *profile_recovery.Result
	var err error
	if cookie {
		result, err = h.service.RecoverManagement(r, target)
	} else {
		result, err = h.service.RecoverAPI(r, target, h.policy)
	}
	if r.Context().Err() != nil {
		return
	}
	if err != nil {
		writeRecoveryError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
func writeRecoveryError(w http.ResponseWriter, r *http.Request, err error) {
	if r != nil && r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, profile_recovery.ErrNotFound), errors.Is(err, datastore.ErrProfilePinAccessNotFound):
		apierror.Write(w, http.StatusNotFound, "profile_not_found", "Profile not found.")
	case errors.Is(err, profile_recovery.ErrForbidden):
		apierror.Write(w, http.StatusForbidden, "profile_recovery_forbidden", "A full account session is required.")
	case errors.Is(err, profile_recovery.ErrLocked):
		apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked")
	case errors.Is(err, datastore.ErrProfileRecoveryChanged), errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		apierror.Write(w, http.StatusConflict, "profile_recovery_changed", "Profile state changed. Reload explicitly again.")
	default:
		apierror.Write(w, http.StatusServiceUnavailable, "profile_recovery_unavailable", "Saved profile state could not be reloaded.")
	}
}
