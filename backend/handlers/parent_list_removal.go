package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"novastream/internal/apierror"
	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/users"
)

type parentListRemovalPersistence interface {
	CommitParentListRemoval(context.Context, string, *datastore.ParentListRemovalIntent) (models.User, error)
}

func (h *UsersHandler) currentParentListRemoval(w http.ResponseWriter, r *http.Request, id string) {
	originalRequest := r
	listURL := strings.TrimSpace(r.URL.Query().Get("url"))
	var err error
	if listURL == "" {
		http.Error(w, "url query parameter is required", http.StatusBadRequest)
		return
	}
	// This operation has no payload. Do not accept a second hidden selection.
	if r.Body != nil {
		body, readErr := io.ReadAll(io.LimitReader(r.Body, 1))
		if len(body) != 0 || readErr != nil {
			http.Error(w, "request body is not supported", http.StatusBadRequest)
			return
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentListRemovalError(w, r, datastore.ErrParentListRemovalChanged)
		return
	}
	var intent *datastore.ParentListRemovalIntent
	if len(values) == 1 {
		intent, err = h.pgPins.ClaimParentListRemoval(r, id, listURL, values[0])
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		token, e := h.pgPins.ParentListRemovalChallenge(r, id, listURL)
		if e != nil {
			writeParentListRemovalError(w, r, e)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		intent, err = h.pgPins.ImmediateParentListRemoval(r, id, listURL)
	}
	if err != nil {
		writeParentListRemovalError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentListRemovalRequest(r, intent)
	if err != nil {
		writeParentListRemovalError(w, originalRequest, err)
		return
	}
	// This exact existing guard owns the common Fresh/Legacy guessing budget.
	// Its boolean is never the datastore credential proof.
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentListRemoval(r, intent); err != nil {
		writeParentListRemovalError(w, originalRequest, err)
		return
	}
	persistence, ok := h.Service.(parentListRemovalPersistence)
	if !ok {
		writeParentListRemovalError(w, originalRequest, datastore.ErrParentListRemovalUnavailable)
		return
	}
	user, err := persistence.CommitParentListRemoval(r.Context(), id, intent)
	if originalRequest.Context().Err() != nil {
		return
	}
	if r.Context().Err() != nil && err == nil {
		err = datastore.ErrParentListRemovalChanged
	}
	if err != nil {
		writeParentListRemovalError(w, originalRequest, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(user)
}
func writeParentListRemovalError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrProfilePinAccessNotFound):
		apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
	case errors.Is(err, datastore.ErrParentListRemovalChanged), errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		apierror.Write(w, http.StatusConflict, "parent_confirmation_changed", "Confirmation expired or the request changed. Save again.")
	case errors.Is(err, datastore.ErrParentListRemovalCredential):
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrParentListRemovalUnknown), errors.Is(err, users.ErrPersistenceUncertain):
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_outcome_unknown", "The result is not known. Reload settings before trying again.")
	default:
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_unavailable", "Parent confirmation is temporarily unavailable.")
	}
}
