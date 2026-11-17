package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"novastream/internal/apierror"
	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/users"
)

type parentListWritePersistence interface {
	CommitParentListAdd(context.Context, string, *datastore.ParentListWriteIntent) (models.User, error)
	CommitParentListReplace(context.Context, string, *datastore.ParentListWriteIntent) (models.User, error)
}

type parentListWriteAction uint8

const (
	parentListWriteAdd parentListWriteAction = iota + 1
	parentListWriteReplace
)

func (h *UsersHandler) currentParentListAdd(w http.ResponseWriter, r *http.Request, id string) {
	h.currentParentListWrite(w, r, id, parentListWriteAdd)
}
func (h *UsersHandler) currentParentListReplace(w http.ResponseWriter, r *http.Request, id string) {
	h.currentParentListWrite(w, r, id, parentListWriteReplace)
}
func (h *UsersHandler) currentParentListWrite(w http.ResponseWriter, r *http.Request, id string, action parentListWriteAction) {
	originalRequest := r
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(io.LimitReader(r.Body, datastore.ParentListWriteBodyLimit+1))
	}
	if err != nil {
		writeParentListWriteError(w, r, datastore.ErrParentListWritePayload)
		return
	}
	if len(body) > datastore.ParentListWriteBodyLimit {
		writeParentListWriteError(w, r, datastore.ErrParentListWriteBodyTooLarge)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentListWriteError(w, r, datastore.ErrParentListWriteChanged)
		return
	}
	var intent *datastore.ParentListWriteIntent
	if len(values) == 1 {
		if action == parentListWriteAdd {
			intent, err = h.pgPins.ClaimParentListAdd(r, id, body, values[0])
		} else {
			intent, err = h.pgPins.ClaimParentListReplace(r, id, body, values[0])
		}
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		var token string
		var e error
		if action == parentListWriteAdd {
			token, e = h.pgPins.ParentListAddChallenge(r, id, body)
		} else {
			token, e = h.pgPins.ParentListReplaceChallenge(r, id, body)
		}
		if e != nil {
			writeParentListWriteError(w, r, e)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		if action == parentListWriteAdd {
			intent, err = h.pgPins.ImmediateParentListAdd(r, id, body)
		} else {
			intent, err = h.pgPins.ImmediateParentListReplace(r, id, body)
		}
	}
	if err != nil {
		writeParentListWriteError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentListWriteRequest(r, intent)
	if err != nil {
		writeParentListWriteError(w, originalRequest, err)
		return
	}
	// This exact existing guard owns the common Fresh/Legacy guessing budget.
	// Its boolean is never the datastore credential proof.
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentListWrite(r, intent); err != nil {
		writeParentListWriteError(w, originalRequest, err)
		return
	}
	persistence, ok := h.Service.(parentListWritePersistence)
	if !ok {
		writeParentListWriteError(w, originalRequest, datastore.ErrParentListWriteUnavailable)
		return
	}
	var user models.User
	if action == parentListWriteAdd {
		user, err = persistence.CommitParentListAdd(r.Context(), id, intent)
	} else {
		user, err = persistence.CommitParentListReplace(r.Context(), id, intent)
	}
	if originalRequest.Context().Err() != nil {
		return
	}
	if r.Context().Err() != nil && err == nil {
		err = datastore.ErrParentListWriteChanged
	}
	if err != nil {
		writeParentListWriteError(w, originalRequest, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(user)
}
func writeParentListWriteError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrParentListWriteBodyTooLarge):
		http.Error(w, "parental list body exceeds the 16 KiB confirmation limit", http.StatusRequestEntityTooLarge)
	case errors.Is(err, datastore.ErrParentListWritePayload):
		http.Error(w, "invalid parental list request body", http.StatusBadRequest)
	case errors.Is(err, datastore.ErrProfilePinAccessNotFound):
		apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
	case errors.Is(err, datastore.ErrParentListWriteChanged), errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		apierror.Write(w, http.StatusConflict, "parent_confirmation_changed", "Confirmation expired or the request changed. Save again.")
	case errors.Is(err, datastore.ErrParentListWriteCredential):
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrParentListWriteUnknown), errors.Is(err, users.ErrPersistenceUncertain):
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_outcome_unknown", "The result is not known. Reload settings before trying again.")
	default:
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_unavailable", "Parent confirmation is temporarily unavailable.")
	}
}
