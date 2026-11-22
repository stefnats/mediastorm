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

type parentModePersistence interface {
	CommitParentMode(context.Context, string, *datastore.ParentModeIntent) (models.User, error)
}

func decodeParentMode(w http.ResponseWriter, r *http.Request) (string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	decoder := json.NewDecoder(r.Body)
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return "", errors.New("mode object required")
	}
	var mode string
	found := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil || key != "mode" || found {
			return "", errors.New("exactly one mode field required")
		}
		var raw json.RawMessage
		if err = decoder.Decode(&raw); err != nil {
			return "", err
		}
		if string(raw) == "null" || json.Unmarshal(raw, &mode) != nil {
			return "", errors.New("mode string required")
		}
		found = true
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return "", errors.New("mode object required")
	}
	var trailing json.RawMessage
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("unexpected trailing JSON")
	}
	if !found || mode != "" && mode != "rating" && mode != "rating_de" && mode != "content_list" {
		return "", errors.New("invalid mode, must be 'rating', 'rating_de', 'content_list', or empty")
	}
	return mode, nil
}
func (h *UsersHandler) currentParentMode(w http.ResponseWriter, r *http.Request, id string) {
	originalRequest := r
	mode, err := decodeParentMode(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentModeError(w, r, datastore.ErrParentModeChanged)
		return
	}
	var intent *datastore.ParentModeIntent
	if len(values) == 1 {
		intent, err = h.pgPins.ClaimParentMode(r, id, mode, values[0])
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		token, e := h.pgPins.ParentModeChallenge(r, id, mode)
		if e != nil {
			writeParentModeError(w, r, e)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		intent, err = h.pgPins.ImmediateParentMode(r, id, mode)
	}
	if err != nil {
		writeParentModeError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentModeRequest(r, intent)
	if err != nil {
		writeParentModeError(w, originalRequest, err)
		return
	}
	// This exact existing guard owns the common Fresh/Legacy guessing budget.
	// Its boolean is never the datastore credential proof.
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentMode(r, intent); err != nil {
		writeParentModeError(w, originalRequest, err)
		return
	}
	persistence, ok := h.Service.(parentModePersistence)
	if !ok {
		writeParentModeError(w, originalRequest, datastore.ErrParentModeUnavailable)
		return
	}
	user, err := persistence.CommitParentMode(r.Context(), id, intent)
	if originalRequest.Context().Err() != nil {
		return
	}
	if r.Context().Err() != nil && err == nil {
		err = datastore.ErrParentModeChanged
	}
	if err != nil {
		writeParentModeError(w, originalRequest, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(user)
}
func writeParentModeError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrProfilePinAccessNotFound):
		apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
	case errors.Is(err, datastore.ErrParentModeChanged), errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		apierror.Write(w, http.StatusConflict, "parent_confirmation_changed", "Confirmation expired or the request changed. Save again.")
	case errors.Is(err, datastore.ErrParentModeCredential):
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrParentModeUnknown), errors.Is(err, users.ErrPersistenceUncertain):
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_outcome_unknown", "The result is not known. Reload settings before trying again.")
	default:
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_unavailable", "Parent confirmation is temporarily unavailable.")
	}
}
