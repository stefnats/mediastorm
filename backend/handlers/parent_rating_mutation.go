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

type parentRatingPersistence interface {
	CommitParentRatingGeneric(context.Context, string, *datastore.ParentRatingIntent) (models.User, error)
	CommitParentRatingMovie(context.Context, string, *datastore.ParentRatingIntent) (models.User, error)
	CommitParentRatingTV(context.Context, string, *datastore.ParentRatingIntent) (models.User, error)
	CommitParentRatingPair(context.Context, string, *datastore.ParentRatingIntent) (models.User, error)
}
type parentRatingJSONPair interface {
	SetKidsMaxRatings(string, string, string) (models.User, error)
}
type parentRatingAction uint8

const (
	parentRatingGeneric parentRatingAction = iota + 1
	parentRatingMovie
	parentRatingTV
	parentRatingPair
)

func readParentRatingBody(r *http.Request) ([]byte, error) {
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(io.LimitReader(r.Body, datastore.ParentRatingBodyLimit+1))
	}
	if err != nil {
		return nil, datastore.ErrParentRatingPayload
	}
	if len(body) > datastore.ParentRatingBodyLimit {
		return nil, datastore.ErrParentRatingBodyTooLarge
	}
	return body, nil
}
func (h *UsersHandler) currentParentRating(w http.ResponseWriter, r *http.Request, id string, action parentRatingAction) {
	originalRequest := r
	w.Header().Set("Cache-Control", "no-store")
	body, err := readParentRatingBody(r)
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentRatingError(w, r, datastore.ErrParentRatingChanged)
		return
	}
	var intent *datastore.ParentRatingIntent
	if len(values) == 1 {
		switch action {
		case parentRatingGeneric:
			intent, err = h.pgPins.ClaimParentRatingGeneric(r, id, body, values[0])
		case parentRatingMovie:
			intent, err = h.pgPins.ClaimParentRatingMovie(r, id, body, values[0])
		case parentRatingTV:
			intent, err = h.pgPins.ClaimParentRatingTV(r, id, body, values[0])
		case parentRatingPair:
			intent, err = h.pgPins.ClaimParentRatingPair(r, id, body, values[0])
		default:
			err = datastore.ErrParentRatingChanged
		}
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		var token string
		switch action {
		case parentRatingGeneric:
			token, err = h.pgPins.ParentRatingGenericChallenge(r, id, body)
		case parentRatingMovie:
			token, err = h.pgPins.ParentRatingMovieChallenge(r, id, body)
		case parentRatingTV:
			token, err = h.pgPins.ParentRatingTVChallenge(r, id, body)
		case parentRatingPair:
			token, err = h.pgPins.ParentRatingPairChallenge(r, id, body)
		default:
			err = datastore.ErrParentRatingChanged
		}
		if err != nil {
			writeParentRatingError(w, r, err)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		switch action {
		case parentRatingGeneric:
			intent, err = h.pgPins.ImmediateParentRatingGeneric(r, id, body)
		case parentRatingMovie:
			intent, err = h.pgPins.ImmediateParentRatingMovie(r, id, body)
		case parentRatingTV:
			intent, err = h.pgPins.ImmediateParentRatingTV(r, id, body)
		case parentRatingPair:
			intent, err = h.pgPins.ImmediateParentRatingPair(r, id, body)
		default:
			err = datastore.ErrParentRatingChanged
		}
	}
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentRatingRequest(r, intent)
	if err != nil {
		writeParentRatingError(w, originalRequest, err)
		return
	}
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentRating(r, intent); err != nil {
		writeParentRatingError(w, originalRequest, err)
		return
	}
	persistence, ok := h.Service.(parentRatingPersistence)
	if !ok {
		writeParentRatingError(w, originalRequest, datastore.ErrParentRatingUnavailable)
		return
	}
	var user models.User
	switch action {
	case parentRatingGeneric:
		user, err = persistence.CommitParentRatingGeneric(r.Context(), id, intent)
	case parentRatingMovie:
		user, err = persistence.CommitParentRatingMovie(r.Context(), id, intent)
	case parentRatingTV:
		user, err = persistence.CommitParentRatingTV(r.Context(), id, intent)
	case parentRatingPair:
		user, err = persistence.CommitParentRatingPair(r.Context(), id, intent)
	default:
		err = datastore.ErrParentRatingChanged
	}
	if originalRequest.Context().Err() != nil {
		return
	}
	if r.Context().Err() != nil && err == nil {
		err = datastore.ErrParentRatingChanged
	}
	if err != nil {
		writeParentRatingError(w, originalRequest, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(user)
}
func writeParentRatingError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrParentRatingBodyTooLarge):
		http.Error(w, "parental rating body exceeds the 16 KiB confirmation limit", http.StatusRequestEntityTooLarge)
	case errors.Is(err, datastore.ErrParentRatingPayload):
		http.Error(w, "invalid parental rating request body", http.StatusBadRequest)
	case errors.Is(err, datastore.ErrProfilePinAccessNotFound):
		apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
	case errors.Is(err, datastore.ErrParentRatingChanged), errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		apierror.Write(w, http.StatusConflict, "parent_confirmation_changed", "Confirmation expired or the request changed. Save again.")
	case errors.Is(err, datastore.ErrParentRatingCredential):
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrParentRatingUnknown), errors.Is(err, users.ErrPersistenceUncertain):
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_outcome_unknown", "The result is not known. Reload settings before trying again.")
	default:
		apierror.Write(w, http.StatusServiceUnavailable, "parent_confirmation_unavailable", "Parent confirmation is temporarily unavailable.")
	}
}
