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
	"novastream/services/profileunlock"
	"novastream/services/user_settings"
	"novastream/services/users"
)

type parentProfileCreationPersistence interface {
	AcquireParentProfileCreationLease(context.Context, *datastore.ParentProfileCreationIntent) (*users.ParentProfileCreationLease, error)
}

// CreateProfileManagement is installed only behind existing Cookie auth and
// explicit PG Kids creation (or any present intent). No adult/API policy here.
func (h *UsersHandler) CreateProfileManagement(w http.ResponseWriter, r *http.Request, settings *user_settings.Service) {
	original := r
	w.Header().Set("Cache-Control", "no-store")
	if h.pgPins == nil || settings == nil {
		writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(io.LimitReader(r.Body, datastore.ParentProfileCreationBodyLimit+1))
	}
	if err != nil {
		writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationPayload)
		return
	}
	if len(body) > datastore.ParentProfileCreationBodyLimit {
		writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationBodyTooLarge)
		return
	}
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationChanged)
		return
	}
	var intent *datastore.ParentProfileCreationIntent
	if len(values) == 1 {
		intent, err = h.pgPins.ClaimParentProfileCreation(r, body, values[0])
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		var token string
		token, err = h.pgPins.ParentProfileCreationChallenge(r, body)
		if err != nil {
			writeParentProfileCreationError(w, r, err)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, 403, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		intent, err = h.pgPins.ImmediateParentProfileCreation(r, body)
	}
	if err != nil {
		writeParentProfileCreationError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentProfileCreationRequest(r, intent)
	if err != nil {
		writeParentProfileCreationError(w, original, err)
		return
	}
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentProfileCreation(r, intent); err != nil {
		writeParentProfileCreationError(w, original, err)
		return
	}
	port, ok := h.Service.(parentProfileCreationPersistence)
	if !ok {
		writeParentProfileCreationError(w, original, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	u, err := port.AcquireParentProfileCreationLease(r.Context(), intent)
	if err != nil {
		writeParentProfileCreationError(w, original, err)
		return
	}
	defer u.Close()
	s, err := settings.AcquireParentProfileCreationDefaultsLease(r.Context(), intent, u.ReceiptLease())
	if err != nil {
		writeParentProfileCreationError(w, original, err)
		return
	}
	defer s.Close()
	quarantine := func() { u.Quarantine(); s.Quarantine() }
	receipt, err := u.Commit()
	if err != nil {
		if errors.Is(err, datastore.ErrParentProfileCreationUnknown) {
			quarantine()
		}
		writeParentProfileCreationError(w, original, err)
		return
	}
	user, err := u.Stage(receipt)
	if err != nil {
		quarantine()
		writeParentProfileCreationError(w, original, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	if err = s.Stage(receipt); err != nil {
		quarantine()
		writeParentProfileCreationError(w, original, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	// Prepare the safe response BEFORE the single irreversible cache PairTake.
	response := parentKidsFlagProfile(user)
	response.AccountID = user.AccountID
	response.ActivityPrivacy = user.ActivityPrivacy
	response.AllowShareLinks = user.AllowShareLinks
	if err = h.pgPins.TakeParentProfileCreationReceipt(intent, receipt, u.ReceiptLease(), s.ReceiptLease()); err != nil {
		quarantine()
		writeParentProfileCreationError(w, original, err)
		return
	}
	u.Complete()
	s.Complete()
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
func writeParentProfileCreationError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrParentProfileCreationPayload):
		apierror.Write(w, 400, "invalid_request", "invalid profile creation request")
	case errors.Is(err, datastore.ErrParentProfileCreationBodyTooLarge):
		apierror.Write(w, 413, "invalid_request", "profile creation request exceeds sixteen kibibytes")
	case errors.Is(err, datastore.ErrParentProfileCreationCredential):
		apierror.Write(w, 403, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrProfilePinAccessChanged), errors.Is(err, datastore.ErrParentProfileCreationChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, datastore.ErrUserSettingsConflict), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		apierror.Write(w, 409, "parent_confirmation_changed", "profile confirmation expired or changed")
	case errors.Is(err, datastore.ErrParentProfileCreationUnknown), errors.Is(err, users.ErrPersistenceUncertain), errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown):
		apierror.Write(w, 503, "parent_confirmation_outcome_unknown", "profile creation outcome is unknown; do not repeat creation")
	default:
		apierror.Write(w, 503, "parent_confirmation_unavailable", "profile creation confirmation unavailable")
	}
}
