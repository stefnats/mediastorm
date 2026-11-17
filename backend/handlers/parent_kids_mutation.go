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
	"novastream/services/user_settings"
	"novastream/services/users"
)

type parentKidsFlagPersistence interface {
	AcquireParentKidsFlagLease(context.Context, string, *datastore.ParentKidsFlagIntent) (*users.ParentKidsFlagLease, error)
}

// SetKidsProfileManagement is called only by the existing authenticated AdminUI
// carrier. Its service argument is server wiring, never a client policy flag.
func (h *UsersHandler) SetKidsProfileManagement(w http.ResponseWriter, r *http.Request, target string, settings *user_settings.Service) {
	h.currentParentKidsFlag(w, r, target, true, settings)
}
func (h *UsersHandler) currentParentKidsFlag(w http.ResponseWriter, r *http.Request, target string, management bool, settings *user_settings.Service) {
	originalRequest := r
	w.Header().Set("Cache-Control", "no-store")
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(io.LimitReader(r.Body, datastore.ParentKidsFlagBodyLimit+1))
	}
	if err != nil {
		writeParentKidsFlagError(w, r, datastore.ErrParentKidsFlagPayload)
		return
	}
	if len(body) > datastore.ParentKidsFlagBodyLimit {
		writeParentKidsFlagError(w, r, datastore.ErrParentKidsFlagBodyTooLarge)
		return
	}
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentKidsFlagError(w, r, datastore.ErrParentKidsFlagChanged)
		return
	}
	var intent *datastore.ParentKidsFlagIntent
	if len(values) == 1 {
		if management {
			intent, err = h.pgPins.ClaimParentKidsFlagManagement(r, target, body, values[0])
		} else {
			intent, err = h.pgPins.ClaimParentKidsFlagAPI(r, target, body, values[0])
		}
	} else if r.Header.Get(parentauth.PasswordHeader) == "" {
		var token string
		if management {
			token, err = h.pgPins.ParentKidsFlagManagementChallenge(r, target, body)
		} else {
			token, err = h.pgPins.ParentKidsFlagAPIChallenge(r, target, body)
		}
		if err != nil {
			writeParentKidsFlagError(w, r, err)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, http.StatusForbidden, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": token})
		return
	} else {
		if management {
			intent, err = h.pgPins.ImmediateParentKidsFlagManagement(r, target, body)
		} else {
			intent, err = h.pgPins.ImmediateParentKidsFlagAPI(r, target, body)
		}
	}
	if err != nil {
		writeParentKidsFlagError(w, r, err)
		return
	}
	defer intent.Close()
	r, err = h.pgPins.BoundParentKidsFlagRequest(r, intent)
	if err != nil {
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	if !h.parentAuthorization.RequireFresh(w, r) {
		return
	}
	if err = h.pgPins.VerifyParentKidsFlag(r, intent); err != nil {
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	port, ok := h.Service.(parentKidsFlagPersistence)
	if !ok {
		writeParentKidsFlagError(w, originalRequest, datastore.ErrParentKidsFlagUnavailable)
		return
	}
	userLease, err := port.AcquireParentKidsFlagLease(r.Context(), target, intent)
	if err != nil {
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	defer userLease.Close()
	required, err := h.pgPins.ParentKidsFlagSettingsRequired(intent)
	if err != nil {
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	var settingsLease *user_settings.ParentKidsFlagDefaultsLease
	if required {
		if !management || settings == nil {
			writeParentKidsFlagError(w, originalRequest, datastore.ErrParentKidsFlagUnavailable)
			return
		}
		settingsLease, err = settings.AcquireParentKidsFlagDefaultsLease(r.Context(), target, intent, userLease.ReceiptLease())
		if err != nil {
			writeParentKidsFlagError(w, originalRequest, err)
			return
		}
		defer settingsLease.Close()
	}
	quarantine := func() {
		userLease.Quarantine()
		if settingsLease != nil {
			settingsLease.Quarantine()
		}
	}
	receipt, err := userLease.Commit()
	if err != nil {
		if errors.Is(err, datastore.ErrParentKidsFlagUnknown) {
			quarantine()
		}
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	user, err := userLease.Stage(receipt)
	if err != nil {
		quarantine()
		writeParentKidsFlagError(w, originalRequest, datastore.ErrParentKidsFlagUnavailable)
		return
	}
	var settingsProof *datastore.ParentKidsFlagCacheLease
	if settingsLease != nil {
		if err = settingsLease.Stage(receipt); err != nil {
			quarantine()
			writeParentKidsFlagError(w, originalRequest, datastore.ErrParentKidsFlagUnavailable)
			return
		}
		settingsProof = settingsLease.ReceiptLease()
	}
	if err = h.pgPins.TakeParentKidsFlagReceipt(intent, receipt, userLease.ReceiptLease(), settingsProof); err != nil {
		quarantine()
		writeParentKidsFlagError(w, originalRequest, err)
		return
	}
	// Both locks are held. No fallible copy, context test or query may separate
	// these historical ACKs after the single receipt consumption point.
	userLease.Complete()
	if settingsLease != nil {
		settingsLease.Complete()
	}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !management {
		json.NewEncoder(w).Encode(user)
		return
	}
	json.NewEncoder(w).Encode(parentKidsFlagProfile(user))
}
func parentKidsFlagProfile(user models.User) ProfileWithPinStatus {
	return ProfileWithPinStatus{ID: user.ID, Name: user.Name, Color: user.Color, IconURL: user.IconURL, HasPin: user.HasPin(), HasIcon: user.HasIcon(), IsKidsProfile: user.IsKidsProfile, KidsMode: user.KidsMode, KidsMaxRating: user.KidsMaxRating, KidsMaxMovieRating: user.KidsMaxMovieRating, KidsMaxTVRating: user.KidsMaxTVRating, KidsAllowedLists: user.KidsAllowedLists, AllowLiveTakeover: user.AllowLiveTakeover, AllowRecordingInterruption: user.AllowRecordingInterruption, CreatedAt: user.CreatedAt, UpdatedAt: user.UpdatedAt}
}
func writeParentKidsFlagError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, datastore.ErrParentKidsFlagPayload):
		apierror.Write(w, 400, "invalid_request", "invalid profile flag request")
	case errors.Is(err, datastore.ErrParentKidsFlagBodyTooLarge):
		apierror.Write(w, 413, "invalid_request", "profile flag request exceeds sixteen kibibytes")
	case errors.Is(err, datastore.ErrProfilePinAccessNotFound), errors.Is(err, users.ErrUserNotFound):
		apierror.Write(w, 404, "profile_not_found", "profile not found")
	case errors.Is(err, datastore.ErrParentKidsFlagCredential):
		apierror.Write(w, 403, parentauth.RequiredCode, "confirm the account password to manage parental controls")
	case errors.Is(err, datastore.ErrParentKidsFlagChanged), errors.Is(err, datastore.ErrUserWriteConflict), errors.Is(err, datastore.ErrUserSettingsConflict), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		apierror.Write(w, 409, "parent_confirmation_changed", "profile confirmation expired or changed")
	case errors.Is(err, datastore.ErrParentKidsFlagUnknown), errors.Is(err, users.ErrPersistenceUncertain), errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown):
		apierror.Write(w, 503, "parent_confirmation_outcome_unknown", "profile change outcome is unknown; reload saved state before another change")
	default:
		apierror.Write(w, 503, "parent_confirmation_unavailable", "profile confirmation unavailable")
	}
}
