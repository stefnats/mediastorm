package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"net/url"
	"novastream/internal/apierror"
	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/internal/requestsecurity"
	"novastream/services/user_settings"
	"novastream/services/users"
	"strings"
	"time"
)

// Cookie profile creation checks origin before reading a body or claiming its
// one-shot parent intent. API and legacy adult/JSON dispatch remain unchanged.
func parentProfileCreationCookieOrigin(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	if !strings.HasPrefix(r.URL.Path, "/admin/") && !strings.HasPrefix(r.URL.Path, "/account/") {
		return true
	}
	origins, sites := r.Header.Values("Origin"), r.Header.Values("Sec-Fetch-Site")
	if len(origins) != 1 || len(sites) > 1 || len(sites) == 1 && sites[0] != "same-origin" {
		return false
	}
	scheme := "http"
	if requestsecurity.IsSecure(r) {
		scheme = "https"
	}
	expected, expectedErr := url.Parse(scheme + "://" + r.Host)
	origin, originErr := url.Parse(origins[0])
	port := func(u *url.URL) string {
		if p := u.Port(); p != "" {
			return p
		}
		if u.Scheme == "https" {
			return "443"
		}
		return "80"
	}
	return expectedErr == nil && originErr == nil && expected.User == nil && expected.Host != "" && origin.User == nil && origin.Scheme == scheme && origin.Host != "" && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == "" && origin.Opaque == "" && !origin.ForceQuery && strings.EqualFold(origin.Hostname(), expected.Hostname()) && port(origin) == port(expected) && !strings.ContainsAny(origins[0], "?#")
}

type parentProfileCreationPersistence interface {
	AcquireParentProfileCreationLease(context.Context, *datastore.ParentProfileCreationIntent) (*users.ParentProfileCreationLease, error)
}

// CreateProfileManagement is installed only behind existing Cookie auth and
// explicit PG Kids creation (or any present intent). No adult/API policy here.
func (h *UsersHandler) CreateProfileManagement(w http.ResponseWriter, r *http.Request, settings *user_settings.Service) {
	original := r
	w.Header().Set("Cache-Control", "no-store")
	if !parentProfileCreationCookieOrigin(r) {
		http.Error(w, "same-origin request required", http.StatusForbidden)
		return
	}
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
	values := r.Header.Values("X-Parent-Intent")
	echoes := r.Header.Values(datastore.ProfileCreationOperationHeader)
	if len(values) > 1 || len(echoes) > 1 || (len(values) == 0) != (len(echoes) == 0) {
		writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationChanged)
		return
	}
	var intent *datastore.ParentProfileCreationIntent
	var locator string
	if len(values) == 1 {
		if values[0] == "" || echoes[0] == "" {
			writeParentProfileCreationError(w, r, datastore.ErrParentProfileCreationChanged)
			return
		}
		locator = echoes[0]
		intent, err = h.pgPins.ClaimParentProfileCreation(r, body, values[0])
	} else {
		bundle, e := h.pgPins.ParentProfileCreationChallenge(r, body)
		if e != nil {
			writeParentProfileCreationError(w, r, e)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		apierror.Write(w, 403, parentauth.RequiredCode, "confirm the account password to manage parental controls", map[string]string{"parentIntent": bundle.ParentIntent, "creationOperation": bundle.CreationOperation})
		return
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
	w.Header().Set(datastore.ProfileCreationOperationHeader, locator)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		ProfileWithPinStatus
		CreationOperation string `json:"creationOperation"`
	}{response, locator})
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

type profileCreationRecoveryPersistence interface {
	PrepareProfileCreationRecovery(context.Context, *datastore.ProfileCreationRead) (*users.StorageRecoveryTicket, error)
	AcquireProfileCreationRecoveryLease(context.Context, *users.StorageRecoveryTicket, *datastore.ProfileCreationResultSnapshot) (*users.StorageRecoveryLease, error)
}
type profileCreationReadResponse struct {
	Outcome                    string                `json:"outcome"`
	Profile                    *ProfileWithPinStatus `json:"profile,omitempty"`
	UsersWriteBlocked          bool                  `json:"usersWriteBlocked"`
	SettingsPendingUncertainty bool                  `json:"settingsPendingUncertainty"`
}

func (h *UsersHandler) ReadProfileCreationManagement(w http.ResponseWriter, original *http.Request, settings *user_settings.Service) {
	w.Header().Set("Cache-Control", "no-store")
	invalid := original.Method != http.MethodPost || original.URL.RawQuery != "" || len(original.Header.Values(parentauth.PasswordHeader)) > 0 || len(original.Header.Values("X-Parent-Intent")) > 0 || len(original.Header.Values(datastore.ProfileCreationOperationHeader)) > 0
	if original.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(original.Body, 1))
		invalid = invalid || err != nil || len(raw) > 0
	}
	if invalid {
		apierror.Write(w, 400, "invalid_request", "operation read requires a bodyless management request")
		return
	}
	if h.pgPins == nil || settings == nil {
		writeParentProfileCreationError(w, original, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	port, ok := h.Service.(profileCreationRecoveryPersistence)
	if !ok {
		writeParentProfileCreationError(w, original, datastore.ErrParentProfileCreationUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(original.Context(), 10*time.Second)
	defer cancel()
	r := original.WithContext(ctx)
	missing := func() {
		if r.Context().Err() != nil {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(profileCreationReadResponse{Outcome: "unqualified_or_missing", UsersWriteBlocked: true, SettingsPendingUncertainty: true})
	}
	read, err := h.pgPins.PrepareProfileCreationRead(r, mux.Vars(r)["operationRef"])
	if errors.Is(err, datastore.ErrProfileCreationResultMissing) {
		missing()
		return
	}
	if err != nil {
		writeParentProfileCreationError(w, r, err)
		return
	}
	defer read.Close()
	u, err := port.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		writeParentProfileCreationError(w, r, err)
		return
	}
	defer u.Close()
	s, err := settings.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		writeParentProfileCreationError(w, r, err)
		return
	}
	defer s.Close()
	snapshot, err := h.pgPins.ReadProfileCreationResult(read)
	if errors.Is(err, datastore.ErrProfileCreationResultMissing) {
		missing()
		return
	}
	if err != nil {
		writeParentProfileCreationError(w, r, err)
		return
	}
	response := profileCreationReadResponse{Outcome: snapshot.Outcome(), UsersWriteBlocked: true, SettingsPendingUncertainty: true}
	if response.Outcome == "created" || response.Outcome == "not_created" {
		ul, err := port.AcquireProfileCreationRecoveryLease(ctx, u, snapshot)
		if err != nil {
			writeParentProfileCreationError(w, r, err)
			return
		}
		defer ul.Close()
		sl, err := settings.AcquireProfileCreationRecoveryLease(ctx, s, snapshot)
		if err != nil {
			writeParentProfileCreationError(w, r, err)
			return
		}
		defer sl.Close()
		value, present, err := ul.Profile()
		if err != nil {
			writeParentProfileCreationError(w, r, err)
			return
		}
		if present {
			dto := parentKidsFlagProfile(value)
			dto.AccountID = value.AccountID
			dto.ActivityPrivacy = value.ActivityPrivacy
			dto.AllowShareLinks = value.AllowShareLinks
			response.Profile = &dto
		}
		if err = h.pgPins.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
			writeParentProfileCreationError(w, r, err)
			return
		}
		us, ss := ul.Complete(), sl.Complete()
		response.UsersWriteBlocked = us.PendingUncertainty
		response.SettingsPendingUncertainty = ss.PendingUncertainty
	}
	if ctx.Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
