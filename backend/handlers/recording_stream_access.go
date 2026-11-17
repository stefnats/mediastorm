package handlers

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/requestsecurity"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

type recordingOwnerSnapshot interface {
	SnapshotStreamOwner(context.Context, string) (recordings.StreamOwner, error)
}
type recordingSessionSnapshot interface {
	SnapshotForAuthorization(context.Context, string) (*models.Session, error)
}
type recordingAccessAccounts interface {
	Get(string) (models.Account, bool)
}
type recordingAccessProfiles interface {
	Get(string) (models.User, bool)
	PinFingerprint(string) string
}
type recordingUnlockValidator interface {
	Validate([]string, string, string, string) bool
}

// RecordingStreamAccess authorizes recording selectors before capture
// projection, media cache reads, probes or file access. It grants neither
// regional VOD approval nor authority to delegate a recording to a scoped token.
type RecordingStreamAccess struct {
	owners   recordingOwnerSnapshot
	sessions recordingSessionSnapshot
	accounts recordingAccessAccounts
	profiles recordingAccessProfiles
	unlocks  recordingUnlockValidator
	policy   func() (config.SecuritySettings, error)
	pgPins   *profileunlock.PGProfilePins
	now      func() time.Time
}

// SetPGProfilePins selects current storage authorization without consulting the
// legacy profile or unlock caches. Request authentication still belongs to the
// outer API or cookie wrapper, which owns the original frame cancellation.
func (a *RecordingStreamAccess) SetPGProfilePins(current *profileunlock.PGProfilePins) {
	a.pgPins = current
}

func NewRecordingStreamAccess(owners recordingOwnerSnapshot, sessions recordingSessionSnapshot, accounts recordingAccessAccounts, profiles recordingAccessProfiles, unlocks recordingUnlockValidator, policy func() (config.SecuritySettings, error)) *RecordingStreamAccess {
	return &RecordingStreamAccess{owners: owners, sessions: sessions, accounts: accounts, profiles: profiles, unlocks: unlocks, policy: policy, now: time.Now}
}

type recordingAccessState struct {
	owner       recordings.StreamOwner
	profile     models.User
	session     models.Session
	fingerprint string
	master      bool
	pinOutcome  profileunlock.PGPinAccessOutcome
	pgExpiry    time.Time
}

// originalExpiry returns the captured PG authority deadline without reading a
// legacy account cache. JSON mode retains its existing account-expiry behavior.
func (a *RecordingStreamAccess) originalExpiry(state recordingAccessState) (time.Time, bool) {
	if a.pgPins != nil {
		return state.pgExpiry, !state.pgExpiry.IsZero()
	}
	expiry := state.session.ExpiresAt
	for _, accountID := range []string{state.session.AccountID, state.profile.AccountID} {
		account, exists := a.accounts.Get(accountID)
		if !exists {
			return time.Time{}, false
		}
		if account.ExpiresAt != nil && account.ExpiresAt.Before(expiry) {
			expiry = *account.ExpiresAt
		}
	}
	return expiry, true
}

func (a *RecordingStreamAccess) current(r *http.Request, path string) (recordingAccessState, bool) {
	var state recordingAccessState
	if a != nil && a.pgPins != nil {
		return a.currentPG(r, path)
	}
	if a == nil || a.owners == nil || a.sessions == nil || a.accounts == nil || a.profiles == nil || a.unlocks == nil || a.policy == nil || a.now == nil || r.Context().Err() != nil {
		return state, false
	}
	// Existing share/party issuers bind a source string, but do not establish
	// that this recording's stored owner was authorized and PIN-unlocked.
	// Recording delegation stays unavailable until such an issuer receipt exists.
	claimed, ok := auth.GetSession(r)
	if !ok || claimed.Scope != "" || claimed.Token == "" {
		return state, false
	}
	session, err := a.sessions.SnapshotForAuthorization(r.Context(), claimed.Token)
	if err != nil || session == nil || session.Token != claimed.Token || session.Scope != "" || session.AccountID != claimed.AccountID || auth.GetAccountID(r) != session.AccountID || !session.ExpiresAt.After(a.now()) {
		return state, false
	}
	actor, ok := a.accounts.Get(session.AccountID)
	if !ok || actor.ID != session.AccountID || actor.ExpiresAt != nil && !actor.ExpiresAt.After(a.now()) {
		return state, false
	}
	owner, err := a.owners.SnapshotStreamOwner(r.Context(), path)
	if err != nil || owner.RecordingID == "" || owner.ProfileID == "" {
		return state, false
	}
	profile, ok := a.profiles.Get(owner.ProfileID)
	if !ok || profile.ID != owner.ProfileID || profile.AccountID == "" || profile.AccountID != actor.ID && !actor.IsMaster {
		return state, false
	}
	account, ok := a.accounts.Get(profile.AccountID)
	if !ok || account.ID != profile.AccountID || account.ExpiresAt != nil && !account.ExpiresAt.After(a.now()) {
		return state, false
	}
	for key, values := range r.URL.Query() {
		if !strings.EqualFold(key, "profileId") && !strings.EqualFold(key, "userId") {
			continue
		}
		for _, value := range values {
			if value != "" && value != profile.ID {
				return state, false
			}
		}
	}
	for key, value := range mux.Vars(r) {
		if (strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId")) && value != "" && value != profile.ID {
			return state, false
		}
	}
	state = recordingAccessState{owner: owner, profile: profile, session: *session, master: actor.IsMaster}
	state.fingerprint = kidsVODHash([]any{owner, profile, profile.PinHash, actor.ID, actor.IsMaster, actor.ExpiresAt, actor.UpdatedAt, account.ID, account.ExpiresAt, account.UpdatedAt, session})
	return state, r.Context().Err() == nil
}

func (a *RecordingStreamAccess) currentPG(r *http.Request, path string) (recordingAccessState, bool) {
	var state recordingAccessState
	if a.owners == nil || a.policy == nil || a.now == nil || r == nil || r.Context().Err() != nil || a.pgPins.CheckOriginal(r) != nil {
		return state, false
	}
	session, ok := auth.GetSession(r)
	if !ok || session.Scope != "" || session.Token == "" {
		return state, false
	}
	owner, err := a.owners.SnapshotStreamOwner(r.Context(), path)
	if err != nil || owner.RecordingID == "" || owner.ProfileID == "" {
		return state, false
	}
	current, err := a.pgPins.Current(r, owner.ProfileID)
	if err != nil {
		return state, false
	}
	switch current.Outcome() {
	case profileunlock.PGPinNoPin, profileunlock.PGPinLocked, profileunlock.PGPinUnlocked:
	default:
		return state, false
	}
	kids, err := current.IsKids()
	if err != nil {
		return state, false
	}
	correlation, err := current.Correlation()
	if err != nil {
		return state, false
	}
	account, err := current.Owner()
	if err != nil || account.ID == "" {
		return state, false
	}
	expiry := session.ExpiresAt
	if account.ExpiresAt != nil && account.ExpiresAt.Before(expiry) {
		expiry = *account.ExpiresAt
	}
	if deadline, ok := r.Context().Deadline(); ok && deadline.Before(expiry) {
		expiry = deadline
	}
	for key, values := range r.URL.Query() {
		if strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId") {
			for _, value := range values {
				if value != "" && value != owner.ProfileID {
					return state, false
				}
			}
		}
	}
	for key, value := range mux.Vars(r) {
		if (strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId")) && value != "" && value != owner.ProfileID {
			return state, false
		}
	}
	state = recordingAccessState{owner: owner, profile: models.User{ID: owner.ProfileID, AccountID: account.ID, IsKidsProfile: kids}, session: session, master: session.IsMaster, pinOutcome: current.Outcome(), pgExpiry: expiry}
	state.fingerprint = kidsVODHash([]any{owner, correlation})
	return state, r.Context().Err() == nil && a.pgPins.CheckOriginal(r) == nil
}

func (a *RecordingStreamAccess) Authorize(w http.ResponseWriter, r *http.Request, path string) (*http.Request, bool) {
	deny := func() (*http.Request, bool) {
		apierror.Write(w, http.StatusNotFound, "resource_not_found", "resource not found")
		return nil, false
	}
	before, ok := a.current(r, path)
	if !ok {
		return deny()
	}
	if before.profile.IsKidsProfile {
		apierror.Write(w, http.StatusForbidden, "kids_live_tv_unavailable", "Live TV is unavailable for kids profiles")
		return nil, false
	}
	policy, err := a.policy()
	if err != nil {
		return deny()
	}
	if a.pgPins != nil && policy.EnforceProfilePins && before.pinOutcome == profileunlock.PGPinLocked {
		apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked", map[string]string{"profileId": before.profile.ID})
		return nil, false
	}
	if a.pgPins == nil && policy.EnforceProfilePins && before.profile.HasPin() {
		fingerprint := a.profiles.PinFingerprint(before.profile.ID)
		if fingerprint == "" || !a.unlocks.Validate(profileunlock.RequestTokens(r), before.session.Token, before.profile.ID, fingerprint) {
			apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked", map[string]string{"profileId": before.profile.ID})
			return nil, false
		}
	}
	latestPolicy, err := a.policy()
	after, ok := a.current(r, path)
	if !ok || err != nil || latestPolicy.EnforceProfilePins != policy.EnforceProfilePins || after.fingerprint != before.fingerprint || r.Context().Err() != nil {
		return deny()
	}
	session := after.session
	session.IsMaster = after.master
	ctx := context.WithValue(r.Context(), auth.ContextKeyIsMaster, after.master)
	ctx = context.WithValue(ctx, auth.ContextKeySession, session)
	return r.WithContext(ctx), true
}

func (a *RecordingStreamAccess) Check(w http.ResponseWriter, r *http.Request, path string) bool {
	_, ok := a.Authorize(w, r, path)
	return ok
}

// This marker is available only through the separately registered internal
// handler. It preserves the existing trusted-local-process assumption; it is
// not an owner/input-bound capability and must not enable strict VOD policy.
type recordingInternalStreamKey struct{}

func isRecordingInternalStream(r *http.Request) bool {
	marked, _ := r.Context().Value(recordingInternalStreamKey{}).(bool)
	return marked && requestsecurity.IsLoopback(r) && (r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions)
}
