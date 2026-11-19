package profileunlock

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/sessions"
)

// PGPinIssuer is a low-level facade over the generation-bound datastore
// protocol. It does not call Service.Issue, Validate, Rebind or its grant cache.
// Prepared requests cannot move between facade instances, even on one store.
type PGPinIssuer struct {
	self *PGPinIssuer
	core *datastore.PGPinIssuer
}

type PGPinIntent = datastore.PGPinIntent
type PGPinProof = datastore.PGPinProof
type PGPinCommitReceipt = datastore.PGPinCommitReceipt

func NewPGPinIssuer(store *datastore.DataStore) *PGPinIssuer {
	v := &PGPinIssuer{core: datastore.NewPGPinIssuer(store)}
	v.self = v
	return v
}
func (*PGPinIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGPinIssuer) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("PGPinIssuer(opaque)")) }
func (v *PGPinIssuer) Prepare(r *http.Request, targetID string, configuredTTL time.Duration) (*PGPinIntent, error) {
	if v == nil || v.self != v || v.core == nil {
		return nil, datastore.ErrPGPinUsed
	}
	return v.core.Prepare(r, targetID, configuredTTL)
}
func (v *PGPinIssuer) Verify(intent *PGPinIntent, rawPIN string) (*PGPinProof, error) {
	if v == nil || v.self != v || v.core == nil {
		return nil, datastore.ErrPGPinUsed
	}
	return v.core.Verify(intent, rawPIN)
}
func (v *PGPinIssuer) Commit(intent *PGPinIntent, proof *PGPinProof) (*PGPinCommitReceipt, error) {
	if v == nil || v.self != v || v.core == nil {
		return nil, datastore.ErrPGPinUsed
	}
	return v.core.Commit(intent, proof)
}

// PGPinAccess is a separate storage-observation adapter. It observes the
// current absence of a PIN as well as current grants and never asks cached
// users.HasPin/PinFingerprint or Service.Validate to decide the result.
type PGPinAccess struct {
	self *PGPinAccess
	core *datastore.ProfilePinAccessReader
}

type PGPinAccessState = datastore.ProfilePinAccessState
type PGPinAccessOutcome = datastore.ProfilePinAccessOutcome

const (
	PGPinUnavailable = datastore.ProfilePinUnavailable
	PGPinNotFound    = datastore.ProfilePinNotFound
	PGPinLocked      = datastore.ProfilePinLocked
	PGPinNoPin       = datastore.ProfilePinNoPin
	PGPinUnlocked    = datastore.ProfilePinUnlocked
)

func NewPGPinAccess(store *datastore.DataStore) *PGPinAccess {
	value := &PGPinAccess{core: datastore.NewProfilePinAccessReader(store)}
	value.self = value
	return value
}
func (*PGPinAccess) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGPinAccess) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("PGPinAccess(opaque)")) }
func (v *PGPinAccess) Read(r *http.Request, targetID string) (*PGPinAccessState, error) {
	if v == nil || v.self != v || v.core == nil {
		return nil, datastore.ErrProfilePinAccessUsed
	}
	if r == nil {
		return nil, datastore.ErrProfilePinAccessUnavailable
	}
	return v.core.Read(r, targetID, RequestTokens(r))
}
func (v *PGPinAccess) Check(state *PGPinAccessState) (PGPinAccessOutcome, error) {
	if v == nil || v.self != v || v.core == nil {
		return PGPinUnavailable, datastore.ErrProfilePinAccessUsed
	}
	return v.core.Check(state)
}

// PGProfilePins is the explicit public PostgreSQL implementation. Constructing
// it does not enable PIN policy; callers retain the existing security setting.
// Once selected, storage errors never delegate to the legacy JSON/cache path.
type PGProfilePins struct {
	self            *PGProfilePins
	store           *datastore.DataStore
	issuer          *PGPinIssuer
	access          *PGPinAccess
	scopeSessions   *sessions.Service
	parentModeMu    *sync.Mutex
	parentModes     map[string]*parentModeEntry
	parentModeBytes int
}

func NewPGProfilePins(store *datastore.DataStore, scoped ...*sessions.Service) *PGProfilePins {
	reader := &PGPinAccess{core: datastore.NewBoundProfilePinAccessReader(store)}
	reader.self = reader
	value := &PGProfilePins{store: store, issuer: NewPGPinIssuer(store), access: reader, parentModeMu: &sync.Mutex{}}
	value.self = value
	if len(scoped) > 0 {
		value.scopeSessions = scoped[0]
	}
	return value
}
func (v *PGProfilePins) valid() bool {
	return v != nil && v.self == v && v.store != nil && v.parentModeMu != nil
}
func (*PGProfilePins) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGProfilePins) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("PGProfilePins(opaque)")) }
func (v *PGProfilePins) Authenticate(r *http.Request, token, clientID string) (*http.Request, context.CancelFunc, error) {
	if !v.valid() {
		return nil, nil, datastore.ErrProfilePinAccessUnavailable
	}
	checked, release, err := v.store.AuthenticateProfilePinRequest(r, token, clientID)
	if err != nil {
		return nil, nil, err
	}
	var resolver func(context.Context, string) (string, error)
	if v.scopeSessions != nil {
		resolver = v.scopeSessions.ResolveProfilePinStreamResource
	}
	checked, err = v.store.ResolveProfilePinStreamRequest(checked, resolver)
	if err != nil {
		release()
		return nil, nil, err
	}
	return checked, release, nil
}
func (v *PGProfilePins) Identity(r *http.Request) (models.Session, models.Account, error) {
	if !v.valid() {
		return models.Session{}, models.Account{}, datastore.ErrProfilePinAccessUnavailable
	}
	return v.store.ProfilePinFrameIdentity(r)
}
func (v *PGProfilePins) CheckOriginal(r *http.Request) error {
	if !v.valid() {
		return datastore.ErrProfilePinAccessUnavailable
	}
	return v.store.CheckProfilePinRequestFrame(r)
}

type PGPinObservation struct {
	ctx         context.Context
	outcome     PGPinAccessOutcome
	kids        bool
	correlation string
	owner       models.Account
}

func (*PGPinObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*PGPinObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("PGPinObservation(opaque)"))
}
func (v *PGPinObservation) Outcome() PGPinAccessOutcome {
	if v == nil || v.ctx == nil || v.ctx.Err() != nil {
		return PGPinUnavailable
	}
	return v.outcome
}
func (v *PGPinObservation) IsKids() (bool, error) {
	if v == nil || v.Outcome() == PGPinUnavailable || v.outcome == PGPinNotFound {
		return false, datastore.ErrProfilePinAccessUnavailable
	}
	return v.kids, nil
}
func (v *PGPinObservation) Correlation() (string, error) {
	if v == nil || v.Outcome() == PGPinUnavailable || v.outcome == PGPinNotFound {
		return "", datastore.ErrProfilePinAccessUnavailable
	}
	return v.correlation, nil
}
func (v *PGProfilePins) Current(r *http.Request, targetID string) (*PGPinObservation, error) {
	if !v.valid() {
		return nil, datastore.ErrProfilePinAccessUnavailable
	}
	state, err := v.access.Read(r, targetID)
	if err != nil {
		return nil, err
	}
	defer state.Close()
	outcome, err := v.access.Check(state)
	if err != nil {
		return nil, err
	}
	value := &PGPinObservation{ctx: r.Context(), outcome: outcome}
	if outcome != PGPinNotFound && outcome != PGPinUnavailable {
		value.owner, err = v.access.core.ProfilePinAccessOwner(state)
		if err != nil {
			return nil, err
		}
		value.kids, value.correlation, err = v.access.core.ProfilePinAccessFacts(state)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

var ErrPGProfileNotFound = errors.New("profile not found")

type PinIssuanceAcknowledger interface {
	AcknowledgePinIssuance(context.Context, string, *datastore.PGPinCommitReceipt) error
}

// Verify performs the actual original request workflow, including a targeted
// users-revision acknowledgement before one-shot token release. No receipt or
// raw token is admitted to Service.grants. A known remote commit may remain
// after a later acknowledgement/release failure; it is never rolled back using
// replacement credentials or silently replayed.
func (v *PGProfilePins) Verify(r *http.Request, targetID, rawPIN string, ttl time.Duration, ack PinIssuanceAcknowledger) (string, time.Time, error) {
	if !v.valid() || ack == nil {
		return "", time.Time{}, datastore.ErrPGPinUnavailable
	}
	current, err := v.Current(r, targetID)
	if err != nil {
		return "", time.Time{}, err
	}
	switch current.Outcome() {
	case PGPinNotFound:
		return "", time.Time{}, ErrPGProfileNotFound
	case PGPinNoPin:
		return "", time.Time{}, v.issuer.core.RecordNoPinVerification(r, targetID)
	case PGPinLocked, PGPinUnlocked:
	default:
		return "", time.Time{}, datastore.ErrPGPinChanged
	}
	intent, err := v.issuer.Prepare(r, targetID, ttl)
	if err != nil {
		return "", time.Time{}, err
	}
	defer intent.Close()
	proof, err := v.issuer.Verify(intent, rawPIN)
	if err != nil {
		return "", time.Time{}, err
	}
	receipt, err := v.issuer.Commit(intent, proof)
	if err != nil {
		return "", time.Time{}, err
	}
	defer receipt.Close()
	if err = ack.AcknowledgePinIssuance(r.Context(), targetID, receipt); err != nil {
		return "", time.Time{}, err
	}
	return v.issuer.core.Release(r, receipt)
}

func (v *PGProfilePins) Owns(r *http.Request, targetID string) (bool, error) {
	current, err := v.Current(r, targetID)
	if err != nil {
		return false, err
	}
	switch current.Outcome() {
	case PGPinNotFound:
		return false, nil
	case PGPinNoPin, PGPinLocked, PGPinUnlocked:
		return true, nil
	default:
		return false, datastore.ErrProfilePinAccessUnavailable
	}
}
func (v *PGProfilePins) IsKids(r *http.Request, targetID string) (bool, error) {
	current, err := v.Current(r, targetID)
	if err != nil {
		return false, err
	}
	if current.Outcome() == PGPinNotFound {
		return false, ErrPGProfileNotFound
	}
	return current.IsKids()
}

type PGPinChangeIntent = datastore.PGPinChangeIntent
type PinChangePersistence interface {
	CommitPGPinChange(context.Context, *datastore.PGPinChangeIntent) (models.User, error)
}

func (v *PGProfilePins) PrepareOwnedPinChange(r *http.Request, targetID, pin string, clearing bool) (*PGPinChangeIntent, error) {
	if !v.valid() {
		return nil, datastore.ErrPGPinUnavailable
	}
	return v.store.PrepareOwnedPinChange(r, targetID, pin, clearing, RequestTokens(r))
}
func (v *PGProfilePins) PrepareAdministrativePinChange(r *http.Request, targetID, pin string, clearing bool) (*PGPinChangeIntent, error) {
	if !v.valid() {
		return nil, datastore.ErrPGPinUnavailable
	}
	return v.store.PrepareAdministrativePinChange(r, targetID, pin, clearing, RequestTokens(r))
}

// CheckVerified is one-shot and checks only this facade's exact original
// verification post-state. It is for the existing cookie response bridge.
func (v *PGProfilePins) CheckVerified(r *http.Request, targetID string) error {
	if !v.valid() {
		return datastore.ErrPGPinUnavailable
	}
	return v.issuer.core.CheckVerifiedProfilePinRequest(r, targetID)
}

func (v *PGPinObservation) Owner() (models.Account, error) {
	if v == nil || v.Outcome() == PGPinUnavailable || v.outcome == PGPinNotFound || v.owner.ID == "" {
		return models.Account{}, datastore.ErrProfilePinAccessUnavailable
	}
	result := models.Account{ID: v.owner.ID}
	if v.owner.ExpiresAt != nil {
		copy := *v.owner.ExpiresAt
		result.ExpiresAt = &copy
	}
	return result, nil
}

const ParentModeIntentHeader = datastore.ParentModeIntentHeader
const parentModeMaximumEntries = 512
const parentModeMaximumActorEntries = 8
const parentModeMaximumBytes = 4 << 20

type ParentModeIntent = datastore.ParentModeIntent
type parentConfirmationAction uint8

const (
	parentConfirmationMode parentConfirmationAction = iota + 1
	parentConfirmationListRemoval
	parentConfirmationListAdd
	parentConfirmationListReplace
	parentConfirmationRatingGeneric
	parentConfirmationRatingMovie
	parentConfirmationRatingTV
	parentConfirmationRatingPair
	parentConfirmationKidsFlag
	parentConfirmationProfileCreation
	parentConfirmationRegionalPolicy
	parentConfirmationLocalFile
)

type parentModeEntry struct {
	action   parentConfirmationAction
	intent   *ParentModeIntent
	removal  *ParentListRemovalIntent
	write    *ParentListWriteIntent
	rating   *ParentRatingIntent
	file     ParentFileAttachment
	kids     *ParentKidsFlagIntent
	creation *datastore.ParentProfileCreationIntent
	actor    string
	bytes    int
	deadline time.Time
	timer    *time.Timer
}

func (entry *parentModeEntry) close() {
	if entry.file != nil {
		entry.file.Close()
	}
	if entry.creation != nil {
		entry.creation.Close()
	}
	if entry.kids != nil {
		entry.kids.Close()
	}
	if entry.intent != nil {
		entry.intent.Close()
	}
	if entry.removal != nil {
		entry.removal.Close()
	}
	if entry.write != nil {
		entry.write.Close()
	}
	if entry.rating != nil {
		entry.rating.Close()
	}
}

func parentModeTokenKey(token string) (string, bool) {
	return parentConfirmationTokenKey(token, "pmi_")
}
func parentConfirmationTokenKey(token, prefix string) (string, bool) {
	if len(token) != 68 || !strings.HasPrefix(token, prefix) {
		return "", false
	}
	raw, err := hex.DecodeString(token[4:])
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != token[4:] {
		return "", false
	}
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:]), true
}
func (v *PGProfilePins) ParentModeChallenge(r *http.Request, target, mode string) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentModeUnavailable
	}
	intent, err := v.store.PrepareParentMode(r, target, mode, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentModeBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	return v.registerParentConfirmation(r, "pmi_", &parentModeEntry{action: parentConfirmationMode, intent: intent, actor: actor, bytes: size, deadline: deadline})
}

// Both typed actions share one bounded registry; neither can spend a second
// per-account budget or reinterpret a slot belonging to the other action.
func (v *PGProfilePins) registerParentConfirmation(r *http.Request, prefix string, entry *parentModeEntry) (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		entry.close()
		return "", datastore.ErrParentModeUnavailable
	}
	token := prefix + hex.EncodeToString(random[:])
	key, _ := parentConfirmationTokenKey(token, prefix)
	v.parentModeMu.Lock()
	defer v.parentModeMu.Unlock()
	if v.parentModes == nil {
		v.parentModes = map[string]*parentModeEntry{}
	}
	count := 0
	for existingKey, existing := range v.parentModes {
		if !existing.deadline.After(time.Now()) {
			delete(v.parentModes, existingKey)
			v.parentModeBytes -= existing.bytes
			existing.timer.Stop()
			existing.close()
			continue
		}
		if existing.actor == entry.actor {
			count++
		}
	}
	if len(v.parentModes) >= parentModeMaximumEntries || count >= parentModeMaximumActorEntries || entry.bytes > parentModeMaximumBytes-v.parentModeBytes || v.parentModes[key] != nil || r.Context().Err() != nil || !entry.deadline.After(time.Now()) {
		entry.close()
		return "", datastore.ErrParentModeUnavailable
	}
	v.parentModes[key] = entry
	v.parentModeBytes += entry.bytes
	entry.timer = time.AfterFunc(time.Until(entry.deadline), func() {
		v.parentModeMu.Lock()
		defer v.parentModeMu.Unlock()
		if v.parentModes[key] == entry {
			delete(v.parentModes, key)
			v.parentModeBytes -= entry.bytes
			entry.close()
		}
	})
	return token, nil
}

func (v *PGProfilePins) claimParentConfirmation(key string, action parentConfirmationAction) *parentModeEntry {
	v.parentModeMu.Lock()
	defer v.parentModeMu.Unlock()
	entry := v.parentModes[key]
	if entry == nil || entry.action != action {
		return nil
	}
	delete(v.parentModes, key)
	v.parentModeBytes -= entry.bytes
	entry.timer.Stop()
	return entry
}
func (v *PGProfilePins) ClaimParentMode(r *http.Request, target, mode, token string) (*ParentModeIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentModeUnavailable
	}
	key, valid := parentModeTokenKey(token)
	if !valid {
		return nil, datastore.ErrParentModeChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationMode)
	if entry == nil {
		return nil, datastore.ErrParentModeChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.intent.Close()
		return nil, datastore.ErrParentModeChanged
	}
	if err := v.store.BindParentModeRetry(r, entry.intent, target, mode, RequestTokens(r)); err != nil {
		entry.intent.Close()
		return nil, err
	}
	return entry.intent, nil
}
func (v *PGProfilePins) ImmediateParentMode(r *http.Request, target, mode string) (*ParentModeIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentModeUnavailable
	}
	intent, err := v.store.PrepareParentMode(r, target, mode, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentModeRetry(r, intent, target, mode, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}
func (v *PGProfilePins) VerifyParentMode(r *http.Request, intent *ParentModeIntent) error {
	if !v.valid() {
		return datastore.ErrParentModeUnavailable
	}
	return v.store.VerifyParentModePassword(r, intent)
}

func (v *PGProfilePins) BoundParentModeRequest(r *http.Request, intent *ParentModeIntent) (*http.Request, error) {
	if !v.valid() {
		return nil, datastore.ErrParentModeUnavailable
	}
	return v.store.BoundParentModeRequest(r, intent)
}

// ParentListRemovalIntent cannot be used as a mode change or media grant.
type ParentListRemovalIntent = datastore.ParentListRemovalIntent

func (v *PGProfilePins) ParentListRemovalChallenge(r *http.Request, target, listURL string) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentListRemovalUnavailable
	}
	intent, err := v.store.PrepareParentListRemoval(r, target, listURL, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentListRemovalBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "pli_", &parentModeEntry{action: parentConfirmationListRemoval, removal: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentListRemovalUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentListRemoval(r *http.Request, target, listURL, token string) (*ParentListRemovalIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentListRemovalUnavailable
	}
	key, valid := parentConfirmationTokenKey(token, "pli_")
	if !valid {
		return nil, datastore.ErrParentListRemovalChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationListRemoval)
	if entry == nil {
		return nil, datastore.ErrParentListRemovalChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentListRemovalChanged
	}
	if err := v.store.BindParentListRemovalRetry(r, entry.removal, target, listURL, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.removal, nil
}
func (v *PGProfilePins) ImmediateParentListRemoval(r *http.Request, target, listURL string) (*ParentListRemovalIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentListRemovalUnavailable
	}
	intent, err := v.store.PrepareParentListRemoval(r, target, listURL, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentListRemovalRetry(r, intent, target, listURL, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}
func (v *PGProfilePins) VerifyParentListRemoval(r *http.Request, intent *ParentListRemovalIntent) error {
	if !v.valid() {
		return datastore.ErrParentListRemovalUnavailable
	}
	return v.store.VerifyParentListRemovalPassword(r, intent)
}
func (v *PGProfilePins) BoundParentListRemovalRequest(r *http.Request, intent *ParentListRemovalIntent) (*http.Request, error) {
	if !v.valid() {
		return nil, datastore.ErrParentListRemovalUnavailable
	}
	return v.store.BoundParentListRemovalRequest(r, intent)
}

// ParentListWriteIntent remains opaque and action-bound throughout the facade.
type ParentListWriteIntent = datastore.ParentListWriteIntent

func (v *PGProfilePins) ParentListAddChallenge(r *http.Request, target string, body []byte) (string, error) {
	return v.parentListWriteChallenge(parentConfirmationListAdd, "pla_", r, target, body)
}
func (v *PGProfilePins) ParentListReplaceChallenge(r *http.Request, target string, body []byte) (string, error) {
	return v.parentListWriteChallenge(parentConfirmationListReplace, "plw_", r, target, body)
}
func (v *PGProfilePins) parentListWriteChallenge(action parentConfirmationAction, prefix string, r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentListWriteUnavailable
	}
	var intent *ParentListWriteIntent
	var err error
	if action == parentConfirmationListAdd {
		intent, err = v.store.PrepareParentListAdd(r, target, body, RequestTokens(r))
	} else if action == parentConfirmationListReplace {
		intent, err = v.store.PrepareParentListReplace(r, target, body, RequestTokens(r))
	} else {
		return "", datastore.ErrParentListWriteChanged
	}
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentListWriteBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, prefix, &parentModeEntry{action: action, write: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentListWriteUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentListAdd(r *http.Request, target string, body []byte, token string) (*ParentListWriteIntent, error) {
	return v.claimParentListWrite(parentConfirmationListAdd, "pla_", r, target, body, token)
}
func (v *PGProfilePins) ClaimParentListReplace(r *http.Request, target string, body []byte, token string) (*ParentListWriteIntent, error) {
	return v.claimParentListWrite(parentConfirmationListReplace, "plw_", r, target, body, token)
}
func (v *PGProfilePins) claimParentListWrite(action parentConfirmationAction, prefix string, r *http.Request, target string, body []byte, token string) (*ParentListWriteIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentListWriteUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, prefix)
	if !ok {
		return nil, datastore.ErrParentListWriteChanged
	}
	entry := v.claimParentConfirmation(key, action)
	if entry == nil {
		return nil, datastore.ErrParentListWriteChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentListWriteChanged
	}
	var err error
	if action == parentConfirmationListAdd {
		err = v.store.BindParentListAddRetry(r, entry.write, target, body, RequestTokens(r))
	} else if action == parentConfirmationListReplace {
		err = v.store.BindParentListReplaceRetry(r, entry.write, target, body, RequestTokens(r))
	} else {
		err = datastore.ErrParentListWriteChanged
	}
	if err != nil {
		entry.close()
		return nil, err
	}
	return entry.write, nil
}
func (v *PGProfilePins) ImmediateParentListAdd(r *http.Request, target string, body []byte) (*ParentListWriteIntent, error) {
	return v.immediateParentListWrite(parentConfirmationListAdd, r, target, body)
}
func (v *PGProfilePins) ImmediateParentListReplace(r *http.Request, target string, body []byte) (*ParentListWriteIntent, error) {
	return v.immediateParentListWrite(parentConfirmationListReplace, r, target, body)
}
func (v *PGProfilePins) immediateParentListWrite(action parentConfirmationAction, r *http.Request, target string, body []byte) (*ParentListWriteIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentListWriteUnavailable
	}
	var intent *ParentListWriteIntent
	var err error
	if action == parentConfirmationListAdd {
		intent, err = v.store.PrepareParentListAdd(r, target, body, RequestTokens(r))
	} else if action == parentConfirmationListReplace {
		intent, err = v.store.PrepareParentListReplace(r, target, body, RequestTokens(r))
	} else {
		return nil, datastore.ErrParentListWriteChanged
	}
	if err != nil {
		return nil, err
	}
	if action == parentConfirmationListAdd {
		err = v.store.BindParentListAddRetry(r, intent, target, body, RequestTokens(r))
	} else {
		err = v.store.BindParentListReplaceRetry(r, intent, target, body, RequestTokens(r))
	}
	if err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}
func (v *PGProfilePins) VerifyParentListWrite(r *http.Request, intent *ParentListWriteIntent) error {
	if !v.valid() {
		return datastore.ErrParentListWriteUnavailable
	}
	return v.store.VerifyParentListWritePassword(r, intent)
}
func (v *PGProfilePins) BoundParentListWriteRequest(r *http.Request, intent *ParentListWriteIntent) (*http.Request, error) {
	if !v.valid() {
		return nil, datastore.ErrParentListWriteUnavailable
	}
	return v.store.BoundParentListWriteRequest(r, intent)
}

type ParentRatingIntent = datastore.ParentRatingIntent

func (v *PGProfilePins) ParentRatingGenericChallenge(r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingGeneric(r, target, body, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "prg_", &parentModeEntry{action: parentConfirmationRatingGeneric, rating: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentRatingGeneric(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "prg_")
	if !ok {
		return nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationRatingGeneric)
	if entry == nil {
		return nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentRatingGenericRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.rating, nil
}
func (v *PGProfilePins) ImmediateParentRatingGeneric(r *http.Request, target string, body []byte) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingGeneric(r, target, body, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentRatingGenericRetry(r, intent, target, body, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}

func (v *PGProfilePins) ParentRatingMovieChallenge(r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingMovie(r, target, body, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "prm_", &parentModeEntry{action: parentConfirmationRatingMovie, rating: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentRatingMovie(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "prm_")
	if !ok {
		return nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationRatingMovie)
	if entry == nil {
		return nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentRatingMovieRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.rating, nil
}
func (v *PGProfilePins) ImmediateParentRatingMovie(r *http.Request, target string, body []byte) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingMovie(r, target, body, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentRatingMovieRetry(r, intent, target, body, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}

func (v *PGProfilePins) ParentRatingTVChallenge(r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingTV(r, target, body, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "prt_", &parentModeEntry{action: parentConfirmationRatingTV, rating: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentRatingTV(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "prt_")
	if !ok {
		return nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationRatingTV)
	if entry == nil {
		return nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentRatingTVRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.rating, nil
}
func (v *PGProfilePins) ImmediateParentRatingTV(r *http.Request, target string, body []byte) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingTV(r, target, body, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentRatingTVRetry(r, intent, target, body, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}

func (v *PGProfilePins) ParentRatingPairChallenge(r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingPair(r, target, body, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "prp_", &parentModeEntry{action: parentConfirmationRatingPair, rating: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentRatingPair(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "prp_")
	if !ok {
		return nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationRatingPair)
	if entry == nil {
		return nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentRatingPairRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.rating, nil
}
func (v *PGProfilePins) ImmediateParentRatingPair(r *http.Request, target string, body []byte) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRatingPair(r, target, body, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentRatingPairRetry(r, intent, target, body, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}

func (v *PGProfilePins) VerifyParentRating(r *http.Request, intent *ParentRatingIntent) error {
	if !v.valid() {
		return datastore.ErrParentRatingUnavailable
	}
	return v.store.VerifyParentRatingPassword(r, intent)
}
func (v *PGProfilePins) BoundParentRatingRequest(r *http.Request, intent *ParentRatingIntent) (*http.Request, error) {
	if !v.valid() {
		return nil, datastore.ErrParentRatingUnavailable
	}
	return v.store.BoundParentRatingRequest(r, intent)
}

// ParentKidsFlagIntent is distinct from the older uninstalled flag-only port.
type ParentKidsFlagIntent = datastore.ParentKidsFlagIntent

func (v *PGProfilePins) parentKidsFlagChallenge(r *http.Request, target string, body []byte, management bool) (string, error) {
	if v == nil || v.self != v || v.store == nil {
		return "", datastore.ErrParentKidsFlagUnavailable
	}
	var i *ParentKidsFlagIntent
	var err error
	if management {
		i, err = v.store.PrepareParentKidsFlagManagement(r, target, body, RequestTokens(r))
	} else {
		i, err = v.store.PrepareParentKidsFlagAPI(r, target, body, RequestTokens(r))
	}
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentKidsFlagBudget(i)
	if err != nil {
		i.Close()
		return "", err
	}
	return v.registerParentConfirmation(r, "pkf_", &parentModeEntry{action: parentConfirmationKidsFlag, kids: i, actor: actor, bytes: size, deadline: deadline})
}
func (v *PGProfilePins) ParentKidsFlagAPIChallenge(r *http.Request, target string, body []byte) (string, error) {
	return v.parentKidsFlagChallenge(r, target, body, false)
}
func (v *PGProfilePins) ParentKidsFlagManagementChallenge(r *http.Request, target string, body []byte) (string, error) {
	return v.parentKidsFlagChallenge(r, target, body, true)
}
func (v *PGProfilePins) claimParentKidsFlag(r *http.Request, target string, body []byte, token string, management bool) (*ParentKidsFlagIntent, error) {
	if v == nil || v.self != v || v.store == nil {
		return nil, datastore.ErrParentKidsFlagUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "pkf_")
	if !ok {
		return nil, datastore.ErrParentKidsFlagChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationKidsFlag)
	if entry == nil || entry.kids == nil {
		return nil, datastore.ErrParentKidsFlagChanged
	}
	var err error
	if management {
		err = v.store.BindParentKidsFlagManagementRetry(r, entry.kids, target, body, RequestTokens(r))
	} else {
		err = v.store.BindParentKidsFlagAPIRetry(r, entry.kids, target, body, RequestTokens(r))
	}
	if err != nil {
		entry.close()
		return nil, err
	}
	return entry.kids, nil
}
func (v *PGProfilePins) ClaimParentKidsFlagAPI(r *http.Request, target string, body []byte, token string) (*ParentKidsFlagIntent, error) {
	return v.claimParentKidsFlag(r, target, body, token, false)
}
func (v *PGProfilePins) ClaimParentKidsFlagManagement(r *http.Request, target string, body []byte, token string) (*ParentKidsFlagIntent, error) {
	return v.claimParentKidsFlag(r, target, body, token, true)
}
func (v *PGProfilePins) immediateParentKidsFlag(r *http.Request, target string, body []byte, management bool) (*ParentKidsFlagIntent, error) {
	if v == nil || v.self != v || v.store == nil {
		return nil, datastore.ErrParentKidsFlagUnavailable
	}
	var i *ParentKidsFlagIntent
	var err error
	if management {
		i, err = v.store.PrepareParentKidsFlagManagement(r, target, body, RequestTokens(r))
	} else {
		i, err = v.store.PrepareParentKidsFlagAPI(r, target, body, RequestTokens(r))
	}
	if err != nil {
		return nil, err
	}
	if management {
		err = v.store.BindParentKidsFlagManagementRetry(r, i, target, body, RequestTokens(r))
	} else {
		err = v.store.BindParentKidsFlagAPIRetry(r, i, target, body, RequestTokens(r))
	}
	if err != nil {
		i.Close()
		return nil, err
	}
	return i, nil
}
func (v *PGProfilePins) ImmediateParentKidsFlagAPI(r *http.Request, target string, body []byte) (*ParentKidsFlagIntent, error) {
	return v.immediateParentKidsFlag(r, target, body, false)
}
func (v *PGProfilePins) ImmediateParentKidsFlagManagement(r *http.Request, target string, body []byte) (*ParentKidsFlagIntent, error) {
	return v.immediateParentKidsFlag(r, target, body, true)
}
func (v *PGProfilePins) BoundParentKidsFlagRequest(r *http.Request, i *ParentKidsFlagIntent) (*http.Request, error) {
	if v == nil || v.self != v || v.store == nil {
		return nil, datastore.ErrParentKidsFlagUnavailable
	}
	return v.store.BoundParentKidsFlagRequest(r, i)
}
func (v *PGProfilePins) VerifyParentKidsFlag(r *http.Request, i *ParentKidsFlagIntent) error {
	if v == nil || v.self != v || v.store == nil {
		return datastore.ErrParentKidsFlagUnavailable
	}
	return v.store.VerifyParentKidsFlagPassword(r, i)
}
func (v *PGProfilePins) ParentKidsFlagSettingsRequired(i *ParentKidsFlagIntent) (bool, error) {
	if v == nil || v.self != v || v.store == nil {
		return false, datastore.ErrParentKidsFlagUnavailable
	}
	return v.store.ParentKidsFlagSettingsRequired(i)
}
func (v *PGProfilePins) TakeParentKidsFlagReceipt(i *ParentKidsFlagIntent, r *datastore.ParentKidsFlagReceipt, u, s *datastore.ParentKidsFlagCacheLease) error {
	if v == nil || v.self != v || v.store == nil {
		return datastore.ErrParentKidsFlagUnavailable
	}
	return v.store.TakeParentKidsFlagReceipt(i, r, u, s)
}

// ParentProfileCreationChallenge shares the existing total/actor/byte registry
// with every other parent action. Its token is never a ledger locator.
type ParentProfileCreationChallengeBundle struct {
	ParentIntent      string `json:"parentIntent"`
	CreationOperation string `json:"creationOperation"`
}

func (v *PGProfilePins) ParentProfileCreationChallenge(r *http.Request, body []byte) (ParentProfileCreationChallengeBundle, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return ParentProfileCreationChallengeBundle{}, datastore.ErrParentProfileCreationUnavailable
	}
	intent, err := v.store.PrepareParentProfileCreationManagement(r, body, RequestTokens(r))
	if err != nil {
		return ParentProfileCreationChallengeBundle{}, err
	}
	locator, err := v.store.ParentProfileCreationLocator(intent)
	if err != nil {
		intent.Close()
		return ParentProfileCreationChallengeBundle{}, err
	}
	actor, size, deadline, err := v.store.ParentProfileCreationBudget(intent)
	if err != nil {
		intent.Close()
		return ParentProfileCreationChallengeBundle{}, err
	}
	token, err := v.registerParentConfirmation(r, "pcc_", &parentModeEntry{action: parentConfirmationProfileCreation, creation: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return ParentProfileCreationChallengeBundle{}, datastore.ErrParentProfileCreationUnavailable
	}
	return ParentProfileCreationChallengeBundle{ParentIntent: token, CreationOperation: locator}, nil
}
func (v *PGProfilePins) ClaimParentProfileCreation(r *http.Request, body []byte, token string) (*datastore.ParentProfileCreationIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "pcc_")
	if !ok {
		return nil, datastore.ErrParentProfileCreationChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationProfileCreation)
	if entry == nil {
		return nil, datastore.ErrParentProfileCreationChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentProfileCreationChanged
	}
	if err := v.store.BindParentProfileCreationManagementRetry(r, entry.creation, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.creation, nil
}
func (v *PGProfilePins) BoundParentProfileCreationRequest(r *http.Request, i *datastore.ParentProfileCreationIntent) (*http.Request, error) {
	if !v.valid() {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.BoundParentProfileCreationRequest(r, i)
}
func (v *PGProfilePins) VerifyParentProfileCreation(r *http.Request, i *datastore.ParentProfileCreationIntent) error {
	if !v.valid() {
		return datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.VerifyParentProfileCreationPassword(r, i)
}
func (v *PGProfilePins) TakeParentProfileCreationReceipt(i *datastore.ParentProfileCreationIntent, r *datastore.ParentProfileCreationReceipt, u, s *datastore.ParentProfileCreationCacheLease) error {
	if !v.valid() {
		return datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.TakeParentProfileCreationReceipt(i, r, u, s)
}

func (v *PGProfilePins) PrepareProfileCreationRead(r *http.Request, locator string) (*datastore.ProfileCreationRead, error) {
	if !v.valid() {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.PrepareProfileCreationReadManagement(r, locator, RequestTokens(r))
}
func (v *PGProfilePins) ReadProfileCreationResult(r *datastore.ProfileCreationRead) (*datastore.ProfileCreationResultSnapshot, error) {
	if !v.valid() {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.ReadProfileCreationResult(r)
}
func (v *PGProfilePins) TakeProfileCreationRecoveryPair(ctx context.Context, r *datastore.ProfileCreationRead, s *datastore.ProfileCreationResultSnapshot, u, w *datastore.ProfileCreationRecoveryLease) error {
	if !v.valid() {
		return datastore.ErrParentProfileCreationUnavailable
	}
	return v.store.TakeProfileCreationRecoveryPair(ctx, r, s, u, w)
}

func (v *PGProfilePins) ParentRegionalPolicyChallenge(r *http.Request, target string, body []byte) (string, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRegionalPolicy(r, target, body, RequestTokens(r))
	if err != nil {
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(intent)
	if err != nil {
		intent.Close()
		return "", err
	}
	token, err := v.registerParentConfirmation(r, "prf_", &parentModeEntry{action: parentConfirmationRegionalPolicy, rating: intent, actor: actor, bytes: size, deadline: deadline})
	if err != nil {
		return "", datastore.ErrParentRatingUnavailable
	}
	return token, nil
}
func (v *PGProfilePins) ClaimParentRegionalPolicy(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "prf_")
	if !ok {
		return nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationRegionalPolicy)
	if entry == nil {
		return nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) {
		entry.close()
		return nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentRegionalPolicyRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, err
	}
	return entry.rating, nil
}
func (v *PGProfilePins) ImmediateParentRegionalPolicy(r *http.Request, target string, body []byte) (*ParentRatingIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentRatingUnavailable
	}
	intent, err := v.store.PrepareParentRegionalPolicy(r, target, body, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	if err = v.store.BindParentRegionalPolicyRetry(r, intent, target, body, RequestTokens(r)); err != nil {
		intent.Close()
		return nil, err
	}
	return intent, nil
}

// ReadRegionalPolicy uses the original authenticated current PG frame and the
// same owned concrete profile/PIN observation as other protected profile reads.
func (v *PGProfilePins) ReadRegionalPolicy(r *http.Request, target string) (*models.RegionalChildPolicy, error) {
	if !v.valid() || r == nil {
		return nil, datastore.ErrRegionalPolicyUnavailable
	}
	observation, err := v.store.ReadRegionalPolicyRequest(r, target, RequestTokens(r))
	return observation, err
}
