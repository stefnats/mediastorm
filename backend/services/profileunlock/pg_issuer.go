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
type parentModeEntry struct {
	intent   *ParentModeIntent
	actor    string
	bytes    int
	deadline time.Time
	timer    *time.Timer
}

func parentModeTokenKey(token string) (string, bool) {
	if len(token) != 68 || !strings.HasPrefix(token, "pmi_") {
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
	var random [32]byte
	if _, err = rand.Read(random[:]); err != nil {
		intent.Close()
		return "", datastore.ErrParentModeUnavailable
	}
	token := "pmi_" + hex.EncodeToString(random[:])
	key, _ := parentModeTokenKey(token)
	v.parentModeMu.Lock()
	defer v.parentModeMu.Unlock()
	if v.parentModes == nil {
		v.parentModes = map[string]*parentModeEntry{}
	}
	count := 0
	for existingKey, entry := range v.parentModes {
		if !entry.deadline.After(time.Now()) {
			delete(v.parentModes, existingKey)
			v.parentModeBytes -= entry.bytes
			entry.timer.Stop()
			entry.intent.Close()
			continue
		}
		if entry.actor == actor {
			count++
		}
	}
	if len(v.parentModes) >= parentModeMaximumEntries || count >= parentModeMaximumActorEntries || size > parentModeMaximumBytes-v.parentModeBytes || v.parentModes[key] != nil || r.Context().Err() != nil || !deadline.After(time.Now()) {
		intent.Close()
		return "", datastore.ErrParentModeUnavailable
	}
	entry := &parentModeEntry{intent: intent, actor: actor, bytes: size, deadline: deadline}
	v.parentModes[key] = entry
	v.parentModeBytes += size
	entry.timer = time.AfterFunc(time.Until(deadline), func() {
		v.parentModeMu.Lock()
		defer v.parentModeMu.Unlock()
		if v.parentModes[key] == entry {
			delete(v.parentModes, key)
			v.parentModeBytes -= entry.bytes
			entry.intent.Close()
		}
	})
	return token, nil
}
func (v *PGProfilePins) ClaimParentMode(r *http.Request, target, mode, token string) (*ParentModeIntent, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, datastore.ErrParentModeUnavailable
	}
	key, valid := parentModeTokenKey(token)
	if !valid {
		return nil, datastore.ErrParentModeChanged
	}
	v.parentModeMu.Lock()
	entry := v.parentModes[key]
	if entry != nil {
		delete(v.parentModes, key)
		v.parentModeBytes -= entry.bytes
		entry.timer.Stop()
	}
	v.parentModeMu.Unlock()
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
