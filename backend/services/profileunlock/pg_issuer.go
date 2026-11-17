package profileunlock

import (
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
)

// PGPinIssuer is an uninstalled facade over the generation-bound datastore
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

// PGPinAccess is a separate, uninstalled typed access adapter. It observes the
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
