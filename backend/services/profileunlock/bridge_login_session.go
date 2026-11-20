package profileunlock

import (
	"fmt"
	"net/http"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrBridgeSessionUnavailable = datastore.ErrBridgeSessionUnavailable
	ErrBridgeSessionChanged     = datastore.ErrBridgeSessionChanged
	ErrBridgeSessionUsed        = datastore.ErrBridgeSessionUsed
	ErrBridgeSessionUnknown     = datastore.ErrBridgeSessionUnknown
)

type BridgeLoginSessionIssuer struct {
	self *BridgeLoginSessionIssuer
	pins *PGProfilePins
	core *datastore.BridgeLoginSessionIssuer
}
type BridgeLoginSessionReceipt struct {
	self   *BridgeLoginSessionReceipt
	issuer *BridgeLoginSessionIssuer
	core   *datastore.BridgeLoginSessionReceipt
}

func (v *PGProfilePins) NewBridgeLoginSessionIssuer(manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*BridgeLoginSessionIssuer, error) {
	if !v.valid() {
		return nil, ErrBridgeSessionUnavailable
	}
	core, err := datastore.NewBridgeLoginSessionIssuer(v.store, manager, security, coordinator)
	if err != nil {
		return nil, err
	}
	value := &BridgeLoginSessionIssuer{pins: v, core: core}
	value.self = value
	return value, nil
}
func (i *BridgeLoginSessionIssuer) valid() bool {
	return i != nil && i.self == i && i.pins != nil && i.pins.valid() && i.core != nil
}
func (i *BridgeLoginSessionIssuer) Commit(request *http.Request, proof *BridgeLoginCredentialProof, observation *config.BridgeLoginPolicyObservation) (*BridgeLoginSessionReceipt, error) {
	if !i.valid() || !proof.valid() || proof.issuer != i.pins {
		return nil, ErrBridgeSessionChanged
	}
	core, err := i.core.Commit(request, proof.core, observation)
	if err != nil {
		return nil, err
	}
	value := &BridgeLoginSessionReceipt{issuer: i, core: core}
	value.self = value
	return value, nil
}
func (i *BridgeLoginSessionIssuer) Release(request *http.Request, receipt *BridgeLoginSessionReceipt) (models.Session, error) {
	if !i.valid() || !receipt.valid() || receipt.issuer != i {
		return models.Session{}, ErrBridgeSessionChanged
	}
	return i.core.Release(request, receipt.core)
}

// Cleanup remains possible after issuer dependencies stop being ready.
func (r *BridgeLoginSessionReceipt) valid() bool {
	return r != nil && r.self == r && r.issuer != nil && r.core != nil
}
func (r *BridgeLoginSessionReceipt) Close() {
	if r.valid() {
		r.core.Close()
	}
}
func (BridgeLoginSessionIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginSessionIssuer(opaque)"))
}
func (BridgeLoginSessionIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginSessionIssuer) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
func (BridgeLoginSessionReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginSessionReceipt(opaque)"))
}
func (BridgeLoginSessionReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginSessionReceipt) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
