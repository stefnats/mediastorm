package profileunlock

import (
	"net/http"

	"novastream/config"
)

// VerifyPasswordWorkflow returns only existing opaque credential evidence.
// The caller still owns the same request and must separately Commit/Release.
func (i *BridgeLoginSessionIssuer) VerifyPasswordWorkflow(r *http.Request, account, profile, client, raw string, original *config.BridgeLoginPolicyObservation) (*BridgeLoginCredentialProof, error) {
	if !i.valid() {
		return nil, ErrBridgeSessionUnavailable
	}
	core, err := i.core.VerifyPasswordWorkflow(r, account, profile, client, raw, original)
	if err != nil {
		return nil, err
	}
	if !i.valid() {
		core.Close()
		return nil, ErrBridgeSessionChanged
	}
	proof := &BridgeLoginCredentialProof{issuer: i.pins, core: core}
	proof.self = proof
	return proof, nil
}
