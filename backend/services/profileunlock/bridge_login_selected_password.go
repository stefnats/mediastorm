package profileunlock

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
)

type BridgeSelectedPasswordProof struct {
	self   *BridgeSelectedPasswordProof
	issuer *BridgeLoginSessionIssuer
	core   *datastore.BridgeSelectedPasswordProof
}

func (p *BridgeSelectedPasswordProof) valid() bool {
	return p != nil && p.self == p && p.issuer != nil && p.core != nil
}
func (BridgeSelectedPasswordProof) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeSelectedPasswordProof(opaque)"))
}
func (BridgeSelectedPasswordProof) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeSelectedPasswordProof) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
func (p *BridgeSelectedPasswordProof) Context() context.Context {
	if !p.valid() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return p.core.Context()
}
func (p *BridgeSelectedPasswordProof) ExpiresAt() time.Time {
	if !p.valid() {
		return time.Time{}
	}
	return p.core.ExpiresAt()
}
func (p *BridgeSelectedPasswordProof) Close() {
	if p.valid() {
		p.core.Close()
	}
}
func (i *BridgeLoginSessionIssuer) VerifySelectedPassword(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation) (*BridgeSelectedPasswordProof, error) {
	return i.verifySelectedPassword(r, username, client, raw, original, nil, false)
}

// Candidate selectors constrain eligibility only; the core still captures and
// checks every genuine original candidate before minting can be attempted.
func (i *BridgeLoginSessionIssuer) VerifySelectedPasswordForCandidates(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation, expected []BridgeLoginCandidate) (*BridgeSelectedPasswordProof, error) {
	if len(expected) == 0 || len(expected) > 3 {
		return nil, ErrBridgeCredentialChanged
	}
	return i.verifySelectedPassword(r, username, client, raw, original, expected, true)
}

func (i *BridgeLoginSessionIssuer) verifySelectedPassword(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation, expected []BridgeLoginCandidate, constrained bool) (*BridgeSelectedPasswordProof, error) {
	if !i.valid() {
		return nil, ErrBridgeSessionUnavailable
	}
	var core *datastore.BridgeSelectedPasswordProof
	var err error
	if constrained {
		selectors := make([]datastore.BridgeLoginCandidate, len(expected))
		for n, candidate := range expected {
			selectors[n] = datastore.BridgeLoginCandidate{AccountID: candidate.AccountID, ProfileID: candidate.ProfileID}
		}
		core, err = i.core.VerifySelectedPasswordForCandidates(r, username, client, raw, original, selectors)
	} else {
		core, err = i.core.VerifySelectedPassword(r, username, client, raw, original)
	}
	if err != nil {
		return nil, err
	}
	if !i.valid() {
		core.Close()
		return nil, ErrBridgeSessionChanged
	}
	p := &BridgeSelectedPasswordProof{issuer: i, core: core}
	p.self = p
	return p, nil
}
func (i *BridgeLoginSessionIssuer) CommitSelected(r *http.Request, proof *BridgeSelectedPasswordProof, original *config.BridgeLoginPolicyObservation) (*BridgeLoginSessionReceipt, error) {
	if !i.valid() || !proof.valid() || proof.issuer != i {
		return nil, ErrBridgeSessionChanged
	}
	core, err := i.core.CommitSelected(r, proof.core, original)
	if err != nil {
		return nil, err
	}
	receipt := &BridgeLoginSessionReceipt{issuer: i, core: core}
	receipt.self = receipt
	return receipt, nil
}
