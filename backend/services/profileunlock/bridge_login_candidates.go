package profileunlock

import (
	"net/http"
	"novastream/config"
	"novastream/internal/datastore"
)

var (
	ErrBridgeLoginCandidatesUnavailable = datastore.ErrBridgeLoginCandidatesUnavailable
	ErrBridgeLoginCandidatesChanged     = datastore.ErrBridgeLoginCandidatesChanged
	ErrBridgeLoginCandidatesBusy        = datastore.ErrBridgeLoginCandidatesBusy
	ErrBridgeLoginCandidatesAmbiguous   = datastore.ErrBridgeLoginCandidatesAmbiguous
)

// BridgeLoginCandidate contains selectors DATA, not credential evidence.
type BridgeLoginCandidate struct{ AccountID, ProfileID string }

func (i *BridgeLoginSessionIssuer) ResolvePasswordCandidates(r *http.Request, username, client string, original *config.BridgeLoginPolicyObservation) ([]BridgeLoginCandidate, error) {
	if !i.valid() {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	current, err := i.core.ResolvePasswordCandidates(r, username, client, original)
	if err != nil {
		return nil, err
	}
	if !i.valid() {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	result := make([]BridgeLoginCandidate, len(current))
	for n, value := range current {
		result[n] = BridgeLoginCandidate{AccountID: value.AccountID, ProfileID: value.ProfileID}
	}
	return result, nil
}
