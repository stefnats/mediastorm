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
