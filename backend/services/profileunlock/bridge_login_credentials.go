package profileunlock

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
)

var (
	ErrBridgeCredentialUnavailable = datastore.ErrBridgeCredentialUnavailable
	ErrBridgeCredentialChanged     = datastore.ErrBridgeCredentialChanged
	ErrBridgeCredentialRejected    = datastore.ErrBridgeCredentialRejected
	ErrBridgeCredentialBusy        = datastore.ErrBridgeCredentialBusy
	ErrBridgeCredentialUsed        = datastore.ErrBridgeCredentialUsed
)

// This facade returns credential evidence only, never a configured login,
// session, token, source, or QuickConnect delegation permission.
type BridgeLoginCredentialIntent struct {
	self   *BridgeLoginCredentialIntent
	issuer *PGProfilePins
	core   *datastore.BridgeLoginCredentialIntent
}
type BridgeLoginCredentialProof struct {
	self   *BridgeLoginCredentialProof
	issuer *PGProfilePins
	core   *datastore.BridgeLoginCredentialProof
}

func (BridgeLoginCredentialIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginCredentialIntent) UnmarshalJSON([]byte) error  { return ErrBridgeCredentialChanged }
func (BridgeLoginCredentialIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginCredentialIntent(opaque)"))
}
func (BridgeLoginCredentialProof) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginCredentialProof) UnmarshalJSON([]byte) error  { return ErrBridgeCredentialChanged }
func (BridgeLoginCredentialProof) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginCredentialProof(opaque)"))
}
func (i *BridgeLoginCredentialIntent) valid() bool {
	return i != nil && i.self == i && i.issuer != nil && i.issuer.valid() && i.core != nil
}
func (p *BridgeLoginCredentialProof) valid() bool {
	return p != nil && p.self == p && p.issuer != nil && p.issuer.valid() && p.core != nil
}
func (v *PGProfilePins) PrepareBridgeLoginCredentials(r *http.Request, account, profile, client string) (*BridgeLoginCredentialIntent, error) {
	if !v.valid() {
		return nil, ErrBridgeCredentialUnavailable
	}
	core, err := v.store.PrepareBridgeLoginCredentials(r, account, profile, client)
	if err != nil {
		return nil, err
	}
	i := &BridgeLoginCredentialIntent{issuer: v, core: core}
	i.self = i
	return i, nil
}
func (v *PGProfilePins) VerifyBridgeAccountPassword(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	if !v.valid() || !i.valid() || i.issuer != v {
		return nil, ErrBridgeCredentialChanged
	}
	core, err := v.store.VerifyBridgeAccountPassword(r, i.core, raw)
	if err != nil {
		return nil, err
	}
	p := &BridgeLoginCredentialProof{issuer: v, core: core}
	p.self = p
	return p, nil
}
func (v *PGProfilePins) VerifyBridgePasswordWithPIN(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	if !v.valid() || !i.valid() || i.issuer != v {
		return nil, ErrBridgeCredentialChanged
	}
	core, err := v.store.VerifyBridgePasswordWithPIN(r, i.core, raw)
	if err != nil {
		return nil, err
	}
	p := &BridgeLoginCredentialProof{issuer: v, core: core}
	p.self = p
	return p, nil
}
func (v *PGProfilePins) VerifyBridgeProfilePIN(r *http.Request, i *BridgeLoginCredentialIntent, raw string) (*BridgeLoginCredentialProof, error) {
	if !v.valid() || !i.valid() || i.issuer != v {
		return nil, ErrBridgeCredentialChanged
	}
	core, err := v.store.VerifyBridgeProfilePIN(r, i.core, raw)
	if err != nil {
		return nil, err
	}
	p := &BridgeLoginCredentialProof{issuer: v, core: core}
	p.self = p
	return p, nil
}
func bridgeCredentialClosedContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (i *BridgeLoginCredentialIntent) Context() context.Context {
	if !i.valid() {
		return bridgeCredentialClosedContext()
	}
	return i.core.Context()
}
func (p *BridgeLoginCredentialProof) Context() context.Context {
	if !p.valid() {
		return bridgeCredentialClosedContext()
	}
	return p.core.Context()
}
func (i *BridgeLoginCredentialIntent) ExpiresAt() time.Time {
	if !i.valid() {
		return time.Time{}
	}
	return i.core.ExpiresAt()
}
func (p *BridgeLoginCredentialProof) ExpiresAt() time.Time {
	if !p.valid() {
		return time.Time{}
	}
	return p.core.ExpiresAt()
}
func (i *BridgeLoginCredentialIntent) Check(ctx context.Context) error {
	if !i.valid() {
		return ErrBridgeCredentialChanged
	}
	return i.core.Check(ctx)
}
func (p *BridgeLoginCredentialProof) Check(ctx context.Context) error {
	if !p.valid() {
		return ErrBridgeCredentialChanged
	}
	return p.core.Check(ctx)
}
func (i *BridgeLoginCredentialIntent) Close() {
	if i.valid() {
		i.core.Close()
	}
}
func (p *BridgeLoginCredentialProof) Close() {
	if p.valid() {
		p.core.Close()
	}
}
