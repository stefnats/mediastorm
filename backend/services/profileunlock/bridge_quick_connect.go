package profileunlock

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrBridgeQuickConnectUnavailable = datastore.ErrBridgeQuickConnectUnavailable
	ErrBridgeQuickConnectChanged     = datastore.ErrBridgeQuickConnectChanged
	ErrBridgeQuickConnectUnsupported = datastore.ErrBridgeQuickConnectUnsupported
	ErrBridgeQuickConnectBusy        = datastore.ErrBridgeQuickConnectBusy
	ErrBridgeQuickConnectUsed        = datastore.ErrBridgeQuickConnectUsed
	ErrBridgeQuickConnectUnknown     = datastore.ErrBridgeQuickConnectUnknown
)

type BridgeQuickConnectIssuer struct {
	self *BridgeQuickConnectIssuer
	pins *PGProfilePins
	core *datastore.BridgeQuickConnectIssuer
}
type BridgeQuickConnectChallenge struct {
	self   *BridgeQuickConnectChallenge
	issuer *BridgeQuickConnectIssuer
	core   *datastore.BridgeQuickConnectChallenge
}
type BridgeQuickConnectApproval struct {
	self      *BridgeQuickConnectApproval
	issuer    *BridgeQuickConnectIssuer
	challenge *BridgeQuickConnectChallenge
	core      *datastore.BridgeQuickConnectApproval
}
type BridgeQuickConnectReceipt struct {
	self   *BridgeQuickConnectReceipt
	issuer *BridgeQuickConnectIssuer
	core   *datastore.BridgeQuickConnectReceipt
}
type BridgeQuickConnectDetails = datastore.BridgeQuickConnectDetails

func (BridgeQuickConnectIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectIssuer(opaque)"))
}
func (BridgeQuickConnectIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectIssuer) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectChallenge) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectChallenge(opaque)"))
}
func (BridgeQuickConnectChallenge) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectChallenge) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectApproval) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectApproval(opaque)"))
}
func (BridgeQuickConnectApproval) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectApproval) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectReceipt(opaque)"))
}
func (BridgeQuickConnectReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectReceipt) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }

func (p *PGProfilePins) NewBridgeQuickConnectIssuer(lifetime context.Context, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*BridgeQuickConnectIssuer, error) {
	if !p.valid() {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	core, err := datastore.NewBridgeQuickConnectIssuer(lifetime, p.store, manager, security, coordinator)
	if err != nil {
		return nil, err
	}
	i := &BridgeQuickConnectIssuer{pins: p, core: core}
	i.self = i
	return i, nil
}
func (i *BridgeQuickConnectIssuer) valid() bool {
	return i != nil && i.self == i && i.pins != nil && i.pins.valid() && i.core != nil
}
func (c *BridgeQuickConnectChallenge) valid(i *BridgeQuickConnectIssuer) bool {
	return c != nil && c.self == c && c.issuer == i && c.core != nil
}
func (i *BridgeQuickConnectIssuer) Initiate(r *http.Request, client string) (*BridgeQuickConnectChallenge, error) {
	if !i.valid() {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	core, err := i.core.Initiate(r, client)
	if err != nil {
		return nil, err
	}
	c := &BridgeQuickConnectChallenge{issuer: i, core: core}
	c.self = c
	return c, nil
}

// The actual PG authentication frame is established before any core approval
// capture. The core mints its distinct durable handoff while that frame is live;
// returning from this method then retires the original request frame normally.
func (i *BridgeQuickConnectIssuer) Approve(r *http.Request, token, client string, c *BridgeQuickConnectChallenge) (*BridgeQuickConnectApproval, error) {
	if !i.valid() || !c.valid(i) || r == nil || client == "" || len(client) > 256 || client != strings.TrimSpace(client) || !utf8.ValidString(client) {
		return nil, ErrBridgeQuickConnectChanged
	}
	// Canonical client input is descriptive and set before authentication, never
	// by cloning/replacing a frame after the original PG capture.
	input := r.Clone(r.Context())
	input.Header.Set("X-Client-ID", client)
	checked, release, err := i.pins.Authenticate(input, token, client)
	if err != nil {
		return nil, err
	}
	defer release()
	session, _, err := i.pins.Identity(checked)
	if err != nil || session.Scope != models.SessionScopeJellyfin || session.ClientID != client {
		return nil, ErrBridgeQuickConnectUnsupported
	}
	return i.ApproveAuthenticated(checked, c)
}

// ApproveAuthenticated consumes this exact already-genuine PG request frame.
// A transport may inspect its Identity DATA only to deny a selector or charge
// its limiter before this call; no frame clone or recapture occurs here.
func (i *BridgeQuickConnectIssuer) ApproveAuthenticated(r *http.Request, c *BridgeQuickConnectChallenge) (*BridgeQuickConnectApproval, error) {
	if !i.valid() || !c.valid(i) || r == nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	core, err := i.core.Approve(r, c.core)
	if err != nil {
		return nil, err
	}
	a := &BridgeQuickConnectApproval{issuer: i, challenge: c, core: core}
	a.self = a
	return a, nil
}
func (i *BridgeQuickConnectIssuer) Commit(r *http.Request, c *BridgeQuickConnectChallenge, a *BridgeQuickConnectApproval, secret, client string) (*BridgeQuickConnectReceipt, error) {
	if !i.valid() || !c.valid(i) || a == nil || a.self != a || a.issuer != i || a.challenge != c || a.core == nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	core, err := i.core.Commit(r, c.core, a.core, secret, client)
	if err != nil {
		return nil, err
	}
	receipt := &BridgeQuickConnectReceipt{issuer: i, core: core}
	receipt.self = receipt
	return receipt, nil
}
func (i *BridgeQuickConnectIssuer) Release(r *http.Request, receipt *BridgeQuickConnectReceipt) (models.Session, error) {
	if !i.valid() || receipt == nil || receipt.self != receipt || receipt.issuer != i || receipt.core == nil {
		return models.Session{}, ErrBridgeQuickConnectChanged
	}
	return i.core.Release(r, receipt.core)
}
func (c *BridgeQuickConnectChallenge) Details() (BridgeQuickConnectDetails, error) {
	if c == nil || c.self != c || c.core == nil {
		return BridgeQuickConnectDetails{}, ErrBridgeQuickConnectChanged
	}
	return c.core.Details()
}
func (c *BridgeQuickConnectChallenge) Close() {
	if c != nil && c.self == c && c.core != nil {
		c.core.Close()
	}
}
func (a *BridgeQuickConnectApproval) Close() {
	if a != nil && a.self == a && a.core != nil {
		a.core.Close()
	}
}
func (r *BridgeQuickConnectReceipt) Close() {
	if r != nil && r.self == r && r.core != nil {
		r.core.Close()
	}
}
func (r *BridgeQuickConnectReceipt) ExpiresAt() time.Time {
	if r == nil || r.self != r || r.core == nil {
		return time.Time{}
	}
	return r.core.ExpiresAt()
}
func (i *BridgeQuickConnectIssuer) Close() {
	if i != nil && i.self == i && i.core != nil {
		i.core.Close()
	}
}
