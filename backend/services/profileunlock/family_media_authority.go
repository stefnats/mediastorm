package profileunlock

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
)

type FamilyMediaMode = datastore.FamilyMediaMode

const (
	FamilyMediaUnknown     = datastore.FamilyMediaUnknown
	FamilyMediaInactive    = datastore.FamilyMediaInactive
	FamilyMediaActiveAdult = datastore.FamilyMediaActiveAdult
)

var (
	ErrFamilyMediaUnavailable       = datastore.ErrFamilyMediaUnavailable
	ErrFamilyMediaChanged           = datastore.ErrFamilyMediaChanged
	ErrFamilyMediaProfileRequired   = datastore.ErrFamilyMediaProfileRequired
	ErrFamilyMediaPINRequired       = datastore.ErrFamilyMediaPINRequired
	ErrFamilyMediaChildDenied       = datastore.ErrFamilyMediaChildDenied
	ErrFamilyMediaScopedUnsupported = datastore.ErrFamilyMediaScopedUnsupported
	ErrFamilyMediaBusy              = datastore.ErrFamilyMediaBusy
)

type FamilyMediaOwner struct {
	self   *FamilyMediaOwner
	issuer *PGProfilePins
	core   *datastore.FamilyMediaOwner
	target string
}

func (FamilyMediaOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyMediaOwner) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyMediaOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyMediaOwner(opaque)"))
}
func (o *FamilyMediaOwner) valid() bool {
	return o != nil && o.self == o && o.issuer != nil && o.issuer.valid() && o.core != nil
}

// Readiness answers only whether this exact facade owns the expected PG store.
// It exports no store, actor, profile, credential or replaceable request frame.
func (v *PGProfilePins) FamilyMediaReady(expected *datastore.DataStore) bool {
	return v.valid() && expected != nil && expected.Pool() != nil && v.store == expected
}
func (v *PGProfilePins) CaptureFamilyMediaOwner(r *http.Request, target string) (*FamilyMediaOwner, error) {
	if !v.valid() || r == nil || r.URL == nil {
		return nil, ErrFamilyMediaUnavailable
	}
	core, err := v.store.CaptureFamilyMediaOwner(r, target, RequestTokens(r))
	if err != nil {
		return nil, err
	}
	owner := &FamilyMediaOwner{issuer: v, core: core, target: target}
	owner.self = owner
	return owner, nil
}
func (o *FamilyMediaOwner) Check(ctx context.Context) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	return o.core.Check(ctx)
}
func (o *FamilyMediaOwner) CheckRequest(r *http.Request, target string) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	if err := o.core.CheckRequest(r, target); err != nil {
		return err
	}
	if o.core.Mode() == FamilyMediaActiveAdult {
		// A fresh HTTP consumer must present its own current unlock. Possession of
		// a server's original worker locator cannot borrow that worker's PIN input.
		current, err := o.issuer.Current(r, target)
		if err != nil || current.Outcome() != PGPinUnlocked {
			return ErrFamilyMediaPINRequired
		}
		kids, err := current.IsKids()
		if err != nil || kids {
			return ErrFamilyMediaChanged
		}
		return o.core.Check(r.Context())
	}
	return nil
}
func (o *FamilyMediaOwner) Handoff(r *http.Request) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	if err := o.CheckRequest(r, o.target); err != nil {
		return err
	}
	// The target is private and the core requires the identical original frame.
	return o.core.Handoff(r)
}
func (o *FamilyMediaOwner) Context() context.Context {
	if o.valid() {
		return o.core.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (o *FamilyMediaOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.core.ExpiresAt()
}
func (o *FamilyMediaOwner) Mode() FamilyMediaMode {
	if !o.valid() {
		return FamilyMediaUnknown
	}
	return o.core.Mode()
}
func (o *FamilyMediaOwner) Close() {
	if o.valid() {
		o.core.Close()
	}
}
