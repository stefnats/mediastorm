package profileunlock

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
)

// FamilyScopedApplicability is inactive-only original scoped applicability,
// never a full owner or an adapter capable of opening a media source.
type FamilyScopedApplicability struct {
	self   *FamilyScopedApplicability
	issuer *PGProfilePins
	core   *datastore.FamilyScopedApplicability
}

func (FamilyScopedApplicability) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyScopedApplicability) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyScopedApplicability) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyScopedApplicability(opaque)"))
}
func (o *FamilyScopedApplicability) valid() bool {
	return o != nil && o.self == o && o.issuer != nil && o.issuer.valid() && o.core != nil
}
func (v *PGProfilePins) CaptureInactiveJellyfinApplicability(r *http.Request) (*FamilyScopedApplicability, error) {
	if !v.valid() {
		return nil, ErrFamilyMediaUnavailable
	}
	core, err := v.store.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		return nil, err
	}
	o := &FamilyScopedApplicability{issuer: v, core: core}
	o.self = o
	return o, nil
}
func (o *FamilyScopedApplicability) Context() context.Context {
	if !o.valid() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return o.core.Context()
}
func (o *FamilyScopedApplicability) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.core.ExpiresAt()
}
func (o *FamilyScopedApplicability) Check(ctx context.Context) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	return o.core.Check(ctx)
}
func (o *FamilyScopedApplicability) CheckRequest(r *http.Request) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	return o.core.CheckRequest(r)
}
func (o *FamilyScopedApplicability) Handoff(r *http.Request) error {
	if !o.valid() {
		return ErrFamilyMediaChanged
	}
	return o.core.Handoff(r)
}
func (o *FamilyScopedApplicability) Close() {
	if o.valid() {
		o.core.Close()
	}
}
