package profileunlock

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
	"novastream/internal/profileaccess"
)

var ErrSharePlaybackParty = datastore.ErrSharePlaybackParty

// SetManagedSharePlayback selects the concrete original source even if it is
// unavailable. Missing/closed/foreign dependencies cannot select legacy mint.
func (v *PGProfilePins) SetManagedSharePlayback(runtime *datastore.ShareLinkRedeemer) error {
	if !v.valid() {
		return datastore.ErrSharePlaybackUnavailable
	}
	v.parentModeMu.Lock()
	defer v.parentModeMu.Unlock()
	if v.managedShare && v.sharePlayback != runtime {
		return datastore.ErrSharePlaybackChanged
	}
	v.managedShare = true
	v.sharePlayback = runtime
	if runtime == nil || !runtime.MatchesStore(v.store) {
		return datastore.ErrSharePlaybackUnavailable
	}
	return nil
}
func (v *PGProfilePins) shareRuntime() (*datastore.ShareLinkRedeemer, bool, error) {
	if !v.valid() {
		return nil, true, datastore.ErrSharePlaybackUnavailable
	}
	v.parentModeMu.Lock()
	runtime, selected := v.sharePlayback, v.managedShare
	v.parentModeMu.Unlock()
	if !selected {
		return nil, false, nil
	}
	if runtime == nil || !runtime.MatchesStore(v.store) {
		return nil, true, datastore.ErrSharePlaybackUnavailable
	}
	return runtime, true, nil
}

type resolvedSharePartyKey struct{}

func (v *PGProfilePins) separateOriginalParty(r *http.Request) bool {
	if !v.valid() || r == nil {
		return false
	}
	producer, _ := r.Context().Value(resolvedSharePartyKey{}).(*PGProfilePins)
	return producer == v && v.store.CheckProfilePinRequestFrame(r) == nil
}
func (v *PGProfilePins) validateManagedShare(r *http.Request) error {
	if v.separateOriginalParty(r) {
		return nil
	}
	runtime, selected, err := v.shareRuntime()
	if err != nil || !selected {
		return shareAuthenticationError(err)
	}
	err = runtime.ValidateSharePlaybackRequest(r)
	if errors.Is(err, datastore.ErrSharePlaybackParty) {
		return nil
	}
	return shareAuthenticationError(err)
}
func shareAuthenticationError(err error) error {
	if errors.Is(err, datastore.ErrSharePlaybackUnavailable) || errors.Is(err, datastore.ErrSharePlaybackBusy) || errors.Is(err, datastore.ErrSharePlaybackUnknown) {
		return errors.Join(datastore.ErrProfilePinAccessUnavailable, err)
	}
	return err
}

// SharePlaybackOwner retains a genuine original scoped core. It cannot become
// a full FamilyMediaOwner or a PIN/master/parent permission by copying DATA.
type SharePlaybackOwner struct {
	self   *SharePlaybackOwner
	issuer *PGProfilePins
	core   *datastore.SharePlaybackOwner
}
type SharePlaybackSourceLease struct {
	self  *SharePlaybackSourceLease
	owner *SharePlaybackOwner
	core  *datastore.SharePlaybackSourceLease
}

func (SharePlaybackOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SharePlaybackOwner(opaque)"))
}
func (SharePlaybackOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SharePlaybackOwner) UnmarshalJSON([]byte) error  { return datastore.ErrSharePlaybackChanged }
func (SharePlaybackSourceLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SharePlaybackSourceLease(opaque)"))
}
func (SharePlaybackSourceLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SharePlaybackSourceLease) UnmarshalJSON([]byte) error {
	return datastore.ErrSharePlaybackChanged
}
func (o *SharePlaybackOwner) valid() bool {
	return o != nil && o.self == o && o.issuer != nil && o.issuer.valid() && o.core != nil
}
func (v *PGProfilePins) CaptureSharePlaybackOwner(r *http.Request, profile, resource string) (*SharePlaybackOwner, error) {
	if v.separateOriginalParty(r) {
		return nil, datastore.ErrSharePlaybackParty
	}
	runtime, selected, err := v.shareRuntime()
	if err != nil {
		return nil, err
	}
	if !selected {
		return nil, datastore.ErrSharePlaybackUnavailable
	}
	core, err := runtime.CaptureSharePlaybackOwner(r, profile, resource)
	if err != nil {
		return nil, err
	}
	o := &SharePlaybackOwner{issuer: v, core: core}
	o.self = o
	return o, nil
}
func (o *SharePlaybackOwner) Context() context.Context {
	if !o.valid() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return o.core.Context()
}
func (o *SharePlaybackOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.core.ExpiresAt()
}
func (o *SharePlaybackOwner) ProfileID() string {
	if !o.valid() {
		return ""
	}
	return o.core.ProfileID()
}
func (o *SharePlaybackOwner) Resource() string {
	if !o.valid() {
		return ""
	}
	return o.core.Resource()
}
func (o *SharePlaybackOwner) Check(ctx context.Context) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	return o.core.Check(ctx)
}
func (o *SharePlaybackOwner) CheckRequest(r *http.Request, profile, resource string) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	return o.core.CheckRequest(r, profile, resource)
}
func (o *SharePlaybackOwner) BindResourceRequest(r *http.Request) (*http.Request, error) {
	if e := o.CheckRequest(r, o.ProfileID(), o.Resource()); e != nil {
		return nil, e
	}
	return profileaccess.WithSharePlaybackOwner(r, o.core), nil
}
func (o *SharePlaybackOwner) Handoff(r *http.Request) (*SharePlaybackSourceLease, error) {
	if !o.valid() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	core, err := o.core.Handoff(r)
	if err != nil {
		return nil, err
	}
	lease := &SharePlaybackSourceLease{owner: o, core: core}
	lease.self = lease
	return lease, nil
}
func (o *SharePlaybackOwner) Close() {
	if o.valid() {
		o.core.Close()
	}
}
func (l *SharePlaybackSourceLease) Context() context.Context {
	if l != nil && l.self == l && l.core != nil {
		return l.core.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (l *SharePlaybackSourceLease) Close() {
	if l != nil && l.self == l && l.core != nil {
		l.core.Close()
	}
}

// SharePlaybackReady is composition readiness only, never source permission.
func (v *PGProfilePins) SharePlaybackReady(runtime *datastore.ShareLinkRedeemer) bool {
	current, selected, err := v.shareRuntime()
	return err == nil && selected && current == runtime && runtime != nil
}
