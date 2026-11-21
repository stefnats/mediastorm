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

// This is a negative dispatch result, never permission for another source.
var ErrWatchPartyNotOriginal = errors.New("not an original party request")

func (v *PGProfilePins) SetManagedWatchPartyPlayback(runtime *datastore.WatchRoomAuthority) error {
	if !v.valid() {
		return datastore.ErrWatchRoomUnavailable
	}
	v.parentModeMu.Lock()
	defer v.parentModeMu.Unlock()
	if v.managedParty && v.partyPlayback != runtime {
		return datastore.ErrWatchRoomChanged
	}
	v.managedParty = true
	v.partyPlayback = runtime
	if runtime == nil || !runtime.MatchesStore(v.store) {
		return datastore.ErrWatchRoomUnavailable
	}
	return nil
}
func (v *PGProfilePins) partyRuntime() (*datastore.WatchRoomAuthority, bool, error) {
	if !v.valid() {
		return nil, true, datastore.ErrWatchRoomUnavailable
	}
	v.parentModeMu.Lock()
	runtime, selected := v.partyPlayback, v.managedParty
	v.parentModeMu.Unlock()
	if !selected {
		return nil, false, nil
	}
	if runtime == nil || !runtime.MatchesStore(v.store) {
		return nil, true, datastore.ErrWatchRoomUnavailable
	}
	return runtime, true, nil
}
func partyAuthenticationError(err error) error {
	if errors.Is(err, datastore.ErrWatchRoomUnavailable) || errors.Is(err, datastore.ErrWatchRoomBusy) || errors.Is(err, datastore.ErrWatchRoomUnknown) {
		return errors.Join(datastore.ErrProfilePinAccessUnavailable, err)
	}
	return err
}
func partySourceError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, datastore.ErrWatchRoomUnavailable) || errors.Is(err, datastore.ErrWatchRoomBusy) || errors.Is(err, datastore.ErrWatchRoomUnknown) {
		return errors.Join(datastore.ErrSharePlaybackUnavailable, err)
	}
	return errors.Join(datastore.ErrSharePlaybackChanged, err)
}
func (v *PGProfilePins) WatchPartyPlaybackReady(runtime *datastore.WatchRoomAuthority) bool {
	current, selected, e := v.partyRuntime()
	return e == nil && selected && runtime != nil && current == runtime
}

type WatchPartyPlaybackOwner struct {
	self   *WatchPartyPlaybackOwner
	issuer *PGProfilePins
	core   *datastore.WatchPartyPlaybackOwner
}
type WatchPartyPlaybackSourceLease struct {
	self  *WatchPartyPlaybackSourceLease
	owner *WatchPartyPlaybackOwner
	core  *datastore.WatchPartyPlaybackSourceLease
}

func (WatchPartyPlaybackOwner) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchPartyPlaybackOwner(opaque)"))
}
func (WatchPartyPlaybackOwner) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchPartyPlaybackOwner) UnmarshalJSON([]byte) error  { return datastore.ErrWatchRoomChanged }
func (WatchPartyPlaybackSourceLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WatchPartyPlaybackSourceLease(opaque)"))
}
func (WatchPartyPlaybackSourceLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WatchPartyPlaybackSourceLease) UnmarshalJSON([]byte) error {
	return datastore.ErrWatchRoomChanged
}
func (o *WatchPartyPlaybackOwner) valid() bool {
	return o != nil && o.self == o && o.issuer != nil && o.issuer.valid() && o.core != nil
}
func (v *PGProfilePins) CaptureWatchPartyPlaybackOwner(r *http.Request, profile, resource string) (*WatchPartyPlaybackOwner, error) {
	if !v.valid() {
		return nil, datastore.ErrSharePlaybackUnavailable
	}
	if !v.separateOriginalParty(r) {
		return nil, ErrWatchPartyNotOriginal
	}
	runtime, selected, e := v.partyRuntime()
	if e != nil || !selected {
		if e == nil {
			e = datastore.ErrWatchRoomUnavailable
		}
		return nil, partySourceError(e)
	}
	core, e := runtime.CapturePartyPlaybackOwner(r, profile, resource)
	if e != nil {
		return nil, partySourceError(e)
	}
	owner := &WatchPartyPlaybackOwner{issuer: v, core: core}
	owner.self = owner
	return owner, nil
}
func (o *WatchPartyPlaybackOwner) Context() context.Context {
	if !o.valid() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return o.core.Context()
}
func (o *WatchPartyPlaybackOwner) ExpiresAt() time.Time {
	if !o.valid() {
		return time.Time{}
	}
	return o.core.ExpiresAt()
}
func (o *WatchPartyPlaybackOwner) ProfileID() string {
	if !o.valid() {
		return ""
	}
	return o.core.ProfileID()
}
func (o *WatchPartyPlaybackOwner) Resource() string {
	if !o.valid() {
		return ""
	}
	return o.core.Resource()
}
func (o *WatchPartyPlaybackOwner) Check(ctx context.Context) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	return partySourceError(o.core.Check(ctx))
}
func (o *WatchPartyPlaybackOwner) CheckRequest(r *http.Request, profile, resource string) error {
	if !o.valid() {
		return datastore.ErrSharePlaybackChanged
	}
	return partySourceError(o.core.CheckRequest(r, profile, resource))
}
func (o *WatchPartyPlaybackOwner) BindResourceRequest(r *http.Request) (*http.Request, error) {
	if e := o.CheckRequest(r, o.ProfileID(), o.Resource()); e != nil {
		return nil, e
	}
	return profileaccess.WithWatchPartyPlaybackOwner(r, o.core), nil
}
func (o *WatchPartyPlaybackOwner) Handoff(r *http.Request) (*WatchPartyPlaybackSourceLease, error) {
	if !o.valid() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	core, e := o.core.Handoff(r)
	if e != nil {
		return nil, partySourceError(e)
	}
	lease := &WatchPartyPlaybackSourceLease{owner: o, core: core}
	lease.self = lease
	return lease, nil
}
func (o *WatchPartyPlaybackOwner) Close() {
	if o.valid() {
		o.core.Close()
	}
}
func (l *WatchPartyPlaybackSourceLease) Context() context.Context {
	if l != nil && l.self == l && l.core != nil {
		return l.core.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (l *WatchPartyPlaybackSourceLease) Close() {
	if l != nil && l.self == l && l.core != nil {
		l.core.Close()
	}
}
