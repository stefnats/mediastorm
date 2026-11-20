package profileunlock

import (
	"context"
	"fmt"
	"time"

	"novastream/internal/datastore"
)

type FamilyJellyfinSourceInfo = datastore.FamilyJellyfinSourceInfo

type FamilyJellyfinSource struct {
	self  *FamilyJellyfinSource
	owner *FamilyMediaOwner
	core  *datastore.FamilyJellyfinSource
}

func (FamilyJellyfinSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyJellyfinSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyJellyfinSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinSource(opaque)"))
}
func (s *FamilyJellyfinSource) valid() bool {
	return s != nil && s.self == s && s.owner.valid() && s.core != nil
}
func (o *FamilyMediaOwner) CaptureJellyfinSource(ctx context.Context, item string) (*FamilyJellyfinSource, error) {
	if !o.valid() {
		return nil, ErrFamilyMediaChanged
	}
	core, err := o.core.CaptureJellyfinSource(ctx, item)
	if err != nil {
		return nil, err
	}
	source := &FamilyJellyfinSource{owner: o, core: core}
	source.self = source
	return source, nil
}
func (s *FamilyJellyfinSource) Check(ctx context.Context) error {
	if !s.valid() {
		return ErrFamilyMediaChanged
	}
	return s.core.Check(ctx)
}
func (s *FamilyJellyfinSource) Context() context.Context {
	if s.valid() {
		return s.core.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (s *FamilyJellyfinSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	return s.core.ExpiresAt()
}
func (s *FamilyJellyfinSource) Info(ctx context.Context) (FamilyJellyfinSourceInfo, error) {
	if !s.valid() {
		return FamilyJellyfinSourceInfo{}, ErrFamilyMediaChanged
	}
	return s.core.Info(ctx)
}
func (s *FamilyJellyfinSource) ReadyForStore(expected *datastore.DataStore) bool {
	return s.valid() && s.owner.issuer.FamilyMediaReady(expected) && s.core.ReadyForStore(expected)
}
func (s *FamilyJellyfinSource) BeginOpen(ctx context.Context) (func(), error) {
	if !s.valid() {
		return nil, ErrFamilyMediaChanged
	}
	return s.core.BeginOpen(ctx)
}
func (s *FamilyJellyfinSource) BindRetirement(retire func(), done <-chan struct{}) error {
	if !s.valid() {
		return ErrFamilyMediaChanged
	}
	return s.core.BindRetirement(retire, done)
}
func (s *FamilyJellyfinSource) Invalidate() {
	if s.valid() {
		s.core.Invalidate()
	}
}
func (s *FamilyJellyfinSource) Close() {
	if s.valid() {
		s.core.Close()
	}
}
