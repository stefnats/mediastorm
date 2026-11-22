package profileunlock

import (
	"context"
	"fmt"
	"novastream/internal/datastore"
	"time"
)

type FamilyNZBSourceInfo = datastore.FamilyNZBSourceInfo
type FamilyNZBSource struct {
	self  *FamilyNZBSource
	owner *FamilyMediaOwner
	core  *datastore.FamilyNZBSource
}
type FamilyNZBStream struct {
	self   *FamilyNZBStream
	source *FamilyNZBSource
	core   *datastore.FamilyNZBStream
}

func (FamilyNZBSource) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("FamilyNZBSource(opaque)")) }
func (FamilyNZBSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyNZBStream) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("FamilyNZBStream(opaque)")) }
func (FamilyNZBStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyNZBStream) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (s *FamilyNZBSource) valid() bool {
	return s != nil && s.self == s && s.owner.valid() && s.core != nil
}
func (r *FamilyNZBStream) valid() bool {
	return r != nil && r.self == r && r.source.valid() && r.core != nil
}
func (o *FamilyMediaOwner) CaptureNZBSource(ctx context.Context, original *datastore.FamilyNZBSources, name string) (*FamilyNZBSource, error) {
	if !o.valid() {
		return nil, ErrFamilyMediaChanged
	}
	core, err := o.core.CaptureNZBSource(ctx, original, name)
	if err != nil {
		return nil, err
	}
	s := &FamilyNZBSource{owner: o, core: core}
	s.self = s
	return s, nil
}
func (s *FamilyNZBSource) Check(ctx context.Context) error {
	if !s.valid() {
		return ErrFamilyMediaChanged
	}
	return s.core.Check(ctx)
}
func (s *FamilyNZBSource) Context() context.Context {
	if !s.valid() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return s.core.Context()
}
func (s *FamilyNZBSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	return s.core.ExpiresAt()
}
func (s *FamilyNZBSource) ReadyForStore(ds *datastore.DataStore) bool {
	return s.valid() && s.owner.issuer.FamilyMediaReady(ds) && s.core.ReadyForStore(ds)
}
func (s *FamilyNZBSource) Info(ctx context.Context) (FamilyNZBSourceInfo, error) {
	if !s.valid() {
		return FamilyNZBSourceInfo{}, ErrFamilyMediaChanged
	}
	return s.core.Info(ctx)
}
func (s *FamilyNZBSource) Open(ctx context.Context, start, length int64) (*FamilyNZBStream, error) {
	if !s.valid() {
		return nil, ErrFamilyMediaChanged
	}
	core, err := s.core.Open(ctx, start, length)
	if err != nil {
		return nil, err
	}
	r := &FamilyNZBStream{source: s, core: core}
	r.self = r
	return r, nil
}
func (s *FamilyNZBSource) Close() {
	if s.valid() {
		s.core.Close()
	}
}
func (r *FamilyNZBStream) Context() context.Context {
	if !r.valid() {
		return (*FamilyNZBSource)(nil).Context()
	}
	return r.core.Context()
}
func (r *FamilyNZBStream) Check(ctx context.Context) error {
	if !r.valid() {
		return ErrFamilyMediaChanged
	}
	return r.core.Check(ctx)
}
func (r *FamilyNZBStream) Read(p []byte) (int, error) {
	if !r.valid() {
		clear(p)
		return 0, ErrFamilyMediaChanged
	}
	return r.core.Read(p)
}
func (r *FamilyNZBStream) Close() error {
	if !r.valid() {
		return ErrFamilyMediaChanged
	}
	return r.core.Close()
}
func (r *FamilyNZBStream) CleanupDone() <-chan struct{} {
	if !r.valid() {
		return nil
	}
	return r.core.CleanupDone()
}
