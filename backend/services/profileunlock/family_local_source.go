package profileunlock

import (
	"context"
	"fmt"
	"io"
	"time"

	"novastream/internal/datastore"
)

type FamilyLocalSourceInfo = datastore.FamilyLocalSourceInfo
type FamilyLocalFileInfo struct {
	ItemID, LibraryID, Name string
	Size                    int64
}
type FamilyLocalFile interface {
	Check(context.Context) error
	Context() context.Context
	Info() FamilyLocalFileInfo
	OpenReader(context.Context, int64, int64) (io.ReadCloser, error)
	Close() error
}
type FamilyLocalSource struct {
	self  *FamilyLocalSource
	owner *FamilyMediaOwner
	core  *datastore.FamilyLocalSource
}

func (FamilyLocalSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyLocalSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyLocalSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyLocalSource(opaque)"))
}
func (s *FamilyLocalSource) valid() bool {
	return s != nil && s.self == s && s.owner.valid() && s.core != nil
}
func (o *FamilyMediaOwner) CaptureLocalSource(ctx context.Context, item string) (*FamilyLocalSource, error) {
	if !o.valid() {
		return nil, ErrFamilyMediaChanged
	}
	core, err := o.core.CaptureLocalSource(ctx, item)
	if err != nil {
		return nil, err
	}
	s := &FamilyLocalSource{owner: o, core: core}
	s.self = s
	return s, nil
}
func (s *FamilyLocalSource) Check(ctx context.Context) error {
	if !s.valid() {
		return ErrFamilyMediaChanged
	}
	return s.core.Check(ctx)
}
func (s *FamilyLocalSource) Context() context.Context {
	if s.valid() {
		return s.core.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (s *FamilyLocalSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	return s.core.ExpiresAt()
}
func (s *FamilyLocalSource) Info(ctx context.Context) (FamilyLocalSourceInfo, error) {
	if !s.valid() {
		return FamilyLocalSourceInfo{}, ErrFamilyMediaChanged
	}
	return s.core.Info(ctx)
}
func (s *FamilyLocalSource) ReadyForStore(expected *datastore.DataStore) bool {
	return s.valid() && s.owner.issuer.FamilyMediaReady(expected) && s.core.ReadyForStore(expected)
}
func (s *FamilyLocalSource) BeginOpen(ctx context.Context) (func(), error) {
	if !s.valid() {
		return nil, ErrFamilyMediaChanged
	}
	return s.core.BeginOpen(ctx)
}
func (s *FamilyLocalSource) BindRetirement(retire func(), done <-chan struct{}) error {
	if !s.valid() {
		return ErrFamilyMediaChanged
	}
	return s.core.BindRetirement(retire, done)
}
func (s *FamilyLocalSource) Close() {
	if s.valid() {
		s.core.Close()
	}
}
func (s *FamilyLocalSource) Invalidate() {
	if s.valid() {
		s.core.Invalidate()
	}
}
