//go:build !linux

package nzbfilesystem

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrOriginalMetadataUnavailable = errors.New("original metadata source unavailable")

type OriginalMetadataInfo struct{ Size int64 }
type OriginalMetadataNamespace struct{}
type OriginalMetadataSource struct{}
type OriginalMetadataStream struct{}

func (*NzbFilesystem) CaptureOriginalMetadataNamespace(context.Context) (*OriginalMetadataNamespace, error) {
	return nil, ErrOriginalMetadataUnavailable
}
func (*OriginalMetadataNamespace) MatchesOriginal(context.Context, *NzbFilesystem, string) error {
	return ErrOriginalMetadataUnavailable
}
func (*OriginalMetadataNamespace) Capture(context.Context, string) (*OriginalMetadataSource, error) {
	return nil, ErrOriginalMetadataUnavailable
}
func (*OriginalMetadataNamespace) SignalClose()             {}
func (*OriginalMetadataNamespace) CloseAndJoin() error      { return ErrOriginalMetadataUnavailable }
func (*OriginalMetadataSource) Check(context.Context) error { return ErrOriginalMetadataUnavailable }
func (*OriginalMetadataSource) Context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (*OriginalMetadataSource) ExpiresAt() time.Time { return time.Time{} }
func (*OriginalMetadataSource) Info(context.Context) (OriginalMetadataInfo, error) {
	return OriginalMetadataInfo{}, ErrOriginalMetadataUnavailable
}
func (*OriginalMetadataSource) Open(context.Context, int64, int64) (*OriginalMetadataStream, error) {
	return nil, ErrOriginalMetadataUnavailable
}
func (*OriginalMetadataSource) SignalClose()             {}
func (*OriginalMetadataSource) CloseAndJoin() error      { return ErrOriginalMetadataUnavailable }
func (*OriginalMetadataStream) Read([]byte) (int, error) { return 0, ErrOriginalMetadataUnavailable }
func (*OriginalMetadataStream) Context() context.Context {
	return (*OriginalMetadataSource)(nil).Context()
}
func (*OriginalMetadataStream) Check(context.Context) error  { return ErrOriginalMetadataUnavailable }
func (*OriginalMetadataStream) Close() error                 { return ErrOriginalMetadataUnavailable }
func (*OriginalMetadataStream) CleanupDone() <-chan struct{} { return nil }

func (OriginalMetadataNamespace) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataNamespace(opaque)"))
}
func (OriginalMetadataNamespace) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataNamespace) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }

func (OriginalMetadataSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataSource(opaque)"))
}
func (OriginalMetadataSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataSource) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }

func (OriginalMetadataStream) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("OriginalMetadataStream(opaque)"))
}
func (OriginalMetadataStream) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*OriginalMetadataStream) UnmarshalJSON([]byte) error  { return ErrOriginalMetadataUnavailable }
