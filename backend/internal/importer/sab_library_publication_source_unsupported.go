//go:build !linux

package importer

import (
	"context"
	"fmt"
	"novastream/config"
	"novastream/internal/database"
)

// No source actor or filesystem fallback exists on unsupported platforms.
type SABStagedPublication struct{}

func (*Service) SABPublicationSourceReady() bool { return false }
func (*Service) OpenSABStagedPublication(context.Context, int64, config.SABPublicationPolicyValues, string, []int) (*SABStagedPublication, error) {
	return nil, database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABStagedPublication) UnmarshalJSON([]byte) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABStagedPublication(unsupported)"))
}
func (*SABStagedPublication) Check(context.Context) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) ReadAt(context.Context, int, []byte, int64) (int, error) {
	return 0, database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) Stage() database.SABPublicationStage {
	return database.SABPublicationStage{}
}
func (*SABStagedPublication) TargetBinding() string { return "" }
func (*SABStagedPublication) Claim(context.Context, string) (database.SABPublicationRecord, error) {
	return database.SABPublicationRecord{}, database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) Close() {}
