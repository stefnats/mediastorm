//go:build !linux

package importer

import (
	"context"
	"fmt"
	"novastream/config"
	"novastream/internal/database"
	"time"
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

type SABPublicationFormat struct {
	Ordinal   int
	Container string
}
type SABPublicationCopiedFile struct {
	Ordinal      int       `json:"ordinal"`
	RelativePath string    `json:"relativePath"`
	Bytes        int64     `json:"bytes"`
	SHA256       string    `json:"sha256"`
	Container    string    `json:"container"`
	ModifiedAt   time.Time `json:"modifiedAt"`
}
type SABPublicationCopyData struct {
	OperationID   string
	Files         []SABPublicationCopiedFile
	Receipt       []byte
	ReceiptDigest string
}
type SABPublicationCopy struct{}
type SABPublicationCopyConsumption struct{}

func (*SABStagedPublication) Copy(context.Context, database.SABPublicationRecord, []SABPublicationFormat) (*SABPublicationCopy, error) {
	return nil, database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) ReopenCopy(context.Context, string) (*SABPublicationCopy, error) {
	return nil, database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopy) Consume(context.Context, *SABStagedPublication, string) (*SABPublicationCopyConsumption, error) {
	return nil, database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopy) Close() {}
func (*SABPublicationCopy) CheckOriginal(context.Context, *SABStagedPublication) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) CheckCopied(context.Context) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopyConsumption) Check(context.Context) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopyConsumption) CheckPhysical(context.Context) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopyConsumption) Data() SABPublicationCopyData { return SABPublicationCopyData{} }
func (*SABPublicationCopyConsumption) RecordCatalogueState(context.Context, string) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABPublicationCopyConsumption) Close() {}
func (*Service) FindSABPublication(context.Context, int64) (*database.SABPublicationRecord, error) {
	return nil, database.ErrSABPublicationUnavailable
}
func (SABPublicationCopy) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationCopy) UnmarshalJSON([]byte) error  { return database.ErrSABPublicationUnavailable }
func (SABPublicationCopy) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationCopy(unsupported)"))
}
func (SABPublicationCopyConsumption) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationCopyConsumption) UnmarshalJSON([]byte) error {
	return database.ErrSABPublicationUnavailable
}
func (SABPublicationCopyConsumption) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationCopyConsumption(unsupported)"))
}
