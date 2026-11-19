package kidsvod

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestParentFileProcessCannotPromoteInternalIOOwnerToPublicPermission(t *testing.T) {
	s, f := newFixture(t)
	source := &pendingFileSource{}
	entry := fileIOOwner(t, s, f, source)
	before := source.opens.Load()
	if lease, err := s.PrepareParentFileProcessRequest(f.request(false), "child", source.ItemID(), entry.id); lease != nil || err == nil {
		t.Fatal("internal IO test owner became a public process permission")
	}
	if source.opens.Load() != before {
		t.Fatal("process input opened without actual original PG file permit")
	}
}
func TestParentFileProcessOpaqueImportCannotRunHandoffOrReserveOutput(t *testing.T) {
	var imported ParentFileProcessLease
	if json.Unmarshal([]byte("{}"), &imported) == nil {
		t.Fatal("imported process authority")
	}
	if imported.Check(t.Context()) == nil || imported.Run(t.Context(), "/synthetic-must-not-run", nil, nil, nil, nil) == nil {
		t.Fatal("imported process ran")
	}
	if imported.Handoff(nil) == nil || imported.BindOutputRetirement(func() {}, make(chan struct{})) == nil {
		t.Fatal("imported process adopted worker lifetime")
	}
	if release, err := imported.ReserveOutput(nil, "child", "item"); err == nil || release != nil {
		t.Fatal("imported process reserved output")
	}
	if raw, err := json.Marshal(&imported); err != nil || string(raw) != "null" {
		t.Fatal("opaque process serialized")
	}
	if fmt.Sprintf("%+v", &imported) != "ParentFileProcessLease(opaque)" {
		t.Fatal("private process details exposed")
	}
	if imported.Context().Err() == nil || imported.Size() != 0 || imported.OutputLimit() != 0 || !imported.ExpiresAt().IsZero() {
		t.Fatal("imported worker retained metadata")
	}
}
func TestParentFileProcessUnavailableContextNeverAcquiresGlobalCapacity(t *testing.T) {
	s, f := newFixture(t)
	ctx, cancel := context.WithCancel(f.request(false).Context())
	cancel()
	parentFileProcessAdmission.Lock()
	before := len(parentFileProcessAdmission.entries)
	bytes := parentFileProcessAdmission.bytes
	parentFileProcessAdmission.Unlock()
	if lease, err := s.PrepareParentFileProcessRequest(f.request(false).WithContext(ctx), "child", "actual-unmatched-file", "lfa_synthetic"); lease != nil || err == nil {
		t.Fatal("canceled request acquired process")
	}
	parentFileProcessAdmission.Lock()
	defer parentFileProcessAdmission.Unlock()
	if len(parentFileProcessAdmission.entries) != before || parentFileProcessAdmission.bytes != bytes {
		t.Fatal("invalid request leaked process reservation")
	}
}
