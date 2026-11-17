package importer

import (
	"context"
	"errors"
	"novastream/internal/database"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestSABManagedCopyPassiveUnknownAliasAndHistoricalRecord(t *testing.T) {
	f := newPreviewFixture(t)
	id, file := f.add(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "target")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	owner := newStageTestAuthority(t)
	policy := stageTestPolicy(root, 1000)
	sourceWatch := watchPreview(t, file)
	targetWatch := watchPreview(t, root)
	before := f.statuses.Load()
	first, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check)
	if e != nil || first.State != "unknown" || first.Reason != "namespace_unobserved" || first.ChargedBytes != nil || first.FileCount != nil {
		t.Fatal("unobserved namespace inferred no copy/zero charge")
	}
	if f.statuses.Load() != before || sourceWatch.events() != 0 || targetWatch.events() != 0 {
		t.Fatal("passive GET demanded provider or opened media")
	}
	staged, e := f.service.StageSABImport(t.Context(), id, f.settings, policy, owner.check)
	if e != nil {
		t.Fatal(e)
	}
	copy, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check)
	if e != nil || copy.State != "copy_recorded" || copy.ChargedBytes == nil || *copy.ChargedBytes != staged.ObservedBytes {
		t.Fatal("historical copy projection")
	}
	other := t.TempDir()
	if e := os.Symlink(parent, filepath.Join(other, "alias")); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(other, "alias", "target")
	aliasedPolicy := stageTestPolicy(alias, 1000)
	sourceWatch.events()
	targetWatch.events()
	before = f.statuses.Load()
	unknown, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, aliasedPolicy, owner.check)
	if e != nil || unknown.State != "unknown" || unknown.Reason != "namespace_unobserved" || unknown.ChargedBytes != nil {
		t.Fatal("unobserved alias invented zero usage")
	}
	if sourceWatch.events() != 0 || targetWatch.events() != 0 || f.statuses.Load() != before {
		t.Fatal("alias GET probed physical root")
	}
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, aliasedPolicy, owner.check); !errors.Is(e, database.ErrSABStageExists) {
		t.Fatal("alias POST recopy or failed binding", e)
	}
	if f.statuses.Load() != before || sourceWatch.events()&syscall.IN_ACCESS != 0 || stageCharge(t, f) != staged.ObservedBytes {
		t.Fatal("existing alias copied/refreshed/reset quota")
	}
	known, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, aliasedPolicy, owner.check)
	if e != nil || known.State != "copy_recorded" {
		t.Fatal("explicit observed alias not bound")
	}
	// GET still returns a historical record when actual source/target paths no
	// longer exist. This proves it is not a current file/existence grant.
	if e := os.Rename(f.local, f.local+"-moved"); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(root, root+"-moved"); e != nil {
		t.Fatal(e)
	}
	historical, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check)
	if e != nil || historical.State != "copy_recorded" {
		t.Fatal("passive getter performed filesystem validation")
	}
}
func TestSABManagedCopyReceiptRetirementDuringFinalCallback(t *testing.T) {
	for _, mode := range []string{"attempt", "removed", "status"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, _ := f.add(t)
			root := t.TempDir()
			owner := newStageTestAuthority(t)
			policy := stageTestPolicy(root, 1000)
			if _, e := f.service.StageSABImport(t.Context(), id, f.settings, policy, owner.check); e != nil {
				t.Fatal(e)
			}
			var calls atomic.Int32
			auth := func(ctx context.Context) error {
				if calls.Add(1) == 4 {
					f.mutate(t, id, func(r *sabFallbackReceipt) {
						switch mode {
						case "attempt":
							r.Attempt = strings.Repeat("e", 32)
						case "removed":
							r.Version = 999
						case "status":
							r.Status.State = "unknown"
							r.Status.Reason = "provider_state_unknown"
						}
					})
				}
				return owner.check(ctx)
			}
			value, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, auth)
			if calls.Load() < 4 || e == nil || value.State != "" {
				t.Fatal("retired receipt projection emitted")
			}
		})
	}
}
func TestSABManagedCopyUnknownStatesAndSQLFailureNeverEmptySuccess(t *testing.T) {
	f := newPreviewFixture(t)
	id, _ := f.add(t)
	root := t.TempDir()
	owner := newStageTestAuthority(t)
	policy := stageTestPolicy(root, 1000)
	disabled, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, stageTestPolicy("", 0), owner.check)
	if e != nil || disabled.Configured || disabled.State != "unknown" || disabled.ChargedBytes != nil {
		t.Fatal("disabled invented state")
	}
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, policy, owner.check); e != nil {
		t.Fatal(e)
	}
	for _, state := range []string{"pending", "failed_partial"} {
		if _, e := f.db.Connection().Exec("UPDATE sab_import_stages SET state=?", state); e != nil {
			t.Fatal(e)
		}
		v, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check)
		if e != nil || v.State != "reconciliation_required" || v.ChargedBytes == nil || *v.ChargedBytes == 0 {
			t.Fatal("pending/restart state invented active/completed/zero")
		}
	}
	if _, e := f.db.Connection().Exec("DROP TABLE sab_import_stage_roots"); e != nil {
		t.Fatal(e)
	}
	if v, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check); e == nil || !reflect.DeepEqual(v, SABManagedCopyView{}) {
		t.Fatal("SQL failure fabricated empty success")
	}
}

type managedAliasGate struct {
	*database.SABStageRepository
	before func()
	after  func()
}

func (g managedAliasGate) BindObservedPath(ctx context.Context, path, ns string) error {
	if g.before != nil {
		g.before()
	}
	e := g.SABStageRepository.BindObservedPath(ctx, path, ns)
	if g.after != nil {
		g.after()
	}
	return e
}
func TestSABManagedCopyObservedAliasOriginalChecksAroundActualBinding(t *testing.T) {
	for _, mode := range []string{"role", "target", "source", "receipt"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, _ := f.add(t)
			parent := t.TempDir()
			root := filepath.Join(parent, "target")
			if e := os.Mkdir(root, 0700); e != nil {
				t.Fatal(e)
			}
			owner := newStageTestAuthority(t)
			if _, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(root, 1000), owner.check); e != nil {
				t.Fatal(e)
			}
			aliasParent := t.TempDir()
			if e := os.Symlink(parent, filepath.Join(aliasParent, "alias")); e != nil {
				t.Fatal(e)
			}
			alias := filepath.Join(aliasParent, "alias", "target")
			policy := stageTestPolicy(alias, 1000)
			before := f.statuses.Load()
			charge := stageCharge(t, f)
			gate := managedAliasGate{SABStageRepository: database.NewSABStageRepository(f.db.Connection()), before: func() {
				switch mode {
				case "role":
					owner.master.Store(false)
				case "target":
					if e := os.Rename(root, root+"-old"); e != nil {
						t.Fatal(e)
					}
					if e := os.Mkdir(root, 0700); e != nil {
						t.Fatal(e)
					}
				case "source":
					f.mu.Lock()
					f.cfg.SABnzbd.FallbackAPIKey = "changed-synthetic"
					f.mu.Unlock()
				case "receipt":
					f.mutate(t, id, func(r *sabFallbackReceipt) { r.Attempt = strings.Repeat("e", 32) })
				}
			}}
			v, e := f.service.stageSABImportWithRepository(t.Context(), id, f.settings, policy, owner.check, gate)
			if e == nil || errors.Is(e, database.ErrSABStageExists) || v.State == "staged_copy" {
				t.Fatal("late alias mutation accepted")
			}
			if f.statuses.Load() != before || stageCharge(t, f) != charge {
				t.Fatal("late alias gate copied or reset charge")
			}
		})
	}
}

type managedAliasUnknown struct{ *database.SABStageRepository }

func (g managedAliasUnknown) BindObservedPath(ctx context.Context, path, ns string) error {
	if e := g.SABStageRepository.BindObservedPath(ctx, path, ns); e != nil {
		return e
	}
	return database.ErrSABStageOutcomeUnknown
}
func TestSABManagedCopyAliasResultUnknownAfterActualBindingNeverRecopies(t *testing.T) {
	f := newPreviewFixture(t)
	id, _ := f.add(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "target")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	owner := newStageTestAuthority(t)
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(root, 1000), owner.check); e != nil {
		t.Fatal(e)
	}
	other := t.TempDir()
	if e := os.Symlink(parent, filepath.Join(other, "alias")); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(other, "alias", "target")
	policy := stageTestPolicy(alias, 1000)
	before := f.statuses.Load()
	charge := stageCharge(t, f)
	v, e := f.service.stageSABImportWithRepository(t.Context(), id, f.settings, policy, owner.check, managedAliasUnknown{database.NewSABStageRepository(f.db.Connection())})
	if !errors.Is(e, database.ErrSABStageOutcomeUnknown) || v.State != "outcome_unknown" {
		t.Fatal("post-real-commit alias result was misreported")
	}
	known, e := f.service.GetSABManagedCopy(t.Context(), id, f.settings, policy, owner.check)
	if e != nil || known.State != "copy_recorded" {
		t.Fatal("actual committed binding absent")
	}
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, policy, owner.check); !errors.Is(e, database.ErrSABStageExists) {
		t.Fatal("unknown alias repeated copy")
	}
	if f.statuses.Load() != before || stageCharge(t, f) != charge {
		t.Fatal("unknown alias demanded SAB or changed charge")
	}
}
