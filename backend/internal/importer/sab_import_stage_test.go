package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"novastream/config"
	"novastream/internal/database"
	"novastream/models"
	"novastream/services/sessions"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Actual nonrenewing in-memory sessions; role is a controlled current-account
// port. This is not a SQL/login or installed HTTP-authority claim.
type stageTestAuthority struct {
	sessions *sessions.Service
	original models.Session
	master   atomic.Bool
}

func newStageTestAuthority(t *testing.T) *stageTestAuthority {
	t.Helper()
	s, e := sessions.NewService("", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.Create("synthetic-stage-master", true, "test", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	a := &stageTestAuthority{sessions: s, original: v}
	a.master.Store(true)
	return a
}
func (a *stageTestAuthority) check(ctx context.Context) error {
	if ctx.Err() != nil || !a.master.Load() || !time.Now().Before(a.original.ExpiresAt) {
		return errors.New("original authority unavailable")
	}
	v, e := a.sessions.SnapshotForAuthorization(ctx, a.original.Token)
	if e != nil || v == nil || !reflect.DeepEqual(*v, a.original) {
		return errors.New("original session unavailable")
	}
	return nil
}
func stageTestPolicy(root string, quota int64) SABImportStageSettings {
	return func(ctx context.Context) (SABImportStagePolicy, error) {
		return SABImportStagePolicy{root, quota}, ctx.Err()
	}
}
func stageCharge(t *testing.T, f *previewFixture) int64 {
	t.Helper()
	var total int64
	if e := f.db.Connection().QueryRow("SELECT COALESCE(SUM(expected_bytes),0) FROM sab_import_stages").Scan(&total); e != nil {
		t.Fatal(e)
	}
	return total
}
func stageOperation(t *testing.T, f *previewFixture) (string, string) {
	t.Helper()
	var id, state string
	if e := f.db.Connection().QueryRow("SELECT operation_id,state FROM sab_import_stages ORDER BY created_at DESC LIMIT 1").Scan(&id, &state); e != nil {
		t.Fatal(e)
	}
	return id, state
}
func TestSABImportStageActualCopyHashReopenAndPreviewStillMetadataOnly(t *testing.T) {
	f := newPreviewFixture(t)
	id, file := f.add(t)
	payload := []byte(strings.Repeat("exact synthetic source bytes", 80000))
	if e := os.WriteFile(file, payload, 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(filepath.Dir(file), "sidecar.PRIVATE_SUFFIX"), []byte("sidecar bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	watch := watchPreview(t, file)
	if _, e := f.call(t.Context(), id); e != nil {
		t.Fatal(e)
	}
	if watch.events()&syscall.IN_ACCESS != 0 {
		t.Fatal("public preview read payload")
	}
	root := t.TempDir()
	owner := newStageTestAuthority(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result, e := f.service.StageSABImport(ctx, id, f.settings, stageTestPolicy(root, 8<<20), owner.check)
	if e != nil || result.State != "staged_copy" || len(result.Files) != 2 {
		t.Fatal("actual copy unavailable", e)
	}
	if watch.events()&syscall.IN_ACCESS == 0 {
		t.Fatal("actual source payload read not observed")
	}
	op, state := stageOperation(t, f)
	if state != "copied" {
		t.Fatal("durable copy completion missing")
	}
	names, e := os.ReadDir(filepath.Dir(file))
	if e != nil {
		t.Fatal(e)
	}
	sort.Slice(names, func(i, j int) bool { return names[i].Name() < names[j].Name() })
	var total int64
	for i, name := range names {
		original, e := os.ReadFile(filepath.Join(filepath.Dir(file), name.Name()))
		if e != nil {
			t.Fatal(e)
		}
		stored, e := os.ReadFile(filepath.Join(root, op, fmt.Sprintf("data-%04d.bin", i+1)))
		if e != nil || !reflect.DeepEqual(original, stored) {
			t.Fatal("exact copied bytes differ")
		}
		sum := sha256.Sum256(original)
		if result.Files[i].SHA256 != hex.EncodeToString(sum[:]) || result.Files[i].Bytes != int64(len(stored)) {
			t.Fatal("written-byte hash or exact size")
		}
		total += int64(len(stored))
	}
	if result.ObservedBytes != total || stageCharge(t, f) != total {
		t.Fatal("logical charge differs from complete inventory")
	}
	before := f.statuses.Load()
	f.db.Close()
	f.open(t)
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(root, 16<<20), owner.check); !errors.Is(e, database.ErrSABStageExists) {
		t.Fatal("copied attempt retried after restart")
	}
	if f.statuses.Load() != before || stageCharge(t, f) != total {
		t.Fatal("reopen or policy change reset charge or demanded status")
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Fatal("reopen created second copy")
	}
}
func TestSABImportStageSeparateRootsBeforeAnySourceWrite(t *testing.T) {
	for _, mode := range []string{"same", "source child", "output directory", "source alias", "target ancestor"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, file := f.add(t)
			target := f.local
			switch mode {
			case "source child":
				target = filepath.Join(f.local, "target")
				if e := os.Mkdir(target, 0700); e != nil {
					t.Fatal(e)
				}
			case "output directory":
				target = filepath.Dir(file)
			case "source alias":
				parent := t.TempDir()
				if e := os.Symlink(filepath.Dir(f.local), filepath.Join(parent, "alias")); e != nil {
					t.Fatal(e)
				}
				target = filepath.Join(parent, "alias", filepath.Base(f.local))
			case "target ancestor":
				target = filepath.Dir(f.local)
			}
			watch := watchPreview(t, f.local)
			if _, e := syscall.InotifyAddWatch(watch.fd, f.local, syscall.IN_CREATE|syscall.IN_DELETE|syscall.IN_MOVED_FROM|syscall.IN_MOVED_TO|syscall.IN_MODIFY); e != nil {
				t.Fatal(e)
			}
			if _, e := syscall.InotifyAddWatch(watch.fd, filepath.Dir(file), syscall.IN_CREATE|syscall.IN_DELETE|syscall.IN_MOVED_FROM|syscall.IN_MOVED_TO|syscall.IN_MODIFY); e != nil {
				t.Fatal(e)
			}
			original, e := os.ReadFile(file)
			if e != nil {
				t.Fatal(e)
			}
			owner := newStageTestAuthority(t)
			if _, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(target, 1000), owner.check); e == nil {
				t.Fatal("nested/same/aliased source target accepted")
			}
			if watch.events() != 0 {
				t.Fatal("source changed before disjointness denial")
			}
			after, e := os.ReadFile(file)
			if e != nil || !reflect.DeepEqual(original, after) {
				t.Fatal("source content changed")
			}
			var count int
			if e := f.db.Connection().QueryRow("SELECT count(*) FROM sab_import_stages").Scan(&count); e != nil || count != 0 {
				t.Fatal("pre-write disjointness denial created claim")
			}
		})
	}
}
func TestSABImportStageUnknownOrInvalidPolicyZeroDemand(t *testing.T) {
	for _, mode := range []string{"nil context", "cancelled", "nil authority", "nil target", "empty root", "relative root", "zero quota", "negative quota", "root symlink", "root FIFO", "retired owner"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, _ := f.add(t)
			root := t.TempDir()
			ctx := t.Context()
			owner := newStageTestAuthority(t)
			auth := owner.check
			policy := stageTestPolicy(root, 1000)
			switch mode {
			case "nil context":
				ctx = nil
			case "cancelled":
				c, stop := context.WithCancel(ctx)
				stop()
				ctx = c
			case "nil authority":
				auth = nil
			case "nil target":
				policy = nil
			case "empty root":
				policy = stageTestPolicy("", 1000)
			case "relative root":
				policy = stageTestPolicy("relative", 1000)
			case "zero quota":
				policy = stageTestPolicy(root, 0)
			case "negative quota":
				policy = stageTestPolicy(root, -1)
			case "root symlink":
				link := filepath.Join(t.TempDir(), "link")
				if e := os.Symlink(root, link); e != nil {
					t.Fatal(e)
				}
				policy = stageTestPolicy(link, 1000)
			case "root FIFO":
				pipe := filepath.Join(t.TempDir(), "pipe")
				if e := syscall.Mkfifo(pipe, 0600); e != nil {
					t.Fatal(e)
				}
				policy = stageTestPolicy(pipe, 1000)
			case "retired owner":
				owner.master.Store(false)
			}
			before := f.statuses.Load()
			if _, e := f.service.StageSABImport(ctx, id, f.settings, policy, auth); e == nil {
				t.Fatal("invalid policy accepted")
			}
			if f.statuses.Load() != before || stageCharge(t, f) != 0 {
				t.Fatal("invalid target/authority demanded or charged")
			}
		})
	}
}
func TestSABImportStageRetirementAfterActualReadKeepsChargeAndNoPublication(t *testing.T) {
	for _, mode := range []string{"role", "session", "cancel", "source credentials", "receipt", "quota change", "source replacement", "source grow", "source root replacement", "target replacement"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, file := f.add(t)
			root := t.TempDir()
			owner := newStageTestAuthority(t)
			watch := watchPreview(t, file)
			var changed atomic.Bool
			quota := int64(1000)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			policy := func(ctx context.Context) (SABImportStagePolicy, error) {
				return SABImportStagePolicy{root, quota}, ctx.Err()
			}
			auth := func(work context.Context) error {
				if watch.events()&syscall.IN_ACCESS != 0 && changed.CompareAndSwap(false, true) {
					switch mode {
					case "role":
						owner.master.Store(false)
					case "session":
						owner.sessions.Revoke(owner.original.Token)
					case "cancel":
						cancel()
					case "source credentials":
						f.mu.Lock()
						f.cfg.SABnzbd.FallbackAPIKey = "changed-synthetic"
						f.mu.Unlock()
					case "receipt":
						f.mutate(t, id, func(r *sabFallbackReceipt) { r.Attempt = strings.Repeat("e", 32) })
					case "quota change":
						quota = 900
					case "source replacement":
						if e := os.Rename(file, file+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.WriteFile(file, []byte("replacement"), 0600); e != nil {
							t.Fatal(e)
						}
					case "source grow":
						if e := os.Truncate(file, 100); e != nil {
							t.Fatal(e)
						}
					case "source root replacement":
						if e := os.Rename(f.local, f.local+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.Mkdir(f.local, 0700); e != nil {
							t.Fatal(e)
						}
					case "target replacement":
						if e := os.Rename(root, root+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.Mkdir(root, 0700); e != nil {
							t.Fatal(e)
						}
					}
				}
				return owner.check(work)
			}
			result, e := f.service.StageSABImport(ctx, id, f.settings, policy, auth)
			if !changed.Load() || e == nil || result.State == "staged_copy" {
				t.Fatal("actual read barrier missed or retired observation published")
			}
			if stageCharge(t, f) == 0 || len(f.service.fallbackSlots) != 0 {
				t.Fatal("failed partial lost charge or held slot")
			}
			_, state := stageOperation(t, f)
			if state != "failed_partial" {
				t.Fatal("failed partial state not recorded")
			}
		})
	}
}

type stageResultLoss struct {
	*database.SABStageRepository
	phase string
}

func (r stageResultLoss) Claim(ctx context.Context, c database.SABStageClaim, n int64) (database.SABStageRecord, error) {
	v, e := r.SABStageRepository.Claim(ctx, c, n)
	if e == nil && r.phase == "claim" {
		return v, database.ErrSABStageOutcomeUnknown
	}
	return v, e
}
func (r stageResultLoss) Finish(ctx context.Context, id, state string, f []database.SABStageFile) error {
	e := r.SABStageRepository.Finish(ctx, id, state, f)
	if e == nil && r.phase == "finish" {
		return database.ErrSABStageOutcomeUnknown
	}
	return e
}
func TestSABImportStageSimulatedLostResultAfterActualSQLiteCommit(t *testing.T) {
	for _, phase := range []string{"claim", "finish"} {
		t.Run(phase, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, _ := f.add(t)
			root := t.TempDir()
			owner := newStageTestAuthority(t)
			policy := stageTestPolicy(root, 1000)
			// An application fault port returns an error only AFTER the real repository
			// operation (and real SQL COMMIT) succeeded. No network ACK is simulated.
			repo := stageResultLoss{database.NewSABStageRepository(f.db.Connection()), phase}
			result, e := f.service.stageSABImportWithRepository(t.Context(), id, f.settings, policy, owner.check, repo)
			if !errors.Is(e, database.ErrSABStageOutcomeUnknown) || result.State != "outcome_unknown" || stageCharge(t, f) == 0 {
				t.Fatal("unknown outcome pretended rollback or success")
			}
			entries, _ := os.ReadDir(root)
			want := 0
			if phase == "finish" {
				want = 1
			}
			if len(entries) != want {
				t.Fatal("claim error created files or finish did not perform actual copy")
			}
			before := f.statuses.Load()
			f.db.Close()
			f.open(t)
			if _, e := f.service.StageSABImport(t.Context(), id, f.settings, policy, owner.check); !errors.Is(e, database.ErrSABStageExists) {
				t.Fatal("unknown attempt retried")
			}
			if f.statuses.Load() != before {
				t.Fatal("unknown attempt refreshed or re-copied after restart")
			}
		})
	}
}
func TestSABImportStageForeignGeneratedDirectoryNeverAdopted(t *testing.T) {
	f := newPreviewFixture(t)
	id, _ := f.add(t)
	root := t.TempDir()
	owner := newStageTestAuthority(t)
	var created atomic.Bool
	auth := func(ctx context.Context) error {
		if !created.Load() {
			var op string
			if e := f.db.Connection().QueryRow("SELECT operation_id FROM sab_import_stages LIMIT 1").Scan(&op); e == nil && created.CompareAndSwap(false, true) {
				if e := os.Mkdir(filepath.Join(root, op), 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(root, op, "foreign"), []byte("foreign sentinel"), 0600); e != nil {
					t.Fatal(e)
				}
			}
		}
		return owner.check(ctx)
	}
	if _, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(root, 1000), auth); e == nil || !created.Load() {
		t.Fatal("foreign directory adopted or barrier missed")
	}
	op, _ := stageOperation(t, f)
	files, e := os.ReadDir(filepath.Join(root, op))
	if e != nil || len(files) != 1 || files[0].Name() != "foreign" {
		t.Fatal("foreign directory changed")
	}
	data, e := os.ReadFile(filepath.Join(root, op, "foreign"))
	if e != nil || string(data) != "foreign sentinel" {
		t.Fatal("foreign file overwritten")
	}
}
func TestSABImportStageSamePhysicalRootAliasCannotResetQuota(t *testing.T) {
	f := newPreviewFixture(t)
	a, _ := f.add(t)
	b, _ := f.add(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "target")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	owner := newStageTestAuthority(t)
	first, e := f.service.StageSABImport(t.Context(), a, f.settings, stageTestPolicy(root, 40), owner.check)
	if e != nil {
		t.Fatal(e)
	}
	other := t.TempDir()
	if e := os.Symlink(parent, filepath.Join(other, "alias")); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(other, "alias", "target")
	if _, e := f.service.StageSABImport(t.Context(), b, f.settings, stageTestPolicy(alias, 40), owner.check); !errors.Is(e, database.ErrSABStageQuota) {
		t.Fatal("configured path alias reset quota", e)
	}
	if stageCharge(t, f) != first.ObservedBytes {
		t.Fatal("alias denial changed charge")
	}
}

func TestSABImportStageTwoActualReadsRetainSlotsUntilReturn(t *testing.T) {
	f := newPreviewFixture(t)
	a, af := f.add(t)
	b, bf := f.add(t)
	c, _ := f.add(t)
	root := t.TempDir()
	policy := stageTestPolicy(root, 1000)
	watches := []*previewWatch{watchPreview(t, af), watchPreview(t, bf)}
	owners := []*stageTestAuthority{newStageTestAuthority(t), newStageTestAuthority(t)}
	entered := make(chan int, 2)
	release := make(chan struct{})
	done := make(chan error, 2)
	var once [2]atomic.Bool
	run := func(index int, id int64, ctx context.Context) {
		_, e := f.service.StageSABImport(ctx, id, f.settings, policy, func(work context.Context) error {
			if watches[index].events()&syscall.IN_ACCESS != 0 && once[index].CompareAndSwap(false, true) {
				entered <- index
				<-release
			}
			return owners[index].check(work)
		})
		done <- e
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go run(0, a, ctx)
	go run(1, b, t.Context())
	for n := 0; n < 2; n++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("actual payload-read barrier")
		}
	}
	if len(f.service.fallbackSlots) != 2 {
		close(release)
		t.Fatal("copy did not own two slots")
	}
	cancel()
	before := f.statuses.Load()
	owner := newStageTestAuthority(t)
	if _, e := f.service.StageSABImport(t.Context(), c, f.settings, policy, owner.check); !errors.Is(e, ErrSABFallbackBusy) {
		close(release)
		t.Fatal("third input accepted")
	}
	if len(f.service.fallbackSlots) != 2 || f.statuses.Load() != before {
		close(release)
		t.Fatal("canceled copy released slot before actual return")
	}
	close(release)
	failures := 0
	for n := 0; n < 2; n++ {
		select {
		case e := <-done:
			if e != nil {
				failures++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("copy/FD lifetime not joined")
		}
	}
	if failures != 1 || len(f.service.fallbackSlots) != 0 || stageCharge(t, f) == 0 {
		t.Fatal("actual return lost charge or slots")
	}
}
func TestSABImportStageSimulatedENOSPCAndExclusiveFilePrimitive(t *testing.T) {
	for _, mode := range []string{"simulated ENOSPC after real write", "foreign existing file"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			sourceName := filepath.Join(t.TempDir(), "source")
			if e := os.WriteFile(sourceName, []byte("0123456789"), 0600); e != nil {
				t.Fatal(e)
			}
			source, e := os.Open(sourceName)
			if e != nil {
				t.Fatal(e)
			}
			defer source.Close()
			info, e := source.Stat()
			if e != nil {
				t.Fatal(e)
			}
			directory, e := os.Open(root)
			if e != nil {
				t.Fatal(e)
			}
			defer directory.Close()
			destination := filepath.Join(root, "data-0001.bin")
			if mode == "foreign existing file" {
				if e := os.WriteFile(destination, []byte("foreign sentinel"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			calls := 0
			writer := func(f *os.File, p []byte) (int, error) {
				calls++
				if calls == 1 {
					return f.Write(p)
				}
				return 0, syscall.ENOSPC
			}
			_, e = stageCopyWithWriter(t.Context(), directory, 0, sabInventoryFile{"private", source, info}, make([]byte, 4), func() error { return nil }, writer)
			if e == nil {
				t.Fatal("partial copy or existing destination accepted")
			}
			data, e := os.ReadFile(destination)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "foreign existing file" {
				if string(data) != "foreign sentinel" || calls != 0 {
					t.Fatal("foreign file overwritten")
				}
			} else if string(data) != "0123" || calls != 2 {
				t.Fatal("simulated write fault did not follow actual partial write")
			}
			// This is a private copy-primitive/fault-port test, not real disk exhaustion
			// and not a current-master/SAB/receipt/SQLite authorization test.
		})
	}
}

func TestSABImportStageMissingServiceFailsBeforeCallbacks(t *testing.T) {
	for _, service := range []*Service{nil, {}, {database: &database.DB{}}} {
		var calls atomic.Int32
		_, e := service.StageSABImport(t.Context(), 1, func(context.Context) (config.SABnzbdSettings, error) {
			calls.Add(1)
			return config.SABnzbdSettings{}, nil
		}, func(context.Context) (SABImportStagePolicy, error) { calls.Add(1); return SABImportStagePolicy{}, nil }, func(context.Context) error { calls.Add(1); return nil })
		if !errors.Is(e, ErrSABImportStageUnavailable) || calls.Load() != 0 {
			t.Fatal("missing service/database reached a callback")
		}
	}
}
func TestSABImportStageEarlierTargetChangeDuringSecondSourceRead(t *testing.T) {
	for _, mode := range []string{"replacement", "size"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, first := f.add(t)
			second := filepath.Join(filepath.Dir(first), "zz-second.mkv")
			if e := os.WriteFile(second, []byte("second synthetic source"), 0600); e != nil {
				t.Fatal(e)
			}
			root := t.TempDir()
			watch := watchPreview(t, second)
			owner := newStageTestAuthority(t)
			var changed atomic.Bool
			auth := func(ctx context.Context) error {
				if watch.events()&syscall.IN_ACCESS != 0 && changed.CompareAndSwap(false, true) {
					op, _ := stageOperation(t, f)
					target := filepath.Join(root, op, "data-0001.bin")
					if mode == "replacement" {
						if e := os.Rename(target, target+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.WriteFile(target, []byte("foreign replacement"), 0600); e != nil {
							t.Fatal(e)
						}
					} else if e := os.Truncate(target, 1); e != nil {
						t.Fatal(e)
					}
				}
				return owner.check(ctx)
			}
			result, e := f.service.StageSABImport(t.Context(), id, f.settings, stageTestPolicy(root, 1000), auth)
			if !changed.Load() || e == nil || result.State == "staged_copy" {
				t.Fatal("earlier target mutation published or actual second-read barrier missed")
			}
			_, state := stageOperation(t, f)
			if state != "failed_partial" || stageCharge(t, f) == 0 || len(f.service.fallbackSlots) != 0 {
				t.Fatal("target mutation released charge or skipped actual cleanup")
			}
		})
	}
}
