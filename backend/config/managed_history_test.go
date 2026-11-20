package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These coordinator tests deliberately use a noncredential file double. Actual
// no-follow descriptors and raw credential decoding are covered separately.
type managedCoordinatorTestFile struct{ closed atomic.Bool }

func (file *managedCoordinatorTestFile) check(ctx context.Context) error {
	if file.closed.Load() || ctx.Err() != nil {
		return ErrManagedConfigurationUnavailable
	}
	return nil
}

func (file *managedCoordinatorTestFile) close() error {
	file.closed.Store(true)
	return nil
}

func managedCoordinatorContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func managedCoordinatorLease(t *testing.T, path string) *ManagedJellyfinLease {
	t.Helper()
	lease, err := captureManagedJellyfin(managedCoordinatorContext(t), NewManager(path), "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
		return &managedCoordinatorTestFile{}, managedJellyfinCredentials{}, nil
	})
	if err != nil {
		t.Fatal("coordinator capture failed")
	}
	t.Cleanup(func() { _ = lease.Close() })
	return lease
}

func managedRequireRetired(t *testing.T, lease *ManagedJellyfinLease) {
	t.Helper()
	if lease.Context() == nil || lease.Context().Err() == nil || lease.Check(context.Background()) == nil {
		t.Fatal("original managed epoch survived a mutation")
	}
}

func TestManagedConfigurationSaveAttemptsRetireOriginal(t *testing.T) {
	for _, name := range []string{"same-value", "normalization-failure", "filesystem-failure", "empty-path", "unrelated-path"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "settings.json")
			manager := NewManager(path)
			settings := DefaultSettings()
			if manager.Save(settings) != nil {
				t.Fatal("write initial synthetic settings")
			}
			lease := managedCoordinatorLease(t, path)
			writer := NewManager(path)
			wantFailure := false
			switch name {
			case "normalization-failure":
				settings.Server.ExternalBackendURL = "file:///unsupported"
				wantFailure = true
			case "filesystem-failure":
				if os.Mkdir(path+".tmp", 0o700) != nil {
					t.Fatal("create actual conflicting staging directory")
				}
				wantFailure = true
			case "empty-path":
				writer = NewManager("")
				wantFailure = true
			case "unrelated-path":
				writer = NewManager(filepath.Join(t.TempDir(), "other.json"))
			}
			err := writer.Save(settings)
			if (err != nil) != wantFailure {
				t.Fatal("Save did not retain its original success/failure contract")
			}
			managedRequireRetired(t, lease)
		})
	}
}

func TestManagedConfigurationReplacementAttemptsRetireOriginal(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "settings.json")
			lease := managedCoordinatorLease(t, path)
			staged := filepath.Join(directory, "staged")
			if !missing && os.WriteFile(staged, []byte("same-value"), 0o600) != nil {
				t.Fatal("write synthetic staged file")
			}
			err := ReplaceManagedSettingsFile(staged, path)
			if (err != nil) != missing {
				t.Fatal("replacement changed rename semantics")
			}
			managedRequireRetired(t, lease)
		})
	}
}

func waitManagedWriterPending(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !managedConfigHistory.TryRLock() {
			return
		}
		managedConfigHistory.RUnlock()
		runtime.Gosched()
	}
	t.Fatal("actual writer did not become pending")
}

func TestManagedConfigurationCaptureDeniesPendingWriterWithoutReading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	lease := managedCoordinatorLease(t, path)
	managedConfigHistory.RLock()
	var release sync.Once
	unlock := func() { release.Do(managedConfigHistory.RUnlock) }
	defer unlock()
	written := make(chan error, 1)
	go func() { written <- NewManager(path).Save(DefaultSettings()) }()
	waitManagedWriterPending(t)
	var read atomic.Bool
	got, err := captureManagedJellyfin(managedCoordinatorContext(t), NewManager(path), "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
		read.Store(true)
		return &managedCoordinatorTestFile{}, managedJellyfinCredentials{}, nil
	})
	if got != nil || !errors.Is(err, ErrManagedConfigurationBusy) || read.Load() {
		t.Fatal("capture waited for or read through an actual pending writer")
	}
	unlock()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal("actual writer failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("actual writer did not finish")
	}
	managedRequireRetired(t, lease)
}

func TestManagedConfigurationCaptureAdmissionIsBoundedAndDoesNotGateWrites(t *testing.T) {
	directory := t.TempDir()
	var leases []*ManagedJellyfinLease
	for index := 0; index < managedLeaseLimit; index++ {
		path := filepath.Join(directory, fmt.Sprintf("settings-%d.json", index/managedPathLeaseLimit))
		leases = append(leases, managedCoordinatorLease(t, path))
	}
	path := filepath.Join(directory, "settings-0.json")
	reader := func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
		t.Fatal("capacity denial performed file IO")
		return nil, managedJellyfinCredentials{}, nil
	}
	if lease, err := captureManagedJellyfin(managedCoordinatorContext(t), NewManager(path), "integration", reader); lease != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("full global/path admission was not enforced")
	}
	if NewManager(filepath.Join(directory, "writer.json")).Save(DefaultSettings()) != nil {
		t.Fatal("lease capacity denied an existing writer")
	}
	managedConfigAdmission.Lock()
	charged := managedConfigAdmission.leases
	managedConfigAdmission.Unlock()
	if charged != managedLeaseLimit {
		t.Fatal("cancellation refunded owners before explicit cleanup")
	}
	if leases[0].Close() != nil {
		t.Fatal("close original slot")
	}
	replacement := managedCoordinatorLease(t, path)
	if replacement.Check(context.Background()) != nil {
		t.Fatal("actual closed slot could not be reused")
	}
}

func TestManagedConfigurationDecodeAdmissionRefundsActualFailures(t *testing.T) {
	started := make(chan struct{}, managedDecodeLimit)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	results := make(chan error, managedDecodeLimit)
	directory := t.TempDir()
	ctx := managedCoordinatorContext(t)
	for index := 0; index < managedDecodeLimit; index++ {
		path := filepath.Join(directory, fmt.Sprintf("settings-%d.json", index))
		go func() {
			_, err := captureManagedJellyfin(ctx, NewManager(path), "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
				started <- struct{}{}
				<-release
				return &managedCoordinatorTestFile{}, managedJellyfinCredentials{}, errors.New("synthetic read failure")
			})
			results <- err
		}()
	}
	for index := 0; index < managedDecodeLimit; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("bounded decoders did not enter")
		}
	}
	var called atomic.Bool
	_, err := captureManagedJellyfin(ctx, NewManager(filepath.Join(directory, "overflow")), "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
		called.Store(true)
		return nil, managedJellyfinCredentials{}, nil
	})
	if !errors.Is(err, ErrManagedConfigurationBusy) || called.Load() {
		t.Fatal("decode admission did not reject before IO")
	}
	releaseOnce.Do(func() { close(release) })
	for index := 0; index < managedDecodeLimit; index++ {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("failed decoder returned a lease")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("decoder cleanup did not join")
		}
	}
	managedConfigAdmission.Lock()
	clean := managedConfigAdmission.leases == 0 && managedConfigAdmission.decoding == 0 && len(managedConfigAdmission.paths) == 0 && len(managedConfigAdmission.owners) == 0
	managedConfigAdmission.Unlock()
	if !clean {
		t.Fatal("failed capture retained admission or history owners")
	}
}

func TestManagedConfigurationPathAdmissionIsIndependentOfGlobalCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	for index := 0; index < managedPathLeaseLimit; index++ {
		managedCoordinatorLease(t, path)
	}
	_, err := captureManagedJellyfin(managedCoordinatorContext(t), NewManager(path), "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
		t.Fatal("per-path overflow performed IO")
		return nil, managedJellyfinCredentials{}, nil
	})
	if !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("original path admission was not bounded")
	}
	other := managedCoordinatorLease(t, filepath.Join(t.TempDir(), "other.json"))
	if other.Check(context.Background()) != nil {
		t.Fatal("path capacity incorrectly exhausted global admission")
	}
}
