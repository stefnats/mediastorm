//go:build linux

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"novastream/config"
)

func managedBackupSettings(token string) []byte {
	raw, _ := json.Marshal(map[string]any{"jellyfin": map[string]any{"accounts": []any{map[string]any{
		"id": "integration", "serverUrl": "https://provider.invalid/jellyfin", "token": token, "userId": "synthetic-user",
	}}}})
	return raw
}

func managedBackupFixture(t *testing.T, separateConfig bool) (*Service, *config.Manager, []byte) {
	t.Helper()
	cacheDir := t.TempDir()
	configDir := cacheDir
	if separateConfig {
		configDir = t.TempDir()
	}
	raw := managedBackupSettings("synthetic-config-original")
	settingsPath := filepath.Join(configDir, "settings.json")
	if os.WriteFile(settingsPath, raw, 0o600) != nil {
		t.Fatal("write original synthetic config")
	}
	if separateConfig && os.WriteFile(filepath.Join(cacheDir, "settings.json"), managedBackupSettings("synthetic-cache-original"), 0o600) != nil {
		t.Fatal("write original synthetic cache settings")
	}
	manager := config.NewManager(settingsPath)
	service, err := NewService(cacheDir, manager)
	if err != nil {
		t.Fatal("create actual backup service")
	}
	return service, manager, raw
}

func managedBackupLease(t *testing.T, manager *config.Manager) *config.ManagedJellyfinLease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	lease, err := manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		t.Fatal("capture original synthetic configuration")
	}
	t.Cleanup(func() { _ = lease.Close() })
	return lease
}

func managedBackupRestore(t *testing.T, service *Service, filename, mode string) error {
	t.Helper()
	if mode == "named" {
		return service.RestoreBackup(filename)
	}
	raw, err := os.ReadFile(filepath.Join(service.backupDir, filename))
	if err != nil {
		t.Fatal("read actual synthetic backup archive")
	}
	return service.RestoreBackupUpload(bytes.NewReader(raw), "uploaded.zip")
}

func requireManagedBackupRetired(t *testing.T, lease *config.ManagedJellyfinLease) {
	t.Helper()
	if lease.Context().Err() == nil || lease.Check(context.Background()) == nil {
		t.Fatal("actual backup replacement did not retire original configuration")
	}
}

func TestManagedConfigBackupSameValueRestoreRetiresOriginal(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			service, manager, original := managedBackupFixture(t, false)
			lease := managedBackupLease(t, manager)
			info, err := service.CreateBackup(BackupTypeManual)
			if err != nil || managedBackupRestore(t, service, info.Filename, mode) != nil {
				t.Fatal("actual same-value backup/restore failed")
			}
			requireManagedBackupRetired(t, lease)
			after, err := os.ReadFile(manager.ConfigPath())
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("same-value restore changed original bytes")
			}
			if current := managedBackupLease(t, manager); current.Check(context.Background()) != nil {
				t.Fatal("new original after restore was not available")
			}
		})
	}
}

func TestManagedConfigBackupActualABARestoreCannotReviveEitherOriginal(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			service, manager, original := managedBackupFixture(t, false)
			first := managedBackupLease(t, manager)
			info, err := service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal("create actual version-A backup")
			}
			settings := config.DefaultSettings()
			settings.Jellyfin.Accounts = []config.JellyfinAccount{{ID: "integration", ServerURL: "https://provider.invalid/jellyfin", Token: "synthetic-version-B", UserID: "synthetic-user"}}
			if config.NewManager(manager.ConfigPath()).Save(settings) != nil {
				t.Fatal("write actual managed version B")
			}
			second := managedBackupLease(t, manager)
			if managedBackupRestore(t, service, info.Filename, mode) != nil {
				t.Fatal("restore actual version A")
			}
			requireManagedBackupRetired(t, first)
			requireManagedBackupRetired(t, second)
			after, err := os.ReadFile(manager.ConfigPath())
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("actual restored version A differs from backup")
			}
			if current := managedBackupLease(t, manager); current.Check(context.Background()) != nil {
				t.Fatal("new version-A original was not usable")
			}
		})
	}
}

func TestManagedConfigBackupFailedFinalRenameStillRetiresOriginal(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			service, manager, _ := managedBackupFixture(t, false)
			lease := managedBackupLease(t, manager)
			info, err := service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal("create actual backup")
			}
			if os.Rename(manager.ConfigPath(), manager.ConfigPath()+".original") != nil || os.Mkdir(manager.ConfigPath(), 0o700) != nil {
				t.Fatal("create actual incompatible rename destination")
			}
			if lease.Context().Err() != nil {
				t.Fatal("fixture incorrectly claimed an external mutation watcher")
			}
			if managedBackupRestore(t, service, info.Filename, mode) == nil {
				t.Fatal("actual incompatible replacement succeeded")
			}
			requireManagedBackupRetired(t, lease)
			infoAfter, err := os.Stat(manager.ConfigPath())
			if err != nil || !infoAfter.IsDir() {
				t.Fatal("failed rename changed its historic destination semantics")
			}
		})
	}
}

func TestManagedConfigBackupPreservesActualCacheDestinationWithSeparateManagerPath(t *testing.T) {
	service, manager, original := managedBackupFixture(t, true)
	lease := managedBackupLease(t, manager)
	cachePath := filepath.Join(service.cacheDir, "settings.json")
	cacheOriginal, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatal("read synthetic cache settings")
	}
	info, err := service.CreateBackup(BackupTypeManual)
	if err != nil || os.WriteFile(cachePath, managedBackupSettings("synthetic-cache-modified"), 0o600) != nil {
		t.Fatal("create or alter actual cache backup fixture")
	}
	if service.RestoreBackup(info.Filename) != nil {
		t.Fatal("actual cache settings restore failed")
	}
	requireManagedBackupRetired(t, lease)
	managerAfter, managerErr := os.ReadFile(manager.ConfigPath())
	cacheAfter, cacheErr := os.ReadFile(cachePath)
	if managerErr != nil || cacheErr != nil || !bytes.Equal(managerAfter, original) || !bytes.Equal(cacheAfter, cacheOriginal) {
		t.Fatal("history hook repaired or changed the original restore destination")
	}
}

func TestManagedConfigBackupMalformedArchiveDoesNotInventSettingsMutation(t *testing.T) {
	service, manager, original := managedBackupFixture(t, false)
	lease := managedBackupLease(t, manager)
	if service.RestoreBackupUpload(bytes.NewReader([]byte("not-a-zip")), "invalid.zip") == nil {
		t.Fatal("malformed actual backup upload succeeded")
	}
	if lease.Check(context.Background()) != nil {
		t.Fatal("archive rejected before settings replacement retired the original")
	}
	after, err := os.ReadFile(manager.ConfigPath())
	if err != nil || !bytes.Equal(after, original) {
		t.Fatal("malformed archive changed original settings")
	}
}
